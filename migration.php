<?php


    // php migration.php --user=root --pass --db=quarantine-control

    // php migration.php --user=root --pass=ZD1FODGRhT --db=qc-enf --file=./database-1600870653.zip

    // php migration.php --user=admin_g18 --pass=ZD1FODGRhT --db=admin_gestor_2018

    function getoptreq ($options, $longopts)  {
        if (PHP_SAPI === 'cli' || empty($_SERVER['REMOTE_ADDR']))  // command line
        {
            return getopt($options, $longopts);
        }
        else if (isset($_REQUEST))  // web script
        {
            $found = array();

            $shortopts = preg_split('@([a-z0-9][:]{0,2})@i', $options, 0, PREG_SPLIT_DELIM_CAPTURE | PREG_SPLIT_NO_EMPTY);
            $opts = array_merge($shortopts, $longopts);

            foreach ($opts as $opt)
            {
                if (substr($opt, -2) === '::')  // optional
                {
                    $key = substr($opt, 0, -2);

                    if (isset($_REQUEST[$key]) && !empty($_REQUEST[$key]))
                    $found[$key] = $_REQUEST[$key];
                    else if (isset($_REQUEST[$key]))
                    $found[$key] = false;
                }
                else if (substr($opt, -1) === ':')  // required value
                {
                    $key = substr($opt, 0, -1);

                    if (isset($_REQUEST[$key]) && !empty($_REQUEST[$key]))
                    $found[$key] = $_REQUEST[$key];
                }
                else if (ctype_alnum($opt))  // no value
                {
                    if (isset($_REQUEST[$opt]))
                    $found[$opt] = false;
                }
            }

            return $found;
        }

        return false;
    }

    $opt = getoptreq("", [
        "user::",
        "pass::",
        "db::",
        "file::"        
    ]);

    class DefaultType {
        protected $ignore_value = "__IGNORE__";
        protected $force_default_value = null;
        protected $regexp_validate = null;
        protected $wrap = "'";

        protected $middlewares = [
            "validate",
            "input_data",
            "json_encode",
            "empty_value",
            "before_wrap",
            "wrapHandle"
        ];

        function __construct( $type, $cols ){
            // if($cols["Field"] == "momment_count"){
            //     print_r($cols);
            //     exit();
            // }
            $this->is_null = $cols["Null"] == "YES" ? true : false;
            $this->field = $cols["Field"];
            $this->default_value = $cols["Default"];
            $this->type = $cols["Type"];
        }

        protected function empty_value($val){
            if( empty($val) || $val == $this->ignore_value ) {
                if( !empty( $this->default_value ) ) return $this->default_value;
                else if( !is_null($this->force_default_value) ) return $this->force_default_value;
                else if( $this->is_null ) return $this->ignore_value;
            
            }else return $val;
        }

        protected function json_encode($val){
            if( is_array($val) ) return json_encode( $val, true );
            else if( is_object($val) ) return json_encode( $val );
            return $val;
        }

        protected function validate( $val ){
            if( !is_null($this->regexp_validate) ) {
                preg_match( $this->regexp_validate, $val, $output_array );
                if( empty($output_array) ) {
                    return $this->ignore_value;
                }
            }
            return $val;
        }

        protected function wrapHandle($value){
            return "{$this->wrap}{$value}{$this->wrap}";
        }

        public function parse( $value ){
            $self = $this;
            return array_reduce($this->middlewares, function($value, $middleware) use ($self) {
                if( method_exists( $self, $middleware ) ){
                    return $self->{$middleware}($value);
                }
                return $value;
            }, $value);           
        }
        
    }

    class DoubleType extends DefaultType {
        protected $force_default_value = '0.0';

        protected function empty_value($val){
            $val = parent::empty_value($val);
            
            if( empty($val) || $val == "" || $val == "0" ) return $this->force_default_value;
            else return $val;
        }
    }
    class IntergerType extends DefaultType {
        protected $force_default_value = 0;
        protected $wrap = "";
    }
    class DateType extends DefaultType {
        protected $wrap = "'";
        protected $regexp_validate = "'/^(19|20)([0-9]{2})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})/'";
    
        function __construct($type, $cols){
            parent::__construct($type, $cols);

            $this->force_default_value = date("Y-m-d H:i:s");
        }
    }
    class TinyintType extends DefaultType {
        protected $force_default_value = 0;
        protected $wrap = "";
        protected $middlewares = [
            "validate_tinyint"
        ];
        
        protected function validate_tinyint($val){
            if( $val == true || $val == 'true' || $val == 1 || $val == '1' ) return 1;
            else return 0;
        }
    }
    class VarcharType extends DefaultType {
        protected function validate($val){
            $val = parent::validate($val);

            preg_match('/^varchar\(([0-9]+)\)/', $this->type, $output);            
            if( !empty($output) ) {
                if( strlen($val) > (int)$output[1] ) {
                    return substr($val, 0, (int)$output[1]);
                }
            } 
            
            return $val;
        }
    }


    class QueryBuilder {
        protected $conn = null;
        public $tables = [];

        protected $all_types = [
            "double" => DoubleType::class,
            "float" => DoubleType::class,

            "int" => IntergerType::class,
            "date" => DateType::class,
            "tinyint" => TinyintType::class,
            "varchar" => VarcharType::class,

            // "varbinary" => DefaultType::class,
            // "decimal" => DefaultType::class,

            // "enum" => DefaultType::class,
            // "char" => DefaultType::class,
            // "timestamp" => DefaultType::class,
            // "set" => DefaultType::class,
            // "longblob" => DefaultType::class,
            // "datetime" => DefaultType::class,
            // "mediumtext" => DefaultType::class,
            // "bigint" => DefaultType::class,
            // "longtext" => DefaultType::class,
            // "smallint" => DefaultType::class,
            // "text" => DefaultType::class,
            // "blob" => DefaultType::class,
            // "time" => DefaultType::class,

        ];

        function __construct($conn){
            $this->conn = $conn;

            $query = $this->conn->query("SHOW TABLES");
            $types = [];

            while( $table = $query->fetch_array() ){                
                $tables_cols = $this->conn->query("SHOW COLUMNS FROM `{$table[0]}`");
                $sctructure = [];
                while( $cols = $tables_cols->fetch_object() ) {
                    preg_match('/^([a-z]+)/', $cols->Type, $output_array);
                    if( isset( $this->all_types[$output_array[1]] ) ) {
                        $proccessor = $this->all_types[ $output_array[1] ];                        
                        $sctructure[$cols->Field] = new $proccessor(
                            $output_array[1],
                            (array)$cols
                        );
                    }else {
                        $sctructure[$cols->Field] = new DefaultType(
                            $output_array[1],
                            (array)$cols
                        );
                    }
                }
                $this->tables[$table[0]] = $sctructure;                
            }

        }

        protected function prepare($table, $data){
            $processedData = array();
            foreach($data as $col => $val){
             
                $xColl = $this->tables[$table][$col];               
                $preccessed_value = $xColl->parse( $val );

                if( $preccessed_value == "'__IGNORE__'" ) continue; 
                $processedData[$col] = $preccessed_value; 
            }
            return $processedData;
        }

        public function insert($table, $data){            
            $saveData = $this->prepare($table, $data);       

            $query = ["INSERT", "INTO", "`$table`"];

            $cols = array_map(function($val){ return "`{$val}`";}, array_keys($saveData));
            $query[] = "( " . implode(", ",  $cols) . " )";

            $query[] = "value";
            $query[] = "( " . implode(", ", array_values($saveData) ) . " )";

            $query = implode(" ", $query );

            return $query;

            // echo $query ."\n";

        }

    }

    class FilesStore {
        protected $list = null;
        protected $database_file;

        function __construct( $database_file = null ){
            global $opt;
            
            echo "# Inicialize FileStore: \n";

            if( is_null( $database_file ) || empty($database_file) ){
                $database_file = "./{$opt['db']}-" . date("dmYHis") . ".zip";
            }
            echo "  > Connect file $database_file: ";
            $this->database_file = $database_file;

            $this->zip = new ZipArchive();

            if ( $this->zip->open( $database_file, ZipArchive::CREATE) !== TRUE ) {
                echo "[ERROR] \n";
                exit();
            }else {
                echo "[OK] \n";

            }

        }

        protected function zip_callback( $fx ){
            $zip = new ZipArchive();
            if ( $zip->open( $this->database_file, ZipArchive::CREATE) === TRUE ) {
                $fx($zip);
                $zip->close();
            }else {
                echo "# Error ZipArchive";
                exit();
            }
        }

        public function set( $namespace, $content ){
            $this->zip_callback( function($zip) use ($namespace, $content) {
                if(!is_string($content)) $content = json_encode($content);
                $zip->addFromString( $namespace, $content );
            });
        }

        public function get( $namespace ){
            if( is_string( $namespace ) )return $this->zip->getFromName($namespace);
            else return $this->zip->getNameIndex($namespace);
        }

        public function extract($files = []){
            if( is_array( $files ) ) $this->zip->extractTo(__DIR__, $files );
            else $this->zip->extractTo(__DIR__, [$files] );
        }

        public function get_list_files(){
            if( !is_null($this->list) ) return $this->list;
           
            $list = [];
            for ( $i=0; $i < $this->zip->numFiles; $i++ ) {
                preg_match(
                    '/(.*)\/(([0-9]+)-([0-9]+).json)/', 
                    $this->zip->getNameIndex($i), 
                    $output
                );
                if( empty($output) ) continue;
                if( !isset( $list[ $output[1] ] ) ) $list[ $output[1] ] = [];
                $list[ $output[1] ][] = $output[2];
            }
            $this->list = $list;

            return $this->list;
        }

        public function close() {
            // $this->zip->close();
        }

    }

    class MigrateEngine {

        private $tables = [];
        private $step = 10;

        function __construct ( $tables, $store, $db ) {
            $this->tables = $tables;
            $this->store = $store;
            $this->db = $db;
        }

        public function migrate( $table = null ){
            if( !is_null($table) ){
                if($this->tables[$table]["status"]) return;

                echo "  > {$table}: ";
                $max = round($this->tables[$table]["registers"] / $this->step) + 1;
                for ($i=0; $i < $max; $i++) { 
                    echo ".";
                    $init = $i*$this->step;
                    $query_json = $this->db->get($table, $init, $this->step );

                    $this->store->set( "{$table}/{$init}-{$this->step}.json", $query_json );
                }

                $this->tables[$table]["status"] = true;
                echo "\n";

            }else {
                foreach( $this->tables as $tab => $sets ){
                    if( $sets["status"] ) continue;

                    if( !empty( $sets["before"] ) ) {
                        foreach( $sets["before"] as $dep ){
                            if( $this->tables[$dep]["status"] ) continue;
                            $this->migrate( $dep ); 
                        }
                    }
                    $this->migrate( $tab ); 
                }
            }
        }


        protected function import_table( $table ){

            if($this->tables[$table]["status"]) return;
            
            echo "  > {$table}:";
            
            foreach( $this->import_files[$table] as $file ){
                $contError = 0;
                $content = json_decode( $this->store->get("{$table}/{$file}"), true );
                if( !is_null($content) ){
                    foreach($content as $data){
                        if( !$this->db->save($table, $data) ) $contError++;
                    }
                }

                if( $contError == 0 ) echo ".";
                else echo "x";
            }

            $this->tables[$table]["status"] = true;
            echo "\n";
            
        }

        protected function loop_hierach( $tables ) {
            foreach ($tables as $table){                
                if( !empty($this->tables[$table]["before"]) ) {
                    $this->loop_hierach( $this->tables[$table]["before"] );
                }
                $this->import_table( $table );                
            }
        }

        private $import_files = null;
        public function import(){
            // print_r(array_keys($this->tables));
            $this->import_files = $this->store->get_list_files();

            $this->loop_hierach( array_keys($this->tables) );

        }
    }

    class MysqlUtlis {
        public $conn = null;
        public $dConn = null;

        function __construct($host, $user, $pass, $db){
            $this->dConn = [$host, $user, $pass, $db];
            
        }

        public function connect(){

            echo "# Connecting db: \n";
            echo "  > Host: {$this->dConn[0]} \n";
            echo "  > User: {$this->dConn[1]} \n";
            echo "  > Pass: {$this->dConn[2]} \n";
            echo "  > Database: {$this->dConn[3]} \n";

            $this->conn = new mysqli(
                $this->dConn[0],
                $this->dConn[1],
                $this->dConn[2],
                $this->dConn[3]
            );

            if( $this->conn->connect_errno ) {
                echo "  > Status: [ERROR] \n";
                exit ();
            } else {
                echo "  > Status: [OK] \n";

                $this->builder = new QueryBuilder($this->conn);
                echo "  > Builder: [OK] \n";
            }
        }

        

        public function query($sql){
        
            $result = $this->conn->query($sql);
            $data = [];
            while ( $line = $result->fetch_assoc() ){
                $data[] = $line;
            }
    
            return $data; 
        }

        public function get_fk( $table ){
            $data = $this->query("SELECT TABLE_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_NAME = '{$table}'");
            $table_deps = [];
            foreach($data as $deps){
                $table_deps[] = $deps["TABLE_NAME"];
            }            
            return $table_deps;
        }

        public function get_tables(){
            $query = $this->query("SHOW TABLES");
            $tables = [];
            $countRegisters = 0;
            foreach($query as $tab){
                $tab_name = array_values($tab)[0];
                $registers = $this->query("SELECT count(*) as registers FROM {$tab_name}");
                $countRegisters += $registers[0]["registers"];
                $tables[$tab_name] = [
                    "before" => [],
                    "registers" => $registers[0]["registers"],
                    "dependencies" => $this->get_fk( $tab_name ),
                    "status" => false
                ];
            } 

            foreach($tables as $key => &$items){
                if( !isset($items["dependencies"])) continue;
                foreach($items["dependencies"] as $deps){
                    if( !isset($tables[$deps]) ) continue;
                    $before = &$tables[$deps]["before"];
                    
                    if( in_array( $key, $before ) ) continue;
                    $before[] = $key;
                }
                unset($items["dependencies"]);
            }

            echo "  > Tables: " . count($tables) . "\n";
            echo "  > Registers: {$countRegisters} \n";

            return $tables;
        }

        public function remove_json( $data ){
            if( is_string($data) ){
                $parsed_data = json_decode($data, true);
                if( !is_null($parsed_data) ) {
                    return $this->remove_json($parsed_data);
                } else {
                    return $data;
                }

            } else if( is_array($data) ) {
                return array_map( function($line){
                    if( is_array($line) || is_string($line) ){
                        return  $this->remove_json($line);
                    } else {
                        return $line;
                    }
                }, $data);

            } else {
                return $data;
            }
        }

        public function get( $table, $init, $length){
            $data = $this->query("SELECT * FROM `$table` LIMIT {$init}, {$length} ");
            $data = $this->remove_json($data);
            return $data;
        }

        public function close() {
            $this->conn->close();
        }

        private function save_structure(){
            // $auth = [
            //     "mysql_config_editor set", 
            //     "--login-path=local",
            //     "--host={$this->dConn[0]}",
            //     "--user={$this->dConn[1]}",
            //     "--password={$this->dConn[1]}"
            // ];

            // shell_exec( implode(" ", $auth) );
            
            $script = [
                "mysqldump",
                // "-h {$this->dConn[0]}",
                // "-u {$this->dConn[1]}",
                // !empty($this->dConn[1]) ? "-p {$this->dConn[1]}" : "",
                "--no-data",
                "{$this->dConn[3]}"
            ];

            return shell_exec( implode(" ", $script) );
        }

        public function init_export( $store ){
        
            echo "  > Structure clone:";
            $save_structure = $this->save_structure();
            if( $save_structure == "null" ) throw new Exception("Erro ao exportar");
            // echo "$save_structure";
            $store->set( "structure.sql", $save_structure );
            echo " [OK]\n";
            // exit();
            $this->connect();
        }

        public function init_import( $store ){
            echo "# Init Import: \n";
            
            $store->extract("structure.sql");
            echo " > Extract Structure: [OK] \n";

            $conn = [
                "mysql",
                "-h {$this->dConn[0]}",
                "-u {$this->dConn[1]}",
                // !empty($this->dConn[1]) ? "-p {$this->dConn[1]}" : "",               
            ]; 

            $shell_script = function($args) use ($conn){
                $vars = array_merge( $conn, $args);
                return  shell_exec(implode(" ", $vars)) ;
            };

            $shell_script([
                "-e \"DROP DATABASE IF EXISTS {$this->dConn[3]}\" "
            ]);
            echo " > Delete old database: [OK] \n";
            
            $shell_script([
                "-e \"CREATE DATABASE {$this->dConn[3]}\" "
            ]);
            echo " > Created database '{$this->dConn[3]}': [OK] \n";
            
            $shell_script([
                $this->dConn[3],
                "< ./structure.sql"
            ]);
            echo "  > Imported structure: [OK] \n";

            unlink("./structure.sql");
            echo " > Delete Structure file: [OK] \n";

            $this->connect();

        }

        public function exec_unsession( $fn ){
            $conn = new mysqli(
                $this->dConn[0],
                $this->dConn[1],
                $this->dConn[2],
                $this->dConn[3]
            );

            if( !$this->conn->connect_errno ){
                $status = $fn($conn);
                $conn->close();
                return $status;
            }
            return false;
        }

        public function save( $table, $data ){
            $query = $this->builder->insert($table, $data);

            $status = $this->exec_unsession( function($conn) use ($query) {
                $status = $conn->query($query);
                if( !$status ) {
                    echo "      -> {$conn->error} \n";
                    // echo "      # {$query} \n";
                    // exit();
                } 
                return $status;
            });

            return $status;
        }

    }

    class PHPMigrate {
        protected $store_con = null;
        protected $files_store = null;

        function __construct ($store_con, $files_store){
            
            $this->store_con = $store_con;

            $this->files_store = $files_store;
        }

        public function close(){
            echo "# Close file conection: \n";
            $this->files_store->close();

            echo "# Close database conection: \n";
            $this->store_con->close();
        }
        

        public function export(){
                        
            $this->store_con->init_export( $this->files_store );  

            echo "# Database Mapping: \n";

            $tables = $this->store_con->get_tables();
            echo "  > Mapping [OK] \n";

            $MigrateEngine = new MigrateEngine(
                $tables,
                $this->files_store,
                $this->store_con
            );
            echo "# Initialize migration \n";

            $MigrateEngine->migrate();

            $this->close();

        }

        public function import(){

            $this->store_con->init_import( $this->files_store );

            echo "# Database Mapping: \n";

            $tables = $this->store_con->get_tables();
            echo "  > Mapping [OK] \n";

            $MigrateEngine = new MigrateEngine(
                $tables,
                $this->files_store,
                $this->store_con
            );
            echo "# Initialize migration \n";

            $MigrateEngine->import();

            $this->close();
        }


    }

    $migrate = new PHPMigrate( 
        $conn = new MysqlUtlis(
            "localhost",
            $opt["user"], 
            $opt["pass"], 
            $opt["db"]
        ),
        
        new FilesStore( isset($opt["file"]) ? $opt["file"] : null  )
    );

    if( !isset($opt["file"]) ) $migrate->export();
    else $migrate->import();

    // $json = json_decode('[{"id":1,"type":"doctor","customer_id":null,"mail":"wendell.vieiracunha@gmail.com","cpf":"136.659.317-03","document":123456789,"avatar":"cIRu2kKE-t6Vs-jQBH-JNbu-tQUppeSLSfKG.svg","name":"Wendell Vieira Cunha","tels":"(24) 98816-3728","authHash":"6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090","birthday":"1992-02-29","area":{"id":"CRM","specializations":[19],"documents":{"number":123456789,"front":"OhHdLT7M-aMFv-iQfS-yLtH-jsCnjwF3r5nr.jpg","back":"zEln24Vn-qent-LxiA-YrLU-uAhjtgIyDGiw.jpg"}},"agreements":{"notAcept":true,"allCovenants":false,"covenants":[]},"billing":null,"use_billing":1,"shipping":null,"status":1,"medical_record":null,"created_at":"2020-06-09 18:57:36","updated_at":"2020-06-09 20:06:49"},{"id":2,"type":"patient","customer_id":"CUS-JG0KSKD51ZVO","mail":"jessialimas@gmail.com","cpf":"136.880.837-92","document":null,"avatar":"cIRu2kKE-t6Vs-jQBH-JNbu-tQUppeSLSfKG.svg","name":"jessica alves lima","tels":"(24) 98846-6544","authHash":"6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090","birthday":"1991-05-10","area":null,"agreements":null,"billing":null,"use_billing":1,"shipping":{"cep":"27210-500","num":654,"logradouro":"Rua Porto Alegre","bairro":"Santo Agostinho","localidade":"Volta Redonda","uf":"RJ"},"status":1,"medical_record":"dfgdfgdfg<div>dfg<\/div><div>df<\/div><div>dfg<\/div>","created_at":"2020-06-09 20:08:35","updated_at":"2020-06-14 22:02:37"}]', true);
    // $conn = new MysqlUtlis(
    //     "localhost",
    //     $opt["user"], 
    //     $opt["pass"], 
    //     $opt["db"]
    // );

  

    // $conn->connect();

    // // print_r($json);