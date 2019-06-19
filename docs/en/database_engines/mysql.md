#MySQL

Manages data on the remote MySQL server.

The engine connects to the remote MySQL database, creates the table structure and proxy queries to tables.


## Data Types Matching

DataTypePtr convertMySQLDataType(const String & mysql_data_type, bool is_nullable, bool is_unsigned, size_t length)
{
    DataTypePtr res;
    if (mysql_data_type == "tinyint")
    {
        if (is_unsigned)
            res = std::make_shared<DataTypeUInt8>();
        else
            res = std::make_shared<DataTypeInt8>();
    }
    else if (mysql_data_type == "smallint")
    {
        if (is_unsigned)
            res = std::make_shared<DataTypeUInt16>();
        else
            res = std::make_shared<DataTypeInt16>();
    }
    else if (mysql_data_type == "int" || mysql_data_type == "mediumint")
    {
        if (is_unsigned)
            res = std::make_shared<DataTypeUInt32>();
        else
            res = std::make_shared<DataTypeInt32>();
    }
    else if (mysql_data_type == "bigint")
    {
        if (is_unsigned)
            res = std::make_shared<DataTypeUInt64>();
        else
            res = std::make_shared<DataTypeInt64>();
    }
    else if (mysql_data_type == "float")
        res = std::make_shared<DataTypeFloat32>();
    else if (mysql_data_type == "double")
        res = std::make_shared<DataTypeFloat64>();
    else if (mysql_data_type == "date")
        res = std::make_shared<DataTypeDate>();
    else if (mysql_data_type == "datetime" || mysql_data_type == "timestamp")
        res = std::make_shared<DataTypeDateTime>();
    else if (mysql_data_type == "binary")
        res = std::make_shared<DataTypeFixedString>(length);
    else
        /// Also String is fallback for all unknown types.
        res = std::make_shared<DataTypeString>();
    if (is_nullable)
        res = std::make_shared<DataTypeNullable>(res);
    return res;
}



## Usage

throw Exception("Database engine " + engine_name + " cannot have parameters, primary_key, order_by, sample_by, settings",

throw Exception("MySQL Database require mysql_hostname, mysql_database_name, mysql_username, mysql_password arguments.",



StoragePtr detachTable(const String &) override
    {
        throw Exception("MySQL database engine does not support detach table.", ErrorCodes::NOT_IMPLEMENTED);
    }

    void loadTables(Context &, ThreadPool *, bool) override
    {
        /// do nothing
    }

    void removeTable(const Context &, const String &) override
    {
        throw Exception("MySQL database engine does not support remove table.", ErrorCodes::NOT_IMPLEMENTED);
    }

    void attachTable(const String &, const StoragePtr &) override
    {
        throw Exception("MySQL database engine does not support attach table.", ErrorCodes::NOT_IMPLEMENTED);
    }

    void renameTable(const Context &, const String &, IDatabase &, const String &) override
    {
        throw Exception("MySQL database engine does not support rename table.", ErrorCodes::NOT_IMPLEMENTED);
    }

    void createTable(const Context &, const String &, const StoragePtr &, const ASTPtr &) override
    {
        throw Exception("MySQL database engine does not support create table.", ErrorCodes::NOT_IMPLEMENTED);
    }

    void alterTable(const Context &, const String &, const ColumnsDescription &, const IndicesDescription &, const ASTModifier &) override
    {
        throw Exception("MySQL database engine does not support alter table.", ErrorCodes::NOT_IMPLEMENTED);
    }




    node1.query("CREATE DATABASE clickhouse_mysql ENGINE = MySQL('mysql1:3306', 'clickhouse', 'root', 'clickhouse')")
           yield cluster

       finally:
           cluster.shutdown()


   def test_sync_tables_list_between_clickhouse_and_mysql(started_cluster):
       mysql_connection = get_mysql_conn()
       assert node1.query('SHOW TABLES FROM clickhouse_mysql FORMAT TSV').rstrip() == ''

       create_mysql_table(mysql_connection, 'first_mysql_table')
       assert node1.query("SHOW TABLES FROM clickhouse_mysql LIKE 'first_mysql_table' FORMAT TSV").rstrip() == 'first_mysql_table'

       create_mysql_table(mysql_connection, 'second_mysql_table')
       assert node1.query("SHOW TABLES FROM clickhouse_mysql LIKE 'second_mysql_table' FORMAT TSV").rstrip() == 'second_mysql_table'

       drop_mysql_table(mysql_connection, 'second_mysql_table')
       assert node1.query("SHOW TABLES FROM clickhouse_mysql LIKE 'second_mysql_table' FORMAT TSV").rstrip() == ''

       mysql_connection.close()

   def test_sync_tables_structure_between_clickhouse_and_mysql(started_cluster):
       mysql_connection = get_mysql_conn()

       create_mysql_table(mysql_connection, 'test_sync_column')

       assert node1.query(
           "SELECT name FROM system.columns WHERE table = 'test_sync_column' AND database = 'clickhouse_mysql' AND name = 'pid' ").rstrip() == ''

       time.sleep(3)
       add_mysql_table_column(mysql_connection, "test_sync_column")

       assert node1.query(
           "SELECT name FROM system.columns WHERE table = 'test_sync_column' AND database = 'clickhouse_mysql' AND name = 'pid' ").rstrip() == 'pid'

       time.sleep(3)
       drop_mysql_table_column(mysql_connection, "test_sync_column")
       assert node1.query(
           "SELECT name FROM system.columns WHERE table = 'test_sync_column' AND database = 'clickhouse_mysql' AND name = 'pid' ").rstrip() == ''

       mysql_connection.close()

   def test_insert_select(started_cluster):
       mysql_connection = get_mysql_conn()
       create_mysql_table(mysql_connection, 'test_insert_select')

       assert node1.query("SELECT count() FROM `clickhouse_mysql`.{}".format('test_insert_select')).rstrip() == '0'
       node1.query("INSERT INTO `clickhouse_mysql`.{}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000) ".format('test_insert_select'))
       assert node1.query("SELECT count() FROM `clickhouse_mysql`.{}".format('test_insert_select')).rstrip() == '10000'
       assert node1.query("SELECT sum(money) FROM `clickhouse_mysql`.{}".format('test_insert_select')).rstrip() == '30000'
       mysql_connection.close()

   def get_mysql_conn():
       conn = pymysql.connect(user='root', password='clickhouse', host='127.0.0.1', port=3308)
       return conn

   def create_mysql_db(conn, name):
       with conn.cursor() as cursor:
           cursor.execute(
               "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(name))
