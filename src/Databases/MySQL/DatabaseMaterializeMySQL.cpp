//#include <Databases/MySQL/DatabaseMaterializeMySQL.h>
//
//#include <Common/quoteString.h>
//#include <Interpreters/Context.h>
//#include <DataTypes/DataTypeString.h>
//#include <DataTypes/DataTypesNumber.h>
//#include <Formats/MySQLBlockInputStream.h>
//#include <Databases/MySQL/MasterStatusInfo.h>
//#include <IO/Operators.h>
//
//namespace DB
//{
//
//static MasterStatusInfo fetchMasterStatus(const mysqlxx::PoolWithFailover::Entry & connection)
//{
//    Block header
//    {
//        {std::make_shared<DataTypeString>(), "File"},
//        {std::make_shared<DataTypeUInt64>(), "Position"},
//        {std::make_shared<DataTypeString>(), "Binlog_Do_DB"},
//        {std::make_shared<DataTypeString>(), "Binlog_Ignore_DB"},
//        {std::make_shared<DataTypeString>(), "Executed_Gtid_Set"},
//    };
//
//    MySQLBlockInputStream input(connection, "SHOW MASTER STATUS;", header, DEFAULT_BLOCK_SIZE);
//    Block master_status = input.read();
//
//    if (!master_status || master_status.rows() != 1)
//        throw Exception("Unable to get master status from MySQL.", ErrorCodes::LOGICAL_ERROR);
//
//    return MasterStatusInfo
//    {
//        (*master_status.getByPosition(0).column)[0].safeGet<String>(),
//        (*master_status.getByPosition(1).column)[0].safeGet<UInt64>(),
//        (*master_status.getByPosition(2).column)[0].safeGet<String>(),
//        (*master_status.getByPosition(3).column)[0].safeGet<String>(),
//        (*master_status.getByPosition(4).column)[0].safeGet<String>()
//    };
//}
//
//static std::vector<String> fetchTablesInDB(const mysqlxx::PoolWithFailover::Entry & connection, const std::string & database)
//{
//    Block header{{std::make_shared<DataTypeString>(), "table_name"}};
//    String query = "SELECT TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.TABLES  WHERE TABLE_SCHEMA = " + quoteString(database);
//
//    std::vector<String> tables_in_db;
//    MySQLBlockInputStream input(connection, query, header, DEFAULT_BLOCK_SIZE);
//
//    while (Block block = input.read())
//    {
//        tables_in_db.reserve(tables_in_db.size() + block.rows());
//        for (size_t index = 0; index < block.rows(); ++index)
//            tables_in_db.emplace_back((*block.getByPosition(0).column)[index].safeGet<String>());
//    }
//
//    return tables_in_db;
//}
//
//DatabaseMaterializeMySQL::DatabaseMaterializeMySQL(
//    const Context & context, const String & database_name_, const String & metadata_path_,
//    const ASTStorage * database_engine_define_, const String & mysql_database_name_, mysqlxx::Pool && pool_)
//    : IDatabase(database_name_)
//    , global_context(context.getGlobalContext()), metadata_path(metadata_path_)
//    , database_engine_define(database_engine_define_->clone()), mysql_database_name(mysql_database_name_), pool(std::move(pool_))
//{
//    try
//    {
//        mysqlxx::PoolWithFailover::Entry connection = pool.get();
//
//        connection->query("FLUSH TABLES;").execute();
//        connection->query("FLUSH TABLES WITH READ LOCK;").execute();
//
//        MasterStatusInfo master_status = fetchMasterStatus(connection);
//        connection->query("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;").execute();
//        connection->query("START TRANSACTION /*!40100 WITH CONSISTENT SNAPSHOT */;").execute();
//
//        std::vector<String> tables_in_db = fetchTablesInDB(connection, mysql_database_name);
//        connection->query("UNLOCK TABLES;");
//
//        for (const auto & dumping_table_name : tables_in_db)
//        {
//            /// TODO: 查询表结构, 根据不同的模式创建对应的表(暂时只支持多version即可)
////            connection->query("SHOW CREATE TABLE " + doubleQuoteString())
//            MySQLBlockInputStream input(
//                "SELECT * FROM " + backQuoteIfNeed(mysql_database_name) + "." + backQuoteIfNeed(dumping_table_name));
//            /// TODO: 查询所有数据写入对应表中(全量dump)
//            /// TODO: 启动slave, 监听事件
//        }
//    }
//    catch (...)
//    {
//        throw;
//    }
//}
//
//
//}
