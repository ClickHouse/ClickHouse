#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#include <Databases/MySQL/DatabaseMaterializeMySQL.h>

#    include <DataStreams/copyData.h>
#    include <DataTypes/DataTypeString.h>
#    include <DataTypes/DataTypesNumber.h>
#    include <Databases/MySQL/MasterStatusInfo.h>
#    include <Formats/MySQLBlockInputStream.h>
#    include <IO/Operators.h>
#    include <IO/ReadBufferFromString.h>
#    include <Interpreters/Context.h>
#    include <Interpreters/MySQL/CreateQueryConvertVisitor.h>
#    include <Interpreters/executeQuery.h>
#    include <Parsers/MySQL/ASTCreateQuery.h>
#    include <Parsers/parseQuery.h>
#    include <Common/quoteString.h>
#    include <Common/setThreadName.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

static MasterStatusInfo fetchMasterStatus(const mysqlxx::PoolWithFailover::Entry & connection)
{
    Block header
    {
        {std::make_shared<DataTypeString>(), "File"},
        {std::make_shared<DataTypeUInt64>(), "Position"},
        {std::make_shared<DataTypeString>(), "Binlog_Do_DB"},
        {std::make_shared<DataTypeString>(), "Binlog_Ignore_DB"},
        {std::make_shared<DataTypeString>(), "Executed_Gtid_Set"},
    };

    MySQLBlockInputStream input(connection, "SHOW MASTER STATUS;", header, DEFAULT_BLOCK_SIZE);
    Block master_status = input.read();

    if (!master_status || master_status.rows() != 1)
        throw Exception("Unable to get master status from MySQL.", ErrorCodes::LOGICAL_ERROR);

    return MasterStatusInfo
    {
        (*master_status.getByPosition(0).column)[0].safeGet<String>(),
        (*master_status.getByPosition(1).column)[0].safeGet<UInt64>(),
        (*master_status.getByPosition(2).column)[0].safeGet<String>(),
        (*master_status.getByPosition(3).column)[0].safeGet<String>(),
        (*master_status.getByPosition(4).column)[0].safeGet<String>()
    };
}

static std::vector<String> fetchTablesInDB(const mysqlxx::PoolWithFailover::Entry & connection, const std::string & database)
{
    Block header{{std::make_shared<DataTypeString>(), "table_name"}};
    String query = "SELECT TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.TABLES  WHERE TABLE_SCHEMA = " + quoteString(database);

    std::vector<String> tables_in_db;
    MySQLBlockInputStream input(connection, query, header, DEFAULT_BLOCK_SIZE);

    while (Block block = input.read())
    {
        tables_in_db.reserve(tables_in_db.size() + block.rows());
        for (size_t index = 0; index < block.rows(); ++index)
            tables_in_db.emplace_back((*block.getByPosition(0).column)[index].safeGet<String>());
    }

    return tables_in_db;
}

DatabaseMaterializeMySQL::DatabaseMaterializeMySQL(
    const Context & context, const String & database_name_, const String & metadata_path_,
    const ASTStorage * database_engine_define_, const String & mysql_database_name_, mysqlxx::Pool && pool_)
    : DatabaseOrdinary(database_name_, metadata_path_, context)
    /*, global_context(context.getGlobalContext()), metadata_path(metadata_path_)*/
    , database_engine_define(database_engine_define_->clone()), mysql_database_name(mysql_database_name_), pool(std::move(pool_))
{
    /// TODO: 做简单的check, 失败即报错
}

String DatabaseMaterializeMySQL::getCreateQuery(const mysqlxx::Pool::Entry & connection, const String & database, const String & table_name)
{
    Block show_create_table_header{
        {std::make_shared<DataTypeString>(), "Table"},
        {std::make_shared<DataTypeUInt64>(), "Create Table"},
    };

    MySQLBlockInputStream show_create_table(
        connection, "SHOW CREATE TABLE " + backQuoteIfNeed(mysql_database_name) + "." + backQuoteIfNeed(table_name),
        show_create_table_header, DEFAULT_BLOCK_SIZE);

    Block create_query_block = show_create_table.read();
    if (!create_query_block || create_query_block.rows() != 1)
        throw Exception("LOGICAL ERROR mysql show create return more rows.", ErrorCodes::LOGICAL_ERROR);

    const auto & create_query = create_query_block.getByName("Create Table").column->getDataAt(0);

    MySQLParser::ParserCreateQuery p_create_query;
    ASTPtr ast = parseQuery(p_create_query, create_query.data, create_query.data + create_query.size, "", 0, 0);

    WriteBufferFromOwnString out;
    MySQLVisitor::CreateQueryConvertVisitor::Data data{.out = out};
    MySQLVisitor::CreateQueryConvertVisitor visitor(data);
    visitor.visit(ast);
    return out.str();
}

void DatabaseMaterializeMySQL::tryToExecuteQuery(const String & query_to_execute)
{
    ReadBufferFromString istr(query_to_execute);
    String dummy_string;
    WriteBufferFromString ostr(dummy_string);

    try
    {
        Context context = global_context;
        context.getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
        context.setCurrentQueryId(""); // generate random query_id
        executeQuery(istr, ostr, false, context, {});
    }
    catch (...)
    {
        tryLogCurrentException(log, "Query " + query_to_execute + " wasn't finished successfully");
        throw;
    }

    LOG_DEBUG(log, "Executed query: " << query_to_execute);
}
void DatabaseMaterializeMySQL::dumpMySQLDatabase()
{
    mysqlxx::PoolWithFailover::Entry connection = pool.get();

    connection->query("FLUSH TABLES;").execute();
    connection->query("FLUSH TABLES WITH READ LOCK;").execute();

    MasterStatusInfo master_status = fetchMasterStatus(connection);
    connection->query("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;").execute();
    connection->query("START TRANSACTION /*!40100 WITH CONSISTENT SNAPSHOT */;").execute();

    std::vector<String> tables_in_db = fetchTablesInDB(connection, mysql_database_name);
    connection->query("UNLOCK TABLES;").execute();

    for (const auto & dumping_table_name : tables_in_db)
    {
        String query_prefix = "/* Dumping " + backQuoteIfNeed(mysql_database_name) + "." + backQuoteIfNeed(dumping_table_name) + " for "
            + backQuoteIfNeed(database_name) + "  Database */ ";

        tryToExecuteQuery(query_prefix + " DROP TABLE IF EXISTS " + backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(dumping_table_name));
        tryToExecuteQuery(query_prefix + getCreateQuery(connection, mysql_database_name, dumping_table_name));

        Context context = global_context;
        context.getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
        context.setCurrentQueryId(""); // generate random query_id
        BlockIO streams = executeQuery(query_prefix + " INSERT INTO " + backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(dumping_table_name), context, true);

        if (!streams.out)
            throw Exception("LOGICAL ERROR out stream is undefined.", ErrorCodes::LOGICAL_ERROR);

        MySQLBlockInputStream input(
            connection, "SELECT * FROM " + backQuoteIfNeed(mysql_database_name) + "." + backQuoteIfNeed(dumping_table_name),
            streams.out->getHeader(), DEFAULT_BLOCK_SIZE);

        copyData(input, *streams.out /*, is_quit*/);
        /// TODO: 启动slave, 监听事件
    }
}
void DatabaseMaterializeMySQL::synchronization()
{
    setThreadName("MySQLDBSync");

    try
    {
        std::unique_lock<std::mutex> lock{sync_mutex};

        /// Check database is exists in ClickHouse.
        while (!sync_quit && !DatabaseCatalog::instance().isDatabaseExist(database_name))
            sync_cond.wait_for(lock, std::chrono::seconds(1));

        /// 查找一下位点文件, 如果不存在需要清理目前的数据库, 然后dump全量数据.
        dumpMySQLDatabase();
    }
    catch(...)
    {
        tryLogCurrentException(log);
    }
}

}

#endif
