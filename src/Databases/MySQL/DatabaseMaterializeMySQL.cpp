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
#    include <Poco/File.h>
#    include <Common/quoteString.h>
#    include <Common/setThreadName.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
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

String DatabaseMaterializeMySQL::getCreateQuery(const mysqlxx::Pool::Entry & connection, const String & database, const String & table_name)
{
    Block show_create_table_header{
        {std::make_shared<DataTypeString>(), "Table"},
        {std::make_shared<DataTypeString>(), "Create Table"},
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

    if (!ast || !ast->as<MySQLParser::ASTCreateQuery>())
        throw Exception("LOGICAL ERROR: ast cannot cast to MySQLParser::ASTCreateQuery.", ErrorCodes::LOGICAL_ERROR);

    WriteBufferFromOwnString out;
    ast->as<MySQLParser::ASTCreateQuery>()->database = database;
    MySQLVisitor::CreateQueryConvertVisitor::Data data{.out = out, .context = global_context};
    MySQLVisitor::CreateQueryConvertVisitor visitor(data);
    visitor.visit(ast);
    return out.str();
}

void DatabaseMaterializeMySQL::dumpMySQLDatabase(const std::function<bool()> & is_cancelled)
{
    mysqlxx::PoolWithFailover::Entry connection = pool.get();

    MasterStatusInfo info(connection, mysql_database_name);

    for (const auto & dumping_table_name : info.need_dumping_tables)
    {
        if (is_cancelled())
            return;

        const auto & table_name = backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(dumping_table_name);
        String query_prefix = "/* Dumping " + backQuoteIfNeed(mysql_database_name) + "." + backQuoteIfNeed(dumping_table_name) + " for "
            + backQuoteIfNeed(database_name) + "  Database */ ";

        tryToExecuteQuery(query_prefix + " DROP TABLE IF EXISTS " + table_name);
        tryToExecuteQuery(query_prefix + getCreateQuery(connection, database_name, dumping_table_name));

        Context context = global_context;
        context.setCurrentQueryId(""); // generate random query_id
        context.getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
        BlockIO streams = executeQuery(query_prefix + " INSERT INTO " + table_name + " VALUES", context, true);

        if (!streams.out)
            throw Exception("LOGICAL ERROR out stream is undefined.", ErrorCodes::LOGICAL_ERROR);

        MySQLBlockInputStream input(
            connection, "SELECT * FROM " + backQuoteIfNeed(mysql_database_name) + "." + backQuoteIfNeed(dumping_table_name),
            streams.out->getHeader(), DEFAULT_BLOCK_SIZE);

        copyData(input, *streams.out, is_cancelled);
        /// TODO: 启动slave, 监听事件
    }
}
void DatabaseMaterializeMySQL::synchronization()
{
    setThreadName("MySQLDBSync");

    try
    {
        std::unique_lock<std::mutex> lock{sync_mutex};

        LOG_DEBUG(log, "Checking " + database_name + " database status.");
        while (!sync_quit && !DatabaseCatalog::instance().isDatabaseExist(database_name))
            sync_cond.wait_for(lock, std::chrono::seconds(1));

        LOG_DEBUG(log, database_name + " database status is OK.");

        Poco::File dumped_flag(getMetadataPath() + "/dumped.flag");

        if (!dumped_flag.exists())
        {
            dumpMySQLDatabase([&]() { return sync_quit.load(std::memory_order_seq_cst); });
            dumped_flag.createFile();
        }
    }
    catch(...)
    {
        tryLogCurrentException(log);
    }
}

}

#endif
