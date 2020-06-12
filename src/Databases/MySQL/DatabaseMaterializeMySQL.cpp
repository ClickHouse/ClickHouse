#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#include <Databases/MySQL/DatabaseMaterializeMySQL.h>

#    include <cstdlib>
#    include <Columns/ColumnTuple.h>
#    include <DataStreams/AddingVersionsBlockOutputStream.h>
#    include <DataStreams/copyData.h>
#    include <Databases/MySQL/queryConvert.h>
#    include <Databases/MySQL/EventConsumer.h>
#    include <Databases/MySQL/MaterializeMetadata.h>
#    include <Formats/MySQLBlockInputStream.h>
#    include <IO/ReadBufferFromString.h>
#    include <Interpreters/Context.h>
#    include <Interpreters/MySQL/CreateQueryVisitor.h>
#    include <Interpreters/executeQuery.h>
#    include <Parsers/parseQuery.h>
#    include <Common/quoteString.h>
#    include <Common/setThreadName.h>
#    include <common/sleep.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

static inline BlockIO tryToExecuteQuery(const String & query_to_execute, const Context & context_, const String & comment)
{
    try
    {
        Context context = context_;
        context.getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
        context.setCurrentQueryId(""); // generate random query_id
        return executeQuery("/*" + comment + "*/ " + query_to_execute, context, true);
    }
    catch (...)
    {
        tryLogCurrentException("DatabaseMaterializeMySQL", "Query " + query_to_execute + " wasn't finished successfully");
        throw;
    }

    LOG_DEBUG(&Logger::get("DatabaseMaterializeMySQL"), "Executed query: " << query_to_execute);
}

DatabaseMaterializeMySQL::DatabaseMaterializeMySQL(
    const Context & context, const String & database_name_, const String & metadata_path_
    , const ASTStorage * database_engine_define_, const String & mysql_database_name_, mysqlxx::Pool && pool_
    , MySQLClient && client_ , std::unique_ptr<MaterializeModeSettings> settings_)
    : DatabaseMaterializeMySQLWrap(std::make_shared<DatabaseOrdinary>(database_name_, metadata_path_, context), database_engine_define_->clone(), "DatabaseMaterializeMySQL")
    , global_context(context.getGlobalContext()), metadata_path(metadata_path_), mysql_database_name(mysql_database_name_)
    , pool(std::move(pool_)), client(std::move(client_)), settings(std::move(settings_))
{
    /// TODO: 做简单的check, 失败即报错
    scheduleSynchronized();
}

BlockOutputStreamPtr DatabaseMaterializeMySQL::getTableOutput(const String & table_name)
{
    String with_database_table_name = backQuoteIfNeed(getDatabaseName()) + "." + backQuoteIfNeed(table_name);
    BlockIO res = tryToExecuteQuery("INSERT INTO " + with_database_table_name + " VALUES", global_context, "");

    if (!res.out)
        throw Exception("LOGICAL ERROR:", ErrorCodes::LOGICAL_ERROR);

    return res.out;
}

void DatabaseMaterializeMySQL::cleanOutdatedTables()
{
    auto ddl_guard = DatabaseCatalog::instance().getDDLGuard(database_name, "");
    const DatabasePtr & clean_database = DatabaseCatalog::instance().getDatabase(database_name);

    for (auto iterator = clean_database->getTablesIterator(); iterator->isValid(); iterator->next())
    {
        String table = backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(iterator->name());
        String comment = String("Clean ") + table + " for dump mysql.";
        tryToExecuteQuery("DROP TABLE " + table, global_context, comment);
    }
}

void DatabaseMaterializeMySQL::dumpDataForTables(mysqlxx::Pool::Entry & connection, MaterializeMetadata & master_info, const std::function<bool()> & is_cancelled)
{
    auto iterator = master_info.need_dumping_tables.begin();
    for (; iterator != master_info.need_dumping_tables.end() && !is_cancelled(); ++iterator)
    {
        const auto & table_name = iterator->first;
        MySQLTableStruct table_struct = visitCreateQuery(iterator->second, global_context, database_name);
        String comment = String("Dumping ") + backQuoteIfNeed(mysql_database_name) + "." + backQuoteIfNeed(table_name);
        tryToExecuteQuery(toCreateQuery(table_struct, global_context), global_context, comment);

        BlockOutputStreamPtr out = std::make_shared<AddingVersionsBlockOutputStream>(master_info.version, getTableOutput(table_name));
        MySQLBlockInputStream input(connection, "SELECT * FROM " + backQuoteIfNeed(mysql_database_name) + "." + backQuoteIfNeed(table_name), out->getHeader(), DEFAULT_BLOCK_SIZE);
        copyData(input, *out, is_cancelled);
    }
}

std::optional<MaterializeMetadata> DatabaseMaterializeMySQL::prepareSynchronized(std::unique_lock<std::mutex> & lock, const std::function<bool()> & is_cancelled)
{
    while (!is_cancelled())
    {
        try
        {
            LOG_DEBUG(log, "Checking " + database_name + " database status.");
            while (!is_cancelled && !DatabaseCatalog::instance().isDatabaseExist(database_name))
                sync_cond.wait_for(lock, std::chrono::seconds(1));

            LOG_DEBUG(log, database_name + " database status is OK.");

            mysqlxx::PoolWithFailover::Entry connection = pool.get();
            MaterializeMetadata metadata(connection, getMetadataPath() + "/.metadata", mysql_database_name);

            if (!metadata.need_dumping_tables.empty())
            {
                metadata.transaction(Position(metadata.binlog_position, metadata.binlog_file), [&]()
                {
                    cleanOutdatedTables();
                    dumpDataForTables(connection, metadata, is_cancelled);
                });
            }

            client.connect();
            client.startBinlogDump(std::rand(), mysql_database_name, metadata.binlog_file, metadata.binlog_position);
            return metadata;
        }
        catch (mysqlxx::Exception & )
        {
            tryLogCurrentException(log);

            /// Avoid busy loop when MySQL is not available.
            sleepForMilliseconds(settings->max_wait_time_when_mysql_unavailable);
        }
    }

    return {};
}

void DatabaseMaterializeMySQL::scheduleSynchronized()
{
    background_thread_pool.scheduleOrThrowOnError([&]()
    {
        ThreadStatus thread_status;
        setThreadName("MySQLDBSync");

        std::unique_lock<std::mutex> lock(sync_mutex);
        const auto quit_requested = [this] { return sync_quit.load(std::memory_order_relaxed); };

        try
        {
            std::optional<MaterializeMetadata> metadata = prepareSynchronized(lock, quit_requested);

            if (!quit_requested() && metadata)
            {
                EventConsumer consumer(getDatabaseName(), global_context, *metadata, *settings);

                while (!quit_requested())
                {
                    const auto & event = client.readOneBinlogEvent();
                    consumer.onEvent(event, client.getPosition());
                }
            }
        }
        catch(...)
        {
            setException(std::current_exception());
        }
    });
}
DatabaseMaterializeMySQL::~DatabaseMaterializeMySQL()
{
    try
    {
        if (!sync_quit)
        {
            {
                sync_quit = true;
                std::lock_guard<std::mutex> lock(sync_mutex);
            }

            sync_cond.notify_one();
            background_thread_pool.wait();
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}

#endif
