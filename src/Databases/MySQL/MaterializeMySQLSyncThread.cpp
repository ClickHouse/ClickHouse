#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#include <Databases/MySQL/MaterializeMySQLSyncThread.h>

#    include <cstdlib>
#    include <random>
#    include <Columns/ColumnTuple.h>
#    include <Columns/ColumnDecimal.h>
#    include <DataStreams/CountingBlockOutputStream.h>
#    include <DataStreams/IBlockStream_fwd.h>
#    include <DataStreams/copyData.h>
#    include <Databases/MySQL/DatabaseMaterializeMySQL.h>
#    include <Databases/MySQL/MaterializeMetadata.h>
#    include <Databases/MySQL/MySQLBinlogEvent.h>
#    include <Databases/MySQL/MySQLUtils.h>
#    include <Formats/MySQLBlockInputStream.h>
#    include <IO/ReadBufferFromString.h>
#    include <Interpreters/Context.h>
#    include <Interpreters/executeQuery.h>
#    include <Storages/StorageMergeTree.h>
#    include <Common/quoteString.h>
#    include <Common/setThreadName.h>
#    include <common/sleep.h>
#    include <ext/bit_cast.h>
#    include <mysqlxx/PoolWithFailover.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int ILLEGAL_MYSQL_VARIABLE;
}

static constexpr auto MYSQL_BACKGROUND_THREAD_NAME = "MySQLDBSync";

static inline DatabaseMaterializeMySQL & getDatabase(const String & database_name)
{
    DatabasePtr database = DatabaseCatalog::instance().getDatabase(database_name);

    if (DatabaseMaterializeMySQL * database_materialize = typeid_cast<DatabaseMaterializeMySQL *>(database.get()))
        return *database_materialize;

    throw Exception("LOGICAL_ERROR: cannot cast to DatabaseMaterializeMySQL, it is a bug.", ErrorCodes::LOGICAL_ERROR);
}

MaterializeMySQLSyncThread::~MaterializeMySQLSyncThread()
{
    try
    {
        stopSynchronization();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

MaterializeMySQLSyncThread::MaterializeMySQLSyncThread(
    const Context & context,
    const String & database_name_,
    const String & mysql_database_name_,
    mysqlxx::Pool && pool_,
    MySQLClient && client_,
    MaterializeMySQLSettings * settings_,
    const String & materialize_metadata_path_,
    const String & mysql_version_)
    : log(&Poco::Logger::get("MaterializeMySQLSyncThread"))
    , global_context(context.getGlobalContext())
    , database_name(database_name_)
    , mysql_database_name(mysql_database_name_)
    , pool(std::move(pool_))
    , client(std::move(client_))
    , settings(settings_)
    , mysql_version(mysql_version_)
    , has_new_consumers(true)
    , has_consumers(false)
{
    query_prefix = "EXTERNAL DDL FROM MySQL(" +
        backQuoteIfNeed(database_name) + ", " +
        backQuoteIfNeed(mysql_database_name) + ") ";

    registerConsumerDatabase(materialize_metadata_path_);
}

void MaterializeMySQLSyncThread::registerConsumerDatabase(const String & materialize_metadata_path)
{
    consumers.push_back({
        .consumer_type = Consumer::Type::kDatabase,
        .materialize_metadata_path = materialize_metadata_path,
        .prepared = false});
    has_new_consumers = true;
}

void MaterializeMySQLSyncThread::synchronization()
{
    setThreadName(MYSQL_BACKGROUND_THREAD_NAME);

    try
    {
        Stopwatch watch;

        while (!isCancelled())
        {
            if (!prepareConsumers()) {
                continue;
            }
            startClient();

            UInt64 max_flush_time = settings->max_flush_data_time;
            BinlogEventPtr binlog_event = client.readOneBinlogEvent(
                std::max(UInt64(1),
                max_flush_time - watch.elapsedMilliseconds()));

            if (binlog_event)
            {
                for (auto & consumer : consumers)
                {
                    onEvent(consumer, binlog_event);
                }
            }

            if (watch.elapsedMilliseconds() > max_flush_time ||
                consumers.front().buffer->checkThresholds(
                    settings->max_rows_in_buffer,
                    settings->max_bytes_in_buffer,
                    settings->max_rows_in_buffers,
                    settings->max_bytes_in_buffers))
            {
                watch.restart();

                for (auto & consumer : consumers)
                {
                    if (!consumer.buffer->data.empty())
                    {
                        flushBuffersData(consumer);
                    }
                }
            }
        }
    }
    catch (...)
    {
        client.disconnect();
        tryLogCurrentException(log);
        getDatabase(database_name).setException(std::current_exception());
    }
}

void MaterializeMySQLSyncThread::stopSynchronization()
{
    if (!sync_quit && background_thread_pool)
    {
        sync_quit = true;
        background_thread_pool->join();
        client.disconnect();
    }
}

void MaterializeMySQLSyncThread::startSynchronization()
{
    background_thread_pool = std::make_unique<ThreadFromGlobalPool>(
        [this]() { synchronization(); });
}

static inline void cleanOutdatedTables(const String & database_name, const Context & context)
{
    auto ddl_guard = DatabaseCatalog::instance().getDDLGuard(database_name, "");
    const DatabasePtr & clean_database = DatabaseCatalog::instance().getDatabase(database_name);

    for (auto iterator = clean_database->getTablesIterator(context); iterator->isValid(); iterator->next())
    {
        Context query_context = createQueryContext(context);
        String comment = "Materialize MySQL step 1: execute MySQL DDL for dump data";
        String table_name = backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(iterator->name());
        tryToExecuteQuery(" DROP TABLE " + table_name, query_context, database_name, comment);
    }
}

static inline void dumpDataForTables(
    mysqlxx::Pool::Entry & connection, std::unordered_map<String, String>& need_dumping_tables,
    const String & query_prefix, const String & database_name, const String & mysql_database_name,
    const Context & context, const std::function<bool()> & is_cancelled)
{
    auto iterator = need_dumping_tables.begin();
    for (; iterator != need_dumping_tables.end() && !is_cancelled(); ++iterator)
    {
        const auto & table_name = iterator->first;
        Context query_context = createQueryContext(context);
        String comment = "Materialize MySQL step 1: execute MySQL DDL for dump data";
        tryToExecuteQuery(query_prefix + " " + iterator->second, query_context, database_name, comment); /// create table.

        auto out = std::make_shared<CountingBlockOutputStream>(getTableOutput(database_name, table_name, query_context));
        MySQLBlockInputStream input(
            connection, "SELECT * FROM " + backQuoteIfNeed(mysql_database_name) + "." + backQuoteIfNeed(table_name),
            out->getHeader(), DEFAULT_BLOCK_SIZE);

        Stopwatch watch;
        copyData(input, *out, is_cancelled);
        const Progress & progress = out->getProgress();
        LOG_INFO(&Poco::Logger::get("MaterializeMySQLSyncThread(" + database_name + ")"),
            "Materialize MySQL step 1: dump {}, {} rows, {} in {} sec., {} rows/sec., {}/sec."
            , table_name, formatReadableQuantity(progress.written_rows), formatReadableSizeWithBinarySuffix(progress.written_bytes)
            , watch.elapsedSeconds(), formatReadableQuantity(static_cast<size_t>(progress.written_rows / watch.elapsedSeconds()))
            , formatReadableSizeWithBinarySuffix(static_cast<size_t>(progress.written_bytes / watch.elapsedSeconds())));
    }
}

static inline UInt32 randomNumber()
{
    std::mt19937 rng;
    rng.seed(std::random_device()());
    std::uniform_int_distribution<std::mt19937::result_type> dist6(std::numeric_limits<UInt32>::min(), std::numeric_limits<UInt32>::max());
    return dist6(rng);
}

static std::vector<String> fetchTablesInDB(
    const mysqlxx::PoolWithFailover::Entry & connection,
    const std::string & database)
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

static std::unordered_map<String, String> fetchTablesCreateQuery(
    const mysqlxx::PoolWithFailover::Entry & connection,
    const String & database_name)
{
    std::vector<String> fetch_tables = fetchTablesInDB(connection, database_name);
    std::unordered_map<String, String> tables_create_query;
    for (const auto & fetch_table_name : fetch_tables)
    {
        Block show_create_table_header{
            {std::make_shared<DataTypeString>(), "Table"},
            {std::make_shared<DataTypeString>(), "Create Table"},
        };

        MySQLBlockInputStream show_create_table(
            connection, "SHOW CREATE TABLE " + backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(fetch_table_name),
            show_create_table_header, DEFAULT_BLOCK_SIZE);

        Block create_query_block = show_create_table.read();
        if (!create_query_block || create_query_block.rows() != 1)
            throw Exception("LOGICAL ERROR mysql show create return more rows.", ErrorCodes::LOGICAL_ERROR);

        tables_create_query[fetch_table_name] = create_query_block.getByName("Create Table").column->getDataAt(0).toString();
    }

    return tables_create_query;
}

void fetchMetadata(
    mysqlxx::PoolWithFailover::Entry & connection,
    const String & database_name,
    MaterializeMetadataPtr materialize_metadata,
    bool fetch_need_dumping_tables,
    bool & opened_transaction,
    std::unordered_map<String, String> & need_dumping_tables)
{

    bool locked_tables = false;

    try
    {
        connection->query("FLUSH TABLES;").execute();
        connection->query("FLUSH TABLES WITH READ LOCK;").execute();

        locked_tables = true;
        materialize_metadata->fetchMasterStatus(connection);

        if (fetch_need_dumping_tables) {
            connection->query("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;").execute();
            connection->query("START TRANSACTION /*!40100 WITH CONSISTENT SNAPSHOT */;").execute();

            opened_transaction = true;
            need_dumping_tables = fetchTablesCreateQuery(
                connection,
                database_name);
        }

        connection->query("UNLOCK TABLES;").execute();
    }
    catch (...)
    {
        if (locked_tables)
            connection->query("UNLOCK TABLES;").execute();

        throw;
    }
}

void MaterializeMySQLSyncThread::dumpTables(
    const Consumer & consumer,
    mysqlxx::Pool::Entry & connection,
    std::unordered_map<String, String> & need_dumping_tables)
{
    if (need_dumping_tables.empty())
    {
        return;
    }

    Position position;
    position.update(
        consumer.materialize_metadata->binlog_position,
        consumer.materialize_metadata->binlog_file,
        consumer.materialize_metadata->executed_gtid_set);

    consumer.materialize_metadata->transaction(position, [&]()
    {
        cleanOutdatedTables(database_name, global_context);
        dumpDataForTables(connection, need_dumping_tables, query_prefix, database_name, mysql_database_name, global_context, [this] { return isCancelled(); });
    });

    const auto & position_message = [&]()
    {
        std::stringstream ss;
        position.dump(ss);
        return ss.str();
    };
    LOG_INFO(log, "MySQL dump database position: \n {}", position_message());
}

bool MaterializeMySQLSyncThread::prepareConsumers() {
    if (!has_new_consumers)
    {
        return has_consumers;
    }

    has_new_consumers = false;
    for (auto & consumer : consumers)
    {
        if (!consumer.prepared)
        {
            has_consumers |= prepareConsumer(consumer);
        }
    }

    return has_consumers;
}

bool MaterializeMySQLSyncThread::prepareConsumer(Consumer & consumer)
{
    bool opened_transaction = false;
    mysqlxx::PoolWithFailover::Entry connection;

    while (!isCancelled())
    {
        try
        {
            connection = pool.get();
            opened_transaction = false;

            consumer.materialize_metadata = std::make_shared<MaterializeMetadata>(
                consumer.materialize_metadata_path,
                mysql_version);
            consumer.materialize_metadata->tryInitFromFile(connection);

            bool is_database = consumer.consumer_type == Consumer::Type::kDatabase;

            std::unordered_map<String, String> need_dumping_tables;
            fetchMetadata(connection, mysql_database_name, consumer.materialize_metadata, is_database, opened_transaction, need_dumping_tables);

            dumpTables(consumer, connection, need_dumping_tables);

            if (opened_transaction)
                connection->query("COMMIT").execute();

            consumer.prepared = true;
            break;
        }
        catch (...)
        {
            tryLogCurrentException(log);

            if (opened_transaction)
                connection->query("ROLLBACK").execute();

            try
            {
                throw;
            }
            catch (const mysqlxx::ConnectionFailed &)
            {
                /// Avoid busy loop when MySQL is not available.
                sleepForMilliseconds(settings->max_wait_time_when_mysql_unavailable);
            }
        }
    }

    switch (consumer.consumer_type) {
        case Consumer::Type::kDatabase:
            consumer.buffer = std::make_shared<MySQLDatabaseBuffer>(database_name);
            break;
        default:
            break;
    }

    return consumer.prepared;
}

void MaterializeMySQLSyncThread::startClient()
{
    if (!client.isConnected())
    {
        client.connect();
        client.startBinlogDumpGTID(
            randomNumber(),
            mysql_database_name,
            consumers.front().materialize_metadata->executed_gtid_set);
    }
}

void MaterializeMySQLSyncThread::flushBuffersData(Consumer & consumer)
{
    consumer.materialize_metadata->transaction(client.getPosition(), [&]() { consumer.buffer->commit(global_context); });

    const auto & position_message = [&]()
    {
        std::stringstream ss;
        client.getPosition().dump(ss);
        return ss.str();
    };
    LOG_INFO(log, "MySQL executed position: \n {}", position_message());
}

void MaterializeMySQLSyncThread::onEvent(Consumer & consumer, const BinlogEventPtr & receive_event)
{
    if (receive_event->type() == MYSQL_WRITE_ROWS_EVENT)
    {
        WriteRowsEvent & write_rows_event = static_cast<WriteRowsEvent &>(*receive_event);
        MySQLBufferAndSortingColumnsPtr buffer = consumer.buffer->getTableDataBuffer(write_rows_event.table, global_context);
        size_t bytes = onWriteOrDeleteData<1>(write_rows_event.rows, buffer->first, ++consumer.materialize_metadata->data_version);
        consumer.buffer->add(buffer->first.rows(), buffer->first.bytes(), write_rows_event.rows.size(), bytes);
    }
    else if (receive_event->type() == MYSQL_UPDATE_ROWS_EVENT)
    {
        UpdateRowsEvent & update_rows_event = static_cast<UpdateRowsEvent &>(*receive_event);
        MySQLBufferAndSortingColumnsPtr buffer = consumer.buffer->getTableDataBuffer(update_rows_event.table, global_context);
        size_t bytes = onUpdateData(update_rows_event.rows, buffer->first, ++consumer.materialize_metadata->data_version, buffer->second);
        consumer.buffer->add(buffer->first.rows(), buffer->first.bytes(), update_rows_event.rows.size(), bytes);
    }
    else if (receive_event->type() == MYSQL_DELETE_ROWS_EVENT)
    {
        DeleteRowsEvent & delete_rows_event = static_cast<DeleteRowsEvent &>(*receive_event);
        MySQLBufferAndSortingColumnsPtr buffer = consumer.buffer->getTableDataBuffer(delete_rows_event.table, global_context);
        size_t bytes = onWriteOrDeleteData<-1>(delete_rows_event.rows, buffer->first, ++consumer.materialize_metadata->data_version);
        consumer.buffer->add(buffer->first.rows(), buffer->first.bytes(), delete_rows_event.rows.size(), bytes);
    }
    else if (receive_event->type() == MYSQL_QUERY_EVENT)
    {
        QueryEvent & query_event = static_cast<QueryEvent &>(*receive_event);
        flushBuffersData(consumer);

        try
        {
            Context query_context = createQueryContext(global_context);
            String comment = "Materialize MySQL step 2: execute MySQL DDL for sync data";
            String event_database = query_event.schema == mysql_database_name ? database_name : "";
            tryToExecuteQuery(query_prefix + query_event.query, query_context, event_database, comment);
        }
        catch (Exception & exception)
        {
            tryLogCurrentException(log);

            /// If some DDL query was not successfully parsed and executed
            /// Then replication may fail on next binlog events anyway
            if (exception.code() != ErrorCodes::SYNTAX_ERROR)
                throw;
        }
    }
    else if (receive_event->header.type != HEARTBEAT_EVENT)
    {
        const auto & dump_event_message = [&]()
        {
            std::stringstream ss;
            receive_event->dump(ss);
            return ss.str();
        };

        LOG_DEBUG(log, "Skip MySQL event: \n {}", dump_event_message());
    }
}

bool MaterializeMySQLSyncThread::isMySQLSyncThread()
{
    return getThreadName() == MYSQL_BACKGROUND_THREAD_NAME;
}

}

#endif
