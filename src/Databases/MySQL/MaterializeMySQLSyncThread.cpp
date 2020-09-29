#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#include <Databases/MySQL/MaterializeMySQLSyncThread.h>

#    include <cstdlib>
#    include <memory>
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
#    include <Databases/MySQL/MySQLDump.h>
#    include <Formats/MySQLBlockInputStream.h>
#    include <IO/ReadBufferFromString.h>
#    include <Interpreters/Context.h>
#    include <Interpreters/executeQuery.h>
#    include <Storages/StorageMergeTree.h>
#    include <Common/setThreadName.h>
#    include <common/sleep.h>
#    include <ext/bit_cast.h>
#    include <mysqlxx/PoolWithFailover.h>

namespace DB
{

using namespace MySQLReplicaConsumer;

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int ILLEGAL_MYSQL_VARIABLE;
}

static constexpr auto MYSQL_BACKGROUND_THREAD_NAME = "MySQLDBSync";

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
    , mysql_database_name(mysql_database_name_)
    , pool(std::move(pool_))
    , client(std::move(client_))
    , settings(settings_)
    , mysql_version(mysql_version_)
    , has_new_consumers(true)
    , has_consumers(false)
{
    registerConsumerDatabase(database_name_, materialize_metadata_path_);
}

void MaterializeMySQLSyncThread::registerConsumerDatabase(
    const String & database_name_,
    const String & materialize_metadata_path)
{
    ConsumerDatabasePtr consumer = std::make_shared<ConsumerDatabase>(
        database_name_,
        mysql_database_name,
        materialize_metadata_path);
    consumers.push_back(consumer);
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
                for (auto consumer : consumers)
                {
                    onEvent(
                        global_context,
                        consumer,
                        binlog_event,
                        log,
                        [this](ConsumerPtr c) -> void { flushBuffersData(c); });
                }
            }

            for (auto consumer : consumers)
            {
                if (watch.elapsedMilliseconds() > max_flush_time ||
                    consumer->buffer->checkThresholds(
                        settings->max_rows_in_buffer,
                        settings->max_bytes_in_buffer,
                        settings->max_rows_in_buffers,
                        settings->max_bytes_in_buffers))
                {
                    watch.restart();

                    if (!consumer->buffer->data.empty())
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

        for (auto consumer : consumers) {
            if (auto c = dynamic_pointer_cast<ConsumerDatabase>(consumer)){
                getDatabase(c->database_name)
                    .setException(std::current_exception());
            }
        }
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

bool MaterializeMySQLSyncThread::prepareConsumers() {
    if (!has_new_consumers)
    {
        return has_consumers;
    }

    has_new_consumers = false;
    for (auto consumer : consumers)
    {
        if (!consumer->prepared)
        {
            has_consumers |= prepareConsumer(consumer);
        }
    }

    return has_consumers;
}

bool MaterializeMySQLSyncThread::prepareConsumer(ConsumerPtr consumer)
{
    bool opened_transaction = false;
    mysqlxx::PoolWithFailover::Entry connection;

    while (!isCancelled())
    {
        try
        {
            connection = pool.get();
            opened_transaction = false;

            consumer->materialize_metadata = std::make_shared<MaterializeMetadata>(
                consumer->materialize_metadata_path,
                mysql_version);
            consumer->materialize_metadata->tryInitFromFile(connection);

            ConsumerDatabasePtr consumer_database =
                std::dynamic_pointer_cast<ConsumerDatabase>(consumer);

            std::unordered_map<String, String> need_dumping_tables;
            fetchMetadata(
                connection,
                mysql_database_name,
                consumer->materialize_metadata,
                static_cast<bool>(consumer_database),
                opened_transaction,
                need_dumping_tables);

            if (consumer_database)
            {
                dumpTables(
                    global_context,
                    connection,
                    log,
                    consumer_database,
                    need_dumping_tables,
                    [this] { return isCancelled(); });
            }

            if (opened_transaction)
                connection->query("COMMIT").execute();

            consumer->prepared = true;
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

    if (ConsumerDatabasePtr c = std::dynamic_pointer_cast<ConsumerDatabase>(consumer)) {
        c->buffer = std::make_shared<MySQLDatabaseBuffer>(c->database_name);
    }

    return consumer->prepared;
}

static inline UInt32 randomNumber()
{
    std::mt19937 rng;
    rng.seed(std::random_device()());
    std::uniform_int_distribution<std::mt19937::result_type> dist6(std::numeric_limits<UInt32>::min(), std::numeric_limits<UInt32>::max());
    return dist6(rng);
}

void MaterializeMySQLSyncThread::startClient()
{
    if (!client.isConnected())
    {
        client.connect();
        client.startBinlogDumpGTID(
            randomNumber(),
            mysql_database_name,
            consumers.front()->materialize_metadata->executed_gtid_set);
    }
}

void MaterializeMySQLSyncThread::flushBuffersData(ConsumerPtr consumer)
{
    consumer->materialize_metadata->transaction(client.getPosition(), [&]() { consumer->buffer->commit(global_context); });

    const auto & position_message = [&]()
    {
        std::stringstream ss;
        client.getPosition().dump(ss);
        return ss.str();
    };
    LOG_INFO(log, "MySQL executed position: \n {}", position_message());
}

bool MaterializeMySQLSyncThread::isMySQLSyncThread()
{
    return getThreadName() == MYSQL_BACKGROUND_THREAD_NAME;
}

}

#endif
