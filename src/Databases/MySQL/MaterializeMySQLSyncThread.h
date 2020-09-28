#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#    include <mutex>
#    include <Core/BackgroundSchedulePool.h>
#    include <Core/MySQL/MySQLClient.h>
#    include <DataStreams/BlockIO.h>
#    include <DataTypes/DataTypeString.h>
#    include <DataTypes/DataTypesNumber.h>
#    include <Databases/DatabaseOrdinary.h>
#    include <Databases/IDatabase.h>
#    include <Databases/MySQL/MaterializeMetadata.h>
#    include <Databases/MySQL/MaterializeMySQLSettings.h>
#    include <Databases/MySQL/MySQLBuffer.h>
#    include <Parsers/ASTCreateQuery.h>
#    include <mysqlxx/Pool.h>
#    include <mysqlxx/PoolWithFailover.h>

namespace DB
{

/** MySQL table structure and data synchronization thread
 *
 *  When catch exception, it always exits immediately.
 *  In this case, you need to execute DETACH DATABASE and ATTACH DATABASE after manual processing
 *
 *  The whole work of the thread includes synchronous full data and real-time pull incremental data
 *
 *  synchronous full data:
 *      We will synchronize the full data when the database is first create or not found binlog file in MySQL after restart.
 *
 *  real-time pull incremental data:
 *      We will pull the binlog event of MySQL to parse and execute when the full data synchronization is completed.
 */
class MaterializeMySQLSyncThread
{
public:
    ~MaterializeMySQLSyncThread();

    MaterializeMySQLSyncThread(
        const Context & context,
        const String & database_name_,
        const String & mysql_database_name_,
        mysqlxx::Pool && pool_,
        MySQLClient && client_,
        MaterializeMySQLSettings * settings_,
        const String & materialize_metadata_path_,
        const String & mysql_version_);

    void stopSynchronization();

    void startSynchronization();

    void registerConsumerDatabase(const String & materialize_metadata_path);

    static bool isMySQLSyncThread();

private:
    Poco::Logger * log;
    const Context & global_context;

    String database_name;
    String mysql_database_name;

    mutable mysqlxx::Pool pool;
    mutable MySQLClient client;
    MaterializeMySQLSettings * settings;
    String mysql_version;

    std::atomic<bool> has_new_consumers;
    bool has_consumers;

    String query_prefix;

    MaterializeMetadataPtr materialize_metadata;

    struct Consumer
    {
        enum Type
        {
            kDatabase,
            kTable
        };
        Type consumer_type;

        String materialize_metadata_path;
        bool prepared;

        MaterializeMetadataPtr materialize_metadata;
        IMySQLBufferPtr buffer;
    };

    std::vector<Consumer> consumers;

    void synchronization();

    bool isCancelled() { return sync_quit.load(std::memory_order_relaxed); }

    void startClient();

    bool prepareConsumers();

    bool prepareConsumer(Consumer & consumer);

    void dumpTables(
        const Consumer & consumer,
        mysqlxx::Pool::Entry & connection,
        std::unordered_map<String, String> & need_dumping_tables);

    void flushBuffersData(Consumer & consumer);

    void onEvent(Consumer & consumer, const MySQLReplication::BinlogEventPtr & event);

    std::atomic<bool> sync_quit{false};
    std::unique_ptr<ThreadFromGlobalPool> background_thread_pool;
};

using MaterializeMySQLSyncThreadPtr = std::shared_ptr<MaterializeMySQLSyncThread>;

}

#endif
