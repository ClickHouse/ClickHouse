#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#    include <mutex>
#    include <Common/quoteString.h>
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

namespace MySQLReplicaConsumer
{
    struct Consumer
    {
        Consumer(
            const String & mysql_database_name_,
            const String & materialize_metadata_path_,
            MaterializeMySQLSettingsPtr settings_)
            : mysql_database_name(mysql_database_name_)
            , materialize_metadata_path(materialize_metadata_path_)
            , settings(settings_)
            , prepared(false)
        {
        }

        String mysql_database_name;
        String materialize_metadata_path;
        MaterializeMySQLSettingsPtr settings;

        bool prepared;

        MaterializeMetadataPtr materialize_metadata;
        IMySQLBufferPtr buffer;

        virtual ~Consumer() = default;
    };

    using ConsumerPtr = std::shared_ptr<Consumer>;

    struct ConsumerDatabase : public Consumer {
        ConsumerDatabase(
            const String & database_name_,
            const String & mysql_database_name_,
            const String & materialize_metadata_path_,
            MaterializeMySQLSettingsPtr settings_)
            : Consumer(mysql_database_name_, materialize_metadata_path_, settings_)
            , database_name(database_name_)
        {
        }

        String database_name;
        String getQueryPrefix() const {
            return "EXTERNAL DDL FROM MySQL(" +
                backQuoteIfNeed(database_name) + ", " +
                backQuoteIfNeed(mysql_database_name) + ") ";
        }
    };

    using ConsumerDatabasePtr = std::shared_ptr<ConsumerDatabase>;
}

using namespace MySQLReplicaConsumer;

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
        const String & mysql_database_name_,
        mysqlxx::Pool && pool_,
        MySQLClient && client_,
        const String & mysql_version_);

    void stopSynchronization();

    void startSynchronization();

    void registerConsumerDatabase(
        const String & database_name_,
        const String & materialize_metadata_path,
        MaterializeMySQLSettingsPtr settings_);

    static bool isMySQLSyncThread();

private:
    Poco::Logger * log;
    const Context & global_context;

    String mysql_database_name;

    mutable mysqlxx::Pool pool;
    mutable MySQLClient client;
    String mysql_version;

    std::atomic<bool> has_new_consumers;
    bool has_consumers;

    UInt64 max_flush_time;
    UInt64 max_wait_time_when_mysql_unavailable;

    std::vector<ConsumerPtr> consumers;

    void synchronization();

    bool isCancelled() { return sync_quit.load(std::memory_order_relaxed); }

    void startClient();

    bool prepareConsumers();

    bool prepareConsumer(ConsumerPtr consumer);

    void flushBuffersData(ConsumerPtr consumer);

    std::atomic<bool> sync_quit{false};
    std::unique_ptr<ThreadFromGlobalPool> background_thread_pool;
};

using MaterializeMySQLSyncThreadPtr = std::shared_ptr<MaterializeMySQLSyncThread>;

}

#endif
