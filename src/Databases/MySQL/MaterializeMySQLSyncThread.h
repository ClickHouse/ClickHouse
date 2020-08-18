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
        const Context & context, const String & database_name_, const String & mysql_database_name_
        , mysqlxx::Pool && pool_, MySQLClient && client_, MaterializeMySQLSettings * settings_);

    void stopSynchronization();

    void startSynchronization();

    static bool isMySQLSyncThread();

private:
    Poco::Logger * log;
    const Context & global_context;

    String database_name;
    String mysql_database_name;

    mutable mysqlxx::Pool pool;
    mutable MySQLClient client;
    MaterializeMySQLSettings * settings;
    String query_prefix;

    struct Buffers
    {
        String database;

        /// thresholds
        size_t max_block_rows = 0;
        size_t max_block_bytes = 0;
        size_t total_blocks_rows = 0;
        size_t total_blocks_bytes = 0;

        using BufferAndSortingColumns = std::pair<Block, std::vector<size_t>>;
        using BufferAndSortingColumnsPtr = std::shared_ptr<BufferAndSortingColumns>;
        std::unordered_map<String, BufferAndSortingColumnsPtr> data;

        Buffers(const String & database_) : database(database_) {}

        void commit(const Context & context);

        void add(size_t block_rows, size_t block_bytes, size_t written_rows, size_t written_bytes);

        bool checkThresholds(size_t check_block_rows, size_t check_block_bytes, size_t check_total_rows, size_t check_total_bytes) const;

        BufferAndSortingColumnsPtr getTableDataBuffer(const String & table, const Context & context);
    };

    void synchronization(const String & mysql_version);

    bool isCancelled() { return sync_quit.load(std::memory_order_relaxed); }

    std::optional<MaterializeMetadata> prepareSynchronized(const String & mysql_version);

    void flushBuffersData(Buffers & buffers, MaterializeMetadata & metadata);

    void onEvent(Buffers & buffers, const MySQLReplication::BinlogEventPtr & event, MaterializeMetadata & metadata);

    std::atomic<bool> sync_quit{false};
    std::unique_ptr<ThreadFromGlobalPool> background_thread_pool;
};

}

#endif
