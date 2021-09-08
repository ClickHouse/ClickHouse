#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#    include <mutex>
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
class MaterializeMySQLSyncThread : WithContext
{
public:
    ~MaterializeMySQLSyncThread();

    MaterializeMySQLSyncThread(
        ContextPtr context,
        const String & database_name_,
        const String & mysql_database_name_,
        mysqlxx::Pool && pool_,
        MySQLClient && client_,
        MaterializeMySQLSettings * settings_);

    void stopSynchronization();

    void startSynchronization();

    void assertMySQLAvailable();

    static bool isMySQLSyncThread();

private:
    Poco::Logger * log;

    String database_name;
    String mysql_database_name;

    mutable mysqlxx::Pool pool;
    mutable MySQLClient client;
    MaterializeMySQLSettings * settings;
    String query_prefix;

    // USE MySQL ERROR CODE:
    // https://dev.mysql.com/doc/mysql-errors/5.7/en/server-error-reference.html
    const int ER_ACCESS_DENIED_ERROR = 1045;
    const int ER_DBACCESS_DENIED_ERROR = 1044;
    const int ER_BAD_DB_ERROR = 1049;

    // https://dev.mysql.com/doc/mysql-errors/8.0/en/client-error-reference.html
    const int CR_SERVER_LOST = 2013;

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

        void commit(ContextPtr context);

        void add(size_t block_rows, size_t block_bytes, size_t written_rows, size_t written_bytes);

        bool checkThresholds(size_t check_block_rows, size_t check_block_bytes, size_t check_total_rows, size_t check_total_bytes) const;

        BufferAndSortingColumnsPtr getTableDataBuffer(const String & table, ContextPtr context);
    };

    void synchronization();

    bool isCancelled() { return sync_quit.load(std::memory_order_relaxed); }

    bool prepareSynchronized(MaterializeMetadata & metadata);

    void flushBuffersData(Buffers & buffers, MaterializeMetadata & metadata);

    void onEvent(Buffers & buffers, const MySQLReplication::BinlogEventPtr & event, MaterializeMetadata & metadata);

    std::atomic<bool> sync_quit{false};
    std::unique_ptr<ThreadFromGlobalPool> background_thread_pool;
    void executeDDLAtomic(const QueryEvent & query_event);

    void setSynchronizationThreadException(const std::exception_ptr & exception);
};

}

#endif
