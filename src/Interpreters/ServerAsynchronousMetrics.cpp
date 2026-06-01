#include <Interpreters/ServerAsynchronousMetrics.h>

#include <Interpreters/Aggregator.h>
#include <Interpreters/AsynchronousMetricLog.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileCacheFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cache/QueryResultCache.h>
#include <Interpreters/ExternalDictionariesLoader.h>

#include <Databases/IDatabase.h>

#include <IO/UncompressedCache.h>
#include <IO/MMappedFileCache.h>
#include <Common/PageCache.h>
#include <Common/quoteString.h>
#include <Common/HTTPConnectionPool.h>
#include <Common/HistogramMetrics.h>
#include <Common/TCPSocketMemInfo.h>
#include <Common/ThreadStackRegistry.h>


#include "config.h"
#if USE_AWS_S3
#include <IO/S3/Client.h>
#endif

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageReplicatedMergeTree.h>
#if CLICKHOUSE_CLOUD
#include <Storages/StorageSharedMergeTree.h>
#endif

#include <Coordination/KeeperAsynchronousMetrics.h>

#if defined(OS_LINUX) && __has_include(<linux/sock_diag.h>)
namespace HistogramMetrics
{
    extern Metric & HTTPPoolTCPBufBytesDiskRcv;
    extern Metric & HTTPPoolTCPBufBytesDiskSnd;
    extern Metric & HTTPPoolTCPBufBytesStorageRcv;
    extern Metric & HTTPPoolTCPBufBytesStorageSnd;
    extern Metric & HTTPPoolTCPBufBytesHTTPRcv;
    extern Metric & HTTPPoolTCPBufBytesHTTPSnd;
}
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_IS_READ_ONLY;
}

namespace
{

template <typename Max, typename T>
void calculateMax(Max & max, T x)
{
    if (Max(x) > max)
        max = x;
}

template <typename Max, typename Sum, typename T>
void calculateMaxAndSum(Max & max, Sum & sum, T x)
{
    sum += x;
    if (Max(x) > max)
        max = x;
}

#if defined(OS_LINUX) && __has_include(<linux/sock_diag.h>)

/// For one connection pool group, observe each tracked socket's rmem/wmem into the
/// corresponding histogram and emit per-group total async metrics.
void emitTCPBufferMetrics(
    const std::vector<uint64_t> & inodes,
    const char * group_name,
    HistogramMetrics::Metric & rcv_histogram,
    HistogramMetrics::Metric & snd_histogram,
    const std::unordered_map<uint64_t, TCPSocketMemInfo> & meminfo_by_inode,
    AsynchronousMetricValues & new_values)
{
    bool any = false;
    uint64_t rmem_total = 0;
    uint64_t wmem_total = 0;

    for (uint64_t inode : inodes)
    {
        if (auto it = meminfo_by_inode.find(inode); it != meminfo_by_inode.end())
        {
            any = true;
            rcv_histogram.observe(static_cast<double>(it->second.rmem));
            snd_histogram.observe(static_cast<double>(it->second.wmem));
            rmem_total += it->second.rmem;
            wmem_total += it->second.wmem;
        }
    }

    if (!any)
        return;

    new_values[fmt::format("HTTPConnectionPool{}TCPRcvBufTotalBytes", group_name)]
        = {static_cast<double>(rmem_total),
           "Total kernel TCP receive buffer memory (sk_rmem_alloc) across all HTTP connection pool sockets."};
    new_values[fmt::format("HTTPConnectionPool{}TCPSndBufTotalBytes", group_name)]
        = {static_cast<double>(wmem_total),
           "Total kernel TCP transmit buffer memory (sk_wmem_alloc) across all HTTP connection pool sockets."};
}

/// Observe kernel TCP buffer memory of HTTP connection pool sockets into per-group histograms,
/// and emit per-group total async metrics. Uses sock_diag netlink to read per-socket rmem/wmem.
void updateHTTPConnectionPoolTCPBufferMetrics(
    const HTTPConnectionPools::PoolSocketInodes & pool_inodes,
    AsynchronousMetricValues & new_values)
{
    if (pool_inodes.empty())
        return;

    auto meminfo_by_inode = getTCPSocketMemInfoByInode();
    if (meminfo_by_inode.empty())
        return;

    emitTCPBufferMetrics(pool_inodes.disk, "Disk",
        HistogramMetrics::HTTPPoolTCPBufBytesDiskRcv, HistogramMetrics::HTTPPoolTCPBufBytesDiskSnd,
        meminfo_by_inode, new_values);
    emitTCPBufferMetrics(pool_inodes.storage, "Storage",
        HistogramMetrics::HTTPPoolTCPBufBytesStorageRcv, HistogramMetrics::HTTPPoolTCPBufBytesStorageSnd,
        meminfo_by_inode, new_values);
    emitTCPBufferMetrics(pool_inodes.http, "HTTP",
        HistogramMetrics::HTTPPoolTCPBufBytesHTTPRcv, HistogramMetrics::HTTPPoolTCPBufBytesHTTPSnd,
        meminfo_by_inode, new_values);
}

#endif

}

ServerAsynchronousMetrics::ServerAsynchronousMetrics(
    ContextPtr global_context_,
    unsigned update_period_seconds,
    bool update_heavy_metrics_,
    unsigned heavy_metrics_update_period_seconds,
    const ProtocolServerMetricsFunc & protocol_server_metrics_func_,
    bool update_jemalloc_epoch_,
    bool update_rss_)
    : WithContext(global_context_)
    , AsynchronousMetrics(update_period_seconds, protocol_server_metrics_func_, update_jemalloc_epoch_, update_rss_, global_context_)
    , update_heavy_metrics(update_heavy_metrics_)
    , heavy_metric_update_period(heavy_metrics_update_period_seconds)
{
#if defined(OS_LINUX)
    try
    {
        vm_smaps.emplace("/proc/self/smaps");
    }
    catch (...) /// Ok, /proc/self/smaps may not exist (non-Linux, sandbox,
    {           /// etc.). The thread-stack metrics will simply not be
    }           /// published in that case.
#endif
}

ServerAsynchronousMetrics::~ServerAsynchronousMetrics()
{
    /// NOTE: stop() from base class is not enough, since this leads to leak on vptr
    stop();
}

void ServerAsynchronousMetrics::updateImpl(TimePoint update_time, TimePoint current_time, bool force_update, bool first_run, AsynchronousMetricValues & new_values)
{
    {
        size_t total_bytes = 0;
        size_t max_bytes = 0;
        size_t total_files = 0;

        for (const auto & cache_data : FileCacheFactory::instance().getUniqueInstances())
        {
            total_bytes += cache_data->cache->getUsedCacheSize();
            max_bytes += cache_data->cache->getMaxCacheSize();
            total_files += cache_data->cache->getFileSegmentsNum();
        }

        new_values["FilesystemCacheBytes"] = { total_bytes,
            "Total bytes in the `cache` virtual filesystem. This cache is hold on disk." };
        new_values["FilesystemCacheCapacity"] = { max_bytes,
            "Total capacity in the `cache` virtual filesystem. This cache is hold on disk." };
        new_values["FilesystemCacheFiles"] = { total_files,
            "Total number of cached file segments in the `cache` virtual filesystem. This cache is hold on disk." };
    }

    if (auto page_cache = getContext()->getPageCache())
    {
        new_values["PageCacheMaxBytes"] = { page_cache->maxSizeInBytes(),
            "Current limit on the size of userspace page cache, in bytes." };
    }

    new_values["Uptime"] = { getContext()->getUptimeSeconds(),
        "The server uptime in seconds. It includes the time spent for server initialization before accepting connections." };

    if (const auto stats = getHashTablesCacheStatistics())
    {
        new_values["HashTableStatsCacheEntries"] = { stats->entries,
            "The number of entries in the cache of hash table sizes."
            " The cache for hash table sizes is used for predictive optimization of GROUP BY." };
        new_values["HashTableStatsCacheHits"] = { stats->hits,
            "The number of times the prediction of a hash table size was correct." };
        new_values["HashTableStatsCacheMisses"] = { stats->misses,
            "The number of times the prediction of a hash table size was incorrect." };
    }

    /// Free space in filesystems at data path and logs path.
    {
        auto stat = getStatVFS(getContext()->getPath());

        new_values["FilesystemMainPathTotalBytes"] = { stat.f_blocks * stat.f_frsize,
            "The size of the volume where the main ClickHouse path is mounted, in bytes." };
        new_values["FilesystemMainPathAvailableBytes"] = { stat.f_bavail * stat.f_frsize,
            "Available bytes on the volume where the main ClickHouse path is mounted." };
        new_values["FilesystemMainPathUsedBytes"] = { (stat.f_blocks - stat.f_bavail) * stat.f_frsize,
            "Used bytes on the volume where the main ClickHouse path is mounted." };
        new_values["FilesystemMainPathTotalINodes"] = { stat.f_files,
            "The total number of inodes on the volume where the main ClickHouse path is mounted. If it is less than 25 million, it indicates a misconfiguration." };
        new_values["FilesystemMainPathAvailableINodes"] = { stat.f_favail,
            "The number of available inodes on the volume where the main ClickHouse path is mounted. If it is close to zero, it indicates a misconfiguration, and you will get 'no space left on device' even when the disk is not full." };
        new_values["FilesystemMainPathUsedINodes"] = { stat.f_files - stat.f_favail,
            "The number of used inodes on the volume where the main ClickHouse path is mounted. This value mostly corresponds to the number of files." };
    }

    {
        /// Current working directory of the server is the directory with logs.
        auto stat = getStatVFS(".");

        new_values["FilesystemLogsPathTotalBytes"] = { stat.f_blocks * stat.f_frsize,
            "The size of the volume where ClickHouse logs path is mounted, in bytes. It's recommended to have at least 10 GB for logs." };
        new_values["FilesystemLogsPathAvailableBytes"] = { stat.f_bavail * stat.f_frsize,
            "Available bytes on the volume where ClickHouse logs path is mounted. If this value approaches zero, you should tune the log rotation in the configuration file." };
        new_values["FilesystemLogsPathUsedBytes"] = { (stat.f_blocks - stat.f_bavail) * stat.f_frsize,
            "Used bytes on the volume where ClickHouse logs path is mounted." };
        new_values["FilesystemLogsPathTotalINodes"] = { stat.f_files,
            "The total number of inodes on the volume where ClickHouse logs path is mounted." };
        new_values["FilesystemLogsPathAvailableINodes"] = { stat.f_favail,
            "The number of available inodes on the volume where ClickHouse logs path is mounted." };
        new_values["FilesystemLogsPathUsedINodes"] = { stat.f_files - stat.f_favail,
            "The number of used inodes on the volume where ClickHouse logs path is mounted." };
    }


    /// Free and total space on every configured disk.
    {
        DisksMap disks_map = getContext()->getDisksMap();
        for (const auto & [name, disk] : disks_map)
        {
            auto total = disk->getTotalSpace();

            /// Some disks don't support information about the space.
            if (total)
            {
                auto available = disk->getAvailableSpace();
                auto unreserved = disk->getUnreservedSpace();

                new_values[fmt::format("DiskTotal_{}", name)] = { *total,
                    "The total size in bytes of the disk (virtual filesystem). Remote filesystems may not provide this information and can show a large value like 16 EiB." };

                if (available)
                {
                    new_values[fmt::format("DiskUsed_{}", name)] = { *total - *available,
                        "Used bytes on the disk (virtual filesystem). Remote filesystems do not always provide this information." };

                    new_values[fmt::format("DiskAvailable_{}", name)] = { *available,
                        "Available bytes on the disk (virtual filesystem). Remote filesystems may not provide this information and can show a large value like 16 EiB." };
                }

                if (unreserved)
                    new_values[fmt::format("DiskUnreserved_{}", name)] = { *unreserved,
                        "Available bytes on the disk (virtual filesystem) without the reservations for merges, fetches, and moves. Remote filesystems may not provide this information and can show a large value like 16 EiB." };
            }

#if USE_AWS_S3
            if (auto s3_client = disk->tryGetS3StorageClient())
            {
                if (auto put_throttler = s3_client->getPutRequestThrottler())
                {
                    new_values[fmt::format("DiskPutObjectThrottlerRPS_{}", name)] = { put_throttler->getMaxSpeed(),
                        "PutObject Request throttling limit on the disk in requests per second (virtual filesystem). Local filesystems may not provide this information." };
                    new_values[fmt::format("DiskPutObjectThrottlerAvailable_{}", name)] = { put_throttler->getAvailable(),
                        "Number of PutObject requests that can be currently issued without hitting throttling limit on the disk (virtual filesystem). Local filesystems may not provide this information." };
                }
                if (auto get_throttler = s3_client->getGetRequestThrottler())
                {
                    new_values[fmt::format("DiskGetObjectThrottlerRPS_{}", name)] = { get_throttler->getMaxSpeed(),
                        "GetObject Request throttling limit on the disk in requests per second (virtual filesystem). Local filesystems may not provide this information." };
                    new_values[fmt::format("DiskGetObjectThrottlerAvailable_{}", name)] = { get_throttler->getAvailable(),
                        "Number of GetObject requests that can be currently issued without hitting throttling limit on the disk (virtual filesystem). Local filesystems may not provide this information." };
                }
            }
#endif
        }
    }

    {
        auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_remote_databases = false});

        size_t max_queue_size = 0;
        size_t max_inserts_in_queue = 0;
        size_t max_merges_in_queue = 0;

        size_t sum_queue_size = 0;
        size_t sum_inserts_in_queue = 0;
        size_t sum_merges_in_queue = 0;

        size_t max_absolute_delay = 0;
        size_t max_relative_delay = 0;

        size_t max_part_count_for_partition = 0;

        size_t number_of_databases = 0;
        for (const auto & [db_name, _] : databases)
            if (db_name != DatabaseCatalog::TEMPORARY_DATABASE)
                ++number_of_databases; /// filter out the internal database for temporary tables, system table "system.databases" behaves the same way

        size_t total_number_of_tables = 0;

        size_t total_number_of_bytes = 0;
        size_t total_number_of_rows = 0;
        size_t total_number_of_parts = 0;

        size_t total_number_of_tables_system = 0;

        size_t total_number_of_bytes_system = 0;
        size_t total_number_of_rows_system = 0;
        size_t total_number_of_parts_system = 0;

        size_t total_primary_key_bytes_memory = 0;
        size_t total_primary_key_bytes_memory_allocated = 0;
        size_t total_index_granularity_bytes_in_memory = 0;
        size_t total_index_granularity_bytes_in_memory_allocated = 0;

        size_t total_projection_primary_key_bytes_memory = 0;
        size_t total_projection_primary_key_bytes_memory_allocated = 0;
        size_t total_projection_index_granularity_bytes_in_memory = 0;
        size_t total_projection_index_granularity_bytes_in_memory_allocated = 0;

        for (const auto & db : databases)
        {
            /// Check if database can contain MergeTree tables
            if (db.second->isExternal())
                continue;

            bool is_system = db.first == DatabaseCatalog::SYSTEM_DATABASE;

            // Note that we skip not yet loaded tables, so metrics could possibly be lower than expected on fully loaded database just after server start if `async_load_databases = true`.
            for (auto iterator = db.second->getTablesIterator(getContext(), {}, /*skip_not_loaded=*/true); iterator->isValid(); iterator->next())
            {
                ++total_number_of_tables;
                if (is_system)
                    ++total_number_of_tables_system;

                const auto & table = iterator->table();
                if (!table)
                    continue;

                if (MergeTreeData * table_merge_tree = dynamic_cast<MergeTreeData *>(table.get()))
                {
                    calculateMax(max_part_count_for_partition, table_merge_tree->getMaxPartsCountAndSizeForPartition().first);

                    size_t bytes = table_merge_tree->totalBytes(getContext()).value();
                    size_t rows = table_merge_tree->totalRows(getContext()).value();
                    size_t parts = table_merge_tree->getActivePartsCount();

                    total_number_of_bytes += bytes;
                    total_number_of_rows += rows;
                    total_number_of_parts += parts;

                    if (is_system)
                    {
                        total_number_of_bytes_system += bytes;
                        total_number_of_rows_system += rows;
                        total_number_of_parts_system += parts;
                    }

                    // only fetch the parts which are in active state
                    auto all_parts = table_merge_tree->getDataPartsVectorForInternalUsage();

                    for (const auto & part : all_parts)
                    {
                        total_primary_key_bytes_memory += part->getIndexSizeInBytes();
                        total_primary_key_bytes_memory_allocated += part->getIndexSizeInAllocatedBytes();
                        total_index_granularity_bytes_in_memory += part->getIndexGranularityBytes();
                        total_index_granularity_bytes_in_memory_allocated += part->getIndexGranularityAllocatedBytes();

                        for (const auto & [_, proj_part] : part->getProjectionParts())
                        {
                            total_projection_primary_key_bytes_memory += proj_part->getIndexSizeInBytes();
                            total_projection_primary_key_bytes_memory_allocated += proj_part->getIndexSizeInAllocatedBytes();
                            total_projection_index_granularity_bytes_in_memory += proj_part->getIndexGranularityBytes();
                            total_projection_index_granularity_bytes_in_memory_allocated += proj_part->getIndexGranularityAllocatedBytes();
                        }
                    }
                }

                if (StorageReplicatedMergeTree * table_replicated_merge_tree = typeid_cast<StorageReplicatedMergeTree *>(table.get()))
                {
                    StorageReplicatedMergeTree::ReplicatedStatus status;
                    table_replicated_merge_tree->getStatus(status, false);

                    calculateMaxAndSum(max_queue_size, sum_queue_size, status.queue.queue_size);
                    calculateMaxAndSum(max_inserts_in_queue, sum_inserts_in_queue, status.queue.inserts_in_queue);
                    calculateMaxAndSum(max_merges_in_queue, sum_merges_in_queue, status.queue.merges_in_queue);

                    if (!status.is_readonly)
                    {
                        try
                        {
                            time_t absolute_delay = 0;
                            time_t relative_delay = 0;
                            table_replicated_merge_tree->getReplicaDelays(absolute_delay, relative_delay);

                            calculateMax(max_absolute_delay, absolute_delay);
                            calculateMax(max_relative_delay, relative_delay);
                        }
                        catch (...)
                        {
                            /// The table can transition to readonly between the `status.is_readonly`
                            /// check above and the call to `getReplicaDelays` (which calls
                            /// `assertNotReadonly` internally). This is a benign race for a
                            /// background metrics thread, so do not pollute the error log /
                            /// stderr with `TABLE_IS_READ_ONLY` exceptions caused by it.
                            auto level = getCurrentExceptionCode() == ErrorCodes::TABLE_IS_READ_ONLY
                                ? LogsLevel::debug
                                : LogsLevel::error;
                            tryLogCurrentException(__PRETTY_FUNCTION__,
                                "Cannot get replica delay for table: " + backQuoteIfNeed(db.first) + "." + backQuoteIfNeed(iterator->name()),
                                level);
                        }
                    }
                }
            }
        }

        new_values["ReplicasMaxQueueSize"] = { max_queue_size, "Maximum queue size (in the number of operations like get, merge) across Replicated tables." };
        new_values["ReplicasMaxInsertsInQueue"] = { max_inserts_in_queue, "Maximum number of INSERT operations in the queue (still to be replicated) across Replicated tables." };
        new_values["ReplicasMaxMergesInQueue"] = { max_merges_in_queue, "Maximum number of merge operations in the queue (still to be applied) across Replicated tables." };

        new_values["ReplicasSumQueueSize"] = { sum_queue_size, "Sum queue size (in the number of operations like get, merge) across Replicated tables." };
        new_values["ReplicasSumInsertsInQueue"] = { sum_inserts_in_queue, "Sum of INSERT operations in the queue (still to be replicated) across Replicated tables." };
        new_values["ReplicasSumMergesInQueue"] = { sum_merges_in_queue, "Sum of merge operations in the queue (still to be applied) across Replicated tables." };

        new_values["ReplicasMaxAbsoluteDelay"] = { max_absolute_delay, "Maximum difference in seconds between the most fresh replicated part and the most fresh data part still to be replicated, across Replicated tables. A very high value indicates a replica with no data." };
        new_values["ReplicasMaxRelativeDelay"] = { max_relative_delay, "Maximum difference between the replica delay and the delay of the most up-to-date replica of the same table, across Replicated tables." };

        new_values["MaxPartCountForPartition"] = { max_part_count_for_partition, "Maximum number of parts per partition across all partitions of all tables of MergeTree family. Values larger than 300 indicates misconfiguration, overload, or massive data loading." };

        new_values["NumberOfDatabases"] = { number_of_databases, "Total number of databases on the server." };
        new_values["NumberOfTables"] = { total_number_of_tables, "Total number of tables summed across the databases on the server, excluding the databases that cannot contain MergeTree tables."
            " The excluded database engines are those who generate the set of tables on the fly, like `Lazy`, `MySQL`, `PostgreSQL`, `SQlite`."};

        new_values["TotalBytesOfMergeTreeTables"] = { total_number_of_bytes, "Total amount of bytes (compressed, including data and indices) stored in all tables of MergeTree family." };
        new_values["TotalRowsOfMergeTreeTables"] = { total_number_of_rows, "Total amount of rows (records) stored in all tables of MergeTree family." };
        new_values["TotalPartsOfMergeTreeTables"] = { total_number_of_parts, "Total amount of data parts in all tables of MergeTree family."
            " Numbers larger than 10 000 will negatively affect the server startup time and it may indicate unreasonable choice of the partition key." };

        new_values["NumberOfTablesSystem"] = { total_number_of_tables_system, "Total number of tables in the system database on the server stored in tables of MergeTree family." };

        new_values["TotalBytesOfMergeTreeTablesSystem"] = { total_number_of_bytes_system, "Total amount of bytes (compressed, including data and indices) stored in tables of MergeTree family in the system database." };
        new_values["TotalRowsOfMergeTreeTablesSystem"] = { total_number_of_rows_system, "Total amount of rows (records) stored in tables of MergeTree family in the system database." };
        new_values["TotalPartsOfMergeTreeTablesSystem"] = { total_number_of_parts_system, "Total amount of data parts in tables of MergeTree family in the system database." };

        new_values["TotalPrimaryKeyBytesInMemory"] = { total_primary_key_bytes_memory, "The total amount of memory (in bytes) used by primary key values (only takes active parts into account)." };
        new_values["TotalPrimaryKeyBytesInMemoryAllocated"] = { total_primary_key_bytes_memory_allocated, "The total amount of memory (in bytes) reserved for primary key values (only takes active parts into account)." };
        new_values["TotalIndexGranularityBytesInMemory"] = { total_index_granularity_bytes_in_memory, "The total amount of memory (in bytes) used by index granules (only takes active parts into account)." };
        new_values["TotalIndexGranularityBytesInMemoryAllocated"] = { total_index_granularity_bytes_in_memory_allocated, "The total amount of memory (in bytes) reserved for index granules (only takes active parts into account)." };

        new_values["TotalProjectionPrimaryKeyBytesInMemory"] = { total_projection_primary_key_bytes_memory, "The total amount of memory (in bytes) used by projection primary key values (only takes active parts into account)." };
        new_values["TotalProjectionPrimaryKeyBytesInMemoryAllocated"] = { total_projection_primary_key_bytes_memory_allocated, "The total amount of memory (in bytes) reserved for projection primary key values (only takes active parts into account)." };
        new_values["TotalProjectionIndexGranularityBytesInMemory"] = { total_projection_index_granularity_bytes_in_memory, "The total amount of memory (in bytes) used by projection index granularity (only takes active parts into account)." };
        new_values["TotalProjectionIndexGranularityBytesInMemoryAllocated"] = { total_projection_index_granularity_bytes_in_memory_allocated, "The total amount of memory (in bytes) reserved for projection index granularity (only takes active parts into account)." };
    }

    {
        const auto user_info = getContext()->getProcessList().getUserInfo(true);
        size_t queries_memory_usage = 0;
        size_t queries_peak_memory_usage = 0;
        for (const auto & [user, info] : user_info)
        {
            queries_memory_usage += info.memory_usage;
            queries_peak_memory_usage += info.peak_memory_usage;
        }
        new_values["QueriesMemoryUsage"] = { queries_memory_usage,
            "Total memory currently used by all running queries on the server, in bytes."
            " Useful for attributing memory pressure to the concurrent query load." };
        new_values["QueriesPeakMemoryUsage"] = { queries_peak_memory_usage,
            "Sum of per-user query memory peaks across all users tracked in `ProcessList`, in bytes."
            " Each user's peak is the high-water mark of that user's memory tracker, which is reset when the user has no running queries."
            " This is therefore an aggregate of currently-tracked per-user peaks, not a single server-wide peak of all queries since startup." };
    }

    new_values["ZooKeeperClientLastZXIDSeen"] = { getContext()->getZooKeeperLastZXIDSeen(), "The last ZXID seen by the current ZooKeeper client session. This value increases monotonically as the client observes transactions from ZooKeeper."};

    {
        Float64 max_merge_elapsed = 0;
        for (const auto & merge : getContext()->getMergeList().get())
            max_merge_elapsed = std::max(merge.elapsed, max_merge_elapsed);
        new_values["LongestRunningMerge"]
            = {max_merge_elapsed, "Elapsed time in seconds of the longest currently running background merge."};
    }

#if USE_NURAFT
    {
        auto keeper_dispatcher = getContext()->tryGetKeeperDispatcher();
        if (keeper_dispatcher)
            updateKeeperInformation(*keeper_dispatcher, new_values);
    }
#endif

#if defined(OS_LINUX) && __has_include(<linux/sock_diag.h>)
    updateHTTPConnectionPoolTCPBufferMetrics(HTTPConnectionPools::instance().getSocketInodes(), new_values);
#endif

    if (update_heavy_metrics)
        updateHeavyMetricsIfNeeded(current_time, update_time, force_update, first_run, new_values);
}

void ServerAsynchronousMetrics::logImpl(AsynchronousMetricValues & new_values)
{
    /// Log the new metrics.
    if (auto asynchronous_metric_log = getContext()->getAsynchronousMetricLog())
        asynchronous_metric_log->addValues(new_values);
}

void ServerAsynchronousMetrics::updateMutationAndDetachedPartsStats()
{
    DetachedPartsStats current_values{};
    MutationStats current_mutation_stats{};

    for (const auto & db : DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_remote_databases = false}))
    {
        if (db.second->isExternal())
            continue;

        for (auto iterator = db.second->getTablesIterator(getContext(), {}, true); iterator->isValid(); iterator->next())
        {
            const auto & table = iterator->table();
            if (!table)
                continue;

            if (MergeTreeData * table_merge_tree = dynamic_cast<MergeTreeData *>(table.get()))
            {
                for (const auto & detached_part: table_merge_tree->getDetachedParts())
                {
                    if (!detached_part.valid_name)
                        continue;

                    if (detached_part.prefix.empty())
                        ++current_values.detached_by_user;

                    ++current_values.count;
                }

                // mutation status
                const auto max_pending_mutations_execution_time_sec = static_cast<std::chrono::seconds::rep>(getContext()->getMaxPendingMutationsExecutionTimeToWarn());
                for (const auto & mutation_status : table_merge_tree->getMutationsStatus())
                {
                    if (!mutation_status.is_done)
                    {
                        ++current_mutation_stats.pending_mutations;
                        // Check if the pending mutation is over the setting max_pending_mutations_execution_time_to_warn
                        // The aim here is to warn the user about mutations that are pending for a very long time (default is 24 hours)
                        {
                            if (!mutation_status.parts_to_do_names.empty())
                            {
                                auto mutation_create_time = std::chrono::system_clock::from_time_t(mutation_status.create_time);
                                auto current_time = std::chrono::system_clock::now();
                                const auto time_elapsed_sec = std::chrono::duration_cast<std::chrono::seconds>(current_time - mutation_create_time).count();

                                if (time_elapsed_sec > max_pending_mutations_execution_time_sec)
                                    ++current_mutation_stats.pending_mutations_over_execution_time;
                            }
                        }
                    }
                }
            }
        }
    }

    detached_parts_stats = current_values;
    mutation_stats = current_mutation_stats;
}

void ServerAsynchronousMetrics::updateThreadStackMetrics([[maybe_unused]] AsynchronousMetricValues & new_values)
{
#if defined(OS_LINUX)
    if (!vm_smaps)
        return;

    try
    {
        /// Snapshot the thread-stack registry once, then walk /proc/self/smaps
        /// and match each VMA's start address against the snapshot.
        ///
        /// /proc/self/smaps is a seq_file iterated across many read() syscalls,
        /// and the kernel releases mmap_lock between them. In a busy
        /// multi-threaded process, new VMAs from jemalloc chunks and new
        /// pthread stacks may appear during the walk and get appended to later
        /// records. By matching against the snapshot taken before the walk,
        /// the match count is bounded by the thread count at snapshot time
        /// regardless of churn. Stacks created during the walk are correctly
        /// ignored (their addresses aren't in the snapshot). Stacks that exit
        /// during the walk before the iterator reaches their address are
        /// undercounted; that residual error is the price of the snapshot.
        ///
        /// The parse is allocation-free: we walk the ReadBuffer's internal
        /// buffer byte-by-byte and accumulate the current line into a small
        /// stack buffer, never growing the heap.
        ///
        /// Format of /proc/PID/smaps is set by the kernel in
        /// fs/proc/task_mmu.c (functions `show_vma_header_prefix`,
        /// `show_smap`, `__show_smap`):
        ///   - The VMA header line begins with `<start>-<end>` written by
        ///     `seq_put_hex_ll(...)` — lowercase hex, '-' separator.
        ///   - Detail lines are written by `SEQ_PUT_DEC(str, val)` which is
        ///     `seq_put_decimal_ull_width(m, str, val >> 10, 8)`, so every
        ///     `Size:` / `Rss:` / `Pss:` / etc. value is an integer kB
        ///     right-aligned in width 8, followed by " kB\n".
        ///   - `Size:` is the first detail line after the header (set by
        ///     show_smap), `Rss:` is emitted a few lines later by
        ///     __show_smap. We don't depend on a specific relative order,
        ///     just that both appear before the next header.
        const auto stack_snapshot = ThreadStackRegistry::snapshot();

        vm_smaps->rewind();

        uint64_t stack_rss_kb = 0;
        uint64_t stack_size_kb = 0;
        uint64_t stack_count = 0;
        bool current_is_stack = false;

        constexpr size_t line_buf_size = 1024;
        char line_buf[line_buf_size];
        size_t line_len = 0;

        auto process_line = [&](const char * data, size_t len)
        {
            if (len == 0)
                return;
            char first = data[0];
            bool is_header = ((first >= '0' && first <= '9') || (first >= 'a' && first <= 'f'));
            if (is_header)
            {
                uintptr_t start = 0;
                bool parsed = false;
                for (size_t i = 0; i < len; ++i)
                {
                    char d = data[i];
                    if (d == '-')
                    {
                        parsed = true;
                        break;
                    }
                    if (d >= '0' && d <= '9')
                        start = (start << 4) | static_cast<uintptr_t>(d - '0');
                    else if (d >= 'a' && d <= 'f')
                        start = (start << 4) | static_cast<uintptr_t>(d - 'a' + 10);
                    else
                    {
                        parsed = false;
                        break;
                    }
                }
                current_is_stack = parsed && stack_snapshot.contains(start);
                if (current_is_stack)
                    ++stack_count;
            }
            else if (current_is_stack)
            {
                uint64_t * dest = nullptr;
                size_t value_offset = 0;
                if (len >= 4 && data[0] == 'R' && data[1] == 's' && data[2] == 's' && data[3] == ':')
                {
                    dest = &stack_rss_kb;
                    value_offset = 4;
                }
                else if (len >= 5 && data[0] == 'S' && data[1] == 'i' && data[2] == 'z' && data[3] == 'e' && data[4] == ':')
                {
                    dest = &stack_size_kb;
                    value_offset = 5;
                }
                if (dest)
                {
                    while (value_offset < len && data[value_offset] == ' ')
                        ++value_offset;
                    uint64_t value = 0;
                    while (value_offset < len && data[value_offset] >= '0' && data[value_offset] <= '9')
                    {
                        value = value * 10 + static_cast<uint64_t>(data[value_offset] - '0');
                        ++value_offset;
                    }
                    *dest += value;
                }
            }
        };

        while (!vm_smaps->eof())
        {
            char c = *vm_smaps->position();
            ++vm_smaps->position();
            if (c == '\n')
            {
                process_line(line_buf, line_len);
                line_len = 0;
            }
            else if (line_len < line_buf_size)
            {
                line_buf[line_len++] = c;
            }
            /// Lines longer than line_buf_size get truncated. We only need
            /// the first few bytes for either the header start address or
            /// the metric key + value, so truncation is harmless for smaps.
        }
        if (line_len > 0)
            process_line(line_buf, line_len);

        /// stack_rss_kb and stack_size_kb are in integer kB (the kernel's
        /// unit in /proc/self/smaps); multiply by 1024 to expose bytes.
        new_values["MemoryThreadStacksResident"] = { stack_rss_kb * 1024,
            "Approximate resident set size of pthread stacks, summed from `Rss:`"
            " of /proc/self/smaps VMAs whose start address matches the snapshot"
            " of currently-registered thread stack bases at the start of the"
            " scrape. Updated on the heavy-metrics cadence. May slightly"
            " undercount under heavy thread churn (Linux only)." };
        new_values["MemoryThreadStacksVirtual"] = { stack_size_kb * 1024,
            "Approximate virtual size of pthread stacks, summed from `Size:`"
            " of matching /proc/self/smaps VMAs. Updated on the heavy-metrics"
            " cadence (Linux only)." };
        new_values["MemoryThreadStacksCount"] = { stack_count,
            "Number of pthread stack VMAs matched in /proc/self/smaps against"
            " the snapshot taken at the start of the scrape. Updated on the"
            " heavy-metrics cadence (Linux only)." };
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        try { vm_smaps.emplace("/proc/self/smaps"); }
        catch (...) { vm_smaps.reset(); } /// Ok, re-open failed; disable on next call.
    }
#endif
}

void ServerAsynchronousMetrics::updateHeavyMetricsIfNeeded(TimePoint current_time, TimePoint update_time, bool force_update, bool first_run, AsynchronousMetricValues & new_values)
{
    const auto time_since_previous_update = current_time - heavy_metric_previous_update_time;
    const bool need_update_heavy_metrics = (time_since_previous_update >= heavy_metric_update_period) || force_update || first_run;

    Stopwatch watch;
    if (need_update_heavy_metrics)
    {
        heavy_metric_previous_update_time = update_time;
        if (first_run)
            heavy_update_interval = static_cast<double>(heavy_metric_update_period.count());
        else
            heavy_update_interval = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(time_since_previous_update).count()) / 1e6;

        /// Test shows that listing 100000 entries consuming around 0.15 sec.
        updateMutationAndDetachedPartsStats();

        /// /proc/self/smaps is gated here because it forces the kernel to
        /// walk page tables for every VMA of the process.
        updateThreadStackMetrics(new_values);

        watch.stop();

        /// Normally heavy metrics don't delay the rest of the metrics calculation
        /// otherwise log the warning message
        auto log_level = std::make_pair(DB::LogsLevel::trace, Poco::Message::PRIO_TRACE);
        if (watch.elapsedSeconds() > (static_cast<double>(update_period.count()) / 2.))
            log_level = std::make_pair(DB::LogsLevel::debug, Poco::Message::PRIO_DEBUG);
        else if (watch.elapsedSeconds() > (static_cast<double>(update_period.count()) / 4. * 3))
            log_level = std::make_pair(DB::LogsLevel::warning, Poco::Message::PRIO_WARNING);
        LOG_IMPL(log, log_level.first, log_level.second,
                 "Update heavy metrics. "
                 "Update period {} sec. "
                 "Update heavy metrics period {} sec. "
                 "Heavy metrics calculation elapsed: {} sec.",
                 update_period.count(),
                 heavy_metric_update_period.count(),
                 watch.elapsedSeconds());

    }

    {
        Duration max_update_delay{0};
        size_t failed_counter = 0;
        const auto & external_dictionaries = getContext()->getExternalDictionariesLoader();

        for (const auto & load_result : external_dictionaries.getLoadResults())
        {
            if (load_result.error_count > 0 && load_result.last_successful_update_time.time_since_epoch().count() > 0)
            {
                max_update_delay = std::max(max_update_delay, std::chrono::duration_cast<Duration>(current_time - load_result.last_successful_update_time));
            }
            failed_counter += load_result.error_count;
        }
        new_values["DictionaryMaxUpdateDelay"] = {
            std::chrono::duration_cast<std::chrono::seconds>(max_update_delay).count(), "The maximum delay (in seconds) of dictionary update"};
        new_values["DictionaryTotalFailedUpdates"] = {failed_counter, "Number of errors since last successful loading in all dictionaries."};
    }

    new_values["AsynchronousHeavyMetricsCalculationTimeSpent"] = { watch.elapsedSeconds(), "Time in seconds spent for calculation of asynchronous heavy (tables related) metrics (this is the overhead of asynchronous metrics)." };

    new_values["AsynchronousHeavyMetricsUpdateInterval"] = { heavy_update_interval, "Heavy (tables related) metrics update interval" };

    new_values["NumberOfDetachedParts"] = { detached_parts_stats.count, "The total number of parts detached from MergeTree tables. A part can be detached by a user with the `ALTER TABLE DETACH` query or by the server itself it the part is broken, unexpected or unneeded. The server does not care about detached parts and they can be removed." };
    new_values["NumberOfDetachedByUserParts"] = { detached_parts_stats.detached_by_user, "The total number of parts detached from MergeTree tables by users with the `ALTER TABLE DETACH` query (as opposed to unexpected, broken or ignored parts). The server does not care about detached parts and they can be removed." };
    new_values["NumberOfPendingMutations"] = { mutation_stats.pending_mutations, "The total number of mutations that are in left to be mutated." };
    new_values["NumberOfPendingMutationsOverExecutionTime"] = { mutation_stats.pending_mutations_over_execution_time, "The total number of mutations which have data part left to be mutated over the specified max_pending_mutations_execution_time_to_warn setting." };
}

}
