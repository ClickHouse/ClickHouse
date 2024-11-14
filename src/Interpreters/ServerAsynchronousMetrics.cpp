#include <Interpreters/ServerAsynchronousMetrics.h>

#include <Interpreters/Aggregator.h>
#include <Interpreters/AsynchronousMetricLog.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cache/QueryCache.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>

#include <Common/PageCache.h>

#include <Databases/IDatabase.h>

#include <IO/UncompressedCache.h>
#include <IO/MMappedFileCache.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MarkCache.h>

#include <Coordination/KeeperAsynchronousMetrics.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SETTING_VALUE;
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
    , AsynchronousMetrics(update_period_seconds, protocol_server_metrics_func_, update_jemalloc_epoch_, update_rss_)
    , update_heavy_metrics(update_heavy_metrics_)
    , heavy_metric_update_period(heavy_metrics_update_period_seconds)
{
    /// sanity check
    if (update_period_seconds == 0 || heavy_metrics_update_period_seconds == 0)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Setting asynchronous_metrics_update_period_s and asynchronous_heavy_metrics_update_period_s must not be zero");
}

ServerAsynchronousMetrics::~ServerAsynchronousMetrics()
{
    /// NOTE: stop() from base class is not enough, since this leads to leak on vptr
    stop();
}

void ServerAsynchronousMetrics::updateImpl(TimePoint update_time, TimePoint current_time, bool force_update, bool first_run, AsynchronousMetricValues & new_values)
{
    if (auto mark_cache = getContext()->getMarkCache())
    {
        new_values["MarkCacheBytes"] = { mark_cache->sizeInBytes(), "Total size of mark cache in bytes" };
        new_values["MarkCacheFiles"] = { mark_cache->count(), "Total number of mark files cached in the mark cache" };
    }

    if (auto page_cache = getContext()->getPageCache())
    {
        auto rss = page_cache->getResidentSetSize();
        new_values["PageCacheBytes"] = { rss.page_cache_rss, "Userspace page cache memory usage in bytes" };
        new_values["PageCachePinnedBytes"] = { page_cache->getPinnedSize(), "Userspace page cache memory that's currently in use and can't be evicted" };

        if (rss.unreclaimable_rss.has_value())
            new_values["UnreclaimableRSS"] = { *rss.unreclaimable_rss, "The amount of physical memory used by the server process, in bytes, excluding memory reclaimable by the OS (MADV_FREE)" };
    }

    if (auto uncompressed_cache = getContext()->getUncompressedCache())
    {
        new_values["UncompressedCacheBytes"] = { uncompressed_cache->sizeInBytes(),
            "Total size of uncompressed cache in bytes. Uncompressed cache does not usually improve the performance and should be mostly avoided." };
        new_values["UncompressedCacheCells"] = { uncompressed_cache->count(),
            "Total number of entries in the uncompressed cache. Each entry represents a decompressed block of data. Uncompressed cache does not usually improve performance and should be mostly avoided." };
    }

    if (auto index_mark_cache = getContext()->getIndexMarkCache())
    {
        new_values["IndexMarkCacheBytes"] = { index_mark_cache->sizeInBytes(), "Total size of mark cache for secondary indices in bytes." };
        new_values["IndexMarkCacheFiles"] = { index_mark_cache->count(), "Total number of mark files cached in the mark cache for secondary indices." };
    }

    if (auto index_uncompressed_cache = getContext()->getIndexUncompressedCache())
    {
        new_values["IndexUncompressedCacheBytes"] = { index_uncompressed_cache->sizeInBytes(),
            "Total size of uncompressed cache in bytes for secondary indices. Uncompressed cache does not usually improve the performance and should be mostly avoided." };
        new_values["IndexUncompressedCacheCells"] = { index_uncompressed_cache->count(),
            "Total number of entries in the uncompressed cache for secondary indices. Each entry represents a decompressed block of data. Uncompressed cache does not usually improve performance and should be mostly avoided." };
    }

    if (auto mmap_cache = getContext()->getMMappedFileCache())
    {
        new_values["MMapCacheCells"] = { mmap_cache->count(),
            "The number of files opened with `mmap` (mapped in memory)."
            " This is used for queries with the setting `local_filesystem_read_method` set to  `mmap`."
            " The files opened with `mmap` are kept in the cache to avoid costly TLB flushes."};
    }

    if (auto query_cache = getContext()->getQueryCache())
    {
        new_values["QueryCacheBytes"] = { query_cache->sizeInBytes(), "Total size of the query cache in bytes." };
        new_values["QueryCacheEntries"] = { query_cache->count(), "Total number of entries in the query cache." };
    }

    {
        auto caches = FileCacheFactory::instance().getAll();
        size_t total_bytes = 0;
        size_t total_files = 0;

        for (const auto & [_, cache_data] : caches)
        {
            total_bytes += cache_data->cache->getUsedCacheSize();
            total_files += cache_data->cache->getFileSegmentsNum();
        }

        new_values["FilesystemCacheBytes"] = { total_bytes,
            "Total bytes in the `cache` virtual filesystem. This cache is hold on disk." };
        new_values["FilesystemCacheFiles"] = { total_files,
            "Total number of cached file segments in the `cache` virtual filesystem. This cache is hold on disk." };
    }

#if USE_EMBEDDED_COMPILER
    if (auto * compiled_expression_cache = CompiledExpressionCacheFactory::instance().tryGetCache())
    {
        new_values["CompiledExpressionCacheBytes"] = { compiled_expression_cache->sizeInBytes(),
            "Total bytes used for the cache of JIT-compiled code." };
        new_values["CompiledExpressionCacheCount"] = { compiled_expression_cache->count(),
            "Total entries in the cache of JIT-compiled code." };
    }
#endif

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
                    "The total size in bytes of the disk (virtual filesystem). Remote filesystems may not provide this information." };

                if (available)
                {
                    new_values[fmt::format("DiskUsed_{}", name)] = { *total - *available,
                        "Used bytes on the disk (virtual filesystem). Remote filesystems not always provide this information." };

                    new_values[fmt::format("DiskAvailable_{}", name)] = { *available,
                        "Available bytes on the disk (virtual filesystem). Remote filesystems may not provide this information." };
                }

                if (unreserved)
                    new_values[fmt::format("DiskUnreserved_{}", name)] = { *unreserved,
                        "Available bytes on the disk (virtual filesystem) without the reservations for merges, fetches, and moves. Remote filesystems may not provide this information." };
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
        auto databases = DatabaseCatalog::instance().getDatabases();

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

        for (const auto & db : databases)
        {
            /// Check if database can contain MergeTree tables
            if (!db.second->canContainMergeTreeTables())
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
                    const auto & settings = getContext()->getSettingsRef();

                    calculateMax(max_part_count_for_partition, table_merge_tree->getMaxPartsCountAndSizeForPartition().first);

                    size_t bytes = table_merge_tree->totalBytes(settings).value();
                    size_t rows = table_merge_tree->totalRows(settings).value();
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
                    }
                }

                if (StorageReplicatedMergeTree * table_replicated_merge_tree = typeid_cast<StorageReplicatedMergeTree *>(table.get()))
                {
                    ReplicatedTableStatus status;
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
                            tryLogCurrentException(__PRETTY_FUNCTION__,
                                "Cannot get replica delay for table: " + backQuoteIfNeed(db.first) + "." + backQuoteIfNeed(iterator->name()));
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
    }

#if USE_NURAFT
    {
        auto keeper_dispatcher = getContext()->tryGetKeeperDispatcher();
        if (keeper_dispatcher)
            updateKeeperInformation(*keeper_dispatcher, new_values);
    }
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

void ServerAsynchronousMetrics::updateDetachedPartsStats()
{
    DetachedPartsStats current_values{};

    for (const auto & db : DatabaseCatalog::instance().getDatabases())
    {
        if (!db.second->canContainMergeTreeTables())
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
            }
        }
    }

    detached_parts_stats = current_values;
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
            heavy_update_interval = heavy_metric_update_period.count();
        else
            heavy_update_interval = std::chrono::duration_cast<std::chrono::microseconds>(time_since_previous_update).count() / 1e6;

        /// Test shows that listing 100000 entries consuming around 0.15 sec.
        updateDetachedPartsStats();

        watch.stop();

        /// Normally heavy metrics don't delay the rest of the metrics calculation
        /// otherwise log the warning message
        auto log_level = std::make_pair(DB::LogsLevel::trace, Poco::Message::PRIO_TRACE);
        if (watch.elapsedSeconds() > (update_period.count() / 2.))
            log_level = std::make_pair(DB::LogsLevel::debug, Poco::Message::PRIO_DEBUG);
        else if (watch.elapsedSeconds() > (update_period.count() / 4. * 3))
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
    new_values["AsynchronousHeavyMetricsCalculationTimeSpent"] = { watch.elapsedSeconds(), "Time in seconds spent for calculation of asynchronous heavy (tables related) metrics (this is the overhead of asynchronous metrics)." };

    new_values["AsynchronousHeavyMetricsUpdateInterval"] = { heavy_update_interval, "Heavy (tables related) metrics update interval" };

    new_values["NumberOfDetachedParts"] = { detached_parts_stats.count, "The total number of parts detached from MergeTree tables. A part can be detached by a user with the `ALTER TABLE DETACH` query or by the server itself it the part is broken, unexpected or unneeded. The server does not care about detached parts and they can be removed." };
    new_values["NumberOfDetachedByUserParts"] = { detached_parts_stats.detached_by_user, "The total number of parts detached from MergeTree tables by users with the `ALTER TABLE DETACH` query (as opposed to unexpected, broken or ignored parts). The server does not care about detached parts and they can be removed." };
}

}
