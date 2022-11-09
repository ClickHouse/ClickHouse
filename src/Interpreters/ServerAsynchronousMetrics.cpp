#include <Interpreters/ServerAsynchronousMetrics.h>

#include <Interpreters/Aggregator.h>
#include <Interpreters/AsynchronousMetricLog.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Context.h>

#include <Databases/IDatabase.h>

#include <IO/UncompressedCache.h>
#include <IO/MMappedFileCache.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeMetadataCache.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MarkCache.h>

#include <Coordination/KeeperAsynchronousMetrics.h>

namespace DB
{

namespace
{

template <typename Max, typename T>
static void calculateMax(Max & max, T x)
{
    if (Max(x) > max)
        max = x;
}

template <typename Max, typename Sum, typename T>
static void calculateMaxAndSum(Max & max, Sum & sum, T x)
{
    sum += x;
    if (Max(x) > max)
        max = x;
}

}

ServerAsynchronousMetrics::ServerAsynchronousMetrics(
    ContextPtr global_context_,
    int update_period_seconds,
    int heavy_metrics_update_period_seconds,
    const ProtocolServerMetricsFunc & protocol_server_metrics_func_)
    : AsynchronousMetrics(update_period_seconds, protocol_server_metrics_func_)
    , WithContext(global_context_)
    , heavy_metric_update_period(heavy_metrics_update_period_seconds)
{}
 
void ServerAsynchronousMetrics::updateImpl(AsynchronousMetricValues & new_values)
{
    if (auto mark_cache = getContext()->getMarkCache())
    {
        new_values["MarkCacheBytes"] = mark_cache->weight();
        new_values["MarkCacheFiles"] = mark_cache->count();
    }

    if (auto uncompressed_cache = getContext()->getUncompressedCache())
    {
        new_values["UncompressedCacheBytes"] = uncompressed_cache->weight();
        new_values["UncompressedCacheCells"] = uncompressed_cache->count();
    }

    if (auto index_mark_cache = getContext()->getIndexMarkCache())
    {
        new_values["IndexMarkCacheBytes"] = index_mark_cache->weight();
        new_values["IndexMarkCacheFiles"] = index_mark_cache->count();
    }

    if (auto index_uncompressed_cache = getContext()->getIndexUncompressedCache())
    {
        new_values["IndexUncompressedCacheBytes"] = index_uncompressed_cache->weight();
        new_values["IndexUncompressedCacheCells"] = index_uncompressed_cache->count();
    }

    if (auto mmap_cache = getContext()->getMMappedFileCache())
    {
        new_values["MMapCacheCells"] = mmap_cache->count();
    }

    {
        auto caches = FileCacheFactory::instance().getAll();
        for (const auto & [_, cache_data] : caches)
        {
            new_values["FilesystemCacheBytes"] = cache_data->cache->getUsedCacheSize();
            new_values["FilesystemCacheFiles"] = cache_data->cache->getFileSegmentsNum();
        }
    }

#if USE_ROCKSDB
    if (auto metadata_cache = getContext()->tryGetMergeTreeMetadataCache())
    {
        new_values["MergeTreeMetadataCacheSize"] = metadata_cache->getEstimateNumKeys();
    }
#endif

#if USE_EMBEDDED_COMPILER
    if (auto * compiled_expression_cache = CompiledExpressionCacheFactory::instance().tryGetCache())
    {
        new_values["CompiledExpressionCacheBytes"] = compiled_expression_cache->weight();
        new_values["CompiledExpressionCacheCount"]  = compiled_expression_cache->count();
    }
#endif


    new_values["Uptime"] = getContext()->getUptimeSeconds();

    if (const auto stats = getHashTablesCacheStatistics())
    {
        new_values["HashTableStatsCacheEntries"] = stats->entries;
        new_values["HashTableStatsCacheHits"] = stats->hits;
        new_values["HashTableStatsCacheMisses"] = stats->misses;
    }

    /// Free space in filesystems at data path and logs path.
    {
        auto stat = getStatVFS(getContext()->getPath());

        new_values["FilesystemMainPathTotalBytes"] = stat.f_blocks * stat.f_frsize;
        new_values["FilesystemMainPathAvailableBytes"] = stat.f_bavail * stat.f_frsize;
        new_values["FilesystemMainPathUsedBytes"] = (stat.f_blocks - stat.f_bavail) * stat.f_frsize;
        new_values["FilesystemMainPathTotalINodes"] = stat.f_files;
        new_values["FilesystemMainPathAvailableINodes"] = stat.f_favail;
        new_values["FilesystemMainPathUsedINodes"] = stat.f_files - stat.f_favail;
    }

    {
        /// Current working directory of the server is the directory with logs.
        auto stat = getStatVFS(".");

        new_values["FilesystemLogsPathTotalBytes"] = stat.f_blocks * stat.f_frsize;
        new_values["FilesystemLogsPathAvailableBytes"] = stat.f_bavail * stat.f_frsize;
        new_values["FilesystemLogsPathUsedBytes"] = (stat.f_blocks - stat.f_bavail) * stat.f_frsize;
        new_values["FilesystemLogsPathTotalINodes"] = stat.f_files;
        new_values["FilesystemLogsPathAvailableINodes"] = stat.f_favail;
        new_values["FilesystemLogsPathUsedINodes"] = stat.f_files - stat.f_favail;
    }

    /// Free and total space on every configured disk.
    {
        DisksMap disks_map = getContext()->getDisksMap();
        for (const auto & [name, disk] : disks_map)
        {
            auto total = disk->getTotalSpace();

            /// Some disks don't support information about the space.
            if (!total)
                continue;

            auto available = disk->getAvailableSpace();
            auto unreserved = disk->getUnreservedSpace();

            new_values[fmt::format("DiskTotal_{}", name)] = total;
            new_values[fmt::format("DiskUsed_{}", name)] = total - available;
            new_values[fmt::format("DiskAvailable_{}", name)] = available;
            new_values[fmt::format("DiskUnreserved_{}", name)] = unreserved;
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

        size_t number_of_databases = databases.size();
        size_t total_number_of_tables = 0;

        size_t total_number_of_bytes = 0;
        size_t total_number_of_rows = 0;
        size_t total_number_of_parts = 0;

        for (const auto & db : databases)
        {
            /// Check if database can contain MergeTree tables
            if (!db.second->canContainMergeTreeTables())
                continue;

            for (auto iterator = db.second->getTablesIterator(getContext()); iterator->isValid(); iterator->next())
            {
                ++total_number_of_tables;
                const auto & table = iterator->table();
                if (!table)
                    continue;

                if (MergeTreeData * table_merge_tree = dynamic_cast<MergeTreeData *>(table.get()))
                {
                    const auto & settings = getContext()->getSettingsRef();

                    calculateMax(max_part_count_for_partition, table_merge_tree->getMaxPartsCountAndSizeForPartition().first);
                    total_number_of_bytes += table_merge_tree->totalBytes(settings).value();
                    total_number_of_rows += table_merge_tree->totalRows(settings).value();
                    total_number_of_parts += table_merge_tree->getPartsCount();
                }

                if (StorageReplicatedMergeTree * table_replicated_merge_tree = typeid_cast<StorageReplicatedMergeTree *>(table.get()))
                {
                    StorageReplicatedMergeTree::Status status;
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

        new_values["ReplicasMaxQueueSize"] = max_queue_size;
        new_values["ReplicasMaxInsertsInQueue"] = max_inserts_in_queue;
        new_values["ReplicasMaxMergesInQueue"] = max_merges_in_queue;

        new_values["ReplicasSumQueueSize"] = sum_queue_size;
        new_values["ReplicasSumInsertsInQueue"] = sum_inserts_in_queue;
        new_values["ReplicasSumMergesInQueue"] = sum_merges_in_queue;

        new_values["ReplicasMaxAbsoluteDelay"] = max_absolute_delay;
        new_values["ReplicasMaxRelativeDelay"] = max_relative_delay;

        new_values["MaxPartCountForPartition"] = max_part_count_for_partition;

        new_values["NumberOfDatabases"] = number_of_databases;
        new_values["NumberOfTables"] = total_number_of_tables;

        new_values["TotalBytesOfMergeTreeTables"] = total_number_of_bytes;
        new_values["TotalRowsOfMergeTreeTables"] = total_number_of_rows;
        new_values["TotalPartsOfMergeTreeTables"] = total_number_of_parts;
    }

#if USE_NURAFT
    {
        auto keeper_dispatcher = getContext()->tryGetKeeperDispatcher();
        if (keeper_dispatcher)
            updateKeeperInformation(*keeper_dispatcher, new_values);
    }
#endif
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

        for (auto iterator = db.second->getTablesIterator(getContext()); iterator->isValid(); iterator->next())
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

void ServerAsynchronousMetrics::updateHeavyMetricsIfNeeded(TimePoint current_time, TimePoint update_time, AsynchronousMetricValues & new_values)
{
    const auto time_after_previous_update = current_time - heavy_metric_previous_update_time;
    const bool update_heavy_metric = time_after_previous_update >= heavy_metric_update_period || first_run;

    if (update_heavy_metric)
    {
        heavy_metric_previous_update_time = update_time;

        Stopwatch watch;

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

    new_values["NumberOfDetachedParts"] = detached_parts_stats.count;
    new_values["NumberOfDetachedByUserParts"] = detached_parts_stats.detached_by_user;
}

}
