#include <Interpreters/AsynchronousMetrics.h>
#include <Interpreters/AsynchronousMetricLog.h>
#include <Interpreters/ExpressionJIT.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Common/CurrentMetrics.h>
#include <Common/typeid_cast.h>
#include <Storages/MarkCache.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <IO/UncompressedCache.h>
#include <Databases/IDatabase.h>
#include <chrono>


#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_JEMALLOC
#    include <jemalloc/jemalloc.h>
#endif


namespace CurrentMetrics
{
    extern const Metric MemoryTracking;
}


namespace DB
{

AsynchronousMetrics::~AsynchronousMetrics()
{
    try
    {
        {
            std::lock_guard lock{mutex};
            quit = true;
        }

        wait_cond.notify_one();
        thread.join();
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


AsynchronousMetricValues AsynchronousMetrics::getValues() const
{
    std::lock_guard lock{mutex};
    return values;
}


void AsynchronousMetrics::run()
{
    setThreadName("AsyncMetrics");

    /// Next minute + 30 seconds. To be distant with moment of transmission of metrics, see MetricsTransmitter.
    const auto get_next_minute = []
    {
        return std::chrono::time_point_cast<std::chrono::minutes, std::chrono::system_clock>(
            std::chrono::system_clock::now() + std::chrono::minutes(1)) + std::chrono::seconds(30);
    };

    while (true)
    {
        try
        {
            update();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        std::unique_lock lock{mutex};
        if (wait_cond.wait_until(lock, get_next_minute(), [this] { return quit; }))
            break;
    }
}


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


void AsynchronousMetrics::update()
{
    AsynchronousMetricValues new_values;

    {
        if (auto mark_cache = context.getMarkCache())
        {
            new_values["MarkCacheBytes"] = mark_cache->weight();
            new_values["MarkCacheFiles"] = mark_cache->count();
        }
    }

    {
        if (auto uncompressed_cache = context.getUncompressedCache())
        {
            new_values["UncompressedCacheBytes"] = uncompressed_cache->weight();
            new_values["UncompressedCacheCells"] = uncompressed_cache->count();
        }
    }

#if USE_EMBEDDED_COMPILER
    {
        if (auto compiled_expression_cache = context.getCompiledExpressionCache())
            new_values["CompiledExpressionCacheCount"]  = compiled_expression_cache->count();
    }
#endif

    new_values["Uptime"] = context.getUptimeSeconds();

    /// Process memory usage according to OS
#if defined(OS_LINUX)
    {
        MemoryStatisticsOS::Data data = memory_stat.get();

        new_values["MemoryVirtual"] = data.virt;
        new_values["MemoryResident"] = data.resident;
        new_values["MemoryShared"] = data.shared;
        new_values["MemoryCode"] = data.code;
        new_values["MemoryDataAndStack"] = data.data_and_stack;

        /// We must update the value of total_memory_tracker periodically.
        /// Otherwise it might be calculated incorrectly - it can include a "drift" of memory amount.
        /// See https://github.com/ClickHouse/ClickHouse/issues/10293
        total_memory_tracker.set(data.resident);
        CurrentMetrics::set(CurrentMetrics::MemoryTracking, data.resident);
    }
#endif

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

        for (const auto & db : databases)
        {
            /// Lazy database can not contain MergeTree tables
            if (db.second->getEngineName() == "Lazy")
                continue;
            for (auto iterator = db.second->getTablesIterator(context); iterator->isValid(); iterator->next())
            {
                ++total_number_of_tables;
                const auto & table = iterator->table();
                if (!table)
                    continue;

                StorageMergeTree * table_merge_tree = dynamic_cast<StorageMergeTree *>(table.get());
                StorageReplicatedMergeTree * table_replicated_merge_tree = dynamic_cast<StorageReplicatedMergeTree *>(table.get());

                if (table_replicated_merge_tree)
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

                    calculateMax(max_part_count_for_partition, table_replicated_merge_tree->getMaxPartsCountForPartition());
                }

                if (table_merge_tree)
                {
                    calculateMax(max_part_count_for_partition, table_merge_tree->getMaxPartsCountForPartition());
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
    }

#if USE_JEMALLOC && JEMALLOC_VERSION_MAJOR >= 4
    {
#    define FOR_EACH_METRIC(M) \
        M("allocated", size_t) \
        M("active", size_t) \
        M("metadata", size_t) \
        M("metadata_thp", size_t) \
        M("resident", size_t) \
        M("mapped", size_t) \
        M("retained", size_t) \
        M("background_thread.num_threads", size_t) \
        M("background_thread.num_runs", uint64_t) \
        M("background_thread.run_interval", uint64_t)

#    define GET_METRIC(NAME, TYPE) \
        do \
        { \
            TYPE value{}; \
            size_t size = sizeof(value); \
            mallctl("stats." NAME, &value, &size, nullptr, 0); \
            new_values["jemalloc." NAME] = value; \
        } while (false);

        FOR_EACH_METRIC(GET_METRIC)

#    undef GET_METRIC
#    undef FOR_EACH_METRIC
    }
#endif

    /// Add more metrics as you wish.

    // Log the new metrics.
    if (auto log = context.getAsynchronousMetricLog())
    {
        log->addValues(new_values);
    }

    // Finally, update the current metrics.
    std::lock_guard lock(mutex);
    values = new_values;
}

}
