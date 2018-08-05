#include <Interpreters/AsynchronousMetrics.h>
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

#if __has_include(<common/config_common.h>)
#include <common/config_common.h>
#endif

#if USE_TCMALLOC
    #include <gperftools/malloc_extension.h> // Y_IGNORE

    /// Initializing malloc extension in global constructor as required.
    struct MallocExtensionInitializer
    {
        MallocExtensionInitializer()
        {
            MallocExtension::Initialize();
        }
    } malloc_extension_initializer;
#endif

#if USE_JEMALLOC
    #include <jemalloc/jemalloc.h>
#endif


namespace DB
{

AsynchronousMetrics::~AsynchronousMetrics()
{
    try
    {
        {
            std::lock_guard<std::mutex> lock{wait_mutex};
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


AsynchronousMetrics::Container AsynchronousMetrics::getValues() const
{
    std::lock_guard<std::mutex> lock{container_mutex};
    return container;
}


void AsynchronousMetrics::set(const std::string & name, Value value)
{
    std::lock_guard<std::mutex> lock{container_mutex};
    container[name] = value;
}


void AsynchronousMetrics::run()
{
    setThreadName("AsyncMetrics");

    std::unique_lock<std::mutex> lock{wait_mutex};

    /// Next minute + 30 seconds. To be distant with moment of transmission of metrics, see MetricsTransmitter.
    const auto get_next_minute = []
    {
        return std::chrono::time_point_cast<std::chrono::minutes, std::chrono::system_clock>(
            std::chrono::system_clock::now() + std::chrono::minutes(1)) + std::chrono::seconds(30);
    };

    while (true)
    {
        if (wait_cond.wait_until(lock, get_next_minute(), [this] { return quit; }))
            break;

        try
        {
            update();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
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
    {
        if (auto mark_cache = context.getMarkCache())
        {
            set("MarkCacheBytes", mark_cache->weight());
            set("MarkCacheFiles", mark_cache->count());
        }
    }

    {
        if (auto uncompressed_cache = context.getUncompressedCache())
        {
            set("UncompressedCacheBytes", uncompressed_cache->weight());
            set("UncompressedCacheCells", uncompressed_cache->count());
        }
    }

    set("Uptime", context.getUptimeSeconds());

    {
        auto databases = context.getDatabases();

        size_t max_queue_size = 0;
        size_t max_inserts_in_queue = 0;
        size_t max_merges_in_queue = 0;

        size_t sum_queue_size = 0;
        size_t sum_inserts_in_queue = 0;
        size_t sum_merges_in_queue = 0;

        size_t max_absolute_delay = 0;
        size_t max_relative_delay = 0;

        size_t max_part_count_for_partition = 0;

        for (const auto & db : databases)
        {
            for (auto iterator = db.second->getIterator(context); iterator->isValid(); iterator->next())
            {
                auto & table = iterator->table();
                StorageMergeTree * table_merge_tree = dynamic_cast<StorageMergeTree *>(table.get());
                StorageReplicatedMergeTree * table_replicated_merge_tree = dynamic_cast<StorageReplicatedMergeTree *>(table.get());

                if (table_replicated_merge_tree)
                {
                    StorageReplicatedMergeTree::Status status;
                    table_replicated_merge_tree->getStatus(status, false);

                    calculateMaxAndSum(max_queue_size, sum_queue_size, status.queue.queue_size);
                    calculateMaxAndSum(max_inserts_in_queue, sum_inserts_in_queue, status.queue.inserts_in_queue);
                    calculateMaxAndSum(max_merges_in_queue, sum_merges_in_queue, status.queue.merges_in_queue);

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

                    calculateMax(max_part_count_for_partition, table_replicated_merge_tree->getData().getMaxPartsCountForPartition());
                }

                if (table_merge_tree)
                {
                    calculateMax(max_part_count_for_partition, table_merge_tree->getData().getMaxPartsCountForPartition());
                }
            }
        }

        set("ReplicasMaxQueueSize", max_queue_size);
        set("ReplicasMaxInsertsInQueue", max_inserts_in_queue);
        set("ReplicasMaxMergesInQueue", max_merges_in_queue);

        set("ReplicasSumQueueSize", sum_queue_size);
        set("ReplicasSumInsertsInQueue", sum_inserts_in_queue);
        set("ReplicasSumMergesInQueue", sum_merges_in_queue);

        set("ReplicasMaxAbsoluteDelay", max_absolute_delay);
        set("ReplicasMaxRelativeDelay", max_relative_delay);

        set("MaxPartCountForPartition", max_part_count_for_partition);
    }

#if USE_TCMALLOC
    {
        /// tcmalloc related metrics. Remove if you switch to different allocator.

        MallocExtension & malloc_extension = *MallocExtension::instance();

        auto malloc_metrics =
        {
            "generic.current_allocated_bytes",
            "generic.heap_size",
            "tcmalloc.current_total_thread_cache_bytes",
            "tcmalloc.central_cache_free_bytes",
            "tcmalloc.transfer_cache_free_bytes",
            "tcmalloc.thread_cache_free_bytes",
            "tcmalloc.pageheap_free_bytes",
            "tcmalloc.pageheap_unmapped_bytes",
        };

        for (auto malloc_metric : malloc_metrics)
        {
            size_t value = 0;
            if (malloc_extension.GetNumericProperty(malloc_metric, &value))
                set(malloc_metric, value);
        }
    }
#endif

#if USE_JEMALLOC
    {
    #define FOR_EACH_METRIC(M) \
        M("allocated", size_t) \
        M("active", size_t) \
        M("metadata", size_t) \
        M("metadata_thp", size_t) \
        M("resident", size_t) \
        M("mapped", size_t) \
        M("retained", size_t) \
        M("background_thread.num_threads", size_t) \
        M("background_thread.num_runs", uint64_t) \
        M("background_thread.run_interval", uint64_t) \

    #define GET_METRIC(NAME, TYPE) \
        do \
        { \
            TYPE value{}; \
            size_t size = sizeof(value); \
            mallctl("stats." NAME, &value, &size, nullptr, 0); \
            set("jemalloc." NAME, value); \
        } while (0);

        FOR_EACH_METRIC(GET_METRIC);

    #undef GET_METRIC
    #undef FOR_EACH_METRIC
    }
#endif

    /// Add more metrics as you wish.
}


}
