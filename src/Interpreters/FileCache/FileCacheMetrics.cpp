#include <Interpreters/FileCache/FileCacheMetrics.h>

#include <Common/DimensionalMetrics.h>
#include <Common/HistogramMetrics.h>

namespace DB::FileCacheMetrics
{

namespace
{
    const HistogramMetrics::Buckets kHitsBuckets   = {0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 1024, 8192};
    const HistogramMetrics::Buckets kSizeBuckets   = {4 * 1024, 16 * 1024, 64 * 1024, 256 * 1024,
                                                      1 * 1024 * 1024, 4 * 1024 * 1024,
                                                      16 * 1024 * 1024, 64 * 1024 * 1024};

    DimensionalMetrics::MetricFamily & evictionsTotal = DimensionalMetrics::Factory::instance().registerMetric(
        "filesystem_cache_evictions_total",
        "Number of file segments evicted from a filesystem cache, labelled by cache name and source priority queue. "
        "Emitted only when the cache has `expose_eviction_metrics` enabled.",
        {"cache_name", "queue"}
    );

    DimensionalMetrics::MetricFamily & evictedBytesTotal = DimensionalMetrics::Factory::instance().registerMetric(
        "filesystem_cache_evicted_bytes_total",
        "Total bytes of file segments evicted from a filesystem cache, labelled by cache name and source priority queue. "
        "Emitted only when the cache has `expose_eviction_metrics` enabled.",
        {"cache_name", "queue"}
    );

    HistogramMetrics::MetricFamily & evictedSegmentHits = HistogramMetrics::Factory::instance().registerMetric(
        "filesystem_cache_evicted_segment_hits",
        "Distribution of cache-hit counts on file segments at the moment of their eviction, labelled by source priority queue.",
        kHitsBuckets,
        {"cache_name", "queue"}
    );

    HistogramMetrics::MetricFamily & evictedSegmentSizeBytes = HistogramMetrics::Factory::instance().registerMetric(
        "filesystem_cache_evicted_segment_size_bytes",
        "Distribution of byte sizes of evicted file segments, labelled by source priority queue.",
        kSizeBuckets,
        {"cache_name", "queue"}
    );

    DimensionalMetrics::MetricFamily & evictionsByClient = DimensionalMetrics::Factory::instance().registerMetric(
        "filesystem_cache_evictions_by_client_total",
        "Number of file segments evicted from a filesystem cache, labelled by cache name, source priority queue, and user id. "
        "Disabled by default; enable via `expose_eviction_metrics_per_client`. Cardinality grows with the number of distinct evicting users.",
        {"cache_name", "queue", "client_id"}
    );

    DimensionalMetrics::MetricFamily & evictedBytesByClient = DimensionalMetrics::Factory::instance().registerMetric(
        "filesystem_cache_evicted_bytes_by_client_total",
        "Total bytes of file segments evicted from a filesystem cache, labelled by cache name, source priority queue, and user id. "
        "Disabled by default; enable via `expose_eviction_metrics_per_client`.",
        {"cache_name", "queue", "client_id"}
    );

    HistogramMetrics::MetricFamily & evictedSegmentHitsByClient = HistogramMetrics::Factory::instance().registerMetric(
        "filesystem_cache_evicted_segment_hits_by_client",
        "Distribution of cache-hit counts on evicted file segments, labelled by source priority queue and user id. "
        "Disabled by default; enable via `expose_eviction_metrics_per_client`.",
        kHitsBuckets,
        {"cache_name", "queue", "client_id"}
    );

    HistogramMetrics::MetricFamily & evictedSegmentSizeBytesByClient = HistogramMetrics::Factory::instance().registerMetric(
        "filesystem_cache_evicted_segment_size_bytes_by_client",
        "Distribution of byte sizes of evicted file segments, labelled by source priority queue and user id. "
        "Disabled by default; enable via `expose_eviction_metrics_per_client`.",
        kSizeBuckets,
        {"cache_name", "queue", "client_id"}
    );
}

const char * queueLabel(FileCacheQueueEntryType queue_type)
{
    switch (queue_type)
    {
        case FileCacheQueueEntryType::SLRU_Protected:
            return "protected";
        case FileCacheQueueEntryType::SLRU_Probationary:
            return "probationary";
        case FileCacheQueueEntryType::LRU:
            return "lru";
        case FileCacheQueueEntryType::SplitCache_Data:
            return "split_data";
        case FileCacheQueueEntryType::SplitCache_System:
            return "split_system";
        default:
            return "unknown";
    }
}

void recordEviction(
    const String & cache_name,
    FileCacheQueueEntryType queue_type,
    size_t bytes,
    size_t hits)
{
    const char * queue = queueLabel(queue_type);
    evictionsTotal.withLabels({cache_name, queue}).increment();
    evictedBytesTotal.withLabels({cache_name, queue}).increment(static_cast<DimensionalMetrics::Value>(bytes));
    evictedSegmentHits.withLabels({cache_name, queue}).observe(static_cast<HistogramMetrics::Value>(hits));
    evictedSegmentSizeBytes.withLabels({cache_name, queue}).observe(static_cast<HistogramMetrics::Value>(bytes));
}

void recordEvictionByClient(
    const String & cache_name,
    FileCacheQueueEntryType queue_type,
    const String & client_id,
    size_t bytes,
    size_t hits)
{
    const char * queue = queueLabel(queue_type);
    evictionsByClient.withLabels({cache_name, queue, client_id}).increment();
    evictedBytesByClient.withLabels({cache_name, queue, client_id}).increment(static_cast<DimensionalMetrics::Value>(bytes));
    evictedSegmentHitsByClient.withLabels({cache_name, queue, client_id}).observe(static_cast<HistogramMetrics::Value>(hits));
    evictedSegmentSizeBytesByClient.withLabels({cache_name, queue, client_id}).observe(static_cast<HistogramMetrics::Value>(bytes));
}

}
