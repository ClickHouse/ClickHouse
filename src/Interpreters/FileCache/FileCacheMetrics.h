#pragma once

#include <Interpreters/FileCache/FileSegmentInfo.h>
#include <base/types.h>

namespace DB::FileCacheMetrics
{

/// Map a queue-entry type to the stable Prometheus label value used by the
/// filesystem_cache_evictions_total / _evicted_bytes_total metrics.
const char * queueLabel(FileCacheQueueEntryType queue_type);

/// Aggregate (always-on when `FileCache::expose_eviction_metrics` is enabled).
/// Each call increments the queue-labelled counter, the bytes counter, and
/// observes the hits and size histograms exactly once.
void recordEviction(
    const String & cache_name,
    FileCacheQueueEntryType queue_type,
    size_t bytes,
    size_t hits);

/// Per-client variant (only called when `expose_eviction_metrics_per_client`
/// is also enabled). Updates the `_by_client` variants of all four families.
void recordEvictionByClient(
    const String & cache_name,
    FileCacheQueueEntryType queue_type,
    const String & client_id,
    size_t bytes,
    size_t hits);

}
