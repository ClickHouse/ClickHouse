WITH
    (SELECT value IN ('ON', '1') FROM system.build_options WHERE name = 'USE_JEMALLOC') AS jemalloc_enabled,
    (SELECT count() FROM system.jemalloc_bins) AS total_bins,
    (SELECT count() FROM system.jemalloc_bins WHERE large) AS large_bins,
    (SELECT count() FROM system.jemalloc_bins WHERE NOT large) AS small_bins,
    (SELECT sum(size * (allocations - deallocations)) FROM system.jemalloc_bins WHERE large) AS large_allocated_bytes,
    (SELECT sum(size * (allocations - deallocations)) FROM system.jemalloc_bins WHERE NOT large) AS small_allocated_bytes
SELECT
    (total_bins > 0) = jemalloc_enabled,
    (large_bins > 0) = jemalloc_enabled,
    (small_bins > 0) = jemalloc_enabled,
    (large_allocated_bytes > 0) = jemalloc_enabled,
    (small_allocated_bytes > 0) = jemalloc_enabled;
