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

-- Slab-level columns: small size classes are slab-based, large ones are not.
-- (No assertions relating two live counters: they are separate mallctl reads
-- and can come from different stats epochs.)
WITH
    (SELECT count() FROM system.jemalloc_bins WHERE NOT large) AS small_bins,
    (SELECT countIf(slab_size >= nregs * size) FROM system.jemalloc_bins WHERE NOT large) AS small_bins_with_sane_slab_size,
    (SELECT countIf(slab_size != 0 OR nonfull_slabs != 0 OR waste != 0) FROM system.jemalloc_bins WHERE large) AS large_bins_with_slab_stats
SELECT
    small_bins_with_sane_slab_size = small_bins,
    large_bins_with_slab_stats = 0;
