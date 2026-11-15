-- Verify dedicated cache arena metrics exist in system.asynchronous_metrics.
-- When jemalloc is enabled, the dedicated cache arena metrics should be present.
WITH
    (SELECT value IN ('ON', '1') FROM system.build_options WHERE name = 'USE_JEMALLOC') AS jemalloc_enabled
SELECT
    (count() > 0) = jemalloc_enabled
FROM system.asynchronous_metrics
WHERE metric LIKE 'jemalloc.cache_arena%';
