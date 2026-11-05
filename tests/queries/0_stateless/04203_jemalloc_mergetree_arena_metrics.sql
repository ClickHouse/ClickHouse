-- Verify that the dedicated MergeTree arena exposes its async metrics when jemalloc is built in.
-- Mirrors the existing `03913_jemalloc_cache_arena_metrics.sql` test for the cache arena.

WITH
    (SELECT value IN ('ON', '1') FROM system.build_options WHERE name = 'USE_JEMALLOC') AS jemalloc_on
SELECT
    (count() > 0) = jemalloc_on
FROM system.asynchronous_metrics
WHERE metric LIKE 'jemalloc.mergetree_arena%';
