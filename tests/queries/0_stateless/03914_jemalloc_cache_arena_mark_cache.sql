-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings

-- Test that mark cache allocations use the dedicated jemalloc cache arena
-- and that SYSTEM CLEAR MARK CACHE properly reclaims arena pages.

DROP TABLE IF EXISTS t_cache_arena_marks;

CREATE TABLE t_cache_arena_marks (a UInt64, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, prewarm_mark_cache = 0;

INSERT INTO t_cache_arena_marks SELECT number, toString(number), number * 1.1 FROM numbers(10000);

SYSTEM CLEAR MARK CACHE;

-- Verify cache is empty
SELECT 'before_select', value FROM system.metrics WHERE metric = 'MarkCacheBytes';

-- Force mark loading
SELECT count() FROM t_cache_arena_marks WHERE NOT ignore(*) FORMAT Null;

-- Verify marks are cached
SELECT 'after_select', value > 0 FROM system.metrics WHERE metric = 'MarkCacheBytes';

-- Wait for asynchronous_metrics to update
SELECT sleep(2) FORMAT Null;

-- Verify cache arena has active pages (guarded for non-jemalloc builds)
WITH (SELECT value IN ('ON', '1') FROM system.build_options WHERE name = 'USE_JEMALLOC') AS jemalloc_enabled
SELECT 'arena_active',
    if(jemalloc_enabled,
       (SELECT value > 0 FROM system.asynchronous_metrics WHERE metric = 'jemalloc.cache_arena.pactive'),
       true);

-- Clear mark cache (triggers JemallocCacheArena::purge())
SYSTEM CLEAR MARK CACHE;

-- Verify cache is empty
SELECT 'after_clear', value FROM system.metrics WHERE metric = 'MarkCacheBytes';

-- Wait for asynchronous_metrics to update
SELECT sleep(2) FORMAT Null;

-- Verify cache arena pages are reclaimed
WITH (SELECT value IN ('ON', '1') FROM system.build_options WHERE name = 'USE_JEMALLOC') AS jemalloc_enabled
SELECT 'arena_reclaimed',
    if(jemalloc_enabled,
       (SELECT value FROM system.asynchronous_metrics WHERE metric = 'jemalloc.cache_arena.pactive') = 0,
       true);

DROP TABLE t_cache_arena_marks;
