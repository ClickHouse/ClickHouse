-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- A read task can contain several disjoint mark ranges. Cached blocks are stored
-- per contiguous range, so a repeated query must be served from the cache for
-- every range, not only for the range that ends at the task's last mark.

SET max_threads = 1;

DROP TABLE IF EXISTS t_cc_multi_range;

CREATE TABLE t_cc_multi_range (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cc_multi_range SELECT number, number * 3 FROM numbers(100000);

SYSTEM DROP COLUMNS CACHE;

-- The predicate selects two disjoint mark ranges of the part with a wide gap
-- between them. The first run populates the cache.
SELECT sum(v), count() FROM t_cc_multi_range WHERE id < 3000 OR id >= 95000
SETTINGS use_columns_cache = 1, log_queries = 1;

-- The second run must be served entirely from the cache.
SELECT sum(v), count() FROM t_cc_multi_range WHERE id < 3000 OR id >= 95000
SETTINGS use_columns_cache = 1, log_queries = 1;

SYSTEM FLUSH LOGS query_log;

-- The query text in query_log can include the preceding SQL comments, so match
-- on a fragment of the query body and exclude this introspection query itself.
SELECT
    ProfileEvents['ColumnsCacheHits'] > 0 AS has_hits,
    ProfileEvents['ColumnsCacheMisses'] > 0 AS has_misses
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%count() FROM t_cc_multi_range WHERE%'
    AND query NOT LIKE '%query_log%'
ORDER BY event_time_microseconds;

DROP TABLE t_cc_multi_range;
