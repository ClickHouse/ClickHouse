-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- A cached subquery result must be complete even if the outer query reads only a part of it.
-- https://github.com/ClickHouse/ClickHouse/issues/124409


SYSTEM DROP QUERY CACHE;

-- The outer LIMIT must stay above the step which stores the subquery result in the cache.
SELECT replaceRegexpAll(explain, '^[^A-Za-z]+', '') AS step
FROM (EXPLAIN PLAN description = 0 SELECT number FROM (SELECT number FROM numbers(10000) SETTINGS use_query_cache = 1) LIMIT 3)
WHERE step IN ('Limit', 'StreamInQueryResultCache');

-- LIMIT stops reading the subquery early. Such a partial result must not be cached.
SELECT number FROM (SELECT number FROM numbers(10000) SETTINGS use_query_cache = 1, max_block_size = 100) LIMIT 3 FORMAT Null;
SELECT count() FROM system.query_cache WHERE is_subquery = 1;
SELECT count() FROM (SELECT number FROM numbers(10000) SETTINGS use_query_cache = 1, max_block_size = 100);
-- A complete read stores the result, the next read is served from the cache (checked below), and the stored result is complete.
SELECT count() FROM system.query_cache WHERE is_subquery = 1;
SELECT /* 05331 limit read */ count() FROM (SELECT number FROM numbers(10000) SETTINGS use_query_cache = 1, max_block_size = 100);

SYSTEM DROP QUERY CACHE;

-- DISTINCT with a limit hint stops reading the subquery early. Such a partial result must not be cached.
SELECT DISTINCT number % 3 FROM (SELECT number FROM numbers(10000) SETTINGS use_query_cache = 1, max_block_size = 100) LIMIT 2 FORMAT Null;
SELECT count() FROM system.query_cache WHERE is_subquery = 1;
SELECT count() FROM (SELECT number FROM numbers(10000) SETTINGS use_query_cache = 1, max_block_size = 100);

SYSTEM DROP QUERY CACHE;

-- The outer LIMIT must not install a limit hint into an in-order aggregation below the cache step.
-- The outer ORDER BY reads the whole subquery result, so the result is stored, and it must be complete.
-- The settings of the outer query are part of the cache key, so the read below repeats them, and it uses both columns of the
-- subquery, because the analyzer removes unused columns from the subquery before its cache key is computed.
DROP TABLE IF EXISTS t_query_cache_partial_read;
CREATE TABLE t_query_cache_partial_read (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 100;
INSERT INTO t_query_cache_partial_read SELECT number % 100, number FROM numbers(10000);

SELECT k, s FROM (SELECT k, sum(v) AS s FROM t_query_cache_partial_read GROUP BY k ORDER BY k SETTINGS use_query_cache = 1) ORDER BY k LIMIT 2
SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1, optimize_aggregation_in_order_limit = 1, max_block_size = 100
FORMAT Null;
SELECT count() FROM system.query_cache WHERE is_subquery = 1;
SELECT /* 05331 aggregation read */ count(), sum(k), sum(s) FROM (SELECT k, sum(v) AS s FROM t_query_cache_partial_read GROUP BY k ORDER BY k SETTINGS use_query_cache = 1)
SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1, optimize_aggregation_in_order_limit = 1, max_block_size = 100;

DROP TABLE t_query_cache_partial_read;

-- Both reads above were served from the cache. With parallel replicas, the hits of the replicas are added to the hits of the initiator.
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits'] > 0 FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND query LIKE '%05331 % read%' AND query NOT LIKE '%system.query_log%'
ORDER BY event_time_microseconds;

SYSTEM DROP QUERY CACHE;
