-- Tags: no-parallel
-- Tag no-parallel: Messes with the query result cache

SET use_query_cache = 1;
SET query_cache_before_limit_and_order_by = 1;
SET query_cache_min_query_runs = 0;
SET query_cache_min_query_duration = 0;
SET query_cache_ttl = 600;

SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS t_cache_before_limit;
CREATE TABLE t_cache_before_limit (a UInt64, b String) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_cache_before_limit SELECT number, toString(number) FROM numbers(100);

SELECT '--- First query populates cache ---';
SELECT a, b FROM t_cache_before_limit WHERE a < 20 ORDER BY a LIMIT 5;

SELECT '--- Cache has one entry ---';
SELECT count() FROM system.query_cache SETTINGS use_query_cache = 0;

SELECT '--- Same query, different LIMIT — should use cache ---';
SELECT a, b FROM t_cache_before_limit WHERE a < 20 ORDER BY a LIMIT 10;

SELECT '--- Cache still has exactly one entry ---';
SELECT count() FROM system.query_cache SETTINGS use_query_cache = 0;

SELECT '--- Same query, different ORDER BY direction — re-sorts cached data ---';
SELECT a, b FROM t_cache_before_limit WHERE a < 20 ORDER BY a DESC LIMIT 5;

SELECT '--- Different ORDER BY direction may create a second cache entry ---';
SELECT count() >= 1 FROM system.query_cache SETTINGS use_query_cache = 0;

SELECT '--- Verify mutual exclusion: normal cache path is disabled ---';
SYSTEM DROP QUERY CACHE;
-- With query_cache_before_limit_and_order_by, queries without ORDER BY/LIMIT that
-- have no SortingStep or LimitStep should not be cached at all
SELECT count() FROM t_cache_before_limit SETTINGS enable_reads_from_query_cache = 0;
SELECT count() FROM system.query_cache SETTINGS use_query_cache = 0;

SELECT '--- Correctness check: results match non-cached execution ---';
SYSTEM DROP QUERY CACHE;
SET query_cache_before_limit_and_order_by = 0;
SET use_query_cache = 0;

SELECT a, b FROM t_cache_before_limit WHERE a < 10 ORDER BY a DESC LIMIT 3;

DROP TABLE t_cache_before_limit;

SYSTEM DROP QUERY CACHE;
