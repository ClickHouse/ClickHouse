-- Tags: no-parallel
-- Tag no-parallel: Messes with the query result cache

SET use_query_cache = 1;
SET query_cache_before_limit_and_order_by = 1;
SET query_cache_min_query_runs = 0;
SET query_cache_min_query_duration = 0;
SET query_cache_ttl = 600;

SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS t_cache_totals;
CREATE TABLE t_cache_totals (region String, amount UInt64) ENGINE = MergeTree ORDER BY region;
INSERT INTO t_cache_totals SELECT
    arrayElement(['A', 'B', 'C'], (number % 3) + 1),
    number + 1
FROM numbers(30);

SELECT '--- WITH TOTALS: first query populates cache (cache miss) ---';
SELECT region, sum(amount) AS s FROM t_cache_totals GROUP BY region WITH TOTALS ORDER BY region LIMIT 2;

SELECT count() FROM system.query_cache SETTINGS use_query_cache = 0;

SELECT '--- WITH TOTALS: different LIMIT, cache hit — data rows and totals both correct ---';
SELECT region, sum(amount) AS s FROM t_cache_totals GROUP BY region WITH TOTALS ORDER BY region LIMIT 3;

SELECT '--- Cache entry count unchanged ---';
SELECT count() FROM system.query_cache SETTINGS use_query_cache = 0;

SELECT '--- Correctness: non-cached WITH TOTALS ---';
SYSTEM DROP QUERY CACHE;
SET query_cache_before_limit_and_order_by = 0;
SET use_query_cache = 0;
SELECT region, sum(amount) AS s FROM t_cache_totals GROUP BY region WITH TOTALS ORDER BY region;

DROP TABLE t_cache_totals;

SYSTEM DROP QUERY CACHE;
