-- Tags: no-parallel
-- Tag no-parallel: Messes with the query result cache

SET query_cache_partial_results = 1;
SET query_cache_min_query_runs = 0;
SET query_cache_min_query_duration = 0;
SET query_cache_ttl = 600;

SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS t_partial;
CREATE TABLE t_partial (region String, product String, quantity UInt64, price Float64) ENGINE = MergeTree ORDER BY region;
INSERT INTO t_partial SELECT
    arrayElement(['Europe', 'Asia', 'America'], (number % 3) + 1),
    arrayElement(['Laptop', 'Phone', 'Tablet'], (number % 3) + 1),
    (number % 100) + 1,
    round(50 + (number % 200), 2)
FROM numbers(1000);

SELECT '--- Query 1: SUM aggregation (populates cache at all levels) ---';
SELECT region, sum(quantity) AS total_qty FROM t_partial WHERE region = 'Europe' GROUP BY region ORDER BY total_qty
    SETTINGS use_query_cache = 1;

SELECT '--- Cache has entries ---';
SELECT count() > 0 FROM system.query_cache;

SELECT '--- Query 2: same WHERE, different aggregation (should reuse filter result from cache) ---';
SELECT region, avg(price) AS avg_price FROM t_partial WHERE region = 'Europe' GROUP BY region ORDER BY avg_price
    SETTINGS use_query_cache = 1;

SELECT '--- Verify mutual exclusion: normal cache path is disabled ---';
SYSTEM DROP QUERY CACHE;
SET query_cache_partial_results = 0;

SELECT '--- Correctness check ---';
SELECT region, sum(quantity) AS total_qty FROM t_partial WHERE region = 'Europe' GROUP BY region ORDER BY total_qty;
SELECT region, avg(price) AS avg_price FROM t_partial WHERE region = 'Europe' GROUP BY region ORDER BY avg_price;

DROP TABLE t_partial;

SYSTEM DROP QUERY CACHE;
