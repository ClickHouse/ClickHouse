-- Tags: no-parallel
-- Tag no-parallel: flushes the query cache

SET query_cache_min_query_runs = 0;
SET query_cache_min_query_duration = 0;
SET query_cache_ttl = 600;

SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS t_multi_level;
CREATE TABLE t_multi_level (region String, product String, val UInt64) ENGINE = MergeTree ORDER BY region;
INSERT INTO t_multi_level SELECT
    arrayElement(['A', 'B'], (number % 2) + 1),
    arrayElement(['X', 'Y', 'Z'], (number % 3) + 1),
    number
FROM numbers(1000);

-- Populate cache with multi-level=3 so multiple plan nodes get cached
SELECT '--- multi-level write ---';
SELECT region, sum(val)
FROM t_multi_level
WHERE region = 'A'
GROUP BY region
ORDER BY sum(val) DESC
FORMAT Null
SETTINGS use_query_cache = 1, query_cache_partial_results = 1, query_cache_partial_results_max_levels = 3;

-- Multiple entries should be created
SELECT '--- multi-level creates multiple entries ---';
SELECT count() >= 2 FROM system.query_cache SETTINGS use_query_cache = 0;

-- Same query should hit cache
SELECT '--- same query hit ---';
SELECT region, sum(val)
FROM t_multi_level
WHERE region = 'A'
GROUP BY region
ORDER BY sum(val) DESC
SETTINGS use_query_cache = 1, query_cache_partial_results = 1, query_cache_partial_results_max_levels = 3;

-- Different aggregation but same filter should hit a lower-level cache entry
SELECT '--- different aggregation, shared filter ---';
SELECT region, avg(val)
FROM t_multi_level
WHERE region = 'A'
GROUP BY region
SETTINGS use_query_cache = 1, query_cache_partial_results = 1, query_cache_partial_results_max_levels = 3;

-- max_levels=1 (default) should create exactly 1 entry
SYSTEM DROP QUERY CACHE;
SELECT '--- single-level write ---';
SELECT region, sum(val)
FROM t_multi_level
WHERE region = 'A'
GROUP BY region
ORDER BY sum(val) DESC
FORMAT Null
SETTINGS use_query_cache = 1, query_cache_partial_results = 1, query_cache_partial_results_max_levels = 1;

SELECT '--- single-level count ---';
SELECT count() FROM system.query_cache SETTINGS use_query_cache = 0;

DROP TABLE IF EXISTS t_multi_level;
SYSTEM DROP QUERY CACHE;
