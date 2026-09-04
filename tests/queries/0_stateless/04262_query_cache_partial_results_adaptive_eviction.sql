-- Tags: no-parallel
-- Tag no-parallel: flushes the query cache

SET query_cache_min_query_runs = 0;
SET query_cache_min_query_duration = 0;
SET query_cache_ttl = 600;

SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS t_adaptive;
CREATE TABLE t_adaptive (region String, val UInt64) ENGINE = MergeTree ORDER BY region;
INSERT INTO t_adaptive SELECT
    arrayElement(['A', 'B'], (number % 2) + 1),
    number
FROM numbers(1000);

-- Verify the setting is accepted and doesn't error
SELECT '--- adaptive eviction setting accepted ---';
SELECT sum(val) FROM t_adaptive WHERE region = 'A'
FORMAT Null
SETTINGS use_query_cache = 1, query_cache_partial_results = 1,
         query_cache_partial_results_adaptive_eviction = 1;

SELECT '--- cache populated ---';
SELECT count() >= 1 FROM system.query_cache SETTINGS use_query_cache = 0;

-- Cache hit with adaptive eviction enabled
SELECT '--- cache hit with adaptive eviction ---';
SELECT sum(val) FROM t_adaptive WHERE region = 'A'
SETTINGS use_query_cache = 1, query_cache_partial_results = 1,
         query_cache_partial_results_adaptive_eviction = 1;

-- Different aggregation also hits (shared sub-plan)
SELECT '--- different agg, shared filter ---';
SELECT avg(val) FROM t_adaptive WHERE region = 'A'
SETTINGS use_query_cache = 1, query_cache_partial_results = 1,
         query_cache_partial_results_adaptive_eviction = 1;

-- Verify adaptive eviction is backward-compatible (disabled by default)
SYSTEM DROP QUERY CACHE;
SELECT '--- default (no adaptive) still works ---';
SELECT sum(val) FROM t_adaptive WHERE region = 'A'
FORMAT Null
SETTINGS use_query_cache = 1, query_cache_partial_results = 1;

SELECT count() >= 1 FROM system.query_cache SETTINGS use_query_cache = 0;

DROP TABLE IF EXISTS t_adaptive;
SYSTEM DROP QUERY CACHE;
