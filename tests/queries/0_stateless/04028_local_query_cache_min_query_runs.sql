-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Regression test: local query cache min_query_runs behaviour is unchanged
-- after introducing the external query result cache abstraction.

SYSTEM CLEAR QUERY CACHE;

-- min_query_runs = 0: cache after 1st invocation
SELECT 1 SETTINGS use_query_cache = true, query_cache_min_query_runs = 0;
SELECT COUNT(*) FROM system.query_cache;

SELECT '---';

SYSTEM CLEAR QUERY CACHE;

-- min_query_runs = 1: cache after 2nd invocation
SELECT 1 SETTINGS use_query_cache = true, query_cache_min_query_runs = 1;
SELECT COUNT(*) FROM system.query_cache;
SELECT 1 SETTINGS use_query_cache = true, query_cache_min_query_runs = 1;
SELECT COUNT(*) FROM system.query_cache;

SELECT '---';

SYSTEM CLEAR QUERY CACHE;

-- min_query_runs = 2: cache after 3rd invocation
SELECT 1 SETTINGS use_query_cache = true, query_cache_min_query_runs = 2;
SELECT COUNT(*) FROM system.query_cache;
SELECT 1 SETTINGS use_query_cache = true, query_cache_min_query_runs = 2;
SELECT COUNT(*) FROM system.query_cache;
SELECT 1 SETTINGS use_query_cache = true, query_cache_min_query_runs = 2;
SELECT COUNT(*) FROM system.query_cache;

SYSTEM CLEAR QUERY CACHE;
