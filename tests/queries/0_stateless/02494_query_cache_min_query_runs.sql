-- Tags: no-parallel, no-random-settings
-- Tag no-parallel: The test messes with internal cache
-- Tag no-random-settings: The test assumes default settings for the query cache.

SET allow_experimental_query_cache = true;

SYSTEM DROP QUERY CACHE;

-- Cache the query after the 1st query invocation
SELECT 1 SETTINGS use_query_cache = true, query_cache_min_query_runs = 0;
SELECT COUNT(*) FROM system.query_cache;

SELECT '---';

SYSTEM DROP QUERY CACHE;

-- Cache the query result after the 2nd query invocation
SELECT 1 SETTINGS use_query_cache = true, query_cache_min_query_runs = 1;
SELECT COUNT(*) FROM system.query_cache;
SELECT 1 SETTINGS use_query_cache = true, query_cache_min_query_runs = 1;
SELECT COUNT(*) FROM system.query_cache;

SELECT '---';

SYSTEM DROP QUERY CACHE;

-- Cache the query result after the 3rd query invocation
SELECT 1 SETTINGS use_query_cache = true, query_cache_min_query_runs = 2;
SELECT COUNT(*) FROM system.query_cache;
SELECT 1 SETTINGS use_query_cache = true, query_cache_min_query_runs = 2;
SELECT COUNT(*) FROM system.query_cache;
SELECT 1 SETTINGS use_query_cache = true, query_cache_min_query_runs = 2;
SELECT COUNT(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;
