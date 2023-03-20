-- Tags: no-parallel, no-random-settings
-- Tag no-parallel: The test messes with internal cache
-- Tag no-random-settings: Assumes default settings for the query cache.

SET allow_experimental_query_cache = true;

SYSTEM DROP QUERY CACHE;

-- This creates an entry in the query cache ...
SELECT 1 SETTINGS use_query_cache = true;
SELECT COUNT(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;

SELECT '---';

-- ... but this does not because the query executes much faster than the specified minumum query duration for caching the result
SELECT 1 SETTINGS use_query_cache = true, query_cache_min_query_duration = 10000;
SELECT COUNT(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;
