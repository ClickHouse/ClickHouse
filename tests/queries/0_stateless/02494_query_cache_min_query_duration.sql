-- Tags: no-parallel
-- Tag no-parallel: The test messes with internal cache

SET allow_experimental_query_cache = true;

-- The test assumes that these two settings have default values. Neutralize the effect of setting randomization:
SET use_query_cache = false;
SET enable_reads_from_query_cache = true;

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
