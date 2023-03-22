-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SET allow_experimental_query_cache = true;

-- The test assumes that these two settings have default values. Neutralize the effect of setting randomization:
SET use_query_cache = false;
SET enable_reads_from_query_cache = true;

-- Cache query result in query cache
SELECT 1 SETTINGS use_query_cache = true;
SELECT count(*) FROM system.query_cache;

-- No query results are cached after DROP
SYSTEM DROP QUERY CACHE;
SELECT count(*) FROM system.query_cache;
