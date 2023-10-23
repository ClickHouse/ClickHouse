-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SET allow_experimental_query_cache = true;

-- Cache query result in query cache
SELECT 1 SETTINGS use_query_cache = true;
SELECT count(*) FROM system.query_cache;

-- No query results are cached after DROP
SYSTEM DROP QUERY CACHE;
SELECT count(*) FROM system.query_cache;
