-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- (it's silly to use what will be tested below but we have to assume other tests cluttered the query cache)
SYSTEM DROP QUERY CACHE;

-- Cache query result in query cache
SELECT 1 SETTINGS use_query_cache = true;
SELECT count(*) FROM system.query_cache;

-- No query results are cached after DROP
SYSTEM DROP QUERY CACHE;
SELECT count(*) FROM system.query_cache;
