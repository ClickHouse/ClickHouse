-- Tags: no-parallel

-- run query and cache query result
SELECT 1 SETTINGS enable_experimental_query_result_cache = true;
SELECT count(*) FROM system.queryresult_cache;

-- query results are no longer in cache after drop
SYSTEM DROP QUERY RESULT CACHE;
SELECT count(*) FROM system.queryresult_cache;
