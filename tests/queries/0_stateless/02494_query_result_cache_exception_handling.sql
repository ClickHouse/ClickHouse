-- Tags: no-parallel

SYSTEM DROP QUERY RESULT CACHE;

-- if an exception is thrown during query execution, no entry must be created in the query result cache
SELECT throwIf(1) SETTINGS enable_experimental_query_result_cache = true; -- { serverError 395 }
SELECT COUNT(*) FROM system.queryresult_cache;

SYSTEM DROP QUERY RESULT CACHE;
