
SET query_cache_tag = '02494_query_cache_exception_handling';

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_exception_handling';

-- If an exception is thrown during query execution, no entry must be created in the query cache
SELECT throwIf(1) SETTINGS use_query_cache = true; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT COUNT(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_exception_handling') AS test_query_cache;

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_exception_handling';
