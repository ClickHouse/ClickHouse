
-- Test for issue #77553: SQL-defined UDFs may be non-deterministic. The query cache should treat them as such, i.e. reject them.
-- Also see test_executable_function_query_cache in tests/integration/test_executable_user_defined_function

SET query_cache_tag = '02494_query_cache_udf_sql';

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_udf_sql';
DROP FUNCTION IF EXISTS udf;

CREATE FUNCTION udf AS (a) -> a + 1;

SELECT '-- query_cache_nondeterministic_function_handling = throw';
SELECT udf(1) FORMAT Null SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'throw'; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT count(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_udf_sql') AS test_query_cache;
SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_udf_sql';

SELECT '-- query_cache_nondeterministic_function_handling = save';
SELECT udf(1) FORMAT Null SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'save';
SELECT count(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_udf_sql') AS test_query_cache;
SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_udf_sql';

SELECT '-- query_cache_nondeterministic_function_handling = ignore';
SELECT udf(1) FORMAT Null SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'ignore';
SELECT count(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_udf_sql') AS test_query_cache;
SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_udf_sql';

DROP FUNCTION udf;
