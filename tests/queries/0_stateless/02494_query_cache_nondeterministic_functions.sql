
SET query_cache_tag = '02494_query_cache_nondeterministic_functions';

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_nondeterministic_functions';

SELECT '-- query_cache_nondeterministic_function_handling = throw';
SELECT count(now()) SETTINGS use_query_cache = true; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT count(now()) SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'throw'; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT count(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_nondeterministic_functions') AS test_query_cache;

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_nondeterministic_functions';

SELECT '-- query_cache_nondeterministic_function_handling = save';
SELECT count(now()) SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'save';
SELECT count(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_nondeterministic_functions') AS test_query_cache;

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_nondeterministic_functions';

SELECT '-- query_cache_nondeterministic_function_handling = ignore';
SELECT count(now()) SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'ignore';
SELECT count(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_nondeterministic_functions') AS test_query_cache;

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_nondeterministic_functions';
