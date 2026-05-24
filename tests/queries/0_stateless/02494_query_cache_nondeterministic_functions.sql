-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;

SELECT '-- query_cache_nondeterministic_function_handling = throw';
SELECT count(now()) SETTINGS use_query_cache = true; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT count(now()) SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'throw'; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT count(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;

SELECT '-- query_cache_nondeterministic_function_handling = save';
SELECT count(now()) SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'save';
SELECT count(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;

SELECT '-- query_cache_nondeterministic_function_handling = ignore';
SELECT count(now()) SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'ignore';
SELECT count(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;
