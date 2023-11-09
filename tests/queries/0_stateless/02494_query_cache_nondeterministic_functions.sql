-- Tag no-parallel: Messes with internal cache
-- Tags: no-parallel

SYSTEM DROP QUERY CACHE;

-- rand() is non-deterministic, the query is rejected by default
-- to throw is the default behavior
SELECT count(rand(1)) SETTINGS use_query_cache = true; -- { serverError CANNOT_USE_QUERY_CACHE_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT count(rand(1)) SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'throw'; -- { serverError CANNOT_USE_QUERY_CACHE_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT count(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;

SELECT '---';

-- 'save' forces caching
SELECT count(rand(1)) SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'save';
SELECT count(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;

SELECT '---';

-- 'ignore' suppresses the exception but doesn't cache
SELECT count(rand(1)) SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'ignore';
SELECT count(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;

SELECT '---';
