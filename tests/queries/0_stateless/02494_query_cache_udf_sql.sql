-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- test for fix to issue #77553
SYSTEM DROP QUERY CACHE;
DROP FUNCTION IF EXISTS 02483_plusone;

CREATE FUNCTION 02483_plusone AS (a) -> a + 1;

SELECT '-- query_cache_nondeterministic_function_handling = throw';
SELECT 02483_plusone(1) SETTINGS use_query_cache = true; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT 02483_plusone(1) SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'throw'; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT count(*) FROM system.query_cache;
SYSTEM DROP QUERY CACHE;

SELECT '-- query_cache_nondeterministic_function_handling = save';
SELECT 02483_plusone(1) SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'save';
SELECT count(*) FROM system.query_cache;
SYSTEM DROP QUERY CACHE;

SELECT '-- query_cache_nondeterministic_function_handling = ignore';
SELECT 02483_plusone(1) SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'ignore';
SELECT count(*) FROM system.query_cache;
SYSTEM DROP QUERY CACHE;

DROP FUNCTION 02483_plusone;
