-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;

-- Throws because of non-deterministic function in main query
SELECT now(), avg FROM (SELECT avg(number) as avg FROM numbers(1, 100)) SETTINGS use_query_cache = true, query_cache_for_subqueries = true; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }

-- Throws because of non-deterministic function in sub-query
SELECT * FROM (SELECT now(), avg(number) as avg FROM numbers(1, 100)) SETTINGS use_query_cache = true, query_cache_for_subqueries = true; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }

-- Should be 0 records in system.query_cache
SELECT count(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;