-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SET enable_analyzer = 1;

SYSTEM DROP QUERY CACHE;

-- The Planner-level subquery cache must reject non-THROW overflow modes just like the top-level cache does
-- (a non-THROW overflow mode may truncate the result, and the cache cannot detect the truncation).
-- The outer query runs with `use_query_cache = 0`, so the equivalent check in `executeQuery` never fires;
-- the subquery opts into caching explicitly.
SELECT * FROM (SELECT sum(number) FROM numbers(100) SETTINGS use_query_cache = 1) SETTINGS read_overflow_mode = 'break'; -- { serverError QUERY_CACHE_USED_WITH_NON_THROW_OVERFLOW_MODE }
SELECT * FROM (SELECT sum(number) FROM numbers(100) SETTINGS use_query_cache = 1) SETTINGS result_overflow_mode = 'break'; -- { serverError QUERY_CACHE_USED_WITH_NON_THROW_OVERFLOW_MODE }
SELECT * FROM (SELECT sum(number) FROM numbers(100) SETTINGS use_query_cache = 1) SETTINGS timeout_overflow_mode = 'break'; -- { serverError QUERY_CACHE_USED_WITH_NON_THROW_OVERFLOW_MODE }

-- Same for subqueries cached via `query_cache_for_subqueries` propagation. The outer query uses the cache, so
-- here the check in `executeQuery` already rejects the query before the Planner is reached.
SELECT * FROM (SELECT sum(number) FROM numbers(100)) SETTINGS use_query_cache = 1, query_cache_for_subqueries = 1, read_overflow_mode = 'break'; -- { serverError QUERY_CACHE_USED_WITH_NON_THROW_OVERFLOW_MODE }

-- Nothing was cached.
SELECT count() FROM system.query_cache;

SYSTEM DROP QUERY CACHE;
