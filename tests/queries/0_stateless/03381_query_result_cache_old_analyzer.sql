-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;

-- Subquery caching is only supported with the new analyzer.
-- With the old analyzer, no subquery cache entries should be created.

SET enable_analyzer = 0;

SELECT number FROM (SELECT number FROM numbers(5)) ORDER BY number
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;
-- Expected: 0

SYSTEM DROP QUERY CACHE;
