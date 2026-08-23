-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache
-- Test that system.query_cache exposes the expected columns and that
-- count/dump work correctly after the IQueryResultCache refactoring.

SYSTEM CLEAR QUERY CACHE;

-- system.query_cache starts empty
SELECT count(*) FROM system.query_cache;

-- Store two entries with different tags
SELECT 100 SETTINGS use_query_cache = true;
SELECT 200 SETTINGS use_query_cache = true, query_cache_tag = 'report';
SELECT count(*) FROM system.query_cache;

-- Verify the column set returned by system.query_cache
SELECT 'columns:';
SELECT name FROM system.columns
WHERE database = 'system' AND table = 'query_cache'
ORDER BY name;

-- Verify stale=0, shared=0 for freshly written entries
SELECT stale, shared FROM system.query_cache ORDER BY query;

-- Clear and confirm empty
SYSTEM CLEAR QUERY CACHE;
SELECT count(*) FROM system.query_cache;
