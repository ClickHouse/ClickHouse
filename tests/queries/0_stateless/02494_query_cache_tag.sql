-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;

-- Cache the query after the query invocation
SELECT 1 SETTINGS use_query_cache = true;
SELECT COUNT(*) FROM system.query_cache;

SELECT '---';

SYSTEM DROP QUERY CACHE;

-- Queries with tag value of this setting or not are considered different cache entries.
SELECT 1 SETTINGS use_query_cache = true;
SELECT COUNT(*) FROM system.query_cache;
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'one';
SELECT COUNT(*) FROM system.query_cache;

SELECT '---';

SYSTEM DROP QUERY CACHE;

-- Queries with different tags values of this setting are considered different cache entries.
SELECT 1 SETTINGS use_query_cache = true;
SELECT COUNT(*) FROM system.query_cache;
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'one';
SELECT COUNT(*) FROM system.query_cache;
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'one diff';
SELECT COUNT(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;
