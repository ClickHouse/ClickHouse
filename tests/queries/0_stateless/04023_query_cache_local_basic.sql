-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache
-- Regression test for local cache hit/miss/TTL/tag/clear after the
-- IQueryResultCache abstraction was introduced in phases 1-4.

SYSTEM CLEAR QUERY CACHE;

-- Miss then hit
SELECT '-- miss then hit --';
SELECT 1 SETTINGS use_query_cache = true;
SELECT 1 SETTINGS use_query_cache = true;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - 600
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query = 'SELECT 1 SETTINGS use_query_cache = true;'
ORDER BY event_time_microseconds;

-- Tags: distinct entries per tag
SELECT '-- tags --';
SYSTEM CLEAR QUERY CACHE;
SELECT 42 SETTINGS use_query_cache = true;
SELECT 42 SETTINGS use_query_cache = true, query_cache_tag = 'a';
SELECT 42 SETTINGS use_query_cache = true, query_cache_tag = 'b';
SELECT count(*) FROM system.query_cache;

-- Clear by tag
SELECT '-- clear by tag --';
SYSTEM CLEAR QUERY CACHE TAG 'a';
SELECT count(*) FROM system.query_cache;
SYSTEM CLEAR QUERY CACHE;
SELECT count(*) FROM system.query_cache;

-- Entries from system.query_cache show the query text
SELECT '-- system.query_cache columns --';
SELECT 7 SETTINGS use_query_cache = true, query_cache_tag = 'test';
SELECT query, tag FROM system.query_cache;

SYSTEM CLEAR QUERY CACHE;
