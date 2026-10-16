-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Verifies that `SETTINGS <query-cache-related setting> = DEFAULT` resets are normalized away from
-- the query cache key, just like the same settings passed as regular assignments. Such resets are
-- stored in `ASTSetQuery::default_settings` (not in `changes`), so they must be filtered with the
-- same predicate; otherwise the reset keeps a non-empty SETTINGS clause and splits the cache entry.

SYSTEM CLEAR QUERY CACHE;

-- Populate the query cache.
SELECT 4538 SETTINGS use_query_cache = true;

-- One entry exists.
SELECT count() FROM system.query_cache;

-- The same query, but resetting query-cache-related settings via `= DEFAULT`. These resets do not
-- affect the result and must be normalized away, so the query is served from the cache (passive mode).
-- Note: the target query below must follow a statement (not a comment), otherwise query_log stores
-- the preceding comment as part of its `query` text and the exact-match lookup misses it.
SELECT '---';
SELECT 4538 SETTINGS use_query_cache = true, enable_writes_to_query_cache = false, query_cache_ttl = DEFAULT, log_comment = DEFAULT;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query = 'SELECT 4538 SETTINGS use_query_cache = true, enable_writes_to_query_cache = false, query_cache_ttl = DEFAULT, log_comment = DEFAULT;';

SYSTEM CLEAR QUERY CACHE;
