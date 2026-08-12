-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Results containing lazily replicated columns (produced here by `ARRAY JOIN`) must be materialized before they are stored in the query
-- result cache. Otherwise a `ColumnReplicated` reaches the serialization and the entry cannot be stored or served back.

SYSTEM CLEAR QUERY CACHE;

DROP TABLE IF EXISTS t_query_cache_replicated;

CREATE TABLE t_query_cache_replicated (k UInt64, arr Array(String)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_query_cache_replicated VALUES (1, ['a', 'b', 'c']), (2, ['d', 'e']);

SELECT '-- uncompressed entry';
SELECT k, a FROM t_query_cache_replicated ARRAY JOIN arr AS a SETTINGS use_query_cache = 1, query_cache_compress_entries = 0, enable_lazy_columns_replication = 1, max_threads = 1;
SELECT k, a FROM t_query_cache_replicated ARRAY JOIN arr AS a SETTINGS use_query_cache = 1, query_cache_compress_entries = 0, enable_lazy_columns_replication = 1, max_threads = 1;

-- `query_cache_compress_entries` is not part of the cache key, so the cache must be cleared to actually store a compressed entry.
SYSTEM CLEAR QUERY CACHE;

SELECT '-- compressed entry';
SELECT k, a FROM t_query_cache_replicated ARRAY JOIN arr AS a SETTINGS use_query_cache = 1, query_cache_compress_entries = 1, enable_lazy_columns_replication = 1, max_threads = 1;
SELECT k, a FROM t_query_cache_replicated ARRAY JOIN arr AS a SETTINGS use_query_cache = 1, query_cache_compress_entries = 1, enable_lazy_columns_replication = 1, max_threads = 1;

-- The first execution of each query was a cache miss, the second one was a cache hit.
SELECT '-- hits and misses';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query LIKE 'SELECT k, a FROM t_query_cache_replicated%'
ORDER BY query, event_time_microseconds;

DROP TABLE t_query_cache_replicated;
SYSTEM CLEAR QUERY CACHE;
