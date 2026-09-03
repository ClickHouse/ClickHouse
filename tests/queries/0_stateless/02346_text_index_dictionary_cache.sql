-- Tags: no-parallel, no-parallel-replicas
-- no-parallel: looks at server-wide metrics

--- These tests verify the caching of deserialized text index token infos in consecutive executions.
--- The tokens cache caches individual token infos (not entire dictionary blocks).

SET enable_analyzer = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_text_index_tokens_cache = 1;
SET log_queries = 1;
SET max_rows_to_read = 0;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = array, dictionary_block_size = 128) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 128;

INSERT INTO tab
SELECT
    number,
    concat('text_', leftPad(toString(number), 3, '0'))
FROM numbers(256);

DROP VIEW IF EXISTS text_index_cache_stats;
CREATE VIEW text_index_cache_stats AS (
  SELECT
    concat('cache_hits = ', toString(ProfileEvents['TextIndexTokensCacheHits']), ', cache_misses = ', toString(ProfileEvents['TextIndexTokensCacheMisses']))
  FROM system.query_log
  WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_kind ='Select'
      AND current_database = currentDatabase()
      AND endsWith(trimRight(query), concat('hasAnyTokens(message, \'', {filter:String}, '\');'))
      AND type='QueryFinish'
  ORDER BY event_time_microseconds DESC
  LIMIT 1
);

SELECT '--- cache miss on a new token.';
SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_000');

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_000');

SELECT '--- cache miss on another new token.';
SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_128');

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_128');

SELECT '--- cache hit on a previously queried token.';
SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_000');

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_000');

SELECT '--- cache miss on a token not previously queried.';
SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_127');

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_127');

SELECT 'Clear text index tokens cache';

SYSTEM CLEAR TEXT INDEX TOKENS CACHE;

SELECT '--- cache miss after clearing cache.';
SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_125');

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_125');

SELECT '--- cache miss on another token after clearing cache.';
SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_129');

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_129');

SELECT '--- cache hit on a token queried after clearing cache.';
SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_125');

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_125');

SYSTEM CLEAR TEXT INDEX TOKENS CACHE;
DROP TABLE tab;
