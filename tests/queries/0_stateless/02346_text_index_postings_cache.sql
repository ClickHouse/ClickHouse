-- Tags: no-parallel, no-parallel-replicas
-- no-parallel: looks at server-wide metrics

--- These tests verify the caching of a deserialized text index posting lists in the consecutive executions.

SET enable_analyzer = 1;
SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_text_index_postings_cache = 1;
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

INSERT INTO tab SELECT number, 'text_pl_1' FROM numbers(64);
INSERT INTO tab SELECT number, 'text_pl_2' FROM numbers(64);
INSERT INTO tab SELECT number, 'text_pl_3' FROM numbers(64);

DROP VIEW IF EXISTS text_index_cache_stats;
CREATE VIEW text_index_cache_stats AS (
  SELECT
    concat('cache_hits = ', ProfileEvents['TextIndexPostingsCacheHits'], ', cache_misses = ', ProfileEvents['TextIndexPostingsCacheMisses'])
  FROM system.query_log
  WHERE query_kind ='Select'
      AND current_database = currentDatabase()
      AND endsWith(trimRight(query), concat('hasAnyTokens(message, \'', {filter:String}, '\');'))
      AND type='QueryFinish'
  ORDER BY query_start_time_microseconds DESC
  LIMIT 1
);

SELECT '--- cache miss on the first token.';
SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_pl_1');

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_pl_1');

SELECT '--- cache miss on the second token.';
SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_pl_2');

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_pl_2');

SELECT '--- cache hit on the first token.';
SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_pl_1');

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_pl_1');

SELECT '--- cache hit on the second token.';
SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_pl_2');

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_pl_2');

SELECT '--- no profile events when cache is disabled.';

SET use_text_index_postings_cache = 0;

SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_pl_3');

SET use_text_index_postings_cache = 1;

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_pl_3');

SELECT 'Clear text index postings cache';

SYSTEM DROP TEXT INDEX POSTINGS CACHE;

SELECT '--- cache miss on the first token.';
SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_pl_1');

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_pl_1');

SELECT '--- cache hit on the first token.';
SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_pl_1');

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_pl_1');

SYSTEM DROP TEXT INDEX POSTINGS CACHE;
DROP VIEW text_index_cache_stats;
DROP TABLE tab;
