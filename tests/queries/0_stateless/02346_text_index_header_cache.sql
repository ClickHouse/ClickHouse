-- Tags: no-parallel, no-parallel-replicas
-- no-parallel: looks at server-wide metrics

--- These tests verify the caching of a deserialized text index header in the consecutive executions.

SET enable_analyzer = 1;
SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_text_index_header_cache = 1;
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

--- The text index would have 4 parts/marks.
INSERT INTO tab
SELECT
    number,
    concat('text_', leftPad(toString(number), 3, '0'))
FROM numbers(512);

SYSTEM STOP MERGES tab;

DROP VIEW IF EXISTS text_index_cache_stats;
CREATE VIEW text_index_cache_stats AS (
  SELECT
    concat('cache_hits = ', toString(ProfileEvents['TextIndexHeaderCacheHits']), ', cache_misses = ', toString(ProfileEvents['TextIndexHeaderCacheMisses']))
  FROM system.query_log
  WHERE query_kind ='Select'
      AND current_database = currentDatabase()
      AND endsWith(trimRight(query), concat('hasAnyTokens(message, \'', {filter:String}, '\');'))
      AND type='QueryFinish'
  LIMIT 1
);

SELECT '--- cache miss on the first run.';
SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_000');

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_000');

SELECT '--- cache hit on the second run.';
SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_511');

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_511');

SELECT '--- no profile events when cache is disabled.';

SET use_text_index_header_cache = 0;

SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_255');

SET use_text_index_header_cache = 1;

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_255');

SELECT 'Clear text index header cache';

SYSTEM DROP TEXT INDEX HEADER CACHE;

SELECT '--- cache miss on the first run.';
SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_001');

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_001');

SELECT '--- cache hit on the second run.';
SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_510');

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_510');

SYSTEM DROP TEXT INDEX HEADER CACHE;
DROP VIEW text_index_cache_stats;
DROP TABLE tab;
