-- Tags: no-parallel, no-parallel-replicas
-- no-parallel: looks at server-wide metrics

--- These tests verify the caching of a deserialized text index dictionary block in the consecutive executions.

SET enable_analyzer = 1;
SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_text_index_dictionary_cache = 1;
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

--- The text index would have two dictionary blocks.
INSERT INTO tab
SELECT
    number,
    concat('text_', leftPad(toString(number), 3, '0'))
FROM numbers(256);

DROP VIEW IF EXISTS text_index_cache_stats;
CREATE VIEW text_index_cache_stats AS (
  SELECT
    concat('cache_hits = ', toString(ProfileEvents['TextIndexDictionaryBlockCacheHits']), ', cache_misses = ', toString(ProfileEvents['TextIndexDictionaryBlockCacheMisses']))
  FROM system.query_log
  WHERE query_kind ='Select'
      AND current_database = currentDatabase()
      AND endsWith(trimRight(query), concat('hasAnyTokens(message, \'', {filter:String}, '\');'))
      AND type='QueryFinish'
  LIMIT 1
);

SELECT 'Tokens between text_000 -> text_127 are in the first dictionary block and text_128 -> text_255 are in the second dictionary block.';

SELECT '--- cache miss on the first dictionary block.';
SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_000');

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_000');

SELECT '--- cache miss on the second dictionary block.';
SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_128');

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_128');

SELECT '--- cache hit on the first dictionary block.';
SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_127');

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_127');

SELECT '--- no profile events when cache is disabled.';

SET use_text_index_dictionary_cache = 0;

SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_126');

SET use_text_index_dictionary_cache = 1;

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_126');

SELECT 'Clear text index cache';

SYSTEM DROP TEXT INDEX DICTIONARY CACHE;

SELECT '--- cache miss on the first dictionary block.';
SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_125');

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_125');

SELECT '--- cache miss on the second dictionary block.';
SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_129');

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_129');

SYSTEM DROP TEXT INDEX DICTIONARY CACHE;
DROP TABLE tab;
