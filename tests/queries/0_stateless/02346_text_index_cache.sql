-- Tags: no-parallel-replicas

SET enable_analyzer = 1;
SET allow_experimental_full_text_index = 1;
SET log_queries = 1;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = array, dictionary_block_size = 128)
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab
SELECT
    number,
    concat('text_', leftPad(toString(number), 3, '0'))
FROM numbers(512);

DROP VIEW IF EXISTS text_index_dictionary_block_cache_stats;
CREATE VIEW text_index_dictionary_block_cache_stats AS (
  SELECT
    concat('cache_hits = ', toString(ProfileEvents['TextIndexDictionaryBlockCacheHits']), ', cache_misses = ', toString(ProfileEvents['TextIndexDictionaryBlockCacheMisses']))
  FROM system.query_log
  WHERE query_kind ='Select'
      AND current_database = currentDatabase()
      AND endsWith(trimRight(query), concat('hasAnyTokens(message, [\'', {filter:String}, '\']);'))
      AND type='QueryFinish'
  LIMIT 1
);

SELECT 'Text index dictionary block cache';

SELECT 'All tokens between text_000 -> text_0127 are in the same dictionary block.';

SELECT '--- the first query misses cache.';
SELECT count() FROM tab WHERE hasAnyTokens(message, ['text_000']);

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_dictionary_block_cache_stats(filter = 'text_000');

SELECT '--- the second query hits cache.';
SELECT count() FROM tab WHERE hasAnyTokens(message, ['text_127']);

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_dictionary_block_cache_stats(filter = 'text_127');

DROP VIEW text_index_dictionary_block_cache_stats;
DROP TABLE tab;

SELECT 'Text index posting list cache';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = array, dictionary_block_size = 128)
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab SELECT number, 'text_pl_1' FROM numbers(1024);
INSERT INTO tab SELECT number, 'text_pl_2' FROM numbers(1024);
INSERT INTO tab SELECT number, 'text_pl_3' FROM numbers(1024);

DROP VIEW IF EXISTS text_index_posting_list_cache_stats;
CREATE VIEW text_index_posting_list_cache_stats AS (
  SELECT
    concat('cache_hits = ', toString(ProfileEvents['TextIndexPostingListCacheHits']), ', cache_misses = ', toString(ProfileEvents['TextIndexPostingListCacheMisses']))
  FROM system.query_log
  WHERE query_kind ='Select'
      AND current_database = currentDatabase()
      AND match(trimRight(query), {filter:String})
      AND type='QueryFinish'
  LIMIT 1
);

SELECT '--- cache misses on the first query execution for tokens.';
SELECT count() FROM tab WHERE hasAnyTokens(message, ['text_pl_1', 'text_pl_2']);

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_posting_list_cache_stats(filter = 'text_pl_1');

SELECT '--- cache hits for the deserialized posting lists.';
SELECT count() FROM tab WHERE hasAnyTokens(message, ['text_pl_2', 'text_pl_3']);

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_posting_list_cache_stats(filter = 'text_pl_3');

DROP VIEW text_index_posting_list_cache_stats;
DROP TABLE tab;
