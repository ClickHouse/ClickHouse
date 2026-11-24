-- Tags: no-parallel, no-parallel-replicas
-- no-parallel: looks at server-wide metrics

--- These tests verify the caching of a deserialized text index posting lists in the consecutive executions.

SET enable_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_text_index_postings_cache = 1;
SET log_queries = 1;
SET max_rows_to_read = 0;
SET query_plan_optimize_count_from_text_index = 0;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id UInt32,
    message String,
    -- posting_list_block_size is pinned: with a smaller randomized value the posting list of a token
    -- is split into several blocks, and each block is a separate postings cache entry, which changes
    -- the expected cache hit/miss counts.
    INDEX idx(message) TYPE text(tokenizer = array, dictionary_block_size = 128, posting_list_block_size = 128)
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
  WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_kind ='Select'
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

SELECT 'Clear text index postings cache';

SYSTEM CLEAR TEXT INDEX POSTINGS CACHE;

SELECT '--- cache miss on the first token.';
SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_pl_1');

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_pl_1');

SELECT '--- cache hit on the first token.';
SELECT count() FROM tab WHERE hasAnyTokens(message, 'text_pl_1');

SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_cache_stats(filter = 'text_pl_1');

SELECT 'A repeated phrase search is served from the cache';

DROP TABLE IF EXISTS tab_phrase;
CREATE TABLE tab_phrase
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1, posting_list_block_size = 256)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1, text_index_posting_list_codec = 'pfor';

INSERT INTO tab_phrase
SELECT number, concat(
    if(number % 3 = 0, 'hello world', 'hello clickhouse world'),
    multiIf(number % 16 = 5, ' needle clickhouse rare', number % 16 = 13, ' needle rare', ''),
    if(number IN (1000, 2000, 3000), ' unicorn clickhouse', ''))
FROM numbers(4096);
OPTIMIZE TABLE tab_phrase FINAL;

SELECT count(), sum(id) FROM tab_phrase WHERE hasPhrase(message, 'needle rare') SETTINGS use_skip_indexes = 1, text_index_hint_max_selectivity = 1., use_query_condition_cache = 0, log_comment = '02346_phrase_cache_1';
SELECT count(), sum(id) FROM tab_phrase WHERE hasPhrase(message, 'needle rare') SETTINGS use_skip_indexes = 1, text_index_hint_max_selectivity = 1., use_query_condition_cache = 0, log_comment = '02346_phrase_cache_2';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['TextIndexPhraseSearches'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('02346_phrase_cache_1', '02346_phrase_cache_2')
ORDER BY log_comment;

DROP TABLE tab_phrase;

SYSTEM CLEAR TEXT INDEX POSTINGS CACHE;
DROP VIEW text_index_cache_stats;
DROP TABLE tab;
