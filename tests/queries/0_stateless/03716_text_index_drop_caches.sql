-- Tags: no-parallel-replicas, no-parallel

DROP TABLE IF EXISTS t_text_index_drop_caches;

SET allow_experimental_full_text_index = 1;
SET use_text_index_header_cache = 1;
SET use_text_index_dictionary_cache = 1;
SET use_text_index_postings_cache = 1;
SET use_skip_indexes_on_data_read = 1;

CREATE TABLE t_text_index_drop_caches
(
    s String,
    INDEX idx(s) TYPE text(tokenizer = sparseGrams(3, 10))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_text_index_drop_caches SELECT 'tkn' || toString(number) || 'nkt' FROM numbers(200000);

SELECT count() FROM t_text_index_drop_caches WHERE s LIKE '%888%' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_text_index_drop_caches WHERE hasAnyTokens(s, '888');
SELECT count() FROM t_text_index_drop_caches WHERE hasAnyTokens(s, '888');

SYSTEM DROP TEXT INDEX CACHES;

SELECT count() FROM t_text_index_drop_caches WHERE hasAnyTokens(s, '888');
SELECT count() FROM t_text_index_drop_caches WHERE hasAnyTokens(s, '888');

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['TextIndexHeaderCacheMisses'] > 0,
    ProfileEvents['TextIndexDictionaryBlockCacheMisses'] > 0,
    ProfileEvents['TextIndexPostingsCacheMisses'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND query LIKE '%SELECT count() FROM t_text_index_drop_caches%' AND type = 'QueryFinish'
ORDER BY event_time_microseconds;

DROP TABLE IF EXISTS t_text_index_drop_caches;
