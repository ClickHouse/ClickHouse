-- Tags: no-parallel-replicas, no-parallel

-- Tests correctness and profile events of SYSTEM CLEAR TEXT INDEX CACHES

DROP TABLE IF EXISTS tab;

SET use_skip_indexes_on_data_read = 1;

-- Force-enable text index caches
SET use_text_index_header_cache = 1;
SET use_text_index_dictionary_cache = 1;
SET use_text_index_postings_cache = 1;

CREATE TABLE tab
(
    s String,
    INDEX idx(s) TYPE text(tokenizer = sparseGrams(3, 10))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab SELECT 'tkn' || toString(number) || 'nkt' FROM numbers(200000);

SELECT count() FROM tab WHERE s LIKE '%888%' SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasAnyTokens(s, '888');
SELECT count() FROM tab WHERE hasAnyTokens(s, '888');

SYSTEM CLEAR TEXT INDEX CACHES;

SELECT count() FROM tab WHERE hasAnyTokens(s, '888');
SELECT count() FROM tab WHERE hasAnyTokens(s, '888');

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['TextIndexHeaderCacheMisses'] > 0,
    ProfileEvents['TextIndexDictionaryBlockCacheMisses'] > 0,
    ProfileEvents['TextIndexPostingsCacheMisses'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND query LIKE '%SELECT count() FROM tab%' AND type = 'QueryFinish'
ORDER BY event_time_microseconds;

DROP TABLE IF EXISTS tab;
