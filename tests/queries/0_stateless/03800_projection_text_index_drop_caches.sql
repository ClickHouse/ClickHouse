SET allow_experimental_projection_text_index = 1;
-- Tags: no-parallel-replicas, no-parallel

-- Tests correctness and profile events of SYSTEM DROP TEXT INDEX CACHES

DROP TABLE IF EXISTS tab;

SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET use_text_index_dictionary_cache = 1;
SET use_text_index_postings_cache = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;

CREATE TABLE tab
(
    s String,
    PROJECTION idx INDEX s TYPE text(tokenizer = sparseGrams(3, 10))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab SELECT 'tkn' || toString(number) || 'nkt' FROM numbers(200000);

SELECT count() FROM tab WHERE s LIKE '%888%' SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasAnyTokens(s, '888') SETTINGS log_comment = 'drop_cache_03800';
SELECT count() FROM tab WHERE hasAnyTokens(s, '888') SETTINGS log_comment = 'drop_cache_03800';

SYSTEM DROP TEXT INDEX CACHES;

SELECT count() FROM tab WHERE hasAnyTokens(s, '888') SETTINGS log_comment = 'drop_cache_03800';
SELECT count() FROM tab WHERE hasAnyTokens(s, '888') SETTINGS log_comment = 'drop_cache_03800';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['TextIndexDictionaryBlockCacheMisses'] > 0,
    ProfileEvents['TextIndexPostingsCacheMisses'] > 0
FROM (
    SELECT * FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = 'drop_cache_03800' AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC
    LIMIT 4
)
ORDER BY event_time_microseconds;

DROP TABLE IF EXISTS tab;
