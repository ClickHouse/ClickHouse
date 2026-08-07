-- Tags: no-parallel, no-parallel-replicas
-- no-parallel: clears the server-wide text index tokens cache

SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET text_index_like_min_pattern_length = 1;
SET text_index_like_max_postings_to_read = 5;
SET use_text_index_pattern_bypass_cache = 1;
SET use_text_index_tokens_cache = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_query_condition_cache = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET max_threads = 1;
SET max_threads_for_indexes = 1;
SET log_queries = 1;
SET log_profile_events = 1;

DROP TABLE IF EXISTS text_index_pattern_bypass_cache;

CREATE TABLE text_index_pattern_bypass_cache
(
    part UInt8,
    id UInt64,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, dictionary_block_size = 64)
)
ENGINE = MergeTree
PARTITION BY part
ORDER BY id;

-- Four parts, each containing 676 alphabetic tokens seven times. Seven occurrences
-- make every posting list non-embedded, so broad patterns exceed the small budget.
INSERT INTO text_index_pattern_bypass_cache
SELECT
    number % 4,
    number,
    concat(
        'p',
        char(97 + intDiv(number, 4) % 26),
        char(97 + intDiv(intDiv(number, 4), 26) % 26))
FROM numbers(4 * 676 * 7);

SYSTEM CLEAR TEXT INDEX TOKENS CACHE;

-- The first execution scans and records a bailout; the second uses the cache.
SELECT count() FROM text_index_pattern_bypass_cache WHERE message LIKE '%pa%' SETTINGS log_comment = '04811_01_first';
SELECT count() FROM text_index_pattern_bypass_cache WHERE message LIKE '%pa%' SETTINGS log_comment = '04811_02_repeat';

-- A different threshold must not reuse the entry.
SELECT count() FROM text_index_pattern_bypass_cache WHERE message LIKE '%pa%'
SETTINGS text_index_like_max_postings_to_read = 1000, log_comment = '04811_03_different_key';

-- The dedicated switch must ignore an existing entry.
SELECT count() FROM text_index_pattern_bypass_cache WHERE message LIKE '%pa%'
SETTINGS use_text_index_pattern_bypass_cache = 0, log_comment = '04811_04_cache_disabled';

-- Clearing the existing token cache also clears pattern-bypass entries.
SYSTEM CLEAR TEXT INDEX TOKENS CACHE;
SELECT count() FROM text_index_pattern_bypass_cache WHERE message LIKE '%pa%' SETTINGS log_comment = '04811_05_after_clear';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['TextIndexReadDictionaryBlocks'] > 0 AS read_dictionary,
    ProfileEvents['TextIndexDiscardPatternScan'] > 0 AS scanned_bailout,
    ProfileEvents['TextIndexPatternBypassCacheHits'] > 0 AS cache_hit
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND event_date >= yesterday()
    AND match(log_comment, '^04811_[0-9]{2}_')
ORDER BY log_comment;

DROP TABLE text_index_pattern_bypass_cache;
