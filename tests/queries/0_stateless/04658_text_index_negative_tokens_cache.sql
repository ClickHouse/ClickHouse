-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: clears the server-wide text index tokens cache

SET enable_analyzer = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_query_condition_cache = 0;
SET use_text_index_tokens_cache = 1;
SET log_queries = 1;
SET log_profile_events = 1;
SET max_rows_to_read = 0;

DROP TABLE IF EXISTS text_index_negative_cache;
CREATE TABLE text_index_negative_cache
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = array, dictionary_block_size = 128) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 128;

INSERT INTO text_index_negative_cache
SELECT number, concat('text_', leftPad(toString(number), 3, '0'))
FROM numbers(256);

DROP VIEW IF EXISTS text_index_negative_cache_stats;
CREATE VIEW text_index_negative_cache_stats AS
SELECT
    ProfileEvents['TextIndexReadDictionaryBlocks'] AS dictionary_reads,
    ProfileEvents['TextIndexTokensCacheHits'] AS positive_hits,
    ProfileEvents['TextIndexTokensCacheMisses'] AS cache_misses,
    ProfileEvents['TextIndexTokensCacheNegativeHits'] AS negative_hits,
    ProfileEvents['TextIndexTokensCacheNegativeMisses'] AS negative_misses
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time >= now() - 600
    AND query_kind = 'Select'
    AND current_database = currentDatabase()
    AND endsWith(trimRight(query), concat('hasAnyTokens(message, \'', {filter:String}, '\');'))
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SET use_text_index_negative_tokens_cache = 0;

SELECT count() FROM text_index_negative_cache WHERE hasAnyTokens(message, 'missing_disabled');
SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_negative_cache_stats(filter = 'missing_disabled');

SELECT count() FROM text_index_negative_cache WHERE hasAnyTokens(message, 'missing_disabled');
SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_negative_cache_stats(filter = 'missing_disabled');

SET use_text_index_negative_tokens_cache = 1;

SELECT count() FROM text_index_negative_cache WHERE hasAnyTokens(message, 'missing_enabled');
SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_negative_cache_stats(filter = 'missing_enabled');

SELECT count() FROM text_index_negative_cache WHERE hasAnyTokens(message, 'missing_enabled');
SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_negative_cache_stats(filter = 'missing_enabled');

SET use_text_index_negative_tokens_cache = 0;

SELECT count() FROM text_index_negative_cache WHERE hasAnyTokens(message, 'missing_enabled');
SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_negative_cache_stats(filter = 'missing_enabled');

SET use_text_index_negative_tokens_cache = 1;

SELECT count() FROM text_index_negative_cache WHERE hasAnyTokens(message, 'missing_enabled');
SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_negative_cache_stats(filter = 'missing_enabled');

SYSTEM CLEAR TEXT INDEX TOKENS CACHE;

SELECT count() FROM text_index_negative_cache WHERE hasAnyTokens(message, 'missing_enabled');
SYSTEM FLUSH LOGS query_log;
SELECT * FROM text_index_negative_cache_stats(filter = 'missing_enabled');

DROP VIEW text_index_negative_cache_stats;
DROP TABLE text_index_negative_cache;
