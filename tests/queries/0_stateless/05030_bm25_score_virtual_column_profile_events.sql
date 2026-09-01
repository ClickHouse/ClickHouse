-- Tags: no-parallel-replicas

SET enable_analyzer = 1;
SET allow_experimental_bm25_score_column = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;
SET log_queries = 1;
SET log_profile_events = 1;

DROP TABLE IF EXISTS tab_bm25_events;

CREATE TABLE tab_bm25_events
(
    id UInt32,
    body String,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', enable_scoring = 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 16, allow_experimental_text_index_scoring = 1;

INSERT INTO tab_bm25_events
SELECT number, concat('token_', toString(number % 7), ' filler_', toString(number % 3), ' raft')
FROM numbers(128);

CREATE VIEW bm25_events_stats AS
SELECT
    ProfileEvents['TextScoreStatsBuilt'] AS stats_built,
    ProfileEvents['TextScoreRowsScored'] > 0 AS scored_rows,
    ProfileEvents['TextIndexReadPostings'] AS postings_reads
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time >= now() - 600
    AND query_kind = 'Select'
    AND current_database = currentDatabase()
    AND Settings['log_comment'] = {comment:String}
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT '-- the collection statistics are built exactly once per query';
SELECT sum(_bm25_score) > 0 FROM tab_bm25_events WHERE hasAnyTokens(body, ['token_1', 'raft'])
SETTINGS log_comment = 'bm25_events_with_score';
SYSTEM FLUSH LOGS query_log;
SELECT stats_built, scored_rows FROM bm25_events_stats(comment = 'bm25_events_with_score');

SELECT '-- decode-once fence: the same query without the score reads each postings block the same number of times';
SELECT count() FROM tab_bm25_events WHERE hasAnyTokens(body, ['token_1', 'raft'])
SETTINGS log_comment = 'bm25_events_without_score';
SYSTEM FLUSH LOGS query_log;

SELECT with_score.postings_reads = without_score.postings_reads
FROM bm25_events_stats(comment = 'bm25_events_with_score') AS with_score, bm25_events_stats(comment = 'bm25_events_without_score') AS without_score;

DROP VIEW bm25_events_stats;
DROP TABLE tab_bm25_events;
