-- Tags: no-parallel-replicas
-- Test checks that granule of the text index is read only once during analysis.

DROP TABLE IF EXISTS t_text_index_read_once;

CREATE TABLE t_text_index_read_once
(
    id UInt64,
    s String,
    INDEX idx_s(s) TYPE text (tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_text_index_read_once SELECT number, 's' || toString(number) FROM numbers(1000);
OPTIMIZE TABLE t_text_index_read_once FINAL;

SELECT count() FROM t_text_index_read_once WHERE hasAllTokens(s, 's322') OR hasAllTokens(s, 's777') SETTINGS use_skip_indexes_on_data_read = 0, max_threads = 1, use_text_index_header_cache = 0;
SELECT count() FROM t_text_index_read_once WHERE hasAllTokens(s, 's322') OR hasAllTokens(s, 's777') SETTINGS use_skip_indexes_on_data_read = 1, max_threads = 1, use_text_index_header_cache = 0;

SELECT id FROM t_text_index_read_once WHERE hasAllTokens(s, 's322') OR hasAllTokens(s, 's777') ORDER BY id SETTINGS use_skip_indexes_on_data_read = 0, max_threads = 1, use_text_index_header_cache = 0;
SELECT id FROM t_text_index_read_once WHERE hasAllTokens(s, 's322') OR hasAllTokens(s, 's777') ORDER BY id SETTINGS use_skip_indexes_on_data_read = 1, max_threads = 1, use_text_index_header_cache = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    'use_skip_indexes_on_data_read: ' || Settings['use_skip_indexes_on_data_read'],
    'ReadSparseIndexBlocks: ' || ProfileEvents['TextIndexReadSparseIndexBlocks'],
    'HeaderCacheHits: ' || ProfileEvents['TextIndexHeaderCacheHits'],
    'HeaderCacheMisses: ' || ProfileEvents['TextIndexHeaderCacheMisses']
FROM system.query_log
WHERE event_date >= yesterday()
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE 'SELECT%FROM t_text_index_read_once%'
ORDER BY event_time_microseconds;

DROP TABLE t_text_index_read_once;
