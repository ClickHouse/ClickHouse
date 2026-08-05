-- Tags: no-parallel-replicas, no-old-analyzer

SET enable_streaming_queries = 1;
SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET query_plan_merge_filters = 1;

DROP TABLE IF EXISTS t_stream_text_index;

CREATE TABLE t_stream_text_index
(
    id UInt64,
    map Map(String, String),
    INDEX idx_map_keys mapKeys(map) TYPE text(tokenizer = 'array') GRANULARITY 1,
    INDEX idx_map_values mapValues(map) TYPE text(tokenizer = 'array') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_stream_text_index SELECT number, map('env', if(number < 3, 'prod', 'staging')) FROM numbers(100);

SELECT id FROM (SELECT id FROM t_stream_text_index STREAM PREWHERE ('prod') IN (map[materialize('env')]) LIMIT 3) ORDER BY id
SETTINGS log_comment = '04235_streaming_queries_text_index_direct_read';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['TextIndexUseHint'] > 0
FROM system.query_log
WHERE event_date >= yesterday()
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = '04235_streaming_queries_text_index_direct_read';

DROP TABLE t_stream_text_index;
