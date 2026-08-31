-- Tags: no-parallel-replicas, no-old-analyzer

SET enable_streaming_queries = 1;
SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET query_plan_merge_filters = 1;

DROP TABLE IF EXISTS t_stream_watermark_push_down;

CREATE TABLE t_stream_watermark_push_down
(
    ts DateTime64(3),
    map Map(String, String),
    INDEX idx_map_keys mapKeys(map) TYPE text(tokenizer = 'array') GRANULARITY 1,
    INDEX idx_map_values mapValues(map) TYPE text(tokenizer = 'array') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_stream_watermark_push_down SELECT toDateTime64('2020-01-01 00:00:00', 3) + number, map('env', if(number < 3, 'prod', 'staging')) FROM numbers(100);

SELECT ts FROM (SELECT ts FROM t_stream_watermark_push_down STREAM BOUNDED WATERMARK FOR ts AS ts - toIntervalSecond(5) PREWHERE ('prod') IN (map[materialize('env')])) ORDER BY ts
SETTINGS log_comment = '05042_streaming_queries_watermark_filter_push_down';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['TextIndexUseHint'] > 0
FROM system.query_log
WHERE event_date >= yesterday()
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = '05042_streaming_queries_watermark_filter_push_down';
