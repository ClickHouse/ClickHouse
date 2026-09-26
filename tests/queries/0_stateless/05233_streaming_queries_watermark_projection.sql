DROP TABLE IF EXISTS t_streaming_watermark_projection;
SET enable_streaming_queries = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET enable_analyzer = 1;
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_streaming_watermark_projection;

CREATE TABLE t_streaming_watermark_projection
(
    ts DateTime64(3),
    x UInt64,
    lag UInt64,
    PROJECTION commit_order (SELECT x, ts, _block_number, _block_offset ORDER BY _block_number, _block_offset),
    PROJECTION by_lag (SELECT lag, count() GROUP BY lag)
)
ENGINE = MergeTree ORDER BY x
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1,
         part_minmax_index_columns = 'with_block_number_offset',
         add_minmax_index_for_block_number_column = 1, add_minmax_index_for_block_offset_column = 1;

INSERT INTO t_streaming_watermark_projection SELECT toDateTime64('2020-01-01 00:00:00', 3) + number, number, number % 3 FROM numbers(6);

ALTER TABLE t_streaming_watermark_projection MATERIALIZE PROJECTION commit_order;

SELECT x, _time_attribute FROM t_streaming_watermark_projection STREAM BOUNDED WATERMARK FOR ts AS ts - INTERVAL 1 SECOND ORDER BY x;

-- The watermark expression reads a column the commit-order projection does not store.
SELECT x, _time_attribute FROM t_streaming_watermark_projection STREAM BOUNDED WATERMARK FOR ts AS ts - toIntervalSecond(lag) ORDER BY x;

SELECT count() FROM t_streaming_watermark_projection STREAM BOUNDED WATERMARK FOR ts AS ts - toIntervalSecond(lag) WHERE _time_attribute = ts;

DROP TABLE t_streaming_watermark_projection;
