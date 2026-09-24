-- WATERMARK FOR over Date, Date32 and DateTime columns; max_block_size = 1 carries the watermark from block to block.

DROP TABLE IF EXISTS t_streaming_watermark_types;

SET enable_streaming_queries = 1;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;

CREATE TABLE t_streaming_watermark_types (d Date, d32 Date32, dt DateTime('UTC'), x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_streaming_watermark_types SELECT toDate('2020-01-01') + number, toDate32('1900-01-01') + number, toDateTime('2020-01-01 00:00:00', 'UTC') + number, number FROM numbers(3);

SELECT x, _time_attribute FROM t_streaming_watermark_types STREAM BOUNDED WATERMARK FOR d AS d - INTERVAL 1 DAY ORDER BY x SETTINGS max_block_size = 1;
SELECT x, _time_attribute FROM t_streaming_watermark_types STREAM BOUNDED WATERMARK FOR d32 AS d32 ORDER BY x SETTINGS max_block_size = 1;
SELECT x, _time_attribute FROM t_streaming_watermark_types STREAM BOUNDED WATERMARK FOR dt AS dt - INTERVAL 10 SECOND ORDER BY x SETTINGS max_block_size = 1;

DROP TABLE t_streaming_watermark_types;
