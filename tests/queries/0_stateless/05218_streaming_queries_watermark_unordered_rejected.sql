-- Tags: no-parallel-replicas

SET enable_streaming_queries = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_streaming_watermark_unordered;

CREATE TABLE t_streaming_watermark_unordered (ts DateTime64(3), x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_streaming_watermark_unordered VALUES ('2020-01-01 00:00:10.000', 1), ('2020-01-01 00:00:20.000', 2);

SELECT x FROM t_streaming_watermark_unordered STREAM UNORDERED WATERMARK FOR ts AS ts - INTERVAL 10 SECOND; -- { serverError ILLEGAL_STREAM }
SELECT x FROM t_streaming_watermark_unordered STREAM BOUNDED UNORDERED WATERMARK FOR ts AS ts - INTERVAL 10 SECOND; -- { serverError ILLEGAL_STREAM }
SELECT x, _time_attribute FROM t_streaming_watermark_unordered STREAM BOUNDED UNORDERED WATERMARK FOR ts AS ts; -- { serverError ILLEGAL_STREAM }

SELECT x FROM t_streaming_watermark_unordered STREAM BOUNDED UNORDERED ORDER BY x;
SELECT x FROM t_streaming_watermark_unordered STREAM BOUNDED WATERMARK FOR ts AS ts - INTERVAL 10 SECOND ORDER BY x;
