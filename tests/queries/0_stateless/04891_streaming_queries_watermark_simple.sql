-- Tags: no-parallel-replicas

SET enable_streaming_queries = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_streaming_watermark_simple;

CREATE TABLE t_streaming_watermark_simple (ts DateTime64(3), x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_streaming_watermark_simple VALUES ('2020-01-01 00:00:10.000', 1), ('2020-01-01 00:00:20.000', 2);
INSERT INTO t_streaming_watermark_simple VALUES ('2020-01-01 00:00:30.000', 3);

-- Bounded read terminates after the first snapshot; the watermark clause does not change the data.
SELECT x FROM t_streaming_watermark_simple STREAM BOUNDED WATERMARK FOR ts AS ts - INTERVAL 10 SECOND;

-- Per-row time attribute virtual column.
SELECT x, _time_attribute FROM t_streaming_watermark_simple STREAM BOUNDED WATERMARK FOR ts AS ts - INTERVAL 10 SECOND;
