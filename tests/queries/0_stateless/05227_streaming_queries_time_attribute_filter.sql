-- Tags: no-parallel-replicas

SET enable_streaming_queries = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_streaming_time_attribute;

CREATE TABLE t_streaming_time_attribute (ts DateTime64(3), x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_streaming_time_attribute VALUES ('2020-01-01 00:00:10.000', 1), ('2020-01-01 00:00:20.000', 2), ('2020-01-01 00:00:30.000', 3);

-- The time attribute is computed by the reader, so it can be filtered in WHERE and in PREWHERE.
SELECT x, _time_attribute FROM t_streaming_time_attribute STREAM BOUNDED WATERMARK FOR ts AS ts WHERE _time_attribute > '2020-01-01 00:00:15.000';
SELECT x, _time_attribute FROM t_streaming_time_attribute STREAM BOUNDED WATERMARK FOR ts AS ts PREWHERE _time_attribute > '2020-01-01 00:00:25.000';

-- The time attribute is the WATERMARK FOR column itself.
SELECT count() FROM t_streaming_time_attribute STREAM BOUNDED WATERMARK FOR ts AS ts WHERE _time_attribute = ts;
