-- A column named like the watermark expression (as CREATE TABLE ... AS SELECT toDateTime(ts) names it) is not that expression.

SET enable_streaming_queries = 1;

CREATE TABLE t_wm (ts DateTime, x UInt8, `toDateTime(ts)` UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_wm VALUES ('2020-01-01 00:00:10', 1, 7), ('2020-01-01 00:00:20', 2, 9);

SELECT x, _time_attribute = ts FROM t_wm STREAM BOUNDED WATERMARK FOR ts AS toDateTime(ts) ORDER BY x;

DROP TABLE t_wm;
