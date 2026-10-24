DROP TABLE IF EXISTS t_streaming_watermark_limit;
SET enable_streaming_queries = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_streaming_watermark_limit;

CREATE TABLE t_streaming_watermark_limit (ts DateTime64(3), x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_streaming_watermark_limit SELECT toDateTime64('2020-01-01 00:00:00', 3) + number, number FROM numbers(10000);

-- A LIMIT finishes the read before the stream is drained; the query must terminate.
SELECT x FROM t_streaming_watermark_limit STREAM BOUNDED WATERMARK FOR ts AS ts - INTERVAL 5 SECOND LIMIT 3;
SELECT count() FROM (SELECT x FROM t_streaming_watermark_limit STREAM BOUNDED WATERMARK FOR ts AS ts LIMIT 1);
SELECT count() FROM (SELECT x FROM t_streaming_watermark_limit STREAM BOUNDED WATERMARK FOR ts AS ts PREWHERE x % 2 = 0 LIMIT 5);

DROP TABLE t_streaming_watermark_limit;
