SET enable_streaming_queries = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_streaming_watermark_params;

CREATE TABLE t_streaming_watermark_params (ts DateTime64(3), x UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

SELECT * FROM t_streaming_watermark_params STREAM WATERMARK FOR ts AS {wm:DateTime64(3)}; -- { serverError UNKNOWN_QUERY_PARAMETER }

SET param_wm = '2020-01-01 00:00:00.000';
SELECT * FROM t_streaming_watermark_params STREAM WATERMARK FOR ts AS {wm:DateTime64(3)}; -- { serverError NOT_IMPLEMENTED }

SET param_delay = 5;
SELECT * FROM t_streaming_watermark_params STREAM WATERMARK FOR ts AS ts - toIntervalSecond({delay:UInt64}); -- { serverError NOT_IMPLEMENTED }

SET param_bad = 42;
SELECT * FROM t_streaming_watermark_params STREAM WATERMARK FOR ts AS {bad:UInt64}; -- { serverError ILLEGAL_STREAM }

DROP TABLE t_streaming_watermark_params;
