set allow_experimental_streaming = 1;
set allow_experimental_analyzer = 1;

SELECT 'start';

DROP TABLE IF EXISTS t_streaming_test;
CREATE TABLE t_streaming_test (a String, b UInt64) ENGINE = MergeTree ORDER BY a SETTINGS queue_mode=1;

-- just to be sure
SELECT 1 STREAM; -- { clientError 62 }

-- system tables does not support streaming
SELECT 1 FROM numbers(2) STREAM; -- { serverError ILLEGAL_STREAM }

-- creating sets from streaming subquery is forbidden
SELECT 1 IN (SELECT b FROM t_streaming_test STREAM); -- { serverError FORBID_FOR_STREAMING_QUERIES }

-- prewhere is forbidden
SELECT * FROM t_streaming_test STREAM PREWHERE b % 2 = 0; -- { serverError ILLEGAL_PREWHERE}

DROP TABLE t_streaming_test;

SELECT 'end';
