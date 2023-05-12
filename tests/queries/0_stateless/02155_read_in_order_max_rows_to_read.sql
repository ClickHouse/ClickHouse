DROP TABLE IF EXISTS t_max_rows_to_read;

CREATE TABLE t_max_rows_to_read (a UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 4;

INSERT INTO t_max_rows_to_read SELECT number FROM numbers(100);

SET max_threads = 1;
SET optimize_read_in_order = 1;

SELECT a FROM t_max_rows_to_read WHERE a = 10 SETTINGS max_rows_to_read = 4;

SELECT a FROM t_max_rows_to_read ORDER BY a LIMIT 5 SETTINGS max_rows_to_read = 12;

-- This should work, but actually it doesn't. Need to investigate.
-- SELECT a FROM t_max_rows_to_read WHERE a > 10 ORDER BY a LIMIT 5 SETTINGS max_rows_to_read = 20;

SELECT a FROM t_max_rows_to_read ORDER BY a LIMIT 20 FORMAT Null SETTINGS max_rows_to_read = 12; -- { serverError 158 }
SELECT a FROM t_max_rows_to_read WHERE a > 10 ORDER BY a LIMIT 5 FORMAT Null SETTINGS max_rows_to_read = 12; -- { serverError 158 }
SELECT a FROM t_max_rows_to_read WHERE a = 10 OR a = 20 FORMAT Null SETTINGS max_rows_to_read = 4; -- { serverError 158 }

DROP TABLE t_max_rows_to_read;
