DROP TABLE IF EXISTS t_max_rows_to_read;

CREATE TABLE t_max_rows_to_read (a UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';

INSERT INTO t_max_rows_to_read SELECT number FROM numbers(100);

SET max_block_size = 10;
SET max_rows_to_read = 20;
SET read_overflow_mode = 'throw';

SELECT number FROM numbers(30); -- { serverError 158 }
SELECT number FROM numbers(30) LIMIT 21; -- { serverError 158 }
SELECT number FROM numbers(30) LIMIT 1;
SELECT number FROM numbers(5);

SELECT a FROM t_max_rows_to_read LIMIT 1;
SELECT a FROM t_max_rows_to_read LIMIT 11 offset 11; -- { serverError 158 }
SELECT a FROM t_max_rows_to_read WHERE a > 50 LIMIT 1; -- { serverError 158 }

DROP TABLE t_max_rows_to_read;
