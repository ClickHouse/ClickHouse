DROP TABLE IF EXISTS test;
CREATE TABLE test (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1000;
INSERT INTO test SELECT * FROM numbers(1000000);
OPTIMIZE TABLE test;

SET max_rows_to_read = 2000;
SELECT count() FROM test WHERE x = 100000;
SET max_rows_to_read = 1000000;
SELECT count() FROM test WHERE x != 100000;
SET max_rows_to_read = 101000;
SELECT count() FROM test WHERE x < 100000;
SET max_rows_to_read = 900000;
SELECT count() FROM test WHERE x > 100000;
SET max_rows_to_read = 101000;
SELECT count() FROM test WHERE x <= 100000;
SET max_rows_to_read = 901000;
SELECT count() FROM test WHERE x >= 100000;

SET max_rows_to_read = 2000;
SELECT count() FROM test WHERE x = '100000';
SET max_rows_to_read = 1000000;
SELECT count() FROM test WHERE x != '100000';
SET max_rows_to_read = 101000;
SELECT count() FROM test WHERE x < '100000';
SET max_rows_to_read = 900000;
SELECT count() FROM test WHERE x > '100000';
SET max_rows_to_read = 101000;
SELECT count() FROM test WHERE x <= '100000';
SET max_rows_to_read = 901000;
SELECT count() FROM test WHERE x >= '100000';

DROP TABLE test;
