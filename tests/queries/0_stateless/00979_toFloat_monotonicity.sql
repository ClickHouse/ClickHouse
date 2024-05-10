DROP TABLE IF EXISTS test1;
DROP TABLE IF EXISTS test2;
DROP TABLE IF EXISTS test3;

CREATE TABLE test1 (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS index_granularity = 1;
CREATE TABLE test2 (s String) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;
CREATE TABLE test3 (d Decimal(4, 3)) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1;

INSERT INTO test1 SELECT * FROM numbers(10000);
SELECT n FROM test1 WHERE toFloat64(n) = 7777.0 SETTINGS max_rows_to_read = 2;
SELECT n FROM test1 WHERE toFloat32(n) = 7777.0 SETTINGS max_rows_to_read = 2;

INSERT INTO test2 SELECT toString(number) FROM numbers(10000);
SELECT s FROM test2 WHERE toFloat64(s) = 7777.0;
SELECT s FROM test2 WHERE toFloat32(s) = 7777.0;

INSERT INTO test3 SELECT toDecimal64(number, 3) FROM numbers(10000);
SELECT d FROM test3 WHERE toFloat64(d) = 7777.0 SETTINGS max_rows_to_read = 2;
SELECT d FROM test3 WHERE toFloat32(d) = 7777.0 SETTINGS max_rows_to_read = 2;

DROP TABLE test1;
DROP TABLE test2;
DROP TABLE test3;
