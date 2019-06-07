-- check ALTER MODIFY COLUMN with partitions

SET send_logs_level = 'none';

DROP TABLE IF EXISTS test.alter_column;

CREATE TABLE test.alter_column(x UInt32, y Int32) ENGINE MergeTree PARTITION BY x ORDER BY x;
INSERT INTO test.alter_column (x, y) SELECT number AS x, -number AS y FROM system.numbers LIMIT 50;

SELECT '*** Check SHOW CREATE TABLE ***';
SHOW CREATE TABLE test.alter_column;

SELECT '*** Check parts ***';
SELECT * FROM test.alter_column ORDER BY _part;

ALTER TABLE test.alter_column MODIFY COLUMN y Int64;

SELECT '*** Check SHOW CREATE TABLE after ALTER MODIFY ***';
SHOW CREATE TABLE test.alter_column;

SELECT '*** Check parts after ALTER MODIFY ***';
SELECT * FROM test.alter_column ORDER BY _part;

DROP TABLE test.alter_column;
