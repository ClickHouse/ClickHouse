DROP TABLE IF EXISTS test.insert_fewer_columns;
CREATE TABLE test.insert_fewer_columns (a UInt8, b UInt8) ENGINE = Memory;
INSERT INTO test.insert_fewer_columns (a) VALUES (1), (2);
SELECT * FROM test.insert_fewer_columns;

-- Test position arguments in insert.
DROP TABLE IF EXISTS test.insert_fewer_columns_2;
CREATE TABLE test.insert_fewer_columns_2 (b UInt8, a UInt8) ENGINE = Memory;
INSERT INTO test.insert_fewer_columns_2 SELECT * FROM test.insert_fewer_columns;
SELECT a, b FROM test.insert_fewer_columns;
SELECT a, b FROM test.insert_fewer_columns_2;

DROP TABLE IF EXISTS test.insert_fewer_columns_2;
DROP TABLE test.insert_fewer_columns;
