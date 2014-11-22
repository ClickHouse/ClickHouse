DROP TABLE IF EXISTS test.insert_fewer_columns;
CREATE TABLE test.insert_fewer_columns (a UInt8, b UInt8) ENGINE = Memory;
INSERT INTO test.insert_fewer_columns (a) VALUES (1), (2);
SELECT * FROM test.insert_fewer_columns;
DROP TABLE test.insert_fewer_columns;
