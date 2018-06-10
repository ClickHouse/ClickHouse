DROP TABLE IF EXISTS test.columns;
CREATE TABLE test.columns (a UInt8, b UInt8, c UInt8) ENGINE = Memory;
INSERT INTO test.columns VALUES (1, 2, 3);
SET max_columns_to_read = 1;

SELECT a FROM (SELECT * FROM test.columns);
SELECT a FROM (SELECT * FROM (SELECT * FROM test.columns));
SELECT a FROM (SELECT * FROM test.columns UNION ALL SELECT * FROM test.columns);

DROP TABLE test.columns;
