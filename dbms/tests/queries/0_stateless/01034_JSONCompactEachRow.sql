DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table (value UInt8, name String) ENGINE = MergeTree() ORDER BY value;
INSERT INTO test_table VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT * FROM test_table FORMAT JSONCompactEachRow;
SELECT name, count() AS c FROM test_table GROUP BY name WITH TOTALS ORDER BY name FORMAT JSONCompactEachRow;
DROP TABLE IF EXISTS test_table;
