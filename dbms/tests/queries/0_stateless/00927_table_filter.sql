DROP TABLE IF EXISTS filtered_table1;
DROP TABLE IF EXISTS filtered_table2;
DROP TABLE IF EXISTS filtered_table3;

-- Filter: a = 1, values: (1, 0), (1, 1)
CREATE TABLE test.filtered_table1 (a UInt8, b UInt8) ENGINE MergeTree ORDER BY a;
INSERT INTO test.filtered_table1 values (0, 0), (0, 1), (1, 0), (1, 1);

-- Filter: max(a + b, c - d) > 5, values: (4, 3, 2, 1), (0, 0, 6, 0)
CREATE TABLE test.filtered_table2 (a UInt8, b UInt8, c UInt8, d UInt8) ENGINE MergeTree ORDER BY a;
INSERT INTO test.filtered_table2 values (0, 0, 0, 0), (1, 2, 3, 4), (4, 3, 2, 1), (0, 0, 6, 0);

-- Filter: c = 1, values: (0, 1), (1, 0)
CREATE TABLE test.filtered_table3 (a UInt8, b UInt8, c UInt16 ALIAS a + b) ENGINE MergeTree ORDER BY a;
INSERT INTO test.filtered_table3 values (0, 0), (0, 1), (1, 0), (1, 1);

SELECT '-- PREWHERE should fail'
SELECT * FROM test.filtered_table1 PREWHERE 1;
SELECT * FROM test.filtered_table2 PREWHERE 1;
SELECT * FROM test.filtered_table3 PREWHERE 1;

SELECT * FROM test.filtered_table1;
SELECT * FROM test.filtered_table2;
SELECT * FROM test.filtered_table3;

DROP TABLE test.filtered_table1;
DROP TABLE test.filtered_table2;
DROP TABLE test.filtered_table3;
