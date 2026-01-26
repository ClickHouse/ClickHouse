-- Tags: no-fasttest

-- Test for issue #53157: INSERT INTO ... SELECT ... WHERE should work
-- when WHERE clause references columns not in the destination table.

-- Setup: create file with two columns
INSERT INTO FUNCTION file('03811_data.parquet') SELECT 1 as c1, 2 as c2 SETTINGS engine_file_truncate_on_insert=1;

-- Destination table with only c1
DROP TABLE IF EXISTS test_03811;
CREATE TABLE test_03811 (c1 Int32) ENGINE = Memory;

-- Test: SELECT c1 with WHERE on c2 (the issue from #53157)
SET use_structure_from_insertion_table_in_table_functions = 2;
INSERT INTO test_03811 SELECT c1 FROM file('03811_data.parquet') WHERE c2 > 0;

SELECT * FROM test_03811;

-- Test: ORDER BY extra column
TRUNCATE TABLE test_03811;
INSERT INTO test_03811 SELECT c1 FROM file('03811_data.parquet') ORDER BY c2;
SELECT * FROM test_03811;

-- Test: verify type from destination table is used (Int32, not inferred UInt8)
TRUNCATE TABLE test_03811;
INSERT INTO test_03811 SELECT c1 FROM file('03811_data.parquet');
SELECT toTypeName(c1) FROM test_03811 LIMIT 1;

-- Cleanup
DROP TABLE test_03811;
