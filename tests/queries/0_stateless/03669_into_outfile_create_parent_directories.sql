-- Test 1: Default behavior (setting = 0) - should fail when directory doesn't exist
DROP TABLE IF EXISTS test_outfile;
CREATE TABLE test_outfile (id UInt32, value String) ENGINE = Memory;
INSERT INTO test_outfile VALUES (1, 'test1'), (2, 'test2'), (3, 'test3');

-- This should fail because the directory doesn't exist
SELECT * FROM test_outfile INTO OUTFILE '/tmp/clickhouse_test_outfile/level1/level2/output.csv' FORMAT CSV; -- { serverError INTO_OUTFILE_NOT_ALLOWED }

-- Test 2: Enable setting - should create parent directories
SELECT * FROM test_outfile INTO OUTFILE '/tmp/clickhouse_test_outfile/level1/level2/output.csv' FORMAT CSV
SETTINGS into_outfile_create_parent_directories = 1;

-- Verify file was created
-- :check_file_exists /tmp/clickhouse_test_outfile/level1/level2/output.csv

-- Test 3: Multiple nested levels
SELECT * FROM test_outfile INTO OUTFILE '/tmp/clickhouse_test_outfile/a/b/c/d/output.tsv' FORMAT TSV
SETTINGS into_outfile_create_parent_directories = 1;

-- Test 4: File in current directory (no parent to create)
SELECT * FROM test_outfile INTO OUTFILE '/tmp/simple_output.csv' FORMAT CSV
SETTINGS into_outfile_create_parent_directories = 1;

-- Test 5: With compression
SELECT * FROM test_outfile INTO OUTFILE '/tmp/clickhouse_test_outfile/compressed/output.csv.gz' COMPRESSION 'gzip' FORMAT CSV
SETTINGS into_outfile_create_parent_directories = 1;

-- Test 6: Different formats
SELECT * FROM test_outfile INTO OUTFILE '/tmp/clickhouse_test_outfile/formats/output.json' FORMAT JSONEachRow
SETTINGS into_outfile_create_parent_directories = 1;

SELECT * FROM test_outfile INTO OUTFILE '/tmp/clickhouse_test_outfile/formats/output.parquet' FORMAT Parquet
SETTINGS into_outfile_create_parent_directories = 1;

-- Test 7: Directory already exists - should not fail
SELECT * FROM test_outfile INTO OUTFILE '/tmp/clickhouse_test_outfile/level1/output2.csv' FORMAT CSV
SETTINGS into_outfile_create_parent_directories = 1;

-- Test 8: Empty result set
SELECT * FROM test_outfile WHERE id > 100 INTO OUTFILE '/tmp/clickhouse_test_outfile/empty/output.csv' FORMAT CSV
SETTINGS into_outfile_create_parent_directories = 1;

-- Test 9: With WHERE clause
SELECT * FROM test_outfile WHERE id = 1 INTO OUTFILE '/tmp/clickhouse_test_outfile/filtered/output.csv' FORMAT CSV
SETTINGS into_outfile_create_parent_directories = 1;

-- Test 10: Complex query
SELECT id, value, length(value) as len FROM test_outfile ORDER BY id 
INTO OUTFILE '/tmp/clickhouse_test_outfile/complex/output.csv' FORMAT CSV
SETTINGS into_outfile_create_parent_directories = 1;

-- Clean up
DROP TABLE test_outfile;