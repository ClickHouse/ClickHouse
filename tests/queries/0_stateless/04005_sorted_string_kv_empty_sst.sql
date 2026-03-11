-- Tags: no-random-merge-tree-settings
-- Test SortedStringKV data type with empty SST file scenarios

-- Test 1: Part becomes empty after DELETE, then insert again
-- This tests: wide part with SST file → DELETE all rows → empty SST → insert new data
SELECT 'Test 1: Empty SST after DELETE, then insert' AS test_name;
DROP TABLE IF EXISTS test_sorted_string_kv_empty_sst_1;
CREATE TABLE test_sorted_string_kv_empty_sst_1 (id UInt64, kv SortedStringKV) 
ENGINE = MergeTree ORDER BY kv.1
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1, index_granularity = 8192;

-- Insert rows to create wide part with SST file
INSERT INTO test_sorted_string_kv_empty_sst_1 VALUES (1, ('key1', 'value1')), (2, ('key2', 'value2'));
SELECT count() FROM test_sorted_string_kv_empty_sst_1;

-- Delete all rows, leaving empty SST file
DELETE FROM test_sorted_string_kv_empty_sst_1 WHERE 1 = 1;
SELECT count() FROM test_sorted_string_kv_empty_sst_1;

-- Insert again to verify table is still functional after empty SST
INSERT INTO test_sorted_string_kv_empty_sst_1 VALUES (3, ('key3', 'value3'));
SELECT count() FROM test_sorted_string_kv_empty_sst_1;
SELECT id, kv.1, kv.2 FROM test_sorted_string_kv_empty_sst_1 ORDER BY id;

DROP TABLE test_sorted_string_kv_empty_sst_1;

-- Test 2: Multiple parts with one becoming empty, then merge
-- This tests: multiple parts → DELETE from one part → empty part → OPTIMIZE merge
SELECT 'Test 2: Multiple parts with empty part merge' AS test_name;
DROP TABLE IF EXISTS test_sorted_string_kv_empty_sst_2;
CREATE TABLE test_sorted_string_kv_empty_sst_2 (id UInt64, kv SortedStringKV) 
ENGINE = MergeTree ORDER BY kv.1
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1, index_granularity = 8192;

-- Insert multiple small parts
INSERT INTO test_sorted_string_kv_empty_sst_2 VALUES (10, ('key10', 'value10'));
INSERT INTO test_sorted_string_kv_empty_sst_2 VALUES (20, ('key20', 'value20'));
INSERT INTO test_sorted_string_kv_empty_sst_2 VALUES (30, ('key30', 'value30'));

-- Delete one row to make one part empty
DELETE FROM test_sorted_string_kv_empty_sst_2 WHERE id = 20;

-- Verify remaining data before merge
SELECT count() FROM test_sorted_string_kv_empty_sst_2;
SELECT id, kv.1, kv.2 FROM test_sorted_string_kv_empty_sst_2 ORDER BY id;

-- Optimize to merge parts (including the empty one)
OPTIMIZE TABLE test_sorted_string_kv_empty_sst_2 FINAL;

-- Verify data integrity after merge
SELECT count() FROM test_sorted_string_kv_empty_sst_2;
SELECT id, kv.1, kv.2 FROM test_sorted_string_kv_empty_sst_2 ORDER BY id;

DROP TABLE test_sorted_string_kv_empty_sst_2;