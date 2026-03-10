-- Tags: no-random-merge-tree-settings
-- Test SortedStringKV data type with MergeTree engine

-- Test 1: Compact part with multiple granules (kv.1 as sort key prefix)
SELECT 'Test 1: Compact part - multiple granules and boundary reading' AS test_name;
DROP TABLE IF EXISTS test_sorted_string_kv_compact;
CREATE TABLE test_sorted_string_kv_compact (id UInt64, kv SortedStringKV) 
ENGINE = MergeTree ORDER BY (kv.1, id)
SETTINGS min_rows_for_wide_part = 1000000, min_bytes_for_wide_part = 1000000000, index_granularity = 8192;

-- Insert data spanning multiple granules
INSERT INTO test_sorted_string_kv_compact 
SELECT number, (concat('key', toString(number)), concat('value', toString(number * 10))) 
FROM numbers(10000);

-- Test reading across granule boundaries (8192)
SELECT count() FROM test_sorted_string_kv_compact;

-- Verify data content at granule boundaries (8191, 8192 are boundary)
-- Note: ClickHouse uses 0-based numbering for internal granule indexing
-- Granule 1: rows 0-8191 (8192 rows), Granule 2: rows 8192-16383
SELECT id, kv.1 AS key, kv.2 AS value FROM test_sorted_string_kv_compact 
WHERE id BETWEEN 8190 AND 8195 ORDER BY id;

-- Verify exact key-value pairs at granule boundaries
SELECT 
    (SELECT (kv.1, kv.2) FROM test_sorted_string_kv_compact WHERE id = 8191) = ('key8191', 'value81910') AS boundary_8191_correct,
    (SELECT (kv.1, kv.2) FROM test_sorted_string_kv_compact WHERE id = 8192) = ('key8192', 'value81920') AS boundary_8192_correct,
    (SELECT (kv.1, kv.2) FROM test_sorted_string_kv_compact WHERE id = 0) = ('key0', 'value0') AS first_row_correct,
    (SELECT (kv.1, kv.2) FROM test_sorted_string_kv_compact WHERE id = 9999) = ('key9999', 'value99990') AS last_row_correct;

-- Test filtering across granules
SELECT count() FROM test_sorted_string_kv_compact WHERE kv.1 LIKE 'key1%';

DROP TABLE test_sorted_string_kv_compact;

-- Test 2: Wide part with continuous_reading across granules (kv.1 as sort key)
SELECT 'Test 2: Wide part - multiple granules with continuous_reading' AS test_name;
DROP TABLE IF EXISTS test_sorted_string_kv_wide_multi;
CREATE TABLE test_sorted_string_kv_wide_multi (id UInt64, kv SortedStringKV) 
ENGINE = MergeTree ORDER BY kv.1
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1, index_granularity = 8192;

-- Insert data in multiple batches, then merge to create wide part
INSERT INTO test_sorted_string_kv_wide_multi 
SELECT number, (concat('key', toString(number)), concat('value', toString(number * 10))) 
FROM numbers(5000);

INSERT INTO test_sorted_string_kv_wide_multi 
SELECT number + 5000, (concat('key', toString(number + 5000)), concat('value', toString((number + 5000) * 10))) 
FROM numbers(5000);

OPTIMIZE TABLE test_sorted_string_kv_wide_multi FINAL;

-- Test continuous reading across granules
SELECT count() FROM test_sorted_string_kv_wide_multi;

-- Verify exact values at granule boundaries in wide part
SELECT id, kv.1 AS key, kv.2 AS value FROM test_sorted_string_kv_wide_multi 
WHERE id BETWEEN 8190 AND 8195 ORDER BY id;

-- Verify first, last, and cross-part boundary values (part boundary is at row 5000)
SELECT 
    (SELECT (kv.1, kv.2) FROM test_sorted_string_kv_wide_multi WHERE id = 0) = ('key0', 'value0') AS first_row_correct,
    (SELECT (kv.1, kv.2) FROM test_sorted_string_kv_wide_multi WHERE id = 4999) = ('key4999', 'value49990') AS part_boundary_start_correct,
    (SELECT (kv.1, kv.2) FROM test_sorted_string_kv_wide_multi WHERE id = 5000) = ('key5000', 'value50000') AS part_boundary_end_correct,
    (SELECT (kv.1, kv.2) FROM test_sorted_string_kv_wide_multi WHERE id = 9999) = ('key9999', 'value99990') AS last_row_correct,
    (SELECT (kv.1, kv.2) FROM test_sorted_string_kv_wide_multi WHERE id = 8191) = ('key8191', 'value81910') AS granule_boundary_correct;

-- Verify that all key-value pairs match expected pattern
SELECT count() = 10000 AS all_rows_have_correct_pattern
FROM test_sorted_string_kv_wide_multi 
WHERE kv.1 = concat('key', toString(id)) AND kv.2 = concat('value', toString(id * 10));

-- Test sequential scan
SELECT sum(toUInt64OrZero(kv.2)) FROM test_sorted_string_kv_wide_multi WHERE id < 100;

DROP TABLE test_sorted_string_kv_wide_multi;

-- Test 3: Wide part - continuous_reading with multiple parts (kv.1 as sort key)
SELECT 'Test 3: Wide part - continuous_reading across multiple parts' AS test_name;
DROP TABLE IF EXISTS test_sorted_string_kv_wide_parts;
CREATE TABLE test_sorted_string_kv_wide_parts (id UInt64, kv SortedStringKV) 
ENGINE = MergeTree ORDER BY kv.1
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1, index_granularity = 8192;

-- Insert multiple small parts
INSERT INTO test_sorted_string_kv_wide_parts VALUES (1, ('part1_key1', 'part1_val1'));
INSERT INTO test_sorted_string_kv_wide_parts VALUES (2, ('part1_key2', 'part1_val2'));
INSERT INTO test_sorted_string_kv_wide_parts VALUES (100, ('part2_key1', 'part2_val1'));
INSERT INTO test_sorted_string_kv_wide_parts VALUES (200, ('part3_key1', 'part3_val1'));

-- Read from multiple parts
SELECT id, kv.1 AS key, kv.2 AS value FROM test_sorted_string_kv_wide_parts ORDER BY id;

-- Merge and read again
OPTIMIZE TABLE test_sorted_string_kv_wide_parts FINAL;
SELECT id, kv.1 AS key, kv.2 AS value FROM test_sorted_string_kv_wide_parts ORDER BY id;

-- Verify data integrity after merge
SELECT 
    (SELECT count() FROM test_sorted_string_kv_wide_parts) = 4 AS row_count_correct,
    (SELECT groupArray((id, kv.1, kv.2)) FROM test_sorted_string_kv_wide_parts) = [(1, 'part1_key1', 'part1_val1'), (2, 'part1_key2', 'part1_val2'), (100, 'part2_key1', 'part2_val1'), (200, 'part3_key1', 'part3_val1')] AS merged_data_correct;

DROP TABLE test_sorted_string_kv_wide_parts;

-- Test 4: SortedStringKV with composite primary key (date, kv.1)
SELECT 'Test 4: SortedStringKV with composite primary key' AS test_name;
DROP TABLE IF EXISTS test_sorted_string_kv_pk;
CREATE TABLE test_sorted_string_kv_pk (id UInt64, kv SortedStringKV, date Date) 
ENGINE = MergeTree ORDER BY (date, kv.1)
SETTINGS index_granularity = 8192;

INSERT INTO test_sorted_string_kv_pk VALUES 
    (1, ('sensor1', '25.5'), '2024-01-01'),
    (2, ('sensor2', '30.0'), '2024-01-01'),
    (3, ('sensor3', '28.3'), '2024-01-02');

SELECT id, kv.1 AS sensor, kv.2 AS reading, date FROM test_sorted_string_kv_pk ORDER BY date, id;

SELECT id, kv.1 AS sensor, kv.2 AS reading FROM test_sorted_string_kv_pk WHERE date = '2024-01-01' ORDER BY id;

DROP TABLE test_sorted_string_kv_pk;

-- Test 5: Empty SortedStringKV column after DELETE (kv.1 as sort key)
SELECT 'Test 5: Empty SortedStringKV column after DELETE' AS test_name;
DROP TABLE IF EXISTS test_sorted_string_kv_empty;
CREATE TABLE test_sorted_string_kv_empty (id UInt64, kv SortedStringKV) 
ENGINE = MergeTree ORDER BY kv.1
SETTINGS index_granularity = 8192;

INSERT INTO test_sorted_string_kv_empty SELECT number, (toString(number), toString(number * 2)) FROM numbers(10);

DELETE FROM test_sorted_string_kv_empty WHERE 1 = 1;

SELECT count() FROM test_sorted_string_kv_empty;

DROP TABLE test_sorted_string_kv_empty;

-- Test 6: Large dataset with multiple granules (kv.1 as sort key)
SELECT 'Test 6: Large dataset - multiple granules and filtering' AS test_name;
DROP TABLE IF EXISTS test_sorted_string_kv_large;
CREATE TABLE test_sorted_string_kv_large (id UInt64, kv SortedStringKV) 
ENGINE = MergeTree ORDER BY kv.1
SETTINGS index_granularity = 8192;

-- Insert data spanning multiple granules (25000 rows = ~3 granules)
INSERT INTO test_sorted_string_kv_large SELECT number, (concat('k', toString(number)), concat('v', toString(number * 100))) FROM numbers(25000);

SELECT count() FROM test_sorted_string_kv_large;

-- Test reading across multiple granules
SELECT count() FROM test_sorted_string_kv_large WHERE id BETWEEN 8190 AND 8200;

-- Verify granule boundary values in large dataset
-- Granule 1 ends at 8191, Granule 2 ends at 16383, Granule 3 ends at 24575
SELECT 
    (SELECT (kv.1, kv.2) FROM test_sorted_string_kv_large WHERE id = 8191) = ('k8191', 'v819100') AS granule_1_boundary_correct,
    (SELECT (kv.1, kv.2) FROM test_sorted_string_kv_large WHERE id = 8192) = ('k8192', 'v819200') AS granule_2_start_correct,
    (SELECT (kv.1, kv.2) FROM test_sorted_string_kv_large WHERE id = 16383) = ('k16383', 'v1638300') AS granule_2_boundary_correct,
    (SELECT (kv.1, kv.2) FROM test_sorted_string_kv_large WHERE id = 16384) = ('k16384', 'v1638400') AS granule_3_start_correct,
    (SELECT (kv.1, kv.2) FROM test_sorted_string_kv_large WHERE id = 24999) = ('k24999', 'v2499900') AS last_row_correct;

-- Verify random sampling of data integrity across all granules
SELECT count() = 25000 AS all_rows_match_pattern
FROM test_sorted_string_kv_large 
WHERE kv.1 = concat('k', toString(id)) AND kv.2 = concat('v', toString(id * 100));

-- Test filtering on large dataset
SELECT count() FROM test_sorted_string_kv_large WHERE kv.1 LIKE 'k1%';

DROP TABLE test_sorted_string_kv_large;

-- Test 7: Multiple SortedStringKV columns in same table (kv1.1 as sort key)
SELECT 'Test 7: Multiple SortedStringKV columns in same table' AS test_name;
DROP TABLE IF EXISTS test_sorted_string_kv_multi;
CREATE TABLE test_sorted_string_kv_multi (id UInt64, kv1 SortedStringKV, kv2 SortedStringKV) 
ENGINE = MergeTree ORDER BY kv1.1
SETTINGS index_granularity = 8192;

INSERT INTO test_sorted_string_kv_multi VALUES 
    (1, ('key1', 'val1'), ('a', 'b')),
    (2, ('key2', 'val2'), ('c', 'd'));

SELECT id, kv1.1 AS k1, kv1.2 AS v1, kv2.1 AS k2, kv2.2 AS v2 FROM test_sorted_string_kv_multi ORDER BY id;

DROP TABLE test_sorted_string_kv_multi;

-- Test 8: NEGATIVE TEST - Wrong sort order (should fail or produce incorrect results)
-- This test intentionally uses ORDER BY id instead of kv.key to demonstrate
-- that SortedStringKV requires the key column to be part of the sort key prefix.
SELECT 'Test 8: NEGATIVE TEST - Wrong sort order (id instead of kv.1)' AS test_name;
DROP TABLE IF EXISTS test_sorted_string_kv_basic;
CREATE TABLE test_sorted_string_kv_basic (id UInt64, kv SortedStringKV) 
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1, index_granularity = 8192;

INSERT INTO test_sorted_string_kv_basic VALUES 
    (1, ('name', 'Alice')),
    (2, ('city', 'Beijing')),
    (3, ('country', 'China')); -- { serverError INCORRECT_DATA }

DROP TABLE test_sorted_string_kv_basic;