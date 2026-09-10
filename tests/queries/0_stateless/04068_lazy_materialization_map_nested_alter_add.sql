-- Tags: no-fasttest

-- Coverage: Map and Nested types added via ALTER ADD COLUMN + lazy materialization
-- These types use SerializationArray::deserializeOffsetsBinaryBulkAndGetNestedOffsetAndLimit
-- which returns {0,0} for missing streams (columns not present in old parts).

-- Test 1: Map type
DROP TABLE IF EXISTS test_lm_map;
CREATE TABLE test_lm_map (id UInt64) ENGINE=MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part=1, min_rows_for_wide_part=1;
INSERT INTO test_lm_map SELECT * FROM numbers(100);
ALTER TABLE test_lm_map ADD COLUMN m Map(String, UInt64) SETTINGS mutations_sync=1;
SELECT id, m FROM test_lm_map WHERE id = 42 ORDER BY id LIMIT 10 SETTINGS query_plan_optimize_lazy_materialization = 1;

-- Also with data in new parts
INSERT INTO test_lm_map SELECT number, map('a', number) FROM numbers(100, 50);
SELECT id, m FROM test_lm_map WHERE id IN (42, 120) ORDER BY id SETTINGS query_plan_optimize_lazy_materialization = 1;
DROP TABLE test_lm_map;

-- Test 2: Nested type
DROP TABLE IF EXISTS test_lm_nested;
CREATE TABLE test_lm_nested (id UInt64) ENGINE=MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part=1, min_rows_for_wide_part=1;
INSERT INTO test_lm_nested SELECT * FROM numbers(100);
ALTER TABLE test_lm_nested ADD COLUMN n Nested(key String, value UInt64) SETTINGS mutations_sync=1;
SELECT id, n.key, n.value FROM test_lm_nested WHERE id = 42 ORDER BY id LIMIT 10 SETTINGS query_plan_optimize_lazy_materialization = 1;
DROP TABLE test_lm_nested;

-- Test 3: Array(Array(UInt64)) - nested arrays
DROP TABLE IF EXISTS test_lm_nested_arr;
CREATE TABLE test_lm_nested_arr (id UInt64) ENGINE=MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part=1, min_rows_for_wide_part=1;
INSERT INTO test_lm_nested_arr SELECT * FROM numbers(100);
ALTER TABLE test_lm_nested_arr ADD COLUMN arr Array(Array(UInt64)) SETTINGS mutations_sync=1;
SELECT id, arr FROM test_lm_nested_arr WHERE id = 42 ORDER BY id LIMIT 10 SETTINGS query_plan_optimize_lazy_materialization = 1;
DROP TABLE test_lm_nested_arr;

-- Test 4: Multiple array-like columns added at once
DROP TABLE IF EXISTS test_lm_multi;
CREATE TABLE test_lm_multi (id UInt64) ENGINE=MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part=1, min_rows_for_wide_part=1;
INSERT INTO test_lm_multi SELECT * FROM numbers(100);
ALTER TABLE test_lm_multi ADD COLUMN arr Array(UInt64), ADD COLUMN m Map(String, String) SETTINGS mutations_sync=1;
SELECT id, arr, m FROM test_lm_multi WHERE id = 42 ORDER BY id LIMIT 10 SETTINGS query_plan_optimize_lazy_materialization = 1;
DROP TABLE test_lm_multi;
