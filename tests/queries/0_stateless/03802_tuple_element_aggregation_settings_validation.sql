-- Test case 1: Error path test - Verify that enabling allow_tuple_element_aggregation on unsupported engines throws error
-- Test with ReplacingMergeTree engine
CREATE TABLE test_replacing_engine (`n` Tuple(a UInt32, b UInt32)) ENGINE = ReplacingMergeTree ORDER BY tuple() SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }

-- Test with CollapsingMergeTree engine
CREATE TABLE test_collapsing_engine (`n` Tuple(a UInt32, b UInt32), `sign` Int8) ENGINE = CollapsingMergeTree(sign) ORDER BY tuple() SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }

-- Test with regular MergeTree engine
CREATE TABLE test_normal_engine (`n` Tuple(a UInt32, b UInt32)) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }

-- Test case 2: Order by tuple validation - Verify that creating table with regular tuple in order by throws error
CREATE TABLE test_tuple_order (`n` Tuple(a UInt32, b UInt32)) ENGINE = SummingMergeTree ORDER BY n SETTINGS allow_tuple_element_aggregation = 1; -- { serverError BAD_ARGUMENTS }

-- Test case 3: ALTER TABLE read-only validation - Verify that ALTER TABLE cannot modify allow_tuple_element_aggregation setting
CREATE TABLE test_alter_table (`n` Tuple(a UInt32, b UInt32)) ENGINE = MergeTree ORDER BY tuple();

-- Attempt to modify allow_tuple_element_aggregation setting after table creation
ALTER TABLE test_alter_table MODIFY SETTING allow_tuple_element_aggregation = 1; -- { serverError READONLY_SETTING }

-- Clean up
DROP TABLE IF EXISTS test_alter_table;