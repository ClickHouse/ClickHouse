-- Tags: no-fasttest

SET allow_experimental_json_type = 1;

DROP TABLE IF EXISTS t_check_json_wide;

CREATE TABLE t_check_json_wide (id UInt64, data JSON)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1;

-- Insert rows with different JSON structures to create parts with varying dynamic substreams.
INSERT INTO t_check_json_wide VALUES (1, '{"a": 1}');
INSERT INTO t_check_json_wide VALUES (2, '{"a": 2, "b": [1, 2, 3]}');
INSERT INTO t_check_json_wide VALUES (3, '{"a": 3, "c": {"d": "hello"}}');

-- CHECK TABLE should succeed on multiple Wide parts with different dynamic substreams.
CHECK TABLE t_check_json_wide SETTINGS check_query_single_value_result = 1;

-- Merge parts and check again.
OPTIMIZE TABLE t_check_json_wide FINAL;

CHECK TABLE t_check_json_wide SETTINGS check_query_single_value_result = 1;

-- Verify data correctness.
SELECT id, data.a FROM t_check_json_wide ORDER BY id;

-- DETACH/ATTACH triggers doCheckConsistency.
ALTER TABLE t_check_json_wide DETACH PARTITION tuple();
ALTER TABLE t_check_json_wide ATTACH PARTITION tuple();

-- Verify data after reattach.
SELECT id, data.a FROM t_check_json_wide ORDER BY id;

DROP TABLE t_check_json_wide;
