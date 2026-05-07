-- Tags: no-fasttest

DROP TABLE IF EXISTS test_check_json_wide;

CREATE TABLE test_check_json_wide (id UInt64, data JSON)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part=1, min_bytes_for_wide_part=1;

INSERT INTO test_check_json_wide VALUES (1, '{"a": 1, "b": "hello"}');
INSERT INTO test_check_json_wide VALUES (2, '{"a": 2, "c": [1, 2, 3]}');
INSERT INTO test_check_json_wide VALUES (3, '{"a": 3, "b": "world", "d": [{"nested": true}]}');

CHECK TABLE test_check_json_wide SETTINGS check_query_single_value_result = 1;

OPTIMIZE TABLE test_check_json_wide FINAL;

CHECK TABLE test_check_json_wide SETTINGS check_query_single_value_result = 1;

SELECT id, data.a FROM test_check_json_wide ORDER BY id;

-- Test that DETACH/ATTACH works (checkConsistency is called during attach).
ALTER TABLE test_check_json_wide DETACH PARTITION tuple();
ALTER TABLE test_check_json_wide ATTACH PARTITION tuple();

SELECT id, data.a FROM test_check_json_wide ORDER BY id;

DROP TABLE test_check_json_wide;
