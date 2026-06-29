-- Test that format settings are respected during internal CAST for JSON type
SET enable_json_type = 1;

-- Direct CAST should respect date_time_input_format setting
SELECT '{"d" : "2024 April 4"}'::JSON AS json, JSONAllPathsWithTypes(json) SETTINGS date_time_input_format = 'best_effort';

-- INSERT SELECT should also respect date_time_input_format setting
DROP TABLE IF EXISTS test_json_datetime;
CREATE TABLE test_json_datetime (json JSON) ENGINE = Memory;

INSERT INTO test_json_datetime SELECT '{"a" : "2024 April 4"}' SETTINGS date_time_input_format = 'best_effort';
SELECT JSONAllPathsWithTypes(json) FROM test_json_datetime;

DROP TABLE test_json_datetime;
