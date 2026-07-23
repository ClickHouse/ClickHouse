-- Test INSERT...SELECT with duplicate paths
DROP TABLE IF EXISTS test_json_duplicates;
CREATE TABLE test_json_duplicates (json JSON) ENGINE = Memory;

-- Should fail without setting
INSERT INTO test_json_duplicates SELECT '{"a": 1, "a": 2}'; -- {serverError INCORRECT_DATA}

-- Should succeed with setting
INSERT INTO test_json_duplicates SELECT '{"a": 1, "a": 2}' SETTINGS type_json_skip_duplicated_paths=1;
SELECT * FROM test_json_duplicates;

DROP TABLE test_json_duplicates;
