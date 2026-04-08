-- Tags: no-fasttest
SET allow_experimental_json_type = 1;

DROP TABLE IF EXISTS test_ci_json_obj;
CREATE TABLE test_ci_json_obj (data JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_ci_json_obj VALUES ('{"Key": "value"}');
INSERT INTO test_ci_json_obj VALUES ('{"Nested": {"InnerKey": 42}}');

-- Case-insensitive extraction should find keys regardless of case
SELECT 'JSONExtractStringCaseInsensitive', JSONExtractStringCaseInsensitive(data, 'key') FROM test_ci_json_obj ORDER BY data LIMIT 1;
SELECT 'JSONExtractStringCaseInsensitive upper', JSONExtractStringCaseInsensitive(data, 'KEY') FROM test_ci_json_obj ORDER BY data LIMIT 1;
SELECT 'JSONExtractIntCaseInsensitive', JSONExtractIntCaseInsensitive(data, 'nested', 'innerkey') FROM test_ci_json_obj ORDER BY data DESC LIMIT 1;
SELECT 'JSONExtractRawCaseInsensitive', JSONExtractRawCaseInsensitive(data, 'key') FROM test_ci_json_obj ORDER BY data LIMIT 1;

-- Case-sensitive extraction with exact case should still work
SELECT 'JSONExtractString exact', JSONExtractString(data, 'Key') FROM test_ci_json_obj ORDER BY data LIMIT 1;

-- Case-sensitive extraction with wrong case should return empty
SELECT 'JSONExtractString wrong case', JSONExtractString(data, 'key') FROM test_ci_json_obj ORDER BY data LIMIT 1;

DROP TABLE test_ci_json_obj;