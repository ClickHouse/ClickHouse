SET allow_experimental_json_type = 1;

-- Case-insensitive extraction should find keys regardless of case
SELECT 'string lower', JSONExtractStringCaseInsensitive('{"Key": "value", "other": "x"}'::JSON, 'key');
SELECT 'string upper', JSONExtractStringCaseInsensitive('{"Key": "value", "other": "x"}'::JSON, 'KEY');
SELECT 'int nested', JSONExtractIntCaseInsensitive('{"Nested": {"InnerKey": 42}, "z": 0}'::JSON, 'nested', 'innerkey');
SELECT 'raw', JSONExtractRawCaseInsensitive('{"Key": "value", "other": "x"}'::JSON, 'key');

-- Case-sensitive extraction with exact case should still work
SELECT 'exact match', JSONExtractString('{"Key": "value"}'::JSON, 'Key');

-- Case-sensitive extraction with wrong case should return empty
SELECT 'wrong case', JSONExtractString('{"Key": "value"}'::JSON, 'key');

-- Multiple keys differing only in case: the result is non-deterministic,
-- but must be one of the matching values, not an unrelated key.
SELECT 'multi-key match',
    JSONExtractStringCaseInsensitive('{"Name": "alice", "NAME": "bob", "name": "charlie", "age": "30"}'::JSON, 'name')
    IN ('alice', 'bob', 'charlie');
SELECT 'multi-key int match',
    JSONExtractIntCaseInsensitive('{"Val": 1, "VAL": 2, "other": 99}'::JSON, 'val')
    IN (1, 2);
