-- Tags: no-fasttest
-- Tag: no-fasttest due to only SIMD JSON is available in fasttest

SELECT '--Edge cases for JSONExtractCaseInsensitive--';

-- Keys with special characters
SELECT JSONExtractStringCaseInsensitive('{"key-with-dash": "value1"}', 'KEY-WITH-DASH') = 'value1';
SELECT JSONExtractStringCaseInsensitive('{"key_with_underscore": "value2"}', 'KEY_WITH_UNDERSCORE') = 'value2';
SELECT JSONExtractStringCaseInsensitive('{"key.with.dots": "value3"}', 'KEY.WITH.DOTS') = 'value3';
SELECT JSONExtractStringCaseInsensitive('{"key with spaces": "value4"}', 'KEY WITH SPACES') = 'value4';

-- Unicode keys (ASCII case-insensitive only)
SELECT JSONExtractStringCaseInsensitive('{"café": "coffee"}', 'CAFÉ') IS NULL; -- Should not match (non-ASCII)
SELECT JSONExtractStringCaseInsensitive('{"café": "coffee"}', 'café') = 'coffee'; -- Exact match works

-- Numeric string keys
SELECT JSONExtractStringCaseInsensitive('{"123": "numeric key"}', '123') = 'numeric key';

-- Empty string key
SELECT JSONExtractStringCaseInsensitive('{"": "empty key"}', '') = 'empty key';

-- Very long keys
SELECT JSONExtractStringCaseInsensitive(
    concat('{"', repeat('VeryLongKey', 100), '": "value"}'), 
    repeat('verylongkey', 100)
) = 'value';

-- Mixed types
SELECT JSONExtractStringCaseInsensitive('{"Key": 123}', 'key') = '123'; -- Number as string
SELECT JSONExtractStringCaseInsensitive('{"Key": true}', 'KEY') = 'true'; -- Bool as string
SELECT JSONExtractIntCaseInsensitive('{"Key": "123"}', 'key') = 123; -- String as number
SELECT JSONExtractBoolCaseInsensitive('{"Key": 1}', 'KEY') = 1; -- Number as bool

-- Null values
SELECT JSONExtractStringCaseInsensitive('{"Key": null}', 'key') IS NULL;
SELECT JSONExtractIntCaseInsensitive('{"Key": null}', 'KEY') = 0;

-- Invalid JSON
SELECT JSONExtractStringCaseInsensitive('not a json', 'key') IS NULL;
SELECT JSONExtractIntCaseInsensitive('{invalid json}', 'key') = 0;

-- Case sensitivity comparison
SELECT JSONExtractString('{"ABC": "def", "abc": "ghi"}', 'abc') = 'ghi'; -- Case sensitive - exact match
SELECT JSONExtractStringCaseInsensitive('{"ABC": "def", "abc": "ghi"}', 'abc') = 'def'; -- Case insensitive - first match

-- Performance test with many keys
WITH json AS (
    SELECT concat('{', 
        arrayStringConcat(
            arrayMap(i -> concat('"key', toString(i), '": ', toString(i)), 
            range(1000)), 
            ','
        ), 
        ', "TARGET": 999}'
    ) AS str
)
SELECT JSONExtractIntCaseInsensitive(str, 'target') = 999 FROM json;

-- Multiple levels of nesting
SELECT JSONExtractStringCaseInsensitive(
    '{"LEVEL1": {"level2": {"LEVEL3": {"level4": "deep"}}}}', 
    'level1', 'LEVEL2', 'level3', 'LEVEL4'
) = 'deep';