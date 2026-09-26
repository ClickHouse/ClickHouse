SELECT '--Edge cases for JSONExtractCaseInsensitive--';

-- Keys with special characters
SELECT JSONExtractStringCaseInsensitive('{"key-with-dash": "value1"}', 'KEY-WITH-DASH');
SELECT JSONExtractStringCaseInsensitive('{"key_with_underscore": "value2"}', 'KEY_WITH_UNDERSCORE');
SELECT JSONExtractStringCaseInsensitive('{"key.with.dots": "value3"}', 'KEY.WITH.DOTS');
SELECT JSONExtractStringCaseInsensitive('{"key with spaces": "value4"}', 'KEY WITH SPACES');

-- Unicode keys (ASCII case-insensitive only)
SELECT JSONExtractStringCaseInsensitive('{"café": "coffee"}', 'CAFÉ'); -- Should not match (non-ASCII)
SELECT JSONExtractStringCaseInsensitive('{"café": "coffee"}', 'café'); -- Exact match works

-- Numeric string keys
SELECT JSONExtractStringCaseInsensitive('{"123": "numeric key"}', '123');

-- Empty string key
SELECT JSONExtractStringCaseInsensitive('{"": "empty key"}', '');

-- Very long keys
SELECT JSONExtractStringCaseInsensitive(
    concat('{"', repeat('VeryLongKey', 100), '": "value"}'), 
    repeat('verylongkey', 100)
);

-- Mixed types
SELECT JSONExtractStringCaseInsensitive('{"Key": 123}', 'key'); -- Number as string
SELECT JSONExtractStringCaseInsensitive('{"Key": true}', 'KEY'); -- Bool as string
SELECT JSONExtractIntCaseInsensitive('{"Key": "123"}', 'key'); -- String as number
SELECT JSONExtractBoolCaseInsensitive('{"Key": 1}', 'KEY'); -- Number as bool

-- Null values
SELECT JSONExtractStringCaseInsensitive('{"Key": null}', 'key');
SELECT JSONExtractIntCaseInsensitive('{"Key": null}', 'KEY');

-- Invalid JSON
SELECT JSONExtractStringCaseInsensitive('not a json', 'key');
SELECT JSONExtractIntCaseInsensitive('{invalid json}', 'key');

-- Case sensitivity comparison
SELECT JSONExtractString('{"ABC": "def", "abc": "ghi"}', 'abc'); -- Case sensitive - exact match
SELECT JSONExtractStringCaseInsensitive('{"ABC": "def", "abc": "ghi"}', 'abc'); -- Case insensitive - first match

-- Multiple levels of nesting
SELECT JSONExtractStringCaseInsensitive(
    '{"LEVEL1": {"level2": {"LEVEL3": {"level4": "deep"}}}}', 
    'level1', 'LEVEL2', 'level3', 'LEVEL4'
);

-- Test additional functions with case-insensitive keys
SELECT JSONExtractArrayRawCaseInsensitive('{"ARRAY": ["test", 123, true]}', 'array');
SELECT length(JSONExtractKeysAndValuesRawCaseInsensitive('{"KEY1": "value1", "key2": 100}'));
SELECT JSONExtractKeysCaseInsensitive('{"ABC": 1, "def": 2, "GHI": 3}')[1];