-- Tags: no-fasttest
-- Tag: no-fasttest due to only SIMD JSON is available in fasttest

SELECT '--JSONExtractCaseInsensitive--';

-- Basic case-insensitive key matching
SELECT JSONExtractStringCaseInsensitive('{"ABC": "def"}', 'abc');
SELECT JSONExtractStringCaseInsensitive('{"abc": "def"}', 'ABC');
SELECT JSONExtractStringCaseInsensitive('{"AbC": "def"}', 'aBc');
SELECT JSONExtractStringCaseInsensitive('{"abc": "def", "ABC": "ghi"}', 'abc'); -- Should return first match

-- Different data types
SELECT JSONExtractIntCaseInsensitive('{"Value": 123}', 'value');
SELECT JSONExtractIntCaseInsensitive('{"VALUE": -456}', 'Value');
SELECT JSONExtractUIntCaseInsensitive('{"COUNT": 789}', 'count');
SELECT JSONExtractFloatCaseInsensitive('{"Price": 12.34}', 'PRICE');
SELECT JSONExtractBoolCaseInsensitive('{"IsActive": true}', 'isactive');

-- Nested objects
SELECT JSONExtractStringCaseInsensitive('{"User": {"Name": "John"}}', 'user', 'name');
SELECT JSONExtractIntCaseInsensitive('{"DATA": {"COUNT": 42}}', 'data', 'Count');

-- Arrays
SELECT JSONExtractIntCaseInsensitive('{"Items": [1, 2, 3]}', 'items', 1);
SELECT JSONExtractStringCaseInsensitive('{"TAGS": ["a", "b", "c"]}', 'tags', 0);

-- Raw extraction
SELECT JSONExtractRawCaseInsensitive('{"Object": {"key": "value"}}', 'OBJECT');
SELECT JSONExtractRawCaseInsensitive('{"Array": [1, 2, 3]}', 'array');

-- Generic extraction with type
SELECT JSONExtractCaseInsensitive('{"Number": 123}', 'number', 'Int32');
SELECT JSONExtractCaseInsensitive('{"Text": "hello"}', 'TEXT', 'String');
SELECT JSONExtractCaseInsensitive('{"List": [1, 2, 3]}', 'list', 'Array(Int32)');

-- Keys and values extraction
SELECT JSONExtractKeysAndValuesCaseInsensitive('{"Name": "Alice", "AGE": 30}', 'String');
SELECT JSONExtractKeysAndValuesCaseInsensitive('{"ID": 1, "Value": 2}', 'Int32');

-- Non-existent keys
SELECT JSONExtractStringCaseInsensitive('{"abc": "def"}', 'xyz');
SELECT JSONExtractIntCaseInsensitive('{"abc": 123}', 'XYZ');

-- Empty JSON
SELECT JSONExtractStringCaseInsensitive('{}', 'key');

-- Multiple keys with different cases (should return the first match)
SELECT JSONExtractStringCaseInsensitive('{"key": "first", "KEY": "second", "Key": "third"}', 'KEY');

-- Complex nested example
SELECT JSONExtractIntCaseInsensitive('{"LEVEL1": {"Level2": {"level3": 999}}}', 'level1', 'LEVEL2', 'LEVEL3');

-- Additional functions: ArrayRaw, KeysAndValuesRaw, Keys
SELECT JSONExtractArrayRawCaseInsensitive('{"Items": [1, 2, 3]}', 'ITEMS');
SELECT JSONExtractKeysAndValuesRawCaseInsensitive('{"Name": "Alice", "AGE": 30}');
SELECT JSONExtractKeysCaseInsensitive('{"Name": "Alice", "AGE": 30}');

-- Testing with both allow_simdjson settings
SELECT '--allow_simdjson=0--';
SET allow_simdjson=0;

SELECT JSONExtractStringCaseInsensitive('{"ABC": "def"}', 'abc');
SELECT JSONExtractIntCaseInsensitive('{"Value": 123}', 'value');
SELECT JSONExtractFloatCaseInsensitive('{"Price": 12.34}', 'PRICE');

SELECT '--allow_simdjson=1--';
SET allow_simdjson=1;

SELECT JSONExtractStringCaseInsensitive('{"ABC": "def"}', 'abc');
SELECT JSONExtractIntCaseInsensitive('{"Value": 123}', 'value');
SELECT JSONExtractFloatCaseInsensitive('{"Price": 12.34}', 'PRICE');