-- Tags: no-fasttest
-- Tag: no-fasttest due to only SIMD JSON is available in fasttest

SELECT '--JSONExtractCaseInsensitive--';

-- Basic case-insensitive key matching
SELECT JSONExtractStringCaseInsensitive('{"ABC": "def"}', 'abc') = 'def';
SELECT JSONExtractStringCaseInsensitive('{"abc": "def"}', 'ABC') = 'def';
SELECT JSONExtractStringCaseInsensitive('{"AbC": "def"}', 'aBc') = 'def';
SELECT JSONExtractStringCaseInsensitive('{"abc": "def", "ABC": "ghi"}', 'abc') = 'def'; -- Should return first match

-- Different data types
SELECT JSONExtractIntCaseInsensitive('{"Value": 123}', 'value') = 123;
SELECT JSONExtractIntCaseInsensitive('{"VALUE": -456}', 'Value') = -456;
SELECT JSONExtractUIntCaseInsensitive('{"COUNT": 789}', 'count') = 789;
SELECT JSONExtractFloatCaseInsensitive('{"Price": 12.34}', 'PRICE') = 12.34;
SELECT JSONExtractBoolCaseInsensitive('{"IsActive": true}', 'isactive') = 1;

-- Nested objects
SELECT JSONExtractStringCaseInsensitive('{"User": {"Name": "John"}}', 'user', 'name') = 'John';
SELECT JSONExtractIntCaseInsensitive('{"DATA": {"COUNT": 42}}', 'data', 'Count') = 42;

-- Arrays
SELECT JSONExtractIntCaseInsensitive('{"Items": [1, 2, 3]}', 'items', 1) = 2;
SELECT JSONExtractStringCaseInsensitive('{"TAGS": ["a", "b", "c"]}', 'tags', 0) = 'a';

-- Raw extraction
SELECT JSONExtractRawCaseInsensitive('{"Object": {"key": "value"}}', 'OBJECT') = '{"key":"value"}';
SELECT JSONExtractRawCaseInsensitive('{"Array": [1, 2, 3]}', 'array') = '[1,2,3]';

-- Generic extraction with type
SELECT JSONExtractCaseInsensitive('{"Number": 123}', 'number', 'Int32') = 123;
SELECT JSONExtractCaseInsensitive('{"Text": "hello"}', 'TEXT', 'String') = 'hello';
SELECT JSONExtractCaseInsensitive('{"List": [1, 2, 3]}', 'list', 'Array(Int32)') = [1, 2, 3];

-- Keys and values extraction
SELECT JSONExtractKeysAndValuesCaseInsensitive('{"Name": "Alice", "AGE": 30}', 'String')[1] = ('Name', 'Alice');
SELECT JSONExtractKeysAndValuesCaseInsensitive('{"ID": 1, "Value": 2}', 'Int32')[2] = ('Value', 2);

-- Non-existent keys
SELECT JSONExtractStringCaseInsensitive('{"abc": "def"}', 'xyz') IS NULL;
SELECT JSONExtractIntCaseInsensitive('{"abc": 123}', 'XYZ') = 0;

-- Empty JSON
SELECT JSONExtractStringCaseInsensitive('{}', 'key') IS NULL;

-- Multiple keys with different cases (should return the first match)
SELECT JSONExtractStringCaseInsensitive('{"key": "first", "KEY": "second", "Key": "third"}', 'KEY') = 'first';

-- Complex nested example
SELECT JSONExtractIntCaseInsensitive('{"LEVEL1": {"Level2": {"level3": 999}}}', 'level1', 'LEVEL2', 'LEVEL3') = 999;

-- Testing with both allow_simdjson settings
SELECT '--allow_simdjson=0--';
SET allow_simdjson=0;

SELECT JSONExtractStringCaseInsensitive('{"ABC": "def"}', 'abc') = 'def';
SELECT JSONExtractIntCaseInsensitive('{"Value": 123}', 'value') = 123;
SELECT JSONExtractFloatCaseInsensitive('{"Price": 12.34}', 'PRICE') = 12.34;

SELECT '--allow_simdjson=1--';
SET allow_simdjson=1;

SELECT JSONExtractStringCaseInsensitive('{"ABC": "def"}', 'abc') = 'def';
SELECT JSONExtractIntCaseInsensitive('{"Value": 123}', 'value') = 123;
SELECT JSONExtractFloatCaseInsensitive('{"Price": 12.34}', 'PRICE') = 12.34;