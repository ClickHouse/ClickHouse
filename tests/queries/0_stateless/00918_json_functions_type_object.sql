-- Tags: no-fasttest

SELECT '--JSONLength--';
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON);
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'b');
SELECT JSONLength('{}'::JSON);

SELECT '--JSONHas--';
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'a');
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'b');
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'c');

SELECT '--isValidJSON--';
SELECT isValidJSON('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON);

SELECT '--JSONKey--';
SELECT JSONKey('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 1);
SELECT JSONKey('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 2);
SELECT JSONKey('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, -1);
SELECT JSONKey('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, -2);

SELECT '--JSONType--';
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON);
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'b');

SELECT '--JSONExtract<numeric>--';
SELECT JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'b', 1);
SELECT JSONExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'b', 2);
SELECT JSONExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'b', -1);
SELECT JSONExtractBool('{"passed": true}'::JSON, 'passed');

SELECT '--JSONExtractString--';
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'a');
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 1);
select JSONExtractString('{"abc":"\\n\\u0000"}'::JSON, 'abc');
select JSONExtractString('{"abc":"\\u263a"}'::JSON, 'abc');

SELECT '--JSONExtract (generic)--';
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'Tuple(String, Array(Float64))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'Tuple(a String, b Array(Float64))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'Tuple(b Array(Float64), a String)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'Tuple(a FixedString(6), c UInt8)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'a', 'String');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'b', 'Array(Float32)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'b', 'Array(Int8)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'b', 'Array(Nullable(Int8))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'b', 'Array(UInt8)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'b', 'Array(Nullable(UInt8))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'b', 1, 'Int8');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'b', 2, 'Int32');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'b', 4, 'Nullable(Int64)');
SELECT JSONExtract('{"passed": true}'::JSON, 'passed', 'UInt8');
SELECT JSONExtract('{"day": "Thursday"}'::JSON, 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)');
SELECT JSONExtract('{"day": 5}'::JSON, 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)');
SELECT JSONExtract('{"a":3,"b":5,"c":7}'::JSON, 'Tuple(a Int, b Int)');
SELECT JSONExtract('{"a":3,"b":5,"c":7}'::JSON, 'Tuple(c Int, a Int)');
SELECT JSONExtract('{"a":3,"b":5,"c":7}'::JSON, 'Tuple(b Int, d Int)');
SELECT JSONExtract('{"a":3,"b":5,"c":7}'::JSON, 'Tuple(Int, Int)');
SELECT JSONExtract('{"a":3}'::JSON, 'Tuple(Int, Int)');
SELECT JSONExtract('{"a":123456, "b":3.55}'::JSON, 'Tuple(a LowCardinality(Int32), b Decimal(5, 2))');
SELECT JSONExtract('{"a":1, "b":"417ddc5d-e556-4d27-95dd-a34d84e46a50"}'::JSON, 'Tuple(a Int8, b UUID)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'a', 'LowCardinality(String)');
SELECT JSONExtract('{"a":3333.6333333333333333333333, "b":"test"}'::JSON, 'Tuple(a Decimal(10,1), b LowCardinality(String))');
SELECT JSONExtract('{"a":3333.6333333333333333333333, "b":"test"}'::JSON, 'Tuple(a Decimal(20,10), b LowCardinality(String))');
SELECT JSONExtract('{"a":123456.123456}'::JSON, 'a', 'Decimal(20, 4)') as a, toTypeName(a);
SELECT toDecimal64(123456789012345.12, 4), JSONExtract('{"a":123456789012345.12}'::JSON, 'a', 'Decimal(30, 4)');
SELECT toDecimal128(1234567890.12345678901234567890, 20), JSONExtract('{"a":1234567890.12345678901234567890, "b":"test"}'::JSON, 'Tuple(a Decimal(35,20), b LowCardinality(String))');
SELECT toDecimal256(1234567890.123456789012345678901234567890, 30), JSONExtract('{"a":1234567890.12345678901234567890, "b":"test"}'::JSON, 'Tuple(a Decimal(45,30), b LowCardinality(String))');
SELECT JSONExtract('{"a": 3}'::JSON, 'a', 'Decimal(5, 2)');

SELECT '--JSONExtractKeysAndValues--';
SELECT JSONExtractKeysAndValues('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'String');
SELECT JSONExtractKeysAndValues('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'Array(Float64)');
SELECT JSONExtractKeysAndValues('{"a": "hello", "b": "world"}'::JSON, 'String');
SELECT JSONExtractKeysAndValues('{"x": {"a": 5, "b": 7, "c": 11}}'::JSON, 'x', 'Int8');
SELECT JSONExtractKeysAndValues('{"a": "hello", "b": "world"}'::JSON, 'LowCardinality(String)');

SELECT '--JSONExtractRaw--';
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON);
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'a');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'b');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'b', 1);
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}'::JSON);
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}'::JSON, 'c');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}'::JSON, 'c', 'd');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}'::JSON, 'c', 'd', 2);
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}'::JSON, 'c', 'd', 3);
SELECT JSONExtractRaw('{"passed": true}'::JSON);
SELECT JSONExtractRaw('{}'::JSON);
SELECT JSONExtractRaw('{"abc":"\\n\\u0000"}'::JSON, 'abc');
SELECT JSONExtractRaw('{"abc":"\\u263a"}'::JSON, 'abc');

SELECT '--JSONExtractArrayRaw--';
SELECT JSONExtractArrayRaw(''::JSON);
SELECT JSONExtractArrayRaw('{"a": "hello", "b": "not_array"}'::JSON);
SELECT JSONExtractArrayRaw('{"arr": []}'::JSON, 'arr');
SELECT JSONExtractArrayRaw('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'b');
SELECT JSONExtractArrayRaw('{"arr": [1,2,3,4,5,"hello"]}'::JSON, 'arr');
SELECT JSONExtractArrayRaw(arrayJoin(JSONExtractArrayRaw('{"arr": [[1,2,3],[4,5,6]]}'::JSON, 'arr')));

SELECT '--JSONExtractKeysAndValuesRaw--';
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'a');
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON, 'b');
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300]}'::JSON);
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}'::JSON);
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}'::JSON, 'c');

SELECT '--const/non-const mixed--';
SELECT JSONExtractString('{"arr": ["a", "b", "c", "d", "e"]}'::JSON, 'arr', idx) FROM (SELECT arrayJoin([1,2,3,4,5]) AS idx);
SELECT JSONExtractString(json::JSON, 's') FROM (SELECT arrayJoin(['{"s":"u"}', '{"s":"v"}']) AS json);
