-- Tags: no-fasttest
-- Tag: no-fasttest due to only SIMD JSON is available in fasttest

SELECT '--allow_simdjson=1--';
SET allow_simdjson=1;

SELECT '--JSONLength--';
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}');
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
SELECT JSONLength('{}');

SELECT '--JSONHas--';
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'a');
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'c');

SELECT '--isValidJSON--';
SELECT isValidJSON('{"a": "hello", "b": [-100, 200.0, 300]}');
SELECT isValidJSON('not a json');
SELECT isValidJSON('"HX-=');

SELECT '--JSONKey--';
SELECT JSONKey('{"a": "hello", "b": [-100, 200.0, 300]}', 1);
SELECT JSONKey('{"a": "hello", "b": [-100, 200.0, 300]}', 2);
SELECT JSONKey('{"a": "hello", "b": [-100, 200.0, 300]}', -1);
SELECT JSONKey('{"a": "hello", "b": [-100, 200.0, 300]}', -2);

SELECT '--JSONType--';
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}');
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
SELECT JSONType('{"a": true}', 'a');

SELECT '--JSONExtract<numeric>--';
SELECT JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1);
SELECT JSONExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2);
SELECT JSONExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1);
SELECT JSONExtractBool('{"passed": true}', 'passed');
SELECT JSONExtractBool('"HX-=');
SELECT JSONExtractBool('-1');

SELECT '--JSONExtractString--';
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 'a');
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 1);
select JSONExtractString('{"abc":"\\n\\u0000"}', 'abc');
select JSONExtractString('{"abc":"\\u263a"}', 'abc');
select JSONExtractString('{"abc":"\\u263"}', 'abc');
select JSONExtractString('{"abc":"hello}', 'abc');

SELECT '--JSONExtract (generic)--';
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(String, Array(Float64))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(a String, b Array(Float64))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(b Array(Float64), a String)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(a FixedString(6), c UInt8)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'a', 'String');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(Float32)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Tuple(Int8, Float32, UInt16)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(Int8)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(Nullable(Int8))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(LowCardinality(Nullable(Int8)))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(UInt8)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(Nullable(UInt8))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(LowCardinality(Nullable(UInt8)))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1, 'Int8');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2, 'Int32');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4, 'Nullable(Int64)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4, 'LowCardinality(Nullable(Int64))');
SELECT JSONExtract('{"passed": true}', 'passed', 'UInt8');
SELECT JSONExtract('{"day": "Thursday"}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)');
SELECT JSONExtract('{"day": 5}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)');
SELECT JSONExtract('{"a":3,"b":5,"c":7}', 'Tuple(a Int, b Int)');
SELECT JSONExtract('{"a":3,"b":5,"c":7}', 'Tuple(c Int, a Int)');
SELECT JSONExtract('{"a":3,"b":5,"c":7}', 'Tuple(b Int, d Int)');
SELECT JSONExtract('{"a":3,"b":5,"c":7}', 'Tuple(Int, Int)');
SELECT JSONExtract('{"a":3}', 'Tuple(Int, Int)');
SELECT JSONExtract('[3,5,7]', 'Tuple(Int, Int)');
SELECT JSONExtract('[3]', 'Tuple(Int, Int)');
SELECT JSONExtract('{"a":123456, "b":3.55}', 'Tuple(a LowCardinality(Int32), b Decimal(5, 2))');
SELECT JSONExtract('{"a":1, "b":"417ddc5d-e556-4d27-95dd-a34d84e46a50"}', 'Tuple(a Int8, b UUID)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'a', 'LowCardinality(String)');
SELECT JSONExtract('{"a":3333.6333333333333333333333, "b":"test"}', 'Tuple(a Decimal(10,1), b LowCardinality(String))');
SELECT JSONExtract('{"a":"3333.6333333333333333333333", "b":"test"}', 'Tuple(a Decimal(10,1), b LowCardinality(String))');
SELECT JSONExtract('{"a":3333.6333333333333333333333, "b":"test"}', 'Tuple(a Decimal(20,10), b LowCardinality(String))');
SELECT JSONExtract('{"a":"3333.6333333333333333333333", "b":"test"}', 'Tuple(a Decimal(20,10), b LowCardinality(String))');
SELECT JSONExtract('{"a":123456.123456}', 'a', 'Decimal(20, 4)') as a, toTypeName(a);
SELECT JSONExtract('{"a":"123456.123456"}', 'a', 'Decimal(20, 4)') as a, toTypeName(a);
SELECT JSONExtract('{"a":"123456789012345.12"}', 'a', 'Decimal(30, 4)') as a, toTypeName(a);
SELECT JSONExtract('{"a":"1234567890.12345678901234567890", "b":"test"}', 'Tuple(a Decimal(35,20), b LowCardinality(String))') as a, toTypeName(a);
SELECT JSONExtract('{"a":"1234567890.123456789012345678901234567890", "b":"test"}', 'Tuple(a Decimal(45,30), b LowCardinality(String))') as a, toTypeName(a);
SELECT toDecimal64(123456789012345.12, 4), JSONExtract('{"a":123456789012345.12}', 'a', 'Decimal(30, 4)');
SELECT toDecimal128(1234567890.12345678901234567890, 20), JSONExtract('{"a":1234567890.12345678901234567890, "b":"test"}', 'Tuple(a Decimal(35,20), b LowCardinality(String))');
SELECT toDecimal256(1234567890.123456789012345678901234567890, 30), JSONExtract('{"a":1234567890.12345678901234567890, "b":"test"}', 'Tuple(a Decimal(45,30), b LowCardinality(String))');
SELECT JSONExtract('{"a":-1234567890}', 'a', 'Int32') as a, toTypeName(a);
SELECT JSONExtract('{"a":1234567890}', 'a', 'UInt32') as a, toTypeName(a);
SELECT JSONExtract('{"a":-1234567890123456789}', 'a', 'Int64') as a, toTypeName(a);
SELECT JSONExtract('{"a":1234567890123456789}', 'a', 'UInt64') as a, toTypeName(a);
SELECT JSONExtract('{"a":-1234567890123456789}', 'a', 'Int128') as a, toTypeName(a);
SELECT JSONExtract('{"a":1234567890123456789}', 'a', 'UInt128') as a, toTypeName(a);
SELECT JSONExtract('{"a":-1234567890123456789}', 'a', 'Int256') as a, toTypeName(a);
SELECT JSONExtract('{"a":1234567890123456789}', 'a', 'UInt256') as a, toTypeName(a);
SELECT JSONExtract('{"a":-123456789.345}', 'a', 'Int32') as a, toTypeName(a);
SELECT JSONExtract('{"a":123456789.345}', 'a', 'UInt32') as a, toTypeName(a);
SELECT JSONExtract('{"a":-123456789012.345}', 'a', 'Int64') as a, toTypeName(a);
SELECT JSONExtract('{"a":123456789012.345}', 'a', 'UInt64') as a, toTypeName(a);
SELECT JSONExtract('{"a":-123456789012.345}', 'a', 'Int128') as a, toTypeName(a);
SELECT JSONExtract('{"a":123456789012.345}', 'a', 'UInt128') as a, toTypeName(a);
SELECT JSONExtract('{"a":-123456789012.345}', 'a', 'Int256') as a, toTypeName(a);
SELECT JSONExtract('{"a":123456789012.345}', 'a', 'UInt256') as a, toTypeName(a);
SELECT JSONExtract('{"a":"-123456789"}', 'a', 'Int32') as a, toTypeName(a);
SELECT JSONExtract('{"a":"123456789"}', 'a', 'UInt32') as a, toTypeName(a);
SELECT JSONExtract('{"a":"-1234567890123456789"}', 'a', 'Int64') as a, toTypeName(a);
SELECT JSONExtract('{"a":"1234567890123456789"}', 'a', 'UInt64') as a, toTypeName(a);
SELECT JSONExtract('{"a":"-12345678901234567890123456789012345678"}', 'a', 'Int128') as a, toTypeName(a);
SELECT JSONExtract('{"a":"12345678901234567890123456789012345678"}', 'a', 'UInt128') as a, toTypeName(a);
SELECT JSONExtract('{"a":"-11345678901234567890123456789012345678901234567890123456789012345678901234567"}', 'a', 'Int256') as a, toTypeName(a);
SELECT JSONExtract('{"a":"11345678901234567890123456789012345678901234567890123456789012345678901234567"}', 'a', 'UInt256') as a, toTypeName(a);
SELECT JSONExtract('{"a":"-1234567899999"}', 'a', 'Int32') as a, toTypeName(a);
SELECT JSONExtract('{"a":"1234567899999"}', 'a', 'UInt32') as a, toTypeName(a);
SELECT JSONExtract('{"a":"-1234567890123456789999"}', 'a', 'Int64') as a, toTypeName(a);
SELECT JSONExtract('{"a":"1234567890123456789999"}', 'a', 'UInt64') as a, toTypeName(a);
SELECT JSONExtract('{"a":0}', 'a', 'Bool') as a, toTypeName(a);
SELECT JSONExtract('{"a":1}', 'a', 'Bool') as a, toTypeName(a);

SELECT JSONExtract('{"a": "-123456789012.345"}', 'a', 'Int64') as a, toTypeName(a);
SELECT JSONExtract('{"a": "123456789012.345"}', 'a', 'UInt64') as a, toTypeName(a);

SELECT JSONExtract('{"a": "-2000.22"}', 'a', 'UInt64') as a, toTypeName(a);
SELECT JSONExtract('{"a": "-2000.22"}', 'a', 'Int8') as a, toTypeName(a);

SELECT JSONExtract('{"a": "hello", "b": "world"}', 'Map(String, String)');
SELECT JSONExtract('{"a": "hello", "b": "world"}', 'Map(LowCardinality(String), String)');
SELECT JSONExtract('{"a": ["hello", 100.0], "b": ["world", 200]}', 'Map(String, Tuple(String, Float64))');
SELECT JSONExtract('{"a": [100.0, 200], "b": [-100, 200.0, 300]}', 'Map(String, Array(Float64))');
SELECT JSONExtract('{"a": {"c": "hello"}, "b": {"d": "world"}}', 'Map(String, Map(String, String))');
SELECT JSONExtract('{"a": {"c": "hello"}, "b": {"d": "world"}}', 'a',  'Map(String, String)');

SELECT '--JSONExtractKeysAndValues--';
SELECT JSONExtractKeysAndValues('{"a": "hello", "b": [-100, 200.0, 300]}', 'String');
SELECT JSONExtractKeysAndValues('{"a": "hello", "b": [-100, 200.0, 300]}', 'Array(Float64)');
SELECT JSONExtractKeysAndValues('{"a": "hello", "b": "world"}', 'String');
SELECT JSONExtractKeysAndValues('{"x": {"a": 5, "b": 7, "c": 11}}', 'x', 'Int8');
SELECT JSONExtractKeysAndValues('{"a": "hello", "b": "world"}', 'LowCardinality(String)');

SELECT '--JSONExtractRaw--';
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'a');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1);
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}', 'c');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}', 'c', 'd');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}', 'c', 'd', 2);
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}', 'c', 'd', 3);
SELECT JSONExtractRaw('{"passed": true}');
SELECT JSONExtractRaw('{}');
SELECT JSONExtractRaw('{"abc":"\\n\\u0000"}', 'abc');
SELECT JSONExtractRaw('{"abc":"\\u263a"}', 'abc');

SELECT '--JSONExtractArrayRaw--';
SELECT JSONExtractArrayRaw('');
SELECT JSONExtractArrayRaw('{"a": "hello", "b": "not_array"}');
SELECT JSONExtractArrayRaw('[]');
SELECT JSONExtractArrayRaw('[[],[]]');
SELECT JSONExtractArrayRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
SELECT JSONExtractArrayRaw('[1,2,3,4,5,"hello"]');
SELECT JSONExtractArrayRaw(arrayJoin(JSONExtractArrayRaw('[[1,2,3],[4,5,6]]')));

SELECT '--JSONExtractKeysAndValuesRaw--';
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'a');
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300]}');
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}');
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}', 'c');

SELECT '--const/non-const mixed--';
SELECT JSONExtractString('["a", "b", "c", "d", "e"]', idx) FROM (SELECT arrayJoin([1,2,3,4,5]) AS idx);
SELECT JSONExtractString(json, 's') FROM (SELECT arrayJoin(['{"s":"u"}', '{"s":"v"}']) AS json);

SELECT '--show error: type should be const string';
SELECT JSONExtractKeysAndValues([], JSONLength('^?V{LSwp')); -- { serverError ILLEGAL_COLUMN }
WITH '{"i": 1, "f": 1.2}' AS json SELECT JSONExtract(json, 'i', JSONType(json, 'i')); -- { serverError ILLEGAL_COLUMN }

SELECT '--show error: key of map type should be String';
SELECT JSONExtract('{"a": [100.0, 200], "b": [-100, 200.0, 300]}', 'Map(Int64, Array(Float64))'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }


SELECT '--allow_simdjson=0--';
SET allow_simdjson=0;

SELECT '--JSONLength--';
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}');
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
SELECT JSONLength('{}');

SELECT '--JSONHas--';
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'a');
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'c');

SELECT '--isValidJSON--';
SELECT isValidJSON('{"a": "hello", "b": [-100, 200.0, 300]}');
SELECT isValidJSON('not a json');
SELECT isValidJSON('"HX-=');

SELECT '--JSONKey--';
SELECT JSONKey('{"a": "hello", "b": [-100, 200.0, 300]}', 1);
SELECT JSONKey('{"a": "hello", "b": [-100, 200.0, 300]}', 2);
SELECT JSONKey('{"a": "hello", "b": [-100, 200.0, 300]}', -1);
SELECT JSONKey('{"a": "hello", "b": [-100, 200.0, 300]}', -2);

SELECT '--JSONType--';
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}');
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
SELECT JSONType('{"a": true}', 'a');

SELECT '--JSONExtract<numeric>--';
SELECT JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1);
SELECT JSONExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2);
SELECT JSONExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1);
SELECT JSONExtractBool('{"passed": true}', 'passed');
SELECT JSONExtractBool('"HX-=');
SELECT JSONExtractBool('-1');

SELECT JSONExtract('{"a": "-123456789012.345"}', 'a', 'Int64') as a, toTypeName(a);
SELECT JSONExtract('{"a": "123456789012.345"}', 'a', 'UInt64') as a, toTypeName(a);

SELECT JSONExtract('{"a": "-2000.22"}', 'a', 'UInt64') as a, toTypeName(a);
SELECT JSONExtract('{"a": "-2000.22"}', 'a', 'Int8') as a, toTypeName(a);

SELECT '--JSONExtractString--';
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 'a');
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 1);
select JSONExtractString('{"abc":"\\n\\u0000"}', 'abc');
select JSONExtractString('{"abc":"\\u263a"}', 'abc');
select JSONExtractString('{"abc":"\\u263"}', 'abc');
select JSONExtractString('{"abc":"hello}', 'abc');

SELECT '--JSONExtract (generic)--';
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(String, Array(Float64))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(a String, b Array(Float64))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(b Array(Float64), a String)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(a FixedString(6), c UInt8)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'a', 'String');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(Float32)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Tuple(Int8, Float32, UInt16)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(Int8)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(Nullable(Int8))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(LowCardinality(Nullable(Int8)))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(UInt8)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(Nullable(UInt8))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(LowCardinality(Nullable(UInt8)))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1, 'Int8');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2, 'Int32');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4, 'Nullable(Int64)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4, 'LowCardinality(Nullable(Int64))');
SELECT JSONExtract('{"passed": true}', 'passed', 'UInt8');
SELECT JSONExtract('{"day": "Thursday"}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)');
SELECT JSONExtract('{"day": 5}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)');
SELECT JSONExtract('{"a":3,"b":5,"c":7}', 'Tuple(a Int, b Int)');
SELECT JSONExtract('{"a":3,"b":5,"c":7}', 'Tuple(c Int, a Int)');
SELECT JSONExtract('{"a":3,"b":5,"c":7}', 'Tuple(b Int, d Int)');
SELECT JSONExtract('{"a":3,"b":5,"c":7}', 'Tuple(Int, Int)');
SELECT JSONExtract('{"a":3}', 'Tuple(Int, Int)');
SELECT JSONExtract('[3,5,7]', 'Tuple(Int, Int)');
SELECT JSONExtract('[3]', 'Tuple(Int, Int)');

SELECT JSONExtract('{"a": "hello", "b": "world"}', 'Map(String, String)');
SELECT JSONExtract('{"a": "hello", "b": "world"}', 'Map(LowCardinality(String), String)');
SELECT JSONExtract('{"a": ["hello", 100.0], "b": ["world", 200]}', 'Map(String, Tuple(String, Float64))');
SELECT JSONExtract('{"a": [100.0, 200], "b": [-100, 200.0, 300]}', 'Map(String, Array(Float64))');
SELECT JSONExtract('{"a": {"c": "hello"}, "b": {"d": "world"}}', 'Map(String, Map(String, String))');
SELECT JSONExtract('{"a": {"c": "hello"}, "b": {"d": "world"}}', 'a',  'Map(String, String)');

SELECT '--JSONExtractKeysAndValues--';
SELECT JSONExtractKeysAndValues('{"a": "hello", "b": [-100, 200.0, 300]}', 'String');
SELECT JSONExtractKeysAndValues('{"a": "hello", "b": [-100, 200.0, 300]}', 'Array(Float64)');
SELECT JSONExtractKeysAndValues('{"a": "hello", "b": "world"}', 'String');
SELECT JSONExtractKeysAndValues('{"x": {"a": 5, "b": 7, "c": 11}}', 'x', 'Int8');

SELECT '--JSONExtractRaw--';
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'a');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1);
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}', 'c');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}', 'c', 'd');
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}', 'c', 'd', 2);
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}', 'c', 'd', 3);
SELECT JSONExtractRaw('{"passed": true}');
SELECT JSONExtractRaw('{}');
SELECT JSONExtractRaw('{"abc":"\\n\\u0000"}', 'abc');
SELECT JSONExtractRaw('{"abc":"\\u263a"}', 'abc');

SELECT '--JSONExtractArrayRaw--';
SELECT JSONExtractArrayRaw('');
SELECT JSONExtractArrayRaw('{"a": "hello", "b": "not_array"}');
SELECT JSONExtractArrayRaw('[]');
SELECT JSONExtractArrayRaw('[[],[]]');
SELECT JSONExtractArrayRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
SELECT JSONExtractArrayRaw('[1,2,3,4,5,"hello"]');
SELECT JSONExtractArrayRaw(arrayJoin(JSONExtractArrayRaw('[[1,2,3],[4,5,6]]')));

SELECT '--JSONExtractKeysAndValuesRaw--';
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'a');
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300]}');
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}');
SELECT JSONExtractKeysAndValuesRaw('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}', 'c');

SELECT '--JSONExtractKeys--';
SELECT JSONExtractKeys('{"a": "hello", "b": [-100, 200.0, 300]}');
SELECT JSONExtractKeys('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
SELECT JSONExtractKeys('{"a": "hello", "b": [-100, 200.0, 300]}', 'a');
SELECT JSONExtractKeys('{"a": "hello", "b": [-100, 200.0, 300], "c":{"d":[121,144]}}', 'c');

SELECT '--const/non-const mixed--';
SELECT JSONExtractString('["a", "b", "c", "d", "e"]', idx) FROM (SELECT arrayJoin([1,2,3,4,5]) AS idx);
SELECT JSONExtractString(json, 's') FROM (SELECT arrayJoin(['{"s":"u"}', '{"s":"v"}']) AS json);

SELECT '--show error: type should be const string';
SELECT JSONExtractKeysAndValues([], JSONLength('^?V{LSwp')); -- { serverError ILLEGAL_COLUMN }
WITH '{"i": 1, "f": 1.2}' AS json SELECT JSONExtract(json, 'i', JSONType(json, 'i')); -- { serverError ILLEGAL_COLUMN }

SELECT '--show error: index type should be integer';
SELECT JSONExtract('[]', JSONExtract('0', 'UInt256'), 'UInt256'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '--show error: key of map type should be String';
SELECT JSONExtract('{"a": [100.0, 200], "b": [-100, 200.0, 300]}', 'Map(Int64, Array(Float64))'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT JSONExtract(materialize(toLowCardinality('{"string_value":null}')), materialize('string_value'), 'LowCardinality(Nullable(String))');
SELECT JSONExtract(materialize('{"string_value":null}'), materialize('string_value'), 'LowCardinality(Nullable(String))');
SELECT JSONExtract(materialize('{"string_value":"Hello"}'), materialize('string_value'), 'LowCardinality(Nullable(String))') AS x;
SELECT JSONExtract(materialize(toLowCardinality('{"string_value":"Hello"}')), materialize('string_value'), 'LowCardinality(Nullable(String))') AS x;
SELECT JSONExtract(materialize('{"string_value":"Hello"}'), materialize(toLowCardinality('string_value')), 'LowCardinality(Nullable(String))') AS x;
SELECT JSONExtract(materialize(toLowCardinality('{"string_value":"Hello"}')), materialize(toLowCardinality('string_value')), 'LowCardinality(Nullable(String))') AS x;
