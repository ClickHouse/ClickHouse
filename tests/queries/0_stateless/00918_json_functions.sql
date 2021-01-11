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

SELECT '--JSONExtract<numeric>--';
SELECT JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1);
SELECT JSONExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2);
SELECT JSONExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1);
SELECT JSONExtractBool('{"passed": true}', 'passed');
SELECT JSONExtractBool('"HX-=');

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
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(UInt8)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(Nullable(UInt8))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1, 'Int8');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2, 'Int32');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4, 'Nullable(Int64)');
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

SELECT '--const/non-const mixed--';
SELECT JSONExtractString('["a", "b", "c", "d", "e"]', idx) FROM (SELECT arrayJoin([1,2,3,4,5]) AS idx);
SELECT JSONExtractString(json, 's') FROM (SELECT arrayJoin(['{"s":"u"}', '{"s":"v"}']) AS json);

SELECT '--show error: type should be const string';
SELECT JSONExtractKeysAndValues([], JSONLength('^?V{LSwp')); -- { serverError 44 }
WITH '{"i": 1, "f": 1.2}' AS json SELECT JSONExtract(json, 'i', JSONType(json, 'i')); -- { serverError 44 }


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

SELECT '--JSONExtract<numeric>--';
SELECT JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1);
SELECT JSONExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2);
SELECT JSONExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1);
SELECT JSONExtractBool('{"passed": true}', 'passed');
SELECT JSONExtractBool('"HX-=');

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
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(UInt8)');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(Nullable(UInt8))');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1, 'Int8');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2, 'Int32');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4, 'Nullable(Int64)');
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

SELECT '--const/non-const mixed--';
SELECT JSONExtractString('["a", "b", "c", "d", "e"]', idx) FROM (SELECT arrayJoin([1,2,3,4,5]) AS idx);
SELECT JSONExtractString(json, 's') FROM (SELECT arrayJoin(['{"s":"u"}', '{"s":"v"}']) AS json);

SELECT '--show error: type should be const string';
SELECT JSONExtractKeysAndValues([], JSONLength('^?V{LSwp')); -- { serverError 44 }
WITH '{"i": 1, "f": 1.2}' AS json SELECT JSONExtract(json, 'i', JSONType(json, 'i')); -- { serverError 44 }
