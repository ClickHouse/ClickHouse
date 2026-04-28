-- Exercise JSONExtract* against the many JSONExtractTree node classes in
-- src/Formats/JSONExtractTree.cpp: NumericNode, BoolNode, StringNode,
-- FixedStringNode, DecimalNode, DateNode, DateTimeNode, UUIDNode, IPv4Node,
-- IPv6Node, EnumNode, LowCardinalityNode, NullableNode, ArrayNode,
-- TupleNode, MapNode, VariantNode, DynamicNode, ObjectNode.

SELECT '--- NumericNode: JSON number types ---';
SELECT JSONExtract('{"x": 1.5}', 'x', 'Float64');
SELECT JSONExtract('{"x": 1.5}', 'x', 'Float32');
SELECT JSONExtract('{"x": 1.5}', 'x', 'Int32');
SELECT JSONExtract('{"x": 123456789}', 'x', 'Int64');
SELECT JSONExtract('{"x": -123456789}', 'x', 'Int64');
SELECT JSONExtract('{"x": 200}', 'x', 'Int8'); -- out of range, returns default
SELECT JSONExtract('{"x": -1}', 'x', 'UInt32');

SELECT '--- NumericNode: strings with allow_type_conversion=1 (default) ---';
SELECT JSONExtract('{"x": "42"}', 'x', 'Int64');
SELECT JSONExtract('{"x": "3.14"}', 'x', 'Float64');
SELECT JSONExtract('{"x": "1.5"}', 'x', 'Int32');
SELECT JSONExtract('{"x": "not a number"}', 'x', 'Int32');
SELECT JSONExtract('{"x": "1e100"}', 'x', 'Int32');

SELECT '--- NumericNode: bool conversion ---';
SELECT JSONExtract('{"x": true}', 'x', 'UInt8');
SELECT JSONExtract('{"x": false}', 'x', 'Int8');

SELECT '--- NumericNode: null -> default with null_as_default setting ---';
SELECT JSONExtract('{"x": null}', 'x', 'Int32') SETTINGS input_format_null_as_default=1;
SELECT JSONExtract('{"x": null}', 'x', 'Int32') SETTINGS input_format_null_as_default=0;

SELECT '--- NumericNode: strings with allow_type_conversion=0 ---';
SELECT JSONExtract('{"x": "42"}', 'x', 'Int64') SETTINGS input_format_json_try_infer_numbers_from_strings=0, input_format_json_read_numbers_as_strings=0;

SELECT '--- BoolNode ---';
SELECT JSONExtract('{"x": true}', 'x', 'Bool');
SELECT JSONExtract('{"x": false}', 'x', 'Bool');
SELECT JSONExtract('{"x": 1}', 'x', 'Bool');
SELECT JSONExtract('{"x": 0}', 'x', 'Bool');
SELECT JSONExtract('{"x": "true"}', 'x', 'Bool');
SELECT JSONExtract('{"x": "false"}', 'x', 'Bool');
SELECT JSONExtract('{"x": null}', 'x', 'Bool');

SELECT '--- StringNode ---';
SELECT JSONExtract('{"x": "hello"}', 'x', 'String');
SELECT JSONExtract('{"x": 42}', 'x', 'String');
SELECT JSONExtract('{"x": 1.5}', 'x', 'String');
SELECT JSONExtract('{"x": true}', 'x', 'String');
SELECT JSONExtract('{"x": null}', 'x', 'String');
SELECT JSONExtract('{"x": [1,2]}', 'x', 'String');
SELECT JSONExtract('{"x": {"a":1}}', 'x', 'String');

SELECT '--- FixedStringNode ---';
SELECT JSONExtract('{"x": "abcde"}', 'x', 'FixedString(5)');
SELECT JSONExtract('{"x": "short"}', 'x', 'FixedString(10)');
SELECT JSONExtract('{"x": "toolong"}', 'x', 'FixedString(3)'); -- returns default
SELECT JSONExtract('{"x": 42}', 'x', 'FixedString(3)');

SELECT '--- DecimalNode ---';
SELECT JSONExtract('{"x": 1.234}', 'x', 'Decimal(10, 3)');
SELECT JSONExtract('{"x": "1.234"}', 'x', 'Decimal(10, 3)');
SELECT JSONExtract('{"x": 0}', 'x', 'Decimal(10, 3)');
SELECT JSONExtract('{"x": true}', 'x', 'Decimal(10, 3)');
SELECT JSONExtract('{"x": "huge number"}', 'x', 'Decimal(10, 3)');

SELECT '--- DateNode / DateTimeNode / DateTime64Node ---';
SELECT JSONExtract('{"x": "2023-01-02"}', 'x', 'Date');
SELECT JSONExtract('{"x": "2023-01-02 03:04:05"}', 'x', 'DateTime(\'UTC\')');
SELECT JSONExtract('{"x": 1672628645}', 'x', 'DateTime(\'UTC\')');
SELECT JSONExtract('{"x": "2023-01-02 03:04:05.123"}', 'x', 'DateTime64(3, \'UTC\')');
SELECT JSONExtract('{"x": "bogus"}', 'x', 'Date');

SELECT '--- UUIDNode ---';
SELECT JSONExtract('{"x": "550e8400-e29b-41d4-a716-446655440000"}', 'x', 'UUID');
SELECT JSONExtract('{"x": "invalid-uuid"}', 'x', 'UUID');
SELECT JSONExtract('{"x": 42}', 'x', 'UUID');

SELECT '--- IPv4 / IPv6 Node ---';
SELECT JSONExtract('{"x": "192.168.1.1"}', 'x', 'IPv4');
SELECT JSONExtract('{"x": "invalid"}', 'x', 'IPv4');
SELECT JSONExtract('{"x": "::1"}', 'x', 'IPv6');
SELECT JSONExtract('{"x": "2001:db8::1"}', 'x', 'IPv6');
SELECT JSONExtract('{"x": 42}', 'x', 'IPv4');

SELECT '--- EnumNode: by name and by number ---';
SELECT JSONExtract('{"x": "a"}', 'x', 'Enum8(''a''=1, ''b''=2)');
SELECT JSONExtract('{"x": 2}', 'x', 'Enum8(''a''=1, ''b''=2)');
SELECT JSONExtract('{"x": "a"}', 'x', 'Enum16(''a''=1, ''b''=2)');

SELECT '--- LowCardinalityNode ---';
SELECT JSONExtract('{"x": "hello"}', 'x', 'LowCardinality(String)');
SELECT JSONExtract('{"x": 42}', 'x', 'LowCardinality(Nullable(String))');

SELECT '--- NullableNode ---';
SELECT JSONExtract('{"x": null}', 'x', 'Nullable(Int32)');
SELECT JSONExtract('{"x": 42}', 'x', 'Nullable(Int32)');
SELECT JSONExtract('{"x": "42"}', 'x', 'Nullable(Int32)');
SELECT JSONExtract('{"x": "not-a-number"}', 'x', 'Nullable(Int32)');

SELECT '--- ArrayNode ---';
SELECT JSONExtract('{"x": [1,2,3]}', 'x', 'Array(Int8)');
SELECT JSONExtract('{"x": [1,"2",3.5]}', 'x', 'Array(Int64)');
SELECT JSONExtract('{"x": [[1,2],[3,4]]}', 'x', 'Array(Array(Int8))');
SELECT JSONExtract('{"x": []}', 'x', 'Array(Int32)');
SELECT JSONExtract('{"x": "not an array"}', 'x', 'Array(Int32)');
SELECT JSONExtract('{"x": null}', 'x', 'Array(Int32)');

SELECT '--- TupleNode: by keys and by position ---';
SELECT JSONExtract('{"x": {"a":1, "b":"hi"}}', 'x', 'Tuple(a Int32, b String)');
SELECT JSONExtract('{"x": [1, "hi"]}', 'x', 'Tuple(Int32, String)');
SELECT JSONExtract('{"x": {"a":1}}', 'x', 'Tuple(a Int32, b String)');
SELECT JSONExtract('{"x": {"b":"hi"}}', 'x', 'Tuple(a Int32, b String)');
SELECT JSONExtract('{"x": "not a tuple"}', 'x', 'Tuple(a Int32, b String)');

SELECT '--- MapNode ---';
SELECT JSONExtract('{"x": {"a":1, "b":2}}', 'x', 'Map(String, Int32)');
SELECT JSONExtract('{"x": {"a":1, "b":"hi"}}', 'x', 'Map(String, String)');
-- JSONExtract only supports Map with String keys; swap to String.
SELECT JSONExtract('{"x": {"1":"a", "2":"b"}}', 'x', 'Map(String, String)');
SELECT JSONExtract('{"x": "not a map"}', 'x', 'Map(String, Int32)');
SELECT JSONExtract('{"x": null}', 'x', 'Map(String, Int32)');

SELECT '--- VariantNode ---';
SELECT JSONExtract('{"x": 1}', 'x', 'Variant(String, Int64, Float64)');
SELECT JSONExtract('{"x": "hi"}', 'x', 'Variant(String, Int64, Float64)');
SELECT JSONExtract('{"x": 3.14}', 'x', 'Variant(String, Int64, Float64)');
SELECT JSONExtract('{"x": true}', 'x', 'Variant(String, Bool)');

SELECT '--- DynamicNode ---';
SELECT JSONExtract('{"x": 42}', 'x', 'Dynamic');
SELECT JSONExtract('{"x": "hi"}', 'x', 'Dynamic');
SELECT JSONExtract('{"x": [1,2,3]}', 'x', 'Dynamic');
SELECT JSONExtract('{"x": {"a":1}}', 'x', 'Dynamic');
SELECT JSONExtract('{"x": null}', 'x', 'Dynamic');

SELECT '--- ObjectNode (JSON type) ---';
SELECT JSONExtract('{"x": {"a":1, "b":"hi"}}', 'x', 'JSON');
SELECT JSONExtract('{"x": {"a":{"b":{"c":42}}}}', 'x', 'JSON');

SELECT '--- Nested path extraction ---';
SELECT JSONExtract('{"a":{"b":{"c":42}}}', 'a', 'b', 'c', 'Int32');
SELECT JSONExtract('{"a":[{"b":1},{"b":2}]}', 'a', 1, 'b', 'Int32');
SELECT JSONExtract('{"a":[{"b":1},{"b":2}]}', 'a', -1, 'b', 'Int32');
SELECT JSONExtract('{"a":[1,2,3]}', 'a', 5, 'Int32'); -- out of range index

SELECT '--- JSONExtractKeys / KeysAndValues / Raw ---';
SELECT JSONExtractKeys('{"a":1,"b":2}');
SELECT JSONExtractKeys('{}');
SELECT JSONExtractKeys('null');
SELECT JSONExtractKeysAndValues('{"a":1,"b":2}', 'Int8');
SELECT JSONExtractKeysAndValuesRaw('{"a":1,"b":[2,3]}');

SELECT '--- JSONExtractArrayRaw ---';
SELECT JSONExtractArrayRaw('[1,2,3]');
SELECT JSONExtractArrayRaw('{"not":"array"}');

SELECT '--- JSONExtract on nonexistent path returns default ---';
SELECT JSONExtract('{"a":1}', 'b', 'Int32');
SELECT JSONExtract('{"a":1}', 'a', 'b', 'Int32');
