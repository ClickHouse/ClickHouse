-- Test for the representation introduced in v25.12 and v26.1: with aggregate_function_input_format = 'value',
-- JSON formats accept the value as a JSON string holding its textual representation, and for the self-describing
-- Dynamic and Variant argument types that content is resolved by its own text, not kept as a JSON string scalar:
-- {"x": "42"} gives the number 42 for a Dynamic argument. The unified deserialization parses the string content
-- with the argument type's whole-text parser (so composite values keep their commas intact, unlike the
-- CSV-based parse of the first release) and adds the native unquoted JSON forms.

SET enable_dynamic_type = 1;
SET enable_variant_type = 1;
SET aggregate_function_input_format = 'value';

SELECT '=== Dynamic: legacy string-wrapped values keep the released resolution ===';
CREATE TABLE test_agg_dynamic_value (id UInt64, val AggregateFunction(any, Dynamic)) ENGINE = Memory;
INSERT INTO test_agg_dynamic_value SELECT * FROM format(JSONEachRow, 'id UInt64, val AggregateFunction(any, Dynamic)',
$$
{"id": 1, "val": "42"}
{"id": 2, "val": "hello"}
{"id": 3, "val": "2020-01-01"}
{"id": 4, "val": "true"}
{"id": 5, "val": "NULL"}
$$);
SELECT id, toString(anyMerge(val)), dynamicType(anyMerge(val)) FROM test_agg_dynamic_value GROUP BY id ORDER BY id;

SELECT '=== Dynamic: native unquoted JSON values ===';
INSERT INTO test_agg_dynamic_value SELECT * FROM format(JSONEachRow, 'id UInt64, val AggregateFunction(any, Dynamic)',
$$
{"id": 6, "val": 42}
{"id": 7, "val": [1, 2]}
{"id": 8, "val": true}
{"id": 9, "val": null}
$$);
SELECT id, toString(anyMerge(val)), dynamicType(anyMerge(val)) FROM test_agg_dynamic_value WHERE id > 5 GROUP BY id ORDER BY id;

-- String-wrapped JSON values are parsed as the whole text of the argument type, so a composite value keeps
-- its commas instead of being truncated at the first one as by the CSV-based parse of the first release.
SELECT '=== Dynamic: legacy string-wrapped composite value is parsed as a whole ===';
INSERT INTO test_agg_dynamic_value SELECT * FROM format(JSONEachRow, 'id UInt64, val AggregateFunction(any, Dynamic)',
$$
{"id": 10, "val": "[1, 2]"}
$$);
SELECT id, toString(anyMerge(val)), dynamicType(anyMerge(val)) FROM test_agg_dynamic_value WHERE id = 10 GROUP BY id;
DROP TABLE test_agg_dynamic_value;

-- For a Variant, the string content uses the whole-text parse of the Variant type: the wrapped "42" resolves
-- to the string '42', "NULL" to NULL, and "[1, 2]" to the whole array [1,2] (the CSV-based parse of the first
-- release resolved them to [42], [0], and the comma-truncated string '[1').
SELECT '=== Variant(String, Array(UInt64)): legacy string-wrapped values use the whole-text parse ===';
CREATE TABLE test_agg_variant_value (id UInt64, val AggregateFunction(any, Variant(String, Array(UInt64)))) ENGINE = Memory;
INSERT INTO test_agg_variant_value SELECT * FROM format(JSONEachRow, 'id UInt64, val AggregateFunction(any, Variant(String, Array(UInt64)))',
$$
{"id": 1, "val": "42"}
{"id": 2, "val": "hello"}
{"id": 3, "val": "[]"}
{"id": 6, "val": "NULL"}
{"id": 7, "val": "[1, 2]"}
$$);
SELECT id, toString(anyMerge(val)), variantType(anyMerge(val)) FROM test_agg_variant_value GROUP BY id ORDER BY id;

SELECT '=== Variant(String, Array(UInt64)): native unquoted JSON values ===';
INSERT INTO test_agg_variant_value SELECT * FROM format(JSONEachRow, 'id UInt64, val AggregateFunction(any, Variant(String, Array(UInt64)))',
$$
{"id": 4, "val": [1, 2]}
{"id": 5, "val": null}
$$);
SELECT id, toString(anyMerge(val)), variantType(anyMerge(val)) FROM test_agg_variant_value WHERE id IN (4, 5) GROUP BY id ORDER BY id;
DROP TABLE test_agg_variant_value;
