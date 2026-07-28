-- Regression test for the representation released in v25.12 and v26.1: with aggregate_function_input_format = 'value',
-- JSON formats accepted the value as a JSON string holding its textual representation, and for the self-describing
-- Dynamic and Variant argument types that content was resolved by its own text, not kept as a JSON string scalar:
-- {"x": "42"} gave the number 42 for a Dynamic argument and the array [42] for Variant(String, Array(UInt64)).
-- The unified deserialization keeps that resolution for quoted tokens and adds the native unquoted JSON forms.

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

-- Released parsed the string-wrapped content with the argument type's CSV deserialization, which truncated a
-- composite value at the first comma (the string '[1' for "[1, 2]"). The unified deserialization reads the whole
-- token instead, so the wrapped composite forms round-trip.
SELECT '=== Dynamic: string-wrapped composite value is no longer truncated at the comma ===';
INSERT INTO test_agg_dynamic_value SELECT * FROM format(JSONEachRow, 'id UInt64, val AggregateFunction(any, Dynamic)',
$$
{"id": 10, "val": "[1, 2]"}
$$);
SELECT id, toString(anyMerge(val)), dynamicType(anyMerge(val)) FROM test_agg_dynamic_value WHERE id = 10 GROUP BY id;
DROP TABLE test_agg_dynamic_value;

-- For a Variant with a composite alternative the released per-value CSV parse was degenerate: it turned the
-- wrapped "42" into the array [42] and "NULL" into [0], and truncated "[1, 2]" at the comma into the string '[1'.
-- The unified deserialization resolves the content through the Variant text parse instead, which is the same
-- replacement of the degenerate CSV-field behavior that 'array' mode makes for composite element forms.
SELECT '=== Variant(String, Array(UInt64)): string-wrapped values resolved by the Variant text parse ===';
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
