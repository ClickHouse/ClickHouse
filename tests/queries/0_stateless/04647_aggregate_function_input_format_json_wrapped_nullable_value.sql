-- Regression test for the representation released in v25.12 and v26.1: with aggregate_function_input_format = 'value',
-- JSON formats accepted the value as a JSON string holding its textual representation, e.g. {"x": "42"}, and parsed
-- that content with the argument type's CSV deserialization. For a Nullable argument that parse recognized the CSV
-- null representation ('\N' by default) and never turned the words NULL and null into a null for string-like nested
-- types. The unified deserialization must keep accepting these forms alongside the native JSON ones.

SET aggregate_function_input_format = 'value';

SELECT '=== Nullable(UInt64): legacy string-wrapped \\N is a null ===';
CREATE TABLE test_agg_nullable_value (id UInt64, val AggregateFunction(any, Nullable(UInt64))) ENGINE = Memory;
INSERT INTO test_agg_nullable_value SELECT * FROM format(JSONEachRow, 'id UInt64, val AggregateFunction(any, Nullable(UInt64))',
$$
{"id": 1, "val": "\\N"}
{"id": 2, "val": "42"}
{"id": 3, "val": 43}
{"id": 4, "val": null}
{"id": 5, "val": "NULL"}
$$);
SELECT id, anyMerge(val) IS NULL, anyMerge(val) FROM test_agg_nullable_value GROUP BY id ORDER BY id;
DROP TABLE test_agg_nullable_value;

SELECT '=== Nullable(String): only \\N is a null, the words NULL and null stay strings ===';
CREATE TABLE test_agg_nullable_string_value (id UInt64, val AggregateFunction(any, Nullable(String))) ENGINE = Memory;
INSERT INTO test_agg_nullable_string_value SELECT * FROM format(JSONEachRow, 'id UInt64, val AggregateFunction(any, Nullable(String))',
$$
{"id": 1, "val": "\\N"}
{"id": 2, "val": "NULL"}
{"id": 3, "val": "null"}
{"id": 4, "val": "hello"}
{"id": 5, "val": "a,b"}
{"id": 6, "val": null}
$$);
SELECT id, anyMerge(val) IS NULL, anyMerge(val) FROM test_agg_nullable_string_value GROUP BY id ORDER BY id;
DROP TABLE test_agg_nullable_string_value;

SELECT '=== LowCardinality(Nullable(String)): the same forms ===';
CREATE TABLE test_agg_lc_nullable_value (id UInt64, val AggregateFunction(any, LowCardinality(Nullable(String)))) ENGINE = Memory;
INSERT INTO test_agg_lc_nullable_value SELECT * FROM format(JSONEachRow, 'id UInt64, val AggregateFunction(any, LowCardinality(Nullable(String)))',
$$
{"id": 1, "val": "\\N"}
{"id": 2, "val": "NULL"}
{"id": 3, "val": "text"}
$$);
SELECT id, anyMerge(val) IS NULL, anyMerge(val) FROM test_agg_lc_nullable_value GROUP BY id ORDER BY id;
DROP TABLE test_agg_lc_nullable_value;

SELECT '=== Nullable(Float64): \\N is a null and NaN still parses ===';
CREATE TABLE test_agg_nullable_float_value (id UInt64, val AggregateFunction(any, Nullable(Float64))) ENGINE = Memory;
INSERT INTO test_agg_nullable_float_value SELECT * FROM format(JSONEachRow, 'id UInt64, val AggregateFunction(any, Nullable(Float64))',
$$
{"id": 1, "val": "\\N"}
{"id": 2, "val": "NaN"}
{"id": 3, "val": "1.5"}
$$);
SELECT id, anyMerge(val) IS NULL, anyMerge(val) FROM test_agg_nullable_float_value GROUP BY id ORDER BY id;
DROP TABLE test_agg_nullable_float_value;

SELECT '=== Nullable(String) in array mode: the legacy string-wrapped array is unaffected ===';
SET aggregate_function_input_format = 'array';
CREATE TABLE test_agg_nullable_array (id UInt64, vals AggregateFunction(groupArray, Nullable(String))) ENGINE = Memory;
INSERT INTO test_agg_nullable_array SELECT * FROM format(JSONEachRow, 'id UInt64, vals AggregateFunction(groupArray, Nullable(String))',
$$
{"id": 1, "vals": "[\"a\",\"b\"]"}
{"id": 2, "vals": ["c", null]}
$$);
SELECT id, groupArrayMerge(vals) FROM test_agg_nullable_array GROUP BY id ORDER BY id;
DROP TABLE test_agg_nullable_array;
