-- Regression tests for review findings on the compatibility parsing of aggregate_function_input_format:
-- 1. In TabSeparated/Escaped formats, the field must be decoded from its escaped form before the legacy
--    per-element parse, so escape sequences inside elements (e.g. `["a\tb"]`) become real characters,
--    as in the representation released in v25.12 and v26.1.
-- 2. In JSON formats, the legacy string-wrapped compatibility form does not shadow any native form:
--    quoted scalars parse to the same values, and for a JSON-typed argument the string-wrapped object form
--    is the only working quoted form (the JSON type itself has no native quoted-token form).

SET schema_inference_make_columns_nullable = 0;
SET enable_json_type = 1;

SELECT '=== TSV array mode: escape sequences inside elements are decoded before element parsing ===';
SET aggregate_function_input_format = 'array';
CREATE TABLE test_agg_tsv_escape (vals AggregateFunction(groupUniqArray, String)) ENGINE = Memory;
INSERT INTO test_agg_tsv_escape SELECT * FROM format(TSV, 'vals AggregateFunction(groupUniqArray, String)',
$$["a\tb","c\nd"]
$$);
SELECT arrayMap(x -> hex(x), arraySort(groupUniqArrayMerge(vals))) FROM test_agg_tsv_escape;
DROP TABLE test_agg_tsv_escape;

SELECT '=== JSONEachRow value mode: string-wrapped object form for a JSON argument ===';
SET aggregate_function_input_format = 'value';
CREATE TABLE test_agg_json_wrapped (user_id UInt64, val AggregateFunction(any, JSON)) ENGINE = Memory;
INSERT INTO test_agg_json_wrapped SELECT * FROM format(JSONEachRow, 'user_id UInt64, val AggregateFunction(any, JSON)',
$$
{"user_id": 1, "val": "{\"a\": 1}"}
{"user_id": 2, "val": {"b": 2}}
$$);
SELECT user_id, anyMerge(val) FROM test_agg_json_wrapped GROUP BY user_id ORDER BY user_id;
DROP TABLE test_agg_json_wrapped;

SELECT '=== JSONEachRow value mode: quoted scalars parse to the same values as native ones ===';
CREATE TABLE test_agg_scalar_quoted (user_id UInt64, val AggregateFunction(sum, UInt64)) ENGINE = Memory;
INSERT INTO test_agg_scalar_quoted SELECT * FROM format(JSONEachRow, 'user_id UInt64, val AggregateFunction(sum, UInt64)',
$$
{"user_id": 1, "val": "42"}
{"user_id": 2, "val": 7}
$$);
SELECT user_id, sumMerge(val) FROM test_agg_scalar_quoted GROUP BY user_id ORDER BY user_id;
DROP TABLE test_agg_scalar_quoted;

SELECT '=== JSONEachRow value mode: quoted String scalar with JSON escapes ===';
CREATE TABLE test_agg_string_quoted (user_id UInt64, val AggregateFunction(any, String)) ENGINE = Memory;
INSERT INTO test_agg_string_quoted SELECT * FROM format(JSONEachRow, 'user_id UInt64, val AggregateFunction(any, String)',
$$
{"user_id": 1, "val": "he\tllo"}
$$);
SELECT user_id, hex(anyMerge(val)) FROM test_agg_string_quoted GROUP BY user_id ORDER BY user_id;
DROP TABLE test_agg_string_quoted;

SELECT '=== JSONEachRow value mode: legacy string-wrapped form still accepted for composite values ===';
CREATE TABLE test_agg_composite_wrapped (user_id UInt64, val AggregateFunction(any, Array(UInt64))) ENGINE = Memory;
INSERT INTO test_agg_composite_wrapped SELECT * FROM format(JSONEachRow, 'user_id UInt64, val AggregateFunction(any, Array(UInt64))',
$$
{"user_id": 1, "val": "[1,2,3]"}
{"user_id": 2, "val": [4,5]}
$$);
SELECT user_id, anyMerge(val) FROM test_agg_composite_wrapped GROUP BY user_id ORDER BY user_id;
DROP TABLE test_agg_composite_wrapped;

SELECT '=== JSONEachRow array mode: legacy string-wrapped form still accepted ===';
SET aggregate_function_input_format = 'array';
CREATE TABLE test_agg_array_wrapped (user_id UInt64, vals AggregateFunction(groupUniqArray, String)) ENGINE = Memory;
INSERT INTO test_agg_array_wrapped SELECT * FROM format(JSONEachRow, 'user_id UInt64, vals AggregateFunction(groupUniqArray, String)',
$$
{"user_id": 1, "vals": "[\"x\",\"y\"]"}
{"user_id": 2, "vals": ["z"]}
$$);
SELECT user_id, arraySort(groupUniqArrayMerge(vals)) FROM test_agg_array_wrapped GROUP BY user_id ORDER BY user_id;
DROP TABLE test_agg_array_wrapped;
