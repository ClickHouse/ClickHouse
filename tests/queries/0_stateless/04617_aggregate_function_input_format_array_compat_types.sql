-- Regression test for the representation released in v25.12 and v26.1: with aggregate_function_input_format = 'array',
-- single-argument aggregates of ANY type accepted elements in CSV quoting (double quotes and barewords),
-- not only String, e.g. '["a","b"]' for an Enum8 argument. The unified deserialization must keep accepting it
-- alongside the new native forms for all scalar argument types.

SET schema_inference_make_columns_nullable = 0;
SET aggregate_function_input_format = 'array';

SELECT '=== Enum8: legacy string-wrapped form with double-quoted elements (JSONEachRow) ===';
CREATE TABLE test_agg_enum_compat (user_id UInt64, vals AggregateFunction(groupUniqArray, Enum8('a' = 1, 'b' = 2, 'c' = 3))) ENGINE = Memory;
INSERT INTO test_agg_enum_compat SELECT * FROM format(JSONEachRow, 'user_id UInt64, vals AggregateFunction(groupUniqArray, Enum8(''a'' = 1, ''b'' = 2, ''c'' = 3))',
$$
{"user_id": 1, "vals": "[\"a\",\"b\"]"}
{"user_id": 2, "vals": ["a", "c"]}
$$);
SELECT user_id, arraySort(groupUniqArrayMerge(vals)) FROM test_agg_enum_compat WHERE user_id IN (1, 2) GROUP BY user_id ORDER BY user_id;

SELECT '=== Enum8: legacy double-quoted elements, native single-quoted (VALUES) ===';
INSERT INTO test_agg_enum_compat VALUES (3, '["a","c"]'), (5, ['a', 'b']);
SELECT user_id, arraySort(groupUniqArrayMerge(vals)) FROM test_agg_enum_compat WHERE user_id IN (3, 5) GROUP BY user_id ORDER BY user_id;
DROP TABLE test_agg_enum_compat;

SELECT '=== Date: legacy double-quoted elements and native single-quoted ===';
CREATE TABLE test_agg_date_compat (user_id UInt64, vals AggregateFunction(groupUniqArray, Date)) ENGINE = Memory;
INSERT INTO test_agg_date_compat VALUES (1, '["2020-01-01","2020-01-02"]'), (2, ['2021-05-05', '2021-06-06']);
INSERT INTO test_agg_date_compat SELECT * FROM format(JSONEachRow, 'user_id UInt64, vals AggregateFunction(groupUniqArray, Date)',
$$
{"user_id": 3, "vals": "[\"2022-02-02\"]"}
$$);
SELECT user_id, arraySort(groupUniqArrayMerge(vals)) FROM test_agg_date_compat GROUP BY user_id ORDER BY user_id;
DROP TABLE test_agg_date_compat;

SELECT '=== UInt64: legacy quoted numbers and native bare numbers ===';
CREATE TABLE test_agg_num_compat (user_id UInt64, vals AggregateFunction(groupUniqArray, UInt64)) ENGINE = Memory;
INSERT INTO test_agg_num_compat VALUES (1, '["1","2"]'), (2, '[''3'',''4'']'), (3, [5, 6]);
SELECT user_id, arraySort(groupUniqArrayMerge(vals)) FROM test_agg_num_compat GROUP BY user_id ORDER BY user_id;
DROP TABLE test_agg_num_compat;

SELECT '=== Nullable(UInt64): native NULL keyword still works ===';
CREATE TABLE test_agg_null_compat (user_id UInt64, vals AggregateFunction(groupUniqArray, Nullable(UInt64))) ENGINE = Memory;
INSERT INTO test_agg_null_compat VALUES (1, [1, NULL, 2]), (2, '["3","4"]');
SELECT user_id, arraySort(groupUniqArrayMerge(vals)) FROM test_agg_null_compat GROUP BY user_id ORDER BY user_id;
DROP TABLE test_agg_null_compat;

SELECT '=== UUID: legacy double-quoted elements ===';
CREATE TABLE test_agg_uuid_compat (user_id UInt64, vals AggregateFunction(groupUniqArray, UUID)) ENGINE = Memory;
INSERT INTO test_agg_uuid_compat VALUES (1, '["00000000-0000-0000-0000-000000000001","00000000-0000-0000-0000-000000000002"]'), (2, ['00000000-0000-0000-0000-000000000003']);
SELECT user_id, arraySort(groupUniqArrayMerge(vals)) FROM test_agg_uuid_compat GROUP BY user_id ORDER BY user_id;
DROP TABLE test_agg_uuid_compat;

SELECT '=== Array argument type keeps the native form (composite types are not on the legacy path) ===';
CREATE TABLE test_agg_array_native (user_id UInt64, vals AggregateFunction(groupUniqArray, Array(String))) ENGINE = Memory;
INSERT INTO test_agg_array_native VALUES (1, [['x', 'y'], ['z']]);
SELECT user_id, arraySort(groupUniqArrayMerge(vals)) FROM test_agg_array_native GROUP BY user_id ORDER BY user_id;
DROP TABLE test_agg_array_native;
