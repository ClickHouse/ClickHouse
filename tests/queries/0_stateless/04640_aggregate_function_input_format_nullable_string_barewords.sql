-- Regression test for the representation released in v25.12 and v26.1: with aggregate_function_input_format = 'array',
-- single-argument string-like Nullable aggregates parsed EVERY element with the argument type's CSV parse,
-- so unquoted words were accepted as string values, including ones starting with N/n: '[NaN,"a"]' produced
-- the string 'NaN' and '[NULL,"a"]' produced the STRING 'NULL' (not a null). These must not be diverted to the
-- quoted parse (which rejects barewords and turns NULL into a null). Non-string Nullable arguments keep the
-- N/n -> quoted dispatch: for them the released CSV parse rejected NULL/null, so native NULL support is additive.

SET schema_inference_make_columns_nullable = 0;
SET aggregate_function_input_format = 'array';

SELECT '=== Nullable(String): barewords NaN, NULLIFY and NULL parse as strings (JSONEachRow legacy string-wrapped) ===';
CREATE TABLE test_agg_nullable_str (user_id UInt64, vals AggregateFunction(groupUniqArray, Nullable(String))) ENGINE = Memory;
INSERT INTO test_agg_nullable_str SELECT * FROM format(JSONEachRow, 'user_id UInt64, vals AggregateFunction(groupUniqArray, Nullable(String))',
$$
{"user_id": 1, "vals": "[NaN,\"a\"]"}
{"user_id": 2, "vals": "[NULL,\"b\"]"}
{"user_id": 3, "vals": "[NULLIFY,\"c\"]"}
{"user_id": 4, "vals": "[nan,\"d\"]"}
$$);
SELECT user_id, arraySort(x -> assumeNotNull(x), groupUniqArrayMerge(vals)) FROM test_agg_nullable_str GROUP BY user_id ORDER BY user_id;
DROP TABLE test_agg_nullable_str;

SELECT '=== Nullable(String): barewords in VALUES string-wrapped and CSV ===';
CREATE TABLE test_agg_nullable_str2 (user_id UInt64, vals AggregateFunction(groupUniqArray, Nullable(String))) ENGINE = Memory;
INSERT INTO test_agg_nullable_str2 VALUES (1, '[NaN,"a"]'), (2, '[NULL,''b'']');
INSERT INTO test_agg_nullable_str2 SELECT * FROM format(CSV, 'user_id UInt64, vals AggregateFunction(groupUniqArray, Nullable(String))',
$$5,"[NaN,""e""]"
6,"[NULL,'f']"
$$);
SELECT user_id, arraySort(x -> assumeNotNull(x), groupUniqArrayMerge(vals)) FROM test_agg_nullable_str2 GROUP BY user_id ORDER BY user_id;
DROP TABLE test_agg_nullable_str2;

SELECT '=== Nullable(Enum8): bareword starting with N parses via CSV (released form) ===';
CREATE TABLE test_agg_nullable_enum (user_id UInt64, vals AggregateFunction(groupUniqArray, Nullable(Enum8('no' = 1, 'yes' = 2)))) ENGINE = Memory;
INSERT INTO test_agg_nullable_enum VALUES (1, '[no,"yes"]');
SELECT user_id, arraySort(x -> assumeNotNull(x), groupUniqArrayMerge(vals)) FROM test_agg_nullable_enum GROUP BY user_id ORDER BY user_id;
DROP TABLE test_agg_nullable_enum;

SELECT '=== Nullable(Float64): NaN and native NULL still parse (N/n -> quoted dispatch kept for non-string types) ===';
CREATE TABLE test_agg_nullable_f64 (user_id UInt64, vals AggregateFunction(groupArray, Nullable(Float64))) ENGINE = Memory;
INSERT INTO test_agg_nullable_f64 VALUES (1, '[NaN,1.5,"2"]'), (2, [NULL, 3.5]), (3, '[null,4.5]');
SELECT user_id, groupArrayMerge(vals) FROM test_agg_nullable_f64 GROUP BY user_id ORDER BY user_id;
DROP TABLE test_agg_nullable_f64;

SELECT '=== Nullable(UInt64): native NULL parses, quoted number kept ===';
CREATE TABLE test_agg_nullable_u64 (user_id UInt64, vals AggregateFunction(groupArray, Nullable(UInt64))) ENGINE = Memory;
INSERT INTO test_agg_nullable_u64 VALUES (1, '[NULL,"1",2]');
SELECT user_id, groupArrayMerge(vals) FROM test_agg_nullable_u64 GROUP BY user_id ORDER BY user_id;
DROP TABLE test_agg_nullable_u64;
