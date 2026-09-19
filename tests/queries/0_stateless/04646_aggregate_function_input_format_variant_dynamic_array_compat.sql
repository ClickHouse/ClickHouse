-- Regression test for the representation released in v25.12 and v26.1: with aggregate_function_input_format = 'array',
-- single-argument aggregates of Variant and Dynamic types accepted elements in CSV quoting, e.g. '["a","b"]' or
-- '[42,"b"]' for AggregateFunction(groupUniqArray, Variant(String, UInt64)), and parsed the bareword NULL as the
-- string 'NULL'. The unified deserialization must keep accepting these forms alongside the new native ones.

SET schema_inference_make_columns_nullable = 0;
SET aggregate_function_input_format = 'array';

SELECT '=== Variant: legacy double-quoted elements (VALUES) ===';
CREATE TABLE test_agg_variant_compat (user_id UInt64, vals AggregateFunction(groupUniqArray, Variant(String, UInt64))) ENGINE = Memory;
INSERT INTO test_agg_variant_compat VALUES (1, '["a","b"]');
SELECT user_id, arraySort(v -> toString(v), groupUniqArrayMerge(vals)) FROM test_agg_variant_compat WHERE user_id = 1 GROUP BY user_id;

SELECT '=== Variant: legacy mixed bareword number and double-quoted string, with variant types ===';
INSERT INTO test_agg_variant_compat VALUES (2, '[42,"b"]');
SELECT user_id, arraySort(v -> toString(v), arrayMap(v -> (toString(v), variantType(v))::Tuple(String, String), groupUniqArrayMerge(vals))) FROM test_agg_variant_compat WHERE user_id = 2 GROUP BY user_id;

SELECT '=== Variant: legacy bareword NULL parses as the string NULL (as released) ===';
INSERT INTO test_agg_variant_compat VALUES (3, '[NULL,"c"]');
SELECT user_id, arraySort(v -> toString(v), arrayMap(v -> (toString(v), variantType(v))::Tuple(String, String), groupUniqArrayMerge(vals))) FROM test_agg_variant_compat WHERE user_id = 3 GROUP BY user_id;

SELECT '=== Variant: legacy string-wrapped form with double-quoted elements (JSONEachRow) ===';
INSERT INTO test_agg_variant_compat SELECT * FROM format(JSONEachRow, 'user_id UInt64, vals AggregateFunction(groupUniqArray, Variant(String, UInt64))',
$$
{"user_id": 4, "vals": "[\"d\",\"e\"]"}
{"user_id": 5, "vals": "[7,\"f\"]"}
$$);
SELECT user_id, arraySort(v -> toString(v), groupUniqArrayMerge(vals)) FROM test_agg_variant_compat WHERE user_id IN (4, 5) GROUP BY user_id ORDER BY user_id;

SELECT '=== Variant: native JSON array form still works (JSONEachRow) ===';
INSERT INTO test_agg_variant_compat SELECT * FROM format(JSONEachRow, 'user_id UInt64, vals AggregateFunction(groupUniqArray, Variant(String, UInt64))',
$$
{"user_id": 6, "vals": ["g", 8]}
$$);
SELECT user_id, arraySort(v -> toString(v), groupUniqArrayMerge(vals)) FROM test_agg_variant_compat WHERE user_id = 6 GROUP BY user_id;

SELECT '=== Variant: native single-quoted string elements (VALUES) ===';
INSERT INTO test_agg_variant_compat VALUES (7, '[''h'',"i"]');
SELECT user_id, arraySort(v -> toString(v), groupUniqArrayMerge(vals)) FROM test_agg_variant_compat WHERE user_id = 7 GROUP BY user_id;
DROP TABLE test_agg_variant_compat;

SELECT '=== Variant with composite variant: native bracketed elements take the quoted parse ===';
CREATE TABLE test_agg_variant_array_compat (user_id UInt64, vals AggregateFunction(groupUniqArray, Variant(Array(UInt64), String))) ENGINE = Memory;
INSERT INTO test_agg_variant_array_compat VALUES (1, '[[1,2],"a"]');
SELECT user_id, arraySort(v -> toString(v), groupUniqArrayMerge(vals)) FROM test_agg_variant_array_compat WHERE user_id = 1 GROUP BY user_id;
DROP TABLE test_agg_variant_array_compat;

SELECT '=== Dynamic: legacy double-quoted elements and bareword number, with dynamic types ===';
CREATE TABLE test_agg_dynamic_compat (user_id UInt64, vals AggregateFunction(groupUniqArray, Dynamic)) ENGINE = Memory;
INSERT INTO test_agg_dynamic_compat VALUES (1, '["a","b"]'), (2, '[42,"c"]');
SELECT user_id, arraySort(v -> toString(v), arrayMap(v -> (toString(v), dynamicType(v))::Tuple(String, String), groupUniqArrayMerge(vals))) FROM test_agg_dynamic_compat WHERE user_id IN (1, 2) GROUP BY user_id ORDER BY user_id;

SELECT '=== Dynamic: legacy string-wrapped form (JSONEachRow) and bareword NULL as string ===';
INSERT INTO test_agg_dynamic_compat SELECT * FROM format(JSONEachRow, 'user_id UInt64, vals AggregateFunction(groupUniqArray, Dynamic)',
$$
{"user_id": 3, "vals": "[5,\"d\"]"}
$$);
INSERT INTO test_agg_dynamic_compat VALUES (4, '[NULL,"e"]');
SELECT user_id, arraySort(v -> toString(v), groupUniqArrayMerge(vals)) FROM test_agg_dynamic_compat WHERE user_id IN (3, 4) GROUP BY user_id ORDER BY user_id;

SELECT '=== Dynamic: native single-quoted string elements (VALUES) ===';
INSERT INTO test_agg_dynamic_compat VALUES (5, '[''f'',"g"]');
SELECT user_id, arraySort(v -> toString(v), groupUniqArrayMerge(vals)) FROM test_agg_dynamic_compat WHERE user_id = 5 GROUP BY user_id;
DROP TABLE test_agg_dynamic_compat;
