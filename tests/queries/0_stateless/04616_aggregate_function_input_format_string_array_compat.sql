-- Regression test for the representation released in v25.12 and v26.1: with aggregate_function_input_format = 'array',
-- single-argument String aggregates accepted elements in CSV quoting, e.g. '["apple","banana"]' (double quotes),
-- in all text formats. The unified deserialization must keep accepting it alongside the new native forms.

SET schema_inference_make_columns_nullable = 0;
SET aggregate_function_input_format = 'array';

CREATE TABLE test_agg_string_compat (user_id UInt64, strings AggregateFunction(groupUniqArray, String)) ENGINE = Memory;

SELECT '=== VALUES: legacy string-wrapped form with double-quoted elements ===';
INSERT INTO test_agg_string_compat VALUES (1, '["apple","banana","cherry"]');
SELECT user_id, arraySort(groupUniqArrayMerge(strings)) FROM test_agg_string_compat WHERE user_id = 1 GROUP BY user_id;

SELECT '=== VALUES: native form with double-quoted and mixed-quoted elements ===';
INSERT INTO test_agg_string_compat VALUES (2, ["grape","melon"]), (3, ['fig',"date"]);
SELECT user_id, arraySort(groupUniqArrayMerge(strings)) FROM test_agg_string_compat WHERE user_id IN (2, 3) GROUP BY user_id ORDER BY user_id;

SELECT '=== VALUES: native form with single-quoted elements still works ===';
INSERT INTO test_agg_string_compat VALUES (4, ['kiwi', 'lime']);
SELECT user_id, arraySort(groupUniqArrayMerge(strings)) FROM test_agg_string_compat WHERE user_id = 4 GROUP BY user_id;

SELECT '=== CSV: double-quoted elements inside a quoted cell ===';
INSERT INTO test_agg_string_compat SELECT * FROM format(CSV, 'user_id UInt64, strings AggregateFunction(groupUniqArray, String)',
$$5,"[""pear"",""plum""]"
$$);
SELECT user_id, arraySort(groupUniqArrayMerge(strings)) FROM test_agg_string_compat WHERE user_id = 5 GROUP BY user_id;

SELECT '=== TabSeparated: double-quoted elements ===';
INSERT INTO test_agg_string_compat SELECT * FROM format(TabSeparated, 'user_id UInt64, strings AggregateFunction(groupUniqArray, String)',
$$6	["peach","mango"]
$$);
SELECT user_id, arraySort(groupUniqArrayMerge(strings)) FROM test_agg_string_compat WHERE user_id = 6 GROUP BY user_id;

SELECT '=== JSONEachRow: legacy string-wrapped form and native form ===';
INSERT INTO test_agg_string_compat SELECT * FROM format(JSONEachRow, 'user_id UInt64, strings AggregateFunction(groupUniqArray, String)',
$$
{"user_id": 7, "strings": "[\"lemon\",\"olive\"]"}
{"user_id": 8, "strings": ["guava", "papaya"]}
$$);
SELECT user_id, arraySort(groupUniqArrayMerge(strings)) FROM test_agg_string_compat WHERE user_id IN (7, 8) GROUP BY user_id ORDER BY user_id;

SELECT '=== Empty array and duplicates are merged ===';
INSERT INTO test_agg_string_compat VALUES (9, '[]'), (10, '["dup","dup","other"]');
SELECT user_id, arraySort(groupUniqArrayMerge(strings)) FROM test_agg_string_compat WHERE user_id IN (9, 10) GROUP BY user_id ORDER BY user_id;

DROP TABLE test_agg_string_compat;
