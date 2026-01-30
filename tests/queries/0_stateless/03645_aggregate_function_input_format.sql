SET schema_inference_make_columns_nullable=0;

CREATE TABLE test_agg_single (user_id UInt64, avg_session_length AggregateFunction(avg, UInt32)) ENGINE = Memory;
CREATE TABLE test_agg_multi (user_id UInt64, corr_values AggregateFunction(corr, Float64, Float64)) ENGINE = Memory;
CREATE TABLE test_agg_string (  user_id UInt64, unique_strings AggregateFunction(uniq, String)) ENGINE = Memory;

SELECT '=== Test 1: State format (default) ===';
SET aggregate_function_input_format = 'state';
INSERT INTO test_agg_single SELECT 123, avgState(CAST(456 AS UInt32));
SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single WHERE user_id = 123 GROUP BY user_id;

SELECT '=== Test 2: Value format - single argument ===';
SET aggregate_function_input_format = 'value';
TRUNCATE TABLE test_agg_single;
INSERT INTO test_agg_single VALUES (124, '456'), (125, 789), (126, 321);
SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single GROUP BY user_id ORDER BY user_id;

SELECT '=== Test 3: Array format - single argument ===';
SET aggregate_function_input_format = 'array';
TRUNCATE TABLE test_agg_single;
INSERT INTO test_agg_single VALUES (127, '[100,200,300]'), (128, [400,500]), (129, [600]);
SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single GROUP BY user_id ORDER BY user_id;

SELECT '=== Test 4: Value format - multiple arguments (tuple) ===';
SET aggregate_function_input_format = 'value';
TRUNCATE TABLE test_agg_multi;
INSERT INTO test_agg_multi VALUES (200, '(1.0,2.0)'), (201, (3.0,4.0));
SELECT user_id, isNaN(corrMerge(corr_values)) as is_nan FROM test_agg_multi GROUP BY user_id ORDER BY user_id;

SELECT '=== Test 5: Array format - multiple arguments ===';
SET aggregate_function_input_format = 'array';
TRUNCATE TABLE test_agg_multi;
INSERT INTO test_agg_multi VALUES (202, [(1.0,2.0),(3.0,4.0),(5.0,6.0)]), (302, [(1.0,2.0),(3.0,4.0),(5.0,6.0)]);
SELECT user_id, corrMerge(corr_values) FROM test_agg_multi GROUP BY user_id;

SELECT '=== Test 6: String aggregate function - value format ===';
SET aggregate_function_input_format = 'value';
INSERT INTO test_agg_string VALUES (300, 'hello'), (301, 'world');
SELECT user_id, uniqMerge(unique_strings) FROM test_agg_string GROUP BY user_id ORDER BY user_id;

SELECT '=== Test 7: String aggregate function - array format ===';
SET aggregate_function_input_format = 'array';
TRUNCATE TABLE test_agg_string;
INSERT INTO test_agg_string VALUES (302, ['apple', 'banana','cherry']), (303, ['dog', 'cat' ,'dog']);
SELECT user_id, uniqMerge(unique_strings) FROM test_agg_string GROUP BY user_id ORDER BY user_id;

SELECT '=== Test 8: VALUES and TabSeparated format - value and TabSeparated ===';
SET aggregate_function_input_format = 'value';
TRUNCATE TABLE test_agg_single;
INSERT INTO test_agg_single VALUES (400, '999');
SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single WHERE user_id = 400 GROUP BY user_id;
INSERT INTO test_agg_single SELECT * FROM format(TabSeparated, 'c0 UInt64, c1 AggregateFunction(avg, UInt32)', '400\t999');
SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single WHERE user_id = 400 GROUP BY user_id;

-- === Test 9: Error handling - invalid format ===
SET aggregate_function_input_format = 'invalid'; -- { serverError BAD_ARGUMENTS }
--
SELECT '=== Test 10: Multiple inserts with different formats ===';
TRUNCATE TABLE test_agg_single;
-- State format
SET aggregate_function_input_format = 'state';
INSERT INTO test_agg_single SELECT 500, avgState(CAST(100 AS UInt32));
-- Value format
SET aggregate_function_input_format = 'value';
INSERT INTO test_agg_single VALUES (501, '200');
-- Array format
SET aggregate_function_input_format = 'array';
INSERT INTO test_agg_single VALUES (502, '[300,400,500]');
SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single GROUP BY user_id ORDER BY user_id;

SELECT '=== Test 11: Empty array handling ===';
SET aggregate_function_input_format = 'array';
INSERT INTO test_agg_single VALUES (600, '[]');
SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single WHERE user_id = 600 GROUP BY user_id;
--
SELECT '=== Test 12: CSV format - value ===';
SET aggregate_function_input_format = 'value';
INSERT INTO test_agg_single SELECT * FROM format(CSV, 'c0 UInt64, c1 AggregateFunction(avg, UInt32)',
$$700,456
$$);
SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single WHERE user_id = 700 GROUP BY user_id;
--
SELECT '=== Test 13: CSV format - array ===';
SET aggregate_function_input_format = 'array';
INSERT INTO test_agg_single SELECT * FROM format(CSV, 'c0 UInt64, c1 AggregateFunction(avg, UInt32)',
$$701,"[100,200,300]"
$$);
SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single WHERE user_id = 701 GROUP BY user_id;

SELECT '=== Test 14: TabSeparated format - value ===';
SET aggregate_function_input_format = 'value';
INSERT INTO test_agg_single SELECT * FROM format(TabSeparated, 'c0 UInt64, c1 AggregateFunction(avg, UInt32)',
$$702	789
$$);
SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single WHERE user_id = 702 GROUP BY user_id;

SELECT '=== Test 15: TabSeparated format - array ===';
SET aggregate_function_input_format = 'array';
INSERT INTO test_agg_single SELECT * FROM format(TabSeparated, 'c0 UInt64, c1 AggregateFunction(avg, UInt32)',
$$703	[400,500,600]
$$);
SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single WHERE user_id = 703 GROUP BY user_id;

SELECT '=== Test 16: JSONEachRow format - value ===';
SET aggregate_function_input_format = 'value';
INSERT INTO test_agg_single SELECT * FROM format(JSONEachRow, 'user_id UInt64, avg_session_length AggregateFunction(avg, UInt32)',
$$
{"user_id": 704, "avg_session_length": 999}
$$);
SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single WHERE user_id = 704 GROUP BY user_id;

SELECT '=== Test 17: JSONEachRow format - array ===';
SET aggregate_function_input_format = 'array';
INSERT INTO test_agg_single SELECT * FROM format(JSONEachRow, 'user_id UInt64, avg_session_length AggregateFunction(avg, UInt32)',
$$
{"user_id": 705, "avg_session_length": [700,800,900]};
$$);
SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single WHERE user_id = 705 GROUP BY user_id;

SELECT '=== Test 18: Multiple JSON records ===';
SET aggregate_function_input_format = 'value';
INSERT INTO test_agg_single SELECT * FROM format(JSONEachRow, 'user_id UInt64, avg_session_length AggregateFunction(avg, UInt32)',
$$
{"user_id": 706, "avg_session_length": 111}
{"user_id": 707, "avg_session_length": 222}
$$);
SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single WHERE user_id > 705 GROUP BY user_id ORDER BY user_id;

SELECT '=== Test 19: RowBinary format - value ===';
INSERT INTO TABLE FUNCTION file(database() || '-test.bin', RowBinary) SELECT number::UInt64, (number * 100)::UInt32 FROM numbers(5);
SET aggregate_function_input_format = 'value';
INSERT INTO test_agg_single SELECT * FROM file(database() || '-test.bin', RowBinary);
SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single WHERE user_id < 100 GROUP BY user_id ORDER BY ALL;

SELECT '=== Test 20: RowBinary format - array ===';
INSERT INTO TABLE FUNCTION file(database() || '-test-array.bin', RowBinary) SELECT number::UInt64, [number, number*2]::Array(UInt32) FROM numbers(5);
SET aggregate_function_input_format = 'array';
TRUNCATE TABLE test_agg_single;
INSERT INTO test_agg_single SELECT * FROM file(database() || '-test-array.bin', RowBinary);
SELECT user_id, avgMerge(avg_session_length) FROM test_agg_single WHERE user_id < 100 GROUP BY user_id ORDER BY ALL;

SELECT '=== Test 21: RowBinary format - multiple arguments ===';
INSERT INTO TABLE FUNCTION file(database() || '-test-multi.bin', RowBinary)
  SELECT number::UInt64, [tuple(number, number*2), tuple(number+1, number*3)]::Array(Tuple(Float64, Float64)) FROM numbers(5);
SET aggregate_function_input_format = 'array';
INSERT INTO test_agg_multi SELECT * FROM file(database() || '-test-multi.bin', RowBinary);
SELECT user_id, corrMerge(corr_values) FROM test_agg_multi WHERE user_id < 100 GROUP BY user_id ORDER BY ALL;

SELECT '=== Test 22: String aggregate function - binary array format ===';
INSERT INTO TABLE FUNCTION file(database() || '-test-string.bin', RowBinary)
    SELECT number::UInt64, [toString(number), toString(number*2)]::Array(String) FROM numbers(5);
SET aggregate_function_input_format = 'array';
INSERT INTO test_agg_string SELECT * FROM file(database() || '-test-string.bin', RowBinary);
SELECT user_id, uniqMerge(unique_strings) FROM test_agg_string WHERE user_id < 100 GROUP BY user_id ORDER BY ALL;
