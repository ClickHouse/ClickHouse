-- With `aggregate_function_input_format` = 'value' or 'array', every input format reads an `AggregateFunction(f, T)` column
-- as `T` (a `Tuple` of the argument types if there are several of them), or as an `Array` of them,
-- in the representation the format uses for that type. The states are built from the values afterwards.

SET aggregate_function_input_format = 'value';

SELECT 'value, one argument';
SELECT k, avgMerge(x) FROM format(TSV, 'k UInt8, x AggregateFunction(avg, UInt32)', '1\t10\n2\t20\n') GROUP BY k ORDER BY k;
SELECT k, avgMerge(x) FROM format(CSV, 'k UInt8, x AggregateFunction(avg, UInt32)', '1,10\n2,20\n') GROUP BY k ORDER BY k;
SELECT k, avgMerge(x) FROM format(JSONEachRow, 'k UInt8, x AggregateFunction(avg, UInt32)', '{"k":1,"x":10}\n{"k":2,"x":20}\n') GROUP BY k ORDER BY k;
SELECT k, avgMerge(x) FROM format(Values, 'k UInt8, x AggregateFunction(avg, UInt32)', '(1, 10), (2, ''20'')') GROUP BY k ORDER BY k;
SELECT k, avgMerge(x) FROM format(RowBinary, 'k UInt8, x AggregateFunction(avg, UInt32)', unhex('010A0000000214000000')) GROUP BY k ORDER BY k;

SELECT 'value, several arguments are a tuple';
SELECT k, argMaxMerge(x) FROM format(TSV, 'k UInt8, x AggregateFunction(argMax, String, UInt32)', '1\t(''a'',1)\n1\t(''b'',2)\n') GROUP BY k;
SELECT k, argMaxMerge(x) FROM format(JSONEachRow, 'k UInt8, x AggregateFunction(argMax, String, UInt32)', '{"k":1,"x":["a",1]}\n{"k":1,"x":["b",2]}\n') GROUP BY k;
SELECT k, argMaxMerge(x) FROM format(RowBinary, 'k UInt8, x AggregateFunction(argMax, String, UInt32)', unhex('0101610100000001016202000000')) GROUP BY k;

SELECT 'value, no arguments are an empty tuple';
SELECT countMerge(x) FROM format(TSV, 'x AggregateFunction(count)', '()\n()\n');
SELECT countMerge(x) FROM format(RowBinary, 'x AggregateFunction(count)', unhex(''));

SELECT 'value, Nullable and LowCardinality arguments';
SELECT k, uniqMerge(x), anyMerge(y) FROM format(TSV, 'k UInt8, x AggregateFunction(uniq, Nullable(String)), y AggregateFunction(any, LowCardinality(String))', '1\t\\N\thello\n1\tabc\tworld\n') GROUP BY k;
SELECT k, uniqMerge(x), anyMerge(y) FROM format(JSONEachRow, 'k UInt8, x AggregateFunction(uniq, Nullable(String)), y AggregateFunction(any, LowCardinality(String))', '{"k":1,"x":null,"y":"hello"}\n{"k":1,"x":"abc","y":"world"}\n') GROUP BY k;

SELECT 'value, nested in Array, Tuple and Map';
SELECT arrayMap(s -> finalizeAggregation(s), a), t.1, finalizeAggregation(t.2), mapApply((mk, mv) -> (mk, finalizeAggregation(mv)), m)
FROM format(JSONEachRow, 'a Array(AggregateFunction(sum, UInt64)), t Tuple(UInt8, AggregateFunction(max, String)), m Map(String, AggregateFunction(sum, UInt64))', '{"a":[1,2,3],"t":[7,"x"],"m":{"p":10,"q":20}}\n');
SELECT arrayMap(s -> finalizeAggregation(s), a), t.1, finalizeAggregation(t.2), mapApply((mk, mv) -> (mk, finalizeAggregation(mv)), m)
FROM format(TSV, 'a Array(AggregateFunction(sum, UInt64)), t Tuple(UInt8, AggregateFunction(max, String)), m Map(String, AggregateFunction(sum, UInt64))', '[1,2,3]\t(7,''x'')\t{''p'':10,''q'':20}\n');

SELECT 'value, Native';
INSERT INTO FUNCTION file(currentDatabase() || '_05233.native', Native, 'k UInt8, x UInt32') SELECT number, number * 10 FROM numbers(3) SETTINGS engine_file_truncate_on_insert = 1;
SELECT k, avgMerge(x) FROM file(currentDatabase() || '_05233.native', Native, 'k UInt8, x AggregateFunction(avg, UInt32)') GROUP BY k ORDER BY k;
SELECT count() FROM file(currentDatabase() || '_05233.native', Native, 'k UInt8, x AggregateFunction(avg, UInt32)');

SELECT 'value, INSERT with omitted fields and asynchronous INSERT';
DROP TABLE IF EXISTS t_05233;
CREATE TABLE t_05233 (k UInt64, x AggregateFunction(sum, UInt64) DEFAULT arrayReduce('sumState', [k * 10])) ENGINE = Memory;
INSERT INTO t_05233 SETTINGS async_insert = 0 FORMAT JSONEachRow {"k":1} {"k":2,"x":5};

INSERT INTO t_05233 SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES (3, 7), (3, 8);
INSERT INTO t_05233 SETTINGS async_insert = 1, wait_for_async_insert = 1 FORMAT JSONEachRow {"k":4,"x":9};

SELECT k, sumMerge(x) FROM t_05233 GROUP BY k ORDER BY k;
DROP TABLE t_05233;

SET aggregate_function_input_format = 'array';

SELECT 'array, one argument';
SELECT k, avgMerge(x) FROM format(TSV, 'k UInt8, x AggregateFunction(avg, UInt32)', '1\t[10,20]\n2\t[]\n') GROUP BY k ORDER BY k;
SELECT k, avgMerge(x) FROM format(CSV, 'k UInt8, x AggregateFunction(avg, UInt32)', '1,"[10,20]"\n2,"[]"\n') GROUP BY k ORDER BY k;
SELECT k, avgMerge(x) FROM format(JSONEachRow, 'k UInt8, x AggregateFunction(avg, UInt32)', '{"k":1,"x":[10,20]}\n{"k":2,"x":[]}\n') GROUP BY k ORDER BY k;
SELECT k, avgMerge(x) FROM format(Values, 'k UInt8, x AggregateFunction(avg, UInt32)', '(1, [10, 20]), (2, ''[]'')') GROUP BY k ORDER BY k;
SELECT k, avgMerge(x) FROM format(RowBinary, 'k UInt8, x AggregateFunction(avg, UInt32)', unhex('01020A000000140000000200')) GROUP BY k ORDER BY k;

SELECT 'array, several arguments';
SELECT k, argMaxMerge(x) FROM format(TSV, 'k UInt8, x AggregateFunction(argMax, String, UInt32)', '1\t[(''a'',1),(''b'',2)]\n') GROUP BY k;
SELECT k, argMaxMerge(x) FROM format(JSONEachRow, 'k UInt8, x AggregateFunction(argMax, String, UInt32)', '{"k":1,"x":[["a",1],["b",2]]}\n') GROUP BY k;

SELECT 'array, no arguments';
SELECT countMerge(x) FROM format(TSV, 'x AggregateFunction(count)', '[(),(),()]\n');
SELECT countMerge(x) FROM format(RowBinary, 'x AggregateFunction(count)', unhex('03'));

SELECT 'array, nested in Array';
SELECT arrayMap(s -> finalizeAggregation(s), a) FROM format(JSONEachRow, 'a Array(AggregateFunction(sum, UInt64))', '{"a":[[1,2],[],[3]]}\n');

SELECT 'array, Native';
INSERT INTO FUNCTION file(currentDatabase() || '_05233.native', Native, 'k UInt8, x Array(UInt32)') SELECT number, [number, number * 3] FROM numbers(3) SETTINGS engine_file_truncate_on_insert = 1;
SELECT k, avgMerge(x) FROM file(currentDatabase() || '_05233.native', Native, 'k UInt8, x AggregateFunction(avg, UInt32)') GROUP BY k ORDER BY k;

SET aggregate_function_input_format = 'state';

SELECT 'state';
SELECT finalizeAggregation(x) FROM format(RowBinary, 'x AggregateFunction(sum, UInt64)', unhex('2A00000000000000'));
SELECT finalizeAggregation(x) FROM format(TSV, 'x AggregateFunction(sum, UInt64)', '*\\0\\0\\0\\0\\0\\0\\0\n');
