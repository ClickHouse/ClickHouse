-- Tags: no-fasttest
-- Parquet is not available in the fast test.

-- `aggregate_function_input_format` applies to the formats that read columns, not rows, as well:
-- an `AggregateFunction(f, T)` column is read as a column of `T` (or `Array(T)`), and the states are built from it.

INSERT INTO FUNCTION file(currentDatabase() || '_05234.parquet', Parquet, 'k UInt8, x UInt32, y Array(UInt32)') SELECT number, number * 10, [number, number * 3] FROM numbers(3) SETTINGS engine_file_truncate_on_insert = 1;

SELECT k, avgMerge(x) FROM file(currentDatabase() || '_05234.parquet', Parquet, 'k UInt8, x AggregateFunction(avg, UInt32)') GROUP BY k ORDER BY k SETTINGS aggregate_function_input_format = 'value';
SELECT k, avgMerge(y) FROM file(currentDatabase() || '_05234.parquet', Parquet, 'k UInt8, y AggregateFunction(avg, UInt32)') GROUP BY k ORDER BY k SETTINGS aggregate_function_input_format = 'array';
SELECT count() FROM file(currentDatabase() || '_05234.parquet', Parquet, 'k UInt8, x AggregateFunction(avg, UInt32)') SETTINGS aggregate_function_input_format = 'value';
