-- Tags: no-fasttest
-- Parquet is not available in the fast test.

-- A column the Parquet file does not contain has no value to aggregate, so the `AggregateFunction` column
-- has to keep its empty state instead of one built from the default of the argument type.

INSERT INTO FUNCTION file(currentDatabase() || '_05242.parquet', Parquet, 'k UInt8, x UInt32') SELECT 1, 10 SETTINGS engine_file_truncate_on_insert = 1;

SELECT 'absent column';
SELECT countMerge(c), avgMerge(a) FROM file(currentDatabase() || '_05242.parquet', Parquet, 'k UInt8, c AggregateFunction(count), a AggregateFunction(avg, UInt32)') SETTINGS aggregate_function_input_format = 'value', input_format_parquet_allow_missing_columns = 1;

SELECT 'control: a present column still builds the state';
SELECT avgMerge(x) FROM file(currentDatabase() || '_05242.parquet', Parquet, 'k UInt8, x AggregateFunction(avg, UInt32)') SETTINGS aggregate_function_input_format = 'value';
