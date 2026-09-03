-- Regression test: a multi-argument aggregate in `value` mode must consume exactly one `CSV` field.
-- The temporary tuple of the argument types is parsed with the tuple `CSV` serialization, which would
-- otherwise read one outer cell per element while `input_format_csv_deserialize_separate_columns_into_tuple`
-- is enabled (the default) and swallow the cells of the following columns.
SET aggregate_function_input_format = 'value';

SELECT 'single field', a, b, finalizeAggregation(x)
FROM format(CSV, 'a UInt8, x AggregateFunction(argMax, String, UInt64), b UInt8', '1,"(\'foo\',10)",9\n2,"(\'bar\',20)",8');

SELECT 'separate cells rejected', a, b, finalizeAggregation(x)
FROM format(CSV, 'a UInt8, x AggregateFunction(argMax, String, UInt64), b UInt8', '1,\'foo\',10,9'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

SELECT 'setting disabled explicitly', a, b, finalizeAggregation(x)
FROM format(CSV, 'a UInt8, x AggregateFunction(argMax, String, UInt64), b UInt8', '1,"(\'foo\',10)",9')
SETTINGS input_format_csv_deserialize_separate_columns_into_tuple = 0;

-- A single-argument aggregate is unaffected: its payload is not a tuple.
SELECT 'single argument', a, b, finalizeAggregation(x)
FROM format(CSV, 'a UInt8, x AggregateFunction(any, UInt64), b UInt8', '1,42,9');
