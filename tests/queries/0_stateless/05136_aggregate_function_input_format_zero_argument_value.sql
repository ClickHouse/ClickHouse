-- Regression test: a zero-argument aggregate in `value` mode must consume exactly one `CSV` field.
-- Its payload is the empty tuple, and the tuple `CSV` serialization consumes no bytes at all for it
-- while `input_format_csv_deserialize_separate_columns_into_tuple` is enabled (the default), which
-- would leave the whole field to the following column.
SET aggregate_function_input_format = 'value';

SELECT 'single field', a, b, countMerge(x)
FROM format(CSV, 'a UInt8, x AggregateFunction(count), b UInt8', '1,(),9\n2,(),8')
GROUP BY a, x, b
ORDER BY a;

SELECT 'setting disabled explicitly', a, b, countMerge(x)
FROM format(CSV, 'a UInt8, x AggregateFunction(count), b UInt8', '1,(),9')
GROUP BY a, x, b
SETTINGS input_format_csv_deserialize_separate_columns_into_tuple = 0;

SELECT 'quoted field', a, b, countMerge(x)
FROM format(CSV, 'a UInt8, x AggregateFunction(count), b UInt8', '1,"()",9')
GROUP BY a, x, b;

-- An empty field stays the default state, as released.
SELECT 'empty field', a, b, countMerge(x)
FROM format(CSV, 'a UInt8, x AggregateFunction(count), b UInt8', '1,,9')
GROUP BY a, x, b;

-- A field that is not the empty tuple is rejected instead of silently spilling into the next column.
SELECT 'unterminated tuple rejected', a, b, countMerge(x)
FROM format(CSV, 'a UInt8, x AggregateFunction(count), b UInt8', '1,(,9')
GROUP BY a, x, b; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- `array` mode is unaffected: the array serialization already reads one bounded field first.
SET aggregate_function_input_format = 'array';

SELECT 'array mode', a, b, countMerge(x)
FROM format(CSV, 'a UInt8, x AggregateFunction(count), b UInt8', '1,"[(),(),()]",9')
GROUP BY a, x, b;
