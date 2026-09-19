-- Regression test for the released direct `CSV` value representation. The complete field was unwrapped
-- before a single Nullable argument used CSV parsing, so quoted `"\\N"` must still create a null state.
SET aggregate_function_input_format = 'value';

SELECT 'nullable uint64', finalizeAggregation(x) IS NULL
FROM format(CSV, 'x AggregateFunction(any, Nullable(UInt64))', '\"\\N\"');

SELECT 'nullable string', finalizeAggregation(x) IS NULL
FROM format(CSV, 'x AggregateFunction(any, Nullable(String))', '\"\\N\"');

SELECT 'low cardinality nullable string', finalizeAggregation(x) IS NULL
FROM format(CSV, 'x AggregateFunction(any, LowCardinality(Nullable(String)))', '\"\\N\"');
