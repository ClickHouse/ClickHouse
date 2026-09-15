-- Regression test for the released direct `CSV` value representation of quoted non-null payloads.
-- The complete field was unwrapped before a single Nullable argument used CSV parsing, so an
-- inner-quoted payload such as `"""42"""` (unwrapped to `"42"`) must still go through the CSV parse,
-- which strips the inner quotes, and must not be rejected or kept literally by the whole-text parse.
SET aggregate_function_input_format = 'value';

SELECT 'nullable uint64', finalizeAggregation(x)
FROM format(CSV, 'x AggregateFunction(any, Nullable(UInt64))', '"""42"""');

SELECT 'nullable uint64 single quotes', finalizeAggregation(x)
FROM format(CSV, 'x AggregateFunction(any, Nullable(UInt64))', '"''42''"');

SELECT 'nullable string', finalizeAggregation(x)
FROM format(CSV, 'x AggregateFunction(any, Nullable(String))', '"""abc"""');

SELECT 'low cardinality nullable string', finalizeAggregation(x)
FROM format(CSV, 'x AggregateFunction(any, LowCardinality(Nullable(String)))', '"""abc"""');

SELECT 'nullable enum', finalizeAggregation(x)
FROM format(CSV, 'x AggregateFunction(any, Nullable(Enum8(''a'' = 1, ''b'' = 2)))', '"""b"""');
