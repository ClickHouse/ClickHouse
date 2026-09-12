-- Regression test: single-argument `array` mode keeps recognizing a custom `format_csv_null_representation`.
-- Released parsed every element with the argument type's `deserializeTextCSV`, so `[Nothing,1]` built a null
-- element for a `Nullable` argument when the representation was `Nothing`. Only the exact `NULL` keyword of the
-- native form takes the quoted parse; every other bareword starting with `N`/`n` keeps the CSV parse.
SET aggregate_function_input_format = 'array';
SET format_csv_null_representation = 'Nothing';

SELECT 'tsvraw', finalizeAggregation(x) FROM format(TSVRaw, 'x AggregateFunction(groupArray, Nullable(UInt64))', '[Nothing,1]');
SELECT 'tsvraw count', finalizeAggregation(x) FROM format(TSVRaw, 'x AggregateFunction(count, Nullable(UInt64))', '[Nothing,1]');
SELECT 'tsvraw last element', finalizeAggregation(x) FROM format(TSVRaw, 'x AggregateFunction(count, Nullable(UInt64))', '[1,Nothing]');
SELECT 'tsvraw low cardinality', finalizeAggregation(x) FROM format(TSVRaw, 'x AggregateFunction(count, LowCardinality(Nullable(UInt64)))', '[Nothing,1]');
SELECT 'tsvraw date', finalizeAggregation(x) FROM format(TSVRaw, 'x AggregateFunction(count, Nullable(Date))', '[Nothing,"2020-01-01"]');
SELECT 'tsv', finalizeAggregation(x) FROM format(TabSeparated, 'x AggregateFunction(count, Nullable(UInt64))', '[Nothing,1]');
SELECT 'csv', finalizeAggregation(x) FROM format(CSV, 'x AggregateFunction(count, Nullable(UInt64))', '"[Nothing,1]"');
SELECT 'json string-wrapped', finalizeAggregation(x) FROM format(JSONEachRow, 'x AggregateFunction(count, Nullable(UInt64))', '{"x":"[Nothing,1]"}');
SELECT 'values', finalizeAggregation(x) FROM format(Values, 'x AggregateFunction(count, Nullable(UInt64))', '([Nothing,1])');

-- `NaN` for a float keeps its CSV parse, and the native `NULL` keyword still builds a null.
SELECT 'tsvraw nan', finalizeAggregation(x) FROM format(TSVRaw, 'x AggregateFunction(groupArray, Nullable(Float64))', '[NaN,1]');
SELECT 'tsvraw NULL keyword', finalizeAggregation(x) FROM format(TSVRaw, 'x AggregateFunction(count, Nullable(UInt64))', '[NULL, null ,1]');
SELECT 'tsvraw bareword rejected', finalizeAggregation(x) FROM format(TSVRaw, 'x AggregateFunction(count, Nullable(UInt64))', '[NULLX,1]'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- For a string-like argument the representation is a null as well, while `NULL` stays a string, as released.
SELECT 'tsvraw string', finalizeAggregation(x) FROM format(TSVRaw, 'x AggregateFunction(groupArray, Nullable(String))', '[Nothing,NULL,"a"]');

-- The default representation still works too.
SET format_csv_null_representation = '\\N';
SELECT 'tsvraw default', finalizeAggregation(x) FROM format(TSVRaw, 'x AggregateFunction(count, Nullable(UInt64))', '[\\N,1]');
