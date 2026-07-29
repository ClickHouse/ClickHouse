-- The raw text formats (`TSVRaw`, the `Raw*` family and `CustomSeparated` with
-- `format_custom_escaping_rule = 'Raw'`) read the whole field and parse it as whole text. Released parsed
-- that field with the argument type's `deserializeTextCSV`, so the CSV null representation `\N` built a null
-- state for a single `Nullable` argument. Check that this form still works.

SET aggregate_function_input_format = 'value';

SELECT 'tsvraw number', anyMerge(x) IS NULL FROM format(TSVRaw, 'x AggregateFunction(any, Nullable(UInt64))', '\\N');
SELECT 'tsvraw string', anyMerge(x) IS NULL FROM format(TSVRaw, 'x AggregateFunction(any, Nullable(String))', '\\N');
SELECT 'tsvraw low cardinality', anyMerge(x) IS NULL FROM format(TSVRaw, 'x AggregateFunction(any, LowCardinality(Nullable(String)))', '\\N');
SELECT 'tsvraw float', anyMerge(x) IS NULL FROM format(TSVRaw, 'x AggregateFunction(any, Nullable(Float64))', '\\N');

-- Ordinary values are unaffected. `NULL` stays the string for a string-like nested type, as released.
SELECT 'tsvraw number value', anyMerge(x) IS NULL, anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, Nullable(UInt64))', '42');
SELECT 'tsvraw string value', anyMerge(x) IS NULL, anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, Nullable(String))', 'abc');
SELECT 'tsvraw string NULL word', anyMerge(x) IS NULL, anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, Nullable(String))', 'NULL');

-- A non-`Nullable` argument is not affected.
SELECT 'tsvraw plain', anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, String)', 'abc');

-- `CustomSeparated` with the raw escaping rule takes the same path.
SET format_custom_escaping_rule = 'Raw';
SELECT 'custom separated number', anyMerge(x) IS NULL FROM format(CustomSeparated, 'x AggregateFunction(any, Nullable(UInt64))', '\\N\n');
SELECT 'custom separated string', anyMerge(x) IS NULL FROM format(CustomSeparated, 'x AggregateFunction(any, Nullable(String))', '\\N\n');
SELECT 'custom separated value', anyMerge(x) IS NULL, anyMerge(x) FROM format(CustomSeparated, 'x AggregateFunction(any, Nullable(UInt64))', '42\n');
