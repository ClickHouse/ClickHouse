-- Regression test: a single `Tuple` argument in `value` mode keeps the released contract. Released read the
-- whole field as a string and parsed it with the tuple's `deserializeTextCSV`, so while
-- `input_format_csv_deserialize_separate_columns_into_tuple` is enabled (the default) the payload is the
-- flattened CSV form of the tuple in one bounded field, e.g. `"2,foo"` in `CSV` or `2,foo` in `TabSeparated`.
-- The unified parse must neither let the tuple consume the cells of the following `CSV` columns nor reject
-- the flattened form on the whole-field entrypoints.
SET aggregate_function_input_format = 'value';

SELECT 'csv bounded field', a, b, finalizeAggregation(x)
FROM format(CSV, 'a UInt8, x AggregateFunction(any, Tuple(UInt64, String)), b UInt8', '1,"2,foo",9\n2,"3,bar",8');

SELECT 'csv separate cells rejected', a, b, finalizeAggregation(x)
FROM format(CSV, 'a UInt8, x AggregateFunction(any, Tuple(UInt64, String)), b UInt8', '1,2,foo,9'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

SELECT 'tsv', finalizeAggregation(x) FROM format(TabSeparated, 'x AggregateFunction(any, Tuple(UInt64, String))', '2,foo');
SELECT 'tsv quoted element', finalizeAggregation(x) FROM format(TabSeparated, 'x AggregateFunction(any, Tuple(UInt64, String))', '2,"fo,o"');
SELECT 'tsv escaped element', finalizeAggregation(x) FROM format(TabSeparated, 'x AggregateFunction(any, Tuple(UInt64, String))', '2,"a\\tb"');
SELECT 'tsvraw', finalizeAggregation(x) FROM format(TSVRaw, 'x AggregateFunction(any, Tuple(UInt64, String))', '2,foo');
SELECT 'json string-wrapped', finalizeAggregation(x) FROM format(JSONEachRow, 'x AggregateFunction(any, Tuple(UInt64, String))', '{"x":"2,foo"}');
SELECT 'values quoted', finalizeAggregation(x) FROM format(Values, 'x AggregateFunction(any, Tuple(UInt64, String))', '(\'2,foo\')');
SELECT 'custom separated raw', finalizeAggregation(x) FROM format(CustomSeparated, 'x AggregateFunction(any, Tuple(UInt64, String))', '2,foo\n') SETTINGS format_custom_escaping_rule = 'Raw';
SELECT 'custom separated escaped', finalizeAggregation(x) FROM format(CustomSeparated, 'x AggregateFunction(any, Tuple(UInt64, String))', '2,foo\n') SETTINGS format_custom_escaping_rule = 'Escaped';

-- With the setting disabled released read the CSV-quoted native form.
SELECT 'tsv setting disabled', finalizeAggregation(x) FROM format(TabSeparated, 'x AggregateFunction(any, Tuple(UInt64, String))', '"(2,\'foo\')"') SETTINGS input_format_csv_deserialize_separate_columns_into_tuple = 0;

-- The native form is accepted as well when the field opens with a parenthesis.
SELECT 'csv native', a, b, finalizeAggregation(x)
FROM format(CSV, 'a UInt8, x AggregateFunction(any, Tuple(UInt64, String)), b UInt8', '1,"(2,\'foo\')",9');
SELECT 'tsvraw native', finalizeAggregation(x) FROM format(TSVRaw, 'x AggregateFunction(any, Tuple(UInt64, String))', '(2,\'foo\')');
SELECT 'tsvraw native string first', finalizeAggregation(x) FROM format(TSVRaw, 'x AggregateFunction(any, Tuple(String, UInt64))', '(\'foo\',2)');
SELECT 'json string-wrapped native', finalizeAggregation(x) FROM format(JSONEachRow, 'x AggregateFunction(any, Tuple(UInt64, String))', '{"x":"(2,\'foo\')"}');
SELECT 'json native', finalizeAggregation(x) FROM format(JSONEachRow, 'x AggregateFunction(any, Tuple(UInt64, String))', '{"x":[2,"foo"]}');
SELECT 'values native', finalizeAggregation(x) FROM format(Values, 'x AggregateFunction(any, Tuple(UInt64, String))', '((2,\'foo\'))');

-- A `Nullable(Tuple)` argument and a multi-argument aggregate are unaffected.
SELECT 'nullable tuple native', finalizeAggregation(x) FROM format(TSVRaw, 'x AggregateFunction(any, Nullable(Tuple(UInt64, String)))', '(2,\'foo\')');
SELECT 'nullable tuple null', finalizeAggregation(x) IS NULL FROM format(TSVRaw, 'x AggregateFunction(any, Nullable(Tuple(UInt64, String)))', '\\N');
SELECT 'multiple arguments', finalizeAggregation(x) FROM format(TSVRaw, 'x AggregateFunction(argMax, String, UInt64)', '(\'foo\',10)');
