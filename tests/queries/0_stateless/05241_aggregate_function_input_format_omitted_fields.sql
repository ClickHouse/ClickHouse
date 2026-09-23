-- A field the input row does not contain has no value to aggregate, so the `AggregateFunction` column has to
-- keep the empty state it starts with: `countMerge` returns 0 and `avgMerge` returns nan, in every
-- `aggregate_function_input_format` mode. In `value` mode the default of the argument type was aggregated
-- instead, so an absent field became a state holding one row.

SET aggregate_function_input_format = 'value';

SELECT 'omitted field';
SELECT countMerge(c), avgMerge(a) FROM format(JSONEachRow, 'k UInt8, c AggregateFunction(count), a AggregateFunction(avg, UInt32)', '{"k":1}');

SELECT 'omitted and present rows in one block';
SELECT k, countMerge(c), avgMerge(a) FROM format(JSONEachRow, 'k UInt8, c AggregateFunction(count), a AggregateFunction(avg, UInt32)', '{"k":1}\n{"k":2,"c":[],"a":7}\n{"k":3}') GROUP BY k ORDER BY k;

SELECT 'nested in a tuple, omitted then present';
SELECT countMerge(t.2) FROM format(JSONEachRow, 'k UInt8, t Tuple(UInt8, AggregateFunction(count))', '{"k":1}');
SELECT countMerge(t.2) FROM format(JSONEachRow, 'k UInt8, t Tuple(UInt8, AggregateFunction(count))', '{"k":1,"t":[1,[]]}');

SELECT 'an omitted state merges as nothing, not as a default value';
SELECT maxMerge(m), countMerge(c) FROM format(JSONEachRow, 'k UInt8, m AggregateFunction(max, Int32), c AggregateFunction(count)', '{"k":1}\n{"k":2,"m":-5,"c":[]}');

SELECT 'every function shape';
SELECT countMerge(c), argMaxMerge(g), uniqExactMerge(u), sumIfMerge(s), uniqMerge(n), anyMerge(l) FROM format(JSONEachRow, 'k UInt8, c AggregateFunction(count), g AggregateFunction(argMax, String, UInt32), u AggregateFunction(uniqExact, UInt32), s AggregateFunction(sumIf, UInt32, UInt8), n AggregateFunction(uniq, Nullable(String)), l AggregateFunction(any, LowCardinality(String))', '{"k":1}');

SELECT 'fewer columns in the row, and a column absent from the file';
SELECT countMerge(c), avgMerge(a) FROM format(CSV, 'k UInt8, c AggregateFunction(count), a AggregateFunction(avg, UInt32)', '1\n') SETTINGS input_format_csv_allow_variable_number_of_columns = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05241.native', Native, 'k UInt8') SELECT 1 SETTINGS engine_file_truncate_on_insert = 1;
SELECT countMerge(c), avgMerge(a) FROM file(currentDatabase() || '_05241.native', Native, 'k UInt8, c AggregateFunction(count), a AggregateFunction(avg, UInt32)');

SELECT 'a null and an empty field carry no value either';
SELECT avgMerge(a) FROM format(JSONEachRow, 'k UInt8, a AggregateFunction(avg, UInt32)', '{"k":1,"a":null}') SETTINGS input_format_null_as_default = 1;
SELECT avgMerge(a) FROM format(JSONEachRow, 'k UInt8, a AggregateFunction(avg, UInt32)', '{"k":1,"a":""}') SETTINGS input_format_json_empty_as_default = 1;

-- Everything below already held before the fix; it is here to keep it holding.

SELECT 'control: the other two modes';
SELECT countMerge(c), avgMerge(a) FROM format(JSONEachRow, 'k UInt8, c AggregateFunction(count), a AggregateFunction(avg, UInt32)', '{"k":1}') SETTINGS aggregate_function_input_format = 'state';
SELECT countMerge(c), avgMerge(a) FROM format(JSONEachRow, 'k UInt8, c AggregateFunction(count), a AggregateFunction(avg, UInt32)', '{"k":1}') SETTINGS aggregate_function_input_format = 'array';

SELECT 'control: an omitted array or map has no elements, a present array keeps all of them';
SELECT k, arrayMap(s -> finalizeAggregation(s), x), length(m) FROM format(JSONEachRow, 'k UInt8, x Array(AggregateFunction(sum, UInt64)), m Map(String, AggregateFunction(sum, UInt64))', '{"k":1}\n{"k":2,"x":[5,6]}') ORDER BY k;

-- Known boundary, not a regression: with `input_format_defaults_for_omitted_fields = 0` the format is asked
-- not to report which fields a row omitted, so nothing downstream can tell an omitted field from a value.
SELECT 'known boundary of input_format_defaults_for_omitted_fields = 0';
SELECT countMerge(c), avgMerge(a) FROM format(JSONEachRow, 'k UInt8, c AggregateFunction(count), a AggregateFunction(avg, UInt32)', '{"k":1}') SETTINGS input_format_defaults_for_omitted_fields = 0;

-- A second boundary: the missing-value mask holds one bit per column per row, so an element defaulted
-- inside a `Tuple` the row does contain is not reported, and the state is built from that default.
-- `state` mode returns nan for the same input.
SELECT 'known boundary of an element defaulted inside a present tuple';
SELECT avgMerge(t.x) FROM format(JSONEachRow, 't Tuple(a UInt8, x AggregateFunction(avg, UInt32))', '{"t":{"a":1}}') SETTINGS input_format_json_defaults_for_missing_elements_in_named_tuple = 1;
