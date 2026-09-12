-- Backward compatibility of `aggregate_function_input_format = 'value'` for a single `Enum` argument.
-- Released versions parsed the payload with the argument type's `deserializeTextCSV`, which never looked at
-- `input_format_tsv_enum_as_number`, so both the enum name and its numeric form were accepted in every
-- format. The unified implementation parses the payload with the escaped / whole-text serialization, which
-- does look at that setting, so the setting is neutralized for this payload.
-- The reference is byte-identical on released `26.7.1`.

SET aggregate_function_input_format = 'value';

SELECT 'default settings';
SELECT anyMerge(x) FROM format(TabSeparated, 'x AggregateFunction(any, Enum8(\'a\' = 1, \'b\' = 2))', 'a\n');
SELECT anyMerge(x) FROM format(TabSeparated, 'x AggregateFunction(any, Enum8(\'a\' = 1, \'b\' = 2))', '2\n');
SELECT anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, Enum8(\'a\' = 1, \'b\' = 2))', 'a\n');
SELECT anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, Enum8(\'a\' = 1, \'b\' = 2))', '2\n');
SELECT anyMerge(x) FROM format(JSONEachRow, 'x AggregateFunction(any, Enum8(\'a\' = 1, \'b\' = 2))', '{"x":"a"}');
SELECT anyMerge(x) FROM format(JSONEachRow, 'x AggregateFunction(any, Enum8(\'a\' = 1, \'b\' = 2))', '{"x":"2"}');
SELECT anyMerge(x) FROM format(CSV, 'x AggregateFunction(any, Enum8(\'a\' = 1, \'b\' = 2))', 'a\n');
SELECT anyMerge(x) FROM format(CSV, 'x AggregateFunction(any, Enum8(\'a\' = 1, \'b\' = 2))', '2\n');

SELECT 'input_format_tsv_enum_as_number';
SET input_format_tsv_enum_as_number = 1;
SELECT anyMerge(x) FROM format(TabSeparated, 'x AggregateFunction(any, Enum8(\'a\' = 1, \'b\' = 2))', 'a\n');
SELECT anyMerge(x) FROM format(TabSeparated, 'x AggregateFunction(any, Enum8(\'a\' = 1, \'b\' = 2))', '2\n');
SELECT anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, Enum8(\'a\' = 1, \'b\' = 2))', 'a\n');
SELECT anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, Enum8(\'a\' = 1, \'b\' = 2))', '2\n');
SELECT anyMerge(x) FROM format(JSONEachRow, 'x AggregateFunction(any, Enum8(\'a\' = 1, \'b\' = 2))', '{"x":"a"}');
SELECT anyMerge(x) FROM format(JSONEachRow, 'x AggregateFunction(any, Enum8(\'a\' = 1, \'b\' = 2))', '{"x":"2"}');
SET input_format_tsv_enum_as_number = 0;

SELECT 'input_format_csv_enum_as_number';
SET input_format_csv_enum_as_number = 1;
SELECT anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, Enum8(\'a\' = 1, \'b\' = 2))', '2\n');
SELECT anyMerge(x) FROM format(CSV, 'x AggregateFunction(any, Enum8(\'a\' = 1, \'b\' = 2))', '2\n');
SET input_format_csv_enum_as_number = 0;

SELECT 'nullable';
SET input_format_tsv_enum_as_number = 1;
SELECT anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, Nullable(Enum8(\'a\' = 1, \'b\' = 2)))', 'a\n');
SELECT anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, Nullable(Enum8(\'a\' = 1, \'b\' = 2)))', '2\n');
SELECT anyMerge(x) FROM format(TSVRaw, 'x AggregateFunction(any, Nullable(Enum8(\'a\' = 1, \'b\' = 2)))', '\\N\n');
SET input_format_tsv_enum_as_number = 0;

-- `array` mode keeps parsing single-argument elements with the released CSV element parse, which is not
-- affected by `input_format_tsv_enum_as_number` either.
SELECT 'array mode';
SET aggregate_function_input_format = 'array', input_format_tsv_enum_as_number = 1;
SELECT arraySort(topKMerge(2)(x)) FROM format(TSVRaw, 'x AggregateFunction(topK(2), Enum8(\'a\' = 1, \'b\' = 2))', '["a","b"]\n');
SELECT arraySort(topKMerge(2)(x)) FROM format(TSVRaw, 'x AggregateFunction(topK(2), Enum8(\'a\' = 1, \'b\' = 2))', '["1","2"]\n');
