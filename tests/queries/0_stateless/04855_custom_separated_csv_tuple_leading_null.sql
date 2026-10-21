SET format_custom_escaping_rule = 'CSV';
SET format_custom_field_delimiter = ',';
SET input_format_custom_detect_header = 0;
SET input_format_custom_allow_variable_number_of_columns = 1;
SET input_format_null_as_default = 1;

SELECT 'A leading null field is the first element';
SELECT * FROM format(CustomSeparated, 't Tuple(Nullable(Int32), Int32), b UInt8', $$2,1,7
$$);
SELECT * FROM format(CustomSeparated, 't Tuple(Nullable(Int32), Int32), b UInt8', $$\N,1,7
$$);
SELECT * FROM format(CustomSeparated, 't Tuple(Nullable(Int32), Nullable(Int32)), b UInt8', $$\N,\N,7
$$);

SELECT 'A nested tuple element is one field';
SELECT * FROM format(CustomSeparated, 't Tuple(Tuple(Int32, Int32), Int32), b UInt8', $$\N,1,7
$$);

SELECT 'A multi-character field delimiter keeps the tuple inside one field';
SELECT * FROM format(CustomSeparated, 't Tuple(Nullable(Int32), Int32), b UInt8', $$\N||7
$$) SETTINGS format_custom_field_delimiter = '||';

SELECT 'Nullable(Tuple) is still read from a single field';
SELECT * FROM format(CustomSeparated, 't Nullable(Tuple(Nullable(Int32), Int32)), b UInt8', $$\N,7
$$) SETTINGS enable_nullable_tuple_type = 1;

SELECT 'The whole-column short-circuit is restored by the opt-out setting';
SELECT * FROM format(CustomSeparated, 't Tuple(Nullable(Int32), Int32), b UInt8', $$\N,1,7
$$) SETTINGS input_format_csv_deserialize_separate_columns_into_tuple = 0;

SELECT 'Regexp reads a tuple inside one capture group';
SELECT * FROM format(Regexp, 't Tuple(Nullable(Int32), Int32), b UInt8', $$\N,1 7
$$) SETTINGS format_regexp = '^(\\S+) (\\S+)$', format_regexp_escaping_rule = 'CSV';

SELECT 'A capture holding only the null field is short by the remaining elements, as in CSV';
SELECT * FROM format(CSV, 't Tuple(Nullable(Int32), Int32)', $$\N
$$); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT * FROM format(Regexp, 't Tuple(Nullable(Int32), Int32), b UInt8', $$\N 7
$$) SETTINGS format_regexp = '^(\\S+) (\\S+)$', format_regexp_escaping_rule = 'CSV'; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
