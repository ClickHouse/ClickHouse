INSERT INTO FUNCTION file(currentDatabase() || '_04408_header_case.csv', 'CSVWithNames', 'C1 UInt32, C2 UInt32')
SELECT 1, 42
SETTINGS engine_file_truncate_on_insert = 1;

SELECT c2
FROM file(currentDatabase() || '_04408_header_case.csv', 'CSVWithNames', 'c1 UInt32, c2 UInt32')
SETTINGS input_format_column_name_matching_mode = 'ignore_case';

INSERT INTO FUNCTION file(currentDatabase() || '_04408_header_known.csv', 'CSVWithNames', 'c1 UInt32, c2 UInt32')
SELECT 1, 42
SETTINGS engine_file_truncate_on_insert = 1;

SELECT c2
FROM file(currentDatabase() || '_04408_header_known.csv', 'CSV', 'c1 UInt32, c2 UInt32')
SETTINGS input_format_skip_unknown_fields = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_04408_header_csv_with_names_and_types.csv', 'CSVWithNamesAndTypes', 'c1 UInt32, c2 UInt32')
SELECT 1, 42
SETTINGS engine_file_truncate_on_insert = 1;

SELECT c2
FROM file(currentDatabase() || '_04408_header_csv_with_names_and_types.csv', 'CSVWithNamesAndTypes', 'c1 UInt32, c2 UInt32')
SETTINGS input_format_skip_unknown_fields = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_04408_header_tsv_with_names.tsv', 'TSVWithNames', 'c1 UInt32, c2 UInt32')
SELECT 1, 42
SETTINGS engine_file_truncate_on_insert = 1;

SELECT c2
FROM file(currentDatabase() || '_04408_header_tsv_with_names.tsv', 'TSVWithNames', 'c1 UInt32, c2 UInt32')
SETTINGS input_format_skip_unknown_fields = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_04408_header_raw_with_names.tsv', 'RawWithNames', 'c1 String, c2 UInt32')
SELECT 'not_uint', 42
SETTINGS engine_file_truncate_on_insert = 1;

SELECT c2
FROM file(currentDatabase() || '_04408_header_raw_with_names.tsv', 'RawWithNames', 'c1 UInt32, c2 UInt32')
SETTINGS input_format_skip_unknown_fields = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_04408_header_custom_with_names.txt', 'CustomSeparatedWithNames', 'c1 UInt32, c2 UInt32')
SELECT 1, 42
SETTINGS
    engine_file_truncate_on_insert = 1,
    format_custom_escaping_rule = 'CSV',
    format_custom_field_delimiter = '|',
    format_custom_row_after_delimiter = '\n';

SELECT c2
FROM file(currentDatabase() || '_04408_header_custom_with_names.txt', 'CustomSeparatedWithNames', 'c1 UInt32, c2 UInt32')
SETTINGS
    input_format_skip_unknown_fields = 0,
    format_custom_escaping_rule = 'CSV',
    format_custom_field_delimiter = '|',
    format_custom_row_after_delimiter = '\n';

INSERT INTO FUNCTION file(currentDatabase() || '_04408_header_json_compact_with_names.json', 'JSONCompactEachRowWithNames', 'c1 UInt32, c2 UInt32')
SELECT 1, 42
SETTINGS engine_file_truncate_on_insert = 1;

SELECT c2
FROM file(currentDatabase() || '_04408_header_json_compact_with_names.json', 'JSONCompactEachRowWithNames', 'c1 UInt32, c2 UInt32')
SETTINGS input_format_skip_unknown_fields = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_04408_header_unknown.csv', 'CSVWithNames', 'c1 UInt32, unknown UInt32, c2 UInt32')
SELECT 1, 7, 42
SETTINGS engine_file_truncate_on_insert = 1;

SELECT c2
FROM file(currentDatabase() || '_04408_header_unknown.csv', 'CSV', 'c1 UInt32, c2 UInt32')
SETTINGS input_format_skip_unknown_fields = 0; -- { serverError INCORRECT_DATA }
