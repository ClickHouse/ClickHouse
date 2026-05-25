INSERT INTO FUNCTION file(currentDatabase() || '_04266_selective_read.csv', 'CSV', 'c1 String, c2 UInt32')
SELECT 'not_uint', 42
SETTINGS engine_file_truncate_on_insert = 1;

SELECT c2
FROM file(currentDatabase() || '_04266_selective_read.csv', 'CSV', 'c1 UInt32, c2 UInt32')
SETTINGS input_format_csv_detect_header = 0;

SELECT *
FROM file(currentDatabase() || '_04266_selective_read.csv', 'CSV', 'c1 UInt32, c2 UInt32')
SETTINGS input_format_csv_detect_header = 0; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

INSERT INTO FUNCTION file(currentDatabase() || '_04266_selective_read.tsv', 'TSV', 'c1 String, c2 UInt32')
SELECT 'not_uint', 42
SETTINGS engine_file_truncate_on_insert = 1;

SELECT c2
FROM file(currentDatabase() || '_04266_selective_read.tsv', 'TSV', 'c1 UInt32, c2 UInt32')
SETTINGS input_format_tsv_detect_header = 0;

SELECT *
FROM file(currentDatabase() || '_04266_selective_read.tsv', 'TSV', 'c1 UInt32, c2 UInt32')
SETTINGS input_format_tsv_detect_header = 0; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

INSERT INTO FUNCTION file(currentDatabase() || '_04266_selective_read_tab_separated.tsv', 'TabSeparated', 'c1 String, c2 UInt32')
SELECT 'not_uint', 42
SETTINGS engine_file_truncate_on_insert = 1;

SELECT c2
FROM file(currentDatabase() || '_04266_selective_read_tab_separated.tsv', 'TabSeparated', 'c1 UInt32, c2 UInt32')
SETTINGS input_format_tsv_detect_header = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_04266_selective_read_tsv_raw.tsv', 'TSVRaw', 'c1 String, c2 UInt32')
SELECT 'not_uint', 42
SETTINGS engine_file_truncate_on_insert = 1;

SELECT c2
FROM file(currentDatabase() || '_04266_selective_read_tsv_raw.tsv', 'TSVRaw', 'c1 UInt32, c2 UInt32')
SETTINGS input_format_tsv_detect_header = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_04266_selective_read_tab_separated_raw.tsv', 'TabSeparatedRaw', 'c1 String, c2 UInt32')
SELECT 'not_uint', 42
SETTINGS engine_file_truncate_on_insert = 1;

SELECT c2
FROM file(currentDatabase() || '_04266_selective_read_tab_separated_raw.tsv', 'TabSeparatedRaw', 'c1 UInt32, c2 UInt32')
SETTINGS input_format_tsv_detect_header = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_04266_selective_read_raw.tsv', 'Raw', 'c1 String, c2 UInt32')
SELECT 'not_uint', 42
SETTINGS engine_file_truncate_on_insert = 1;

SELECT c2
FROM file(currentDatabase() || '_04266_selective_read_raw.tsv', 'Raw', 'c1 UInt32, c2 UInt32')
SETTINGS input_format_tsv_detect_header = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_04266_selective_read_custom.txt', 'CustomSeparated', 'c1 String, c2 UInt32')
SELECT 'not_uint', 42
SETTINGS
    engine_file_truncate_on_insert = 1,
    format_custom_escaping_rule = 'CSV',
    format_custom_field_delimiter = '|',
    format_custom_row_after_delimiter = '\n';

SELECT c2
FROM file(currentDatabase() || '_04266_selective_read_custom.txt', 'CustomSeparated', 'c1 UInt32, c2 UInt32')
SETTINGS
    format_custom_escaping_rule = 'CSV',
    format_custom_field_delimiter = '|',
    format_custom_row_after_delimiter = '\n',
    input_format_custom_detect_header = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_04266_selective_read_custom_ignore_spaces.txt', 'CustomSeparated', 'c1 String, c2 UInt32')
SELECT 'not_uint', 42
SETTINGS
    engine_file_truncate_on_insert = 1,
    format_custom_escaping_rule = 'CSV',
    format_custom_field_delimiter = '|',
    format_custom_row_after_delimiter = '\n';

SELECT c2
FROM file(currentDatabase() || '_04266_selective_read_custom_ignore_spaces.txt', 'CustomSeparatedIgnoreSpaces', 'c1 UInt32, c2 UInt32')
SETTINGS
    format_custom_escaping_rule = 'CSV',
    format_custom_field_delimiter = '|',
    format_custom_row_after_delimiter = '\n',
    input_format_custom_detect_header = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_04266_selective_read_custom_raw.txt', 'CustomSeparated', 'c1 UInt32, c2 UInt32')
SELECT 123, 42
SETTINGS
    engine_file_truncate_on_insert = 1,
    format_custom_escaping_rule = 'Raw',
    format_custom_field_delimiter = '|',
    format_custom_row_after_delimiter = '\n';

SELECT c2
FROM file(currentDatabase() || '_04266_selective_read_custom_raw.txt', 'CustomSeparated', 'c1 UInt32, c2 UInt32')
SETTINGS
    format_custom_escaping_rule = 'Raw',
    format_custom_field_delimiter = '|',
    format_custom_row_after_delimiter = '\n',
    input_format_custom_detect_header = 0; -- { serverError UNEXPECTED_DATA_AFTER_PARSED_VALUE }
