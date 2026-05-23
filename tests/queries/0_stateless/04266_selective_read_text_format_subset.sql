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
