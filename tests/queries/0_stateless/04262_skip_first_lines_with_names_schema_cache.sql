-- Tags: no-parallel

-- Test that skip_first_lines is included in the schema cache key for WithNames formats.
-- https://github.com/ClickHouse/ClickHouse/issues/104527

-- CSV

INSERT INTO FUNCTION file('04262_test.csv', 'CSVWithNames') SELECT 'a' AS header1, 'b' AS header2 SETTINGS engine_file_truncate_on_insert=1;
INSERT INTO FUNCTION file('04262_test.csv', 'CSV') VALUES ('1', '2');

SYSTEM DROP SCHEMA CACHE FOR FILE;

DESC file('04262_test.csv', CSVWithNames) SETTINGS input_format_csv_skip_first_lines=1;
DESC file('04262_test.csv', CSVWithNames) SETTINGS input_format_csv_skip_first_lines=0;

-- TSV

INSERT INTO FUNCTION file('04262_test.tsv', 'TSVWithNames') SELECT 'a' AS header1, 'b' AS header2 SETTINGS engine_file_truncate_on_insert=1;
INSERT INTO FUNCTION file('04262_test.tsv', 'TSV') VALUES ('1', '2');

SYSTEM DROP SCHEMA CACHE FOR FILE;

DESC file('04262_test.tsv', TSVWithNames) SETTINGS input_format_tsv_skip_first_lines=1;
DESC file('04262_test.tsv', TSVWithNames) SETTINGS input_format_tsv_skip_first_lines=0;
