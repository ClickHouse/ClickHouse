DROP TABLE IF EXISTS test_01073_crlf_in_output_csv_format;
CREATE TABLE test_01073_crlf_in_output_csv_format (value UInt8, word String) ENGINE = MergeTree() ORDER BY value;
INSERT INTO test_01073_crlf_in_output_csv_format VALUES (1, 'hello'), (2, 'world');
SET output_format_csv_crlf_end_of_line = 1;
SELECT * FROM test_01073_crlf_in_output_csv_format FORMAT CSV;
SET output_format_csv_crlf_end_of_line = 0;
SELECT * FROM test_01073_crlf_in_output_csv_format FORMAT CSV;
DROP TABLE IF EXISTS test_01073_crlf_in_output_csv_format;
