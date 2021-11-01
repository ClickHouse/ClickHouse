DROP TABLE IF EXISTS test_data;
CREATE TABLE test_data (
    col1 Nullable(String),
    col2 Nullable(String),
    col3 Nullable(String)
) ENGINE = Memory;

INSERT INTO test_data VALUES ('val1', NULL, 'val3');

SELECT '# output_format_csv_null_representation should initially be \\N';
SELECT * FROM test_data FORMAT CSV;

SELECT '# Changing output_format_csv_null_representation';
SET output_format_csv_null_representation = '∅';
SELECT * FROM test_data FORMAT CSV;
SET output_format_csv_null_representation = '\\N';
