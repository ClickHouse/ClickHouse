-- Tags: no-fasttest

-- default settings.
DROP TABLE IF EXISTS test_parquet;
CREATE TABLE test_parquet (col1 int, col2 String) ENGINE=File(Parquet);
INSERT INTO test_parquet SELECT number, toString(number) FROM numbers(100);
SELECT col1, col2 FROM test_parquet;


-- Parquet will have indexes in columns. We are not checking that indexes exist here, there is an integration test test_parquet_page_index for that. We just check that a setting doesn't break the SELECT
DROP TABLE IF EXISTS test_parquet;
CREATE TABLE test_parquet (col1 int, col2 String) ENGINE=File(Parquet)  SETTINGS output_format_parquet_use_custom_encoder=false, output_format_parquet_write_page_index=true;
INSERT INTO test_parquet SELECT number, toString(number) FROM numbers(100);
SELECT col1, col2 FROM test_parquet;


-- Parquet will not have indexes in columns.
DROP TABLE IF EXISTS test_parquet;
CREATE TABLE test_parquet (col1 int, col2 String) ENGINE=File(Parquet)  SETTINGS output_format_parquet_use_custom_encoder=false, output_format_parquet_write_page_index=false;
INSERT INTO test_parquet SELECT number, toString(number) FROM numbers(100);
SELECT col1, col2 FROM test_parquet;
