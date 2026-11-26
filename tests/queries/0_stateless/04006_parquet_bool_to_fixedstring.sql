-- Tags: no-fasttest
-- Parquet Bool to FixedString conversion should use string representation ("true"/"false"),
-- not raw bytes. https://github.com/ClickHouse/ClickHouse/issues/78909

SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '04006_false.parquet', 'Parquet', 'c0 Bool') VALUES (false);
INSERT INTO FUNCTION file(currentDatabase() || '04006_true.parquet', 'Parquet', 'c0 Bool') VALUES (true);

-- FixedString(5) should accept "false" (5 chars)
SELECT hex(c0) FROM file(currentDatabase() || '04006_false.parquet', 'Parquet', 'c0 FixedString(5)') SETTINGS input_format_parquet_use_native_reader_v3 = 1;

-- FixedString(4) should accept "true" (4 chars)
SELECT hex(c0) FROM file(currentDatabase() || '04006_true.parquet', 'Parquet', 'c0 FixedString(4)') SETTINGS input_format_parquet_use_native_reader_v3 = 1;

-- FixedString(1) should reject "false" (too large)
SELECT c0 FROM file(currentDatabase() || '04006_false.parquet', 'Parquet', 'c0 FixedString(1)') SETTINGS input_format_parquet_use_native_reader_v3 = 1; -- {serverError TOO_LARGE_STRING_SIZE}

-- FixedString(3) should reject "true" (too large)
SELECT c0 FROM file(currentDatabase() || '04006_true.parquet', 'Parquet', 'c0 FixedString(3)') SETTINGS input_format_parquet_use_native_reader_v3 = 1; -- {serverError TOO_LARGE_STRING_SIZE}
