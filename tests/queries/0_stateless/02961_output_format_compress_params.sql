INSERT INTO FUNCTION file('test_02961.csv', 'CSV', 'x UInt64', 'zstd') SELECT number FROM numbers(1000000) SETTINGS output_format_compression_level = 10, output_format_compression_zstd_window_log = 30, engine_file_truncate_on_insert = 1;
-- Simple check that output_format_compression_zstd_window_log = 30 works
SELECT count() FROM file('test_02961.csv', 'CSV', 'x UInt64', 'zstd');  -- { serverError ZSTD_DECODER_FAILED }
SELECT count() FROM file('test_02961.csv', 'CSV', 'x UInt64', 'zstd') SETTINGS zstd_window_log_max = 30;
