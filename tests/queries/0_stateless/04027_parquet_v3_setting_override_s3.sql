-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

-- Test that `input_format_parquet_use_native_reader_v3` setting at SELECT time
-- overrides the setting specified at CREATE TABLE time.

SET s3_truncate_on_insert = 1;

INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/test/04027_parquet_v3_setting_override.parquet', format = Parquet)
SELECT number AS id FROM numbers(1);

DROP TABLE IF EXISTS t_04027_parquet_v3_override;

CREATE TABLE t_04027_parquet_v3_override (id UInt64)
ENGINE = S3(s3_conn, url = 'http://localhost:11111/test/04027_parquet_v3_setting_override.parquet', format = Parquet)
SETTINGS input_format_parquet_use_native_reader_v3 = false;

SELECT * FROM t_04027_parquet_v3_override ORDER BY id LIMIT 5 SETTINGS input_format_parquet_use_native_reader_v3 = true;

DROP TABLE t_04027_parquet_v3_override;
