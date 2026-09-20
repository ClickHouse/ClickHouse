-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

-- Smoke test: read a Parquet file from S3.
-- (Previously tested that `input_format_parquet_use_native_reader_v3` SELECT-time override
-- worked, but that setting is now obsolete -- the v3 reader is always used.)

SET s3_truncate_on_insert = 1;

INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/test/04027_parquet_v3_setting_override.parquet', format = Parquet)
SELECT number AS id FROM numbers(1);

SELECT * FROM s3(s3_conn, url = 'http://localhost:11111/test/04027_parquet_v3_setting_override.parquet', format = Parquet) ORDER BY id LIMIT 5;
