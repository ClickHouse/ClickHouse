-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3/MinIO

-- A stats-less Iceberg manifest (no per-column statistics) must return real
-- values, not NULLs: empty stats were misread as "all columns absent" (#1545).
SELECT * FROM icebergS3(s3_conn, filename = 'iceberg_no_column_stats') ORDER BY ALL
SETTINGS allow_experimental_iceberg_read_optimization = 1;
