-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- This table was created by spark, but with changed column names in parquet
SELECT * FROM icebergS3(s3_conn, filename='field_ids_struct_test', SETTINGS iceberg_metadata_table_uuid = '149ecc15-7afc-4311-86b3-3a4c8d4ec08e') ORDER BY ALL;
