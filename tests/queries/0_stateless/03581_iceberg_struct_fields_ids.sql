-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- This table was created by spark, but with changed column names in parquet
-- The purpose of this test is to verify that a column with a structured type with different names in Parquet and Iceberg metadata will be read correctly.
SELECT * FROM icebergS3(s3_conn, filename='field_ids_struct_test', SETTINGS iceberg_metadata_table_uuid = '149ecc15-7afc-4311-86b3-3a4c8d4ec08e') ORDER BY ALL;
SELECT * FROM icebergS3(s3_conn, filename='field_ids_complex_test', SETTINGS iceberg_metadata_table_uuid = 'd4b695ca-ceeb-4537-8a2a-eee90dc6e313') ORDER BY ALL;
