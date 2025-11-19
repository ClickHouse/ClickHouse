
-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- Verify that exception is thrown when Iceberg table contains corrupted Avro manifest files.
-- This test verifies not so much thrown error during avro files parsing but rather
-- that we correctly process exceptions thrown from iceberg iterator in background execution.

SELECT * FROM icebergS3('http://localhost:11111/test/corrupted_avro_files_test/', 'clickhouse', 'clickhouse') SETTINGS use_iceberg_metadata_files_cache = False; -- { serverError INCORRECT_DATA}