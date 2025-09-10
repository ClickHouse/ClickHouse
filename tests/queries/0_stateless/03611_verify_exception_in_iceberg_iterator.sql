
-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

SELECT * FROM icebergS3('http://localhost:11111/test/corrupted_avro_files_test/', 'clickhouse', 'clickhouse') SETTINGS use_iceberg_metadata_files_cache = False; -- {ILLEGAL_DATA}