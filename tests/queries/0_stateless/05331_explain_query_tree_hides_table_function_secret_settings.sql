-- Tags: no-fasttest
-- no-fasttest: Masking requires the features to be built
-- Secret settings passed inside a table function call are hidden in the query tree dump.

SELECT trim(explain) FROM (EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM iceberg('http://localhost:11111/test/x', SETTINGS auth_header = 'plain_secret')) WHERE explain LIKE '%SETTINGS%';
SELECT trim(explain) FROM (EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM icebergS3('http://localhost:11111/test/x', SETTINGS iceberg_metadata_file_path = 'metadata.json', aws_secret_access_key = 'plain_secret')) WHERE explain LIKE '%SETTINGS%';
