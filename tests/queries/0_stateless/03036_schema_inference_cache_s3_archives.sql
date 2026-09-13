-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- The schema inference cache is keyed on `session_timezone`, and this test lists every entry of
-- a source instead of counting one. The cache outlives a single run, so repeated runs against
-- one server - the flaky check, a retry - each add an entry of their own for the same file once
-- the randomized `session_timezone` differs. Pin it to keep the listing deterministic.
SET session_timezone = 'UTC';

SELECT * FROM s3(s3_conn, filename='03036_archive1.zip :: example{1,2}.csv') ORDER BY tuple(*);
SELECT schema_inference_mode, splitByChar('/', source)[-1] as file, schema FROM system.schema_inference_cache WHERE file = '03036_archive1.zip::example1.csv' ORDER BY file;

SET schema_inference_mode = 'union';
SELECT * FROM s3(s3_conn, filename='03036_json_archive.zip :: example{11,12}.jsonl') ORDER BY tuple(*);
SELECT schema_inference_mode, splitByChar('/', source)[-1] as file, schema FROM system.schema_inference_cache WHERE startsWith(file, '03036_json_archive.zip') ORDER BY file;