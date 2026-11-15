-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

SELECT * FROM s3(s3_conn, filename='03036_archive1.zip :: example{1,2}.csv') ORDER BY tuple(*);
SELECT schema_inference_mode, splitByChar('/', source)[-1] as file, schema FROM system.schema_inference_cache WHERE file = '03036_archive1.zip::example1.csv' ORDER BY file;

SET schema_inference_mode = 'union';
SELECT * FROM s3(s3_conn, filename='03036_json_archive.zip :: example{11,12}.jsonl') ORDER BY tuple(*);
SELECT schema_inference_mode, splitByChar('/', source)[-1] as file, schema FROM system.schema_inference_cache WHERE startsWith(file, '03036_json_archive.zip') ORDER BY file;