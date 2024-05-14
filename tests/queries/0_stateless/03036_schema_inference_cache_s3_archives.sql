-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

select * from s3(s3_conn, filename='03036_archive1.zip :: example{1,2}.csv') order by tuple(*);
select schema_inference_mode, splitByChar('/', source)[-1] as file, schema from system.schema_inference_cache order by file;

set schema_inference_mode = 'union';
select * from s3(s3_conn, filename='03036_json_archive.zip :: example{11,12}.jsonl') order by tuple(*);
select schema_inference_mode, splitByChar('/', source)[-1] as file, schema from system.schema_inference_cache order by file;