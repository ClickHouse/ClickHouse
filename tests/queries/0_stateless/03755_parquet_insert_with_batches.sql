-- Tags: no-fasttest
DROP DATABASE IF EXISTS test_parquet_insert_with_batches;
CREATE DATABASE test_parquet_insert_with_batches;
CREATE TABLE test_parquet_insert_with_batches.t0 (c0 JSON) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO TABLE FUNCTION url('http://127.0.0.1:8123/?query=INSERT+INTO+test_parquet_insert_with_batches.t0+(c0)+FORMAT+Parquet', 'Parquet', 'c0 JSON') SELECT '{"c0":1}' FROM numbers(10) SETTINGS output_format_parquet_batch_size = 4;
SELECT count(*) FROM test_parquet_insert_with_batches.t0;
DROP DATABASE test_parquet_insert_with_batches;
