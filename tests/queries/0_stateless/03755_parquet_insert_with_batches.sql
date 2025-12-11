-- Tags: no-fasttest
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 JSON) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO TABLE FUNCTION url('http://localhost:8123/?query=INSERT+INTO+t0+(c0)+FORMAT+Parquet', 'Parquet', 'c0 JSON') SELECT '{"c0":1}' FROM numbers(10) SETTINGS output_format_parquet_batch_size = 4;
SELECT count(*) FROM t0;
