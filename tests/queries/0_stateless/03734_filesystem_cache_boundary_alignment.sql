-- Fixes issue: https://github.com/ClickHouse/ClickHouse/issues/85552
-- Tags: no-fasttest
-- fasttest: depends on IcebergS3, S3

DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS v0;

SET allow_experimental_insert_into_iceberg=1;

CREATE TABLE t0 (c0 Int, c1 String) ENGINE = IcebergS3(s3_conn, filename = 't0/', format = 'CSV');
CREATE TABLE t1 (c0 Int, c1 Array(String)) ENGINE = Distributed('test_shard_localhost', default, t0);

REPLACE TABLE t1 (c0 String, c1 Int) ENGINE = S3(s3_conn, filename = 't1/', format = 'CSV');

CREATE MATERIALIZED VIEW v0 TO t0 (c1 Int, c0 String) AS (SELECT 1 c0, 'a' c1 FROM t1);

INSERT INTO TABLE t1 (c0, c1) SETTINGS filesystem_cache_boundary_alignment = 18446744073709551615, filesystem_cache_name = 'fcache0' VALUES (1, 1);

DROP TABLE t0;
DROP TABLE t1;
DROP TABLE v0;
