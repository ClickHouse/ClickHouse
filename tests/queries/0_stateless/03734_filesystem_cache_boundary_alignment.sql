-- Fixes issue: https://github.com/ClickHouse/ClickHouse/issues/85552
-- Tags: no-parallel, no-fasttest, no-random-settings

SET allow_experimental_insert_into_iceberg=1;

CREATE TABLE t0 (c0 Int, c1 String) ENGINE = IcebergS3(s3_conn, filename = 'test_03734_t0/', format = 'CSV');
CREATE TABLE t1 (c0 Int, c1 Array(String)) ENGINE = Distributed('test_shard_localhost', default, t0);

REPLACE TABLE t1 (c0 String, c1 Int) ENGINE = S3(s3_conn, filename = 'test_03734_t1/', format = 'CSV');

CREATE MATERIALIZED VIEW v0 TO t0 (c1 Int, c0 String) AS (SELECT 1 c0, 'test_03734' c1 FROM t1);

INSERT INTO TABLE t1 (c0, c1) SETTINGS filesystem_cache_boundary_alignment = 18446744073709551615, filesystem_cache_name = 'test_03734_fcache0' VALUES (1, 1);
