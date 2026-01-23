-- Tags: no-fasttest, no-parallel-replicas

DROP TABLE IF EXISTS t0, t1, v0;
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE t1 (c0 Int) ENGINE = IcebergS3(s3_conn, filename = '03800_mv_from_iceberg/t1');
CREATE MATERIALIZED VIEW v0 TO t0 AS (SELECT c0 FROM t1);
INSERT INTO TABLE t1 (c0) SETTINGS allow_experimental_insert_into_iceberg = 1 VALUES (1);
