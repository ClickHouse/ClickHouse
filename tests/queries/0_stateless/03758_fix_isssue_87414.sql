-- Tags: no-fasttest, no-parallel-replicas

CREATE TABLE t0 ENGINE = IcebergS3(s3_conn, filename = 'issue87414/test/t0');
SELECT count(*), sum(c0) FROM t0;
INSERT INTO TABLE t0 (c0) SETTINGS write_full_path_in_iceberg_metadata = 1, allow_experimental_insert_into_iceberg=1 VALUES (1);
SELECT count(*), sum(c0) FROM t0;
