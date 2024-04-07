-- Tags: no-parallel, no-fasttest, no-random-settings

DROP TABLE IF EXISTS t_s3_partition_by_truncate_03036;

CREATE TABLE t_s3_partition_by_truncate_03036 (a UInt64)
ENGINE = S3(s3_conn, filename = 'test_03036_truncate_{_partition_id}', format = Parquet)
PARTITION BY a;

INSERT INTO t_s3_partition_by_truncate_03036 SELECT number FROM numbers(10);
INSERT INTO t_s3_partition_by_truncate_03036 SELECT number FROM numbers(10) SETTINGS s3_truncate_on_insert=true;

SET max_threads = 1;
SELECT count() FROM s3(s3_conn, filename = 'test_03036_truncate_*', format = Parquet, structure = 'a UInt64');

DROP TABLE t_s3_partition_by_truncate_03036;
