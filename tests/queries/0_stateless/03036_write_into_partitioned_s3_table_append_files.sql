-- Tags: no-parallel, no-fasttest, no-random-settings

DROP TABLE IF EXISTS t_s3_partition_by_append_03036;

CREATE TABLE t_s3_partition_by_append_03036 (a UInt64)
ENGINE = S3(s3_conn, filename = 'test_03036_append_{_partition_id}', format = Parquet)
PARTITION BY a;

INSERT INTO t_s3_partition_by_append_03036 SELECT number FROM numbers(10);
INSERT INTO t_s3_partition_by_append_03036 SELECT number FROM numbers(10) SETTINGS s3_create_new_file_on_insert=1;

SET max_threads = 1;
SELECT count() FROM s3(s3_conn, filename = 'test_03036_append_*', format = Parquet, structure = 'a UInt64');

DROP TABLE t_s3_partition_by_append_03036;
