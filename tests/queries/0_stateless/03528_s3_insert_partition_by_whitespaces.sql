-- Tags: no-fasttest
-- ^ for S3

INSERT INTO FUNCTION
   s3(
       s3_conn,
       filename = currentDatabase() || '/{_partition_id}/test.parquet',
       format = Parquet
    )
    PARTITION BY 1
SELECT
    *
FROM system.numbers
LIMIT 10;

SELECT * FROM s3(s3_conn, filename = currentDatabase() || '/1/test.parquet');

INSERT INTO FUNCTION
   s3(
       s3_conn,
       filename = currentDatabase() || '/{_partition_id}/test.parquet',
       format = Parquet
    ) PARTITION BY 2 SELECT
    *
FROM system.numbers
LIMIT 10;

SELECT * FROM s3(s3_conn, filename = currentDatabase() || '/2/test.parquet');
