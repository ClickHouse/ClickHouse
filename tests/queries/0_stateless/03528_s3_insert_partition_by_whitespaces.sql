-- Tags: no-fasttest
-- ^ for S3

INSERT INTO FUNCTION
   s3(
       s3_conn,
       filename = currentDatabase() || '/test1.parquet',
       format = Parquet
    )
    PARTITION BY rand() % 10
SELECT
    *
FROM system.numbers
LIMIT 10;

SELECT * FROM s3(s3_conn, filename = currentDatabase() || '/test1.parquet');


INSERT INTO FUNCTION
   s3(
       s3_conn,
       filename = currentDatabase() || '/test2.parquet',
       format = Parquet
    ) PARTITION BY rand() % 10 SELECT
    *
FROM system.numbers
LIMIT 10;

SELECT * FROM s3(s3_conn, filename = currentDatabase() || '/test2.parquet');
