-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

SELECT count() FROM s3(s3_conn, filename='03322_*.parquet', format='Parquet', structure='a Int, b Int, c Int');
SELECT count() FROM s3(s3_conn, filename='03322_*.parquet', format='Parquet', structure='a Int, b Int, c Int') WHERE a = 1;
SELECT count() FROM s3(s3_conn, filename='03322_*.parquet', format='Parquet', structure='a Int, b Int, c Int');

