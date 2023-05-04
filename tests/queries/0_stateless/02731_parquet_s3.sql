-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- Reading from s3 a parquet file of size between ~1 MB and ~2 MB was broken at some point.
insert into function s3(s3_conn, filename='test_02731_parquet_s3.parquet') select cityHash64(number) from numbers(170000) settings s3_truncate_on_insert=1;

select sum(*) from s3(s3_conn, filename='test_02731_parquet_s3.parquet') settings remote_filesystem_read_method='threadpool', remote_filesystem_read_prefetch=1;
