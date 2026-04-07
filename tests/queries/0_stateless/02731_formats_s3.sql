-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- Reading from s3 a parquet file of size between ~1 MB and ~2 MB was broken at some point
-- (bug in CachedOnDiskReadBufferFromFile).
select sum(*) from s3(s3_conn, filename='02731.parquet') settings remote_filesystem_read_method='threadpool', remote_filesystem_read_prefetch=1;

-- Reading from s3 of arrow files of ~40 MB (max_download_buffer_size * 4) was broken at some point
-- (bug in ParallelReadBuffer).
select sum(*) from s3(s3_conn, filename='02731.arrow') settings remote_filesystem_read_method='read', max_download_buffer_size = 1048576;
