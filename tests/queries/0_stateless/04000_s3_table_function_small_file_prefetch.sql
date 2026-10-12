-- Tags: no-fasttest, no-random-settings, no-parallel-replicas
-- - no-fasttest -- requires S3
-- - no-random-settings -- deterministic prefetch behavior
-- - no-parallel-replicas -- other replicas may do prefetch
--
-- Regression test: reading many small files via the s3() table function must use the initial
-- prefetch path. A change that taught the read buffer to support readBigAt accidentally
-- suppressed the small-object prefetch (it was gated on `!supportsReadAt()`), turning tiny-file
-- reads synchronous and latency-bound.

-- Write 16 tiny files. Each is far below 2 * max_download_buffer_size, i.e. "object_too_small",
-- so it takes the prefetch path.
INSERT INTO FUNCTION s3(s3_conn, filename='04000_prefetch_{_partition_id}.tsv', format='TSV', partition_strategy='wildcard')
PARTITION BY (a % 16)
SELECT number AS a, toString(number) AS b
FROM numbers(16000)
SETTINGS s3_truncate_on_insert = 1;

-- Read them back single-threaded with the prefetch path enabled. The filesystem cache is
-- disabled so every read goes to object storage and is accounted in RemoteFSPrefetches.
-- Read the data with FORMAT Null (a real read of every file); count() can be answered without
-- reading the data (optimize_count_from_files) and would not exercise the prefetch path.
SELECT * FROM s3(s3_conn, filename='04000_prefetch_*.tsv', format='TSV', structure='a UInt64, b String')
FORMAT Null
SETTINGS max_threads = 1,
         remote_filesystem_read_method = 'threadpool',
         remote_filesystem_read_prefetch = 1,
         enable_filesystem_cache = 0,
         log_comment = '04000_s3_small_file_prefetch';

-- Read them again with the filesystem cache on (the cache named here is defined in the
-- stateless test server config). The initial small-object prefetch must be issued also when
-- the read goes through the cache: the cache does no read-ahead of its own and a fresh file is
-- a cache miss, so without the prefetch every small file costs a synchronous round trip to
-- object storage. This is the ClickHouse Cloud configuration for object storage table engines
-- (e.g. `S3Queue` ingestion of many small files).
SELECT * FROM s3(s3_conn, filename='04000_prefetch_*.tsv', format='TSV', structure='a UInt64, b String')
FORMAT Null
SETTINGS max_threads = 1,
         remote_filesystem_read_method = 'threadpool',
         remote_filesystem_read_prefetch = 1,
         filesystem_cache_name = 'cache_for_readbigat',
         enable_filesystem_cache = 1,
         log_comment = '04000_s3_small_file_prefetch_with_cache';

SYSTEM FLUSH LOGS query_log;

-- Each query reads all 16 files, so each file must get the initial small-object prefetch:
-- expect at least one prefetch issued (RemoteFSPrefetches) and consumed
-- (RemoteFSPrefetchedReads) per file. Before the fix both were 0 and the reads were
-- synchronous. The cached query must also actually exercise the cached read path (bytes served
-- from the cache or from the source through the cache).
SELECT ProfileEvents['RemoteFSPrefetches'] >= 16, ProfileEvents['RemoteFSPrefetchedReads'] >= 16
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase()
  AND type = 'QueryFinish' AND query_kind = 'Select'
  AND log_comment = '04000_s3_small_file_prefetch'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT ProfileEvents['RemoteFSPrefetches'] >= 16,
       ProfileEvents['RemoteFSPrefetchedReads'] >= 16,
       ProfileEvents['CachedReadBufferReadFromCacheBytes'] + ProfileEvents['CachedReadBufferReadFromSourceBytes'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase()
  AND type = 'QueryFinish' AND query_kind = 'Select'
  AND log_comment = '04000_s3_small_file_prefetch_with_cache'
ORDER BY event_time_microseconds DESC
LIMIT 1;
