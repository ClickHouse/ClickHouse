-- Tags: no-random-settings, no-fasttest, no-parallel

SET allow_prefetched_read_pool_for_remote_filesystem=0;
SET allow_prefetched_read_pool_for_local_filesystem=0;
SET max_threads = 1;
SET remote_read_min_bytes_for_seek = 100000;
-- Will affect INSERT, but not merge
SET s3_check_objects_after_upload=1;

DROP TABLE IF EXISTS t_compact_bytes_s3;
CREATE TABLE t_compact_bytes_s3(c1 UInt32, c2 UInt32, c3 UInt32, c4 UInt32, c5 UInt32)
ENGINE = MergeTree ORDER BY c1
SETTINGS index_granularity = 512, min_bytes_for_wide_part = '10G', storage_policy = 's3_no_cache', write_marks_for_substreams_in_compact_parts=1;

INSERT INTO t_compact_bytes_s3 SELECT number, number, number, number, number FROM numbers(512 * 32 * 40);

SYSTEM DROP MARK CACHE;
OPTIMIZE TABLE t_compact_bytes_s3 FINAL;

SYSTEM DROP MARK CACHE;
SELECT count() FROM t_compact_bytes_s3 WHERE NOT ignore(c2, c4);
SYSTEM FLUSH LOGS query_log;

-- Errors in S3 requests will be automatically retried, however ProfileEvents can be wrong. That is why we subtract errors.
SELECT
    ProfileEvents['S3ReadRequestsCount'] - ProfileEvents['S3ReadRequestsErrors'],
    ProfileEvents['ReadBufferFromS3Bytes'] < ProfileEvents['ReadCompressedBytes'] * 1.1
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query ilike '%INSERT INTO t_compact_bytes_s3 SELECT number, number, number%';

SELECT
    ProfileEvents['S3ReadRequestsCount'] - ProfileEvents['S3ReadRequestsErrors'],
    ProfileEvents['ReadBufferFromS3Bytes'] < ProfileEvents['ReadCompressedBytes'] * 1.1
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query ilike '%OPTIMIZE TABLE t_compact_bytes_s3 FINAL%';

DROP TABLE IF EXISTS t_compact_bytes_s3;
