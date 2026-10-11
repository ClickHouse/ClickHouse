-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings
-- no-fasttest: needs S3

-- A merge of a Compact part written in stripes reads it with a buffer per column. With a single buffer it would have
-- to seek for every column of every granule (more than a thousand S3 requests for this part instead of a few).

SET allow_prefetched_read_pool_for_remote_filesystem = 0;
SET allow_prefetched_read_pool_for_local_filesystem = 0;
SET max_threads = 1;
SET remote_read_min_bytes_for_seek = 100000;

DROP TABLE IF EXISTS t_compact_stripes_merge_s3;

CREATE TABLE t_compact_stripes_merge_s3 (c1 UInt32, c2 UInt32, c3 UInt32, c4 UInt32, c5 UInt32)
ENGINE = MergeTree ORDER BY c1
SETTINGS index_granularity = 512, min_bytes_for_wide_part = '10G', storage_policy = 's3_no_cache',
    write_marks_for_substreams_in_compact_parts = 1, auto_statistics_types = '',
    compact_parts_max_granules_to_buffer = 128, compact_parts_max_bytes_to_buffer = '128Mi';

INSERT INTO t_compact_stripes_merge_s3 SELECT number, number, number, number, number FROM numbers(512 * 32 * 40);

SELECT 'granules', count() FROM mergeTreeIndex(currentDatabase(), t_compact_stripes_merge_s3);

OPTIMIZE TABLE t_compact_stripes_merge_s3 FINAL;

SELECT count(), sum(c1), sum(c5) FROM t_compact_stripes_merge_s3;

SYSTEM FLUSH LOGS query_log;

SELECT 'merge requests below 100', ProfileEvents['S3ReadRequestsCount'] - ProfileEvents['S3ReadRequestsErrors'] < 100
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query ILIKE 'OPTIMIZE TABLE t_compact_stripes_merge_s3 FINAL%';

DROP TABLE t_compact_stripes_merge_s3;
