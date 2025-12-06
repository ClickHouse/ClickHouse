-- Tags: no-parallel, no-parallel-replicas, no-fasttest

DROP TABLE IF EXISTS t_index_on_data_read_s3;

CREATE TABLE t_index_on_data_read_s3
(
    k UInt64,
    v UInt64,
    INDEX idx_v (v) TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k PARTITION BY k
SETTINGS storage_policy = 's3_no_cache', min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0, index_granularity = 1, index_granularity_bytes = '10M';

INSERT INTO t_index_on_data_read_s3 VALUES (1, 100), (2, 200);

SET max_threads = 4;
SET use_query_condition_cache = 0;
SET use_skip_indexes_on_data_read = 1;
SET remote_filesystem_read_method = 'threadpool';
SET remote_filesystem_read_prefetch = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET use_uncompressed_cache = 0;

SYSTEM DROP MARK CACHE;
SYSTEM DROP INDEX MARK CACHE;
SYSTEM DROP INDEX UNCOMPRESSED CACHE;
SELECT count() FROM t_index_on_data_read_s3 WHERE v = 100 SETTINGS allow_prefetched_read_pool_for_remote_filesystem = 1;

SYSTEM DROP MARK CACHE;
SYSTEM DROP INDEX MARK CACHE;
SYSTEM DROP INDEX UNCOMPRESSED CACHE;
SELECT count() FROM t_index_on_data_read_s3 WHERE v = 100 SETTINGS allow_prefetched_read_pool_for_remote_filesystem = 0;

SYSTEM DROP MARK CACHE;
SYSTEM DROP INDEX MARK CACHE;
SYSTEM DROP INDEX UNCOMPRESSED CACHE;
SELECT count() FROM t_index_on_data_read_s3 WHERE v = 300 SETTINGS allow_prefetched_read_pool_for_remote_filesystem = 1;

SYSTEM DROP MARK CACHE;
SYSTEM DROP INDEX MARK CACHE;
SYSTEM DROP INDEX UNCOMPRESSED CACHE;
SELECT count() FROM t_index_on_data_read_s3 WHERE v = 300 SETTINGS allow_prefetched_read_pool_for_remote_filesystem = 0;

SYSTEM FLUSH LOGS query_log;

SELECT read_rows, ProfileEvents['S3GetObject']
FROM system.query_log
WHERE current_database = currentDatabase() AND query LIKE 'SELECT count() FROM t_index_on_data_read_s3%' AND type = 'QueryFinish'
ORDER BY event_time_microseconds;

DROP TABLE t_index_on_data_read_s3;
