-- Tags: no-parallel, no-parallel-replicas, no-fasttest

DROP TABLE IF EXISTS t_projection_on_data_read_s3;

CREATE TABLE t_projection_on_data_read_s3
(
    k UInt64,
    v UInt64,
    PROJECTION p (SELECT _part_offset ORDER BY v)
)
ENGINE = MergeTree
ORDER BY k PARTITION BY k
SETTINGS storage_policy = 's3_no_cache', min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0, index_granularity = 1024, index_granularity_bytes = '10M', use_primary_key_cache = 0;

INSERT INTO t_projection_on_data_read_s3 SELECT 1, number * 2 FROM numbers(100000);
INSERT INTO t_projection_on_data_read_s3 SELECT 2, number * 2 + 1 FROM numbers(100000);

SET max_threads = 4;
SET use_query_condition_cache = 0;
SET use_skip_indexes_on_data_read = 1;
SET remote_filesystem_read_method = 'threadpool';
SET remote_filesystem_read_prefetch = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET optimize_use_projection_filtering = 1;
SET min_table_rows_to_use_projection_index = 0;
SET use_uncompressed_cache = 0;

SYSTEM DROP MARK CACHE;
SELECT * FROM t_projection_on_data_read_s3 WHERE v = 55555 SETTINGS allow_prefetched_read_pool_for_remote_filesystem = 1;

SYSTEM DROP MARK CACHE;
SELECT * FROM t_projection_on_data_read_s3 WHERE v = 55555 SETTINGS allow_prefetched_read_pool_for_remote_filesystem = 0;

SYSTEM DROP MARK CACHE;
SELECT * FROM t_projection_on_data_read_s3 WHERE v = 11115555 SETTINGS allow_prefetched_read_pool_for_remote_filesystem = 1;

SYSTEM DROP MARK CACHE;
SELECT * FROM t_projection_on_data_read_s3 WHERE v = 11115555 SETTINGS allow_prefetched_read_pool_for_remote_filesystem = 0;

SYSTEM FLUSH LOGS query_log;

SELECT read_rows, ProfileEvents['S3GetObject']
FROM system.query_log
WHERE current_database = currentDatabase() AND query LIKE 'SELECT * FROM t_projection_on_data_read_s3%' AND type = 'QueryFinish'
ORDER BY event_time_microseconds;

DROP TABLE t_projection_on_data_read_s3;
