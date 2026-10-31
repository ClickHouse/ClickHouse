-- Tags: no-distributed-cache, no-encrypted-storage, no-parallel-replicas
-- The executor does not run with the distributed cache or decryption; with parallel replicas the counters stay on the replicas.

-- A read of two key ranges fetches no byte of the hole between them.

DROP TABLE IF EXISTS t_fetch_bound;

CREATE TABLE t_fetch_bound (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    ratio_of_defaults_for_sparse_serialization = 1;

INSERT INTO t_fetch_bound SELECT number, cityHash64(number) FROM numbers(2000000);
OPTIMIZE TABLE t_fetch_bound FINAL;

SET use_reader_executor = 1;
SET remote_filesystem_read_method = 'read', local_filesystem_read_method = 'pread', enable_filesystem_cache = 0;
-- The page cache fills whole cells, past the ranges.
SET use_page_cache_for_disks_without_file_cache = 0, use_page_cache_for_local_disks = 0, use_page_cache_for_object_storage = 0;
-- The second query must read through the executor too.
SET use_uncompressed_cache = 0, use_columns_cache = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
-- The hole is about 1.6 MB in `k`: keep it below `reader_executor_min_bytes_for_seek`, which is read through.
SET reader_executor_min_bytes_for_seek = 65536;

SELECT sum(v) FROM t_fetch_bound WHERE k < 200000 OR k >= 1800000 SETTINGS log_comment = '05366_two_ranges' FORMAT Null;
SELECT sum(v) FROM t_fetch_bound WHERE k < 200000 OR k >= 1800000
SETTINGS log_comment = '05366_two_ranges_long_connections', reader_executor_use_long_connections = 1 FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['ReaderExecutorBytesFromSource'] > 0 AS executor_ran,
    ProfileEvents['ReaderExecutorBytesOutsideRequestMap'] AS outside_map
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05366_two_ranges', '05366_two_ranges_long_connections')
ORDER BY log_comment;

DROP TABLE t_fetch_bound;
