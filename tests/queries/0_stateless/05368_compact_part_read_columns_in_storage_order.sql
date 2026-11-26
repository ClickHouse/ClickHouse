-- Tags: no-fasttest
-- no-fasttest: needs an S3 disk.

-- A compact part stores the columns of each granule one after another. Reading them in another order, as FINAL
-- does by reading the sorting key last, must not send a new request to object storage for every granule.

DROP TABLE IF EXISTS t_compact_order;

CREATE TABLE t_compact_order (k UInt64, v UInt64) ENGINE = ReplacingMergeTree ORDER BY k
SETTINGS storage_policy = 's3_no_cache', index_granularity = 64, index_granularity_bytes = 10485760,
    min_bytes_for_wide_part = 10737418240, min_bytes_for_full_part_storage = 0,
    compress_per_column_in_compact_parts = 1, ratio_of_defaults_for_sparse_serialization = 1,
    compact_parts_max_granules_to_buffer = 1;

SYSTEM STOP MERGES t_compact_order;

INSERT INTO t_compact_order SELECT number, number FROM numbers(40000) SETTINGS max_insert_threads = 1;
INSERT INTO t_compact_order SELECT number, number % 32000 FROM numbers(40000) SETTINGS max_insert_threads = 1;

SELECT part_type, part_storage_type, count(), sum(marks) FROM system.parts
WHERE database = currentDatabase() AND table = 't_compact_order' AND active GROUP BY ALL;

SELECT v FROM t_compact_order FINAL FORMAT Null
SETTINGS max_threads = 1, remote_filesystem_read_method = 'threadpool', use_uncompressed_cache = 0, load_marks_asynchronously = 0,
    use_page_cache_for_disks_without_file_cache = 0, use_page_cache_for_object_storage = 0, use_columns_cache = 0,
    enable_parallel_replicas = 0, allow_prefetched_read_pool_for_remote_filesystem = 0,
    split_parts_ranges_into_intersecting_and_non_intersecting_final = 0, split_intersecting_parts_ranges_into_layers_final = 0,
    merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    log_comment = '05368_final';

SELECT v, k FROM t_compact_order FORMAT Null
SETTINGS max_threads = 1, remote_filesystem_read_method = 'threadpool', use_uncompressed_cache = 0, load_marks_asynchronously = 0,
    use_page_cache_for_disks_without_file_cache = 0, use_page_cache_for_object_storage = 0, use_columns_cache = 0,
    enable_parallel_replicas = 0, allow_prefetched_read_pool_for_remote_filesystem = 0,
    log_comment = '05368_columns_reversed';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['S3GetObject'] < 100 AS few_requests
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05368_final', '05368_columns_reversed')
ORDER BY log_comment;

SELECT count(), sum(v) FROM t_compact_order FINAL;

DROP TABLE t_compact_order;
