DROP TABLE IF EXISTS test_00808;

CREATE TABLE test_00808
(
    `date` Date,
    `id` Int8,
    `name` String,
    `value` Int64,
    `sign` Int8
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY (id, date)
SETTINGS index_granularity = 62918, min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0., replace_long_file_name_to_hash = false, max_file_name_length = 79, min_bytes_for_full_part_storage = 536870912, compact_parts_max_bytes_to_buffer = 525685803, compact_parts_max_granules_to_buffer = 148, compact_parts_merge_max_bytes_to_prefetch_part = 23343456, merge_max_block_size = 22854, old_parts_lifetime = 93., prefer_fetch_merged_part_size_threshold = 3610162434, vertical_merge_algorithm_min_rows_to_activate = 1000000, vertical_merge_algorithm_min_columns_to_activate = 1, min_merge_bytes_to_use_direct_io = 1, index_granularity_bytes = 9550183, concurrent_part_removal_threshold = 34, allow_vertical_merges_from_compact_to_wide_parts = false, cache_populated_by_fetch = false, marks_compress_block_size = 97756, primary_key_compress_block_size = 80902;

INSERT INTO test_00808 VALUES('2000-01-01', 1, 'test string 1', 1, 1);
INSERT INTO test_00808 VALUES('2000-01-01', 2, 'test string 2', 2, 1);

SET enable_analyzer=1, allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree=1, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

SELECT * FROM (SELECT * FROM test_00808 FINAL) WHERE id = 1; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE test_00808;
