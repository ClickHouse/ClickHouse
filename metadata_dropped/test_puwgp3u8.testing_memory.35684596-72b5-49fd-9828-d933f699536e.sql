ATTACH TABLE _ UUID '35684596-72b5-49fd-9828-d933f699536e'
(
    `a` UInt64,
    `b` String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 49239, min_bytes_for_wide_part = 1073741824, ratio_of_defaults_for_sparse_serialization = 0.5846956372261047, replace_long_file_name_to_hash = false, max_file_name_length = 128, min_bytes_for_full_part_storage = 311347968, compact_parts_max_bytes_to_buffer = 452960105, compact_parts_max_granules_to_buffer = 1, compact_parts_merge_max_bytes_to_prefetch_part = 26010360, merge_max_block_size = 6432, old_parts_lifetime = 10., prefer_fetch_merged_part_size_threshold = 10737418240, vertical_merge_algorithm_min_rows_to_activate = 1000000, vertical_merge_algorithm_min_columns_to_activate = 1, min_merge_bytes_to_use_direct_io = 10737418240, index_granularity_bytes = 5483939, use_const_adaptive_granularity = true, enable_index_granularity_compression = false, concurrent_part_removal_threshold = 100, allow_vertical_merges_from_compact_to_wide_parts = true, enable_block_number_column = true, enable_block_offset_column = false, cache_populated_by_fetch = true, compress_marks = false, compress_primary_key = false, marks_compress_block_size = 93808, primary_key_compress_block_size = 78941, use_primary_key_cache = true, prewarm_primary_key_cache = true, prewarm_mark_cache = false
