ATTACH TABLE _ UUID 'd24a0709-5e14-4a3c-a428-b73aaacb72db'
(
    `a` UInt64,
    `b` String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 35713, min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.1607450246810913, replace_long_file_name_to_hash = false, max_file_name_length = 33, min_bytes_for_full_part_storage = 536870912, compact_parts_max_bytes_to_buffer = 133370827, compact_parts_max_granules_to_buffer = 130, compact_parts_merge_max_bytes_to_prefetch_part = 4321887, merge_max_block_size = 16910, old_parts_lifetime = 480., prefer_fetch_merged_part_size_threshold = 8298821396, vertical_merge_algorithm_min_rows_to_activate = 245027, vertical_merge_algorithm_min_columns_to_activate = 1, min_merge_bytes_to_use_direct_io = 6372605115, index_granularity_bytes = 5144273, use_const_adaptive_granularity = true, enable_index_granularity_compression = true, concurrent_part_removal_threshold = 32, allow_vertical_merges_from_compact_to_wide_parts = false, enable_block_number_column = true, enable_block_offset_column = true, cache_populated_by_fetch = true, compress_marks = false, compress_primary_key = false, marks_compress_block_size = 11150, primary_key_compress_block_size = 9718, use_primary_key_cache = true, prewarm_primary_key_cache = true, prewarm_mark_cache = false
