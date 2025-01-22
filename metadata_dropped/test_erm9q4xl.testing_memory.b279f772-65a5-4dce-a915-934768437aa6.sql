ATTACH TABLE _ UUID 'b279f772-65a5-4dce-a915-934768437aa6'
(
    `a` UInt64,
    `b` String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 49866, min_bytes_for_wide_part = 1073741824, ratio_of_defaults_for_sparse_serialization = 0.37733888626098633, replace_long_file_name_to_hash = true, max_file_name_length = 122, min_bytes_for_full_part_storage = 158092085, compact_parts_max_bytes_to_buffer = 308093789, compact_parts_max_granules_to_buffer = 256, compact_parts_merge_max_bytes_to_prefetch_part = 12063361, merge_max_block_size = 13507, old_parts_lifetime = 480., prefer_fetch_merged_part_size_threshold = 2898778038, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 40, min_merge_bytes_to_use_direct_io = 10737418240, index_granularity_bytes = 9945034, use_const_adaptive_granularity = false, enable_index_granularity_compression = false, concurrent_part_removal_threshold = 32, allow_vertical_merges_from_compact_to_wide_parts = false, enable_block_number_column = true, enable_block_offset_column = false, cache_populated_by_fetch = false, compress_marks = true, compress_primary_key = false, marks_compress_block_size = 47172, primary_key_compress_block_size = 18100, use_primary_key_cache = true, prewarm_primary_key_cache = true, prewarm_mark_cache = false
