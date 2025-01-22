ATTACH TABLE _ UUID 'e15054ac-8cb2-4898-81ab-8f8433a0f82a'
(
    `a` UInt64,
    `b` String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 28000, min_bytes_for_wide_part = 231695830, ratio_of_defaults_for_sparse_serialization = 0., replace_long_file_name_to_hash = true, max_file_name_length = 6, min_bytes_for_full_part_storage = 395209869, compact_parts_max_bytes_to_buffer = 292410606, compact_parts_max_granules_to_buffer = 139, compact_parts_merge_max_bytes_to_prefetch_part = 4554192, merge_max_block_size = 4124, old_parts_lifetime = 228., prefer_fetch_merged_part_size_threshold = 1568712288, vertical_merge_algorithm_min_rows_to_activate = 1000000, vertical_merge_algorithm_min_columns_to_activate = 1, min_merge_bytes_to_use_direct_io = 10737418240, index_granularity_bytes = 10769678, use_const_adaptive_granularity = false, enable_index_granularity_compression = false, concurrent_part_removal_threshold = 100, allow_vertical_merges_from_compact_to_wide_parts = false, enable_block_number_column = false, enable_block_offset_column = true, cache_populated_by_fetch = false, compress_marks = false, compress_primary_key = true, marks_compress_block_size = 72526, primary_key_compress_block_size = 38806, use_primary_key_cache = false, prewarm_primary_key_cache = true, prewarm_mark_cache = false
