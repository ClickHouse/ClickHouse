ATTACH TABLE _ UUID 'e7ee50bc-5b90-4549-ad52-3903b4e178e5'
(
    `a` UInt64,
    `b` String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 36009, min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1., replace_long_file_name_to_hash = false, max_file_name_length = 0, min_bytes_for_full_part_storage = 0, compact_parts_max_bytes_to_buffer = 109794606, compact_parts_max_granules_to_buffer = 256, compact_parts_merge_max_bytes_to_prefetch_part = 17495713, merge_max_block_size = 17061, old_parts_lifetime = 480., prefer_fetch_merged_part_size_threshold = 10737418240, vertical_merge_algorithm_min_rows_to_activate = 1000000, vertical_merge_algorithm_min_columns_to_activate = 34, min_merge_bytes_to_use_direct_io = 914524523, index_granularity_bytes = 838221, use_const_adaptive_granularity = false, enable_index_granularity_compression = true, concurrent_part_removal_threshold = 29, allow_vertical_merges_from_compact_to_wide_parts = false, enable_block_number_column = false, enable_block_offset_column = false, cache_populated_by_fetch = true, compress_marks = true, compress_primary_key = true, marks_compress_block_size = 58548, primary_key_compress_block_size = 88260, use_primary_key_cache = false, prewarm_primary_key_cache = true, prewarm_mark_cache = true
