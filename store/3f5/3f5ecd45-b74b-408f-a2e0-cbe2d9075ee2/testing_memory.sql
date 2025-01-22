ATTACH TABLE _ UUID '94c07ff0-1814-4f44-ba3f-9130e042027b'
(
    `a` UInt64,
    `b` String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 3826, min_bytes_for_wide_part = 539131753, ratio_of_defaults_for_sparse_serialization = 0., replace_long_file_name_to_hash = false, max_file_name_length = 1, min_bytes_for_full_part_storage = 0, compact_parts_max_bytes_to_buffer = 233457436, compact_parts_max_granules_to_buffer = 1, compact_parts_merge_max_bytes_to_prefetch_part = 2039946, merge_max_block_size = 11259, old_parts_lifetime = 480., prefer_fetch_merged_part_size_threshold = 7926328036, vertical_merge_algorithm_min_rows_to_activate = 634677, vertical_merge_algorithm_min_columns_to_activate = 1, min_merge_bytes_to_use_direct_io = 10737418240, index_granularity_bytes = 22528807, use_const_adaptive_granularity = false, enable_index_granularity_compression = false, concurrent_part_removal_threshold = 45, allow_vertical_merges_from_compact_to_wide_parts = true, enable_block_number_column = true, enable_block_offset_column = false, cache_populated_by_fetch = true, compress_marks = true, compress_primary_key = true, marks_compress_block_size = 68362, primary_key_compress_block_size = 24811, use_primary_key_cache = true, prewarm_primary_key_cache = true, prewarm_mark_cache = true
