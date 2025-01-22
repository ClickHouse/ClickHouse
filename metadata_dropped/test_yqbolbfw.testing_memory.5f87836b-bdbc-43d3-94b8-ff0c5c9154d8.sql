ATTACH TABLE _ UUID '5f87836b-bdbc-43d3-94b8-ff0c5c9154d8'
(
    `a` UInt64,
    `b` String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 10340, min_bytes_for_wide_part = 1073741824, ratio_of_defaults_for_sparse_serialization = 1., replace_long_file_name_to_hash = true, max_file_name_length = 27, min_bytes_for_full_part_storage = 227269946, compact_parts_max_bytes_to_buffer = 447883327, compact_parts_max_granules_to_buffer = 185, compact_parts_merge_max_bytes_to_prefetch_part = 3807218, merge_max_block_size = 2969, old_parts_lifetime = 10., prefer_fetch_merged_part_size_threshold = 7802677751, vertical_merge_algorithm_min_rows_to_activate = 1000000, vertical_merge_algorithm_min_columns_to_activate = 100, min_merge_bytes_to_use_direct_io = 250312586, index_granularity_bytes = 9211155, use_const_adaptive_granularity = true, enable_index_granularity_compression = true, concurrent_part_removal_threshold = 41, allow_vertical_merges_from_compact_to_wide_parts = false, enable_block_number_column = false, enable_block_offset_column = false, cache_populated_by_fetch = true, compress_marks = true, compress_primary_key = false, marks_compress_block_size = 21117, primary_key_compress_block_size = 38480, use_primary_key_cache = true, prewarm_primary_key_cache = true, prewarm_mark_cache = false
