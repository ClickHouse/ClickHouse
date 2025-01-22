ATTACH TABLE _ UUID '031912dc-b46e-4e59-83c7-776ab585e5a6'
(
    `a` UInt64,
    `b` String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 40491, min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0., replace_long_file_name_to_hash = true, max_file_name_length = 0, min_bytes_for_full_part_storage = 536870912, compact_parts_max_bytes_to_buffer = 364257046, compact_parts_max_granules_to_buffer = 114, compact_parts_merge_max_bytes_to_prefetch_part = 9384504, merge_max_block_size = 12095, old_parts_lifetime = 415., prefer_fetch_merged_part_size_threshold = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1, min_merge_bytes_to_use_direct_io = 1, index_granularity_bytes = 26833912, use_const_adaptive_granularity = true, enable_index_granularity_compression = false, concurrent_part_removal_threshold = 0, allow_vertical_merges_from_compact_to_wide_parts = true, enable_block_number_column = false, enable_block_offset_column = false, cache_populated_by_fetch = false, compress_marks = false, compress_primary_key = false, marks_compress_block_size = 25021, primary_key_compress_block_size = 31313, use_primary_key_cache = true, prewarm_primary_key_cache = false, prewarm_mark_cache = true
