ATTACH TABLE _ UUID '81a55bb6-7591-4693-ab95-0ba48f0c3dfc'
(
    `a` UInt64,
    `b` String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 56112, min_bytes_for_wide_part = 1073741824, ratio_of_defaults_for_sparse_serialization = 0.4880082607269287, replace_long_file_name_to_hash = true, max_file_name_length = 0, min_bytes_for_full_part_storage = 0, compact_parts_max_bytes_to_buffer = 465802590, compact_parts_max_granules_to_buffer = 256, compact_parts_merge_max_bytes_to_prefetch_part = 14040267, merge_max_block_size = 10841, old_parts_lifetime = 405., prefer_fetch_merged_part_size_threshold = 10737418240, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 100, min_merge_bytes_to_use_direct_io = 8879778421, index_granularity_bytes = 2090591, use_const_adaptive_granularity = true, enable_index_granularity_compression = true, concurrent_part_removal_threshold = 100, allow_vertical_merges_from_compact_to_wide_parts = false, enable_block_number_column = true, enable_block_offset_column = true, cache_populated_by_fetch = false, compress_marks = false, compress_primary_key = false, marks_compress_block_size = 84584, primary_key_compress_block_size = 88958, use_primary_key_cache = false, prewarm_primary_key_cache = true, prewarm_mark_cache = false
