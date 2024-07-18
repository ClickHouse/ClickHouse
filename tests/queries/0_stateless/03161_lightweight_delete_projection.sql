
SET lightweight_deletes_sync = 2, alter_sync = 2;

Set max_insert_threads = 2,
group_by_two_level_threshold = 704642,
group_by_two_level_threshold_bytes = 49659607,
distributed_aggregation_memory_efficient = 0,
fsync_metadata = 0,
output_format_parallel_formatting = 0,
input_format_parallel_parsing = 1,
min_chunk_bytes_for_parallel_parsing = 14437539,
max_read_buffer_size = 887507,
prefer_localhost_replica = 0,
max_block_size = 73908,
max_joined_block_size_rows = 21162,
max_threads = 2,
optimize_append_index = 0,
optimize_if_chain_to_multiif = 1,
optimize_if_transform_strings_to_enum = 0,
optimize_read_in_order = 0,
optimize_or_like_chain = 1,
optimize_substitute_columns = 1,
enable_multiple_prewhere_read_steps = 1,
read_in_order_two_level_merge_threshold = 13,
optimize_aggregation_in_order = 1,
aggregation_in_order_max_block_bytes = 37110261,
use_uncompressed_cache = 0,
min_bytes_to_use_direct_io = 10737418240,
min_bytes_to_use_mmap_io = 1,
local_filesystem_read_method ='pread',
remote_filesystem_read_method ='threadpool',
local_filesystem_read_prefetch = 0,
filesystem_cache_segments_batch_size = 3,
read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 1,
throw_on_error_from_cache_on_write_operations = 0,
remote_filesystem_read_prefetch = 1,
allow_prefetched_read_pool_for_remote_filesystem = 0,
filesystem_prefetch_max_memory_usage = '32Mi',
filesystem_prefetches_limit = 0,
filesystem_prefetch_min_bytes_for_single_read_task ='16Mi',
filesystem_prefetch_step_marks = 50,
filesystem_prefetch_step_bytes = 0,
compile_aggregate_expressions = 0,
compile_sort_description = 1,
merge_tree_coarse_index_granularity = 16,
optimize_distinct_in_order = 0,
max_bytes_before_external_sort = 0,
max_bytes_before_external_group_by = 0,
max_bytes_before_remerge_sort = 820113150,
min_compress_block_size = 1262249,
max_compress_block_size = 1472188,
merge_tree_compact_parts_min_granules_to_multibuffer_read = 56,
optimize_sorting_by_input_stream_properties = 1,
http_response_buffer_size = 1883022,
http_wait_end_of_query = False,
enable_memory_bound_merging_of_aggregation_results = 1,
min_count_to_compile_expression = 0,
min_count_to_compile_aggregate_expression = 0,
min_count_to_compile_sort_description = 0,
session_timezone ='Africa/Khartoum',
prefer_warmed_unmerged_parts_seconds = 10,
use_page_cache_for_disks_without_file_cache = True,
page_cache_inject_eviction = False,
merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.02,
prefer_external_sort_block_bytes = 100000000,
cross_join_min_rows_to_compress = 100000000,
cross_join_min_bytes_to_compress = 1,
min_external_table_block_size_bytes = 100000000,
max_parsing_threads = 0;


DROP TABLE IF EXISTS users;

-- compact part
CREATE TABLE users (
    uid Int16,
    name String,
    age Int16,
    projection p1 (select count(), age group by age),
    projection p2 (select age, name group by age, name)
) ENGINE = MergeTree order by uid
SETTINGS min_bytes_for_wide_part = 10485760,
ratio_of_defaults_for_sparse_serialization = 1.0,
prefer_fetch_merged_part_size_threshold = 1,
vertical_merge_algorithm_min_rows_to_activate = 1,
vertical_merge_algorithm_min_columns_to_activate = 100,
allow_vertical_merges_from_compact_to_wide_parts = 0,
min_merge_bytes_to_use_direct_io = 114145183,
index_granularity_bytes = 2660363,
merge_max_block_size = 13460,
index_granularity = 51768,
marks_compress_block_size = 59418,
primary_key_compress_block_size = 88795,
replace_long_file_name_to_hash = 0,
max_file_name_length = 0,
min_bytes_for_full_part_storage = 536870912,
compact_parts_max_bytes_to_buffer = 378557913,
compact_parts_max_granules_to_buffer = 254,
compact_parts_merge_max_bytes_to_prefetch_part = 26969686,
cache_populated_by_fetch = 0,
concurrent_part_removal_threshold = 38,
old_parts_lifetime = 480;

INSERT INTO users VALUES (1231, 'John', 33);

-- testing throw default mode
ALTER TABLE users MODIFY SETTING lightweight_mutation_projection_mode = 'throw';

DELETE FROM users WHERE uid = 1231;  -- { serverError NOT_IMPLEMENTED }

-- testing drop mode
ALTER TABLE users MODIFY SETTING lightweight_mutation_projection_mode = 'drop';

DELETE FROM users WHERE uid = 1231;

SELECT * FROM users ORDER BY uid;

SYSTEM FLUSH LOGS;

-- expecting no projection
SELECT
    name
FROM system.projection_parts
WHERE (database = currentDatabase()) AND (`table` = 'users') AND (active = 1);

-- testing rebuild mode
INSERT INTO users VALUES (6666, 'Ksenia', 48), (8888, 'Alice', 50);

ALTER TABLE users MODIFY SETTING lightweight_mutation_projection_mode = 'rebuild';

DELETE FROM users WHERE uid = 6666;

SELECT * FROM users ORDER BY uid;

SYSTEM FLUSH LOGS;

-- expecting projection p1, p2
SELECT
    name
FROM system.projection_parts
WHERE (database = currentDatabase()) AND (`table` = 'users') AND (active = 1);

DROP TABLE users;


-- wide part
CREATE TABLE users (
    uid Int16,
    name String,
    age Int16,
    projection p1 (select count(), age group by age),
    projection p2 (select age, name group by age, name)
) ENGINE = MergeTree order by uid
SETTINGS min_bytes_for_wide_part = 0,
ratio_of_defaults_for_sparse_serialization = 1.0,
prefer_fetch_merged_part_size_threshold = 1,
vertical_merge_algorithm_min_rows_to_activate = 1,
vertical_merge_algorithm_min_columns_to_activate = 100,
allow_vertical_merges_from_compact_to_wide_parts = 0,
min_merge_bytes_to_use_direct_io = 114145183,
index_granularity_bytes = 2660363,
merge_max_block_size = 13460,
index_granularity = 51768,
marks_compress_block_size = 59418,
primary_key_compress_block_size = 88795,
replace_long_file_name_to_hash = 0,
max_file_name_length = 0,
min_bytes_for_full_part_storage = 536870912,
compact_parts_max_bytes_to_buffer = 378557913,
compact_parts_max_granules_to_buffer = 254,
compact_parts_merge_max_bytes_to_prefetch_part = 26969686,
cache_populated_by_fetch = 0,
concurrent_part_removal_threshold = 38,
old_parts_lifetime = 480;

INSERT INTO users VALUES (1231, 'John', 33);

-- testing throw default mode
ALTER TABLE users MODIFY SETTING lightweight_mutation_projection_mode = 'throw';

DELETE FROM users WHERE uid = 1231;  -- { serverError NOT_IMPLEMENTED }

-- testing drop mode
ALTER TABLE users MODIFY SETTING lightweight_mutation_projection_mode = 'drop';

DELETE FROM users WHERE uid = 1231;

SELECT * FROM users ORDER BY uid;

SYSTEM FLUSH LOGS;

-- expecting no projection
SELECT
    name
FROM system.projection_parts
WHERE (database = currentDatabase()) AND (`table` = 'users') AND (active = 1);

-- testing rebuild mode
INSERT INTO users VALUES (6666, 'Ksenia', 48), (8888, 'Alice', 50);

ALTER TABLE users MODIFY SETTING lightweight_mutation_projection_mode = 'rebuild';

DELETE FROM users WHERE uid = 6666;

SELECT * FROM users ORDER BY uid;

SYSTEM FLUSH LOGS;

-- expecting projection p1, p2
SELECT
    name
FROM system.projection_parts
WHERE (database = currentDatabase()) AND (`table` = 'users') AND (active = 1);


DROP TABLE users;