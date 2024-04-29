#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# reset --log_comment
CLICKHOUSE_LOG_COMMENT=
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH_CLIENT="$CLICKHOUSE_CLIENT --allow_merge_tree_settings --allow_experimental_dynamic_type=1 --allow_experimental_variant_type=1 --use_variant_as_common_type=1 --stacktrace --max_insert_threads 3 --group_by_two_level_threshold 1000000 --group_by_two_level_threshold_bytes 42526602 --distributed_aggregation_memory_efficient 1 --fsync_metadata 1 --output_format_parallel_formatting 0 --input_format_parallel_parsing 0 --min_chunk_bytes_for_parallel_parsing 8125230 --max_read_buffer_size 859505 --prefer_localhost_replica 1 --max_block_size 34577 --max_threads 41 --optimize_append_index 0 --optimize_if_chain_to_multiif 1 --optimize_if_transform_strings_to_enum 1 --optimize_read_in_order 1 --optimize_or_like_chain 0 --optimize_substitute_columns 1 --enable_multiple_prewhere_read_steps 1 --read_in_order_two_level_merge_threshold 99 --optimize_aggregation_in_order 1 --aggregation_in_order_max_block_bytes 27635208 --use_uncompressed_cache 0 --min_bytes_to_use_direct_io 10737418240 --min_bytes_to_use_mmap_io 6451111320 --local_filesystem_read_method pread --remote_filesystem_read_method read --local_filesystem_read_prefetch 1 --filesystem_cache_segments_batch_size 50 --read_from_filesystem_cache_if_exists_otherwise_bypass_cache 0 --throw_on_error_from_cache_on_write_operations 0 --remote_filesystem_read_prefetch 1 --allow_prefetched_read_pool_for_remote_filesystem 0 --filesystem_prefetch_max_memory_usage 64Mi --filesystem_prefetches_limit 10 --filesystem_prefetch_min_bytes_for_single_read_task 16Mi --filesystem_prefetch_step_marks 0 --filesystem_prefetch_step_bytes 100Mi --compile_aggregate_expressions 0 --compile_sort_description 1 --merge_tree_coarse_index_granularity 32 --optimize_distinct_in_order 0 --max_bytes_before_external_sort 10737418240 --max_bytes_before_external_group_by 10737418240 --max_bytes_before_remerge_sort 1374192967 --min_compress_block_size 2152247 --max_compress_block_size 1830907 --merge_tree_compact_parts_min_granules_to_multibuffer_read 79 --optimize_sorting_by_input_stream_properties 1 --http_response_buffer_size 106072 --http_wait_end_of_query True --enable_memory_bound_merging_of_aggregation_results 0 --min_count_to_compile_expression 0 --min_count_to_compile_aggregate_expression 3 --min_count_to_compile_sort_description 3 --session_timezone Africa/Khartoum --prefer_warmed_unmerged_parts_seconds 4 --use_page_cache_for_disks_without_file_cache False --page_cache_inject_eviction True --merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability 0.03 --ratio_of_defaults_for_sparse_serialization 0.9779014012142565 --prefer_fetch_merged_part_size_threshold 4254002758 --vertical_merge_algorithm_min_rows_to_activate 1 --vertical_merge_algorithm_min_columns_to_activate 1 --allow_vertical_merges_from_compact_to_wide_parts 1 --min_merge_bytes_to_use_direct_io 1 --index_granularity_bytes 4982992 --merge_max_block_size 16662 --index_granularity 22872 --min_bytes_for_wide_part 1073741824 --compress_marks 0 --compress_primary_key 0 --marks_compress_block_size 86328 --primary_key_compress_block_size 64101 --replace_long_file_name_to_hash 0 --max_file_name_length 81 --min_bytes_for_full_part_storage 536870912 --compact_parts_max_bytes_to_buffer 480908080 --compact_parts_max_granules_to_buffer 1 --compact_parts_merge_max_bytes_to_prefetch_part 4535313 --cache_populated_by_fetch 0"

function run()
{
    echo "initial insert"
    $CH_CLIENT -q "insert into test select number, number from numbers(3)"

    echo "alter add column 1"
    $CH_CLIENT -q "alter table test add column d Dynamic(max_types=3) settings mutations_sync=1"
    $CH_CLIENT -q "select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d)"
    $CH_CLIENT -q "select x, y, d, d.String, d.UInt64, d.\`Tuple(a UInt64)\`.a from test order by x"

    echo "insert after alter add column 1"
    $CH_CLIENT -q "insert into test select number, number, number from numbers(3, 3)"
    $CH_CLIENT -q "insert into test select number, number, 'str_' || toString(number) from numbers(6, 3)"
    $CH_CLIENT -q "insert into test select number, number, NULL from numbers(9, 3)"
    $CH_CLIENT -q "insert into test select number, number, multiIf(number % 3 == 0, number, number % 3 == 1, 'str_' || toString(number), NULL) from numbers(12, 3)"
    $CH_CLIENT -q "select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d)"
    $CH_CLIENT -q "select x, y, d, d.String, d.UInt64, d.Date, d.\`Tuple(a UInt64)\`.a from test order by x"

    echo "alter modify column 1"
    $CH_CLIENT -q "alter table test modify column d Dynamic(max_types=1) settings mutations_sync=1"
    $CH_CLIENT -q "select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d)"
    $CH_CLIENT -q "select x, y, d, d.String, d.UInt64, d.Date, d.\`Tuple(a UInt64)\`.a from test order by x"

    echo "insert after alter modify column 1"
    $CH_CLIENT -q "insert into test select number, number, multiIf(number % 4 == 0, number, number % 4 == 1, 'str_' || toString(number), number % 4 == 2, toDate(number), NULL) from numbers(15, 4)"
    $CH_CLIENT -q "select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d)"
    $CH_CLIENT -q "select x, y, d, d.String, d.UInt64, d.Date, d.\`Tuple(a UInt64)\`.a from test order by x"

    echo "alter modify column 2"
    $CH_CLIENT -q "alter table test modify column d Dynamic(max_types=3) settings mutations_sync=1"
    $CH_CLIENT -q "select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d)"
    $CH_CLIENT -q "select x, y, d, d.String, d.UInt64, d.Date, d.\`Tuple(a UInt64)\`.a from test order by x"

    echo "insert after alter modify column 2"
    $CH_CLIENT -q "insert into test select number, number, multiIf(number % 4 == 0, number, number % 4 == 1, 'str_' || toString(number), number % 4 == 2, toDate(number), NULL) from numbers(19, 4)"
    $CH_CLIENT -q "select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d)"
    $CH_CLIENT -q "select x, y, d, d.String, d.UInt64, d.Date, d.\`Tuple(a UInt64)\`.a from test order by x"

    echo "alter modify column 3"
    $CH_CLIENT -q "alter table test modify column y Dynamic settings mutations_sync=1"
    $CH_CLIENT -q "select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d)"
    $CH_CLIENT -q "select x, y, y.UInt64, y.String, y.\`Tuple(a UInt64)\`.a, d.String, d.UInt64, d.Date, d.\`Tuple(a UInt64)\`.a from test order by x"

    echo "insert after alter modify column 3"
    $CH_CLIENT -q "insert into test select number, multiIf(number % 3 == 0, number, number % 3 == 1, 'str_' || toString(number), NULL), NULL from numbers(23, 3)"
    $CH_CLIENT -q "select count(), dynamicType(d) from test group by dynamicType(d) order by count(), dynamicType(d)"
    $CH_CLIENT -q "select x, y, y.UInt64, y.String, y.\`Tuple(a UInt64)\`.a, d.String, d.UInt64, d.Date, d.\`Tuple(a UInt64)\`.a from test order by x"
}

$CH_CLIENT -q "drop table if exists test;"

echo "Memory"
$CH_CLIENT -q "create table test (x UInt64, y UInt64) engine=Memory"
run
$CH_CLIENT -q "drop table test;"

echo "MergeTree compact"
$CH_CLIENT -q "create table test (x UInt64, y UInt64) engine=MergeTree order by x settings min_rows_for_wide_part=100000000, min_bytes_for_wide_part=1000000000;"
run
$CH_CLIENT -q "drop table test;"

echo "MergeTree wide"
$CH_CLIENT -q "create table test (x UInt64, y UInt64 ) engine=MergeTree order by x settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1;"
run
$CH_CLIENT -q "drop table test;"
