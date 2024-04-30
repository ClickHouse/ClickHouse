#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# reset --log_comment
CLICKHOUSE_LOG_COMMENT=
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH_CLIENT="$CLICKHOUSE_CLIENT --allow_merge_tree_settings --allow_experimental_variant_type=1 --allow_suspicious_variant_types=1 --max_insert_threads 0 --group_by_two_level_threshold 454338 --group_by_two_level_threshold_bytes 50000000 --distributed_aggregation_memory_efficient 1 --fsync_metadata 0 --output_format_parallel_formatting 0 --input_format_parallel_parsing 1 --min_chunk_bytes_for_parallel_parsing 10898151 --max_read_buffer_size 730200 --prefer_localhost_replica 1 --max_block_size 77643 --max_threads 18 --optimize_append_index 0 --optimize_if_chain_to_multiif 0 --optimize_if_transform_strings_to_enum 0 --optimize_read_in_order 0 --optimize_or_like_chain 0 --optimize_substitute_columns 0 --enable_multiple_prewhere_read_steps 0 --read_in_order_two_level_merge_threshold 20 --optimize_aggregation_in_order 1 --aggregation_in_order_max_block_bytes 39857781 --use_uncompressed_cache 1 --min_bytes_to_use_direct_io 1 --min_bytes_to_use_mmap_io 10737418240 --local_filesystem_read_method io_uring --remote_filesystem_read_method threadpool --local_filesystem_read_prefetch 1 --filesystem_cache_segments_batch_size 10 --read_from_filesystem_cache_if_exists_otherwise_bypass_cache 1 --throw_on_error_from_cache_on_write_operations 1 --remote_filesystem_read_prefetch 0 --allow_prefetched_read_pool_for_remote_filesystem 0 --filesystem_prefetch_max_memory_usage 128Mi --filesystem_prefetches_limit 0 --filesystem_prefetch_min_bytes_for_single_read_task 8Mi --filesystem_prefetch_step_marks 0 --filesystem_prefetch_step_bytes 100Mi --compile_aggregate_expressions 0 --compile_sort_description 0 --merge_tree_coarse_index_granularity 30 --optimize_distinct_in_order 1 --max_bytes_before_external_sort 10737418240 --max_bytes_before_external_group_by 1 --max_bytes_before_remerge_sort 2279999838 --min_compress_block_size 56847 --max_compress_block_size 2399536 --merge_tree_compact_parts_min_granules_to_multibuffer_read 39 --optimize_sorting_by_input_stream_properties 1 --http_response_buffer_size 2739586 --http_wait_end_of_query False --enable_memory_bound_merging_of_aggregation_results 1 --min_count_to_compile_expression 3 --min_count_to_compile_aggregate_expression 0 --min_count_to_compile_sort_description 3 --session_timezone America/Mazatlan --prefer_warmed_unmerged_parts_seconds 7 --use_page_cache_for_disks_without_file_cache False --page_cache_inject_eviction True --merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability 0.19 --ratio_of_defaults_for_sparse_serialization 0.0 --prefer_fetch_merged_part_size_threshold 1 --vertical_merge_algorithm_min_rows_to_activate 389696 --vertical_merge_algorithm_min_columns_to_activate 100 --allow_vertical_merges_from_compact_to_wide_parts 0 --min_merge_bytes_to_use_direct_io 10737418240 --index_granularity_bytes 16233524 --merge_max_block_size 6455 --index_granularity 16034 --min_bytes_for_wide_part 0 --compress_marks 0 --compress_primary_key 0 --marks_compress_block_size 15959 --primary_key_compress_block_size 70269 --replace_long_file_name_to_hash 1 --max_file_name_length 123 --min_bytes_for_full_part_storage 0 --compact_parts_max_bytes_to_buffer 511937149 --compact_parts_max_granules_to_buffer 142 --compact_parts_merge_max_bytes_to_prefetch_part 28443027 --cache_populated_by_fetch 0 --concurrent_part_removal_threshold 0 --old_parts_lifetime 480"

function test6_insert()
{
    echo "test6 insert"
    $CH_CLIENT -q "insert into test with 'Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))' as type select number, multiIf(number % 6 == 0, CAST(NULL, type), number % 6 == 1, CAST(('str_' || toString(number))::Variant(String), type), number % 6 == 2, CAST(number, type), number % 6 == 3, CAST(('lc_str_' || toString(number))::LowCardinality(String), type), number % 6 == 4, CAST(tuple(number, number + 1)::Tuple(a UInt32, b UInt32), type), CAST(range(number % 20 + 1)::Array(UInt64), type)) as res from numbers(1200000);"
}

function test6_select()
{
    echo "test6 select"
    $CH_CLIENT -nmq "select v from test format Null;
    select count() from test where isNotNull(v);
    select v.String from test format Null;
    select count() from test where isNotNull(v.String);
    select v.UInt64 from test format Null;
    select count() from test where isNotNull(v.UInt64);
    select v.\`LowCardinality(String)\` from test format Null;
    select count() from test where isNotNull(v.\`LowCardinality(String)\`);
    select v.\`Tuple(a UInt32, b UInt32)\` from test format Null;
    select v.\`Tuple(a UInt32, b UInt32)\`.a from test format Null;
    select count() from test where isNotNull(v.\`Tuple(a UInt32, b UInt32)\`.a);
    select v.\`Tuple(a UInt32, b UInt32)\`.b from test format Null;
    select count() from test where isNotNull(v.\`Tuple(a UInt32, b UInt32)\`.b);
    select v.\`Array(UInt64)\` from test format Null;
    select count() from test where not empty(v.\`Array(UInt64)\`);
    select v.\`Array(UInt64)\`.size0 from test format Null;
    select count() from test where isNotNull(v.\`Array(UInt64)\`.size0);"
    echo "-----------------------------------------------------------------------------------------------------------"
}

function run()
{
    test6_insert
    test6_select
    if [ $1 == 1 ]; then
        $CH_CLIENT -q "optimize table test final;"
        test6_select
    fi
    $CH_CLIENT -q "truncate table test;"
}

$CH_CLIENT -q "drop table if exists test;"

echo "Memory"
$CH_CLIENT -q "create table test (id UInt64, v Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))) engine=Memory;"
run 0
$CH_CLIENT -q "drop table test;"

echo "MergeTree compact"
$CH_CLIENT -q "create table test (id UInt64, v Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))) engine=MergeTree order by id settings min_rows_for_wide_part=100000000, min_bytes_for_wide_part=1000000000;"
run 1
$CH_CLIENT -q "drop table test;"

echo "MergeTree wide"
$CH_CLIENT -q "create table test (id UInt64, v Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))) engine=MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1;"
run 1
$CH_CLIENT -q "drop table test;"
