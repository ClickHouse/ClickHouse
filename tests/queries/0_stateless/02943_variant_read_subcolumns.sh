#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# reset --log_comment
CLICKHOUSE_LOG_COMMENT=
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_variant_type=1 --use_variant_as_common_type=1 --allow_suspicious_variant_types=1 --max_insert_threads 4 --group_by_two_level_threshold 752249 --group_by_two_level_threshold_bytes 15083870 --distributed_aggregation_memory_efficient 1 --fsync_metadata 1 --output_format_parallel_formatting 0 --input_format_parallel_parsing 0 --min_chunk_bytes_for_parallel_parsing 6583861 --max_read_buffer_size 640584 --prefer_localhost_replica 1 --max_block_size 38844 --max_threads 48 --optimize_append_index 0 --optimize_if_chain_to_multiif 1 --optimize_if_transform_strings_to_enum 0 --optimize_read_in_order 1 --optimize_or_like_chain 0 --optimize_substitute_columns 1 --enable_multiple_prewhere_read_steps 1 --read_in_order_two_level_merge_threshold 4 --optimize_aggregation_in_order 0 --aggregation_in_order_max_block_bytes 18284646 --use_uncompressed_cache 1 --min_bytes_to_use_direct_io 10737418240 --min_bytes_to_use_mmap_io 10737418240 --local_filesystem_read_method pread --remote_filesystem_read_method read --local_filesystem_read_prefetch 1 --filesystem_cache_segments_batch_size 0 --read_from_filesystem_cache_if_exists_otherwise_bypass_cache 0 --throw_on_error_from_cache_on_write_operations 1 --remote_filesystem_read_prefetch 0 --allow_prefetched_read_pool_for_remote_filesystem 0 --filesystem_prefetch_max_memory_usage 128Mi --filesystem_prefetches_limit 0 --filesystem_prefetch_min_bytes_for_single_read_task 16Mi --filesystem_prefetch_step_marks 50 --filesystem_prefetch_step_bytes 0 --compile_aggregate_expressions 1 --compile_sort_description 0 --merge_tree_coarse_index_granularity 31 --optimize_distinct_in_order 1 --max_bytes_before_external_sort 1 --max_bytes_before_external_group_by 1 --max_bytes_before_remerge_sort 2640239625 --min_compress_block_size 3114155 --max_compress_block_size 226550 --merge_tree_compact_parts_min_granules_to_multibuffer_read 118 --optimize_sorting_by_input_stream_properties 0 --http_response_buffer_size 543038 --http_wait_end_of_query False --enable_memory_bound_merging_of_aggregation_results 1 --min_count_to_compile_expression 3 --min_count_to_compile_aggregate_expression 3 --min_count_to_compile_sort_description 0 --session_timezone America/Mazatlan --prefer_warmed_unmerged_parts_seconds 8 --use_page_cache_for_disks_without_file_cache False --page_cache_inject_eviction True --merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability 0.82 "


function test()
{
    echo "test"
    $CH_CLIENT -q "insert into test select number, multiIf(number % 3 == 2, NULL, number % 3 == 1, number, arrayMap(x -> multiIf(number % 9 == 0, NULL, number % 9 == 3, 'str_' || toString(number), number), range(number % 10))) from numbers(1000000) settings min_insert_block_size_rows=100000"
    $CH_CLIENT -q "select v, v.UInt64, v.\`Array(Variant(String, UInt64))\`,  v.\`Array(Variant(String, UInt64))\`.size0, v.\`Array(Variant(String, UInt64))\`.UInt64 from test order by id format Null"
    $CH_CLIENT -q "select v.UInt64, v.\`Array(Variant(String, UInt64))\`,  v.\`Array(Variant(String, UInt64))\`.size0, v.\`Array(Variant(String, UInt64))\`.UInt64 from test order by id format Null"
    $CH_CLIENT -q "select v.\`Array(Variant(String, UInt64))\`,  v.\`Array(Variant(String, UInt64))\`.size0, v.\`Array(Variant(String, UInt64))\`.UInt64, v.\`Array(Variant(String, UInt64))\`.String from test order by id format Null"
}

$CH_CLIENT -q "drop table if exists test;"

echo "Memory"
$CH_CLIENT -q "create table test (id UInt64, v Variant(UInt64, Array(Variant(String, UInt64)))) engine=Memory"
test
$CH_CLIENT -q "drop table test;"

echo "MergeTree compact"
$CH_CLIENT -q "create table test (id UInt64, v Variant(UInt64, Array(Variant(String, UInt64)))) engine=MergeTree order by id settings min_rows_for_wide_part=1000000000, min_bytes_for_wide_part=10000000000;"
test
$CH_CLIENT -q "drop table test;"

echo "MergeTree wide"
$CH_CLIENT -q "create table test (id UInt64, v Variant(UInt64, Array(Variant(String, UInt64)))) engine=MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1;"
test
$CH_CLIENT -q "drop table test;"

