#pragma once

#include "random_generator.h"
#include "sql_grammar.pb.h"

#include <functional>
#include <thread>

namespace chfuzz {

const std::function<void(RandomGenerator&,std::string&)> TrueOrFalse = [](RandomGenerator &rg, std::string &ret) {
	ret += rg.NextBool() ? "1" : "0";
};

const std::function<void(RandomGenerator&,std::string&)> ZeroOneTwo = [](RandomGenerator &rg, std::string &ret) {
	ret += std::to_string(rg.RandomInt<uint32_t>(0, 2));
};

const std::function<void(RandomGenerator&,std::string&)> ZeroToThree = [](RandomGenerator &rg, std::string &ret) {
	ret += std::to_string(rg.RandomInt<uint32_t>(0, 3));
};

const std::map<std::string, std::function<void(RandomGenerator&,std::string&)>> ServerSettings = {
	{"aggregate_functions_null_for_empty", TrueOrFalse},
	{"aggregation_in_order_max_block_bytes", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(0, 50000000));
	}},
	{"allow_experimental_parallel_reading_from_replicas", ZeroOneTwo},
	{"allow_nullable_key", TrueOrFalse},
	{"allow_prefetched_read_pool_for_remote_filesystem", TrueOrFalse},
	{"allow_suspicious_low_cardinality_types", TrueOrFalse},
	{"alter_sync", ZeroOneTwo},
	{"any_join_distinct_right_table_keys", TrueOrFalse},
	{"async_insert", TrueOrFalse},
	{"async_insert_deduplicate", TrueOrFalse},
	{"async_insert_use_adaptive_busy_timeout", TrueOrFalse},
	{"cast_keep_nullable", TrueOrFalse},
	{"check_query_single_value_result", TrueOrFalse},
	{"compile_aggregate_expressions", TrueOrFalse},
	{"compile_sort_description", TrueOrFalse},
	{"cross_join_min_bytes_to_compress", [](RandomGenerator &rg, std::string &ret) {
		const std::vector<std::string> &choices = {"0", "1", "100000000"};
		ret += rg.PickRandomlyFromVector(choices);
	}},
	{"cross_join_min_rows_to_compress", [](RandomGenerator &rg, std::string &ret) {
		const std::vector<std::string> &choices = {"0", "1", "100000000"};
		ret += rg.PickRandomlyFromVector(choices);
	}},
	{"data_type_default_nullable", TrueOrFalse},
	{"deduplicate_blocks_in_dependent_materialized_views", TrueOrFalse},
	{"describe_include_subcolumns", TrueOrFalse},
	{"distributed_aggregation_memory_efficient", TrueOrFalse},
	{"distributed_group_by_no_merge", ZeroOneTwo},
	{"enable_memory_bound_merging_of_aggregation_results", TrueOrFalse},
	{"enable_multiple_prewhere_read_steps", TrueOrFalse},
	{"input_format_import_nested_json", TrueOrFalse},
	{"filesystem_cache_segments_batch_size", [](RandomGenerator &rg, std::string &ret) {
		std::vector<uint32_t> choices{0, 3, 10, 50};
		ret += std::to_string(rg.PickRandomlyFromVector(choices));
	}},
	{"filesystem_prefetch_max_memory_usage", [](RandomGenerator &rg, std::string &ret) {
		const std::vector<std::string> &choices = {"32Mi", "64Mi", "128Mi"};
		ret += "'";
		ret += rg.PickRandomlyFromVector(choices);
		ret += "'";
	}},
	{"filesystem_prefetch_min_bytes_for_single_read_task", [](RandomGenerator &rg, std::string &ret) {
		const std::vector<std::string> &choices = {"1Mi", "8Mi", "16Mi"};
		ret += "'";
		ret += rg.PickRandomlyFromVector(choices);
		ret += "'";
	}},
	{"filesystem_prefetch_step_bytes", [](RandomGenerator &rg, std::string &ret) {
		ret += "'";
		ret += rg.NextBool() ? "0" : "100Mi";
		ret += "'";
	}},
	{"filesystem_prefetch_step_marks", [](RandomGenerator &rg, std::string &ret) {
		ret += rg.NextBool() ? "0" : "50";
	}},
	{"filesystem_prefetches_limit", [](RandomGenerator &rg, std::string &ret) {
		ret += rg.NextBool() ? "0" : "10";
	}},
	{"flatten_nested", TrueOrFalse},
	{"force_optimize_projection", TrueOrFalse},
	{"fsync_metadata", TrueOrFalse},
	{"group_by_two_level_threshold", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.2, 0.2, 1, 1000000));
	}},
	{"group_by_two_level_threshold_bytes", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.2, 0.2, 1, 50000000));
	}},
	{"group_by_use_nulls", TrueOrFalse},
	{"http_response_buffer_size", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(0, 10 * 1048576));
	}},
	{"http_wait_end_of_query", TrueOrFalse},
	{"input_format_parallel_parsing", TrueOrFalse},
	{"insert_null_as_default", TrueOrFalse},
	{"insert_quorum", ZeroOneTwo},
	{"join_algorithm", [](RandomGenerator &rg, std::string &ret) {
		const std::vector<std::string> &choices = {"default", "grace_hash", "hash", "parallel_hash", "partial_merge",
												   "direct", "auto", "full_sorting_merge", "prefer_partial_merge"};
		ret += "'";
		ret += rg.PickRandomlyFromVector(choices);
		ret += "'";
	}},
	{"join_any_take_last_row", TrueOrFalse},
	{"join_default_strictness", [](RandomGenerator &rg, std::string &ret) {
		const std::vector<std::string> &choices = {"ALL", "ANY", "ASOF"}; /*Don't use empty case*/
		ret += "'";
		ret += rg.PickRandomlyFromVector(choices);
		ret += "'";
	}},
	{"join_use_nulls", TrueOrFalse},
	{"lightweight_deletes_sync", ZeroOneTwo},
	{"local_filesystem_read_method", [](RandomGenerator &rg, std::string &ret) {
		const std::vector<std::string> &choices = {"read", "pread", "mmap", "pread_threadpool", "io_uring"};
		ret += "'";
		ret += rg.PickRandomlyFromVector(choices);
		ret += "'";
	}},
	{"local_filesystem_read_prefetch", TrueOrFalse},
	{"log_query_threads", TrueOrFalse},
	{"low_cardinality_use_single_dictionary_for_part", TrueOrFalse},
	{"max_block_size", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(8000, 100000));
	}},
	{"max_bytes_before_external_group_by", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.3, 0.5, 0, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
	}},
	{"max_bytes_before_external_sort", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.3, 0.5, 0, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
	}},
	{"max_bytes_before_remerge_sort", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(1, 3000000000));
	}},
	{"max_compress_block_size", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(1, 1048576 * 3));
	}},
	{"async_insert_threads", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(0, std::thread::hardware_concurrency()));
	}},
	{"max_final_threads", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(0, std::thread::hardware_concurrency()));
	}},
	{"max_insert_threads", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(0, std::thread::hardware_concurrency()));
	}},
	{"max_joined_block_size_rows", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(8000, 100000));
	}},
	{"max_parsing_threads", [](RandomGenerator &rg, std::string &ret) {
		const std::vector<std::string> &choices = {"0", "1", "10"};
		ret += rg.PickRandomlyFromVector(choices);
	}},
	{"max_read_buffer_size", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(500000, 1048576));
	}},
	{"max_threads", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(1, std::thread::hardware_concurrency()));
	}},
	{"merge_tree_coarse_index_granularity", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(2, 32));
	}},
	{"merge_tree_compact_parts_min_granules_to_multibuffer_read", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(1, 128));
	}},
	{"merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(std::ceil(rg.RandomZeroOne() * 100.0) / 100.0);
	}},
	{"min_bytes_to_use_direct_io", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.2, 0.5, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
	}},
	{"min_bytes_to_use_mmap_io", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.2, 0.5, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
	}},
	{"min_chunk_bytes_for_parallel_parsing", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(std::max(1024, static_cast<int>(rg.RandomGauss(10 * 1024 * 1024, 5 * 1000 * 1000))));
	}},
	{"min_compress_block_size", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(1, 1048576 * 3));
	}},
	{"min_count_to_compile_aggregate_expression", ZeroToThree},
	{"min_count_to_compile_expression", ZeroToThree},
	{"min_count_to_compile_sort_description", ZeroToThree},
	{"min_external_table_block_size_bytes", [](RandomGenerator &rg, std::string &ret) {
		const std::vector<std::string> &choices = {"0", "1", "100000000"};
		ret += rg.PickRandomlyFromVector(choices);
	}},
	{"mutations_sync", ZeroOneTwo},
	{"optimize_aggregation_in_order", TrueOrFalse},
	{"optimize_aggregation_in_order", TrueOrFalse},
	{"optimize_append_index", TrueOrFalse},
	{"optimize_distinct_in_order", TrueOrFalse},
	{"optimize_functions_to_subcolumns", TrueOrFalse},
	{"optimize_if_chain_to_multiif", TrueOrFalse},
	{"optimize_if_transform_strings_to_enum", TrueOrFalse},
	{"optimize_move_to_prewhere_if_final", TrueOrFalse},
	{"optimize_or_like_chain", TrueOrFalse},
	{"optimize_read_in_order", TrueOrFalse},
	{"optimize_skip_merged_partitions", TrueOrFalse},
	{"optimize_skip_unused_shards", ZeroOneTwo},
	{"optimize_sorting_by_input_stream_properties", TrueOrFalse},
	{"optimize_substitute_columns", TrueOrFalse},
	{"optimize_syntax_fuse_functions", TrueOrFalse},
	{"optimize_trivial_approximate_count_query", TrueOrFalse},
	{"output_format_parallel_formatting", TrueOrFalse},
	{"page_cache_inject_eviction", TrueOrFalse},
	{"parallel_distributed_insert_select", ZeroOneTwo},
	{"partial_merge_join_optimizations", TrueOrFalse},
	{"precise_float_parsing", TrueOrFalse},
	{"prefer_external_sort_block_bytes", [](RandomGenerator &rg, std::string &ret) {
		const std::vector<std::string> &choices = {"0", "1", "100000000"};
		ret += rg.PickRandomlyFromVector(choices);
	}},
	{"prefer_localhost_replica", TrueOrFalse},
	{"query_plan_aggregation_in_order", TrueOrFalse},
	{"read_from_filesystem_cache_if_exists_otherwise_bypass_cache", TrueOrFalse},
	{"read_in_order_two_level_merge_threshold", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(0, 100));
	}},
	{"remote_filesystem_read_method", [](RandomGenerator &rg, std::string &ret) {
		ret += "'";
		ret += rg.NextBool() ? "read" : "threadpool";
		ret += "'";
	}},
	{"remote_filesystem_read_prefetch", TrueOrFalse},
	{"session_timezone", [](RandomGenerator &rg, std::string &ret) {
		const std::vector<std::string> &choices = {"America/Mazatlan", "America/Hermosillo", "Mexico/BajaSur", "Africa/Khartoum", "Africa/Juba"};
		ret += "'";
		ret += rg.PickRandomlyFromVector(choices);
		ret += "'";
	}},
	{"throw_on_error_from_cache_on_write_operations", TrueOrFalse},
	{"transform_null_in", TrueOrFalse},
	{"ttl_only_drop_parts", TrueOrFalse},
	{"update_insert_deduplication_token_in_dependent_materialized_views", TrueOrFalse},
	{"use_page_cache_for_disks_without_file_cache", TrueOrFalse},
	{"use_structure_from_insertion_table_in_table_functions", ZeroOneTwo},
	{"use_uncompressed_cache", TrueOrFalse}
};

const std::map<std::string, std::function<void(RandomGenerator&,std::string&)>> MergeTreeTableSettings = {
	{"allow_experimental_block_number_column", TrueOrFalse},
	{"allow_floating_point_partition_key", TrueOrFalse},
	{"allow_vertical_merges_from_compact_to_wide_parts", TrueOrFalse},
	{"cache_populated_by_fetch", TrueOrFalse},
	{"compact_parts_max_bytes_to_buffer", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(1024, UINT32_C(512) * UINT32_C(1024) * UINT32_C(1024)));
	}},
	{"compact_parts_max_granules_to_buffer", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.15, 0.15, 1, 256));
	}},
	{"compact_parts_merge_max_bytes_to_prefetch_part", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(1, 32 * 1024 * 1024));
	}},
	{"compress_marks", TrueOrFalse},
	{"compress_primary_key", TrueOrFalse},
	{"deduplicate_merge_projection_mode", [](RandomGenerator &rg, std::string &ret) {
		const std::vector<std::string> &choices = {"throw", "drop", "rebuild"};
		ret += "'";
		ret += rg.PickRandomlyFromVector(choices);
		ret += "'";
	}},
	{"detach_not_byte_identical_parts", TrueOrFalse},
	{"exclude_deleted_rows_for_part_size_in_merge", TrueOrFalse},
	{"index_granularity_bytes", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(1024, 30 * 1024 * 1024));
	}},
	{"lightweight_mutation_projection_mode", [](RandomGenerator &rg, std::string &ret) {
		const std::vector<std::string> &choices = {"throw", "drop", "rebuild"};
		ret += "'";
		ret += rg.PickRandomlyFromVector(choices);
		ret += "'";
	}},
	{"load_existing_rows_count_for_old_parts", TrueOrFalse},
	{"marks_compress_block_size", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(8000, 100000));
	}},
	{"max_file_name_length", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.3, 0.3, 0, 128));
	}},
	{"merge_max_block_size", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(1, 65536));
	}},
	{"merge_max_block_size", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(1, 8192 * 3));
	}},
	{"min_bytes_for_full_part_storage", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.3, 0.3, 0, 512 * 1024 * 1024));
	}},
	{"min_bytes_for_wide_part", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.3, 0.3, 0, UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
	}},
	{"min_merge_bytes_to_use_direct_io", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.25, 0.25, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
	}},
	{"min_rows_for_wide_part", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.3, 0.3, 0, UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
	}},
	{"old_parts_lifetime", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.2, 0.3, 10, 8 * 60));
	}},
	{"prefer_fetch_merged_part_size_threshold", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.2, 0.5, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
	}},
	{"primary_key_compress_block_size", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(8000, 100000));
	}},
	{"ratio_of_defaults_for_sparse_serialization", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<double>(0.3, 0.5, 0.0, 1.0));
	}},
	{"replace_long_file_name_to_hash", TrueOrFalse},
	{"use_async_block_ids_cache", TrueOrFalse}
};

const std::map<std::string, std::function<void(RandomGenerator&,std::string&)>> MergeTreeColumnSettings = {
	{"min_compress_block_size", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(1, 1048576 * 3));
	}},
	{"max_compress_block_size", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(1, 1048576 * 3));
	}}
};

void SetRandomSetting(RandomGenerator &rg, const std::map<std::string, std::function<void(RandomGenerator&,std::string&)>> &settings,
					  std::string &ret, sql_query_grammar::SetValue *set);

}
