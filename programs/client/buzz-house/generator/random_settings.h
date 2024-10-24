#pragma once

#include "random_generator.h"
#include "buzz-house/ast/sql_grammar.pb.h"

#include <cstdint>
#include <functional>
#include <thread>

namespace buzzhouse {

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
		ret += std::to_string(rg.RandomInt<uint32_t>(0, 500000));
	}},
	{"allow_experimental_parallel_reading_from_replicas", ZeroOneTwo},
	{"allow_experimental_shared_set_join", TrueOrFalse},
	{"allow_nullable_key", TrueOrFalse},
	{"allow_prefetched_read_pool_for_remote_filesystem", TrueOrFalse},
	{"allow_suspicious_low_cardinality_types", TrueOrFalse},
	{"alter_sync", ZeroOneTwo},
	{"any_join_distinct_right_table_keys", TrueOrFalse},
	{"async_insert", TrueOrFalse},
	{"async_insert_deduplicate", TrueOrFalse},
	{"async_insert_threads", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(0, std::thread::hardware_concurrency()));
	}},
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
	{"deduplicate_blocks_in_dependent_materialized_views", TrueOrFalse},
	{"describe_include_subcolumns", TrueOrFalse},
	{"distributed_aggregation_memory_efficient", TrueOrFalse},
	{"distributed_group_by_no_merge", ZeroOneTwo},
	{"enable_analyzer", TrueOrFalse},
	{"enable_deflate_qpl_codec", TrueOrFalse},
	{"enable_early_constant_folding", TrueOrFalse},
	{"enable_extended_results_for_datetime_functions", TrueOrFalse},
	{"enable_http_compression", TrueOrFalse},
	{"enable_memory_bound_merging_of_aggregation_results", TrueOrFalse},
	{"enable_multiple_prewhere_read_steps", TrueOrFalse},
	{"enable_named_columns_in_function_tuple", TrueOrFalse},
	{"enable_optimize_predicate_expression", TrueOrFalse},
	{"enable_optimize_predicate_expression_to_final_subquery", TrueOrFalse},
	{"enable_parallel_replicas", TrueOrFalse},
	{"enable_scalar_subquery_optimization", TrueOrFalse},
	{"enable_sharing_sets_for_mutations", TrueOrFalse},
	{"enable_software_prefetch_in_aggregation", TrueOrFalse},
	{"enable_unaligned_array_join", TrueOrFalse},
	{"enable_vertical_final", TrueOrFalse},
	{"exact_rows_before_limit", TrueOrFalse},
	{"input_format_csv_try_infer_numbers_from_strings", TrueOrFalse},
	{"input_format_import_nested_json", TrueOrFalse},
	{"input_format_json_empty_as_default", TrueOrFalse},
	{"input_format_try_infer_variants", TrueOrFalse},
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
	{"group_by_overflow_mode", [](RandomGenerator &rg, std::string &ret) {
		const std::vector<std::string> &choices = {"throw", "break", "any"};
		ret += "'";
		ret += rg.PickRandomlyFromVector(choices);
		ret += "'";
	}},
	{"group_by_two_level_threshold", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.2, 0.2, 1, 100000));
	}},
	{"group_by_two_level_threshold_bytes", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.2, 0.2, 1, 500000));
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
	{"join_overflow_mode", [](RandomGenerator &rg, std::string &ret) {
		ret += "'";
		ret += rg.NextBool() ? "throw" : "break";
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
	{"low_cardinality_allow_in_native_format", TrueOrFalse},
	{"low_cardinality_max_dictionary_size", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(UINT32_C(1) << (rg.NextLargeNumber() % 18));
	}},
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
		ret += std::to_string(rg.RandomInt<uint32_t>(1, 300000));
	}},
	{"max_compress_block_size", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(1, 1048576 * 3));
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
	{"max_parts_to_move", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.2, 0.5, 1, UINT32_C(4096)));
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
	{"optimize_aggregators_of_group_by_keys", TrueOrFalse},
	{"optimize_append_index", TrueOrFalse},
	{"optimize_arithmetic_operations_in_aggregate_functions", TrueOrFalse},
	{"optimize_count_from_files", TrueOrFalse},
	{"optimize_distinct_in_order", TrueOrFalse},
	{"optimize_group_by_constant_keys", TrueOrFalse},
	{"optimize_group_by_function_keys", TrueOrFalse},
	{"optimize_functions_to_subcolumns", TrueOrFalse},
	{"optimize_if_chain_to_multiif", TrueOrFalse},
	{"optimize_if_transform_strings_to_enum", TrueOrFalse},
	{"optimize_injective_functions_in_group_by", TrueOrFalse},
	{"optimize_injective_functions_inside_uniq", TrueOrFalse},
	{"optimize_move_to_prewhere_if_final", TrueOrFalse},
	{"optimize_multiif_to_if", TrueOrFalse},
	{"optimize_normalize_count_variants", TrueOrFalse},
	{"optimize_on_insert", TrueOrFalse},
	{"optimize_or_like_chain", TrueOrFalse},
	{"optimize_read_in_order", TrueOrFalse},
	{"optimize_redundant_functions_in_order_by", TrueOrFalse},
	{"optimize_rewrite_aggregate_function_with_if", TrueOrFalse},
	{"optimize_rewrite_array_exists_to_has", TrueOrFalse},
	{"optimize_rewrite_sum_if_to_count_if", TrueOrFalse},
	{"optimize_skip_merged_partitions", TrueOrFalse},
	{"optimize_skip_unused_shards", TrueOrFalse},
	{"optimize_sorting_by_input_stream_properties", TrueOrFalse},
	{"optimize_substitute_columns", TrueOrFalse},
	{"optimize_syntax_fuse_functions", TrueOrFalse},
	{"optimize_time_filter_with_preimage", TrueOrFalse},
	{"optimize_trivial_approximate_count_query", TrueOrFalse},
	{"optimize_trivial_count_query", TrueOrFalse},
	{"optimize_trivial_insert_select", TrueOrFalse},
	{"optimize_uniq_to_count", TrueOrFalse},
	{"optimize_use_implicit_projections", TrueOrFalse},
	{"optimize_use_projections", TrueOrFalse},
	{"optimize_using_constraints", TrueOrFalse},
	{"output_format_arrow_fixed_string_as_fixed_byte_array", TrueOrFalse},
	{"output_format_arrow_low_cardinality_as_dictionary", TrueOrFalse},
	{"output_format_arrow_string_as_string", TrueOrFalse},
	{"output_format_arrow_use_64_bit_indexes_for_dictionary", TrueOrFalse},
	{"output_format_arrow_use_signed_indexes_for_dictionary", TrueOrFalse},
	{"output_format_binary_encode_types_in_binary_format", TrueOrFalse},
	{"output_format_binary_write_json_as_string", TrueOrFalse},
	{"output_format_bson_string_as_string", TrueOrFalse},
	{"output_format_csv_crlf_end_of_line", TrueOrFalse},
	{"output_format_csv_serialize_tuple_into_separate_columns", TrueOrFalse},
	{"output_format_decimal_trailing_zeros", TrueOrFalse},
	{"output_format_enable_streaming", TrueOrFalse},
	{"output_format_json_array_of_rows", TrueOrFalse},
	{"output_format_json_escape_forward_slashes", TrueOrFalse},
	{"output_format_json_named_tuples_as_objects", TrueOrFalse},
	{"output_format_json_quote_64bit_floats", TrueOrFalse},
	{"output_format_json_quote_64bit_integers", TrueOrFalse},
	{"output_format_json_quote_decimals", TrueOrFalse},
	{"output_format_json_quote_denormals", TrueOrFalse},
	{"output_format_json_skip_null_value_in_named_tuples", TrueOrFalse},
	{"output_format_json_validate_utf8", TrueOrFalse},
	{"output_format_markdown_escape_special_characters", TrueOrFalse},
	{"output_format_native_encode_types_in_binary_format", TrueOrFalse},
	{"output_format_native_write_json_as_string", TrueOrFalse},
	{"output_format_orc_string_as_string", TrueOrFalse},
	{"output_format_parallel_formatting", TrueOrFalse},
	{"output_format_parquet_compliant_nested_types", TrueOrFalse},
	{"output_format_parquet_fixed_string_as_fixed_byte_array", TrueOrFalse},
	{"output_format_parquet_parallel_encoding", TrueOrFalse},
	{"output_format_parquet_string_as_string", TrueOrFalse},
	{"output_format_parquet_use_custom_encoder", TrueOrFalse},
	{"output_format_parquet_write_page_index", TrueOrFalse},
	{"output_format_pretty_highlight_digit_groups", TrueOrFalse},
	{"output_format_pretty_row_numbers", TrueOrFalse},
	{"output_format_protobuf_nullables_with_google_wrappers", TrueOrFalse},
	{"output_format_sql_insert_include_column_names", TrueOrFalse},
	{"output_format_sql_insert_quote_names", TrueOrFalse},
	{"output_format_sql_insert_use_replace", TrueOrFalse},
	{"output_format_tsv_crlf_end_of_line", TrueOrFalse},
	{"output_format_values_escape_quote_with_quote", TrueOrFalse},
	{"output_format_write_statistics", TrueOrFalse},
	{"page_cache_inject_eviction", TrueOrFalse},
	{"parallel_distributed_insert_select", ZeroOneTwo},
	{"parallel_replicas_local_plan", TrueOrFalse},
	{"partial_merge_join_optimizations", TrueOrFalse},
	{"precise_float_parsing", TrueOrFalse},
	{"prefer_external_sort_block_bytes", [](RandomGenerator &rg, std::string &ret) {
		const std::vector<std::string> &choices = {"0", "1", "100000000"};
		ret += rg.PickRandomlyFromVector(choices);
	}},
	{"prefer_localhost_replica", TrueOrFalse},
	{"prefer_merge_sort_block_bytes", [](RandomGenerator &rg, std::string &ret) {
		const std::vector<std::string> &choices = {"0", "1", "100000000"};
		ret += rg.PickRandomlyFromVector(choices);
	}},
	{"query_plan_aggregation_in_order", TrueOrFalse},
	{"query_plan_enable_optimizations", TrueOrFalse},
	{"read_from_filesystem_cache_if_exists_otherwise_bypass_cache", TrueOrFalse},
	{"read_in_order_two_level_merge_threshold", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(0, 100));
	}},
	{"read_in_order_use_buffering", TrueOrFalse},
	{"remote_filesystem_read_method", [](RandomGenerator &rg, std::string &ret) {
		ret += "'";
		ret += rg.NextBool() ? "read" : "threadpool";
		ret += "'";
	}},
	{"remote_filesystem_read_prefetch", TrueOrFalse},
	{"rows_before_aggregation", TrueOrFalse},
	{"session_timezone", [](RandomGenerator &rg, std::string &ret) {
		const std::vector<std::string> &choices = {"America/Mazatlan", "America/Hermosillo", "Mexico/BajaSur", "Africa/Khartoum", "Africa/Juba"};
		ret += "'";
		ret += rg.PickRandomlyFromVector(choices);
		ret += "'";
	}},
	{"throw_on_error_from_cache_on_write_operations", TrueOrFalse},
	{"totals_mode", [](RandomGenerator &rg, std::string &ret) {
		const std::vector<std::string> &choices = {"before_having", "after_having_exclusive", "after_having_inclusive", "after_having_auto"};
		ret += "'";
		ret += rg.PickRandomlyFromVector(choices);
		ret += "'";
	}},
	{"transform_null_in", TrueOrFalse},
	{"type_json_skip_duplicated_paths", TrueOrFalse},
	{"update_insert_deduplication_token_in_dependent_materialized_views", TrueOrFalse},
	{"use_json_alias_for_old_object_type", TrueOrFalse},
	{"use_page_cache_for_disks_without_file_cache", TrueOrFalse},
	{"use_skip_indexes", TrueOrFalse},
	{"use_structure_from_insertion_table_in_table_functions", ZeroOneTwo},
	{"use_uncompressed_cache", TrueOrFalse},
	{"use_variant_as_common_type", TrueOrFalse}
};

const std::map<std::string, std::function<void(RandomGenerator&,std::string&)>> MergeTreeTableSettings = {
	{"adaptive_write_buffer_initial_size", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(1, 32 * 1024 * 1024));
	}},
	{"allow_experimental_block_number_column", TrueOrFalse},
	{"allow_experimental_replacing_merge_with_cleanup", TrueOrFalse},
	{"allow_floating_point_partition_key", TrueOrFalse},
	{"allow_remote_fs_zero_copy_replication", TrueOrFalse},
	{"allow_suspicious_indices", TrueOrFalse},
	{"allow_vertical_merges_from_compact_to_wide_parts", TrueOrFalse},
	{"always_use_copy_instead_of_hardlinks", TrueOrFalse},
	{"assign_part_uuids", TrueOrFalse},
	{"cache_populated_by_fetch", TrueOrFalse},
	{"check_sample_column_is_correct", TrueOrFalse},
	{"compact_parts_max_bytes_to_buffer", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(1024, UINT32_C(512) * UINT32_C(1024) * UINT32_C(1024)));
	}},
	{"compact_parts_max_granules_to_buffer", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.15, 0.15, 1, 256));
	}},
	{"compact_parts_merge_max_bytes_to_prefetch_part", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(1, 32 * 1024 * 1024));
	}},
	{"compatibility_allow_sampling_expression_not_in_primary_key", TrueOrFalse},
	{"compress_marks", TrueOrFalse},
	{"compress_primary_key", TrueOrFalse},
	{"concurrent_part_removal_threshold", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.2, 0.3, 0, 100));
	}},
	{"deduplicate_merge_projection_mode", [](RandomGenerator &rg, std::string &ret) {
		const std::vector<std::string> &choices = {"throw", "drop", "rebuild"};
		ret += "'";
		ret += rg.PickRandomlyFromVector(choices);
		ret += "'";
	}},
	{"detach_not_byte_identical_parts", TrueOrFalse},
	{"disable_freeze_partition_for_zero_copy_replication", TrueOrFalse},
	{"disable_detach_partition_for_zero_copy_replication", TrueOrFalse},
	{"disable_fetch_partition_for_zero_copy_replication", TrueOrFalse},
	{"enable_block_number_column", TrueOrFalse},
	{"enable_block_offset_column", TrueOrFalse},
	{"enable_mixed_granularity_parts", TrueOrFalse},
	{"enable_vertical_merge_algorithm", TrueOrFalse},
	{"exclude_deleted_rows_for_part_size_in_merge", TrueOrFalse},
	{"force_read_through_cache_for_merges", TrueOrFalse},
	{"fsync_after_insert", TrueOrFalse},
	{"fsync_part_directory", TrueOrFalse},
	{"index_granularity", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(UINT32_C(1) << (rg.NextLargeNumber() % 18));
	}},
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
	{"max_bytes_to_merge_at_max_space_in_pool", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.25, 0.25, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
	}},
	{"max_bytes_to_merge_at_min_space_in_pool", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.25, 0.25, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
	}},
	{"max_file_name_length", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.3, 0.3, 0, 128));
	}},
	{"max_number_of_mutations_for_replica", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.2, 0.3, 1, 100));
	}},
	{"max_parts_to_merge_at_once", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.2, 0.3, 0, 1000));
	}},
	{"max_replicated_merges_in_queue", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.3, 0.3, 1, 100));
	}},
	{"max_replicated_mutations_in_queue", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.3, 0.3, 1, 100));
	}},
	{"merge_max_block_size", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(1, 8192 * 3));
	}},
	{"merge_max_block_size_bytes", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.3, 0.3, 0, 128 * 1024 * 1024));
	}},
	{"min_age_to_force_merge_on_partition_only", TrueOrFalse},
	{"min_bytes_for_full_part_storage", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.3, 0.3, 0, 512 * 1024 * 1024));
	}},
	{"min_bytes_for_wide_part", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.3, 0.3, 0, UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
	}},
	{"min_compressed_bytes_to_fsync_after_fetch", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.3, 0.3, 0, 128 * 1024 * 1024));
	}},
	{"min_compressed_bytes_to_fsync_after_merge", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.3, 0.3, 0, 128 * 1024 * 1024));
	}},
	{"min_merge_bytes_to_use_direct_io", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.25, 0.25, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
	}},
	{"min_rows_for_full_part_storage", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.3, 0.3, 0, 1000));
	}},
	{"min_rows_to_fsync_after_merge", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.3, 0.3, 0, 1000));
	}},
	{"min_rows_for_wide_part", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.3, 0.3, 0, 1000));
	}},
	{"non_replicated_deduplication_window", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.3, 0.3, 0, 1000));
	}},
	{"number_of_free_entries_in_pool_to_lower_max_size_of_merge", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.2, 0.3, 1, 100));
	}},
	{"number_of_free_entries_in_pool_to_execute_mutation", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.2, 0.3, 1, 100));
	}},
	{"number_of_free_entries_in_pool_to_execute_optimize_entire_partition", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.2, 0.3, 1, 100));
	}},
	{"old_parts_lifetime", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.2, 0.3, 10, 8 * 60));
	}},
	{"optimize_row_order", TrueOrFalse},
	{"prefer_fetch_merged_part_size_threshold", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.2, 0.5, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
	}},
	{"primary_key_compress_block_size", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.RandomInt<uint32_t>(8000, 100000));
	}},
	{"primary_key_lazy_load", TrueOrFalse},
	{"primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<double>(0.3, 0.5, 0.0, 1.0));
	}},
	{"ratio_of_defaults_for_sparse_serialization", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<double>(0.3, 0.5, 0.0, 1.0));
	}},
	{"remote_fs_zero_copy_path_compatible_mode", TrueOrFalse},
	{"remove_empty_parts", TrueOrFalse},
	{"remove_rolled_back_parts_immediately", TrueOrFalse},
	{"replace_long_file_name_to_hash", TrueOrFalse},
	{"replicated_max_mutations_in_one_entry", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.2, 0.3, 0, 10000));
	}},
	{"ttl_only_drop_parts", TrueOrFalse},
	{"use_adaptive_write_buffer_for_dynamic_subcolumns", TrueOrFalse},
	{"use_async_block_ids_cache", TrueOrFalse},
	{"use_compact_variant_discriminators_serialization", TrueOrFalse},
	{"vertical_merge_algorithm_min_bytes_to_activate", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.4, 0.4, 1, 10000));
	}},
	{"vertical_merge_algorithm_min_columns_to_activate", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.4, 0.4, 1, 100));
	}},
	{"vertical_merge_algorithm_min_rows_to_activate", [](RandomGenerator &rg, std::string &ret) {
		ret += std::to_string(rg.ThresholdGenerator<uint32_t>(0.4, 0.4, 1, 10000));
	}}
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
