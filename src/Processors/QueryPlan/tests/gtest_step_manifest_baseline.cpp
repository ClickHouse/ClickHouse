#include <gtest/gtest.h>

#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/StepManifest.h>

using namespace DB;

/// The declarations of every step manifest, in the canonical form of `describeManifest`. A change
/// here is a change of what a step puts on the wire in the framed format. On master a new line is
/// fine when it belongs to a new step name; an existing line must not change, because readers inside
/// the support window depend on it. On a release branch nothing here may change at all. Update the
/// text below only after that review.
static const char * expected_manifests = R"MANIFESTS(name Aggregating introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field keys Logical vector<String>
  field aggregates Logical AggregateDescriptions
  field grouping_sets Logical vector<vector<String>>
  field final Logical no-cache-key bool
  field overflow_row Logical bool
  field group_by_use_nulls Logical bool
  field only_merge Logical bool
  field sort_description_for_merging Logical SortDescription
  field group_by_sort_description Logical SortDescription
  field explicit_sorting_required_for_aggregation_in_order Physical bool
  field hash_table_stats_key Physical UInt64
  setting max_block_size Physical UInt64
  setting aggregation_in_order_max_block_bytes Physical UInt64
  setting aggregation_sort_result_by_bucket_number Physical bool
  setting aggregation_in_order_memory_bound_merging Physical bool
  setting max_rows_to_group_by Logical UInt64
  setting group_by_overflow_mode Logical enum8
  setting group_by_two_level_threshold Physical UInt64
  setting group_by_two_level_threshold_bytes Physical UInt64
  setting max_bytes_before_external_group_by Physical UInt64
  setting empty_result_for_aggregation_by_empty_set Logical bool
  setting min_free_disk_space_for_temporary_data Physical UInt64
  setting compile_aggregate_expressions Physical bool
  setting min_count_to_compile_aggregate_expression Physical UInt64
  setting enable_software_prefetch_in_aggregation Physical bool
  setting optimize_group_by_constant_keys Physical bool
  setting min_hit_rate_to_use_consecutive_keys_optimization Physical Float32
  setting collect_hash_table_stats_during_aggregation Physical bool
  setting max_entries_for_hash_table_stats Physical UInt64
  setting max_size_to_preallocate_for_aggregation Physical UInt64
  setting enable_producing_buckets_out_of_order_in_aggregation Physical bool
  setting enable_parallel_single_level_merge Physical bool
  setting enable_adaptive_aggregator Physical bool
  setting adaptive_aggregator_freeze_threshold Physical UInt64
  setting adaptive_aggregator_freeze_threshold_bytes Physical UInt64
  setting serialize_string_in_memory_with_zero_byte Physical bool
  setting enable_packed_string_keys_in_aggregation Physical bool
  initializers 0000000000000000000000
name ArrayJoin introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field columns Logical vector<String>
  field is_left Logical bool
  field is_unaligned Logical bool
  field enable_lazy_columns_replication Physical bool
  field element_filter Logical optional<ActionsDAG>
  field element_filter_column_name Logical String
  field remove_element_filter_column Logical bool
  setting max_block_size Physical UInt64
  initializers 00000000000000
name BroadcastReceive introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field exchange_id Logical String
  field source_shards Physical vector<String>
  initializers 0000
name BroadcastSend introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field exchange_id Logical String
  field num_buckets Physical UInt64
  initializers 0000
name BuildRuntimeFilter introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field filter_column_name Logical String
  field filter_column_type Logical String
  field filter_name Logical String
  field allow_to_use_not_exact_filter Logical bool
  setting join_runtime_filter_exact_values_limit Physical UInt64
  setting join_runtime_bloom_filter_bytes Physical UInt64
  setting join_runtime_bloom_filter_hash_functions Physical UInt64
  setting join_runtime_filter_pass_ratio_threshold_for_disabling Physical Float64
  setting join_runtime_filter_blocks_to_skip_before_reenabling Physical UInt64
  setting join_runtime_bloom_filter_max_ratio_of_set_bits Physical Float64
  initializers 00000000
name Cube introduced_in 9 full_digest always logical_digest always
format 1 introduced_in 16
  field keys Logical vector<String>
  field aggregates Logical AggregateDescriptionsWithoutArguments
  field final Logical bool
  field overflow_row Logical bool
  field use_nulls Logical bool
  setting max_block_size Physical UInt64
  setting min_hit_rate_to_use_consecutive_keys_optimization Physical Float32
  setting serialize_string_in_memory_with_zero_byte Physical bool
  setting enable_packed_string_keys_in_aggregation Physical bool
  initializers 0000000000
name Distinct introduced_in 1 full_digest always logical_digest predicate
format 1 introduced_in 16
  field columns Logical vector<String>
  field limit_hint Logical UInt64
  field distinct_sort_desc Logical SortDescription
  field skip_stream_merging Physical bool
  setting max_rows_in_distinct Logical UInt64
  setting max_bytes_in_distinct Logical UInt64
  setting distinct_overflow_mode Logical enum8
  initializers 00000000
name Expression introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field actions_dag Logical ActionsDAG
  initializers 000000
name Extremes introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  initializers 
name Filter introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field actions_dag Logical ActionsDAG
  field filter_column_name Logical String
  field remove_filter_column Logical bool
  field condition Physical optional<pair<UInt64,String>>
  initializers 000000000000
name FractionalLimit introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field limit_fraction Logical Float64
  field offset_fraction Logical Float64
  field offset Logical UInt64
  field with_ties Logical bool
  field description Logical SortDescription
  initializers 00000000000000000000000000000000000000
name FractionalOffset introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field fractional_offset Logical Float64
  initializers 0000000000000000
name GatherReceive introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field exchange_id Logical String
  field num_buckets Physical UInt64
  field maintain_sort_description Logical optional<SortDescription>
  initializers 000000
name GatherSend introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field exchange_id Logical String
  field maintain_sort_description Logical optional<SortDescription>
  initializers 0000
name IntersectOrExcept introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field operator Logical enum8
  initializers 00
name Join introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field join Logical JoinExpressions
  setting join_algorithm Physical vector<enum8>
  setting max_block_size Physical UInt64
  setting max_rows_in_join Logical UInt64
  setting max_bytes_in_join Logical UInt64
  setting default_max_bytes_in_join Logical UInt64
  setting max_joined_block_size_rows Physical UInt64
  setting max_joined_block_size_bytes Physical UInt64
  setting min_joined_block_size_rows Physical UInt64
  setting min_joined_block_size_bytes Physical UInt64
  setting joined_block_split_single_row Physical bool
  setting parallel_non_joined_rows_processing Physical bool
  setting join_overflow_mode Logical enum8
  setting join_any_take_last_row Logical bool
  setting cross_join_min_rows_to_compress Physical UInt64
  setting cross_join_min_bytes_to_compress Physical UInt64
  setting partial_merge_join_left_table_buffer_bytes Physical UInt64
  setting partial_merge_join_rows_in_right_blocks Physical UInt64
  setting join_on_disk_max_files_to_merge Physical UInt64
  setting grace_hash_join_initial_buckets Physical UInt64
  setting grace_hash_join_max_buckets Physical UInt64
  setting max_bytes_before_external_join Physical UInt64
  setting max_bytes_ratio_before_external_join Physical Float64
  setting max_rows_in_set_to_optimize_join Logical UInt64
  setting temporary_files_codec Physical String
  setting temporary_files_buffer_size Physical UInt64
  setting collect_hash_table_stats_during_joins Physical bool
  setting max_size_to_preallocate_for_joins Physical UInt64
  setting parallel_hash_join_threshold Physical UInt64
  setting join_output_by_rowlist_perkey_rows_threshold Physical UInt64
  setting allow_experimental_join_right_table_sorting Physical bool
  setting join_to_sort_minimum_perkey_rows Physical UInt64
  setting join_to_sort_maximum_table_rows Physical UInt64
  setting allow_dynamic_type_in_join_keys Physical bool
  setting use_join_disjunctions_push_down Physical bool
  setting enable_lazy_columns_replication Physical bool
  setting enable_software_prefetch_in_join Physical bool
  setting use_hash_table_stats_for_join_reordering Physical bool
  setting enable_hash_join_row_store Physical bool
  setting min_rows_ratio_for_hash_join_row_store Physical Float64
  setting enable_join_fixed_hash_table_conversion Physical bool
  setting join_runtime_filter_from_fixed_hash_table Physical bool
  setting max_rows_to_sort Logical UInt64
  setting max_bytes_to_sort Logical UInt64
  setting sort_overflow_mode Logical enum8
  setting max_bytes_before_remerge_sort Physical UInt64
  setting remerge_sort_lowered_memory_bytes_ratio Physical Float32
  setting max_bytes_before_external_sort Physical UInt64
  setting max_bytes_ratio_before_external_sort Physical Float64
  setting min_free_disk_space_for_temporary_data Physical UInt64
  setting prefer_external_sort_block_bytes Physical UInt64
  initializers 000000000004030000
name Limit introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field limit Logical UInt64
  field offset Logical UInt64
  field always_read_till_end Logical bool
  field with_ties Logical bool
  field description Logical SortDescription
  field is_shard_limit Logical bool
  initializers 000000000000
name LimitBy introduced_in 1 full_digest always logical_digest predicate
format 1 introduced_in 16
  field group_length Logical UInt64
  field group_offset Logical UInt64
  field columns Logical vector<String>
  field sorted_columns_descr Logical SortDescription
  field skip_stream_merging Physical bool
  initializers 0000000000
name LimitRange introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field conditions Logical ActionsDAG
  field start_column_name Logical optional<String>
  field end_column_name Logical optional<String>
  field start_all Logical bool
  field limit Logical optional<UInt64>
  field always_read_till_end Logical bool
  initializers 0000000000000000
name MergingAggregated introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field keys Logical vector<String>
  field aggregates Logical AggregateDescriptions
  field grouping_sets Logical vector<vector<String>>
  field final Logical bool
  field overflow_row Logical bool
  field group_by_sort_description Logical SortDescription
  field should_produce_results_in_order_of_bucket_number Physical bool
  field memory_bound_merging_of_aggregation_results_enabled Physical bool
  setting max_block_size Physical UInt64
  setting aggregation_in_order_max_block_bytes Physical UInt64
  setting min_hit_rate_to_use_consecutive_keys_optimization Physical Float32
  setting distributed_aggregation_memory_efficient Physical bool
  setting serialize_string_in_memory_with_zero_byte Physical bool
  setting enable_packed_string_keys_in_aggregation Physical bool
  initializers 0000000000000000
name NegativeLimit introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field limit Logical UInt64
  field offset Logical UInt64
  field with_ties Logical bool
  field description Logical SortDescription
  field is_shard_limit Logical bool
  initializers 0000000000
name NegativeLimitBy introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field group_length Logical UInt64
  field group_offset Logical UInt64
  field columns Logical vector<String>
  field sorted_columns_descr Logical SortDescription
  initializers 00000000
name NegativeOffset introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field offset Logical UInt64
  initializers 00
name ObjectFilter introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field actions_dag Logical ActionsDAG
  field filter_column_name Logical String
  initializers 00000000
name Offset introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field offset Logical UInt64
  initializers 00
name PreDistinct introduced_in 1 full_digest always logical_digest predicate
format 1 introduced_in 16
  field columns Logical vector<String>
  field limit_hint Logical UInt64
  field distinct_sort_desc Logical SortDescription
  field skip_stream_merging Physical bool
  setting max_rows_in_distinct Logical UInt64
  setting max_bytes_in_distinct Logical UInt64
  setting distinct_overflow_mode Logical enum8
  initializers 00000000
name ReadFromMergeTree introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field database Logical String
  field table Logical String
  field columns Logical vector<String>
  field max_block_size Physical UInt64
  field num_streams Physical UInt64
  field final Logical bool
  field sample_size_ratio Logical optional<Rational>
  field sample_offset_ratio Logical optional<Rational>
  field row_level_filter Logical optional<FilterDAGInfo>
  field prewhere_info Logical optional<PrewhereInfo>
  field parallel_reading_from_replicas Physical bool
  field distributed_read_bucket_count Physical UInt64
  field distributed_read_param_name Physical String
  field read_in_order Logical optional<ReadInOrder>
  initializers 0000000000000000000000000000
name ReadFromStorage introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field storage_name Logical String
  initializers 00
name ReadFromTable introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field table Logical String
  field final Logical bool
  field sample_size_ratio Logical optional<Rational>
  field sample_offset_ratio Logical optional<Rational>
  field use_parallel_replicas Physical bool
  initializers 0000000000
name ReadFromTableFunction introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field serialized_ast Logical String
  field final Logical bool
  field sample_size_ratio Logical optional<Rational>
  field sample_offset_ratio Logical optional<Rational>
  initializers 00000000
name ReadNothing introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  initializers 
name Rollup introduced_in 9 full_digest always logical_digest always
format 1 introduced_in 16
  field keys Logical vector<String>
  field aggregates Logical AggregateDescriptionsWithoutArguments
  field final Logical bool
  field overflow_row Logical bool
  field use_nulls Logical bool
  setting max_block_size Physical UInt64
  setting min_hit_rate_to_use_consecutive_keys_optimization Physical Float32
  setting serialize_string_in_memory_with_zero_byte Physical bool
  setting enable_packed_string_keys_in_aggregation Physical bool
  initializers 0000000000
name ShuffleReceive introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field exchange_id Logical String
  field source_shards Physical vector<String>
  initializers 0000
name ShuffleSend introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field exchange_id Logical String
  field key_names Logical vector<String>
  field num_buckets Physical UInt64
  field hash_cast_type_names Logical vector<String>
  initializers 00000000
name Sorting introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field result_description Logical SortDescription
  field partition_by_description Logical SortDescription
  field finish_sorting Logical bool
  field prefix_description Logical SortDescription
  field limit Logical UInt64
  field use_buffering Physical bool
  field apply_virtual_row_conversions Logical bool
  field skip_scatter_by_partition Logical bool
  field is_sorting_for_merge_join Physical bool
  field is_partial_top_n Logical bool
  field always_read_till_end Logical bool
  field limit_by_columns Logical vector<String>
  field limit_by_group_length Logical UInt64
  field read_in_order_use_buffering Physical bool
  field read_in_order_use_virtual_row_per_block Physical bool
  setting max_block_size Physical UInt64
  setting max_rows_to_sort Logical UInt64
  setting max_bytes_to_sort Logical UInt64
  setting sort_overflow_mode Logical enum8
  setting max_bytes_before_remerge_sort Physical UInt64
  setting remerge_sort_lowered_memory_bytes_ratio Physical Float32
  setting max_bytes_before_external_sort Physical UInt64
  setting max_bytes_ratio_before_external_sort Physical Float64
  setting min_free_disk_space_for_temporary_data Physical UInt64
  setting prefer_external_sort_block_bytes Physical UInt64
  setting temporary_files_codec Physical String
  setting temporary_files_buffer_size Physical UInt64
  initializers 000000000000000000000000000000
name TotalsHaving introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field aggregates Logical AggregateDescriptions
  field overflow_row Logical bool
  field actions_dag Logical optional<ActionsDAG>
  field filter_column_name Logical String
  field remove_filter Logical bool
  field final Logical bool
  setting totals_mode Logical enum8
  setting totals_auto_threshold Logical Float32
  initializers 000000000000
name Union introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 16
  field allow_narrowing Logical bool
  initializers 00
name Window introduced_in 4 full_digest always logical_digest always
format 1 introduced_in 16
  field window_name Logical String
  field partition_by Logical SortDescription
  field order_by Logical SortDescription
  field frame Logical WindowFrame
  field window_functions Logical WindowFunctions
  field streams_fan_out Physical bool
  initializers 00000003020001020002000000
)MANIFESTS";

TEST(StepManifestBaseline, DeclarationsAreUnchanged)
{
    if (!QueryPlanStepRegistry::instance().hasStep("Expression"))
        QueryPlanStepRegistry::registerPlanSteps();

    String actual = QueryPlanStepRegistry::instance().dumpManifests();
    EXPECT_EQ(actual, expected_manifests)
        << "The step manifests changed. If the change belongs to a new step name, "
           "replace the expected text with this:\n" << actual;
}
