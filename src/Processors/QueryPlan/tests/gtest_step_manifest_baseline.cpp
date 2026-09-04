#include <gtest/gtest.h>

#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/StepManifest.h>

using namespace DB;

/// The declarations of every step manifest, in the canonical form of `describeManifest`. A change
/// here is a change of what a step puts on the wire in the framed format. On master a new line is
/// fine when it belongs to a new name or to a new appended format at the current plan version; an
/// existing line must not change, because readers inside the support window depend on it. On a
/// release branch nothing here may change at all. Update the text below only after that review.
static const char * expected_manifests = R"MANIFESTS(name Aggregating introduced_in 1 custom full_digest always logical_digest always
name ArrayJoin introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 12
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
format 1 introduced_in 12
  field exchange_id Logical String
  field source_shards Physical vector<String>
  initializers 0000
name BroadcastSend introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 12
  field exchange_id Logical String
  field num_buckets Physical UInt64
  initializers 0000
name BuildRuntimeFilter introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 12
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
name Cube introduced_in 9 custom full_digest always logical_digest always
name Distinct introduced_in 1 full_digest always logical_digest predicate
format 1 introduced_in 12
  field columns Logical vector<String>
  field limit_hint Logical UInt64
  field distinct_sort_desc Logical SortDescription
  field skip_stream_merging Physical bool
  setting max_rows_in_distinct Logical UInt64
  setting max_bytes_in_distinct Logical UInt64
  setting distinct_overflow_mode Logical enum8
  initializers 00000000
name Expression introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 12
  field actions_dag Logical ActionsDAG
  field prevent_input_removal Physical bool
  initializers 00000000
name Extremes introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 12
  initializers 
name Filter introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 12
  field actions_dag Logical ActionsDAG
  field filter_column_name Logical String
  field remove_filter_column Logical bool
  field prevent_input_removal Physical bool
  field condition Physical optional<pair<UInt64,String>>
  initializers 00000000000000
name FractionalLimit introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 12
  field limit_fraction Logical Float64
  field offset_fraction Logical Float64
  field offset Logical UInt64
  field with_ties Logical bool
  field description Logical SortDescription
  initializers 00000000000000000000000000000000000000
name FractionalOffset introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 12
  field fractional_offset Logical Float64
  initializers 0000000000000000
name GatherReceive introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 12
  field exchange_id Logical String
  field num_buckets Physical UInt64
  field maintain_sort_description Logical optional<SortDescription>
  initializers 000000
name GatherSend introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 12
  field exchange_id Logical String
  field maintain_sort_description Logical optional<SortDescription>
  initializers 0000
name Join introduced_in 1 custom full_digest always logical_digest always
name Limit introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 12
  field limit Logical UInt64
  field offset Logical UInt64
  field always_read_till_end Logical bool
  field with_ties Logical bool
  field description Logical SortDescription
  field is_shard_limit Logical bool
  initializers 000000000000
name LimitBy introduced_in 1 full_digest always logical_digest predicate
format 1 introduced_in 12
  field group_length Logical UInt64
  field group_offset Logical UInt64
  field columns Logical vector<String>
  field sorted_columns_descr Logical SortDescription
  field skip_stream_merging Physical bool
  initializers 0000000000
name MergingAggregated introduced_in 1 custom full_digest always logical_digest always
name NegativeLimit introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 12
  field limit Logical UInt64
  field offset Logical UInt64
  field with_ties Logical bool
  field description Logical SortDescription
  field is_shard_limit Logical bool
  initializers 0000000000
name NegativeLimitBy introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 12
  field group_length Logical UInt64
  field group_offset Logical UInt64
  field columns Logical vector<String>
  field sorted_columns_descr Logical SortDescription
  initializers 00000000
name NegativeOffset introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 12
  field offset Logical UInt64
  initializers 00
name ObjectFilter introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 12
  field actions_dag Logical ActionsDAG
  field filter_column_name Logical String
  initializers 00000000
name Offset introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 12
  field offset Logical UInt64
  initializers 00
name PreDistinct introduced_in 1 full_digest always logical_digest predicate
format 1 introduced_in 12
  field columns Logical vector<String>
  field limit_hint Logical UInt64
  field distinct_sort_desc Logical SortDescription
  field skip_stream_merging Physical bool
  setting max_rows_in_distinct Logical UInt64
  setting max_bytes_in_distinct Logical UInt64
  setting distinct_overflow_mode Logical enum8
  initializers 00000000
name ReadFromMergeTree introduced_in 1 custom full_digest always logical_digest always
name ReadNothing introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 12
  initializers 
name Rollup introduced_in 9 custom full_digest always logical_digest always
name ShuffleReceive introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 12
  field exchange_id Logical String
  field source_shards Physical vector<String>
  initializers 0000
name ShuffleSend introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 12
  field exchange_id Logical String
  field key_names Logical vector<String>
  field num_buckets Physical UInt64
  field hash_cast_type_names Logical vector<String>
  initializers 00000000
name Sorting introduced_in 1 custom full_digest always logical_digest always
name TotalsHaving introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 12
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
format 1 introduced_in 12
  field allow_narrowing Logical bool
  initializers 00
name Window introduced_in 4 custom full_digest always logical_digest always
)MANIFESTS";

TEST(StepManifestBaseline, DeclarationsAreUnchanged)
{
    if (!QueryPlanStepRegistry::instance().hasStep("Expression"))
        QueryPlanStepRegistry::registerPlanSteps();

    String actual = QueryPlanStepRegistry::instance().dumpManifests();
    EXPECT_EQ(actual, expected_manifests)
        << "The step manifests changed. If the change is a new name or a new appended format at the current plan version, "
           "replace the expected text with this:\n" << actual;
}
