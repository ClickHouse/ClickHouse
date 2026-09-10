#pragma once
#include <Interpreters/Aggregator.h>
#include <Core/Defines.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

struct MergingAggregatedWire;

/// This step finishes aggregation. See AggregatingSortedTransform.
class MergingAggregatedStep : public ITransformingStep
{
public:
    MergingAggregatedStep(
        const SharedHeader & input_header_,
        Aggregator::Params params_,
        GroupingSetsParamsList grouping_sets_params_,
        bool final_,
        bool memory_efficient_aggregation_,
        size_t memory_efficient_merge_threads_,
        bool should_produce_results_in_order_of_bucket_number_,
        size_t max_block_size_,
        size_t memory_bound_merging_max_block_bytes_,
        bool memory_bound_merging_of_aggregation_results_enabled_);

    String getName() const override { return "MergingAggregated"; }
    const Aggregator::Params & getParams() const { return params; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    void applyOrder(SortDescription input_sort_description);
    const SortDescription & getSortDescription() const override;
    const SortDescription & getGroupBySortDescription() const { return group_by_sort_description; }

    bool memoryBoundMergingWillBeUsed() const;

    bool isGroupingSets() const { return !grouping_sets_params.empty(); }
    const auto & getGroupingSetsParamsList() const { return grouping_sets_params; }

    void serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const override;
    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }
    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifest in `MergingAggregatedStep.cpp` declares.
    MergingAggregatedWire toWire() const;
    static QueryPlanStepPtr fromWire(MergingAggregatedWire wire, Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

private:
    /// Streams below the framed format.
    void serializeSettingsLegacy(QueryPlanSerializationSettings & settings, UInt64 version) const;
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);
    void updateOutputHeader() override;

    Aggregator::Params params;
    GroupingSetsParamsList grouping_sets_params;
    bool final;
    const bool memory_efficient_aggregation;
    size_t max_threads;
    size_t memory_efficient_merge_threads;
    const size_t max_block_size;
    const size_t memory_bound_merging_max_block_bytes;
    SortDescription group_by_sort_description;

    /// These settings are used to determine if we should resize pipeline to 1 at the end.
    const bool should_produce_results_in_order_of_bucket_number;
    const bool memory_bound_merging_of_aggregation_results_enabled;
};

/// What `MergingAggregatedStep` puts on the wire in the framed format. The members from
/// `max_block_size` on travel through the settings channel.
struct MergingAggregatedWire
{
    Names keys;
    AggregateDescriptions aggregates;
    /// The used keys of every grouping set; the missing keys of a set are the other keys.
    std::vector<Names> grouping_sets;
    bool final = false;
    bool overflow_row = false;
    SortDescription group_by_sort_description;
    bool should_produce_results_in_order_of_bucket_number = false;
    bool memory_bound_merging_of_aggregation_results_enabled = false;

    UInt64 max_block_size = DEFAULT_BLOCK_SIZE;
    UInt64 aggregation_in_order_max_block_bytes = 50000000;
    Float32 min_hit_rate_to_use_consecutive_keys_optimization = 0.5;
    bool distributed_aggregation_memory_efficient = true;
    bool serialize_string_in_memory_with_zero_byte = true;
    bool enable_packed_string_keys_in_aggregation = true;
};

}
