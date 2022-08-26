#pragma once
#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

/// This step finishes aggregation. See AggregatingSortedTransform.
class MergingAggregatedStep : public ITransformingStep
{
public:
    MergingAggregatedStep(
        const DataStream & input_stream_,
        Aggregator::Params params_,
        bool final_,
        bool memory_efficient_aggregation_,
        size_t max_threads_,
        size_t memory_efficient_merge_threads_,
        size_t max_block_size_,
        size_t memory_bound_merging_max_block_bytes_,
        SortDescription group_by_sort_description_,
        bool precedes_merging_,
        bool memory_bound_merging_of_aggregation_results_enabled_);

    String getName() const override { return "MergingAggregated"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    bool memoryBoundMergingWillBeUsed() const;

private:
    void updateOutputStream() override;

    Aggregator::Params params;
    bool final;
    bool memory_efficient_aggregation;
    size_t max_threads;
    size_t memory_efficient_merge_threads;
    const size_t max_block_size;
    const size_t memory_bound_merging_max_block_bytes;
    const SortDescription group_by_sort_description;

    /// It is used to determine if we should resize pipeline to 1 at the end.
    const bool precedes_merging;

    const bool memory_bound_merging_of_aggregation_results_enabled;
};

}
