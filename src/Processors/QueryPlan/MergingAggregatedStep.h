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
        bool should_produce_results_in_order_of_bucket_number_,
        size_t max_block_size_,
        size_t memory_bound_merging_max_block_bytes_,
        SortDescription group_by_sort_description_,
        bool memory_bound_merging_of_aggregation_results_enabled_);

    String getName() const override { return "MergingAggregated"; }
    const Aggregator::Params & getParams() const { return params; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    void applyOrder(SortDescription input_sort_description, DataStream::SortScope sort_scope);

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
    SortDescription group_by_sort_description;

    /// These settings are used to determine if we should resize pipeline to 1 at the end.
    const bool should_produce_results_in_order_of_bucket_number;
    const bool memory_bound_merging_of_aggregation_results_enabled;
};

}
