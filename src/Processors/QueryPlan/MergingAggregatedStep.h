#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Storages/SelectQueryInfo.h>
#include <QueryPipeline/SizeLimits.h>
#include "Core/SortDescription.h"

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
        AggregatingTransformParamsPtr params_,
        bool memory_efficient_aggregation_,
        size_t max_threads_,
        size_t memory_efficient_merge_threads_,
        const SelectQueryInfo & query_info_,
        ContextPtr context_,
        bool optimize_distributed_aggregation_,
        SortDescription description_,
        UInt64 limit_);

    String getName() const override { return "MergingAggregated"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    AggregatingTransformParamsPtr params;
    bool memory_efficient_aggregation;
    size_t max_threads;
    size_t memory_efficient_merge_threads;
    const SelectQueryInfo & query_info;
    ContextPtr context;
    bool optimize_distributed_aggregation;
    SortDescription description;
    UInt64 limit;
};

}
