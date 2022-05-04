#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>
#include <Storages/SelectQueryInfo.h>
#include <Interpreters/Aggregator.h>

#include <Interpreters/Context_fwd.h>

namespace DB
{

struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

/// Aggregation. See AggregatingTransform.
class CudaAggregatingStep : public ITransformingStep
{
public:
    CudaAggregatingStep(
        const DataStream & input_stream_,
        Aggregator::Params params_,
        bool final_,
        size_t max_block_size_,
        size_t aggregation_in_order_max_block_bytes_,
        size_t merge_threads_,
        size_t temporary_data_merge_threads_,
        bool storage_has_evenly_distributed_read_,
        InputOrderInfoPtr group_by_info_,
        SortDescription group_by_sort_description_,
        ContextPtr context_);

    String getName() const override { return "Aggregating"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;

    void describeActions(FormatSettings &) const override;
    void describePipeline(FormatSettings & settings) const override;

    const Aggregator::Params & getParams() const { return params; }

private:
    ContextPtr context;

    Aggregator::Params params;
    bool final;
    size_t merge_threads;

    InputOrderInfoPtr group_by_info;
    SortDescription group_by_sort_description;

    Processors aggregating_in_order;
    Processors aggregating_sorted;
    Processors finalizing;

    Processors aggregating;

};

}
