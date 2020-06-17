#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <DataStreams/SizeLimits.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

class AggregatingStep : public ITransformingStep
{
public:
    AggregatingStep(
        const DataStream & input_stream_,
        AggregatingTransformParamsPtr transform_params_,
        size_t max_block_size_,
        size_t merge_threads_,
        size_t temporary_data_merge_threads_,
        bool storage_has_evenly_distributed_read_,
        InputOrderInfoPtr group_by_info_,
        SortDescription group_by_sort_description_);

    String getName() const override { return "Aggregating"; }

    void transformPipeline(QueryPipeline & pipeline) override;

private:
    AggregatingTransformParamsPtr transform_params;
    size_t max_block_size;
    size_t merge_threads;
    size_t temporary_data_merge_threads;

    bool storage_has_evenly_distributed_read;

    InputOrderInfoPtr group_by_info;
    SortDescription group_by_sort_description;
};

}

