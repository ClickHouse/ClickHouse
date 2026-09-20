#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/PromQLRangeRateTransform.h>


namespace DB
{

/// Merges streams ordered by `(id, bucket)` and adds the single-stream native
/// kernel for `rate(selector[window])` to a query pipeline.
class PromQLRangeRateStep final : public ITransformingStep
{
public:
    using CollectorPtr = PromQLRangeRateTransform::CollectorPtr;

    PromQLRangeRateStep(
        SharedHeader input_header_,
        CollectorPtr collector_,
        AggregateFunctionPtr rate_function_,
        size_t max_samples_per_series_,
        size_t max_output_block_size_,
        bool parallel_processing_requested_ = false,
        size_t max_parallel_lanes_ = 0,
        std::optional<Field> raw_min_time_ = {},
        std::optional<Field> raw_max_time_ = {});

    String getName() const override { return "PromQLRangeRate"; }
    bool isInputOrderDependent() const override { return true; }

    bool isParallelProcessingRequested() const { return parallel_processing_requested; }
    bool isParallelProcessingEnabled() const { return parallel_processing_enabled; }
    size_t getMaxParallelLanes() const { return max_parallel_lanes; }
    void enableParallelProcessing() { parallel_processing_enabled = true; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    void updateOutputHeader() override;

    CollectorPtr collector;
    AggregateFunctionPtr rate_function;
    size_t max_samples_per_series;
    size_t max_output_block_size;
    size_t max_parallel_lanes;
    bool parallel_processing_requested;
    bool parallel_processing_enabled = false;
    std::optional<Field> raw_min_time;
    std::optional<Field> raw_max_time;
};

}
