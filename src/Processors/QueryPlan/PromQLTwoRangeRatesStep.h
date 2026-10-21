#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/PromQLTwoRangeRatesTransform.h>


namespace DB
{

/// Merges streams ordered by `(id, bucket)` and evaluates the exact native
/// `rate(metric_a[window]) + rate(metric_b[window])` vector-matching island.
class PromQLTwoRangeRatesStep final : public ITransformingStep
{
public:
    using CollectorPtr = PromQLTwoRangeRatesTransform::CollectorPtr;

    PromQLTwoRangeRatesStep(
        SharedHeader input_header_,
        CollectorPtr collector_,
        AggregateFunctionPtr rate_function_,
        String first_metric_name_,
        String second_metric_name_,
        size_t max_samples_per_series_,
        size_t max_output_block_size_,
        size_t max_join_groups_,
        size_t max_grid_cells_,
        bool parallel_processing_requested_ = false,
        size_t max_parallel_lanes_ = 0,
        std::optional<Field> raw_min_time_ = {},
        std::optional<Field> raw_max_time_ = {},
        bool storage_fusion_requested_ = false);

    String getName() const override { return "PromQLTwoRangeRates"; }
    bool isInputOrderDependent() const override { return true; }

    bool isParallelProcessingRequested() const { return parallel_processing_requested; }
    bool isParallelProcessingEnabled() const { return parallel_processing_enabled; }
    size_t getMaxParallelLanes() const { return max_parallel_lanes; }
    void enableParallelProcessing() { parallel_processing_enabled = true; }
    bool isStorageFusionRequested() const { return storage_fusion_requested; }
    PromQLTwoRangeRatesFusionConfigPtr getFusionConfig() const;

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    void updateOutputHeader() override;

    CollectorPtr collector;
    AggregateFunctionPtr rate_function;
    String first_metric_name;
    String second_metric_name;
    size_t max_samples_per_series;
    size_t max_output_block_size;
    size_t max_join_groups;
    size_t max_grid_cells;
    size_t max_parallel_lanes;
    bool parallel_processing_requested;
    bool parallel_processing_enabled = false;
    std::optional<Field> raw_min_time;
    std::optional<Field> raw_max_time;
    bool storage_fusion_requested;
};

}
