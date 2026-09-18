#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/PromQLRangeSumByTransform.h>


namespace DB
{

/// Merges streams ordered by `(id, bucket)` and adds the single-stream native
/// kernel for `sum by (...) (rate(selector[window]))` to a query pipeline.
class PromQLRangeSumByStep final : public ITransformingStep
{
public:
    using CollectorPtr = PromQLRangeSumByTransform::CollectorPtr;
    using SeriesDictionaryReadiness = PromQLRangeSumByTransform::SeriesDictionaryReadiness;

    PromQLRangeSumByStep(
        SharedHeader input_header_,
        CollectorPtr collector_,
        AggregateFunctionPtr rate_function_,
        AggregateFunctionPtr sum_function_,
        Strings labels_to_keep_,
        size_t max_output_groups_,
        SeriesDictionaryReadiness dictionary_readiness_ = SeriesDictionaryReadiness::PublishedNativeDictionary,
        bool parallel_processing_requested_ = false);

    String getName() const override { return "PromQLRangeSumBy"; }
    bool isInputOrderDependent() const override { return true; }

    bool isParallelProcessingRequested() const { return parallel_processing_requested; }
    bool isParallelProcessingEnabled() const { return parallel_processing_enabled; }
    void enableParallelProcessing() { parallel_processing_enabled = true; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    void updateOutputHeader() override;

    CollectorPtr collector;
    AggregateFunctionPtr rate_function;
    AggregateFunctionPtr sum_function;
    Strings labels_to_keep;
    size_t max_output_groups;
    SeriesDictionaryReadiness dictionary_readiness;
    bool parallel_processing_requested;
    bool parallel_processing_enabled = false;
};

}
