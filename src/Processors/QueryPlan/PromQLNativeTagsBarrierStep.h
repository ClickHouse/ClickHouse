#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/Transforms/PromQLNativeTagsBarrierTransform.h>

namespace DB
{

/// Two-input plan step which drains a tags dependency before exposing the
/// main/samples streams to the rest of the query pipeline.
class PromQLNativeTagsBarrierStep final : public IQueryPlanStep
{
public:
    using CollectorPtr = PromQLNativeTagsBarrierTransform::CollectorPtr;
    using TagsExtractor = PromQLNativeTagsBarrierTransform::TagsExtractor;

    PromQLNativeTagsBarrierStep(
        SharedHeader main_header,
        SharedHeader tags_header,
        CollectorPtr collector_,
        TagsExtractor tags_extractor_);

    String getName() const override { return "PromQLNativeTagsBarrier"; }

    QueryPipelineBuilderPtr updatePipeline(
        QueryPipelineBuilders pipelines,
        const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

private:
    void updateOutputHeader() override { output_header = input_headers.front(); }

    CollectorPtr collector;
    TagsExtractor tags_extractor;
};

}
