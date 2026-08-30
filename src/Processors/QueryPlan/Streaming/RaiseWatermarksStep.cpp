#include <Processors/QueryPlan/Streaming/RaiseWatermarksStep.h>

#include <Processors/Streaming/RaiseWatermarksTransform.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

namespace
{

ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        .data_stream_traits = {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        .transform_traits = {
            .preserves_number_of_rows = true,
        },
    };
}

}

RaiseWatermarksStep::RaiseWatermarksStep(SharedHeader input_header_, Field initial_watermark_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , initial_watermark(std::move(initial_watermark_))
{
}

void RaiseWatermarksStep::updateOutputHeader()
{
    output_header = input_headers.front();
}

void RaiseWatermarksStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&] (const SharedHeader & header)
    {
        return std::make_shared<RaiseWatermarksTransform>(header, initial_watermark);
    });
}

QueryPlanStepPtr RaiseWatermarksStep::clone() const
{
    return std::make_unique<RaiseWatermarksStep>(input_headers.front(), initial_watermark);
}

}
