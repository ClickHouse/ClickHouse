#include <Processors/QueryPlan/Streaming/CalculateWatermarksStep.h>
#include <Processors/Streaming/CalculateWatermarksTransform.h>
#include <Processors/Port.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Interpreters/Context.h>
#include <Interpreters/Streaming/Utils.h>

#include <Core/Block.h>

namespace DB
{

namespace
{

ITransformingStep::Traits getCalculatorTraits()
{
    return ITransformingStep::Traits
    {
        .data_stream_traits = {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        .transform_traits = {
            .preserves_number_of_rows = false,
        },
    };
}

}

CalculateWatermarksStep::CalculateWatermarksStep(SharedHeader input_header_, WatermarkSettingsPtr watermark_, ContextPtr context_)
    : ITransformingStep(input_header_, input_header_, getCalculatorTraits())
    , watermark(std::move(watermark_))
    , context(std::move(context_))
{
    updateInputHeader(input_header_);
}

void CalculateWatermarksStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&] (const SharedHeader & header)
    {
        auto watermark_expression = buildWatermarkActionsDAG(watermark->expression, *input_headers.front(), context);
        return std::make_shared<CalculateWatermarksTransform>(header, std::move(watermark_expression), context);
    });
}

void CalculateWatermarksStep::updateOutputHeader()
{
    output_header = input_headers.front();
}

QueryPlanStepPtr CalculateWatermarksStep::clone() const
{
    return std::make_unique<CalculateWatermarksStep>(input_headers.front(), watermark, context);
}

}
