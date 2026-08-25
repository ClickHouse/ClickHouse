#include <Processors/QueryPlan/Streaming/CalculateWatermarksStep.h>
#include <Processors/Streaming/CalculateWatermarksTransform.h>
#include <Processors/Port.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Interpreters/Context.h>
#include <Interpreters/Streaming/Utils.h>

#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Streaming/StreamingVirtualColumns.h>

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

CalculateWatermarksStep::CalculateWatermarksStep(SharedHeader input_header_, WatermarkSettingsPtr watermark_settings_, Field initial_watermark_, ContextPtr context_)
    : ITransformingStep(input_header_, input_header_, getCalculatorTraits())
    , watermark_settings(std::move(watermark_settings_))
    , initial_watermark(std::move(initial_watermark_))
    , context(std::move(context_))
{
    updateInputHeader(input_header_);
}

void CalculateWatermarksStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&] (const SharedHeader & header)
    {
        auto watermark_expression = buildWatermarkActionsDAG(watermark_settings->expression, *input_headers.front(), context);
        return std::make_shared<CalculateWatermarksTransform>(header, getOutputHeader(), watermark_settings->column, std::move(watermark_expression), initial_watermark, context);
    });
}

void CalculateWatermarksStep::updateOutputHeader()
{
    Block extended = *input_headers.front();
    auto type = extended.getByName(watermark_settings->column).type;
    extended.insert(ColumnWithTypeAndName(type->createColumn(), type, TimeAttributeColumn::name));
    extended.insert(ColumnWithTypeAndName(type->createColumn(), type, WatermarkColumn::name));
    output_header = std::make_shared<const Block>(std::move(extended));
}

QueryPlanStepPtr CalculateWatermarksStep::clone() const
{
    return std::make_unique<CalculateWatermarksStep>(input_headers.front(), watermark_settings, initial_watermark, context);
}

}
