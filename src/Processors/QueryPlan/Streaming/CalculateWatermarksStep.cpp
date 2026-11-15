#include <Processors/QueryPlan/Streaming/CalculateWatermarksStep.h>
#include <Processors/Streaming/CalculateWatermarksTransform.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Interpreters/Streaming/Utils.h>

#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>

#include <DataTypes/IDataType.h>

#include <utility>

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

SharedHeader buildOutputHeader(const Block & input_header, const String & time_attribute_column)
{
    Block header = input_header;

    const auto type = input_header.getByName(time_attribute_column).type;
    header.insert(ColumnWithTypeAndName(type->createColumn(), type, WatermarkColumn::name));

    return std::make_shared<const Block>(std::move(header));
}

}

CalculateWatermarksStep::CalculateWatermarksStep(SharedHeader input_header_, WatermarkSettingsPtr watermark_settings_, Field initial_watermark_, ContextPtr context_)
    : ITransformingStep(input_header_, buildOutputHeader(*input_header_, watermark_settings_->time_attribute_column), getTraits())
    , watermark_settings(std::move(watermark_settings_))
    , initial_watermark(std::move(initial_watermark_))
    , context(std::move(context_))
{
}

void CalculateWatermarksStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const SharedHeader & header)
    {
        auto watermark_expression = buildWatermarkActionsDAG(watermark_settings->expression, *input_headers.front(), context);
        return std::make_shared<CalculateWatermarksTransform>(header, getOutputHeader(), std::move(watermark_expression), initial_watermark, context);
    });
}

void CalculateWatermarksStep::updateOutputHeader()
{
    output_header = buildOutputHeader(*input_headers.front(), watermark_settings->time_attribute_column);
}

QueryPlanStepPtr CalculateWatermarksStep::clone() const
{
    return std::make_unique<CalculateWatermarksStep>(input_headers.front(), watermark_settings, initial_watermark, context);
}

}
