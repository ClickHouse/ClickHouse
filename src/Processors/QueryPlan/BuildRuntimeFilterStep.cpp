#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/BuildRuntimeFilterTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{


namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }
    };
}

BuildRuntimeFilterStep::BuildRuntimeFilterStep(
    const SharedHeader & input_header_,
    String filter_column_name_,
    String filter_name_)
    : ITransformingStep(
        input_header_,
        input_header_,
        getTraits())
    , filter_column_name(std::move(filter_column_name_))
    , filter_name(filter_name_)
{
}

void BuildRuntimeFilterStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const SharedHeader & header, QueryPipelineBuilder::StreamType)
    {
        return std::make_shared<BuildRuntimeFilterTransform>(header, filter_column_name, filter_name);
    });
}

void BuildRuntimeFilterStep::updateOutputHeader()
{
    output_header = input_headers.front();
}

void BuildRuntimeFilterStep::serialize(Serialization & ctx) const
{
    writeStringBinary(filter_column_name, ctx.out);
    writeStringBinary(filter_name, ctx.out);
}

QueryPlanStepPtr BuildRuntimeFilterStep::deserialize(Deserialization & ctx)
{
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "BuildRuntimeFilterStep must have one input stream");

    String filter_column_name;
    readStringBinary(filter_column_name, ctx.in);

    String filter_name;
    readStringBinary(filter_name, ctx.in);

    return std::make_unique<BuildRuntimeFilterStep>(ctx.input_headers.front(), std::move(filter_column_name), std::move(filter_name));
}

QueryPlanStepPtr BuildRuntimeFilterStep::clone() const
{
    return std::make_unique<BuildRuntimeFilterStep>(*this);
}

void registerBuildRuntimeFilterStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("BuildRuntimeFilter", BuildRuntimeFilterStep::deserialize);
}

}
