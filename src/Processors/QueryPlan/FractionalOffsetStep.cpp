#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Processors/FractionalOffsetTransform.h>
#include <Processors/OffsetTransform.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/FractionalOffsetStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <base/types.h>
#include <Common/JSONBuilder.h>

namespace DB
{

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
            .preserves_number_of_rows = false,
        }
    };
}

FractionalOffsetStep::FractionalOffsetStep(const SharedHeader & input_header_, Float64 fractional_offset_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , fractional_offset(fractional_offset_)
{
}

void FractionalOffsetStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto transform = std::make_shared<FractionalOffsetTransform>(pipeline.getHeader(), fractional_offset, pipeline.getNumStreams());

    pipeline.addTransform(std::move(transform));
}

void FractionalOffsetStep::describeActions(FormatSettings & settings) const
{
    settings.out << String(settings.offset, ' ') << "Fractional Offset " << fractional_offset << '\n';
}

void FractionalOffsetStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Fractional Offset", fractional_offset);
}

void FractionalOffsetStep::serialize(Serialization & ctx) const
{
    writeFloatBinary(fractional_offset, ctx.out);
}

std::unique_ptr<IQueryPlanStep> FractionalOffsetStep::deserialize(Deserialization & ctx)
{
    Float64 offset;
    readFloatBinary(offset, ctx.in);

    return std::make_unique<FractionalOffsetStep>(ctx.input_headers.front(), offset);
}

void registerFractionalOffsetStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("FractionalOffset", FractionalOffsetStep::deserialize);
}

}
