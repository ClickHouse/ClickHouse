#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/OffsetTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/Operators.h>
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

OffsetStep::OffsetStep(const Header & input_header_, size_t offset_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , offset(offset_)
{
}

void OffsetStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto transform = std::make_shared<OffsetTransform>(
            pipeline.getHeader(), offset, pipeline.getNumStreams());

    pipeline.addTransform(std::move(transform));
}

void OffsetStep::describeActions(FormatSettings & settings) const
{
    settings.out << String(settings.offset, ' ') << "Offset " << offset << '\n';
}

void OffsetStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Offset", offset);
}

void OffsetStep::serialize(Serialization & ctx) const
{
    writeVarUInt(offset, ctx.out);
}

std::unique_ptr<IQueryPlanStep> OffsetStep::deserialize(Deserialization & ctx)
{
    UInt64 offset;
    readVarUInt(offset, ctx.in);

    return std::make_unique<OffsetStep>(ctx.input_headers.front(), offset);
}

void registerOffsetStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Offset", OffsetStep::deserialize);
}

}
