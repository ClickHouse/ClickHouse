#include <IO/Operators.h>
#include <Processors/NegativeOffsetTransform.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/NegativeOffsetStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
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

NegativeOffsetStep::NegativeOffsetStep(const SharedHeader & input_header_, UInt64 offset_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , offset(offset_)
{
}

void NegativeOffsetStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto transform = std::make_shared<NegativeOffsetTransform>(pipeline.getHeader(), offset, pipeline.getNumStreams());

    pipeline.addTransform(std::move(transform));
}

void NegativeOffsetStep::describeActions(FormatSettings & settings) const
{
    settings.out << String(settings.offset, ' ') << "Negative Offset " << offset << '\n';
}

void NegativeOffsetStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Negative Offset", offset);
}

void NegativeOffsetStep::serialize(Serialization & ctx) const
{
    writeVarUInt(offset, ctx.out);
}

std::unique_ptr<IQueryPlanStep> NegativeOffsetStep::deserialize(Deserialization & ctx)
{
    UInt64 offset;
    readVarUInt(offset, ctx.in);

    return std::make_unique<NegativeOffsetStep>(ctx.input_headers.front(), offset);
}

void registerNegativeOffsetStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("NegativeOffset", NegativeOffsetStep::deserialize);
}

}
