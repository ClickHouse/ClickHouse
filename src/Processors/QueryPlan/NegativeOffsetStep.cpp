#include <IO/Operators.h>
#include <Processors/NegativeOffsetTransform.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/NegativeOffsetStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/StepManifest.h>
#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>
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

    if (dataflow_cache_updater)
        pipeline.addSimpleTransform([&](const SharedHeader & header)
                                    { return std::make_shared<RuntimeDataflowStatisticsCollector>(header, dataflow_cache_updater); });
}

void NegativeOffsetStep::describeActions(FormatSettings & settings) const
{
    const auto & prefix = settings.detail_prefix;
    settings.out << prefix << "Negative Offset " << offset << '\n';
}

void NegativeOffsetStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Negative Offset", offset);
}

namespace
{

constexpr auto NEGATIVE_OFFSET_MANIFEST = StepManifest<NegativeOffsetStep, NegativeOffsetWire>("NegativeOffset")
    .nameIntroducedIn(1)
    .baseFormat(
        field("offset", WireFieldClass::Logical, &NegativeOffsetWire::offset));

}

NegativeOffsetWire NegativeOffsetStep::toWire() const
{
    return NegativeOffsetWire{offset};
}

QueryPlanStepPtr NegativeOffsetStep::fromWire(NegativeOffsetWire wire, Deserialization & ctx)
{
    return std::make_unique<NegativeOffsetStep>(ctx.input_headers.front(), wire.offset);
}

void NegativeOffsetStep::serialize(Serialization & ctx) const
{
    if (usesManifest(ctx.version))
        writeManifestPayload(NEGATIVE_OFFSET_MANIFEST, toWire(), ctx);
    else
        serializeLegacy(ctx);
}

QueryPlanStepPtr NegativeOffsetStep::deserialize(Deserialization & ctx)
{
    if (usesManifest(ctx.version))
        return fromWire(readManifestPayload(NEGATIVE_OFFSET_MANIFEST, ctx), ctx);
    return deserializeLegacy(ctx);
}

void NegativeOffsetStep::serializeLegacy(Serialization & ctx) const
{
    writeVarUInt(offset, ctx.out);
}

QueryPlanStepPtr NegativeOffsetStep::deserializeLegacy(Deserialization & ctx)
{
    UInt64 offset = 0;
    readVarUInt(offset, ctx.in);

    return std::make_unique<NegativeOffsetStep>(ctx.input_headers.front(), offset);
}

void registerNegativeOffsetStep(QueryPlanStepRegistry & registry);
void registerNegativeOffsetStep(QueryPlanStepRegistry & registry)
{
    registerManifest<NEGATIVE_OFFSET_MANIFEST>(registry, NegativeOffsetStep::deserialize);
}

}
