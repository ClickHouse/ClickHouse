#include <Processors/Port.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/StepManifest.h>
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

OffsetStep::OffsetStep(const SharedHeader & input_header_, size_t offset_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , offset(offset_)
{
}

void OffsetStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto transform = std::make_shared<OffsetTransform>(
            pipeline.getHeader(), offset, pipeline.getNumStreams());

    pipeline.addTransform(std::move(transform));

    if (dataflow_cache_updater)
        pipeline.addSimpleTransform([&](const SharedHeader & header)
                                    { return std::make_shared<RuntimeDataflowStatisticsCollector>(header, dataflow_cache_updater); });
}

void OffsetStep::describeActions(FormatSettings & settings) const
{
    const auto & prefix = settings.detail_prefix;
    settings.out << prefix << "Offset " << offset << '\n';
}

void OffsetStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Offset", offset);
}

namespace
{

constexpr auto OFFSET_MANIFEST = StepManifest<OffsetStep, OffsetWire>("Offset")
    .nameIntroducedIn(1)
    .baseFormat(field("offset", WireFieldClass::Logical, &OffsetWire::offset));

}

OffsetWire OffsetStep::toWire() const
{
    return OffsetWire{offset};
}

QueryPlanStepPtr OffsetStep::fromWire(OffsetWire wire, Deserialization & ctx)
{
    return std::make_unique<OffsetStep>(ctx.input_headers.front(), wire.offset);
}

void OffsetStep::serialize(Serialization & ctx) const
{
    if (usesManifest(ctx.version))
        writeManifestPayload(OFFSET_MANIFEST, toWire(), ctx);
    else
        serializeLegacy(ctx);
}

QueryPlanStepPtr OffsetStep::deserialize(Deserialization & ctx)
{
    if (usesManifest(ctx.version))
        return fromWire(readManifestPayload(OFFSET_MANIFEST, ctx), ctx);
    return deserializeLegacy(ctx);
}

void OffsetStep::serializeLegacy(Serialization & ctx) const
{
    writeVarUInt(offset, ctx.out);
}

QueryPlanStepPtr OffsetStep::deserializeLegacy(Deserialization & ctx)
{
    UInt64 offset = 0;
    readVarUInt(offset, ctx.in);

    return std::make_unique<OffsetStep>(ctx.input_headers.front(), offset);
}

QueryPlanStepPtr OffsetStep::clone() const
{
    return std::make_unique<OffsetStep>(*this);
}

void registerOffsetStep(QueryPlanStepRegistry & registry);
void registerOffsetStep(QueryPlanStepRegistry & registry)
{
    registerManifest<OFFSET_MANIFEST>(registry, OffsetStep::deserialize);
}

}
