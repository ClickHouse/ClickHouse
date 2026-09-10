#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Processors/FractionalOffsetTransform.h>
#include <Processors/OffsetTransform.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/FractionalOffsetStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/StepManifest.h>
#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>
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

    if (dataflow_cache_updater)
        pipeline.addSimpleTransform([&](const SharedHeader & header)
                                    { return std::make_shared<RuntimeDataflowStatisticsCollector>(header, dataflow_cache_updater); });
}

void FractionalOffsetStep::describeActions(FormatSettings & settings) const
{
    const auto & prefix = settings.detail_prefix;
    settings.out << prefix << "Fractional Offset " << fractional_offset << '\n';
}

void FractionalOffsetStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Fractional Offset", fractional_offset);
}

namespace
{

constexpr auto FRACTIONAL_OFFSET_MANIFEST = StepManifest<FractionalOffsetStep, FractionalOffsetWire>("FractionalOffset")
    .nameIntroducedIn(1)
    .baseFormat(
        field("fractional_offset", WireFieldClass::Logical, &FractionalOffsetWire::fractional_offset));

}

FractionalOffsetWire FractionalOffsetStep::toWire() const
{
    return FractionalOffsetWire{fractional_offset};
}

QueryPlanStepPtr FractionalOffsetStep::fromWire(FractionalOffsetWire wire, Deserialization & ctx)
{
    return std::make_unique<FractionalOffsetStep>(ctx.input_headers.front(), wire.fractional_offset);
}

void FractionalOffsetStep::serialize(Serialization & ctx) const
{
    if (usesManifest(ctx.version))
        writeManifestPayload(FRACTIONAL_OFFSET_MANIFEST, toWire(), ctx);
    else
        serializeLegacy(ctx);
}

QueryPlanStepPtr FractionalOffsetStep::deserialize(Deserialization & ctx)
{
    if (usesManifest(ctx.version))
        return fromWire(readManifestPayload(FRACTIONAL_OFFSET_MANIFEST, ctx), ctx);
    return deserializeLegacy(ctx);
}

void FractionalOffsetStep::serializeLegacy(Serialization & ctx) const
{
    writeFloatBinary(fractional_offset, ctx.out);
}

QueryPlanStepPtr FractionalOffsetStep::deserializeLegacy(Deserialization & ctx)
{
    Float64 offset = 0;
    readFloatBinary(offset, ctx.in);

    return std::make_unique<FractionalOffsetStep>(ctx.input_headers.front(), offset);
}

void registerFractionalOffsetStep(QueryPlanStepRegistry & registry);
void registerFractionalOffsetStep(QueryPlanStepRegistry & registry)
{
    registerManifest<FRACTIONAL_OFFSET_MANIFEST>(registry, FractionalOffsetStep::deserialize);
}

}
