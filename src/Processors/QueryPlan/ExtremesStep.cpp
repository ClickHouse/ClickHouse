#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/StepManifest.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

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
            .preserves_number_of_rows = true,
        }
    };
}

ExtremesStep::ExtremesStep(const SharedHeader & input_header)
    : ITransformingStep(input_header, input_header, getTraits())
{
}

void ExtremesStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addExtremesTransform();
}

namespace
{

constexpr auto EXTREMES_MANIFEST = StepManifest<ExtremesStep, ExtremesWire>("Extremes")
    .nameIntroducedIn(1)
    .baseFormat();

}

ExtremesWire ExtremesStep::toWire() const
{
    return {};
}

QueryPlanStepPtr ExtremesStep::fromWire(ExtremesWire, Deserialization & ctx)
{
    return std::make_unique<ExtremesStep>(ctx.input_headers.front());
}

void ExtremesStep::serialize(Serialization & ctx) const
{
    if (usesManifest(ctx.version))
        writeManifestPayload(EXTREMES_MANIFEST, toWire(), ctx);
    else
        serializeLegacy(ctx);
}

QueryPlanStepPtr ExtremesStep::deserialize(Deserialization & ctx)
{
    if (usesManifest(ctx.version))
        return fromWire(readManifestPayload(EXTREMES_MANIFEST, ctx), ctx);
    return deserializeLegacy(ctx);
}

void ExtremesStep::serializeLegacy(Serialization & ctx) const
{
    (void)ctx;
}

QueryPlanStepPtr ExtremesStep::deserializeLegacy(Deserialization & ctx)
{
    return std::make_unique<ExtremesStep>(ctx.input_headers.front());
}

QueryPlanStepPtr ExtremesStep::clone() const
{
    return std::make_unique<ExtremesStep>(*this);
}

void registerExtremesStep(QueryPlanStepRegistry & registry);
void registerExtremesStep(QueryPlanStepRegistry & registry)
{
    registerManifest<EXTREMES_MANIFEST>(registry, ExtremesStep::deserialize);
}

}
