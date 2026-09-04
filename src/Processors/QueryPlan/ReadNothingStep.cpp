#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/QueryPlan/StepManifest.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Sources/NullSource.h>

namespace DB
{

ReadNothingStep::ReadNothingStep(SharedHeader output_header_)
    : ISourceStep(std::move(output_header_))
{
}

QueryPlanStepPtr ReadNothingStep::clone() const
{
    return std::make_unique<ReadNothingStep>(getOutputHeader());
}

void ReadNothingStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.init(Pipe(std::make_shared<NullSource>(getOutputHeader())));
}

namespace
{

constexpr auto READ_NOTHING_MANIFEST = StepManifest<ReadNothingStep, ReadNothingWire>("ReadNothing")
    .nameIntroducedIn(1)
    .baseFormat();

}

ReadNothingWire ReadNothingStep::toWire() const
{
    return {};
}

QueryPlanStepPtr ReadNothingStep::fromWire(ReadNothingWire, Deserialization & ctx)
{
    return std::make_unique<ReadNothingStep>(ctx.output_header);
}

void ReadNothingStep::serialize(Serialization & ctx) const
{
    if (usesManifest(ctx.version))
        writeManifestPayload(READ_NOTHING_MANIFEST, toWire(), ctx);
    else
        serializeLegacy(ctx);
}

QueryPlanStepPtr ReadNothingStep::deserialize(Deserialization & ctx)
{
    if (usesManifest(ctx.version))
        return fromWire(readManifestPayload(READ_NOTHING_MANIFEST, ctx), ctx);
    return deserializeLegacy(ctx);
}

void ReadNothingStep::serializeLegacy(Serialization & ctx) const
{
    /// The output header is the whole state, and the plan writes it generically for every node.
    (void)ctx;
}

QueryPlanStepPtr ReadNothingStep::deserializeLegacy(Deserialization & ctx)
{
    return std::make_unique<ReadNothingStep>(ctx.output_header);
}

void registerReadNothingStep(QueryPlanStepRegistry & registry);
void registerReadNothingStep(QueryPlanStepRegistry & registry)
{
    registerManifest<READ_NOTHING_MANIFEST>(registry, &ReadNothingStep::deserialize);
}

}
