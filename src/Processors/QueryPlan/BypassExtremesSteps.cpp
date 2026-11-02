#include <Processors/QueryPlan/BypassExtremesSteps.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
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

DetachExtremesStep::DetachExtremesStep(const SharedHeader & input_header_)
    : ITransformingStep(input_header_, input_header_, getTraits())
{
}

void DetachExtremesStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.detachExtremes();
}

void DetachExtremesStep::serialize(Serialization &) const
{
}

QueryPlanStepPtr DetachExtremesStep::deserialize(Deserialization & ctx)
{
    return std::make_unique<DetachExtremesStep>(ctx.input_headers.front());
}

QueryPlanStepPtr DetachExtremesStep::clone() const
{
    return std::make_unique<DetachExtremesStep>(*this);
}

AttachExtremesStep::AttachExtremesStep(const SharedHeader & input_header_)
    : ITransformingStep(input_header_, input_header_, getTraits())
{
}

void AttachExtremesStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.attachExtremes();
}

void AttachExtremesStep::serialize(Serialization &) const
{
}

QueryPlanStepPtr AttachExtremesStep::deserialize(Deserialization & ctx)
{
    return std::make_unique<AttachExtremesStep>(ctx.input_headers.front());
}

QueryPlanStepPtr AttachExtremesStep::clone() const
{
    return std::make_unique<AttachExtremesStep>(*this);
}

void registerBypassExtremesSteps(QueryPlanStepRegistry & registry);
void registerBypassExtremesSteps(QueryPlanStepRegistry & registry)
{
    registry.registerStep("DetachExtremes", DetachExtremesStep::deserialize);
    registry.registerStep("AttachExtremes", AttachExtremesStep::deserialize);
}

}
