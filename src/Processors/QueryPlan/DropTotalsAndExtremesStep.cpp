#include <Processors/QueryPlan/DropTotalsAndExtremesStep.h>
#include <Processors/QueryPlan/Serialization.h>
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

DropTotalsAndExtremesStep::DropTotalsAndExtremesStep(const SharedHeader & input_header)
    : ITransformingStep(input_header, input_header, getTraits())
{
}

void DropTotalsAndExtremesStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.dropTotalsAndExtremesViaTransform();
}

void DropTotalsAndExtremesStep::serialize(Serialization & ctx) const
{
    (void)ctx;
}

QueryPlanStepPtr DropTotalsAndExtremesStep::deserialize(Deserialization & ctx)
{
    return std::make_unique<DropTotalsAndExtremesStep>(ctx.input_headers.front());
}

QueryPlanStepPtr DropTotalsAndExtremesStep::clone() const
{
    return std::make_unique<DropTotalsAndExtremesStep>(*this);
}

void registerDropTotalsAndExtremesStep(QueryPlanStepRegistry & registry);
void registerDropTotalsAndExtremesStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("DropTotalsAndExtremes", DropTotalsAndExtremesStep::deserialize);
}

}
