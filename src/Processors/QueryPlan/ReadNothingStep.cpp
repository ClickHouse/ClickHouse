#include <Processors/QueryPlan/ReadNothingStep.h>
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

void ReadNothingStep::serialize(Serialization & ctx) const
{
    /// The output header is the whole state, and the plan writes it generically for every node.
    (void)ctx;
}

QueryPlanStepPtr ReadNothingStep::deserialize(Deserialization & ctx)
{
    return std::make_unique<ReadNothingStep>(ctx.output_header);
}

void registerReadNothingStep(QueryPlanStepRegistry & registry);
void registerReadNothingStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("ReadNothing", &ReadNothingStep::deserialize);
}

}
