#include <Processors/QueryPlan/ReadNothingStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Sources/NullSource.h>

namespace DB
{

ReadNothingStep::ReadNothingStep(Block output_header_)
    : ISourceStep(std::move(output_header_))
{
}

void ReadNothingStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.init(Pipe(std::make_shared<NullSource>(getOutputHeader())));
}

}
