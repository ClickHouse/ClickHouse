#include <Processors/QueryPlan/ReadNothingStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Sources/NullSource.h>

namespace DB
{

ReadNothingStep::ReadNothingStep(Block output_header)
    : ISourceStep(DataStream{.header = std::move(output_header)})
{
}

void ReadNothingStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.init(Pipe(std::make_shared<NullSource>(getOutputStream().header)));
}

}
