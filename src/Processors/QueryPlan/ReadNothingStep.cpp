#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Sources/NullSource.h>

namespace DB
{

ReadNothingStep::ReadNothingStep(Block output_header)
    : ISourceStep(DataStream{.header = std::move(output_header), .has_single_port = true})
{
}

void ReadNothingStep::initializePipeline(QueryPipeline & pipeline)
{
    pipeline.init(Pipe(std::make_shared<NullSource>(getOutputStream().header)));
}

}
