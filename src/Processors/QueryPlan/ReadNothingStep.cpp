#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Sources/NullSource.h>

namespace DB
{

ReadNothingStep::ReadNothingStep(DataStream output_stream_)
    : ISourceStep(std::move(output_stream_))
{
}

void ReadNothingStep::initializePipeline(QueryPipeline & pipeline)
{
    pipeline.init(Pipe(std::make_shared<NullSource>(output_stream.header)));
}

}
