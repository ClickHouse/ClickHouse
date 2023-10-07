#include <Processors/QueryPlan/ReadNothingStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Sources/NullSource.h>

namespace DB
{

ReadNothingStep::ReadNothingStep(Block output_header, bool is_streaming_)
    : ISourceStep(DataStream{.header = std::move(output_header), .has_single_port = true, .is_streaming = is_streaming_})
{
}

void ReadNothingStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto source = std::make_shared<NullSource>(getOutputStream().header);
    source->setStreaming(isStreaming());
    pipeline.init(Pipe(std::move(source)));
}

}
