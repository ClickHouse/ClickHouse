#include <Processors/QueryPlan/ReadFromCommonBufferStep.h>

#include <Processors/Sources/ReadFromCommonBufferSource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

ReadFromCommonBufferStep::ReadFromCommonBufferStep(
    const SharedHeader & header_,
    ChunkBufferPtr chunk_buffer_,
    size_t max_streams_)
    : ISourceStep(header_)
    , chunk_buffer(std::move(chunk_buffer_))
    , max_streams(max_streams_)
{}

void ReadFromCommonBufferStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    Pipes pipes;
    pipes.reserve(max_streams);

    for (size_t i = 0; i < max_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<ReadFromCommonBufferSource>(getOutputHeader(), chunk_buffer));
    }

    pipeline.init(Pipe::unitePipes(std::move(pipes)));
}

}
