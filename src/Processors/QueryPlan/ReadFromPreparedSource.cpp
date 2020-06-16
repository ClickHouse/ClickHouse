#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

ReadFromPreparedSource::ReadFromPreparedSource(Pipe pipe_)
    : ISourceStep(DataStream{.header = pipe_.getHeader()})
    , pipe(std::move(pipe_))
{
}

void ReadFromPreparedSource::initializePipeline(QueryPipeline & pipeline)
{
    pipeline.init(std::move(pipe));
}

}
