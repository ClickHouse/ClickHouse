#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

ReadFromPreparedSource::ReadFromPreparedSource(Pipe pipe_, std::shared_ptr<Context> context_)
    : ISourceStep(DataStream{.header = pipe_.getHeader(), .has_single_port = true})
    , pipe(std::move(pipe_))
    , context(std::move(context_))
{
}

void ReadFromPreparedSource::initializePipeline(QueryPipeline & pipeline)
{
    pipeline.init(std::move(pipe));
    pipeline.addInterpreterContext(std::move(context));
}

}
