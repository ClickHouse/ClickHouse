#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

ReadFromPreparedSource::ReadFromPreparedSource(Pipe pipe_, std::shared_ptr<const Context> context_)
    : ISourceStep(DataStream{.header = pipe_.getHeader()})
    , pipe(std::move(pipe_))
    , context(std::move(context_))
{
}

void ReadFromPreparedSource::initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));

    if (context)
        pipeline.addInterpreterContext(std::move(context));
}

}
