#include <Processors/Formats/IInputFormat.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

ReadFromPreparedSource::ReadFromPreparedSource(Pipe pipe_)
    : ISourceStep(pipe_.getHeader())
    , pipe(std::move(pipe_))
{
}

void ReadFromPreparedSource::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

ReadFromStorageStep::ReadFromStorageStep(
    Pipe pipe_,
    String storage_name,
    ContextPtr context_,
    const SelectQueryInfo & query_info_)
    : ReadFromPreparedSource(std::move(pipe_))
    , context(std::move(context_))
    , query_info(query_info_)
{
    setStepDescription(storage_name);

    for (const auto & processor : pipe.getProcessors())
        processor->setStorageLimits(query_info.storage_limits);
}

}
