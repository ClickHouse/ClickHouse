#include <Interpreters/Context.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

ReadFromPreparedSource::ReadFromPreparedSource(Pipe pipe_, ContextPtr context_, const String & qualified_projection_name_)
    : ISourceStep(DataStream{.header = pipe_.getHeader()})
    , pipe(std::move(pipe_))
    , context(context_)
    , qualified_projection_name(qualified_projection_name_)
{
}

void ReadFromPreparedSource::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    if (context && context->hasQueryContext() && !qualified_projection_name.empty())
        context->getQueryContext()->addQueryAccessInfo(qualified_projection_name);

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

}
