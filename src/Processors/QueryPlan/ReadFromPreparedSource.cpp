#include <Processors/Formats/IInputFormat.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/SourceWithKeyCondition.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

ReadFromPreparedSource::ReadFromPreparedSource(Pipe pipe_)
    : SourceStepWithFilter(DataStream{.header = pipe_.getHeader()})
    , pipe(std::move(pipe_))
{
}

void ReadFromPreparedSource::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

void ReadFromStorageStep::applyFilters()
{
    if (!context)
        return;

    std::shared_ptr<const KeyCondition> key_condition;
    if (!context->getSettingsRef().allow_experimental_analyzer)
    {
        for (const auto & processor : pipe.getProcessors())
            if (auto * source = dynamic_cast<SourceWithKeyCondition *>(processor.get()))
                source->setKeyCondition(query_info, context);
    }
    else
    {
        for (const auto & processor : pipe.getProcessors())
            if (auto * source = dynamic_cast<SourceWithKeyCondition *>(processor.get()))
                source->setKeyCondition(filter_nodes.nodes, context);
    }
}

}
