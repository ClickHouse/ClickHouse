#include <Processors/Formats/IInputFormat.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/SourceWithKeyCondition.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

ReadFromPreparedSource::ReadFromPreparedSource(Pipe pipe_, ContextPtr context_, Context::QualifiedProjectionName qualified_projection_name_)
    : SourceStepWithFilter(DataStream{.header = pipe_.getHeader()})
    , pipe(std::move(pipe_))
    , context(std::move(context_))
    , qualified_projection_name(std::move(qualified_projection_name_))
{
}

void ReadFromPreparedSource::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    if (context && context->hasQueryContext())
        context->getQueryContext()->addQueryAccessInfo(qualified_projection_name);

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

void ReadFromStorageStep::applyFilters()
{
    std::cout << "xxx1" << std::endl;
    if (!context)
    {
        std::cout << "xxx2" << std::endl;
        return;
    }

    std::shared_ptr<const KeyCondition> key_condition;
    if (!context->getSettingsRef().allow_experimental_analyzer)
    {
        for (const auto & processor : pipe.getProcessors())
        {
            std::cout << "processor:" << processor->getName() << std::endl;
            if (auto * source = dynamic_cast<SourceWithKeyCondition *>(processor.get()))
            {
                std::cout << "xxx4" << std::endl;
                source->setKeyCondition(query_info, context);
            }
        }
    }
    else
    {
        for (const auto & processor : pipe.getProcessors())
        {
            std::cout << "processor:" << processor->getName() << std::endl;
            if (auto * source = dynamic_cast<SourceWithKeyCondition *>(processor.get()))
            {
                std::cout << "xxx4" << std::endl;
                source->setKeyCondition(filter_nodes.nodes, context);
            }
        }
    }
}

}
