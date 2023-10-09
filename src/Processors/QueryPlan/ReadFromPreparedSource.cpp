#include <Processors/Formats/IInputFormat.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
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
            if (auto * source = dynamic_cast<ISource *>(processor.get()))
            {
                std::cout << "xxx4" << std::endl;
                source->setKeyCondition(query_info, context);
            }
        }
    }
    else
    {
        std::unordered_map<std::string, ColumnWithTypeAndName> node_name_to_input_node_column;
        const auto & table_expression_data = query_info.planner_context->getTableExpressionDataOrThrow(query_info.table_expression);
        for (const auto & [column_identifier, column_name] : table_expression_data.getColumnIdentifierToColumnName())
        {
            const auto & column = table_expression_data.getColumnOrThrow(column_name);
            node_name_to_input_node_column.emplace(column_identifier, ColumnWithTypeAndName(column.type, column_name));
        }
        auto filter_actions_dag = ActionsDAG::buildFilterActionsDAG(filter_nodes.nodes, node_name_to_input_node_column, context);
        key_condition = std::make_shared<const KeyCondition>(
            filter_actions_dag,
            context,
            pipe.getHeader().getNames(),
            std::make_shared<ExpressionActions>(std::make_shared<ActionsDAG>(pipe.getHeader().getColumnsWithTypeAndName())),
            NameSet{});
    }

    std::cout << "xxx3" << std::endl;
    if (key_condition)
    {
        for (const auto & processor : pipe.getProcessors())
        {
            std::cout << "processor:" << processor->getName() << std::endl;
            if (auto * source = dynamic_cast<ISource *>(processor.get()))
            {
                std::cout << "xxx4" << std::endl;
                source->setKeyCondition(key_condition);
            }
        }
    }
}

}
