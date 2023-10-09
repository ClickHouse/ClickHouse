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
    /// When analyzer is enabled, query_info.filter_asts is missing sets and maybe some type casts,
    /// so don't use it. I'm not sure how to support analyzer here: https://github.com/ClickHouse/ClickHouse/issues/53536
    std::optional<KeyCondition> key_condition;
    if (!context->getSettingsRef().allow_experimental_analyzer)
    {
        key_condition.emplace(
            query_info,
            context,
            pipe.getHeader().getNames(),
            std::make_shared<ExpressionActions>(std::make_shared<ActionsDAG>(pipe.getHeader().getColumnsWithTypeAndName())));

    }
    else if (query_info.planner_context)
    {
        std::unordered_map<std::string, ColumnWithTypeAndName> node_name_to_input_node_column;
        const auto & table_expression_data = query_info.planner_context->getTableExpressionDataOrThrow(query_info.table_expression);
        for (const auto & [column_identifier, column_name] : table_expression_data.getColumnIdentifierToColumnName())
        {
            const auto & column = table_expression_data.getColumnOrThrow(column_name);
            node_name_to_input_node_column.emplace(column_identifier, ColumnWithTypeAndName(column.type, column_name));
        }
        auto filter_actions_dag = ActionsDAG::buildFilterActionsDAG(filter_nodes.nodes, node_name_to_input_node_column, context);
        key_condition.emplace(
            filter_actions_dag,
            context,
            pipe.getHeader().getNames(),
            std::make_shared<ExpressionActions>(std::make_shared<ActionsDAG>(pipe.getHeader().getColumnsWithTypeAndName())),
            NameSet{});
    }

    if (key_condition.has_value())
    {
        for (const auto & processor : pipe.getProcessors())
            if (auto * input_format = dynamic_cast<IInputFormat *>(processor.get()))
                input_format->setKeyCondition(*key_condition);
    }
}

}
