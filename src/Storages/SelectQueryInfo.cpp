#include <Interpreters/Set.h>
#include <Parsers/ASTSelectQuery.h>
#include <Planner/PlannerContext.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

SelectQueryInfo::SelectQueryInfo()
    : prepared_sets(std::make_shared<PreparedSets>())
{}

bool SelectQueryInfo::isFinal() const
{
    if (table_expression_modifiers)
        return table_expression_modifiers->hasFinal();

    const auto & select = query->as<ASTSelectQuery &>();
    return select.final();
}

std::unordered_map<std::string, ColumnWithTypeAndName> SelectQueryInfo::buildNodeNameToInputNodeColumn() const
{
    std::unordered_map<std::string, ColumnWithTypeAndName> node_name_to_input_node_column;
    if (planner_context)
    {
        auto & table_expression_data = planner_context->getTableExpressionDataOrThrow(table_expression);
        const auto & alias_column_expressions = table_expression_data.getAliasColumnExpressions();
        for (const auto & [column_identifier, column_name] : table_expression_data.getColumnIdentifierToColumnName())
        {
            /// ALIAS columns cannot be used in the filter expression without being calculated in ActionsDAG,
            /// so they should not be added to the input nodes.
            if (alias_column_expressions.contains(column_name))
                continue;
            const auto & column = table_expression_data.getColumnOrThrow(column_name);
            node_name_to_input_node_column.emplace(column_identifier, ColumnWithTypeAndName(column.type, column_name));
        }
    }
    return node_name_to_input_node_column;
}

PrewhereInfo PrewhereInfo::clone() const
{
    PrewhereInfo prewhere_info;

    prewhere_info.prewhere_actions = prewhere_actions.clone();
    prewhere_info.prewhere_column_name = prewhere_column_name;
    prewhere_info.remove_prewhere_column = remove_prewhere_column;
    prewhere_info.need_filter = need_filter;

    return prewhere_info;
}

void PrewhereInfo::serialize(IQueryPlanStep::Serialization & ctx) const
{
    prewhere_actions.serialize(ctx.out, ctx.registry);
    writeStringBinary(prewhere_column_name, ctx.out);
    writeBinary(remove_prewhere_column, ctx.out);
}

PrewhereInfo PrewhereInfo::deserialize(IQueryPlanStep::Deserialization & ctx)
{
    PrewhereInfo prewhere_info;

    prewhere_info.prewhere_actions = ActionsDAG::deserialize(ctx.in, ctx.registry, ctx.context);
    readStringBinary(prewhere_info.prewhere_column_name, ctx.in);
    readBinary(prewhere_info.remove_prewhere_column, ctx.in);
    prewhere_info.need_filter = true;

    return prewhere_info;
}

void FilterDAGInfo::serialize(IQueryPlanStep::Serialization & ctx) const
{
    actions.serialize(ctx.out, ctx.registry);
    writeStringBinary(column_name, ctx.out);
    writeBinary(do_remove_column, ctx.out);
}

FilterDAGInfo FilterDAGInfo::deserialize(IQueryPlanStep::Deserialization & ctx)
{
    FilterDAGInfo filter_dag_info;

    filter_dag_info.actions = ActionsDAG::deserialize(ctx.in, ctx.registry, ctx.context);
    readStringBinary(filter_dag_info.column_name, ctx.in);
    readBinary(filter_dag_info.do_remove_column, ctx.in);

    return filter_dag_info;
}

}
