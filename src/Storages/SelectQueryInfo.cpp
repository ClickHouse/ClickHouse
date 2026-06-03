#include <Interpreters/Set.h>
#include <Parsers/ASTSelectQuery.h>
#include <Planner/PlannerContext.h>
#include <Storages/SelectQueryInfo.h>

#include <ranges>
#include <unordered_set>

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

bool SelectQueryInfo::isStream() const
{
    return table_expression_modifiers && table_expression_modifiers->hasStream();
}

std::unordered_map<std::string, ColumnWithTypeAndName> SelectQueryInfo::buildNodeNameToInputNodeColumn() const
{
    std::unordered_map<std::string, ColumnWithTypeAndName> node_name_to_input_node_column;
    if (planner_context)
    {
        auto & table_expression_data = planner_context->getTableExpressionDataOrThrow(table_expression);
        for (const auto & [column_identifier, column_name] : table_expression_data.getColumnIdentifierToColumnName())
        {
            /// ALIAS columns cannot be used in the filter expression without being calculated in ActionsDAG,
            /// so they should not be added to the input nodes.
            if (table_expression_data.hasAliasColumn(column_name))
                continue;
            const auto & column = table_expression_data.getColumnOrThrow(column_name);
            node_name_to_input_node_column.emplace(column_identifier, ColumnWithTypeAndName(nullptr, column.type, column_name));
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

FilterDAGInfo FilterDAGInfo::clone() const
{
    FilterDAGInfo filter_dag_info;

    filter_dag_info.actions = actions.clone();
    filter_dag_info.column_name = column_name;
    filter_dag_info.do_remove_column = do_remove_column;

    return filter_dag_info;
}

FilterDAGInfoPtr FilterDAGInfo::clonePtr(const FilterDAGInfoPtr & filter_info, bool do_remove_column)
{
    auto result = filter_info ? std::make_shared<FilterDAGInfo>(filter_info->clone()) : nullptr;
    if (result)
        result->do_remove_column = do_remove_column;
    return result;
}

FilterDAGInfoPtr FilterDAGInfo::combineConjunction(const FilterDAGInfoPtr & left_filter_info, const FilterDAGInfoPtr & right_filter_info)
{
    if (!left_filter_info)
        return clonePtr(right_filter_info, true);

    if (!right_filter_info)
        return clonePtr(left_filter_info, true);

    auto merged_actions = ActionsDAG::merge(left_filter_info->actions.clone(), right_filter_info->actions.clone());
    const auto * left_filter_node = &merged_actions.findInOutputs(left_filter_info->column_name);
    const auto * right_filter_node = &merged_actions.findInOutputs(right_filter_info->column_name);

    auto combined_actions = ActionsDAG::buildFilterActionsDAG({left_filter_node, right_filter_node}, {}, true);
    chassert(combined_actions);

    auto combined_filter_info = std::make_shared<FilterDAGInfo>();
    combined_filter_info->actions = std::move(*combined_actions);
    combined_filter_info->column_name = combined_filter_info->actions.getOutputs().front()->result_name;
    combined_filter_info->do_remove_column = true;
    combined_filter_info->projectInputs();

    return combined_filter_info;
}

void FilterDAGInfo::projectInputs()
{
    auto & outputs = actions.getOutputs();
    auto existing_outputs = std::ranges::to<std::unordered_set>(outputs);

    for (const auto * node : actions.getInputs())
    {
        if (existing_outputs.contains(node))
            continue;

        outputs.push_back(node);
    }
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
