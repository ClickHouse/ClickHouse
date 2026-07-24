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

PrewhereInfoPtr PrewhereInfo::clone() const
{
    PrewhereInfoPtr prewhere_info = std::make_shared<PrewhereInfo>();

    if (row_level_filter)
        prewhere_info->row_level_filter = row_level_filter->clone();

    prewhere_info->prewhere_actions = prewhere_actions.clone();

    prewhere_info->row_level_column_name = row_level_column_name;
    prewhere_info->prewhere_column_name = prewhere_column_name;
    prewhere_info->remove_prewhere_column = remove_prewhere_column;
    prewhere_info->need_filter = need_filter;
    prewhere_info->generated_by_optimizer = generated_by_optimizer;

    return prewhere_info;
}

void PrewhereInfo::serialize(IQueryPlanStep::Serialization & ctx) const
{
    writeBinary(row_level_filter.has_value(), ctx.out);
    if (row_level_filter.has_value())
        row_level_filter->serialize(ctx.out, ctx.registry);
    prewhere_actions.serialize(ctx.out, ctx.registry);
    writeStringBinary(row_level_column_name, ctx.out);
    writeStringBinary(prewhere_column_name, ctx.out);
    writeBinary(remove_prewhere_column, ctx.out);
    writeBinary(need_filter, ctx.out);
    writeBinary(generated_by_optimizer, ctx.out);
}

PrewhereInfoPtr PrewhereInfo::deserialize(IQueryPlanStep::Deserialization & ctx)
{
    PrewhereInfoPtr result = std::make_shared<PrewhereInfo>();
    bool has_row_level_filter;
    readBinary(has_row_level_filter, ctx.in);
    if (has_row_level_filter)
        result->row_level_filter = ActionsDAG::deserialize(ctx.in, ctx.registry, ctx.context);
    result->prewhere_actions = ActionsDAG::deserialize(ctx.in, ctx.registry, ctx.context);
    readStringBinary(result->row_level_column_name, ctx.in);
    readStringBinary(result->prewhere_column_name, ctx.in);
    readBinary(result->remove_prewhere_column, ctx.in);
    readBinary(result->need_filter, ctx.in);
    readBinary(result->generated_by_optimizer, ctx.in);
    return result;
}

}
