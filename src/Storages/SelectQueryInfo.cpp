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

}
