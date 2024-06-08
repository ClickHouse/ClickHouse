#include <Planner/PlannerContext.h>

#include <Analyzer/TableNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

const ColumnIdentifier & GlobalPlannerContext::createColumnIdentifier(const QueryTreeNodePtr & column_node)
{
    const auto & column_node_typed = column_node->as<ColumnNode &>();
    auto column_source_node = column_node_typed.getColumnSource();

    return createColumnIdentifier(column_node_typed.getColumn(), column_source_node);
}

const ColumnIdentifier & GlobalPlannerContext::createColumnIdentifier(const NameAndTypePair & column, const QueryTreeNodePtr & column_source_node)
{
    std::string column_identifier;

    const auto & source_alias = column_source_node->getAlias();
    if (!source_alias.empty())
        column_identifier = source_alias + "." + column.name;
    else
        column_identifier = column.name;

    auto [it, inserted] = column_identifiers.emplace(column_identifier);
    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column identifier {} is already registered", column_identifier);

    assert(inserted);

    return *it;
}

bool GlobalPlannerContext::hasColumnIdentifier(const ColumnIdentifier & column_identifier)
{
    return column_identifiers.contains(column_identifier);
}

PlannerContext::PlannerContext(ContextMutablePtr query_context_, GlobalPlannerContextPtr global_planner_context_, const SelectQueryOptions & select_query_options_)
    : query_context(std::move(query_context_))
    , global_planner_context(std::move(global_planner_context_))
    , is_ast_level_optimization_allowed(!(query_context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY || select_query_options_.ignore_ast_optimizations))
{}

PlannerContext::PlannerContext(ContextMutablePtr query_context_, PlannerContextPtr planner_context_)
    : query_context(std::move(query_context_))
    , global_planner_context(planner_context_->global_planner_context)
    , is_ast_level_optimization_allowed(planner_context_->is_ast_level_optimization_allowed)
{}

TableExpressionData & PlannerContext::getOrCreateTableExpressionData(const QueryTreeNodePtr & table_expression_node)
{
    auto [it, _] = table_expression_node_to_data.emplace(table_expression_node, TableExpressionData());
    return it->second;
}

const TableExpressionData & PlannerContext::getTableExpressionDataOrThrow(const QueryTreeNodePtr & table_expression_node) const
{
    auto table_expression_data_it = table_expression_node_to_data.find(table_expression_node);
    if (table_expression_data_it == table_expression_node_to_data.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Table expression {} is not registered in planner context",
            table_expression_node->formatASTForErrorMessage());

    return table_expression_data_it->second;
}

TableExpressionData & PlannerContext::getTableExpressionDataOrThrow(const QueryTreeNodePtr & table_expression_node)
{
    auto table_expression_data_it = table_expression_node_to_data.find(table_expression_node);
    if (table_expression_data_it == table_expression_node_to_data.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Table expression {} is not registered in planner context",
            table_expression_node->formatASTForErrorMessage());

    return table_expression_data_it->second;
}

const TableExpressionData * PlannerContext::getTableExpressionDataOrNull(const QueryTreeNodePtr & table_expression_node) const
{
    auto table_expression_data_it = table_expression_node_to_data.find(table_expression_node);
    if (table_expression_data_it == table_expression_node_to_data.end())
        return nullptr;

    return &table_expression_data_it->second;
}

TableExpressionData * PlannerContext::getTableExpressionDataOrNull(const QueryTreeNodePtr & table_expression_node)
{
    auto table_expression_data_it = table_expression_node_to_data.find(table_expression_node);
    if (table_expression_data_it == table_expression_node_to_data.end())
        return nullptr;

    return &table_expression_data_it->second;
}

const ColumnIdentifier & PlannerContext::getColumnNodeIdentifierOrThrow(const QueryTreeNodePtr & column_node) const
{
    auto & column_node_typed = column_node->as<ColumnNode &>();
    const auto & column_name = column_node_typed.getColumnName();
    auto column_source = column_node_typed.getColumnSource();
    const auto & table_expression_data = getTableExpressionDataOrThrow(column_source);
    return table_expression_data.getColumnIdentifierOrThrow(column_name);
}

const ColumnIdentifier * PlannerContext::getColumnNodeIdentifierOrNull(const QueryTreeNodePtr & column_node) const
{
    auto & column_node_typed = column_node->as<ColumnNode &>();
    const auto & column_name = column_node_typed.getColumnName();
    auto column_source = column_node_typed.getColumnSourceOrNull();
    if (!column_source)
        return nullptr;

    const auto * table_expression_data = getTableExpressionDataOrNull(column_source);
    if (!table_expression_data)
        return nullptr;

    return table_expression_data->getColumnIdentifierOrNull(column_name);
}

PlannerContext::SetKey PlannerContext::createSetKey(const DataTypePtr & left_operand_type, const QueryTreeNodePtr & set_source_node)
{
    const auto set_source_hash = set_source_node->getTreeHash();
    if (set_source_node->as<ConstantNode>())
    {
        /* We need to hash the type of the left operand because we can build different sets for different types.
         * (It's done for performance reasons. It's cheaper to convert a small set of values from literal to the type of the left operand.)
         *
         * For example in expression `(a :: Decimal(9, 1) IN (1.0, 2.5)) AND (b :: Decimal(9, 0) IN (1, 2.5))`
         * we need to build two different sets:
         *   - `{1, 2.5} :: Set(Decimal(9, 1))` for a
         *   - `{1} :: Set(Decimal(9, 0))` for b (2.5 omitted because bercause it's not representable as Decimal(9, 0)).
         */
        return "__set_" + left_operand_type->getName() + '_' + toString(set_source_hash);
    }

    /// For other cases we will cast left operand to the type of the set source, so no difference in types.
    return "__set_" + toString(set_source_hash);
}

}
