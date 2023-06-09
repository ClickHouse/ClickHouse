#include <Planner/PlannerContext.h>

#include <Analyzer/TableNode.h>
#include <Analyzer/ColumnNode.h>

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

const ColumnIdentifier & GlobalPlannerContext::createColumnIdentifier(const NameAndTypePair & column, const QueryTreeNodePtr & /*column_source_node*/)
{
    std::string column_identifier;

    column_identifier += column.name;
    column_identifier += '_' + std::to_string(column_identifiers.size());

    auto [it, inserted] = column_identifiers.emplace(column_identifier);
    assert(inserted);

    return *it;
}

bool GlobalPlannerContext::hasColumnIdentifier(const ColumnIdentifier & column_identifier)
{
    return column_identifiers.contains(column_identifier);
}

PlannerContext::PlannerContext(ContextMutablePtr query_context_, GlobalPlannerContextPtr global_planner_context_)
    : query_context(std::move(query_context_))
    , global_planner_context(std::move(global_planner_context_))
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

PlannerContext::SetKey PlannerContext::createSetKey(const QueryTreeNodePtr & set_source_node)
{
    auto set_source_hash = set_source_node->getTreeHash();
    return "__set_" + toString(set_source_hash.first) + '_' + toString(set_source_hash.second);
}

// void PlannerContext::registerSet(const SetKey & key, PlannerSet planner_set)
// {
//     if (!planner_set.getSet().isValid())
//         throw Exception(ErrorCodes::LOGICAL_ERROR, "Set must be initialized");

//     const auto & subquery_node = planner_set.getSubqueryNode();
//     if (subquery_node)
//     {
//         auto node_type = subquery_node->getNodeType();

//        if (node_type != QueryTreeNodeType::QUERY &&
//            node_type != QueryTreeNodeType::UNION &&
//            node_type != QueryTreeNodeType::TABLE)
//            throw Exception(ErrorCodes::LOGICAL_ERROR,
//                "Invalid node for set table expression. Expected query or union. Actual {}",
//                subquery_node->formatASTForErrorMessage());
//    }

//     set_key_to_set.emplace(key, std::move(planner_set));
// }

// bool PlannerContext::hasSet(const SetKey & key) const
// {
//     return set_key_to_set.contains(key);
// }

// const PlannerSet & PlannerContext::getSetOrThrow(const SetKey & key) const
// {
//     auto it = set_key_to_set.find(key);
//     if (it == set_key_to_set.end())
//         throw Exception(ErrorCodes::LOGICAL_ERROR,
//             "No set is registered for key {}",
//             key);

//     return it->second;
// }

// PlannerSet * PlannerContext::getSetOrNull(const SetKey & key)
// {
//     auto it = set_key_to_set.find(key);
//     if (it == set_key_to_set.end())
//         return nullptr;

//     return &it->second;
// }

}
