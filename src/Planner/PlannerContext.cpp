#include <Planner/PlannerContext.h>

#include <Analyzer/TableNode.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

GlobalPlannerContext::SetKey GlobalPlannerContext::getSetKey(const IQueryTreeNode * set_source_node) const
{
    auto set_source_hash = set_source_node->getTreeHash();
    return "__set_" + toString(set_source_hash.first) + '_' + toString(set_source_hash.second);
}

void GlobalPlannerContext::registerSet(const SetKey & key, SetPtr set)
{
    set_key_to_set.emplace(key, std::move(set));
}

SetPtr GlobalPlannerContext::getSetOrNull(const SetKey & key) const
{
    auto it = set_key_to_set.find(key);
    if (it == set_key_to_set.end())
        return nullptr;

    return it->second;
}

SetPtr GlobalPlannerContext::getSetOrThrow(const SetKey & key) const
{
    auto it = set_key_to_set.find(key);
    if (it == set_key_to_set.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "No set is registered for key {}",
            key);

    return it->second;
}

void GlobalPlannerContext::registerSubqueryNodeForSet(const SetKey & key, SubqueryNodeForSet subquery_node_for_set)
{
    auto node_type = subquery_node_for_set.subquery_node->getNodeType();
    if (node_type != QueryTreeNodeType::QUERY &&
        node_type != QueryTreeNodeType::UNION)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Invalid node for set table expression. Expected query or union. Actual {}",
            subquery_node_for_set.subquery_node->formatASTForErrorMessage());
    if (!subquery_node_for_set.set)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Set must be initialized");

    set_key_to_subquery_node.emplace(key, std::move(subquery_node_for_set));
}

PlannerContext::PlannerContext(ContextPtr query_context_, GlobalPlannerContextPtr global_planner_context_)
    : query_context(std::move(query_context_))
    , global_planner_context(std::move(global_planner_context_))
{}

ColumnIdentifier PlannerContext::getColumnUniqueIdentifier(const IQueryTreeNode * column_source_node, std::string column_name)
{
    auto column_unique_prefix = "__column_" + std::to_string(column_identifier_counter);
    ++column_identifier_counter;

    std::string table_expression_identifier;
    auto table_expression_identifier_it = table_expression_node_to_identifier.find(column_source_node);
    if (table_expression_identifier_it != table_expression_node_to_identifier.end())
        table_expression_identifier = table_expression_identifier_it->second;

    std::string debug_identifier_suffix;

    if (column_source_node->hasAlias())
    {
        debug_identifier_suffix += column_source_node->getAlias();
    }
    else if (const auto * table_source_node = column_source_node->as<TableNode>())
    {
        debug_identifier_suffix += table_source_node->getStorageID().getFullNameNotQuoted();
    }
    else
    {
        auto column_source_node_type = column_source_node->getNodeType();
        if (column_source_node_type == QueryTreeNodeType::JOIN)
            debug_identifier_suffix += "join";
        else if (column_source_node_type == QueryTreeNodeType::ARRAY_JOIN)
            debug_identifier_suffix += "array_join";
        else if (column_source_node_type == QueryTreeNodeType::TABLE_FUNCTION)
            debug_identifier_suffix += "table_function";
        else if (column_source_node_type == QueryTreeNodeType::QUERY)
            debug_identifier_suffix += "subquery";

        if (!table_expression_identifier.empty())
            debug_identifier_suffix += '_' + table_expression_identifier;
    }

    if (!column_name.empty())
        debug_identifier_suffix += '.' + column_name;

    if (!debug_identifier_suffix.empty())
        column_unique_prefix += '_' + debug_identifier_suffix;

    return column_unique_prefix;
}

void PlannerContext::registerColumnNode(const IQueryTreeNode * column_node, const ColumnIdentifier & column_identifier)
{
    assert(column_node->getNodeType() == QueryTreeNodeType::COLUMN);
    column_node_to_column_identifier.emplace(column_node, column_identifier);
}

const ColumnIdentifier & PlannerContext::getColumnNodeIdentifierOrThrow(const IQueryTreeNode * column_node) const
{
    assert(column_node->getNodeType() == QueryTreeNodeType::COLUMN);

    auto it = column_node_to_column_identifier.find(column_node);
    if (it == column_node_to_column_identifier.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Column identifier is not initialized for column {}",
            column_node->formatASTForErrorMessage());

    return it->second;
}

const ColumnIdentifier * PlannerContext::getColumnNodeIdentifierOrNull(const IQueryTreeNode * column_node) const
{
    assert(column_node->getNodeType() == QueryTreeNodeType::COLUMN);

    auto it = column_node_to_column_identifier.find(column_node);
    if (it == column_node_to_column_identifier.end())
        return nullptr;

    return &it->second;
}

}
