#include <Analyzer/Resolve/IdentifierResolveScope.h>

#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool group_by_use_nulls;
    extern const SettingsBool join_use_nulls;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

IdentifierResolveScope::IdentifierResolveScope(QueryTreeNodePtr scope_node_, IdentifierResolveScope * parent_scope_)
    : scope_node(std::move(scope_node_))
    , parent_scope(parent_scope_)
{
    if (parent_scope)
    {
        subquery_depth = parent_scope->subquery_depth;
        context = parent_scope->context;
        projection_mask_map = parent_scope->projection_mask_map;
    }
    else
        projection_mask_map = std::make_shared<std::map<IQueryTreeNode::Hash, size_t>>();

    if (auto * union_node = scope_node->as<UnionNode>())
    {
        context = union_node->getContext();
    }
    else if (auto * query_node = scope_node->as<QueryNode>())
    {
        context = query_node->getContext();
        group_by_use_nulls = context->getSettingsRef()[Setting::group_by_use_nulls]
            && (query_node->isGroupByWithGroupingSets() || query_node->isGroupByWithRollup() || query_node->isGroupByWithCube());
    }

    if (context)
        join_use_nulls = context->getSettingsRef()[Setting::join_use_nulls];
    else if (parent_scope)
        join_use_nulls = parent_scope->join_use_nulls;

    aliases.alias_name_to_expression_node = &aliases.alias_name_to_expression_node_before_group_by;
}

[[maybe_unused]] const IdentifierResolveScope * IdentifierResolveScope::getNearestQueryScope() const
{
    const IdentifierResolveScope * scope_to_check = this;
    while (scope_to_check != nullptr)
    {
        if (scope_to_check->scope_node->getNodeType() == QueryTreeNodeType::QUERY)
            break;

        scope_to_check = scope_to_check->parent_scope;
    }

    return scope_to_check;
}

IdentifierResolveScope * IdentifierResolveScope::getNearestQueryScope()
{
    IdentifierResolveScope * scope_to_check = this;
    while (scope_to_check != nullptr)
    {
        if (scope_to_check->scope_node->getNodeType() == QueryTreeNodeType::QUERY)
            break;

        scope_to_check = scope_to_check->parent_scope;
    }

    return scope_to_check;
}

AnalysisTableExpressionData & IdentifierResolveScope::getTableExpressionDataOrThrow(const QueryTreeNodePtr & table_expression_node)
{
    auto it = table_expression_node_to_data.find(table_expression_node);
    if (it == table_expression_node_to_data.end())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Table expression {} data must be initialized. In scope {}",
            table_expression_node->formatASTForErrorMessage(),
            scope_node->formatASTForErrorMessage());
    }

    return it->second;
}

const AnalysisTableExpressionData & IdentifierResolveScope::getTableExpressionDataOrThrow(const QueryTreeNodePtr & table_expression_node) const
{
    auto it = table_expression_node_to_data.find(table_expression_node);
    if (it == table_expression_node_to_data.end())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Table expression {} data must be initialized. In scope {}",
            table_expression_node->formatASTForErrorMessage(),
            scope_node->formatASTForErrorMessage());
    }

    return it->second;
}

void IdentifierResolveScope::pushExpressionNode(const QueryTreeNodePtr & node)
{
    bool had_aggregate_function = expressions_in_resolve_process_stack.hasAggregateFunction();
    expressions_in_resolve_process_stack.push(node);
    if (group_by_use_nulls && had_aggregate_function != expressions_in_resolve_process_stack.hasAggregateFunction())
        aliases.alias_name_to_expression_node = &aliases.alias_name_to_expression_node_before_group_by;
}

void IdentifierResolveScope::popExpressionNode()
{
    bool had_aggregate_function = expressions_in_resolve_process_stack.hasAggregateFunction();
    expressions_in_resolve_process_stack.pop();
    if (group_by_use_nulls && had_aggregate_function != expressions_in_resolve_process_stack.hasAggregateFunction())
        aliases.alias_name_to_expression_node = &aliases.alias_name_to_expression_node_after_group_by;
}

/// Dump identifier resolve scope
[[maybe_unused]] void IdentifierResolveScope::dump(WriteBuffer & buffer) const
{
    buffer << "Scope node " << scope_node->formatASTForErrorMessage() << '\n';
    buffer << "Identifier lookup to resolve state " << identifier_lookup_to_resolve_state.size() << '\n';
    for (const auto & [identifier, state] : identifier_lookup_to_resolve_state)
    {
        buffer << "Identifier " << identifier.dump() << " resolve result ";
        state.resolve_result.dump(buffer);
        buffer << '\n';
    }

    buffer << "Expression argument name to node " << expression_argument_name_to_node.size() << '\n';
    for (const auto & [alias_name, node] : expression_argument_name_to_node)
        buffer << "Alias name " << alias_name << " node " << node->formatASTForErrorMessage() << '\n';

    buffer << "Alias name to expression node table size " << aliases.alias_name_to_expression_node->size() << '\n';
    for (const auto & [alias_name, node] : *aliases.alias_name_to_expression_node)
        buffer << "Alias name " << alias_name << " expression node " << node->dumpTree() << '\n';

    buffer << "Alias name to function node table size " << aliases.alias_name_to_lambda_node.size() << '\n';
    for (const auto & [alias_name, node] : aliases.alias_name_to_lambda_node)
        buffer << "Alias name " << alias_name << " lambda node " << node->formatASTForErrorMessage() << '\n';

    buffer << "Alias name to table expression node table size " << aliases.alias_name_to_table_expression_node.size() << '\n';
    for (const auto & [alias_name, node] : aliases.alias_name_to_table_expression_node)
        buffer << "Alias name " << alias_name << " node " << node->formatASTForErrorMessage() << '\n';

    buffer << "CTE name to query node table size " << cte_name_to_query_node.size() << '\n';
    for (const auto & [cte_name, node] : cte_name_to_query_node)
        buffer << "CTE name " << cte_name << " node " << node->formatASTForErrorMessage() << '\n';

    buffer << "WINDOW name to window node table size " << window_name_to_window_node.size() << '\n';
    for (const auto & [window_name, node] : window_name_to_window_node)
        buffer << "CTE name " << window_name << " node " << node->formatASTForErrorMessage() << '\n';

    buffer << "Nodes with duplicated aliases size " << aliases.nodes_with_duplicated_aliases.size() << '\n';
    for (const auto & node : aliases.nodes_with_duplicated_aliases)
        buffer << "Alias name " << node->getAlias() << " node " << node->formatASTForErrorMessage() << '\n';

    buffer << "Expression resolve process stack " << '\n';
    expressions_in_resolve_process_stack.dump(buffer);

    buffer << "Table expressions in resolve process size " << table_expressions_in_resolve_process.size() << '\n';
    for (const auto & node : table_expressions_in_resolve_process)
        buffer << "Table expression " << node->formatASTForErrorMessage() << '\n';

    buffer << "Non cached identifier lookups during expression resolve " << non_cached_identifier_lookups_during_expression_resolve.size() << '\n';
    for (const auto & identifier_lookup : non_cached_identifier_lookups_during_expression_resolve)
        buffer << "Identifier lookup " << identifier_lookup.dump() << '\n';

    buffer << "Table expression node to data " << table_expression_node_to_data.size() << '\n';
    for (const auto & [table_expression_node, table_expression_data] : table_expression_node_to_data)
        buffer << "Table expression node " << table_expression_node->formatASTForErrorMessage() << " data " << table_expression_data.dump() << '\n';

    buffer << "Use identifier lookup to result cache " << use_identifier_lookup_to_result_cache << '\n';
    buffer << "Subquery depth " << subquery_depth << '\n';
}

[[maybe_unused]] String IdentifierResolveScope::dump() const
{
    WriteBufferFromOwnString buffer;
    dump(buffer);

    return buffer.str();
}
}
