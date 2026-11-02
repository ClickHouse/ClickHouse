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
        global_with_aliases = parent_scope->global_with_aliases;
    }
    else
        projection_mask_map = std::make_shared<std::map<IQueryTreeNode::Hash, size_t>>();

    if (auto * union_node = scope_node->as<UnionNode>())
    {
        if (parent_scope && parent_scope->context)
            union_node->getMutableContext()->setDistributed(parent_scope->context->isDistributed());

        context = union_node->getContext();
    }
    else if (auto * query_node = scope_node->as<QueryNode>())
    {
        if (parent_scope && parent_scope->context)
            query_node->getMutableContext()->setDistributed(parent_scope->context->isDistributed());

        context = query_node->getContext();
        group_by_use_nulls = context->getSettingsRef()[Setting::group_by_use_nulls]
            && (query_node->isGroupByWithGroupingSets() || query_node->isGroupByWithRollup() || query_node->isGroupByWithCube());
    }

    if (context)
        join_use_nulls = context->getSettingsRef()[Setting::join_use_nulls];
    else if (parent_scope)
        join_use_nulls = parent_scope->join_use_nulls;
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
    expressions_in_resolve_process_stack.push(node);
}

void IdentifierResolveScope::popExpressionNode()
{
    expressions_in_resolve_process_stack.pop();
}

namespace
{

void dump_mapping(WriteBuffer & buffer, const String & mapping_name, const std::unordered_map<std::string, QueryTreeNodePtr> & mapping)
{
    if (mapping.empty())
        return;

    buffer << mapping_name << " table size: " << mapping.size() << '\n';
    for (const auto & [alias_name, node] : mapping)
        buffer << " { '" << alias_name << "' : " << node->formatASTForErrorMessage() << " }\n";
}

void dump_list(WriteBuffer & buffer, const String & list_name, const std::ranges::viewable_range auto & list)
{
    if (list.empty())
        return;

    buffer << list_name << " table size: " << list.size() << '\n';
    for (const auto & node : list)
        buffer << " { '" << node->getAlias() << "' : " << node->formatASTForErrorMessage() << " }\n";
}

}

/// Dump identifier resolve scope
[[maybe_unused]] void IdentifierResolveScope::dump(WriteBuffer & buffer) const
{
    buffer << "Scope node " << scope_node->formatConvertedASTForErrorMessage() << '\n';

    buffer << "Identifier lookup to resolve state " << identifier_in_lookup_process.size() << '\n';
    for (const auto & [identifier, state] : identifier_in_lookup_process)
    {
        buffer << " { '" << identifier.dump() << "' : ";
        buffer << state.count;
        buffer << " }\n";
    }

    dump_mapping(buffer, "Expression argument name to node", expression_argument_name_to_node);
    dump_mapping(buffer, "Alias name to expression node", aliases.alias_name_to_expression_node);
    dump_mapping(buffer, "Alias name to function node", aliases.alias_name_to_lambda_node);
    dump_mapping(buffer, "Alias name to table expression node", aliases.alias_name_to_table_expression_node);
    dump_mapping(buffer, "CTE name to query node", cte_name_to_query_node);
    dump_mapping(buffer, "WINDOW name to window node", window_name_to_window_node);

    dump_list(buffer, "Nodes with duplicated aliases size ", aliases.nodes_with_duplicated_aliases);
    dump_list(buffer, "Nodes to remove aliases ", aliases.node_to_remove_aliases);

    expressions_in_resolve_process_stack.dump(buffer);

    if (!table_expressions_in_resolve_process.empty())
    {
        buffer << "Table expressions in resolve process size " << table_expressions_in_resolve_process.size() << '\n';
        for (const auto & node : table_expressions_in_resolve_process)
            buffer << " { " << node->formatASTForErrorMessage() << " }\n";
    }

    buffer << "Table expression node to data: " << table_expression_node_to_data.size() << '\n';
    for (const auto & [table_expression_node, table_expression_data] : table_expression_node_to_data)
        buffer << " { " << table_expression_node->formatASTForErrorMessage() << " data:\n  " << table_expression_data.dump() << " }\n";

    dump_list(buffer, "Registered table expression nodes", registered_table_expression_nodes);

    buffer << "Subquery depth " << subquery_depth << '\n';
}

[[maybe_unused]] String IdentifierResolveScope::dump() const
{
    WriteBufferFromOwnString buffer;
    dump(buffer);

    return buffer.str();
}
}
