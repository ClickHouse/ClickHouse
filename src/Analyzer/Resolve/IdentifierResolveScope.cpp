#include <Analyzer/Resolve/IdentifierResolveScope.h>

#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>
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

        if (parent_scope->identifier_resolve_cache_force_disabled)
            disableIdentifierCachePermanently();
        else if (!parent_scope->identifier_resolve_cache_enabled)
            disableIdentifierCache();
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

/// Whether `node`'s subtree (including `node` itself) contains any node from `nodes`,
/// using the set's hash-ignore-types comparison.
bool subtreeContainsAnyNode(const QueryTreeNodePtr & node, const QueryTreeNodePtrWithHashIgnoreTypesSet & nodes)
{
    if (nodes.contains(node))
        return true;
    for (const auto & child : node->getChildren())
        if (child && subtreeContainsAnyNode(child, nodes))
            return true;
    return false;
}

}

bool IdentifierResolveScope::canCacheIdentifier(
    const IdentifierLookup & lookup,
    const IdentifierResolveContext & resolve_context) const
{
    if (!identifier_resolve_cache_enabled)
        return false;

    /// Cannot use cache when the resolve context differs from the default — a cached
    /// result from a permissive lookup must not be reused in a stricter context that
    /// disables CTE, database catalog, niladic function, or other resolution paths.
    if (!resolve_context.isDefaultContext())
        return false;

    /// Cannot use cache when there is an expression being resolved that has the
    /// same alias as the identifier we're looking up. Caching in this situation
    /// would cause transitive aliases to resolve incorrectly.
    /// Example: SELECT (id + 2) AS id, id AS b FROM test_table;
    /// Here, `id` inside `(id + 2)` resolves to test_table.id, but `id` in `id AS b`
    /// should resolve to the alias `(id + 2)`. Caching the first result would break the second.
    /// Match on the first identifier component, mirroring alias binding in
    /// tryResolveIdentifierFromAliases: a compound lookup like `value.a` binds to an
    /// in-flight alias named `value`, so it must be excluded from the cache as well.
    if (expressions_in_resolve_process_stack.hasExpressionWithAlias(lookup.identifier.front()))
        return false;

    return true;
}

std::optional<IdentifierResolveResult> IdentifierResolveScope::findCachedIdentifier(
    const IdentifierLookup & lookup,
    const IdentifierResolveContext & resolve_context) const
{
    if (!canCacheIdentifier(lookup, resolve_context))
        return {};

    auto it = identifier_resolve_cache.find(lookup);
    if (it == identifier_resolve_cache.end())
        return {};

    return it->second;
}

void IdentifierResolveScope::tryCacheIdentifier(
    const IdentifierLookup & lookup,
    const IdentifierResolveResult & result,
    const IdentifierResolveContext & resolve_context)
{
    if (!canCacheIdentifier(lookup, resolve_context))
        return;

    /// Only cache node types that are expensive to clone (have children).
    /// ColumnNode, ConstantNode, IdentifierNode are cheap O(1) clones — not worth
    /// the COW complexity of sharing them.
    auto node_type = result.resolved_identifier->getNodeType();
    switch (node_type)
    {
        case QueryTreeNodeType::FUNCTION:
        case QueryTreeNodeType::QUERY:
        case QueryTreeNodeType::UNION:
        case QueryTreeNodeType::LAMBDA:
        case QueryTreeNodeType::LIST:
            break;
        default:
            return;
    }

    /// Don't cache a result whose subtree contains a `nullable_group_by_keys` node — its
    /// type depends on context: non-nullable inside aggregate functions, nullable outside
    /// (see convertToNullable calls after resolution). This must cover not just the key
    /// itself but any function alias over it (e.g. `k + 1 AS a`), whose resolved subtree
    /// references the key and would otherwise be shared across contexts with the wrong type.
    if (!nullable_group_by_keys.empty() && subtreeContainsAnyNode(result.resolved_identifier, nullable_group_by_keys))
        return;

    identifier_resolve_cache[lookup] = result;
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
