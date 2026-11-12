#include <Storages/ProjectionPlanner.h>

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/TableNode.h>
#include <Interpreters/Context.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Planner/CollectSets.h>
#include <Planner/PlannerAggregation.h>
#include <Planner/PlannerCorrelatedSubqueries.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageDummy.h>

namespace DB
{

namespace
{

/// Remembers the names of the functions of `node` as they are written in the projection query.
void collectUnresolvedNodeNames(const QueryTreeNodePtr & node, QueryTreeNodeToName & result)
{
    if (node->getNodeType() == QueryTreeNodeType::FUNCTION)
    {
        /// A subquery is named by the hash of its AST, which `toAST` does not reproduce.
        if (node->hasOriginalAST())
        {
            result.emplace(node, node->getOriginalAST()->getColumnName());
        }
        else
        {
            ConvertToASTOptions ast_options;
            ast_options.add_cast_for_constants = false;
            ast_options.fully_qualified_identifiers = false;
            result.emplace(node, node->toAST(ast_options)->getColumnName());
        }
    }

    for (const auto & child : node->getChildren())
        if (child)
            collectUnresolvedNodeNames(child, result);
}

/// collects the stored name of each expression in node
void collectStoredNodeNames(const QueryTreeNodePtr & node, const QueryTreeNodeToName & unresolved_node_names, QueryTreeNodeToName & result)
{
    auto node_type = node->getNodeType();
    if (node_type == QueryTreeNodeType::COLUMN || node_type == QueryTreeNodeType::CONSTANT || node_type == QueryTreeNodeType::FUNCTION)
    {
        /// a constant folded from an expression takes the name of that expression
        const QueryTreeNodePtr * written_node = &node;
        if (const auto * constant_node = node->as<ConstantNode>(); constant_node && constant_node->hasSourceExpression())
            written_node = &constant_node->getSourceExpression();

        if (auto it = unresolved_node_names.find(*written_node); it != unresolved_node_names.end())
        {
            result.emplace(node, it->second);
        }
        else
        {
            /// the analysis made this node: a column has no table prefix and a constant is a plain literal
            ConvertToASTOptions ast_options;
            ast_options.use_source_expression_for_constants = true;
            ast_options.add_cast_for_constants = false;
            ast_options.fully_qualified_identifiers = false;
            result.emplace(node, node->toAST(ast_options)->getColumnName());
        }
    }

    for (const auto & child : node->getChildren())
        if (child)
            collectStoredNodeNames(child, unresolved_node_names, result);
}

}

QueryTreeNodePtr buildProjectionQueryTree(
    const ASTPtr & query_ast,
    const StoragePtr & source,
    const ContextPtr & context,
    QueryTreeNodeToName * unresolved_node_names)
{
    auto query_tree = buildQueryTree(query_ast, context);
    auto & query_node = query_tree->as<QueryNode &>();

    /// A projection query is stored without a `FROM` clause, so its join tree is a reference to
    /// `system.one` which has to be replaced with the table expression providing the input columns.
    query_node.getJoinTreeNode() = std::make_shared<TableNode>(source, context);

    /** The Analyzer unwraps a `tuple` at the top level of `GROUP BY` into separate keys, while a
      * projection stores such a key as a single column named after the whole tuple. Wrap the key into
      * one more `tuple` so that unwrapping gives back the key the projection stores.
      */
    if (query_node.hasGroupBy() && !query_node.isGroupByWithGroupingSets())
    {
        for (auto & group_by_key_node : query_node.getGroupBy().getNodes())
        {
            const auto * function_node = group_by_key_node->as<FunctionNode>();
            if (!function_node || function_node->getFunctionName() != "tuple")
                continue;

            auto wrapped_key_node = std::make_shared<FunctionNode>("tuple");
            wrapped_key_node->getArguments().getNodes().push_back(group_by_key_node);
            group_by_key_node = std::move(wrapped_key_node);
        }
    }

    if (unresolved_node_names)
        collectUnresolvedNodeNames(query_tree, *unresolved_node_names);

    QueryTreePassManager query_tree_pass_manager(context);
    addQueryTreePasses(query_tree_pass_manager, /*only_analyze=*/true);
    /// Optimization passes are not applied because some of them are invalid for projections.
    /// Example: 'SELECT min(c0), max(c0), count() GROUP BY -c0' for minmax_count projection can be rewritten to
    /// 'SELECT min(c0), max(c0), count() GROUP BY c0' which is incorrect cause we store a column '-c0' in projection.
    query_tree_pass_manager.runOnlyResolve(query_tree);

    return query_tree;
}

ProjectionPlanner::ProjectionPlanner(const ASTPtr & query_ast, const ColumnsDescription & source_columns, const ContextPtr & context)
{
    auto query_context = Context::createCopy(context);
    /// the stored query is final, so the settings of the session must not rewrite it again
    query_context->setSetting("enable_positional_arguments", false);
    query_context->setSetting("rewrite_in_to_join", false);

    auto source = std::make_shared<StorageDummy>(StorageID{"dummy", "dummy"}, source_columns);
    QueryTreeNodeToName unresolved_node_names;
    query_tree = buildProjectionQueryTree(query_ast, source, query_context, &unresolved_node_names);
    auto & query_node = query_tree->as<QueryNode &>();

    /// the analysis resolved the tree with the context of the query node
    planner_context = std::make_shared<PlannerContext>(
        query_node.getMutableContext(),
        std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{}),
        SelectQueryOptions{});
    collectSets(query_tree, *planner_context);
    collectStoredNodeNames(query_tree, unresolved_node_names, node_to_name);

    NameSet unique_aggregation_keys;
    for (const auto & key_node : query_node.getGroupBy().getNodes())
    {
        auto key_name = calculateActionNodeName(key_node, *planner_context, node_to_name, /*use_column_identifier_as_action_node_name=*/false);
        if (unique_aggregation_keys.emplace(key_name).second)
            aggregation_keys.push_back(std::move(key_name));
    }

    aggregates = extractAggregateDescriptions(
        collectAggregateFunctionNodes(query_tree), *planner_context, node_to_name, /*use_column_identifier_as_action_node_name=*/false);
}

ActionsDAG ProjectionPlanner::buildBeforeAggregationActions() const
{
    ActionsDAG actions_dag;
    ColumnNodePtrWithHashSet no_correlated_columns;
    PlannerActionsVisitor actions_visitor(planner_context, no_correlated_columns, /*use_column_identifier_as_action_node_name=*/false, node_to_name);

    NameSet output_names;
    auto add_outputs = [&](const QueryTreeNodePtr & expressions)
    {
        auto [nodes, correlated_subtrees] = actions_visitor.visit(actions_dag, expressions);
        correlated_subtrees.assertEmpty("in a projection");
        for (const auto * node : nodes)
            if (output_names.emplace(node->result_name).second)
                actions_dag.getOutputs().push_back(node);
    };

    add_outputs(query_tree->as<QueryNode &>().getGroupByNode());
    for (const auto & aggregate_function_node : collectAggregateFunctionNodes(query_tree))
        add_outputs(aggregate_function_node->as<FunctionNode &>().getArgumentsNode());

    return actions_dag;
}

}
