#include <Planner/PlannerProjections.h>

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/createUniqueAliasesIfNecessary.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Planner/CollectSets.h>
#include <Planner/Planner.h>
#include <Planner/PlannerAggregation.h>
#include <Planner/PlannerCorrelatedSubqueries.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageDummy.h>

namespace DB
{

namespace
{

/// Provides source data for the projection pipeline
class ProjectionDataSource final : public ISource
{
public:
    explicit ProjectionDataSource(SharedHeader block)
        : ISource(std::make_shared<const Block>(block->cloneEmpty()))
        , chunk(block->getColumns(), block->rows())
    {
    }

    String getName() const override { return "ProjectionDataSource"; }

    /// Avoid tracking read progress for projection calculation pipeline.
    std::optional<ReadProgress> getReadProgress() override { return std::nullopt; }

protected:
    Chunk generate() override { return std::move(chunk); }

private:
    Chunk chunk;
};

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

QueryTreeNodePtr buildProjectionQueryTreeImpl(
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

}

QueryTreeNodePtr buildProjectionQueryTree(const ASTPtr & query_ast, const StoragePtr & source, const ContextPtr & context)
{
    return buildProjectionQueryTreeImpl(query_ast, source, context, nullptr);
}

ProjectionPlanner::ProjectionPlanner(const ASTPtr & query_ast, const ColumnsDescription & source_columns, const ContextPtr & context)
{
    auto query_context = Context::createCopy(context);
    /// the stored query is final, so the settings of the session must not rewrite it again
    query_context->setSetting("enable_positional_arguments", false);
    query_context->setSetting("rewrite_in_to_join", false);
    query_context->setSetting("rewrite_count_distinct_if_with_count_distinct_implementation", false);

    auto source = std::make_shared<StorageDummy>(StorageID{"dummy", "dummy"}, source_columns);
    QueryTreeNodeToName unresolved_node_names;
    query_tree = buildProjectionQueryTreeImpl(query_ast, source, query_context, &unresolved_node_names);
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
        auto key_name = calculateActionNodeName(key_node, *planner_context, node_to_name);
        if (unique_aggregation_keys.emplace(key_name).second)
            aggregation_keys.push_back(std::move(key_name));
    }

    aggregates = extractAggregateDescriptions(collectAggregateFunctionNodes(query_tree), *planner_context, node_to_name);
}

ActionsDAG ProjectionPlanner::buildActions(const QueryTreeNodes & expressions)
{
    ActionsDAG actions_dag;
    ColumnNodePtrWithHashSet no_correlated_columns;
    PlannerActionsVisitor actions_visitor(planner_context, no_correlated_columns, /*use_column_identifier_as_action_node_name=*/false, &node_to_name);

    NameSet output_names;
    for (const auto & expression : expressions)
    {
        auto [nodes, correlated_subtrees] = actions_visitor.visit(actions_dag, expression);
        correlated_subtrees.assertEmpty("in a projection");
        for (const auto * node : nodes)
            if (output_names.emplace(node->result_name).second)
                actions_dag.getOutputs().push_back(node);
    }

    return actions_dag;
}

ActionsDAG ProjectionPlanner::buildBeforeAggregationActions()
{
    QueryTreeNodes expressions{query_tree->as<QueryNode &>().getGroupByNode()};
    for (const auto & aggregate_function_node : collectAggregateFunctionNodes(query_tree))
        expressions.push_back(aggregate_function_node->as<FunctionNode &>().getArgumentsNode());

    return buildActions(expressions);
}

Pipe ProjectionPlanner::buildPipe(Block source)
{
    const auto & query_context = planner_context->getQueryContext();
    const auto & query_node = query_tree->as<QueryNode &>();

    Pipe pipe(std::make_shared<ProjectionDataSource>(std::make_shared<const Block>(std::move(source))));
    ExpressionActionsSettings actions_settings(query_context, CompileExpressions::yes);

    if (query_node.hasWhere())
    {
        auto filter_dag = buildActions({query_node.getWhere()});
        auto filter_column_name = filter_dag.getOutputs().front()->result_name;
        /// the next step reads the source columns again, so the filter keeps the columns that it reads
        for (const auto * input : filter_dag.getInputs())
            if (input->result_name != filter_column_name)
                filter_dag.getOutputs().push_back(input);

        auto filter = std::make_shared<ExpressionActions>(std::move(filter_dag), actions_settings);
        pipe.addSimpleTransform([&](const SharedHeader & header)
        {
            return std::make_shared<FilterTransform>(header, filter, filter_column_name, /*remove_filter_column=*/false);
        });
    }

    bool is_aggregate = query_node.hasGroupBy() || !aggregates.empty();
    auto expression_dag = is_aggregate ? buildBeforeAggregationActions() : buildActions({query_node.getProjectionNode()});
    /// a normal projection stores only the columns of its SELECT
    if (!is_aggregate)
        expression_dag.appendInputsForUnusedColumns(pipe.getHeader());

    auto expression = std::make_shared<ExpressionActions>(std::move(expression_dag), actions_settings);
    pipe.addSimpleTransform([&](const SharedHeader & header) { return std::make_shared<ExpressionTransform>(header, expression); });

    if (is_aggregate)
    {
        auto params = std::make_shared<AggregatingTransformParams>(
            pipe.getSharedHeader(), getAggregatorParams(query_context, aggregation_keys, aggregates), /*final=*/false);
        pipe.addSimpleTransform([&](const SharedHeader & header) { return std::make_shared<AggregatingTransform>(header, params, nullptr); });
    }

    /// only after the actions: this changes the trees of the subqueries, and the name of a set comes from its tree
    buildSubquerySets();

    return pipe;
}

void ProjectionPlanner::buildSubquerySets()
{
    const auto & query_context = planner_context->getQueryContext();
    auto subquery_options = SelectQueryOptions{}.subquery();
    for (auto & subquery : planner_context->getPreparedSets().getSubqueries())
    {
        auto subquery_tree = subquery->detachQueryTree();
        createUniqueAliasesIfNecessary(subquery_tree, query_context);
        Planner subquery_planner(
            subquery_tree,
            subquery_options,
            std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{}));
        subquery_planner.buildQueryPlanIfNeeded();
        subquery->setQueryPlan(std::make_unique<QueryPlan>(std::move(subquery_planner).extractQueryPlan()));
        subquery->buildSetInplace(query_context);
    }
}

}
