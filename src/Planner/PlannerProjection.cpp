#include <Planner/PlannerProjection.h>

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Analyzer/WindowFunctionsUtils.h>
#include <Analyzer/createUniqueAliasesIfNecessary.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/getEffectiveMaxThreads.h>
#include <Planner/CollectSets.h>
#include <Planner/CollectTableExpressionData.h>
#include <Planner/Planner.h>
#include <Planner/PlannerAggregation.h>
#include <Planner/PlannerCorrelatedSubqueries.h>
#include <Planner/TableExpressionData.h>
#include <Planner/Utils.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageDummy.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsUInt64 aggregation_in_order_max_block_bytes;
    extern const SettingsNonZeroUInt64 max_block_size;
}

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

/// the name that a projection part stores the expression under: the name of the expression as the query writes it
String getStoredName(const QueryTreeNodePtr & node)
{
    /// a constant folded from an expression takes the name of that expression
    const auto * constant_node = node->as<ConstantNode>();
    const auto & written_node = constant_node && constant_node->hasSourceExpression() ? constant_node->getSourceExpression() : node;

    /// the analysis makes a new node for a column; a function and a literal keep the expression that the query writes
    if (const auto * column_node = written_node->as<ColumnNode>())
        return column_node->getColumnName();

    if (!written_node->hasOriginalAST())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Expression {} of a projection has no original AST", written_node->formatASTForErrorMessage());

    return written_node->getOriginalAST()->getColumnName();
}


/// a projection stores what a window function reads, not its result
void collectExpressionsBeforeWindow(const QueryTreeNodePtr & node, QueryTreeNodes & result)
{
    auto node_type = node->getNodeType();
    bool is_expression = node_type == QueryTreeNodeType::FUNCTION || node_type == QueryTreeNodeType::COLUMN || node_type == QueryTreeNodeType::CONSTANT;
    if (is_expression && !hasWindowFunctionNodes(node))
    {
        result.push_back(node);
        return;
    }

    for (const auto & child : node->getChildren())
        if (child)
            collectExpressionsBeforeWindow(child, result);
}

}

QueryTreeNodePtr buildProjectionQueryTree(const ASTPtr & query_ast, const StoragePtr & source, const ContextPtr & context)
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
    query_context->setSetting("rewrite_count_distinct_if_with_count_distinct_implementation", false);

    auto source = std::make_shared<StorageDummy>(StorageID{"dummy", "dummy"}, source_columns);
    query_tree = buildProjectionQueryTree(query_ast, source, query_context);
    auto & query_node = query_tree->as<QueryNode &>();

    /// the analysis resolved the tree with the context of the query node
    planner_context = std::make_shared<PlannerContext>(
        query_node.getMutableContext(),
        std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{}),
        SelectQueryOptions{});
    collectSets(query_tree, *planner_context);
    collectTableExpressionData(query_tree, planner_context);

    NameSet unique_aggregation_keys;
    for (const auto & key_node : query_node.getGroupBy().getNodes())
    {
        auto key_name = getStoredName(key_node);
        if (!unique_aggregation_keys.emplace(key_name).second)
            continue;

        aggregation_keys.push_back(std::move(key_name));
        planner_key_names.push_back(calculateActionNodeName(key_node, *planner_context));
    }

    NameSet unique_aggregate_names;
    for (const auto & aggregate_function_node : collectAggregateFunctionNodes(query_tree))
    {
        auto aggregate = extractAggregateDescriptions({aggregate_function_node}, *planner_context).front();
        aggregate.column_name = getStoredName(aggregate_function_node);
        if (unique_aggregate_names.emplace(aggregate.column_name).second)
            aggregates.push_back(std::move(aggregate));
    }
}

ActionsDAG ProjectionPlanner::buildActions(const QueryTreeNodes & expressions)
{
    ActionsDAG actions_dag;
    ColumnNodePtrWithHashSet no_correlated_columns;
    PlannerActionsVisitor actions_visitor(planner_context, no_correlated_columns);

    NameSet output_names;
    for (const auto & expression : expressions)
    {
        auto [nodes, correlated_subtrees] = actions_visitor.visit(actions_dag, expression);
        correlated_subtrees.assertEmpty("in a projection");
        for (const auto * node : nodes)
            if (output_names.emplace(node->result_name).second)
                actions_dag.getOutputs().push_back(node);
    }

    /// the source has the names of the columns, and the actions read the identifiers of the columns
    const auto & source_columns = planner_context->getTableExpressionDataOrThrow(query_tree->as<QueryNode &>().getJoinTreeNode());
    ActionsDAG rename_dag;
    for (const auto * input : actions_dag.getInputs())
    {
        const auto * column_name = source_columns.getColumnNameOrNull(input->result_name);
        if (!column_name)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Input {} of a projection is not a column of its source", input->result_name);

        rename_dag.getOutputs().push_back(&rename_dag.addAlias(rename_dag.addInput(*column_name, input->result_type), input->result_name));
    }

    return ActionsDAG::merge(std::move(rename_dag), std::move(actions_dag));
}

ActionsDAG ProjectionPlanner::buildBeforeAggregationActions()
{
    QueryTreeNodes expressions{query_tree->as<QueryNode &>().getGroupByNode()};
    for (const auto & aggregate_function_node : collectAggregateFunctionNodes(query_tree))
        expressions.push_back(aggregate_function_node->as<FunctionNode &>().getArgumentsNode());

    auto actions_dag = buildActions(expressions);

    /// a key gets the name that the part stores as an alias, the DAG keeps one node for each expression
    NamesWithAliases key_aliases;
    for (size_t i = 0; i < aggregation_keys.size(); ++i)
        if (planner_key_names[i] != aggregation_keys[i])
            key_aliases.emplace_back(planner_key_names[i], aggregation_keys[i]);
    actions_dag.addAliases(key_aliases);

    return actions_dag;
}

QueryPlan ProjectionPlanner::buildPlanForSource(Block source)
{
    QueryPlan plan;
    /// the functions of the steps refer to the context of the analysis
    plan.addInterpreterContext(planner_context->getQueryContext());

    const auto & query_node = query_tree->as<QueryNode &>();
    /// the query reads a subcolumn as a column, and the block has only the column that contains it
    for (const auto & column_name : collectReferencedColumnNames(query_tree, query_node.getJoinTreeNode()))
        if (!source.has(column_name))
            source.insert(source.getColumnOrSubcolumnByName(column_name));

    /// TODO: read the block from StorageValues, then the plan reads it as a table
    plan.addStep(std::make_unique<ReadFromPreparedSource>(
        Pipe(std::make_shared<ProjectionDataSource>(std::make_shared<const Block>(std::move(source))))));

    if (query_node.hasWhere())
    {
        auto filter_dag = buildActions({query_node.getWhere()});
        auto filter_column_name = filter_dag.getOutputs().front()->result_name;
        /// the next step reads the source columns again, so the filter keeps the columns that it reads
        for (const auto * input : filter_dag.getInputs())
            if (input->result_name != filter_column_name)
                filter_dag.getOutputs().push_back(input);

        plan.addStep(std::make_unique<FilterStep>(
            plan.getCurrentHeader(), std::move(filter_dag), filter_column_name, /*remove_filter_column_=*/false));
    }

    return plan;
}

QueryPlan ProjectionPlanner::buildPlanForNormalProjection(Block source)
{
    auto plan = buildPlanForSource(std::move(source));

    QueryTreeNodes stored_expressions;
    collectExpressionsBeforeWindow(query_tree->as<QueryNode &>().getProjectionNode(), stored_expressions);
    auto projection_dag = buildActions(stored_expressions);

    /// the outputs are the expressions under the names that the part stores
    NamesWithAliases stored_columns;
    NameSet stored_names;
    for (const auto & expression : stored_expressions)
    {
        auto stored_name = getStoredName(expression);
        if (stored_names.emplace(stored_name).second)
            stored_columns.emplace_back(calculateActionNodeName(expression, *planner_context), std::move(stored_name));
    }
    projection_dag.project(stored_columns);
    /// the projection stores only the columns of its SELECT
    projection_dag.appendInputsForUnusedColumns(*plan.getCurrentHeader());
    plan.addStep(std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(projection_dag)));

    addSubquerySetsStep(plan);
    return plan;
}

QueryPlan ProjectionPlanner::buildPlanForAggregateProjection(Block source)
{
    auto plan = buildPlanForSource(std::move(source));

    plan.addStep(std::make_unique<ExpressionStep>(plan.getCurrentHeader(), buildBeforeAggregationActions()));

    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();
    auto max_threads = getEffectiveMaxThreads(*query_context);
    plan.addStep(std::make_unique<AggregatingStep>(
        plan.getCurrentHeader(),
        getAggregatorParams(query_context, aggregation_keys, aggregates),
        GroupingSetsParamsList{},
        /*final_=*/false,
        settings[Setting::max_block_size],
        settings[Setting::aggregation_in_order_max_block_bytes],
        /*merge_threads_=*/max_threads,
        /*temporary_data_merge_threads_=*/max_threads,
        /*storage_has_evenly_distributed_read_=*/false,
        /*group_by_use_nulls_=*/false,
        SortDescription{},
        SortDescription{},
        /*should_produce_results_in_order_of_bucket_number_=*/false,
        /*memory_bound_merging_of_aggregation_results_enabled_=*/false,
        /*explicit_sorting_required_for_aggregation_in_order_=*/false));

    addSubquerySetsStep(plan);
    return plan;
}

void ProjectionPlanner::addSubquerySetsStep(QueryPlan & plan)
{
    auto subqueries = planner_context->getPreparedSets().getSubqueries();
    if (subqueries.empty())
        return;

    /// only after the actions: this changes the trees of the subqueries, and the name of a set comes from its tree
    const auto & query_context = planner_context->getQueryContext();
    auto subquery_options = SelectQueryOptions{}.subquery();
    for (auto & subquery : subqueries)
    {
        auto subquery_tree = subquery->detachQueryTree();
        createUniqueAliasesIfNecessary(subquery_tree, query_context);
        Planner subquery_planner(
            subquery_tree,
            subquery_options,
            std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{}));
        subquery_planner.buildQueryPlanIfNeeded();
        subquery->setQueryPlan(std::make_unique<QueryPlan>(std::move(subquery_planner).extractQueryPlan()));
    }

    addCreatingSetsStep(plan, std::move(subqueries), query_context);
}

}
