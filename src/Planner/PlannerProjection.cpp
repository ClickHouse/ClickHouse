#include <Planner/PlannerProjection.h>

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/Utils.h>
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
#include <Planner/PlannerProjectionAnalysis.h>
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
#include <Storages/IStorage.h>

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
    /// the block can have no columns, so it does not always know the number of rows
    ProjectionDataSource(const Block & block, size_t rows)
        : ISource(std::make_shared<const Block>(block.cloneEmpty()))
        , chunk(block.getColumns(), rows)
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

/// collects the columns of the table that the expression reads
void collectSourceColumnNodes(const QueryTreeNodePtr & node, const QueryTreeNodePtr & table_node, QueryTreeNodes & result)
{
    if (const auto * column_node = node->as<ColumnNode>(); column_node && column_node->getColumnSourceOrNull() == table_node)
        result.push_back(node);

    for (const auto & child : node->getChildren())
        if (child && !isQueryOrUnionNode(child))
            collectSourceColumnNodes(child, table_node, result);
}


/// a projection stores what a window function reads, not its result
/// returns true if the node has a window function
bool collectExpressionsBeforeWindow(const QueryTreeNodePtr & node, QueryTreeNodes & result)
{
    size_t size_before = result.size();
    const auto * function_node = node->as<FunctionNode>();
    bool has_window_function = function_node && function_node->isWindowCall();
    for (const auto & child : node->getChildren())
        if (child && !isQueryOrUnionNode(child))
            has_window_function |= collectExpressionsBeforeWindow(child, result);

    auto node_type = node->getNodeType();
    bool is_expression = node_type == QueryTreeNodeType::FUNCTION || node_type == QueryTreeNodeType::COLUMN || node_type == QueryTreeNodeType::CONSTANT;
    if (is_expression && !has_window_function)
    {
        /// the part stores the expression, not its arguments
        result.resize(size_before);
        result.push_back(node);
    }

    return has_window_function;
}

}

ProjectionPlanner::ProjectionPlanner(const ASTPtr & query_ast, const StoragePtr & source, const ContextPtr & context)
{
    query_tree = buildProjectionQueryTree(query_ast, source, context);
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
        auto key_name = getStoredName(key_node, query_node.getContext());
        if (!unique_aggregation_keys.emplace(key_name).second)
            continue;

        aggregation_keys.push_back(std::move(key_name));
        planner_key_names.push_back(calculateActionNodeName(key_node, *planner_context));
    }

    NameSet unique_aggregate_names;
    for (const auto & aggregate_function_node : collectAggregateFunctionNodes(query_tree))
    {
        auto aggregate = extractAggregateDescriptions({aggregate_function_node}, *planner_context).front();
        aggregate.column_name = getStoredName(aggregate_function_node, query_node.getContext());
        if (unique_aggregate_names.emplace(aggregate.column_name).second)
            aggregates.push_back(std::move(aggregate));
    }
}

NamesAndTypes ProjectionPlanner::getSourceColumns() const
{
    return planner_context->getTableExpressionDataOrThrow(query_tree->as<QueryNode &>().getJoinTreeNode()).getColumns();
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

    return actions_dag;
}

ActionsDAG ProjectionPlanner::buildKeysAndArgumentsActions()
{
    QueryTreeNodes expressions{query_tree->as<QueryNode &>().getGroupByNode()};
    for (const auto & aggregate_function_node : collectAggregateFunctionNodes(query_tree))
        expressions.push_back(aggregate_function_node->as<FunctionNode &>().getArgumentsNode());

    return buildActions(expressions);
}

ActionsDAG ProjectionPlanner::buildBeforeAggregationActions(ActionsDAG::NodeRawConstPtrs & stored_keys)
{
    auto actions_dag = buildKeysAndArgumentsActions();

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
    actions_dag = ActionsDAG::merge(std::move(rename_dag), std::move(actions_dag));

    /// a stored name is not an output: an output of the planner can have the same name
    for (size_t i = 0; i < aggregation_keys.size(); ++i)
        stored_keys.push_back(&actions_dag.addAlias(actions_dag.findInOutputs(planner_key_names[i]), aggregation_keys[i]));

    return actions_dag;
}

QueryPlan ProjectionPlanner::buildPlanForSource(Block source)
{
    QueryPlan plan;
    /// the functions of the steps refer to the context of the analysis
    plan.addInterpreterContext(planner_context->getQueryContext());

    const auto & query_node = query_tree->as<QueryNode &>();
    /// the plan reads each column under its identifier, so a name of the source cannot be a name that the planner gives
    const auto & source_columns = planner_context->getTableExpressionDataOrThrow(query_node.getJoinTreeNode());
    Block columns;
    for (const auto & column_name : source_columns.getColumnNames())
    {
        /// the query reads a subcolumn as a column, and the block has only the column that contains it
        auto column = source.getColumnOrSubcolumnByName(column_name);
        column.name = source_columns.getColumnIdentifierOrThrow(column_name);
        columns.insert(std::move(column));
    }

    /// TODO: read the block from StorageValues, then the plan reads it as a table
    plan.addStep(std::make_unique<ReadFromPreparedSource>(Pipe(std::make_shared<ProjectionDataSource>(columns, source.rows()))));

    if (query_node.hasWhere())
    {
        auto filter_dag = buildActions({query_node.getWhere()});
        auto filter_column_name = filter_dag.getOutputs().front()->result_name;
        /// the next step reads the source columns again, so the filter keeps the columns that it reads
        for (const auto * input : filter_dag.getInputs())
            filter_dag.getOutputs().push_back(input);

        plan.addStep(std::make_unique<FilterStep>(
            plan.getCurrentHeader(), std::move(filter_dag), filter_column_name, /*remove_filter_column_=*/true));
    }

    return plan;
}

QueryPlan ProjectionPlanner::buildPlanForNormalProjection(Block source)
{
    auto plan = buildPlanForSource(std::move(source));

    const auto & query_node = query_tree->as<QueryNode &>();
    QueryTreeNodes stored_expressions;
    collectExpressionsBeforeWindow(query_node.getProjectionNode(), stored_expressions);
    /// the part is sorted by ORDER BY, so it stores the columns that ORDER BY reads
    if (query_node.hasOrderBy())
        collectSourceColumnNodes(query_node.getOrderByNode(), query_node.getJoinTreeNode(), stored_expressions);
    auto projection_dag = buildActions(stored_expressions);

    /// the outputs are the expressions under the names that the part stores
    NamesWithAliases stored_columns;
    NameSet stored_names;
    NameSet constant_names;
    for (const auto & expression : stored_expressions)
    {
        auto stored_name = getStoredName(expression, planner_context->getQueryContext());
        if (expression->as<ConstantNode>())
            constant_names.insert(stored_name);
        if (stored_names.emplace(stored_name).second)
            stored_columns.emplace_back(calculateActionNodeName(expression, *planner_context), std::move(stored_name));
    }
    projection_dag.project(stored_columns);
    /// `a + NULL` gives a constant column, but the part needs a full one
    for (auto & output : projection_dag.getOutputs())
        if (!constant_names.contains(output->result_name))
            output = &projection_dag.materializeNode(*output);
    /// the projection stores only the columns of its SELECT
    projection_dag.appendInputsForUnusedColumns(*plan.getCurrentHeader());
    plan.addStep(std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(projection_dag)));

    addSubquerySetsStep(plan);
    return plan;
}

QueryPlan ProjectionPlanner::buildPlanForAggregateProjection(Block source)
{
    auto plan = buildPlanForSource(std::move(source));

    plan.addStep(std::make_unique<ExpressionStep>(plan.getCurrentHeader(), buildKeysAndArgumentsActions()));

    /// two keys of the projection can be one expression for the planner
    Names keys;
    NameSet unique_keys;
    for (const auto & key : planner_key_names)
        if (unique_keys.emplace(key).second)
            keys.push_back(key);

    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();
    auto max_threads = getEffectiveMaxThreads(*query_context);
    plan.addStep(std::make_unique<AggregatingStep>(
        plan.getCurrentHeader(),
        getAggregatorParams(query_context, keys, aggregates),
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

    /// the keys get the names that the part stores only after the aggregation, where no other column has a name of the planner
    NamesWithAliases stored_columns;
    for (size_t i = 0; i < aggregation_keys.size(); ++i)
        stored_columns.emplace_back(planner_key_names[i], aggregation_keys[i]);
    for (const auto & aggregate : aggregates)
        stored_columns.emplace_back(aggregate.column_name, String{});

    ActionsDAG rename_dag(plan.getCurrentHeader()->getColumnsWithTypeAndName());
    rename_dag.project(stored_columns);
    plan.addStep(std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(rename_dag)));

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
