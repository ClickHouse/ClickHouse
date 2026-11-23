#include <Planner/PlannerProjection.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/Passes/NormalizeCountVariantsPass.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/TableNode.h>
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
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
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
    extern const SettingsBool prefer_column_name_to_alias;
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

ASTPtr makeStoredNameAST(const QueryTreeNodePtr & node, const ContextPtr & context);

ASTPtr makeStoredNameASTs(const QueryTreeNodes & nodes, const ContextPtr & context)
{
    auto list = make_intrusive<ASTExpressionList>();
    for (const auto & node : nodes)
        list->children.push_back(makeStoredNameAST(node, context));
    return list;
}

/// Makes the expression that the old analysis kept in the query of a projection. A projection part stores
/// a column under the name of this expression, so the AST gives the name, and this function only selects what to name.
ASTPtr makeStoredNameAST(const QueryTreeNodePtr & node, const ContextPtr & context)
{
    if (const auto * column_node = node->as<ColumnNode>())
        return make_intrusive<ASTIdentifier>(column_node->getColumnName());

    if (const auto * constant_node = node->as<ConstantNode>())
    {
        /// a constant folded from an expression of the query has the name of that expression
        if (constant_node->hasSourceExpression() && constant_node->getSourceExpression()->hasOriginalAST())
            return makeStoredNameAST(constant_node->getSourceExpression(), context);

        /// a constant that the analysis makes has the name of the value
        if (constant_node->hasSourceExpression() || !node->hasOriginalAST())
            return make_intrusive<ASTLiteral>(constant_node->getValue());
    }

    if (const auto * function_node = node->as<FunctionNode>())
    {
        /// The node of an aggregate function has the name of the function that the factory gives, which can be
        /// another function (`any(b) RESPECT NULLS`, `sum(NULL)`, `median(a)`). The stored name has the name that the
        /// analysis takes for the name of the query. This is not for a call that the analysis replaces
        /// with a call of other arguments (`count(*)`, `sum(1)`).
        const auto * written_function
            = function_node->isAggregationCall() && node->hasOriginalAST() ? node->getOriginalAST()->as<ASTFunction>() : nullptr;
        bool has_written_name = written_function
            && (written_function->arguments ? written_function->arguments->children.size() : 0) == function_node->getArguments().getNodes().size();

        auto function = make_intrusive<ASTFunction>();
        function->setNullsAction(has_written_name ? written_function->getNullsAction() : function_node->getNullsAction());
        function->name = has_written_name
            ? getAggregateFunctionCanonicalNameIfAny(QueryAnalyzer::rewriteAggregateFunctionNameIfNeeded(
                written_function->name, function->getNullsAction(), function_node->getArgumentTypes(), function_node->getAggregateFunction()->getParameters(), context))
            : function_node->getFunctionName();

        if (!function_node->getParameters().getNodes().empty())
        {
            function->parameters = makeStoredNameASTs(function_node->getParameters().getNodes(), context);
            function->children.push_back(function->parameters);
        }

        function->arguments = makeStoredNameASTs(function_node->getArguments().getNodes(), context);
        function->children.push_back(function->arguments);
        return function;
    }

    if (const auto * lambda_node = node->as<LambdaNode>())
    {
        ASTs arguments;
        for (const auto & name : lambda_node->getArguments().getNames())
            arguments.push_back(make_intrusive<ASTIdentifier>(name));

        return makeASTFunction("lambda", makeASTFunction("tuple", std::move(arguments)), makeStoredNameAST(lambda_node->getExpression(), context));
    }

    /// a literal and a subquery have the name of the expression that the query writes
    if (node->hasOriginalAST())
        return node->getOriginalAST();

    if (!isQueryOrUnionNode(node))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expression {} of a projection has no original AST", node->formatASTForErrorMessage());

    /// the analysis makes a subquery for `exists`
    auto hash = node->getTreeHash();
    return make_intrusive<ASTIdentifier>(fmt::format("__subquery_{}_{}", hash.low64, hash.high64));
}

String getStoredName(const QueryTreeNodePtr & node, const ContextPtr & context)
{
    return makeStoredNameAST(node, context)->getColumnName();
}

/// Makes the plan calculate each expression over the columns of the table, as the old analysis did.
/// An ALIAS column becomes its expression, because the source does not have the column.
/// A constant that the analysis folds from an expression over a column becomes that expression again.
/// Returns true if the expression reads a column of the table.
bool restoreExpressionsOverColumns(QueryTreeNodePtr & node, const QueryTreeNodePtr & table_node)
{
    while (const auto * column_node = node->as<ColumnNode>())
    {
        if (!column_node->hasExpression())
            break;

        auto expression = column_node->getExpression();
        node = std::move(expression);
    }

    if (const auto * constant_node = node->as<ConstantNode>();
        constant_node && constant_node->hasSourceExpression() && !isQueryOrUnionNode(constant_node->getSourceExpression()))
    {
        auto source_expression = constant_node->getSourceExpression();
        if (!restoreExpressionsOverColumns(source_expression, table_node))
            return false;

        node = std::move(source_expression);
        return true;
    }

    const auto * column_node = node->as<ColumnNode>();
    bool reads_column = column_node && column_node->getColumnSourceOrNull() == table_node;
    for (auto & child : node->getChildren())
        if (child && !isQueryOrUnionNode(child))
            reads_column |= restoreExpressionsOverColumns(child, table_node);

    return reads_column;
}

/// A constant key has one value, so a projection does not keep it. The last key stays if the query has no
/// aggregate function, because the query must remain an aggregation.
void removeConstantGroupByKeys(QueryNode & query_node, bool has_aggregate_functions)
{
    if (!query_node.hasGroupBy() || query_node.isGroupByWithGroupingSets())
        return;

    auto & keys = query_node.getGroupBy().getNodes();
    for (size_t i = 0; i < keys.size();)
    {
        if (!keys[i]->as<ConstantNode>() || (!has_aggregate_functions && keys.size() == 1))
        {
            ++i;
            continue;
        }

        /// the last key takes the place of the removed one: the existing parts are sorted in this order
        if (i + 1 < keys.size())
            keys[i] = std::move(keys.back());
        keys.pop_back();
    }
}

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

QueryTreeNodePtr buildProjectionQueryTree(const ASTPtr & query_ast, const StoragePtr & source, const ContextPtr & context, bool only_analyze)
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
    addQueryTreePasses(query_tree_pass_manager, only_analyze);
    /// Optimization passes are not applied because some of them are invalid for projections.
    /// Example: 'SELECT min(c0), max(c0), count() GROUP BY -c0' for minmax_count projection can be rewritten to
    /// 'SELECT min(c0), max(c0), count() GROUP BY c0' which is incorrect cause we store a column '-c0' in projection.
    query_tree_pass_manager.runOnlyResolve(query_tree);

    /// `count(1)` and `sum(1)` are stored as `count()`
    NormalizeCountVariantsPass().run(query_tree, context);

    auto table_node = query_node.getJoinTreeNode();
    for (auto & child : query_node.getChildren())
        if (child)
            restoreExpressionsOverColumns(child, table_node);

    removeConstantGroupByKeys(query_node, hasAggregateFunctionNodes(query_tree));

    return query_tree;
}

ContextMutablePtr createProjectionAnalysisContext(const ASTPtr & query_ast, const ContextPtr & context)
{
    auto query_context = Context::createCopy(context);
    /// the settings of the session must not change the meaning of the query of a projection
    query_context->setSetting("enable_positional_arguments", false);
    query_context->setSetting("rewrite_in_to_join", false);
    query_context->setSetting("rewrite_count_distinct_if_with_count_distinct_implementation", false);
    /// `exists` stays an `IN` that is calculated with the projection, as a scalar it is a constant of the query
    query_context->setSetting("execute_exists_as_scalar_subquery", false);
    /// the query keeps the value of a scalar subquery, not a reference to the scalars of a query
    query_context->setSetting("enable_scalar_subquery_optimization", false);
    /// a projection is calculated locally; with parallel replicas the analysis replaces the set of `GLOBAL IN` with a temporary table
    query_context->setSetting("enable_parallel_replicas", Field(0));
    /// aliases are resolved as a reload of the table resolves them, with the settings of the server
    query_context->setSetting(
        "prefer_column_name_to_alias", context->getGlobalContext()->getSettingsRef()[Setting::prefer_column_name_to_alias].value);
    /// the analysis resolves positional arguments only in an initial query
    query_context->setQueryKindInitial();

    /// the settings of the stored query are a part of the projection, so the constraints of the session do not apply to them
    if (const auto & settings = query_ast->as<ASTSelectQuery &>().settings())
        for (const auto & change : settings->as<ASTSetQuery &>().changes)
            query_context->setSetting(change.name, change.value);

    return query_context;
}

ProjectionPlanner::ProjectionPlanner(const ASTPtr & query_ast, const StoragePtr & source, const ContextPtr & context)
{
    query_tree = buildProjectionQueryTree(query_ast, source, createProjectionAnalysisContext(query_ast, context));
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
    /// a function can give a constant column for an expression over a column (`a + NULL`), but the part stores a value for each row
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
