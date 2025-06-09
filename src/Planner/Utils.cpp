#include <Planner/Utils.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>

#include <Columns/getLeastSuperColumn.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>

#include <IO/WriteBufferFromString.h>

#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/indexHint.h>

#include <Storages/StorageDummy.h>

#include <Interpreters/Context.h>

#include <AggregateFunctions/WindowFunction.h>

#include <Analyzer/Utils.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/WindowNode.h>

#include <Core/Settings.h>

#include <Planner/CollectSets.h>
#include <Planner/CollectTableExpressionData.h>
#include <Planner/PlannerActionsVisitor.h>

#include <Processors/QueryPlan/ExpressionStep.h>

#include <stack>

namespace DB
{
namespace Setting
{
    extern const SettingsString additional_result_filter;
    extern const SettingsUInt64 max_bytes_to_read;
    extern const SettingsUInt64 max_bytes_to_read_leaf;
    extern const SettingsSeconds max_estimated_execution_time;
    extern const SettingsUInt64 max_execution_speed;
    extern const SettingsUInt64 max_execution_speed_bytes;
    extern const SettingsSeconds max_execution_time;
    extern const SettingsUInt64 max_query_size;
    extern const SettingsUInt64 max_rows_to_read;
    extern const SettingsUInt64 max_rows_to_read_leaf;
    extern const SettingsUInt64 min_execution_speed;
    extern const SettingsUInt64 min_execution_speed_bytes;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsOverflowMode read_overflow_mode;
    extern const SettingsOverflowMode read_overflow_mode_leaf;
    extern const SettingsSeconds timeout_before_checking_execution_speed;
    extern const SettingsOverflowMode timeout_overflow_mode;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int UNION_ALL_RESULT_STRUCTURES_MISMATCH;
    extern const int INTERSECT_OR_EXCEPT_RESULT_STRUCTURES_MISMATCH;
}

String dumpQueryPlan(const QueryPlan & query_plan)
{
    WriteBufferFromOwnString query_plan_buffer;
    query_plan.explainPlan(query_plan_buffer, ExplainPlanOptions{true, true, true, true});

    return query_plan_buffer.str();
}

String dumpQueryPipeline(const QueryPlan & query_plan)
{
    QueryPlan::ExplainPipelineOptions explain_pipeline;
    WriteBufferFromOwnString query_pipeline_buffer;
    query_plan.explainPipeline(query_pipeline_buffer, explain_pipeline);

    return query_pipeline_buffer.str();
}

Block buildCommonHeaderForUnion(const Blocks & queries_headers, SelectUnionMode union_mode)
{
    size_t num_selects = queries_headers.size();
    Block common_header = queries_headers.front();
    size_t columns_size = common_header.columns();

    for (size_t query_number = 1; query_number < num_selects; ++query_number)
    {
        int error_code = 0;

        if (union_mode == SelectUnionMode::UNION_DEFAULT ||
            union_mode == SelectUnionMode::UNION_ALL ||
            union_mode == SelectUnionMode::UNION_DISTINCT)
            error_code = ErrorCodes::UNION_ALL_RESULT_STRUCTURES_MISMATCH;
        else
            error_code = ErrorCodes::INTERSECT_OR_EXCEPT_RESULT_STRUCTURES_MISMATCH;

        if (queries_headers.at(query_number).columns() != columns_size)
            throw Exception(error_code,
                            "Different number of columns in {} elements: {} and {}",
                            toString(union_mode),
                            common_header.dumpNames(),
                            queries_headers[query_number].dumpNames());
    }

    std::vector<const ColumnWithTypeAndName *> columns(num_selects);

    for (size_t column_number = 0; column_number < columns_size; ++column_number)
    {
        for (size_t i = 0; i < num_selects; ++i)
            columns[i] = &queries_headers[i].getByPosition(column_number);

        ColumnWithTypeAndName & result_element = common_header.getByPosition(column_number);
        result_element = getLeastSuperColumn(columns);
    }

    return common_header;
}

void addConvertingToCommonHeaderActionsIfNeeded(
    std::vector<std::unique_ptr<QueryPlan>> & query_plans,
    const Block & union_common_header,
    Blocks & query_plans_headers)
{
    size_t queries_size = query_plans.size();
    for (size_t i = 0; i < queries_size; ++i)
    {
        auto & query_node_plan = query_plans[i];
        if (blocksHaveEqualStructure(query_node_plan->getCurrentHeader(), union_common_header))
            continue;

        auto actions_dag = ActionsDAG::makeConvertingActions(
            query_node_plan->getCurrentHeader().getColumnsWithTypeAndName(),
            union_common_header.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Position);
        auto converting_step = std::make_unique<ExpressionStep>(query_node_plan->getCurrentHeader(), std::move(actions_dag));
        converting_step->setStepDescription("Conversion before UNION");
        query_node_plan->addStep(std::move(converting_step));

        query_plans_headers[i] = query_node_plan->getCurrentHeader();
    }
}

ASTPtr queryNodeToSelectQuery(const QueryTreeNodePtr & query_node, bool set_subquery_cte_name)
{
    auto & query_node_typed = query_node->as<QueryNode &>();

    // In case of cross-replication we don't know what database is used for the table.
    // Each shard will use the default database (in the case of cross-replication shards may have different defaults).
    auto result_ast = query_node_typed.toAST({
        .qualify_indentifiers_with_database = false,
        .set_subquery_cte_name = set_subquery_cte_name
    });

    while (true)
    {
        if (auto * /*select_query*/ _ = result_ast->as<ASTSelectQuery>())
            break;
        if (auto * select_with_union = result_ast->as<ASTSelectWithUnionQuery>())
            result_ast = select_with_union->list_of_selects->children.at(0);
        else if (auto * subquery = result_ast->as<ASTSubquery>())
            result_ast = subquery->children.at(0);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Query node invalid conversion to select query");
    }

    if (result_ast == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query node invalid conversion to select query");

    return result_ast;
}

ASTPtr queryNodeToDistributedSelectQuery(const QueryTreeNodePtr & query_node)
{
    /// Remove CTEs information from distributed queries.
    /// Now, if cte_name is set for subquery node, AST -> String serialization will only print cte name.
    /// But CTE is defined only for top-level query part, so may not be sent.
    /// Removing cte_name forces subquery to be always printed.
    auto ast = queryNodeToSelectQuery(query_node, /*set_subquery_cte_name=*/false);
    return ast;
}

namespace
{

StreamLocalLimits getLimitsForStorage(const Settings & settings, const SelectQueryOptions & options)
{
    StreamLocalLimits limits;
    limits.mode = LimitsMode::LIMITS_TOTAL;
    limits.size_limits = SizeLimits(settings[Setting::max_rows_to_read], settings[Setting::max_bytes_to_read], settings[Setting::read_overflow_mode]);
    limits.speed_limits.max_execution_time = settings[Setting::max_execution_time];
    limits.timeout_overflow_mode = settings[Setting::timeout_overflow_mode];

    /** Quota and minimal speed restrictions are checked on the initiating server of the request, and not on remote servers,
      *  because the initiating server has a summary of the execution of the request on all servers.
      *
      * But limits on data size to read and maximum execution time are reasonable to check both on initiator and
      *  additionally on each remote server, because these limits are checked per block of data processed,
      *  and remote servers may process way more blocks of data than are received by initiator.
      *
      * The limits to throttle maximum execution speed is also checked on all servers.
      */
    if (options.to_stage == QueryProcessingStage::Complete)
    {
        limits.speed_limits.min_execution_rps = settings[Setting::min_execution_speed];
        limits.speed_limits.min_execution_bps = settings[Setting::min_execution_speed_bytes];
    }

    limits.speed_limits.max_execution_rps = settings[Setting::max_execution_speed];
    limits.speed_limits.max_execution_bps = settings[Setting::max_execution_speed_bytes];
    limits.speed_limits.timeout_before_checking_execution_speed = settings[Setting::timeout_before_checking_execution_speed];
    limits.speed_limits.max_estimated_execution_time = settings[Setting::max_estimated_execution_time];

    return limits;
}

}

StorageLimits buildStorageLimits(const Context & context, const SelectQueryOptions & options)
{
    const auto & settings = context.getSettingsRef();

    StreamLocalLimits limits;
    SizeLimits leaf_limits;

    /// Set the limits and quota for reading data, the speed and time of the query.
    if (!options.ignore_limits)
    {
        limits = getLimitsForStorage(settings, options);
        leaf_limits = SizeLimits(settings[Setting::max_rows_to_read_leaf], settings[Setting::max_bytes_to_read_leaf], settings[Setting::read_overflow_mode_leaf]);
    }

    return {limits, leaf_limits};
}

std::pair<ActionsDAG, CorrelatedSubtrees> buildActionsDAGFromExpressionNode(
    const QueryTreeNodePtr & expression_node,
    const ColumnsWithTypeAndName & input_columns,
    const PlannerContextPtr & planner_context,
    const ColumnNodePtrWithHashSet & correlated_columns_set,
    bool use_column_identifier_as_action_node_name)
{
    ActionsDAG action_dag(input_columns);
    PlannerActionsVisitor actions_visitor(planner_context, correlated_columns_set, use_column_identifier_as_action_node_name);
    auto [expression_dag_index_nodes, correlated_subtrees] = actions_visitor.visit(action_dag, expression_node);
    action_dag.getOutputs() = std::move(expression_dag_index_nodes);

    return std::make_pair(std::move(action_dag), std::move(correlated_subtrees));
}

bool sortDescriptionIsPrefix(const SortDescription & prefix, const SortDescription & full)
{
    size_t prefix_size = prefix.size();
    if (prefix_size > full.size())
        return false;

    for (size_t i = 0; i < prefix_size; ++i)
    {
        if (full[i] != prefix[i])
            return false;
    }

    return true;
}

bool queryHasArrayJoinInJoinTree(const QueryTreeNodePtr & query_node)
{
    const auto & query_node_typed = query_node->as<const QueryNode &>();

    std::vector<QueryTreeNodePtr> join_tree_nodes_to_process;
    join_tree_nodes_to_process.push_back(query_node_typed.getJoinTree());

    while (!join_tree_nodes_to_process.empty())
    {
        auto join_tree_node_to_process = join_tree_nodes_to_process.back();
        join_tree_nodes_to_process.pop_back();

        auto join_tree_node_type = join_tree_node_to_process->getNodeType();

        switch (join_tree_node_type)
        {
            case QueryTreeNodeType::TABLE:
                [[fallthrough]];
            case QueryTreeNodeType::QUERY:
                [[fallthrough]];
            case QueryTreeNodeType::UNION:
                [[fallthrough]];
            case QueryTreeNodeType::TABLE_FUNCTION:
            {
                break;
            }
            case QueryTreeNodeType::ARRAY_JOIN:
            {
                return true;
            }
            case QueryTreeNodeType::CROSS_JOIN:
            {
                auto & cross_join_node = join_tree_node_to_process->as<CrossJoinNode &>();
                for (const auto & expr : cross_join_node.getTableExpressions())
                    join_tree_nodes_to_process.push_back(expr);

                break;
            }
            case QueryTreeNodeType::JOIN:
            {
                auto & join_node = join_tree_node_to_process->as<JoinNode &>();
                join_tree_nodes_to_process.push_back(join_node.getLeftTableExpression());
                join_tree_nodes_to_process.push_back(join_node.getRightTableExpression());
                break;
            }
            default:
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Unexpected node type for table expression. "
                                "Expected table, table function, query, union, join or array join. Actual {}",
                                join_tree_node_to_process->getNodeTypeName());
            }
        }
    }

    return false;
}

bool queryHasWithTotalsInAnySubqueryInJoinTree(const QueryTreeNodePtr & query_node)
{
    const auto & query_node_typed = query_node->as<const QueryNode &>();

    std::vector<QueryTreeNodePtr> join_tree_nodes_to_process;
    join_tree_nodes_to_process.push_back(query_node_typed.getJoinTree());

    while (!join_tree_nodes_to_process.empty())
    {
        auto join_tree_node_to_process = join_tree_nodes_to_process.back();
        join_tree_nodes_to_process.pop_back();

        auto join_tree_node_type = join_tree_node_to_process->getNodeType();

        switch (join_tree_node_type)
        {
            case QueryTreeNodeType::TABLE:
                [[fallthrough]];
            case QueryTreeNodeType::TABLE_FUNCTION:
            {
                break;
            }
            case QueryTreeNodeType::QUERY:
            {
                auto & query_node_to_process = join_tree_node_to_process->as<QueryNode &>();
                if (query_node_to_process.isGroupByWithTotals())
                    return true;

                join_tree_nodes_to_process.push_back(query_node_to_process.getJoinTree());
                break;
            }
            case QueryTreeNodeType::UNION:
            {
                auto & union_node = join_tree_node_to_process->as<UnionNode &>();
                auto & union_queries = union_node.getQueries().getNodes();

                for (auto & union_query : union_queries)
                    join_tree_nodes_to_process.push_back(union_query);
                break;
            }
            case QueryTreeNodeType::ARRAY_JOIN:
            {
                auto & array_join_node = join_tree_node_to_process->as<ArrayJoinNode &>();
                join_tree_nodes_to_process.push_back(array_join_node.getTableExpression());
                break;
            }
            case QueryTreeNodeType::CROSS_JOIN:
            {
                auto & cross_join_node = join_tree_node_to_process->as<CrossJoinNode &>();
                for (const auto & expr : cross_join_node.getTableExpressions())
                    join_tree_nodes_to_process.push_back(expr);

                break;
            }
            case QueryTreeNodeType::JOIN:
            {
                auto & join_node = join_tree_node_to_process->as<JoinNode &>();
                join_tree_nodes_to_process.push_back(join_node.getLeftTableExpression());
                join_tree_nodes_to_process.push_back(join_node.getRightTableExpression());
                break;
            }
            default:
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Unexpected node type for table expression. "
                                "Expected table, table function, query, union, join or array join. Actual {}",
                                join_tree_node_to_process->getNodeTypeName());
            }
        }
    }

    return false;
}

QueryTreeNodePtr mergeConditionNodes(const QueryTreeNodes & condition_nodes, const ContextPtr & context)
{
    auto function_node = std::make_shared<FunctionNode>("and");
    auto and_function = FunctionFactory::instance().get("and", context);
    function_node->getArguments().getNodes() = condition_nodes;
    function_node->resolveAsFunction(and_function->build(function_node->getArgumentColumns()));

    return function_node;
}

QueryTreeNodePtr replaceTableExpressionsWithDummyTables(
    const QueryTreeNodePtr & query_node,
    const QueryTreeNodes & table_nodes,
    const ContextPtr & context,
    ResultReplacementMap * result_replacement_map)
{
    std::unordered_map<const IQueryTreeNode *, QueryTreeNodePtr> replacement_map;

    for (const auto & table_expression : table_nodes)
    {
        auto * table_node = table_expression->as<TableNode>();
        auto * table_function_node = table_expression->as<TableFunctionNode>();

        if (table_node || table_function_node)
        {
            const auto & storage_snapshot = table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot();
            auto get_column_options = GetColumnsOptions(GetColumnsOptions::All).withExtendedObjects().withVirtuals();
            const auto & storage = storage_snapshot->storage;

            auto storage_dummy = std::make_shared<StorageDummy>(
                storage.getStorageID(),
                ColumnsDescription(storage_snapshot->getColumns(get_column_options)),
                storage_snapshot,
                storage.supportsReplication());

            auto dummy_table_node = std::make_shared<TableNode>(std::move(storage_dummy), context);
            if (table_node && table_node->hasTableExpressionModifiers())
                dummy_table_node->getTableExpressionModifiers() = table_node->getTableExpressionModifiers();

            if (result_replacement_map)
                result_replacement_map->emplace(table_expression, dummy_table_node);

            dummy_table_node->setAlias(table_expression->getAlias());
            replacement_map.emplace(table_expression.get(), std::move(dummy_table_node));
        }
    }

    return query_node->cloneAndReplace(replacement_map);
}

SelectQueryInfo buildSelectQueryInfo(const QueryTreeNodePtr & query_tree, const PlannerContextPtr & planner_context)
{
    SelectQueryInfo select_query_info;
    select_query_info.query = queryNodeToSelectQuery(query_tree);
    select_query_info.query_tree = query_tree;
    select_query_info.planner_context = planner_context;
    return select_query_info;
}

FilterDAGInfo buildFilterInfo(ASTPtr filter_expression,
        const QueryTreeNodePtr & table_expression,
        PlannerContextPtr & planner_context,
        NameSet table_expression_required_names_without_filter)
{
    const auto & query_context = planner_context->getQueryContext();
    auto filter_query_tree = buildQueryTree(filter_expression, query_context);

    QueryAnalysisPass query_analysis_pass(table_expression);
    query_analysis_pass.run(filter_query_tree, query_context);

    return buildFilterInfo(std::move(filter_query_tree), table_expression, planner_context, std::move(table_expression_required_names_without_filter));
}

FilterDAGInfo buildFilterInfo(QueryTreeNodePtr filter_query_tree,
        const QueryTreeNodePtr & table_expression,
        PlannerContextPtr & planner_context,
        NameSet table_expression_required_names_without_filter)
{
    if (table_expression_required_names_without_filter.empty())
    {
        auto & table_expression_data = planner_context->getTableExpressionDataOrThrow(table_expression);
        const auto & table_expression_names = table_expression_data.getColumnNames();
        table_expression_required_names_without_filter.insert(table_expression_names.begin(), table_expression_names.end());
    }

    collectSourceColumns(filter_query_tree, planner_context, false /*keep_alias_columns*/);
    collectSets(filter_query_tree, *planner_context);

    ActionsDAG filter_actions_dag;

    ColumnNodePtrWithHashSet empty_correlated_columns_set;
    PlannerActionsVisitor actions_visitor(planner_context, empty_correlated_columns_set, false /*use_column_identifier_as_action_node_name*/);
    auto [expression_nodes, correlated_subtrees] = actions_visitor.visit(filter_actions_dag, filter_query_tree);
    correlated_subtrees.assertEmpty("in row-policy and additional table filters");
    if (expression_nodes.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Filter actions must return single output node. Actual {}",
            expression_nodes.size());

    auto & filter_actions_outputs = filter_actions_dag.getOutputs();
    filter_actions_outputs = std::move(expression_nodes);

    std::string filter_node_name = filter_actions_outputs[0]->result_name;
    bool remove_filter_column = true;

    for (const auto & filter_input_node : filter_actions_dag.getInputs())
        if (table_expression_required_names_without_filter.contains(filter_input_node->result_name))
            filter_actions_outputs.push_back(filter_input_node);

    return {std::move(filter_actions_dag), std::move(filter_node_name), remove_filter_column};
}

ASTPtr parseAdditionalResultFilter(const Settings & settings)
{
    const String & additional_result_filter = settings[Setting::additional_result_filter];
    if (additional_result_filter.empty())
        return {};

    ParserExpression parser;
    auto additional_result_filter_ast = parseQuery(
        parser,
        additional_result_filter.data(),
        additional_result_filter.data() + additional_result_filter.size(),
        "additional result filter",
        settings[Setting::max_query_size],
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks]);
    return additional_result_filter_ast;
}

void appendSetsFromActionsDAG(const ActionsDAG & dag, UsefulSets & useful_sets)
{
    for (const auto & node : dag.getNodes())
    {
        if (node.column)
        {
            const IColumn * column = node.column.get();
            if (const auto * column_const = typeid_cast<const ColumnConst *>(column))
                column = &column_const->getDataColumn();

            if (const auto * column_set = typeid_cast<const ColumnSet *>(column))
                useful_sets.insert(column_set->getData());
        }

        if (node.type == ActionsDAG::ActionType::FUNCTION && node.function_base->getName() == "indexHint")
        {
            ActionsDAG::NodeRawConstPtrs children;
            if (const auto * adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor *>(node.function_base.get()))
            {
                if (const auto * index_hint = typeid_cast<const FunctionIndexHint *>(adaptor->getFunction().get()))
                {
                    appendSetsFromActionsDAG(index_hint->getActions(), useful_sets);
                }
            }
        }
    }
}

std::optional<WindowFrame> extractWindowFrame(const FunctionNode & node)
{
    if (!node.isWindowFunction())
        return {};
    auto & window_node = node.getWindowNode()->as<WindowNode &>();
    const auto & window_frame = window_node.getWindowFrame();
    if (!window_frame.is_default)
        return window_frame;
    auto aggregate_function = node.getAggregateFunction();
    if (const auto * win_func = dynamic_cast<const IWindowFunction *>(aggregate_function.get()))
    {
        return win_func->getDefaultFrame();
    }
    return {};
}

}
