#include <Planner/Utils.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>

#include <Columns/getLeastSuperColumn.h>

#include <IO/WriteBufferFromString.h>

#include <Functions/FunctionFactory.h>

#include <Storages/StorageDummy.h>

#include <Interpreters/Context.h>

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

#include <Planner/PlannerActionsVisitor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNION_ALL_RESULT_STRUCTURES_MISMATCH;
    extern const int INTERSECT_OR_EXCEPT_RESULT_STRUCTURES_MISMATCH;
}

String dumpQueryPlan(QueryPlan & query_plan)
{
    WriteBufferFromOwnString query_plan_buffer;
    query_plan.explainPlan(query_plan_buffer, QueryPlan::ExplainPlanOptions{true, true, true, true});

    return query_plan_buffer.str();
}

String dumpQueryPipeline(QueryPlan & query_plan)
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

ASTPtr queryNodeToSelectQuery(const QueryTreeNodePtr & query_node)
{
    auto & query_node_typed = query_node->as<QueryNode &>();
    auto result_ast = query_node_typed.toAST();

    while (true)
    {
        if (auto * select_query = result_ast->as<ASTSelectQuery>())
            break;
        else if (auto * select_with_union = result_ast->as<ASTSelectWithUnionQuery>())
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

/** There are no limits on the maximum size of the result for the subquery.
  * Since the result of the query is not the result of the entire query.
  */
void updateContextForSubqueryExecution(ContextMutablePtr & mutable_context)
{
    /** The subquery in the IN / JOIN section does not have any restrictions on the maximum size of the result.
      * Because the result of this query is not the result of the entire query.
      * Constraints work instead
      *  max_rows_in_set, max_bytes_in_set, set_overflow_mode,
      *  max_rows_in_join, max_bytes_in_join, join_overflow_mode,
      *  which are checked separately (in the Set, Join objects).
      */
    Settings subquery_settings = mutable_context->getSettings();
    subquery_settings.max_result_rows = 0;
    subquery_settings.max_result_bytes = 0;
    /// The calculation of extremes does not make sense and is not necessary (if you do it, then the extremes of the subquery can be taken for whole query).
    subquery_settings.extremes = false;
    mutable_context->setSettings(subquery_settings);
}

namespace
{

StreamLocalLimits getLimitsForStorage(const Settings & settings, const SelectQueryOptions & options)
{
    StreamLocalLimits limits;
    limits.mode = LimitsMode::LIMITS_TOTAL;
    limits.size_limits = SizeLimits(settings.max_rows_to_read, settings.max_bytes_to_read, settings.read_overflow_mode);
    limits.speed_limits.max_execution_time = settings.max_execution_time;
    limits.timeout_overflow_mode = settings.timeout_overflow_mode;

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
        limits.speed_limits.min_execution_rps = settings.min_execution_speed;
        limits.speed_limits.min_execution_bps = settings.min_execution_speed_bytes;
    }

    limits.speed_limits.max_execution_rps = settings.max_execution_speed;
    limits.speed_limits.max_execution_bps = settings.max_execution_speed_bytes;
    limits.speed_limits.timeout_before_checking_execution_speed = settings.timeout_before_checking_execution_speed;

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
        leaf_limits = SizeLimits(settings.max_rows_to_read_leaf, settings.max_bytes_to_read_leaf, settings.read_overflow_mode_leaf);
    }

    return {limits, leaf_limits};
}

ActionsDAGPtr buildActionsDAGFromExpressionNode(const QueryTreeNodePtr & expression_node,
    const ColumnsWithTypeAndName & input_columns,
    const PlannerContextPtr & planner_context)
{
    ActionsDAGPtr action_dag = std::make_shared<ActionsDAG>(input_columns);
    PlannerActionsVisitor actions_visitor(planner_context);
    auto expression_dag_index_nodes = actions_visitor.visit(action_dag, expression_node);
    action_dag->getOutputs() = std::move(expression_dag_index_nodes);

    return action_dag;
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

QueryTreeNodePtr replaceTablesAndTableFunctionsWithDummyTables(const QueryTreeNodePtr & query_node,
    const ContextPtr & context,
    ResultReplacementMap * result_replacement_map)
{
    auto & query_node_typed = query_node->as<QueryNode &>();
    auto table_expressions = extractTableExpressions(query_node_typed.getJoinTree());
    std::unordered_map<const IQueryTreeNode *, QueryTreeNodePtr> replacement_map;

    for (auto & table_expression : table_expressions)
    {
        auto * table_node = table_expression->as<TableNode>();
        auto * table_function_node = table_expression->as<TableFunctionNode>();
        if (!table_node && !table_function_node)
            continue;

        const auto & storage_snapshot = table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot();
        auto storage_dummy = std::make_shared<StorageDummy>(storage_snapshot->storage.getStorageID(),
            storage_snapshot->metadata->getColumns());
        auto dummy_table_node = std::make_shared<TableNode>(std::move(storage_dummy), context);

        if (result_replacement_map)
            result_replacement_map->emplace(table_expression, dummy_table_node);

        replacement_map.emplace(table_expression.get(), std::move(dummy_table_node));
    }

    return query_node->cloneAndReplace(replacement_map);
}

QueryTreeNodePtr buildSubqueryToReadColumnsFromTableExpression(const NamesAndTypes & columns,
    const QueryTreeNodePtr & table_expression,
    const ContextPtr & context)
{
    auto projection_columns = columns;

    QueryTreeNodes subquery_projection_nodes;
    subquery_projection_nodes.reserve(projection_columns.size());

    for (const auto & column : projection_columns)
        subquery_projection_nodes.push_back(std::make_shared<ColumnNode>(column, table_expression));

    if (subquery_projection_nodes.empty())
    {
        auto constant_data_type = std::make_shared<DataTypeUInt64>();
        subquery_projection_nodes.push_back(std::make_shared<ConstantNode>(1UL, constant_data_type));
        projection_columns.push_back({"1", std::move(constant_data_type)});
    }

    auto context_copy = Context::createCopy(context);
    updateContextForSubqueryExecution(context_copy);

    auto query_node = std::make_shared<QueryNode>(std::move(context_copy));

    query_node->resolveProjectionColumns(projection_columns);
    query_node->getProjection().getNodes() = std::move(subquery_projection_nodes);
    query_node->getJoinTree() = table_expression;
    query_node->setIsSubquery(true);

    return query_node;
}

SelectQueryInfo buildSelectQueryInfo(const QueryTreeNodePtr & query_tree, const PlannerContextPtr & planner_context)
{
    SelectQueryInfo select_query_info;
    select_query_info.original_query = queryNodeToSelectQuery(query_tree);
    select_query_info.query = select_query_info.original_query;
    select_query_info.query_tree = query_tree;
    select_query_info.planner_context = planner_context;
    return select_query_info;
}

}
