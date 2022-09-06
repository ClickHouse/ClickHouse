#include <Planner/Utils.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>

#include <Columns/getLeastSuperColumn.h>

#include <IO/WriteBufferFromString.h>

#include <Interpreters/Context.h>

#include <Analyzer/QueryNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>

#include <Planner/PlannerActionsVisitor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int LOGICAL_ERROR;
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

Block buildCommonHeaderForUnion(const Blocks & queries_headers)
{
    size_t num_selects = queries_headers.size();
    Block common_header = queries_headers.front();
    size_t num_columns = common_header.columns();

    for (size_t query_num = 1; query_num < num_selects; ++query_num)
    {
        if (queries_headers.at(query_num).columns() != num_columns)
            throw Exception(ErrorCodes::TYPE_MISMATCH,
                            "Different number of columns in UNION elements: {} and {}",
                            common_header.dumpNames(),
                            queries_headers[query_num].dumpNames());
    }

    std::vector<const ColumnWithTypeAndName *> columns(num_selects);

    for (size_t column_num = 0; column_num < num_columns; ++column_num)
    {
        for (size_t i = 0; i < num_selects; ++i)
            columns[i] = &queries_headers[i].getByPosition(column_num);

        ColumnWithTypeAndName & result_elem = common_header.getByPosition(column_num);
        result_elem = getLeastSuperColumn(columns);
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
ContextPtr buildSubqueryContext(const ContextPtr & context)
{
    /** The subquery in the IN / JOIN section does not have any restrictions on the maximum size of the result.
      * Because the result of this query is not the result of the entire query.
      * Constraints work instead
      *  max_rows_in_set, max_bytes_in_set, set_overflow_mode,
      *  max_rows_in_join, max_bytes_in_join, join_overflow_mode,
      *  which are checked separately (in the Set, Join objects).
      */
    auto subquery_context = Context::createCopy(context);
    Settings subquery_settings = context->getSettings();
    subquery_settings.max_result_rows = 0;
    subquery_settings.max_result_bytes = 0;
    /// The calculation of extremes does not make sense and is not necessary (if you do it, then the extremes of the subquery can be taken for whole query).
    subquery_settings.extremes = false;
    subquery_context->setSettings(subquery_settings);

    return subquery_context;
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

ActionsDAGPtr buildActionsDAGFromExpressionNode(const QueryTreeNodePtr & expression_node, const ColumnsWithTypeAndName & input_columns, const PlannerContextPtr & planner_context)
{
    ActionsDAGPtr action_dag = std::make_shared<ActionsDAG>(input_columns);
    PlannerActionsVisitor actions_visitor(planner_context);
    auto expression_dag_index_nodes = actions_visitor.visit(action_dag, expression_node);
    action_dag->getOutputs().clear();

    for (auto & expression_dag_index_node : expression_dag_index_nodes)
        action_dag->getOutputs().push_back(expression_dag_index_node);

    return action_dag;
}

}
