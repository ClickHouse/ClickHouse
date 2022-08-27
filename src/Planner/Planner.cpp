#include <Planner/Planner.h>

#include <DataTypes/DataTypeString.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/CastOverloadResolver.h>

#include <QueryPipeline/Pipe.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/IntersectOrExceptStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Interpreters/Context.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/ArrayJoinAction.h>

#include <Storages/SelectQueryInfo.h>
#include <Storages/IStorage.h>

#include <Analyzer/Utils.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/SortColumnNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>

#include <Planner/Utils.h>
#include <Planner/PlannerContext.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Planner/PlannerJoins.h>
#include <Planner/PlannerSorting.h>
#include <Planner/ActionsChain.h>
#include <Planner/PlannerCollectSets.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int INVALID_JOIN_ON_EXPRESSION;
    extern const int ILLEGAL_AGGREGATION;
    extern const int NOT_AN_AGGREGATE;
    extern const int INVALID_WITH_FILL_EXPRESSION;
}

/** ClickHouse query planner.
  *
  * TODO: JOIN support ASOF. JOIN support strictness. JOIN support constants. JOIN support ON t1.id = t1.id
  * TODO: JOIN drop unnecessary columns after ON, USING section
  * TODO: Support display names
  * TODO: Support RBAC. Support RBAC for ALIAS columns.
  * TODO: Support distributed query processing
  * TODO: Support PREWHERE
  * TODO: Support GROUP BY, HAVING
  * TODO: Support ORDER BY, LIMIT
  * TODO: Support WINDOW FUNCTIONS
  * TODO: Support DISTINCT
  * TODO: Support trivial count optimization
  * TODO: Support totals, extremes
  * TODO: Support projections
  * TODO: Support read in order optimization
  * TODO: UNION storage limits
  * TODO: Interpreter resources
  * TODO: Support max streams
  * TODO: Support GROUPINS SETS, const aggregation keys, overflow row
  * TODO: Support interpolate, LIMIT BY.
  * TODO: Support ORDER BY read in order optimization
  * TODO: Support GROUP BY read in order optimization
  */

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

StorageLimits getStorageLimits(const Context & context, const SelectQueryOptions & options)
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

class CollectAggregateFunctionNodesMatcher
{
public:
    using Visitor = ConstInDepthQueryTreeVisitor<CollectAggregateFunctionNodesMatcher, true, false>;

    struct Data
    {
        Data(String assert_no_aggregates_place_message_, const PlannerContext & planner_context_)
            : assert_no_aggregates_place_message(std::move(assert_no_aggregates_place_message_))
            , planner_context(planner_context_)
        {}

        explicit Data(const PlannerContext & planner_context_)
            : planner_context(planner_context_)
        {}

        String assert_no_aggregates_place_message;
        const PlannerContext & planner_context;
        QueryTreeNodes aggregate_function_nodes;
    };

    static void visit(const QueryTreeNodePtr & node, Data & data)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node)
            return;

        if (!function_node->isAggregateFunction())
            return;

        if (!data.assert_no_aggregates_place_message.empty())
            throw Exception(ErrorCodes::ILLEGAL_AGGREGATION,
                "Aggregate function {} is found {} in query",
                function_node->getName(),
                data.assert_no_aggregates_place_message);

        data.aggregate_function_nodes.push_back(node);
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node)
    {
        return child_node->getNodeType() != QueryTreeNodeType::QUERY;
    }
};

using CollectAggregateFunctionNodesVisitor = CollectAggregateFunctionNodesMatcher::Visitor;

void assertNoAggregatesFunctions(const QueryTreeNodePtr & node, const String & assert_no_aggregates_place_message, const PlannerContext & planner_context)
{
    CollectAggregateFunctionNodesVisitor::Data data(assert_no_aggregates_place_message, planner_context);
    CollectAggregateFunctionNodesVisitor(data).visit(node);
}

AggregateDescriptions extractAggregateDescriptions(const QueryTreeNodePtr & query_tree, const PlannerContext & planner_context)
{
    CollectAggregateFunctionNodesVisitor::Data data(planner_context);
    CollectAggregateFunctionNodesVisitor(data).visit(query_tree);

    QueryTreeNodeToName node_to_name;
    NameSet unique_aggregate_action_node_names;
    AggregateDescriptions aggregate_descriptions;

    for (auto & aggregate_function_node : data.aggregate_function_nodes)
    {
        for (auto & aggregate_function_node_child : aggregate_function_node->getChildren())
            assertNoAggregatesFunctions(aggregate_function_node_child, "inside another aggregate function", planner_context);

        auto & aggregagte_function_node_typed = aggregate_function_node->as<FunctionNode &>();
        String node_name = calculateActionsDAGNodeName(aggregate_function_node.get(), data.planner_context, node_to_name);
        auto [_, inserted] = unique_aggregate_action_node_names.emplace(node_name);
        if (!inserted)
            continue;

        AggregateDescription aggregate_description;
        aggregate_description.function = aggregagte_function_node_typed.getAggregateFunction();

        const auto & parameters_nodes = aggregagte_function_node_typed.getParameters().getNodes();
        aggregate_description.parameters.reserve(parameters_nodes.size());

        for (const auto & parameter_node : parameters_nodes)
        {
            const auto & constant_node = parameter_node->as<const ConstantNode &>();
            aggregate_description.parameters.push_back(constant_node.getConstantValue());
        }

        const auto & arguments_nodes = aggregagte_function_node_typed.getArguments().getNodes();
        aggregate_description.argument_names.reserve(arguments_nodes.size());

        for (const auto & argument_node : arguments_nodes)
        {
            String argument_node_name = calculateActionsDAGNodeName(argument_node.get(), data.planner_context, node_to_name);
            aggregate_description.argument_names.emplace_back(std::move(argument_node_name));
        }

        aggregate_description.column_name = node_name;
        aggregate_descriptions.push_back(std::move(aggregate_description));
    }

    return aggregate_descriptions;
}

class ValidateGroupByColumnsMatcher
{
public:
    using Visitor = ConstInDepthQueryTreeVisitor<ValidateGroupByColumnsMatcher, true, false>;

    struct Data
    {
        Data(const NameSet & group_by_keys_column_names_, const PlannerContext & planner_context_)
            : group_by_keys_column_names(group_by_keys_column_names_)
            , planner_context(planner_context_)
        {}

        const NameSet & group_by_keys_column_names;
        const PlannerContext & planner_context;
        QueryTreeNodeToName node_to_name;
    };

    static void visit(const QueryTreeNodePtr & node, Data & data)
    {
        auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            return;

        String column_node_name = calculateActionsDAGNodeName(node.get(), data.planner_context);
        if (!data.group_by_keys_column_names.contains(column_node_name))
            throw Exception(ErrorCodes::NOT_AN_AGGREGATE,
                "Column {} is not under aggregate function and not in GROUP BY keys",
                column_node->formatASTForErrorMessage());
    }

    static bool needChildVisit(const QueryTreeNodePtr & parent_node, const QueryTreeNodePtr & child_node)
    {
        auto * function_node = parent_node->as<FunctionNode>();
        if (function_node && function_node->isAggregateFunction())
            return false;

        return child_node->getNodeType() != QueryTreeNodeType::QUERY;
    }
};

using ValidateGroupByColumnsVisitor = typename ValidateGroupByColumnsMatcher::Visitor;

class CollectTableExpressionIdentifiersVisitor
{
public:
    void visit(const QueryTreeNodePtr & join_tree_node, PlannerContext & planner_context)
    {
        auto & table_expression_node_to_identifier = planner_context.getTableExpressionNodeToIdentifier();
        auto join_tree_node_type = join_tree_node->getNodeType();

        switch (join_tree_node_type)
        {
            case QueryTreeNodeType::QUERY:
                [[fallthrough]];
            case QueryTreeNodeType::UNION:
                [[fallthrough]];
            case QueryTreeNodeType::TABLE:
                [[fallthrough]];
            case QueryTreeNodeType::TABLE_FUNCTION:
            {
                std::string table_expression_identifier = std::to_string(table_expression_node_to_identifier.size());
                table_expression_node_to_identifier.emplace(join_tree_node.get(), table_expression_identifier);
                break;
            }
            case QueryTreeNodeType::JOIN:
            {
                auto & join_node = join_tree_node->as<JoinNode &>();
                visit(join_node.getLeftTableExpression(), planner_context);

                std::string table_expression_identifier = std::to_string(table_expression_node_to_identifier.size());
                table_expression_node_to_identifier.emplace(join_tree_node.get(), table_expression_identifier);

                visit(join_node.getRightTableExpression(), planner_context);
                break;
            }
            case QueryTreeNodeType::ARRAY_JOIN:
            {
                auto & array_join_node = join_tree_node->as<ArrayJoinNode &>();
                visit(array_join_node.getTableExpression(), planner_context);

                std::string table_expression_identifier = std::to_string(table_expression_node_to_identifier.size());
                table_expression_node_to_identifier.emplace(join_tree_node.get(), table_expression_identifier);
                break;
            }
            default:
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Expected query, union, table, table function, join or array join query node. Actual {} {}",
                    join_tree_node->getNodeTypeName(),
                    join_tree_node->formatASTForErrorMessage());
            }
        }
    }
};

class CollectSourceColumnsMatcher
{
public:
    using Visitor = InDepthQueryTreeVisitor<CollectSourceColumnsMatcher, true, false>;

    struct Data
    {
        PlannerContext & planner_context;
    };

    static void visit(QueryTreeNodePtr & node, Data & data)
    {
        auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            return;

        auto column_source_node = column_node->getColumnSource();
        auto column_source_node_type = column_source_node->getNodeType();

        if (column_source_node_type == QueryTreeNodeType::ARRAY_JOIN ||
            column_source_node_type == QueryTreeNodeType::LAMBDA)
            return;

        /// JOIN using expression
        if (column_node->hasExpression() && column_source_node->getNodeType() == QueryTreeNodeType::JOIN)
            return;

        auto & table_expression_node_to_columns = data.planner_context.getTableExpressionNodeToColumns();

        auto [it, _] = table_expression_node_to_columns.emplace(column_source_node.get(), TableExpressionColumns());
        auto & table_expression_columns = it->second;

        if (column_node->hasExpression())
        {
            /// Replace ALIAS column with expression
            table_expression_columns.addAliasColumnName(column_node->getColumnName());
            node = column_node->getExpression();
            visit(node, data);
            return;
        }

        if (column_source_node_type != QueryTreeNodeType::TABLE &&
            column_source_node_type != QueryTreeNodeType::TABLE_FUNCTION &&
            column_source_node_type != QueryTreeNodeType::QUERY &&
            column_source_node_type != QueryTreeNodeType::UNION)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Expected table, table function, query or union column source. Actual {}",
                column_source_node->formatASTForErrorMessage());

        bool column_already_exists = table_expression_columns.hasColumn(column_node->getColumnName());

        if (!column_already_exists)
        {
            auto column_identifier = data.planner_context.getColumnUniqueIdentifier(column_source_node.get(), column_node->getColumnName());
            data.planner_context.registerColumnNode(column_node, column_identifier);
            table_expression_columns.addColumn(column_node->getColumn(), column_identifier);
        }
        else
        {
            auto column_identifier = table_expression_columns.getColumnIdentifierOrThrow(column_node->getColumnName());
            data.planner_context.registerColumnNode(column_node, column_identifier);
        }
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node)
    {
        return child_node->getNodeType() != QueryTreeNodeType::QUERY;
    }
};

using CollectSourceColumnsVisitor = CollectSourceColumnsMatcher::Visitor;

ActionsDAGPtr convertExpressionNodeIntoDAG(const QueryTreeNodePtr & expression_node, const ColumnsWithTypeAndName & inputs, const PlannerContextPtr & planner_context)
{
    ActionsDAGPtr action_dag = std::make_shared<ActionsDAG>(inputs);
    PlannerActionsVisitor actions_visitor(planner_context);
    auto expression_dag_index_nodes = actions_visitor.visit(action_dag, expression_node);
    action_dag->getOutputs().clear();

    for (auto & expression_dag_index_node : expression_dag_index_nodes)
        action_dag->getOutputs().push_back(expression_dag_index_node);

    return action_dag;
}

QueryPlan buildQueryPlanForJoinTreeNode(QueryTreeNodePtr join_tree_node,
    SelectQueryInfo & select_query_info,
    const SelectQueryOptions & select_query_options,
    PlannerContextPtr & planner_context);

QueryPlan buildQueryPlanForTableExpression(QueryTreeNodePtr table_expression,
    SelectQueryInfo & table_expression_query_info,
    const SelectQueryOptions & select_query_options,
    PlannerContextPtr & planner_context)
{
    auto * table_node = table_expression->as<TableNode>();
    auto * table_function_node = table_expression->as<TableFunctionNode>();
    auto * query_node = table_expression->as<QueryNode>();
    auto * union_node = table_expression->as<UnionNode>();

    QueryPlan query_plan;

    auto & table_expression_node_to_columns = planner_context->getTableExpressionNodeToColumns();

    /** Use default columns to support case when there are no columns in query.
      * Example: SELECT 1;
      */
    const auto & [it, _] = table_expression_node_to_columns.emplace(table_expression.get(), TableExpressionColumns());
    auto & table_expression_columns = it->second;

    if (table_node || table_function_node)
    {
        const auto & storage = table_node ? table_node->getStorage() : table_function_node->getStorage();
        const auto & storage_snapshot = table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot();

        auto from_stage = storage->getQueryProcessingStage(planner_context->getQueryContext(), select_query_options.to_stage, storage_snapshot, table_expression_query_info);
        const auto & columns_names = table_expression_columns.getColumnsNames();
        Names column_names(columns_names.begin(), columns_names.end());

        std::optional<NameAndTypePair> read_additional_column;

        bool plan_has_multiple_table_expressions = table_expression_node_to_columns.size() > 1;
        if (column_names.empty() && (plan_has_multiple_table_expressions || storage->getName() == "SystemOne"))
        {
            auto column_names_and_types = storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns());
            read_additional_column = column_names_and_types.front();
        }

        if (read_additional_column)
        {
            auto column_identifier = planner_context->getColumnUniqueIdentifier(table_expression.get(), read_additional_column->name);
            column_names.push_back(read_additional_column->name);
            table_expression_columns.addColumn(*read_additional_column, column_identifier);
        }

        if (!column_names.empty())
        {
            const auto & query_context = planner_context->getQueryContext();
            size_t max_block_size = query_context->getSettingsRef().max_block_size;
            size_t max_streams = query_context->getSettingsRef().max_threads;
            storage->read(query_plan, column_names, storage_snapshot, table_expression_query_info, query_context, from_stage, max_block_size, max_streams);
        }

        /// Create step which reads from empty source if storage has no data.
        if (!query_plan.isInitialized())
        {
            auto source_header = storage_snapshot->getSampleBlockForColumns(column_names);
            Pipe pipe(std::make_shared<NullSource>(source_header));
            auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
            read_from_pipe->setStepDescription("Read from NullSource");
            query_plan.addStep(std::move(read_from_pipe));
        }
    }
    else if (query_node || union_node)
    {
        auto subquery_options = select_query_options.subquery();
        Planner subquery_planner(table_expression, subquery_options, planner_context->getQueryContext(), planner_context->getGlobalPlannerContext());
        subquery_planner.buildQueryPlanIfNeeded();
        query_plan = std::move(subquery_planner).extractQueryPlan();
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected table, table function, query or union. Actual {}", table_expression->formatASTForErrorMessage());
    }

    auto rename_actions_dag = std::make_shared<ActionsDAG>(query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName());

    for (const auto & [column_name, column_identifier] : table_expression_columns.getColumnNameToIdentifier())
    {
        auto position = query_plan.getCurrentDataStream().header.getPositionByName(column_name);
        const auto * node_to_rename = rename_actions_dag->getOutputs()[position];
        rename_actions_dag->getOutputs()[position] = &rename_actions_dag->addAlias(*node_to_rename, column_identifier);
    }

    auto rename_step = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), rename_actions_dag);
    rename_step->setStepDescription("Change column names to column identifiers");
    query_plan.addStep(std::move(rename_step));

    return query_plan;
}

QueryPlan buildQueryPlanForJoinNode(QueryTreeNodePtr join_tree_node,
    SelectQueryInfo & select_query_info,
    const SelectQueryOptions & select_query_options,
    PlannerContextPtr & planner_context)
{
    auto & join_node = join_tree_node->as<JoinNode &>();

    auto left_plan = buildQueryPlanForJoinTreeNode(join_node.getLeftTableExpression(),
        select_query_info,
        select_query_options,
        planner_context);
    auto left_plan_output_columns = left_plan.getCurrentDataStream().header.getColumnsWithTypeAndName();

    auto right_plan = buildQueryPlanForJoinTreeNode(join_node.getRightTableExpression(),
        select_query_info,
        select_query_options,
        planner_context);
    auto right_plan_output_columns = right_plan.getCurrentDataStream().header.getColumnsWithTypeAndName();

    if (join_node.getStrictness() == JoinStrictness::Asof)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "JOIN {} ASOF is not supported",
            join_node.formatASTForErrorMessage());

    JoinClausesAndActions join_clauses_and_actions;

    if (join_node.isOnJoinExpression())
    {
        join_clauses_and_actions = buildJoinClausesAndActions(left_plan_output_columns,
            right_plan_output_columns,
            join_tree_node,
            planner_context);

        auto left_join_expressions_actions_step = std::make_unique<ExpressionStep>(left_plan.getCurrentDataStream(), join_clauses_and_actions.left_join_expressions_actions);
        left_join_expressions_actions_step->setStepDescription("JOIN actions");
        left_plan.addStep(std::move(left_join_expressions_actions_step));

        auto right_join_expressions_actions_step = std::make_unique<ExpressionStep>(right_plan.getCurrentDataStream(), join_clauses_and_actions.right_join_expressions_actions);
        right_join_expressions_actions_step->setStepDescription("JOIN actions");
        right_plan.addStep(std::move(right_join_expressions_actions_step));
    }

    std::unordered_map<ColumnIdentifier, DataTypePtr> left_plan_column_name_to_cast_type;
    std::unordered_map<ColumnIdentifier, DataTypePtr> right_plan_column_name_to_cast_type;

    if (join_node.isUsingJoinExpression())
    {
        auto & join_node_using_columns_list = join_node.getJoinExpression()->as<ListNode &>();
        for (auto & join_node_using_node : join_node_using_columns_list.getNodes())
        {
            auto & join_node_using_column_node = join_node_using_node->as<ColumnNode &>();
            auto & inner_columns_list = join_node_using_column_node.getExpressionOrThrow()->as<ListNode &>();

            auto & left_inner_column_node = inner_columns_list.getNodes().at(0);
            auto & left_inner_column = left_inner_column_node->as<ColumnNode &>();

            auto & right_inner_column_node = inner_columns_list.getNodes().at(1);
            auto & right_inner_column = right_inner_column_node->as<ColumnNode &>();

            const auto & join_node_using_column_node_type = join_node_using_column_node.getColumnType();
            if (!left_inner_column.getColumnType()->equals(*join_node_using_column_node_type))
            {
                auto left_inner_column_identifier = planner_context->getColumnNodeIdentifierOrThrow(left_inner_column_node.get());
                left_plan_column_name_to_cast_type.emplace(left_inner_column_identifier, join_node_using_column_node_type);
            }

            if (!right_inner_column.getColumnType()->equals(*join_node_using_column_node_type))
            {
                auto right_inner_column_identifier = planner_context->getColumnNodeIdentifierOrThrow(right_inner_column_node.get());
                right_plan_column_name_to_cast_type.emplace(right_inner_column_identifier, join_node_using_column_node_type);
            }
        }
    }

    auto join_cast_plan_output_nodes = [&](QueryPlan & plan_to_add_cast, std::unordered_map<std::string, DataTypePtr> & plan_column_name_to_cast_type)
    {
        auto cast_actions_dag = std::make_shared<ActionsDAG>(plan_to_add_cast.getCurrentDataStream().header.getColumnsWithTypeAndName());

        for (auto & output_node : cast_actions_dag->getOutputs())
        {
            auto it = plan_column_name_to_cast_type.find(output_node->result_name);
            if (it == plan_column_name_to_cast_type.end())
                continue;

            const auto & cast_type = it->second;
            auto cast_type_name = cast_type->getName();

            ColumnWithTypeAndName column;
            column.name = "__constant_" + cast_type_name;
            column.column = DataTypeString().createColumnConst(0, cast_type_name);
            column.type = std::make_shared<DataTypeString>();

            const auto * cast_type_constant_node = &cast_actions_dag->addColumn(std::move(column));

            FunctionCastBase::Diagnostic diagnostic = {output_node->result_name, output_node->result_name};
            FunctionOverloadResolverPtr func_builder_cast
                = CastInternalOverloadResolver<CastType::nonAccurate>::createImpl(std::move(diagnostic));

            ActionsDAG::NodeRawConstPtrs children = {output_node, cast_type_constant_node};
            output_node = &cast_actions_dag->addFunction(func_builder_cast, std::move(children), output_node->result_name);
        }

        auto cast_join_columns_step
            = std::make_unique<ExpressionStep>(plan_to_add_cast.getCurrentDataStream(), std::move(cast_actions_dag));
        cast_join_columns_step->setStepDescription("Cast JOIN USING columns");
        plan_to_add_cast.addStep(std::move(cast_join_columns_step));
    };

    if (!left_plan_column_name_to_cast_type.empty())
        join_cast_plan_output_nodes(left_plan, left_plan_column_name_to_cast_type);

    if (!right_plan_column_name_to_cast_type.empty())
        join_cast_plan_output_nodes(right_plan, right_plan_column_name_to_cast_type);

    const auto & query_context = planner_context->getQueryContext();
    JoinKind join_kind = join_node.getKind();
    bool join_use_nulls = query_context->getSettingsRef().join_use_nulls;
    auto to_nullable_function = FunctionFactory::instance().get("toNullable", query_context);

    auto join_cast_plan_columns_to_nullable = [&](QueryPlan & plan_to_add_cast)
    {
        auto cast_actions_dag = std::make_shared<ActionsDAG>(plan_to_add_cast.getCurrentDataStream().header.getColumnsWithTypeAndName());

        for (auto & output_node : cast_actions_dag->getOutputs())
        {
            if (output_node->type == ActionsDAG::ActionType::INPUT && output_node->result_name.starts_with("__column"))
                output_node = &cast_actions_dag->addFunction(to_nullable_function, {output_node}, output_node->result_name);
        }

        auto cast_join_columns_step = std::make_unique<ExpressionStep>(plan_to_add_cast.getCurrentDataStream(), std::move(cast_actions_dag));
        cast_join_columns_step->setStepDescription("Cast JOIN columns to Nullable");
        plan_to_add_cast.addStep(std::move(cast_join_columns_step));
    };

    if (join_use_nulls)
    {
        if (isFull(join_kind))
        {
            join_cast_plan_columns_to_nullable(left_plan);
            join_cast_plan_columns_to_nullable(right_plan);
        }
        else if (isLeft(join_kind))
        {
            join_cast_plan_columns_to_nullable(right_plan);
        }
        else if (isRight(join_kind))
        {
            join_cast_plan_columns_to_nullable(left_plan);
        }
    }

    auto table_join = std::make_shared<TableJoin>();
    table_join->getTableJoin() = join_node.toASTTableJoin()->as<ASTTableJoin &>();
    if (join_node.getKind() == JoinKind::Comma)
        table_join->getTableJoin().kind = JoinKind::Cross;
    table_join->getTableJoin().strictness = JoinStrictness::All;

    if (join_node.isOnJoinExpression())
    {
        const auto & join_clauses = join_clauses_and_actions.join_clauses;
        auto & table_join_clauses = table_join->getClauses();

        for (const auto & join_clause : join_clauses)
        {
            table_join_clauses.emplace_back();
            auto & table_join_clause = table_join_clauses.back();

            const auto & join_clause_left_key_nodes = join_clause.getLeftKeyNodes();
            const auto & join_clause_right_key_nodes = join_clause.getRightKeyNodes();

            size_t join_clause_key_nodes_size = join_clause_left_key_nodes.size();
            assert(join_clause_key_nodes_size == join_clause_right_key_nodes.size());

            for (size_t i = 0; i < join_clause_key_nodes_size; ++i)
            {
                table_join_clause.key_names_left.push_back(join_clause_left_key_nodes[i]->result_name);
                table_join_clause.key_names_right.push_back(join_clause_right_key_nodes[i]->result_name);
            }

            const auto & join_clause_get_left_filter_condition_nodes = join_clause.getLeftFilterConditionNodes();
            if (!join_clause_get_left_filter_condition_nodes.empty())
            {
                if (join_clause_get_left_filter_condition_nodes.size() != 1)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "JOIN {} left filter conditions size must be 1. Actual {}",
                        join_node.formatASTForErrorMessage(),
                        join_clause_get_left_filter_condition_nodes.size());

                const auto & join_clause_left_filter_condition_name = join_clause_get_left_filter_condition_nodes[0]->result_name;
                table_join_clause.analyzer_left_filter_condition_column_name = join_clause_left_filter_condition_name;
            }

            const auto & join_clause_get_right_filter_condition_nodes = join_clause.getRightFilterConditionNodes();
            if (!join_clause_get_right_filter_condition_nodes.empty())
            {
                if (join_clause_get_right_filter_condition_nodes.size() != 1)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "JOIN {} right filter conditions size must be 1. Actual {}",
                        join_node.formatASTForErrorMessage(),
                        join_clause_get_right_filter_condition_nodes.size());

                const auto & join_clause_right_filter_condition_name = join_clause_get_right_filter_condition_nodes[0]->result_name;
                table_join_clause.analyzer_right_filter_condition_column_name = join_clause_right_filter_condition_name;
            }
        }
    }
    else if (join_node.isUsingJoinExpression())
    {
        auto & table_join_clauses = table_join->getClauses();
        table_join_clauses.emplace_back();
        auto & table_join_clause = table_join_clauses.back();

        auto & using_list = join_node.getJoinExpression()->as<ListNode &>();

        for (auto & join_using_node : using_list.getNodes())
        {
            auto & join_using_column_node = join_using_node->as<ColumnNode &>();
            if (!join_using_column_node.getExpression() ||
                join_using_column_node.getExpression()->getNodeType() != QueryTreeNodeType::LIST)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "JOIN {} column in USING does not have inner columns",
                    join_node.formatASTForErrorMessage());

            auto & using_join_columns_list = join_using_column_node.getExpression()->as<ListNode &>();
            auto & using_join_left_join_column_node = using_join_columns_list.getNodes().at(0);
            auto & using_join_right_join_column_node = using_join_columns_list.getNodes().at(1);

            auto left_column_identifier = planner_context->getColumnNodeIdentifierOrThrow(using_join_left_join_column_node.get());
            auto right_column_identifier = planner_context->getColumnNodeIdentifierOrThrow(using_join_right_join_column_node.get());

            table_join_clause.key_names_left.push_back(left_column_identifier);
            table_join_clause.key_names_right.push_back(right_column_identifier);
        }
    }

    auto left_table_names = left_plan.getCurrentDataStream().header.getNames();
    NameSet left_table_names_set(left_table_names.begin(), left_table_names.end());

    auto columns_from_joined_table = right_plan.getCurrentDataStream().header.getNamesAndTypesList();
    table_join->setColumnsFromJoinedTable(columns_from_joined_table, left_table_names_set, "");

    for (auto & column_from_joined_table : columns_from_joined_table)
    {
        if (column_from_joined_table.name.starts_with("__column"))
            table_join->addJoinedColumn(column_from_joined_table);
    }

    size_t max_block_size = query_context->getSettingsRef().max_block_size;
    size_t max_streams = query_context->getSettingsRef().max_threads;

    JoinPtr join_ptr = std::make_shared<HashJoin>(table_join, right_plan.getCurrentDataStream().header, false /*any_take_last_row*/);
    QueryPlanStepPtr join_step = std::make_unique<JoinStep>(
        left_plan.getCurrentDataStream(),
        right_plan.getCurrentDataStream(),
        join_ptr,
        max_block_size,
        max_streams,
        false /*optimize_read_in_order*/);

    join_step->setStepDescription(fmt::format("JOIN {}", JoinPipelineType::FillRightFirst));

    std::vector<QueryPlanPtr> plans;
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(left_plan)));
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(right_plan)));

    auto result_plan = QueryPlan();
    result_plan.unitePlans(std::move(join_step), {std::move(plans)});

    return result_plan;
}

QueryPlan buildQueryPlanForArrayJoinNode(QueryTreeNodePtr table_expression,
    SelectQueryInfo & select_query_info,
    const SelectQueryOptions & select_query_options,
    PlannerContextPtr & planner_context)
{
    auto & array_join_node = table_expression->as<ArrayJoinNode &>();

    auto left_plan = buildQueryPlanForJoinTreeNode(array_join_node.getTableExpression(),
        select_query_info,
        select_query_options,
        planner_context);
    auto left_plan_output_columns = left_plan.getCurrentDataStream().header.getColumnsWithTypeAndName();

    ActionsDAGPtr array_join_action_dag = std::make_shared<ActionsDAG>(left_plan_output_columns);
    PlannerActionsVisitor actions_visitor(planner_context);

    NameSet array_join_columns;
    for (auto & array_join_expression : array_join_node.getJoinExpressions().getNodes())
    {
        auto & array_join_expression_column = array_join_expression->as<ColumnNode &>();
        const auto & array_join_column_name = array_join_expression_column.getColumnName();
        array_join_columns.insert(array_join_column_name);

        auto expression_dag_index_nodes = actions_visitor.visit(array_join_action_dag, array_join_expression_column.getExpressionOrThrow());
        for (auto & expression_dag_index_node : expression_dag_index_nodes)
        {
            const auto * array_join_column_node = &array_join_action_dag->addAlias(*expression_dag_index_node, array_join_column_name);
            array_join_action_dag->getOutputs().push_back(array_join_column_node);
        }
    }

    auto array_join_actions = std::make_unique<ExpressionStep>(left_plan.getCurrentDataStream(), array_join_action_dag);
    array_join_actions->setStepDescription("ARRAY JOIN actions");
    left_plan.addStep(std::move(array_join_actions));

    auto array_join_action = std::make_shared<ArrayJoinAction>(array_join_columns, array_join_node.isLeft(), planner_context->getQueryContext());
    auto array_join_step = std::make_unique<ArrayJoinStep>(left_plan.getCurrentDataStream(), std::move(array_join_action));
    array_join_step->setStepDescription("ARRAY JOIN");
    left_plan.addStep(std::move(array_join_step));

    return left_plan;
}

QueryPlan buildQueryPlanForJoinTreeNode(QueryTreeNodePtr join_tree_node,
    SelectQueryInfo & select_query_info,
    const SelectQueryOptions & select_query_options,
    PlannerContextPtr & planner_context)
{
    auto join_tree_node_type = join_tree_node->getNodeType();

    switch (join_tree_node_type)
    {
        case QueryTreeNodeType::QUERY:
            [[fallthrough]];
        case QueryTreeNodeType::UNION:
            [[fallthrough]];
        case QueryTreeNodeType::TABLE:
            [[fallthrough]];
        case QueryTreeNodeType::TABLE_FUNCTION:
        {
            SelectQueryInfo table_expression_query_info = select_query_info;
            return buildQueryPlanForTableExpression(join_tree_node, table_expression_query_info, select_query_options, planner_context);
        }
        case QueryTreeNodeType::JOIN:
        {
            return buildQueryPlanForJoinNode(join_tree_node, select_query_info, select_query_options, planner_context);
        }
        case QueryTreeNodeType::ARRAY_JOIN:
        {
            return buildQueryPlanForArrayJoinNode(join_tree_node, select_query_info, select_query_options, planner_context);
        }
        default:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Expected query, table, table function, join or array join query node. Actual {}",
                join_tree_node->formatASTForErrorMessage());
        }
    }
}

void addBuildSubqueriesForSetsStepIfNeeded(QueryPlan & query_plan, const SelectQueryOptions & select_query_options, const PlannerContextPtr & planner_context)
{
    if (select_query_options.is_subquery)
        return;

    SubqueriesForSets subqueries_for_sets;
    const auto & subquery_node_to_sets = planner_context->getGlobalPlannerContext()->getSubqueryNodesForSets();

    for (auto [key, subquery_node_for_set] : subquery_node_to_sets)
    {
        /** The subquery in the IN / JOIN section does not have any restrictions on the maximum size of the result.
          * Because the result of this query is not the result of the entire query.
          * Constraints work instead
          *  max_rows_in_set, max_bytes_in_set, set_overflow_mode,
          *  max_rows_in_join, max_bytes_in_join, join_overflow_mode,
          *  which are checked separately (in the Set, Join objects).
          */
        auto subquery_context = Context::createCopy(planner_context->getQueryContext());
        Settings subquery_settings = planner_context->getQueryContext()->getSettings();
        subquery_settings.max_result_rows = 0;
        subquery_settings.max_result_bytes = 0;
        /// The calculation of `extremes` does not make sense and is not necessary (if you do it, then the `extremes` of the subquery can be taken instead of the whole query).
        subquery_settings.extremes = false;
        subquery_context->setSettings(subquery_settings);

        auto subquery_options = select_query_options.subquery();
        Planner subquery_planner(
            subquery_node_for_set.subquery_node,
            subquery_options,
            planner_context->getQueryContext(),
            planner_context->getGlobalPlannerContext());
        subquery_planner.buildQueryPlanIfNeeded();

        SubqueryForSet subquery_for_set;
        subquery_for_set.set = subquery_node_for_set.set;
        subquery_for_set.source = std::make_unique<QueryPlan>(std::move(subquery_planner).extractQueryPlan());

        subqueries_for_sets.emplace(key, std::move(subquery_for_set));
    }

    const Settings & settings = planner_context->getQueryContext()->getSettingsRef();
    SizeLimits limits(settings.max_rows_to_transfer, settings.max_bytes_to_transfer, settings.transfer_overflow_mode);
    addCreatingSetsStep(query_plan, std::move(subqueries_for_sets), limits, planner_context->getQueryContext());
}

}

Planner::Planner(const QueryTreeNodePtr & query_tree_,
    const SelectQueryOptions & select_query_options_,
    ContextPtr context_)
    : WithContext(context_)
    , query_tree(query_tree_)
    , select_query_options(select_query_options_)
    , planner_context(std::make_shared<PlannerContext>(context_, std::make_shared<GlobalPlannerContext>()))
{
    if (query_tree->getNodeType() != QueryTreeNodeType::QUERY &&
        query_tree->getNodeType() != QueryTreeNodeType::UNION)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Expected QUERY or UNION node. Actual {}",
            query_tree->formatASTForErrorMessage());
}

/// Initialize interpreter with query tree after query analysis phase and global planner context
Planner::Planner(const QueryTreeNodePtr & query_tree_,
    const SelectQueryOptions & select_query_options_,
    ContextPtr context_,
    GlobalPlannerContextPtr global_planner_context_)
    : WithContext(context_)
    , query_tree(query_tree_)
    , select_query_options(select_query_options_)
    , planner_context(std::make_shared<PlannerContext>(context_, std::move(global_planner_context_)))
{
    if (query_tree->getNodeType() != QueryTreeNodeType::QUERY &&
        query_tree->getNodeType() != QueryTreeNodeType::UNION)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Expected QUERY or UNION node. Actual {}",
            query_tree->formatASTForErrorMessage());
}

void Planner::buildQueryPlanIfNeeded()
{
    if (query_plan.isInitialized())
        return;

    auto current_context = getContext();

    if (auto * union_query_tree = query_tree->as<UnionNode>())
    {
        auto union_mode = union_query_tree->getUnionMode();
        if (union_mode == SelectUnionMode::Unspecified)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "UNION mode must be initialized");

        size_t queries_size = union_query_tree->getQueries().getNodes().size();

        std::vector<std::unique_ptr<QueryPlan>> query_plans;
        query_plans.reserve(queries_size);

        Blocks query_plans_headers;
        query_plans_headers.reserve(queries_size);

        for (auto & query_node : union_query_tree->getQueries().getNodes())
        {
            Planner query_planner(query_node, select_query_options, current_context);
            query_planner.buildQueryPlanIfNeeded();
            auto query_node_plan = std::make_unique<QueryPlan>(std::move(query_planner).extractQueryPlan());
            query_plans_headers.push_back(query_node_plan->getCurrentDataStream().header);
            query_plans.push_back(std::move(query_node_plan));
        }

        Block union_common_header = buildCommonHeaderForUnion(query_plans_headers);
        DataStreams query_plans_streams;
        query_plans_streams.reserve(query_plans.size());

        for (auto & query_node_plan : query_plans)
        {
            if (blocksHaveEqualStructure(query_node_plan->getCurrentDataStream().header, union_common_header))
            {
                query_plans_streams.push_back(query_node_plan->getCurrentDataStream());
                continue;
            }

            auto actions_dag = ActionsDAG::makeConvertingActions(
                    query_node_plan->getCurrentDataStream().header.getColumnsWithTypeAndName(),
                    union_common_header.getColumnsWithTypeAndName(),
                    ActionsDAG::MatchColumnsMode::Position);
            auto converting_step = std::make_unique<ExpressionStep>(query_node_plan->getCurrentDataStream(), std::move(actions_dag));
            converting_step->setStepDescription("Conversion before UNION");
            query_node_plan->addStep(std::move(converting_step));

            query_plans_streams.push_back(query_node_plan->getCurrentDataStream());
        }

        const auto & settings = current_context->getSettingsRef();
        auto max_threads = settings.max_threads;

        if (union_mode == SelectUnionMode::ALL || union_mode == SelectUnionMode::DISTINCT)
        {
            auto union_step = std::make_unique<UnionStep>(std::move(query_plans_streams), max_threads);
            query_plan.unitePlans(std::move(union_step), std::move(query_plans));

            if (union_query_tree->getUnionMode() == SelectUnionMode::DISTINCT)
            {
                /// Add distinct transform
                SizeLimits limits(settings.max_rows_in_distinct, settings.max_bytes_in_distinct, settings.distinct_overflow_mode);

                auto distinct_step = std::make_unique<DistinctStep>(
                    query_plan.getCurrentDataStream(),
                    limits,
                    0 /*limit hint*/,
                    query_plan.getCurrentDataStream().header.getNames(),
                    false /*pre distinct*/,
                    settings.optimize_distinct_in_order);

                query_plan.addStep(std::move(distinct_step));
            }
        }
        else if (union_mode == SelectUnionMode::INTERSECT || union_mode == SelectUnionMode::EXCEPT)
        {
            IntersectOrExceptStep::Operator intersect_or_except_operator = IntersectOrExceptStep::Operator::INTERSECT;
            if (union_mode == SelectUnionMode::EXCEPT)
                intersect_or_except_operator = IntersectOrExceptStep::Operator::EXCEPT;

            auto union_step = std::make_unique<IntersectOrExceptStep>(std::move(query_plans_streams), intersect_or_except_operator, max_threads);
            query_plan.unitePlans(std::move(union_step), std::move(query_plans));
        }

        return;
    }

    auto & query_node = query_tree->as<QueryNode &>();

    SelectQueryInfo select_query_info;
    select_query_info.original_query = queryNodeToSelectQuery(query_tree);
    select_query_info.query = select_query_info.original_query;

    StorageLimitsList storage_limits;
    storage_limits.push_back(getStorageLimits(*current_context, select_query_options));
    select_query_info.storage_limits = std::make_shared<StorageLimitsList>(storage_limits);

    CollectTableExpressionIdentifiersVisitor collect_table_expression_identifiers_visitor;
    collect_table_expression_identifiers_visitor.visit(query_node.getJoinTree(), *planner_context);

    CollectSourceColumnsVisitor::Data data {*planner_context};
    CollectSourceColumnsVisitor collect_source_columns_visitor(data);
    collect_source_columns_visitor.visit(query_tree);

    collectSets(query_tree, *planner_context);

    query_plan = buildQueryPlanForJoinTreeNode(query_node.getJoinTree(), select_query_info, select_query_options, planner_context);

    ActionsChain actions_chain;
    std::optional<size_t> where_action_step_index;
    std::string where_action_node_name;

    if (query_node.hasWhere())
    {
        const auto * chain_available_output_columns = actions_chain.getLastStepAvailableOutputColumnsOrNull();
        const auto & where_input = chain_available_output_columns ? *chain_available_output_columns : query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName();

        auto where_actions = convertExpressionNodeIntoDAG(query_node.getWhere(), where_input, planner_context);
        where_action_node_name = where_actions->getOutputs().at(0)->result_name;
        actions_chain.addStep(std::make_unique<ActionsChainStep>(std::move(where_actions)));
        where_action_step_index = actions_chain.getLastStepIndex();
    }

    if (query_node.hasWhere())
        assertNoAggregatesFunctions(query_node.getWhere(), "in WHERE", *planner_context);
    if (query_node.hasPrewhere())
        assertNoAggregatesFunctions(query_node.getWhere(), "in PREWHERE", *planner_context);

    AggregateDescriptions aggregates_descriptions = extractAggregateDescriptions(query_tree, *planner_context);
    ColumnsWithTypeAndName aggregates_columns;
    aggregates_columns.reserve(aggregates_descriptions.size());
    for (auto & aggregate_description : aggregates_descriptions)
        aggregates_columns.emplace_back(nullptr, aggregate_description.function->getReturnType(), aggregate_description.column_name);

    NameSet aggregate_keys_set;
    Names aggregate_keys;
    std::optional<size_t> aggregate_step_index;

    if (query_node.hasGroupBy())
    {
        const auto * chain_available_output_columns = actions_chain.getLastStepAvailableOutputColumnsOrNull();
        const auto & group_by_input = chain_available_output_columns ? *chain_available_output_columns : query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName();

        /// Only aggregation keys, and aggregates are available for next steps after GROUP BY step
        auto group_by_actions = convertExpressionNodeIntoDAG(query_node.getGroupByNode(), group_by_input, planner_context);

        aggregate_keys.reserve(group_by_actions->getOutputs().size());
        for (auto & output : group_by_actions->getOutputs())
        {
            aggregate_keys_set.insert(output->result_name);
            aggregate_keys.push_back(output->result_name);
        }

        ValidateGroupByColumnsVisitor::Data validate_group_by_visitor_data(aggregate_keys_set, *planner_context);
        ValidateGroupByColumnsVisitor validate_columns_visitor(validate_group_by_visitor_data);
        validate_columns_visitor.visit(query_node.getProjectionNode());
        validate_columns_visitor.visit(query_node.getOrderByNode());

        auto aggregate_step = std::make_unique<ActionsChainStep>(std::move(group_by_actions), ActionsChainStep::AvailableOutputColumnsStrategy::OUTPUT_NODES, aggregates_columns);
        actions_chain.addStep(std::move(aggregate_step));
        aggregate_step_index = actions_chain.getLastStepIndex();
    }

    std::optional<size_t> before_order_by_step_index;
    if (query_node.hasOrderBy())
    {
        const auto * chain_available_output_columns = actions_chain.getLastStepAvailableOutputColumnsOrNull();
        const auto & order_by_input = chain_available_output_columns ? *chain_available_output_columns : query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName();

        ActionsDAGPtr actions_dag = std::make_shared<ActionsDAG>(order_by_input);
        auto & actions_dag_outputs = actions_dag->getOutputs();
        actions_dag_outputs.clear();

        PlannerActionsVisitor actions_visitor(planner_context);

        /** We add only sort column sort expression in before ORDER BY actions DAG.
          * WITH fill expressions must be constant nodes.
          */
        auto & order_by_node_list = query_node.getOrderByNode()->as<ListNode &>();
        for (auto & sort_column_node : order_by_node_list.getNodes())
        {
            auto & sort_column_node_typed = sort_column_node->as<SortColumnNode &>();
            auto expression_dag_index_nodes = actions_visitor.visit(actions_dag, sort_column_node_typed.getExpression());
            actions_dag_outputs.insert(actions_dag_outputs.end(), expression_dag_index_nodes.begin(), expression_dag_index_nodes.end());
        }

        auto actions_step_before_order_by = std::make_unique<ActionsChainStep>(std::move(actions_dag));
        actions_chain.addStep(std::move(actions_step_before_order_by));
        before_order_by_step_index = actions_chain.getLastStepIndex();
    }

    const auto * chain_available_output_columns = actions_chain.getLastStepAvailableOutputColumnsOrNull();
    const auto & projection_input = chain_available_output_columns ? *chain_available_output_columns : query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName();
    auto projection_actions = convertExpressionNodeIntoDAG(query_node.getProjectionNode(), projection_input, planner_context);

    const auto & projection_action_dag_nodes = projection_actions->getOutputs();
    size_t projection_action_dag_nodes_size = projection_action_dag_nodes.size();

    auto & projection_nodes = query_node.getProjection().getNodes();
    size_t projection_nodes_size = projection_nodes.size();

    if (projection_nodes_size != projection_action_dag_nodes_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "QueryTree projection nodes size mismatch. Expected {}. Actual {}",
            projection_action_dag_nodes_size,
            projection_nodes_size);

    Names projection_names;
    projection_names.reserve(projection_nodes_size);

    NamesWithAliases projection_names_with_aliases;
    projection_names_with_aliases.reserve(projection_nodes_size);

    for (size_t i = 0; i < projection_nodes_size; ++i)
    {
        auto & node = projection_nodes[i];
        auto node_name = node->getName();
        const auto * action_dag_node = projection_action_dag_nodes[i];
        const auto & actions_dag_node_name = action_dag_node->result_name;

        projection_names.push_back(actions_dag_node_name);

        if (node->hasAlias())
            projection_names_with_aliases.push_back({actions_dag_node_name, node->getAlias()});
        else
            projection_names_with_aliases.push_back({actions_dag_node_name, node_name});
    }

    projection_actions->project(projection_names_with_aliases);

    actions_chain.addStep(std::make_unique<ActionsChainStep>(std::move(projection_actions)));
    size_t projection_action_step_index = actions_chain.getLastStepIndex();

    // std::cout << "Chain dump before finalize" << std::endl;
    // std::cout << actions_chain.dump() << std::endl;

    actions_chain.finalize();

    // std::cout << "Chain dump after finalize" << std::endl;
    // std::cout << actions_chain.dump() << std::endl;

    if (where_action_step_index)
    {
        auto & where_actions_chain_node = actions_chain.at(*where_action_step_index);
        bool remove_filter = !where_actions_chain_node->getChildRequiredOutputColumnsNames().contains(where_action_node_name);
        auto where_step = std::make_unique<FilterStep>(query_plan.getCurrentDataStream(),
            where_actions_chain_node->getActions(),
            where_action_node_name,
            remove_filter);
        where_step->setStepDescription("WHERE");
        query_plan.addStep(std::move(where_step));
    }

    if (!aggregates_descriptions.empty() || query_node.hasGroupBy())
    {
        if (aggregate_step_index)
        {
            auto & aggregate_actions_chain_node = actions_chain.at(*aggregate_step_index);
            auto expression_before_aggregation = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(),
                aggregate_actions_chain_node->getActions());
            expression_before_aggregation->setStepDescription("Before GROUP BY");
            query_plan.addStep(std::move(expression_before_aggregation));
        }

        const Settings & settings = planner_context->getQueryContext()->getSettingsRef();

        bool overflow_row = false;
        bool query_analyzer_const_aggregation_keys = false;

        const auto stats_collecting_params = Aggregator::Params::StatsCollectingParams(
            select_query_info.query,
            settings.collect_hash_table_stats_during_aggregation,
            settings.max_entries_for_hash_table_stats,
            settings.max_size_to_preallocate_for_aggregation);

        Aggregator::Params aggregator_params = Aggregator::Params(
            aggregate_keys,
            aggregates_descriptions,
            overflow_row,
            settings.max_rows_to_group_by,
            settings.group_by_overflow_mode,
            settings.group_by_two_level_threshold,
            settings.group_by_two_level_threshold_bytes,
            settings.max_bytes_before_external_group_by,
            settings.empty_result_for_aggregation_by_empty_set
                || (settings.empty_result_for_aggregation_by_constant_keys_on_empty_set && aggregate_keys.empty()
                    && query_analyzer_const_aggregation_keys),
            planner_context->getQueryContext()->getTemporaryVolume(),
            settings.max_threads,
            settings.min_free_disk_space_for_temporary_data,
            settings.compile_aggregate_expressions,
            settings.min_count_to_compile_aggregate_expression,
            /* only_merge */ false,
            stats_collecting_params
        );

        GroupingSetsParamsList grouping_sets_params;
        SortDescription group_by_sort_description;

        auto merge_threads = settings.max_threads;
        auto temporary_data_merge_threads = settings.aggregation_memory_efficient_merge_threads
            ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads)
            : static_cast<size_t>(settings.max_threads);

        bool storage_has_evenly_distributed_read = false;
        auto & table_expression_node_to_columns = planner_context->getTableExpressionNodeToColumns();
        if (table_expression_node_to_columns.size() == 1)
        {
            auto it = table_expression_node_to_columns.begin();
            const auto * table_expression_node = it->first;
            if (const auto * table_node = table_expression_node->as<TableNode>())
                storage_has_evenly_distributed_read = table_node->getStorage()->hasEvenlyDistributedRead();
            else if (const auto * table_function_node = table_expression_node->as<TableFunctionNode>())
                storage_has_evenly_distributed_read = table_function_node->getStorageOrThrow()->hasEvenlyDistributedRead();
        }

        const bool should_produce_results_in_order_of_bucket_number
            = select_query_options.to_stage == QueryProcessingStage::WithMergeableState && settings.distributed_aggregation_memory_efficient;

        bool aggregate_final = select_query_options.to_stage > QueryProcessingStage::WithMergeableState;

        InputOrderInfoPtr input_order_info;

        auto aggregating_step = std::make_unique<AggregatingStep>(
            query_plan.getCurrentDataStream(),
            std::move(aggregator_params),
            std::move(grouping_sets_params),
            aggregate_final,
            settings.max_block_size,
            settings.aggregation_in_order_max_block_bytes,
            merge_threads,
            temporary_data_merge_threads,
            storage_has_evenly_distributed_read,
            settings.group_by_use_nulls,
            std::move(input_order_info),
            std::move(group_by_sort_description),
            should_produce_results_in_order_of_bucket_number);
        query_plan.addStep(std::move(aggregating_step));
    }

    if (query_node.isDistinct())
    {
        const Settings & settings = planner_context->getQueryContext()->getSettingsRef();
        UInt64 limit_hint_for_distinct = 0;
        bool pre_distinct = true;

        SizeLimits limits(settings.max_rows_in_distinct, settings.max_bytes_in_distinct, settings.distinct_overflow_mode);

        auto distinct_step = std::make_unique<DistinctStep>(
            query_plan.getCurrentDataStream(),
            limits,
            limit_hint_for_distinct,
            projection_names,
            pre_distinct,
            settings.optimize_distinct_in_order);

        if (pre_distinct)
            distinct_step->setStepDescription("Preliminary DISTINCT");
        else
            distinct_step->setStepDescription("DISTINCT");

        query_plan.addStep(std::move(distinct_step));
    }

    if (before_order_by_step_index)
    {
        auto & aggregate_actions_chain_node = actions_chain.at(*before_order_by_step_index);
        auto expression_step_before_order_by = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(),
            aggregate_actions_chain_node->getActions());
        expression_step_before_order_by->setStepDescription("Before ORDER BY");
        query_plan.addStep(std::move(expression_step_before_order_by));
    }

    SortDescription sort_description;

    if (query_node.hasOrderBy())
    {
        sort_description = extractSortDescription(query_node.getOrderByNode(), *planner_context);
        String sort_description_dump = dumpSortDescription(sort_description);

        UInt64 limit = 0;

        const Settings & settings = planner_context->getQueryContext()->getSettingsRef();

        /// Merge the sorted blocks.
        auto sorting_step = std::make_unique<SortingStep>(
            query_plan.getCurrentDataStream(),
            sort_description,
            settings.max_block_size,
            limit,
            SizeLimits(settings.max_rows_to_sort, settings.max_bytes_to_sort, settings.sort_overflow_mode),
            settings.max_bytes_before_remerge_sort,
            settings.remerge_sort_lowered_memory_bytes_ratio,
            settings.max_bytes_before_external_sort,
            planner_context->getQueryContext()->getTemporaryVolume(),
            settings.min_free_disk_space_for_temporary_data);

        sorting_step->setStepDescription("Sorting for ORDER BY");
        query_plan.addStep(std::move(sorting_step));

        SortDescription fill_description;
        for (auto & description : sort_description)
        {
            if (description.with_fill)
                fill_description.push_back(description);
        }

        if (!fill_description.empty())
        {
            InterpolateDescriptionPtr interpolate_descr = nullptr;
            auto filling_step = std::make_unique<FillingStep>(query_plan.getCurrentDataStream(), std::move(fill_description), interpolate_descr);
            query_plan.addStep(std::move(filling_step));
        }
    }

    UInt64 limit_offset = 0;
    if (query_node.hasOffset())
    {
        /// Validated during query analysis stage
        limit_offset = query_node.getOffset()->as<ConstantNode &>().getConstantValue().safeGet<UInt64>();
    }

    if (query_node.hasLimit())
    {
        const Settings & settings = planner_context->getQueryContext()->getSettingsRef();
        bool always_read_till_end = settings.exact_rows_before_limit;
        bool limit_with_ties = query_node.isLimitWithTies();

        /// Validated during query analysis stage
        UInt64 limit_length = query_node.getLimit()->as<ConstantNode &>().getConstantValue().safeGet<UInt64>();

        SortDescription limit_with_ties_sort_description;

        if (query_node.isLimitWithTies())
        {
            /// Validated during parser stage
            if (!query_node.hasOrderBy())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "LIMIT WITH TIES without ORDER BY");

            limit_with_ties_sort_description = sort_description;
        }

        auto limit = std::make_unique<LimitStep>(query_plan.getCurrentDataStream(),
            limit_length,
            limit_offset,
            always_read_till_end,
            limit_with_ties,
            limit_with_ties_sort_description);

        if (limit_with_ties)
            limit->setStepDescription("LIMIT WITH TIES");

        query_plan.addStep(std::move(limit));
    }
    else if (query_node.hasOffset())
    {
        auto offsets_step = std::make_unique<OffsetStep>(query_plan.getCurrentDataStream(), limit_offset);
        query_plan.addStep(std::move(offsets_step));
    }

    auto projection_step = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), actions_chain[projection_action_step_index]->getActions());
    projection_step->setStepDescription("Projection");
    query_plan.addStep(std::move(projection_step));

    addBuildSubqueriesForSetsStepIfNeeded(query_plan, select_query_options, planner_context);
}

}
