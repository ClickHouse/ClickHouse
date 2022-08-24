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
#include <Planner/ActionsChain.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int INVALID_JOIN_ON_EXPRESSION;
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
  * TODO: Support building sets for IN functions
  * TODO: Support trivial count optimization
  * TODO: Support totals, extremes
  * TODO: Support projections
  * TODO: Support read in order optimization
  * TODO: UNION storage limits
  * TODO: Interpreter resources
  */

namespace
{

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
                    "Expected query, table, table function, join or array join query node. Actual {}",
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
    PlannerActionsVisitor actions_visitor(action_dag, planner_context);
    auto expression_dag_index_nodes = actions_visitor.visit(expression_node);
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
        Planner subquery_planner(table_expression, select_query_options, planner_context->getQueryContext(), planner_context->getGlobalPlannerContext());
        subquery_planner.initializeQueryPlanIfNeeded();
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
    PlannerActionsVisitor actions_visitor(array_join_action_dag, planner_context);

    NameSet array_join_columns;
    for (auto & array_join_expression : array_join_node.getJoinExpressions().getNodes())
    {
        auto & array_join_expression_column = array_join_expression->as<ColumnNode &>();
        const auto & array_join_column_name = array_join_expression_column.getColumnName();
        array_join_columns.insert(array_join_column_name);

        auto expression_dag_index_nodes = actions_visitor.visit(array_join_expression_column.getExpressionOrThrow());
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

void Planner::initializeQueryPlanIfNeeded()
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
            query_planner.initializeQueryPlanIfNeeded();
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
                continue;

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
    auto query = query_node.toAST();

    SelectQueryInfo select_query_info;
    select_query_info.original_query = query;
    select_query_info.query = query;

    CollectTableExpressionIdentifiersVisitor collect_table_expression_identifiers_visitor;
    collect_table_expression_identifiers_visitor.visit(query_node.getJoinTree(), *planner_context);

    CollectSourceColumnsVisitor::Data data {*planner_context};
    CollectSourceColumnsVisitor collect_source_columns_visitor(data);
    collect_source_columns_visitor.visit(query_tree);

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

    NamesWithAliases projection_names;

    for (size_t i = 0; i < projection_nodes_size; ++i)
    {
        auto & node = projection_nodes[i];
        auto node_name = node->getName();
        const auto * action_dag_node = projection_action_dag_nodes[i];
        const auto & actions_dag_node_name = action_dag_node->result_name;

        if (node->hasAlias())
            projection_names.push_back({actions_dag_node_name, node->getAlias()});
        else
            projection_names.push_back({actions_dag_node_name, node_name});
    }

    projection_actions->project(projection_names);

    actions_chain.addStep(std::make_unique<ActionsChainStep>(std::move(projection_actions)));
    size_t projection_action_step_index = actions_chain.getLastStepIndex();

    // std::cout << "Chain dump before finalize" << std::endl;
    // std::cout << planner_context.actions_chain.dump() << std::endl;

    actions_chain.finalize();

    // std::cout << "Chain dump after finalize" << std::endl;
    // std::cout << planner_context.actions_chain.dump() << std::endl;

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

    // std::cout << "Query plan dump" << std::endl;
    // std::cout << dumpQueryPlan(query_plan) << std::endl;

    auto projection_step = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), actions_chain[projection_action_step_index]->getActions());
    projection_step->setStepDescription("Projection");
    query_plan.addStep(std::move(projection_step));
}

}
