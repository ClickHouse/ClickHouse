#include <Planner/PlannerJoinTree.h>

#include <DataTypes/DataTypeString.h>

#include <Functions/FunctionFactory.h>
#include <Functions/CastOverloadResolver.h>

#include <Storages/IStorage.h>
#include <Storages/StorageDictionary.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/ArrayJoinNode.h>

#include <Processors/Sources/NullSource.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>

#include <Interpreters/Context.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/ArrayJoinAction.h>

#include <Planner/Planner.h>
#include <Planner/PlannerJoins.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Planner/Utils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_JOIN_ON_EXPRESSION;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int SYNTAX_ERROR;
}

namespace
{

QueryPlan buildQueryPlanForTableExpression(QueryTreeNodePtr table_expression,
    SelectQueryInfo & select_query_info,
    const SelectQueryOptions & select_query_options,
    PlannerContextPtr & planner_context)
{
    auto * table_node = table_expression->as<TableNode>();
    auto * table_function_node = table_expression->as<TableFunctionNode>();
    auto * query_node = table_expression->as<QueryNode>();
    auto * union_node = table_expression->as<UnionNode>();

    QueryPlan query_plan;

    auto & table_expression_data = planner_context->getTableExpressionDataOrThrow(table_expression);

    if (table_node || table_function_node)
    {
        const auto & storage = table_node ? table_node->getStorage() : table_function_node->getStorage();
        const auto & storage_snapshot = table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot();

        auto table_expression_query_info = select_query_info;
        table_expression_query_info.table_expression = table_expression;

        if (table_node)
            table_expression_query_info.table_expression_modifiers = table_node->getTableExpressionModifiers();
        else
            table_expression_query_info.table_expression_modifiers = table_function_node->getTableExpressionModifiers();

        auto from_stage = storage->getQueryProcessingStage(planner_context->getQueryContext(), select_query_options.to_stage, storage_snapshot, table_expression_query_info);
        const auto & columns_names = table_expression_data.getColumnsNames();
        Names column_names(columns_names.begin(), columns_names.end());

        if (column_names.empty())
        {
            auto column_names_and_types = storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns());
            auto additional_column_to_read = column_names_and_types.front();

            const auto & column_identifier = planner_context->getGlobalPlannerContext()->createColumnIdentifier(additional_column_to_read, table_expression);
            column_names.push_back(additional_column_to_read.name);
            table_expression_data.addColumn(additional_column_to_read, column_identifier);
        }

        const auto & query_context = planner_context->getQueryContext();
        size_t max_block_size = query_context->getSettingsRef().max_block_size;
        size_t max_streams = query_context->getSettingsRef().max_threads;

        bool need_rewrite_query_with_final = storage->needRewriteQueryWithFinal(column_names);
        if (need_rewrite_query_with_final)
        {
            if (table_expression_query_info.table_expression_modifiers)
            {
                const auto & table_expression_modifiers = table_expression_query_info.table_expression_modifiers;
                auto sample_size_ratio = table_expression_modifiers->getSampleSizeRatio();
                auto sample_offset_ratio = table_expression_modifiers->getSampleOffsetRatio();

                table_expression_query_info.table_expression_modifiers = TableExpressionModifiers(true /*has_final*/,
                    sample_size_ratio,
                    sample_offset_ratio);
            }
            else
            {
                table_expression_query_info.table_expression_modifiers = TableExpressionModifiers(true /*has_final*/,
                    {} /*sample_size_ratio*/,
                    {} /*sample_offset_ratio*/);
            }
        }

        storage->read(query_plan, column_names, storage_snapshot, table_expression_query_info, query_context, from_stage, max_block_size, max_streams);

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
        auto subquery_context = buildSubqueryContext(planner_context->getQueryContext());
        Planner subquery_planner(table_expression, subquery_options, std::move(subquery_context), planner_context->getGlobalPlannerContext());
        subquery_planner.buildQueryPlanIfNeeded();
        query_plan = std::move(subquery_planner).extractQueryPlan();
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected table, table function, query or union. Actual {}", table_expression->formatASTForErrorMessage());
    }

    auto rename_actions_dag = std::make_shared<ActionsDAG>(query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName());

    for (const auto & [column_name, column_identifier] : table_expression_data.getColumnNameToIdentifier())
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

    JoinClausesAndActions join_clauses_and_actions;
    JoinKind join_kind = join_node.getKind();

    auto join_constant = tryExtractConstantFromJoinNode(join_tree_node);
    if (join_constant)
    {
        /** If there is JOIN with always true constant, we transform it to cross.
          * If there is JOIN with always false constant, we do not process JOIN keys.
          * It is expected by join algorithm to handle such case.
          *
          * Example: SELECT * FROM test_table AS t1 INNER JOIN test_table AS t2 ON 1;
          */
        if (*join_constant)
            join_kind = JoinKind::Cross;
    }
    else if (join_node.isOnJoinExpression())
    {
        join_clauses_and_actions = buildJoinClausesAndActions(left_plan_output_columns,
            right_plan_output_columns,
            join_tree_node,
            planner_context);

        join_clauses_and_actions.left_join_expressions_actions->projectInput();
        auto left_join_expressions_actions_step = std::make_unique<ExpressionStep>(left_plan.getCurrentDataStream(), join_clauses_and_actions.left_join_expressions_actions);
        left_join_expressions_actions_step->setStepDescription("JOIN actions");
        left_plan.addStep(std::move(left_join_expressions_actions_step));

        join_clauses_and_actions.right_join_expressions_actions->projectInput();
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
                auto left_inner_column_identifier = planner_context->getColumnNodeIdentifierOrThrow(left_inner_column_node);
                left_plan_column_name_to_cast_type.emplace(left_inner_column_identifier, join_node_using_column_node_type);
            }

            if (!right_inner_column.getColumnType()->equals(*join_node_using_column_node_type))
            {
                auto right_inner_column_identifier = planner_context->getColumnNodeIdentifierOrThrow(right_inner_column_node);
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
            Field cast_type_constant_value(cast_type_name);

            ColumnWithTypeAndName column;
            column.name = calculateConstantActionNodeName(cast_type_constant_value);
            column.column = DataTypeString().createColumnConst(0, cast_type_constant_value);
            column.type = std::make_shared<DataTypeString>();

            const auto * cast_type_constant_node = &cast_actions_dag->addColumn(std::move(column));

            FunctionCastBase::Diagnostic diagnostic = {output_node->result_name, output_node->result_name};
            FunctionOverloadResolverPtr func_builder_cast
                = CastInternalOverloadResolver<CastType::nonAccurate>::createImpl(std::move(diagnostic));

            ActionsDAG::NodeRawConstPtrs children = {output_node, cast_type_constant_node};
            output_node = &cast_actions_dag->addFunction(func_builder_cast, std::move(children), output_node->result_name);
        }

        cast_actions_dag->projectInput();
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
    bool join_use_nulls = query_context->getSettingsRef().join_use_nulls;
    auto to_nullable_function = FunctionFactory::instance().get("toNullable", query_context);

    auto join_cast_plan_columns_to_nullable = [&](QueryPlan & plan_to_add_cast)
    {
        auto cast_actions_dag = std::make_shared<ActionsDAG>(plan_to_add_cast.getCurrentDataStream().header.getColumnsWithTypeAndName());

        for (auto & output_node : cast_actions_dag->getOutputs())
        {
            if (planner_context->getGlobalPlannerContext()->hasColumnIdentifier(output_node->result_name))
                output_node = &cast_actions_dag->addFunction(to_nullable_function, {output_node}, output_node->result_name);
        }

        cast_actions_dag->projectInput();
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

    auto table_join = std::make_shared<TableJoin>(query_context->getSettings(), query_context->getTemporaryVolume());
    table_join->getTableJoin() = join_node.toASTTableJoin()->as<ASTTableJoin &>();
    table_join->getTableJoin().kind = join_kind;

    if (join_kind == JoinKind::Comma)
        table_join->getTableJoin().kind = JoinKind::Cross;

    table_join->setIsJoinWithConstant(join_constant != std::nullopt);

    if (join_node.isOnJoinExpression())
    {
        const auto & join_clauses = join_clauses_and_actions.join_clauses;
        bool is_asof = table_join->strictness() == JoinStrictness::Asof;

        if (join_clauses.size() > 1)
        {
            if (is_asof)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "ASOF join {} doesn't support multiple ORs for keys in JOIN ON section",
                    join_node.formatASTForErrorMessage());
        }

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

            if (is_asof)
            {
                if (!join_clause.hasASOF())
                    throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                        "JOIN {} no inequality in ASOF JOIN ON section.",
                        join_node.formatASTForErrorMessage());

                if (table_join_clause.key_names_left.size() <= 1)
                    throw Exception(ErrorCodes::SYNTAX_ERROR,
                        "JOIN {} ASOF join needs at least one equi-join column",
                        join_node.formatASTForErrorMessage());
            }

            if (join_clause.hasASOF())
            {
                const auto & asof_conditions = join_clause.getASOFConditions();
                assert(asof_conditions.size() == 1);

                const auto & asof_condition = asof_conditions[0];
                table_join->setAsofInequality(asof_condition.asof_inequality);

                /// Execution layer of JOIN algorithms expects that ASOF keys are last JOIN keys
                std::swap(table_join_clause.key_names_left.at(asof_condition.key_index), table_join_clause.key_names_left.back());
                std::swap(table_join_clause.key_names_right.at(asof_condition.key_index), table_join_clause.key_names_right.back());
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

            auto left_column_identifier = planner_context->getColumnNodeIdentifierOrThrow(using_join_left_join_column_node);
            auto right_column_identifier = planner_context->getColumnNodeIdentifierOrThrow(using_join_right_join_column_node);

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
        if (planner_context->getGlobalPlannerContext()->hasColumnIdentifier(column_from_joined_table.name))
            table_join->addJoinedColumn(column_from_joined_table);
    }

    auto join_algorithm = chooseJoinAlgorithm(table_join, join_node.getRightTableExpression(), right_plan.getCurrentDataStream().header, planner_context);

    auto result_plan = QueryPlan();

    if (join_algorithm->isFilled())
    {
        size_t max_block_size = query_context->getSettingsRef().max_block_size;

        auto filled_join_step = std::make_unique<FilledJoinStep>(
            left_plan.getCurrentDataStream(),
            join_algorithm,
            max_block_size);

        filled_join_step->setStepDescription("Filled JOIN");
        left_plan.addStep(std::move(filled_join_step));

        result_plan = std::move(left_plan);
    }
    else
    {
        size_t max_block_size = query_context->getSettingsRef().max_block_size;
        size_t max_streams = query_context->getSettingsRef().max_threads;

        auto join_step = std::make_unique<JoinStep>(
            left_plan.getCurrentDataStream(),
            right_plan.getCurrentDataStream(),
            std::move(join_algorithm),
            max_block_size,
            max_streams,
            false /*optimize_read_in_order*/);

        join_step->setStepDescription(fmt::format("JOIN {}", JoinPipelineType::FillRightFirst));

        std::vector<QueryPlanPtr> plans;
        plans.emplace_back(std::make_unique<QueryPlan>(std::move(left_plan)));
        plans.emplace_back(std::make_unique<QueryPlan>(std::move(right_plan)));

        result_plan.unitePlans(std::move(join_step), {std::move(plans)});
    }

    auto drop_unused_columns_after_join_actions_dag = std::make_shared<ActionsDAG>(result_plan.getCurrentDataStream().header.getColumnsWithTypeAndName());
    ActionsDAG::NodeRawConstPtrs updated_outputs;
    std::unordered_set<std::string_view> updated_outputs_names;

    for (auto & output : drop_unused_columns_after_join_actions_dag->getOutputs())
    {
        if (updated_outputs_names.contains(output->result_name) || !planner_context->getGlobalPlannerContext()->hasColumnIdentifier(output->result_name))
            continue;

        updated_outputs.push_back(output);
        updated_outputs_names.insert(output->result_name);
    }

    drop_unused_columns_after_join_actions_dag->getOutputs() = std::move(updated_outputs);

    auto drop_unused_columns_after_join_transform_step = std::make_unique<ExpressionStep>(result_plan.getCurrentDataStream(), std::move(drop_unused_columns_after_join_actions_dag));
    drop_unused_columns_after_join_transform_step->setStepDescription("DROP unused columns after JOIN");
    result_plan.addStep(std::move(drop_unused_columns_after_join_transform_step));

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

    array_join_action_dag->projectInput();
    auto array_join_actions = std::make_unique<ExpressionStep>(left_plan.getCurrentDataStream(), array_join_action_dag);
    array_join_actions->setStepDescription("ARRAY JOIN actions");
    left_plan.addStep(std::move(array_join_actions));

    auto array_join_action = std::make_shared<ArrayJoinAction>(array_join_columns, array_join_node.isLeft(), planner_context->getQueryContext());
    auto array_join_step = std::make_unique<ArrayJoinStep>(left_plan.getCurrentDataStream(), std::move(array_join_action));
    array_join_step->setStepDescription("ARRAY JOIN");
    left_plan.addStep(std::move(array_join_step));

    return left_plan;
}

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
            return buildQueryPlanForTableExpression(join_tree_node, select_query_info, select_query_options, planner_context);
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
