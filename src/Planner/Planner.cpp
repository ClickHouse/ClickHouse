#include <Planner/Planner.h>

#include <DataTypes/DataTypeString.h>

#include <Functions/FunctionFactory.h>
#include <Functions/CastOverloadResolver.h>

#include <QueryPipeline/Pipe.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/IntersectOrExceptStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/RollupStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Interpreters/Context.h>

#include <Storages/SelectQueryInfo.h>
#include <Storages/IStorage.h>

#include <Analyzer/Utils.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/InterpolateNode.h>
#include <Analyzer/WindowNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/CollectAggregateFunctionNodes.h>
#include <Analyzer/CollectWindowFunctionNodes.h>

#include <Planner/Utils.h>
#include <Planner/PlannerContext.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Planner/PlannerJoins.h>
#include <Planner/PlannerAggregation.h>
#include <Planner/PlannerSorting.h>
#include <Planner/PlannerWindowFunctions.h>
#include <Planner/ActionsChain.h>
#include <Planner/CollectSets.h>
#include <Planner/CollectTableExpressionData.h>
#include <Planner/PlannerJoinTree.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

/** ClickHouse query planner.
  *
  * TODO: JOIN support ASOF. JOIN support strictness. JOIN support constants. JOIN support ON t1.id = t1.id
  * TODO: JOIN drop unnecessary columns after ON, USING section
  * TODO: Support display names
  * TODO: Support RBAC. Support RBAC for ALIAS columns
  * TODO: Support distributed query processing
  * TODO: Support PREWHERE
  * TODO: Support ORDER BY
  * TODO: Support DISTINCT
  * TODO: Support trivial count optimization
  * TODO: Support projections
  * TODO: Support read in order optimization
  * TODO: UNION storage limits
  * TODO: Support max streams
  * TODO: Support ORDER BY read in order optimization
  * TODO: Support GROUP BY read in order optimization
  * TODO: Support Key Condition
  */

namespace
{

void addBuildSubqueriesForSetsStepIfNeeded(QueryPlan & query_plan, const SelectQueryOptions & select_query_options, const PlannerContextPtr & planner_context)
{
    if (select_query_options.is_subquery)
        return;

    PreparedSets::SubqueriesForSets subqueries_for_sets;
    const auto & subquery_node_to_sets = planner_context->getGlobalPlannerContext()->getSubqueryNodesForSets();

    for (auto [key, subquery_node_for_set] : subquery_node_to_sets)
    {
        auto subquery_context = buildSubqueryContext(planner_context->getQueryContext());
        auto subquery_options = select_query_options.subquery();
        Planner subquery_planner(
            subquery_node_for_set.subquery_node,
            subquery_options,
            std::move(subquery_context),
            planner_context->getGlobalPlannerContext());
        subquery_planner.buildQueryPlanIfNeeded();

        SubqueryForSet subquery_for_set;
        subquery_for_set.set = subquery_node_for_set.set;
        subquery_for_set.source = std::make_unique<QueryPlan>(std::move(subquery_planner).extractQueryPlan());

        subqueries_for_sets.emplace(key, std::move(subquery_for_set));
    }

    addCreatingSetsStep(query_plan, std::move(subqueries_for_sets), planner_context->getQueryContext());
}

}

Planner::Planner(const QueryTreeNodePtr & query_tree_,
    const SelectQueryOptions & select_query_options_,
    ContextPtr context_)
    : query_tree(query_tree_)
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
    : query_tree(query_tree_)
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

    auto query_context = planner_context->getQueryContext();

    if (auto * union_query_tree = query_tree->as<UnionNode>())
    {
        auto union_mode = union_query_tree->getUnionMode();
        if (union_mode == SelectUnionMode::UNION_DEFAULT ||
            union_mode == SelectUnionMode::EXCEPT_DEFAULT ||
            union_mode == SelectUnionMode::INTERSECT_DEFAULT)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "UNION mode must be initialized");

        size_t queries_size = union_query_tree->getQueries().getNodes().size();

        std::vector<std::unique_ptr<QueryPlan>> query_plans;
        query_plans.reserve(queries_size);

        Blocks query_plans_headers;
        query_plans_headers.reserve(queries_size);

        for (auto & query_node : union_query_tree->getQueries().getNodes())
        {
            Planner query_planner(query_node, select_query_options, query_context);
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

        const auto & settings = query_context->getSettingsRef();
        auto max_threads = settings.max_threads;

        bool is_distinct = union_mode == SelectUnionMode::UNION_DISTINCT || union_mode == SelectUnionMode::INTERSECT_DISTINCT ||
            union_mode == SelectUnionMode::EXCEPT_DISTINCT;

        if (union_mode == SelectUnionMode::UNION_ALL || union_mode == SelectUnionMode::UNION_DISTINCT)
        {
            auto union_step = std::make_unique<UnionStep>(std::move(query_plans_streams), max_threads);
            query_plan.unitePlans(std::move(union_step), std::move(query_plans));
        }
        else if (union_mode == SelectUnionMode::INTERSECT_ALL || union_mode == SelectUnionMode::INTERSECT_DISTINCT ||
            union_mode == SelectUnionMode::EXCEPT_ALL || union_mode == SelectUnionMode::EXCEPT_DISTINCT)
        {
            IntersectOrExceptStep::Operator intersect_or_except_operator = IntersectOrExceptStep::Operator::UNKNOWN;

            if (union_mode == SelectUnionMode::INTERSECT_ALL)
                intersect_or_except_operator = IntersectOrExceptStep::Operator::INTERSECT_ALL;
            else if (union_mode == SelectUnionMode::INTERSECT_DISTINCT)
                intersect_or_except_operator = IntersectOrExceptStep::Operator::INTERSECT_DISTINCT;
            else if (union_mode == SelectUnionMode::EXCEPT_ALL)
                intersect_or_except_operator = IntersectOrExceptStep::Operator::EXCEPT_ALL;
            else if (union_mode == SelectUnionMode::EXCEPT_DISTINCT)
                intersect_or_except_operator = IntersectOrExceptStep::Operator::EXCEPT_DISTINCT;

            auto union_step = std::make_unique<IntersectOrExceptStep>(std::move(query_plans_streams), intersect_or_except_operator, max_threads);
            query_plan.unitePlans(std::move(union_step), std::move(query_plans));
        }

        if (is_distinct)
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

        return;
    }

    auto & query_node = query_tree->as<QueryNode &>();

    if (query_node.hasPrewhere())
    {
        if (query_node.hasWhere())
        {
            auto function_node = std::make_shared<FunctionNode>("and");
            auto and_function = FunctionFactory::instance().get("and", query_context);
            function_node->resolveAsFunction(std::move(and_function), std::make_shared<DataTypeUInt8>());
            function_node->getArguments().getNodes() = {query_node.getPrewhere(), query_node.getWhere()};
            query_node.getWhere() = std::move(function_node);
            query_node.getPrewhere() = {};
        }
        else
        {
            query_node.getWhere() = query_node.getPrewhere();
        }
    }

    SelectQueryInfo select_query_info;
    select_query_info.original_query = queryNodeToSelectQuery(query_tree);
    select_query_info.query = select_query_info.original_query;
    select_query_info.planner_context = planner_context;

    StorageLimitsList storage_limits;
    storage_limits.push_back(buildStorageLimits(*query_context, select_query_options));
    select_query_info.storage_limits = std::make_shared<StorageLimitsList>(storage_limits);

    collectTableExpressionData(query_tree, *planner_context);
    collectSets(query_tree, *planner_context);

    query_plan = buildQueryPlanForJoinTreeNode(query_node.getJoinTree(), select_query_info, select_query_options, planner_context);

    ActionsChain actions_chain;
    std::optional<size_t> where_action_step_index;
    std::string where_filter_action_node_name;

    if (query_node.hasWhere())
    {
        const auto * chain_available_output_columns = actions_chain.getLastStepAvailableOutputColumnsOrNull();
        const auto & where_input = chain_available_output_columns ? *chain_available_output_columns : query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName();

        auto where_actions = buildActionsDAGFromExpressionNode(query_node.getWhere(), where_input, planner_context);
        where_filter_action_node_name = where_actions->getOutputs().at(0)->result_name;
        actions_chain.addStep(std::make_unique<ActionsChainStep>(std::move(where_actions)));
        where_action_step_index = actions_chain.getLastStepIndex();
    }

    auto aggregate_function_nodes = collectAggregateFunctionNodes(query_tree);
    AggregateDescriptions aggregates_descriptions = extractAggregateDescriptions(aggregate_function_nodes, *planner_context);
    ColumnsWithTypeAndName aggregates_columns;
    aggregates_columns.reserve(aggregates_descriptions.size());
    for (auto & aggregate_description : aggregates_descriptions)
        aggregates_columns.emplace_back(nullptr, aggregate_description.function->getReturnType(), aggregate_description.column_name);

    Names aggregation_keys;
    std::optional<size_t> aggregate_step_index;

    const auto * chain_available_output_columns = actions_chain.getLastStepAvailableOutputColumnsOrNull();
    const auto & group_by_input = chain_available_output_columns ? *chain_available_output_columns
                                                                 : query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName();

    /// Only aggregation keys, and aggregates are available for next steps after GROUP BY step
    ActionsDAGPtr group_by_actions_dag = std::make_shared<ActionsDAG>(group_by_input);
    group_by_actions_dag->getOutputs().clear();
    std::unordered_set<std::string_view> group_by_actions_dag_output_nodes_names;

    PlannerActionsVisitor actions_visitor(planner_context);
    GroupingSetsParamsList grouping_sets_parameters_list;
    bool group_by_with_constant_keys = false;
    bool disable_grouping_sets = false;

    if (query_node.hasGroupBy())
    {
        if (query_node.isGroupByWithGroupingSets())
        {
            for (auto & grouping_set_keys_list_node : query_node.getGroupBy().getNodes())
            {
                auto & grouping_set_keys_list_node_typed = grouping_set_keys_list_node->as<ListNode &>();
                grouping_sets_parameters_list.emplace_back();
                auto & grouping_sets_parameters = grouping_sets_parameters_list.back();

                for (auto & grouping_set_key_node : grouping_set_keys_list_node_typed.getNodes())
                {
                    group_by_with_constant_keys |= grouping_set_key_node->hasConstantValue();

                    auto expression_dag_nodes = actions_visitor.visit(group_by_actions_dag, grouping_set_key_node);
                    aggregation_keys.reserve(expression_dag_nodes.size());

                    for (auto & expression_dag_node : expression_dag_nodes)
                    {
                        grouping_sets_parameters.used_keys.push_back(expression_dag_node->result_name);
                        if (group_by_actions_dag_output_nodes_names.contains(expression_dag_node->result_name))
                            continue;

                        aggregation_keys.push_back(expression_dag_node->result_name);
                        group_by_actions_dag->getOutputs().push_back(expression_dag_node);
                        group_by_actions_dag_output_nodes_names.insert(expression_dag_node->result_name);
                    }
                }
            }

            for (auto & grouping_sets_parameter : grouping_sets_parameters_list)
            {
                NameSet grouping_sets_used_keys;
                Names grouping_sets_keys;

                for (auto & key : grouping_sets_parameter.used_keys)
                {
                    auto [_, inserted] = grouping_sets_used_keys.insert(key);
                    if (inserted)
                        grouping_sets_keys.push_back(key);
                }

                for (auto & key : aggregation_keys)
                {
                    if (grouping_sets_used_keys.contains(key))
                        continue;

                    grouping_sets_parameter.missing_keys.push_back(key);
                }

                grouping_sets_parameter.used_keys = std::move(grouping_sets_keys);
            }

            /// It is expected by execution layer that if there are only 1 grouping sets it will be removed
            if (grouping_sets_parameters_list.size() == 1)
            {
                disable_grouping_sets = true;
                grouping_sets_parameters_list.clear();
            }
        }
        else
        {
            for (auto & group_by_key_node : query_node.getGroupBy().getNodes())
                group_by_with_constant_keys |= group_by_key_node->hasConstantValue();

            auto expression_dag_nodes = actions_visitor.visit(group_by_actions_dag, query_node.getGroupByNode());
            aggregation_keys.reserve(expression_dag_nodes.size());

            for (auto & expression_dag_node : expression_dag_nodes)
            {
                if (group_by_actions_dag_output_nodes_names.contains(expression_dag_node->result_name))
                    continue;

                aggregation_keys.push_back(expression_dag_node->result_name);
                group_by_actions_dag->getOutputs().push_back(expression_dag_node);
                group_by_actions_dag_output_nodes_names.insert(expression_dag_node->result_name);
            }
        }
    }

    if (!aggregate_function_nodes.empty())
    {
        for (auto & aggregate_function_node : aggregate_function_nodes)
        {
            auto & aggregate_function_node_typed = aggregate_function_node->as<FunctionNode &>();
            for (const auto & aggregate_function_node_argument : aggregate_function_node_typed.getArguments().getNodes())
            {
                auto expression_dag_nodes = actions_visitor.visit(group_by_actions_dag, aggregate_function_node_argument);
                for (auto & expression_dag_node : expression_dag_nodes)
                {
                    if (group_by_actions_dag_output_nodes_names.contains(expression_dag_node->result_name))
                        continue;

                    group_by_actions_dag->getOutputs().push_back(expression_dag_node);
                    group_by_actions_dag_output_nodes_names.insert(expression_dag_node->result_name);
                }
            }
        }
    }

    if (!group_by_actions_dag->getOutputs().empty())
    {
        /** For non ordinary GROUP BY we add virtual __grouping_set column
          * With set number, which is used as an additional key at the stage of merging aggregating data.
          */
        if (query_node.isGroupByWithRollup() || query_node.isGroupByWithCube() || (query_node.isGroupByWithGroupingSets() && !disable_grouping_sets))
            aggregates_columns.emplace_back(nullptr, std::make_shared<DataTypeUInt64>(), "__grouping_set");

        resolveGroupingFunctions(query_tree, aggregation_keys, grouping_sets_parameters_list, *planner_context);
        auto aggregate_step = std::make_unique<ActionsChainStep>(std::move(group_by_actions_dag), ActionsChainStep::AvailableOutputColumnsStrategy::OUTPUT_NODES, aggregates_columns);
        actions_chain.addStep(std::move(aggregate_step));
        aggregate_step_index = actions_chain.getLastStepIndex();
    }

    std::optional<size_t> having_action_step_index;
    std::string having_filter_action_node_name;

    if (query_node.hasHaving())
    {
        chain_available_output_columns = actions_chain.getLastStepAvailableOutputColumnsOrNull();
        const auto & having_input = chain_available_output_columns ? *chain_available_output_columns : query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName();

        auto having_actions = buildActionsDAGFromExpressionNode(query_node.getHaving(), having_input, planner_context);
        having_filter_action_node_name = having_actions->getOutputs().at(0)->result_name;
        actions_chain.addStep(std::make_unique<ActionsChainStep>(std::move(having_actions)));
        having_action_step_index = actions_chain.getLastStepIndex();
    }

    auto window_function_nodes = collectWindowFunctionNodes(query_tree);
    auto window_descriptions = extractWindowDescriptions(window_function_nodes, *planner_context);
    std::optional<size_t> before_window_step_index;

    if (!window_function_nodes.empty())
    {
        chain_available_output_columns = actions_chain.getLastStepAvailableOutputColumnsOrNull();
        const auto & window_input = chain_available_output_columns ? *chain_available_output_columns
                                                                 : query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName();

        ActionsDAGPtr before_window_actions_dag = std::make_shared<ActionsDAG>(window_input);
        before_window_actions_dag->getOutputs().clear();

        std::unordered_set<std::string_view> before_window_actions_dag_output_nodes_names;

        for (auto & window_function_node : window_function_nodes)
        {
            auto & window_function_node_typed = window_function_node->as<FunctionNode &>();
            auto & window_node = window_function_node_typed.getWindowNode()->as<WindowNode &>();

            auto expression_dag_nodes = actions_visitor.visit(before_window_actions_dag, window_function_node_typed.getArgumentsNode());
            aggregation_keys.reserve(expression_dag_nodes.size());

            for (auto & expression_dag_node : expression_dag_nodes)
            {
                if (before_window_actions_dag_output_nodes_names.contains(expression_dag_node->result_name))
                    continue;

                before_window_actions_dag->getOutputs().push_back(expression_dag_node);
                before_window_actions_dag_output_nodes_names.insert(expression_dag_node->result_name);
            }

            expression_dag_nodes = actions_visitor.visit(before_window_actions_dag, window_node.getPartitionByNode());
            aggregation_keys.reserve(expression_dag_nodes.size());

            for (auto & expression_dag_node : expression_dag_nodes)
            {
                if (before_window_actions_dag_output_nodes_names.contains(expression_dag_node->result_name))
                    continue;

                before_window_actions_dag->getOutputs().push_back(expression_dag_node);
                before_window_actions_dag_output_nodes_names.insert(expression_dag_node->result_name);
            }

            /** We add only sort column sort expression in before WINDOW actions DAG.
              * WITH fill expressions must be constant nodes.
              */
            auto & order_by_node_list = window_node.getOrderBy();
            for (auto & sort_node : order_by_node_list.getNodes())
            {
                auto & sort_node_typed = sort_node->as<SortNode &>();
                expression_dag_nodes = actions_visitor.visit(before_window_actions_dag, sort_node_typed.getExpression());

                for (auto & expression_dag_node : expression_dag_nodes)
                {
                    if (before_window_actions_dag_output_nodes_names.contains(expression_dag_node->result_name))
                        continue;

                    before_window_actions_dag->getOutputs().push_back(expression_dag_node);
                    before_window_actions_dag_output_nodes_names.insert(expression_dag_node->result_name);
                }
            }
        }

        ColumnsWithTypeAndName window_functions_additional_columns;

        for (auto & window_description : window_descriptions)
            for (auto & window_function : window_description.window_functions)
                window_functions_additional_columns.emplace_back(nullptr, window_function.aggregate_function->getReturnType(), window_function.column_name);

        auto before_window_step = std::make_unique<ActionsChainStep>(std::move(before_window_actions_dag),
            ActionsChainStep::AvailableOutputColumnsStrategy::ALL_NODES,
            window_functions_additional_columns);
        actions_chain.addStep(std::move(before_window_step));
        before_window_step_index = actions_chain.getLastStepIndex();
    }

    chain_available_output_columns = actions_chain.getLastStepAvailableOutputColumnsOrNull();
    const auto & projection_input = chain_available_output_columns ? *chain_available_output_columns : query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName();
    auto projection_actions = buildActionsDAGFromExpressionNode(query_node.getProjectionNode(), projection_input, planner_context);

    auto projection_columns = query_node.getProjectionColumns();
    size_t projection_columns_size = projection_columns.size();

    Names projection_action_names;
    NamesWithAliases projection_action_names_with_display_aliases;
    projection_action_names_with_display_aliases.reserve(projection_columns_size);

    auto & projection_actions_outputs = projection_actions->getOutputs();
    size_t projection_outputs_size = projection_actions_outputs.size();

    if (projection_columns_size != projection_outputs_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "QueryTree projection nodes size mismatch. Expected {}. Actual {}",
            projection_outputs_size,
            projection_columns_size);

    for (size_t i = 0; i < projection_outputs_size; ++i)
    {
        auto & projection_column = projection_columns[i];
        const auto * projection_node = projection_actions_outputs[i];
        const auto & projection_node_name = projection_node->result_name;

        projection_action_names.push_back(projection_node_name);
        projection_action_names_with_display_aliases.push_back({projection_node_name, projection_column.name});
    }

    auto projection_actions_step = std::make_unique<ActionsChainStep>(std::move(projection_actions));
    actions_chain.addStep(std::move(projection_actions_step));
    size_t projection_step_index = actions_chain.getLastStepIndex();

    std::optional<size_t> before_order_by_step_index;
    if (query_node.hasOrderBy())
    {
        chain_available_output_columns = actions_chain.getLastStepAvailableOutputColumnsOrNull();
        const auto & order_by_input = chain_available_output_columns ? *chain_available_output_columns : query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName();

        ActionsDAGPtr before_order_by_actions_dag = std::make_shared<ActionsDAG>(order_by_input);
        auto & before_order_by_actions_dag_outputs = before_order_by_actions_dag->getOutputs();
        before_order_by_actions_dag_outputs.clear();

        std::unordered_set<std::string_view> before_order_by_actions_dag_outputs_node_names;

        /** We add only sort node sort expression in before ORDER BY actions DAG.
          * WITH fill expressions must be constant nodes.
          */
        auto & order_by_node_list = query_node.getOrderBy();
        for (auto & sort_node : order_by_node_list.getNodes())
        {
            auto & sort_node_typed = sort_node->as<SortNode &>();
            auto expression_dag_nodes = actions_visitor.visit(before_order_by_actions_dag, sort_node_typed.getExpression());

            for (auto & action_dag_node : expression_dag_nodes)
            {
                if (before_order_by_actions_dag_outputs_node_names.contains(action_dag_node->result_name))
                    continue;

                before_order_by_actions_dag_outputs.push_back(action_dag_node);
                before_order_by_actions_dag_outputs_node_names.insert(action_dag_node->result_name);
            }
        }

        auto actions_step_before_order_by = std::make_unique<ActionsChainStep>(std::move(before_order_by_actions_dag));
        actions_chain.addStep(std::move(actions_step_before_order_by));
        before_order_by_step_index = actions_chain.getLastStepIndex();
    }

    std::optional<size_t> before_limit_by_step_index;
    Names limit_by_columns_names;

    if (query_node.hasLimitBy())
    {
        chain_available_output_columns = actions_chain.getLastStepAvailableOutputColumnsOrNull();
        const auto & limit_by_input = chain_available_output_columns ? *chain_available_output_columns : query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName();
        auto limit_by_actions = buildActionsDAGFromExpressionNode(query_node.getLimitByNode(), limit_by_input, planner_context);

        limit_by_columns_names.reserve(limit_by_actions->getOutputs().size());
        for (auto & output_node : limit_by_actions->getOutputs())
            limit_by_columns_names.push_back(output_node->result_name);

        auto actions_step_before_limit_by = std::make_unique<ActionsChainStep>(std::move(limit_by_actions));
        actions_chain.addStep(std::move(actions_step_before_limit_by));
        before_limit_by_step_index = actions_chain.getLastStepIndex();
    }

    chain_available_output_columns = actions_chain.getLastStepAvailableOutputColumnsOrNull();
    const auto & project_names_input = chain_available_output_columns ? *chain_available_output_columns : query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName();
    auto project_names_actions = std::make_shared<ActionsDAG>(project_names_input);

    project_names_actions->project(projection_action_names_with_display_aliases);
    actions_chain.addStep(std::make_unique<ActionsChainStep>(std::move(project_names_actions)));
    size_t project_names_action_step_index = actions_chain.getLastStepIndex();

    // std::cout << "Chain dump before finalize" << std::endl;
    // std::cout << actions_chain.dump() << std::endl;

    actions_chain.finalize();

    // std::cout << "Chain dump after finalize" << std::endl;
    // std::cout << actions_chain.dump() << std::endl;

    if (where_action_step_index)
    {
        auto & where_actions_chain_node = actions_chain.at(*where_action_step_index);
        bool remove_filter = !where_actions_chain_node->getChildRequiredOutputColumnsNames().contains(where_filter_action_node_name);
        auto where_step = std::make_unique<FilterStep>(query_plan.getCurrentDataStream(),
            where_actions_chain_node->getActions(),
            where_filter_action_node_name,
            remove_filter);
        where_step->setStepDescription("WHERE");
        query_plan.addStep(std::move(where_step));
    }

    bool having_executed = false;

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

        const auto stats_collecting_params = Aggregator::Params::StatsCollectingParams(
            select_query_info.query,
            settings.collect_hash_table_stats_during_aggregation,
            settings.max_entries_for_hash_table_stats,
            settings.max_size_to_preallocate_for_aggregation);

        bool aggregate_overflow_row =
            query_node.isGroupByWithTotals() &&
            settings.max_rows_to_group_by &&
            settings.group_by_overflow_mode == OverflowMode::ANY &&
            settings.totals_mode != TotalsMode::AFTER_HAVING_EXCLUSIVE;

        Aggregator::Params aggregator_params = Aggregator::Params(
            aggregation_keys,
            aggregates_descriptions,
            aggregate_overflow_row,
            settings.max_rows_to_group_by,
            settings.group_by_overflow_mode,
            settings.group_by_two_level_threshold,
            settings.group_by_two_level_threshold_bytes,
            settings.max_bytes_before_external_group_by,
            settings.empty_result_for_aggregation_by_empty_set
                || (settings.empty_result_for_aggregation_by_constant_keys_on_empty_set && aggregation_keys.empty()
                    && group_by_with_constant_keys),
            planner_context->getQueryContext()->getTemporaryVolume(),
            settings.max_threads,
            settings.min_free_disk_space_for_temporary_data,
            settings.compile_aggregate_expressions,
            settings.min_count_to_compile_aggregate_expression,
            settings.max_block_size,
            /* only_merge */ false,
            stats_collecting_params
        );

        SortDescription group_by_sort_description;

        auto merge_threads = settings.max_threads;
        auto temporary_data_merge_threads = settings.aggregation_memory_efficient_merge_threads
            ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads)
            : static_cast<size_t>(settings.max_threads);

        bool storage_has_evenly_distributed_read = false;
        const auto & table_expression_node_to_data = planner_context->getTableExpressionNodeToData();

        if (table_expression_node_to_data.size() == 1)
        {
            auto it = table_expression_node_to_data.begin();
            const auto & table_expression_node = it->first;
            if (const auto * table_node = table_expression_node->as<TableNode>())
                storage_has_evenly_distributed_read = table_node->getStorage()->hasEvenlyDistributedRead();
            else if (const auto * table_function_node = table_expression_node->as<TableFunctionNode>())
                storage_has_evenly_distributed_read = table_function_node->getStorageOrThrow()->hasEvenlyDistributedRead();
        }

        const bool should_produce_results_in_order_of_bucket_number
            = select_query_options.to_stage == QueryProcessingStage::WithMergeableState && settings.distributed_aggregation_memory_efficient;

        InputOrderInfoPtr input_order_info;
        bool aggregate_final =
            select_query_options.to_stage > QueryProcessingStage::WithMergeableState &&
            !query_node.isGroupByWithTotals() && !query_node.isGroupByWithRollup() && !query_node.isGroupByWithCube();

        auto aggregating_step = std::make_unique<AggregatingStep>(
            query_plan.getCurrentDataStream(),
            aggregator_params,
            std::move(grouping_sets_parameters_list),
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

        if (query_node.isGroupByWithRollup())
        {
            auto rollup_step = std::make_unique<RollupStep>(query_plan.getCurrentDataStream(), std::move(aggregator_params), true /*final*/, settings.group_by_use_nulls);
            query_plan.addStep(std::move(rollup_step));
        }
        else if (query_node.isGroupByWithCube())
        {
            auto cube_step = std::make_unique<CubeStep>(query_plan.getCurrentDataStream(), std::move(aggregator_params), true /*final*/, settings.group_by_use_nulls);
            query_plan.addStep(std::move(cube_step));
        }

        if (query_node.isGroupByWithTotals())
        {
            bool remove_having_filter = false;
            std::shared_ptr<ActionsDAG> having_actions;

            if (having_action_step_index)
            {
                auto & having_actions_chain_node = actions_chain.at(*having_action_step_index);
                remove_having_filter = !having_actions_chain_node->getChildRequiredOutputColumnsNames().contains(having_filter_action_node_name);
                having_actions = having_actions_chain_node->getActions();
                having_executed = true;
            }

            bool final = !query_node.isGroupByWithRollup() && !query_node.isGroupByWithCube();
            auto totals_having_step = std::make_unique<TotalsHavingStep>(
                query_plan.getCurrentDataStream(),
                aggregates_descriptions,
                aggregate_overflow_row,
                having_actions,
                having_filter_action_node_name,
                remove_having_filter,
                settings.totals_mode,
                settings.totals_auto_threshold,
                final);

            query_plan.addStep(std::move(totals_having_step));
        }
    }

    if (!having_executed && having_action_step_index)
    {
        auto & having_actions_chain_node = actions_chain.at(*having_action_step_index);
        bool remove_filter = !having_actions_chain_node->getChildRequiredOutputColumnsNames().contains(having_filter_action_node_name);
        auto having_step = std::make_unique<FilterStep>(query_plan.getCurrentDataStream(),
            having_actions_chain_node->getActions(),
            having_filter_action_node_name,
            remove_filter);
        having_step->setStepDescription("HAVING");
        query_plan.addStep(std::move(having_step));
    }

    if (before_window_step_index)
    {
        auto & before_window_actions_chain_node = actions_chain.at(*before_window_step_index);
        auto expression_step_before_window = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(),
            before_window_actions_chain_node->getActions());
        expression_step_before_window->setStepDescription("Before WINDOW");
        query_plan.addStep(std::move(expression_step_before_window));

        sortWindowDescriptions(window_descriptions);
        size_t window_descriptions_size = window_descriptions.size();

        const auto & settings = query_context->getSettingsRef();
        for (size_t i = 0; i < window_descriptions_size; ++i)
        {
            const auto & window_description = window_descriptions[i];

            /** We don't need to sort again if the input from previous window already
              * has suitable sorting. Also don't create sort steps when there are no
              * columns to sort by, because the sort nodes are confused by this. It
              * happens in case of `over ()`.
              */
            if (!window_description.full_sort_description.empty() &&
                (i == 0 || !sortDescriptionIsPrefix(window_description.full_sort_description, window_descriptions[i - 1].full_sort_description)))
            {
                auto sorting_step = std::make_unique<SortingStep>(
                    query_plan.getCurrentDataStream(),
                    window_description.full_sort_description,
                    settings.max_block_size,
                    0 /* LIMIT */,
                    SizeLimits(settings.max_rows_to_sort, settings.max_bytes_to_sort, settings.sort_overflow_mode),
                    settings.max_bytes_before_remerge_sort,
                    settings.remerge_sort_lowered_memory_bytes_ratio,
                    settings.max_bytes_before_external_sort,
                    query_context->getTemporaryVolume(),
                    settings.min_free_disk_space_for_temporary_data,
                    settings.optimize_sorting_by_input_stream_properties);

                sorting_step->setStepDescription("Sorting for window '" + window_description.window_name + "'");
                query_plan.addStep(std::move(sorting_step));
            }

            auto window_step = std::make_unique<WindowStep>(query_plan.getCurrentDataStream(), window_description, window_description.window_functions);
            window_step->setStepDescription("Window step for window '" + window_description.window_name + "'");
            query_plan.addStep(std::move(window_step));
        }
    }

    auto & projection_actions_chain_node = actions_chain.at(projection_step_index);
    auto expression_step_projection = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(),
        projection_actions_chain_node->getActions());
    expression_step_projection->setStepDescription("Projection");
    query_plan.addStep(std::move(expression_step_projection));

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
            projection_action_names,
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
        auto & before_order_by_actions_chain_node = actions_chain.at(*before_order_by_step_index);
        auto expression_step_before_order_by = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(),
            before_order_by_actions_chain_node->getActions());
        expression_step_before_order_by->setStepDescription("Before ORDER BY");
        query_plan.addStep(std::move(expression_step_before_order_by));
    }

    QueryPlanStepPtr filling_step;
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
            settings.min_free_disk_space_for_temporary_data,
            settings.optimize_sorting_by_input_stream_properties);

        sorting_step->setStepDescription("Sorting for ORDER BY");
        query_plan.addStep(std::move(sorting_step));

        NameSet column_names_with_fill;
        SortDescription fill_description;
        for (auto & description : sort_description)
        {
            if (description.with_fill)
            {
                fill_description.push_back(description);
                column_names_with_fill.insert(description.column_name);
            }
        }

        if (!fill_description.empty())
        {
            InterpolateDescriptionPtr interpolate_description;

            if (query_node.hasInterpolate())
            {
                auto interpolate_actions_dag = std::make_shared<ActionsDAG>();

                auto & interpolate_list_node = query_node.getInterpolate()->as<ListNode &>();
                auto & interpolate_list_nodes = interpolate_list_node.getNodes();

                if (interpolate_list_nodes.empty())
                {
                    auto query_plan_columns = query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName();
                    for (auto & query_plan_column : query_plan_columns)
                    {
                        if (column_names_with_fill.contains(query_plan_column.name))
                            continue;

                        const auto * input_action_node = &interpolate_actions_dag->addInput(query_plan_column);
                        interpolate_actions_dag->getOutputs().push_back(input_action_node);
                    }
                }
                else
                {
                    for (auto & interpolate_node : interpolate_list_nodes)
                    {
                        auto & interpolate_node_typed = interpolate_node->as<InterpolateNode &>();
                        auto expression_to_interpolate_expression_nodes = actions_visitor.visit(interpolate_actions_dag, interpolate_node_typed.getExpression());
                        auto interpolate_expression_nodes = actions_visitor.visit(interpolate_actions_dag, interpolate_node_typed.getInterpolateExpression());

                        if (expression_to_interpolate_expression_nodes.size() != 1)
                            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expression to interpolate expected to have single action node");

                        if (interpolate_expression_nodes.size() != 1)
                            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Interpolate expression expected to have single action node");

                        const auto * expression_to_interpolate = expression_to_interpolate_expression_nodes[0];
                        const auto & expression_to_interpolate_name = expression_to_interpolate->result_name;

                        const auto * interpolate_expression = interpolate_expression_nodes[0];
                        if (!interpolate_expression->result_type->equals(*expression_to_interpolate->result_type))
                        {
                            auto cast_type_name = expression_to_interpolate->result_type->getName();
                            Field cast_type_constant_value(cast_type_name);

                            ColumnWithTypeAndName column;
                            column.name = calculateConstantActionNodeName(cast_type_name);
                            column.column = DataTypeString().createColumnConst(0, cast_type_constant_value);
                            column.type = std::make_shared<DataTypeString>();

                            const auto * cast_type_constant_node = &interpolate_actions_dag->addColumn(std::move(column));

                            FunctionCastBase::Diagnostic diagnostic = {interpolate_expression->result_name, interpolate_expression->result_name};
                            FunctionOverloadResolverPtr func_builder_cast
                                = CastInternalOverloadResolver<CastType::nonAccurate>::createImpl(std::move(diagnostic));

                            ActionsDAG::NodeRawConstPtrs children = {interpolate_expression, cast_type_constant_node};
                            interpolate_expression = &interpolate_actions_dag->addFunction(func_builder_cast, std::move(children), interpolate_expression->result_name);
                        }

                        const auto * alias_node = &interpolate_actions_dag->addAlias(*interpolate_expression, expression_to_interpolate_name);
                        interpolate_actions_dag->getOutputs().push_back(alias_node);
                    }

                    interpolate_actions_dag->removeUnusedActions();
                }

                Aliases empty_aliases;
                interpolate_description = std::make_shared<InterpolateDescription>(std::move(interpolate_actions_dag), empty_aliases);
            }

            filling_step = std::make_unique<FillingStep>(query_plan.getCurrentDataStream(), std::move(fill_description), interpolate_description);
        }
    }

    if (before_limit_by_step_index)
    {
        auto & before_limit_by_actions_chain_node = actions_chain.at(*before_limit_by_step_index);
        auto expression_step_before_limit_by = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(),
            before_limit_by_actions_chain_node->getActions());
        expression_step_before_limit_by->setStepDescription("Before LIMIT BY");
        query_plan.addStep(std::move(expression_step_before_limit_by));
    }

    if (query_node.hasLimitByLimit() && query_node.hasLimitBy())
    {
        /// Constness of LIMIT BY limit is validated during query analysis stage
        UInt64 limit_by_limit = query_node.getLimitByLimit()->getConstantValue().getValue().safeGet<UInt64>();
        UInt64 limit_by_offset = 0;

        if (query_node.hasLimitByOffset())
        {
            /// Constness of LIMIT BY offset is validated during query analysis stage
            limit_by_offset = query_node.getLimitByOffset()->getConstantValue().getValue().safeGet<UInt64>();
        }

        auto limit_by_step = std::make_unique<LimitByStep>(query_plan.getCurrentDataStream(), limit_by_limit, limit_by_offset, limit_by_columns_names);
        query_plan.addStep(std::move(limit_by_step));
    }

    if (filling_step)
        query_plan.addStep(std::move(filling_step));

    if (query_context->getSettingsRef().extremes)
    {
        auto extremes_step = std::make_unique<ExtremesStep>(query_plan.getCurrentDataStream());
        query_plan.addStep(std::move(extremes_step));
    }

    UInt64 limit_offset = 0;
    if (query_node.hasOffset())
    {
        /// Constness of offset is validated during query analysis stage
        limit_offset = query_node.getOffset()->getConstantValue().getValue().safeGet<UInt64>();
    }

    if (query_node.hasLimit())
    {
        const Settings & settings = planner_context->getQueryContext()->getSettingsRef();
        bool always_read_till_end = settings.exact_rows_before_limit;
        bool limit_with_ties = query_node.isLimitWithTies();

        /// Constness of limit is validated during query analysis stage
        UInt64 limit_length = query_node.getLimit()->getConstantValue().getValue().safeGet<UInt64>();

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

    auto projection_step = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), actions_chain[project_names_action_step_index]->getActions());
    projection_step->setStepDescription("Project names");
    query_plan.addStep(std::move(projection_step));

    addBuildSubqueriesForSetsStepIfNeeded(query_plan, select_query_options, planner_context);

    /// Extend lifetime of context, table locks, storages
    query_plan.addInterpreterContext(planner_context->getQueryContext());

    for (const auto & [table_expression, _] : planner_context->getTableExpressionNodeToData())
    {
        if (auto * table_node = table_expression->as<TableNode>())
        {
            query_plan.addStorageHolder(table_node->getStorage());
            query_plan.addTableLock(table_node->getStorageLock());
        }
        else if (auto * table_function_node = table_expression->as<TableFunctionNode>())
        {
            query_plan.addStorageHolder(table_function_node->getStorage());
        }
    }
}

}
