#include <Planner/PlannerExpressionAnalysis.h>

#include <DataTypes/DataTypesNumber.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/WindowNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/InterpolateNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/AggregationUtils.h>
#include <Analyzer/WindowFunctionsUtils.h>

#include <Planner/ActionsChain.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Planner/PlannerAggregation.h>
#include <Planner/PlannerWindowFunctions.h>
#include <Planner/Utils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/** Construct filter analysis result for filter expression node
  * Actions before filter are added into into actions chain.
  * It is client responsibility to update filter analysis result if filter column must be removed after chain is finalized.
  */
FilterAnalysisResult analyzeFilter(const QueryTreeNodePtr & filter_expression_node,
    const ColumnsWithTypeAndName & join_tree_input_columns,
    const PlannerContextPtr & planner_context,
    ActionsChain & actions_chain)
{
    const auto * chain_available_output_columns = actions_chain.getLastStepAvailableOutputColumnsOrNull();
    const auto & filter_input = chain_available_output_columns ? *chain_available_output_columns : join_tree_input_columns;

    FilterAnalysisResult result;

    result.filter_actions = buildActionsDAGFromExpressionNode(filter_expression_node, filter_input, planner_context);
    result.filter_column_name = result.filter_actions->getOutputs().at(0)->result_name;
    actions_chain.addStep(std::make_unique<ActionsChainStep>(result.filter_actions));

    return result;
}

/** Construct aggregation analysis result if query tree has GROUP BY or aggregates.
  * Actions before aggregation are added into actions chain, if result is not null optional.
  */
std::optional<AggregationAnalysisResult> analyzeAggregation(QueryTreeNodePtr & query_tree,
    const ColumnsWithTypeAndName & join_tree_input_columns,
    const PlannerContextPtr & planner_context,
    ActionsChain & actions_chain)
{
    auto & query_node = query_tree->as<QueryNode &>();

    auto aggregate_function_nodes = collectAggregateFunctionNodes(query_tree);
    auto aggregates_descriptions = extractAggregateDescriptions(aggregate_function_nodes, *planner_context);

    ColumnsWithTypeAndName aggregates_columns;
    aggregates_columns.reserve(aggregates_descriptions.size());
    for (auto & aggregate_description : aggregates_descriptions)
        aggregates_columns.emplace_back(nullptr, aggregate_description.function->getReturnType(), aggregate_description.column_name);

    Names aggregation_keys;

    const auto * chain_available_output_columns = actions_chain.getLastStepAvailableOutputColumnsOrNull();
    const auto & group_by_input = chain_available_output_columns ? *chain_available_output_columns : join_tree_input_columns;

    ActionsDAGPtr before_aggregation_actions = std::make_shared<ActionsDAG>(group_by_input);
    before_aggregation_actions->getOutputs().clear();

    std::unordered_set<std::string_view> before_aggregation_actions_output_node_names;

    GroupingSetsParamsList grouping_sets_parameters_list;
    bool group_by_with_constant_keys = false;
    bool disable_grouping_sets = false;

    PlannerActionsVisitor actions_visitor(planner_context);

    /// Add expressions from GROUP BY

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

                    auto expression_dag_nodes = actions_visitor.visit(before_aggregation_actions, grouping_set_key_node);
                    aggregation_keys.reserve(expression_dag_nodes.size());

                    for (auto & expression_dag_node : expression_dag_nodes)
                    {
                        grouping_sets_parameters.used_keys.push_back(expression_dag_node->result_name);
                        if (before_aggregation_actions_output_node_names.contains(expression_dag_node->result_name))
                            continue;

                        aggregation_keys.push_back(expression_dag_node->result_name);
                        before_aggregation_actions->getOutputs().push_back(expression_dag_node);
                        before_aggregation_actions_output_node_names.insert(expression_dag_node->result_name);
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

            auto expression_dag_nodes = actions_visitor.visit(before_aggregation_actions, query_node.getGroupByNode());
            aggregation_keys.reserve(expression_dag_nodes.size());

            for (auto & expression_dag_node : expression_dag_nodes)
            {
                if (before_aggregation_actions_output_node_names.contains(expression_dag_node->result_name))
                    continue;

                aggregation_keys.push_back(expression_dag_node->result_name);
                before_aggregation_actions->getOutputs().push_back(expression_dag_node);
                before_aggregation_actions_output_node_names.insert(expression_dag_node->result_name);
            }
        }
    }

    /// Add expressions from aggregate functions arguments

    for (auto & aggregate_function_node : aggregate_function_nodes)
    {
        auto & aggregate_function_node_typed = aggregate_function_node->as<FunctionNode &>();
        for (const auto & aggregate_function_node_argument : aggregate_function_node_typed.getArguments().getNodes())
        {
            auto expression_dag_nodes = actions_visitor.visit(before_aggregation_actions, aggregate_function_node_argument);
            for (auto & expression_dag_node : expression_dag_nodes)
            {
                if (before_aggregation_actions_output_node_names.contains(expression_dag_node->result_name))
                    continue;

                before_aggregation_actions->getOutputs().push_back(expression_dag_node);
                before_aggregation_actions_output_node_names.insert(expression_dag_node->result_name);
            }
        }
    }

    if (aggregation_keys.empty() && aggregates_descriptions.empty())
        return {};

    /** For non ordinary GROUP BY we add virtual __grouping_set column
      * With set number, which is used as an additional key at the stage of merging aggregating data.
      */
    if (query_node.isGroupByWithRollup() || query_node.isGroupByWithCube() || (query_node.isGroupByWithGroupingSets() && !disable_grouping_sets))
        aggregates_columns.emplace_back(nullptr, std::make_shared<DataTypeUInt64>(), "__grouping_set");

    resolveGroupingFunctions(query_tree, aggregation_keys, grouping_sets_parameters_list, *planner_context);

    /// Only aggregation keys and aggregates are available for next steps after GROUP BY step
    auto aggregate_step = std::make_unique<ActionsChainStep>(before_aggregation_actions, ActionsChainStep::AvailableOutputColumnsStrategy::OUTPUT_NODES, aggregates_columns);
    actions_chain.addStep(std::move(aggregate_step));

    AggregationAnalysisResult aggregation_analysis_result;
    aggregation_analysis_result.before_aggregation_actions = before_aggregation_actions;
    aggregation_analysis_result.aggregation_keys = std::move(aggregation_keys);
    aggregation_analysis_result.aggregate_descriptions = std::move(aggregates_descriptions);
    aggregation_analysis_result.grouping_sets_parameters_list = std::move(grouping_sets_parameters_list);
    aggregation_analysis_result.group_by_with_constant_keys = group_by_with_constant_keys;

    return aggregation_analysis_result;
}

/** Construct aggregation analysis result if query tree has window functions.
  * Actions before window functions are added into actions chain, if result is not null optional.
  */
std::optional<WindowAnalysisResult> analyzeWindow(QueryTreeNodePtr & query_tree,
    const ColumnsWithTypeAndName & join_tree_input_columns,
    const PlannerContextPtr & planner_context,
    ActionsChain & actions_chain)
{
    auto window_function_nodes = collectWindowFunctionNodes(query_tree);
    if (window_function_nodes.empty())
        return {};

    auto window_descriptions = extractWindowDescriptions(window_function_nodes, *planner_context);

    const auto * chain_available_output_columns = actions_chain.getLastStepAvailableOutputColumnsOrNull();
    const auto & window_input = chain_available_output_columns ? *chain_available_output_columns : join_tree_input_columns;

    PlannerActionsVisitor actions_visitor(planner_context);

    ActionsDAGPtr before_window_actions = std::make_shared<ActionsDAG>(window_input);
    before_window_actions->getOutputs().clear();

    std::unordered_set<std::string_view> before_window_actions_output_node_names;

    for (auto & window_function_node : window_function_nodes)
    {
        auto & window_function_node_typed = window_function_node->as<FunctionNode &>();
        auto & window_node = window_function_node_typed.getWindowNode()->as<WindowNode &>();

        auto expression_dag_nodes = actions_visitor.visit(before_window_actions, window_function_node_typed.getArgumentsNode());

        for (auto & expression_dag_node : expression_dag_nodes)
        {
            if (before_window_actions_output_node_names.contains(expression_dag_node->result_name))
                continue;

            before_window_actions->getOutputs().push_back(expression_dag_node);
            before_window_actions_output_node_names.insert(expression_dag_node->result_name);
        }

        expression_dag_nodes = actions_visitor.visit(before_window_actions, window_node.getPartitionByNode());

        for (auto & expression_dag_node : expression_dag_nodes)
        {
            if (before_window_actions_output_node_names.contains(expression_dag_node->result_name))
                continue;

            before_window_actions->getOutputs().push_back(expression_dag_node);
            before_window_actions_output_node_names.insert(expression_dag_node->result_name);
        }

        /** We add only sort column sort expression in before WINDOW actions DAG.
          * WITH fill expressions must be constant nodes.
          */
        auto & order_by_node_list = window_node.getOrderBy();
        for (auto & sort_node : order_by_node_list.getNodes())
        {
            auto & sort_node_typed = sort_node->as<SortNode &>();
            expression_dag_nodes = actions_visitor.visit(before_window_actions, sort_node_typed.getExpression());

            for (auto & expression_dag_node : expression_dag_nodes)
            {
                if (before_window_actions_output_node_names.contains(expression_dag_node->result_name))
                    continue;

                before_window_actions->getOutputs().push_back(expression_dag_node);
                before_window_actions_output_node_names.insert(expression_dag_node->result_name);
            }
        }
    }

    ColumnsWithTypeAndName window_functions_additional_columns;

    for (auto & window_description : window_descriptions)
        for (auto & window_function : window_description.window_functions)
            window_functions_additional_columns.emplace_back(nullptr, window_function.aggregate_function->getReturnType(), window_function.column_name);

    auto before_window_step = std::make_unique<ActionsChainStep>(before_window_actions,
        ActionsChainStep::AvailableOutputColumnsStrategy::ALL_NODES,
        window_functions_additional_columns);
    actions_chain.addStep(std::move(before_window_step));

    WindowAnalysisResult result;
    result.before_window_actions = std::move(before_window_actions);
    result.window_descriptions = std::move(window_descriptions);

    return result;
}

/** Construct projection analysis result.
  * Projection actions are added into actions chain.
  * It is client responsibility to update projection analysis result with project names actions after chain is finalized.
  */
ProjectionAnalysisResult analyzeProjection(const QueryNode & query_node,
    const ColumnsWithTypeAndName & join_tree_input_columns,
    const PlannerContextPtr & planner_context,
    ActionsChain & actions_chain)
{
    const auto * chain_available_output_columns = actions_chain.getLastStepAvailableOutputColumnsOrNull();
    const auto & projection_input = chain_available_output_columns ? *chain_available_output_columns : join_tree_input_columns;
    auto projection_actions = buildActionsDAGFromExpressionNode(query_node.getProjectionNode(), projection_input, planner_context);

    auto projection_columns = query_node.getProjectionColumns();
    size_t projection_columns_size = projection_columns.size();

    Names projection_column_names;
    NamesWithAliases projection_column_names_with_display_aliases;
    projection_column_names_with_display_aliases.reserve(projection_columns_size);

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

        projection_column_names.push_back(projection_node_name);
        projection_column_names_with_display_aliases.push_back({projection_node_name, projection_column.name});
    }

    auto projection_actions_step = std::make_unique<ActionsChainStep>(projection_actions);
    actions_chain.addStep(std::move(projection_actions_step));

    ProjectionAnalysisResult result;
    result.projection_actions = std::move(projection_actions);
    result.projection_column_names = std::move(projection_column_names);
    result.projection_column_names_with_display_aliases = std::move(projection_column_names_with_display_aliases);

    return result;
}

/** Construct sort analysis result.
  * Actions before sort are added into actions chain.
  */
SortAnalysisResult analyzeSort(const QueryNode & query_node,
    const ColumnsWithTypeAndName & join_tree_input_columns,
    const PlannerContextPtr & planner_context,
    ActionsChain & actions_chain)
{
    const auto *chain_available_output_columns = actions_chain.getLastStepAvailableOutputColumnsOrNull();
    const auto & order_by_input = chain_available_output_columns ? *chain_available_output_columns : join_tree_input_columns;

    ActionsDAGPtr before_sort_actions = std::make_shared<ActionsDAG>(order_by_input);
    auto & before_sort_actions_outputs = before_sort_actions->getOutputs();
    before_sort_actions_outputs.clear();

    PlannerActionsVisitor actions_visitor(planner_context);

    std::unordered_set<std::string_view> before_sort_actions_dag_output_node_names;

    /** We add only sort node sort expression in before ORDER BY actions DAG.
      * WITH fill expressions must be constant nodes.
      */
    const auto & order_by_node_list = query_node.getOrderBy();
    for (const auto & sort_node : order_by_node_list.getNodes())
    {
        auto & sort_node_typed = sort_node->as<SortNode &>();
        auto expression_dag_nodes = actions_visitor.visit(before_sort_actions, sort_node_typed.getExpression());

        for (auto & action_dag_node : expression_dag_nodes)
        {
            if (before_sort_actions_dag_output_node_names.contains(action_dag_node->result_name))
                continue;

            before_sort_actions_outputs.push_back(action_dag_node);
            before_sort_actions_dag_output_node_names.insert(action_dag_node->result_name);
        }
    }

    auto actions_step_before_sort = std::make_unique<ActionsChainStep>(before_sort_actions);
    actions_chain.addStep(std::move(actions_step_before_sort));

    return SortAnalysisResult{std::move(before_sort_actions)};
}

/** Construct limit by analysis result.
  * Actions before limit by are added into actions chain.
  */
LimitByAnalysisResult analyzeLimitBy(const QueryNode & query_node,
    const ColumnsWithTypeAndName & join_tree_input_columns,
    const PlannerContextPtr & planner_context,
    ActionsChain & actions_chain)
{
    const auto * chain_available_output_columns = actions_chain.getLastStepAvailableOutputColumnsOrNull();
    const auto & limit_by_input = chain_available_output_columns ? *chain_available_output_columns : join_tree_input_columns;
    auto before_limit_by_actions = buildActionsDAGFromExpressionNode(query_node.getLimitByNode(), limit_by_input, planner_context);

    Names limit_by_column_names;
    limit_by_column_names.reserve(before_limit_by_actions->getOutputs().size());
    for (auto & output_node : before_limit_by_actions->getOutputs())
        limit_by_column_names.push_back(output_node->result_name);

    auto actions_step_before_limit_by = std::make_unique<ActionsChainStep>(before_limit_by_actions);
    actions_chain.addStep(std::move(actions_step_before_limit_by));

    return LimitByAnalysisResult{std::move(before_limit_by_actions), std::move(limit_by_column_names)};
}

}

PlannerExpressionsAnalysisResult buildExpressionAnalysisResult(QueryTreeNodePtr query_tree,
    const ColumnsWithTypeAndName & join_tree_input_columns,
    const PlannerContextPtr & planner_context)
{
    auto & query_node = query_tree->as<QueryNode &>();

    ActionsChain actions_chain;

    std::optional<FilterAnalysisResult> where_analysis_result_optional;
    std::optional<size_t> where_action_step_index_optional;

    if (query_node.hasWhere())
    {
        where_analysis_result_optional = analyzeFilter(query_node.getWhere(), join_tree_input_columns, planner_context, actions_chain);
        where_action_step_index_optional = actions_chain.getLastStepIndex();
    }

    auto aggregation_analysis_result_optional = analyzeAggregation(query_tree, join_tree_input_columns, planner_context, actions_chain);

    std::optional<FilterAnalysisResult> having_analysis_result_optional;
    std::optional<size_t> having_action_step_index_optional;

    if (query_node.hasHaving())
    {
        having_analysis_result_optional = analyzeFilter(query_node.getHaving(), join_tree_input_columns, planner_context, actions_chain);
        having_action_step_index_optional = actions_chain.getLastStepIndex();
    }

    auto window_analysis_result_optional = analyzeWindow(query_tree, join_tree_input_columns, planner_context, actions_chain);
    auto projection_analysis_result = analyzeProjection(query_node, join_tree_input_columns, planner_context, actions_chain);

    std::optional<SortAnalysisResult> sort_analysis_result_optional;
    if (query_node.hasOrderBy())
        sort_analysis_result_optional = analyzeSort(query_node, join_tree_input_columns, planner_context, actions_chain);

    std::optional<LimitByAnalysisResult> limit_by_analysis_result_optional;

    if (query_node.hasLimitBy())
        limit_by_analysis_result_optional = analyzeLimitBy(query_node, join_tree_input_columns, planner_context, actions_chain);

    const auto * chain_available_output_columns = actions_chain.getLastStepAvailableOutputColumnsOrNull();
    const auto & project_names_input = chain_available_output_columns ? *chain_available_output_columns : join_tree_input_columns;
    auto project_names_actions = std::make_shared<ActionsDAG>(project_names_input);
    project_names_actions->project(projection_analysis_result.projection_column_names_with_display_aliases);
    actions_chain.addStep(std::make_unique<ActionsChainStep>(project_names_actions));

    // std::cout << "Chain dump before finalize" << std::endl;
    // std::cout << actions_chain.dump() << std::endl;

    actions_chain.finalize();

    // std::cout << "Chain dump after finalize" << std::endl;
    // std::cout << actions_chain.dump() << std::endl;

    projection_analysis_result.project_names_actions = std::move(project_names_actions);

    PlannerExpressionsAnalysisResult expressions_analysis_result(std::move(projection_analysis_result));

    if (where_action_step_index_optional && where_analysis_result_optional)
    {
        auto & where_analysis_result = *where_analysis_result_optional;
        auto & where_actions_chain_node = actions_chain.at(*where_action_step_index_optional);
        where_analysis_result.remove_filter_column = !where_actions_chain_node->getChildRequiredOutputColumnsNames().contains(where_analysis_result.filter_column_name);
        expressions_analysis_result.addWhere(std::move(where_analysis_result));
    }

    if (aggregation_analysis_result_optional)
        expressions_analysis_result.addAggregation(std::move(*aggregation_analysis_result_optional));

    if (having_action_step_index_optional && having_analysis_result_optional)
    {
        auto & having_analysis_result = *where_analysis_result_optional;
        auto & having_actions_chain_node = actions_chain.at(*having_action_step_index_optional);
        having_analysis_result.remove_filter_column = !having_actions_chain_node->getChildRequiredOutputColumnsNames().contains(having_analysis_result.filter_column_name);
        expressions_analysis_result.addHaving(std::move(having_analysis_result));
    }

    if (window_analysis_result_optional)
        expressions_analysis_result.addWindow(std::move(*window_analysis_result_optional));

    if (sort_analysis_result_optional)
        expressions_analysis_result.addSort(std::move(*sort_analysis_result_optional));

    if (limit_by_analysis_result_optional)
        expressions_analysis_result.addLimitBy(std::move(*limit_by_analysis_result_optional));

    return expressions_analysis_result;
}

}
