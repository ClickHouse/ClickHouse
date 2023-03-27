#include <Planner/PlannerExpressionAnalysis.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/ConstantNode.h>
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
    const ColumnsWithTypeAndName & input_columns,
    const PlannerContextPtr & planner_context,
    ActionsChain & actions_chain)
{
    FilterAnalysisResult result;

    result.filter_actions = buildActionsDAGFromExpressionNode(filter_expression_node, input_columns, planner_context);
    result.filter_column_name = result.filter_actions->getOutputs().at(0)->result_name;
    actions_chain.addStep(std::make_unique<ActionsChainStep>(result.filter_actions));

    return result;
}

/** Construct aggregation analysis result if query tree has GROUP BY or aggregates.
  * Actions before aggregation are added into actions chain, if result is not null optional.
  */
std::optional<AggregationAnalysisResult> analyzeAggregation(const QueryTreeNodePtr & query_tree,
    const ColumnsWithTypeAndName & input_columns,
    const PlannerContextPtr & planner_context,
    ActionsChain & actions_chain)
{
    auto & query_node = query_tree->as<QueryNode &>();

    auto aggregate_function_nodes = collectAggregateFunctionNodes(query_tree);
    auto aggregates_descriptions = extractAggregateDescriptions(aggregate_function_nodes, *planner_context);

    ColumnsWithTypeAndName available_columns_after_aggregation;
    available_columns_after_aggregation.reserve(aggregates_descriptions.size());
    for (auto & aggregate_description : aggregates_descriptions)
        available_columns_after_aggregation.emplace_back(nullptr, aggregate_description.function->getResultType(), aggregate_description.column_name);

    Names aggregation_keys;

    ActionsDAGPtr before_aggregation_actions = std::make_shared<ActionsDAG>(input_columns);
    before_aggregation_actions->getOutputs().clear();

    std::unordered_set<std::string_view> before_aggregation_actions_output_node_names;

    GroupingSetsParamsList grouping_sets_parameters_list;
    bool group_by_with_constant_keys = false;

    PlannerActionsVisitor actions_visitor(planner_context);

    /// Add expressions from GROUP BY
    bool group_by_use_nulls = planner_context->getQueryContext()->getSettingsRef().group_by_use_nulls &&
        (query_node.isGroupByWithGroupingSets() || query_node.isGroupByWithRollup() || query_node.isGroupByWithCube());

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
                    auto is_constant_key = grouping_set_key_node->as<ConstantNode>() != nullptr;
                    group_by_with_constant_keys |= is_constant_key;

                    if (is_constant_key && !aggregates_descriptions.empty())
                        continue;

                    auto expression_dag_nodes = actions_visitor.visit(before_aggregation_actions, grouping_set_key_node);
                    aggregation_keys.reserve(expression_dag_nodes.size());

                    for (auto & expression_dag_node : expression_dag_nodes)
                    {
                        grouping_sets_parameters.used_keys.push_back(expression_dag_node->result_name);
                        if (before_aggregation_actions_output_node_names.contains(expression_dag_node->result_name))
                            continue;

                        auto expression_type_after_aggregation = group_by_use_nulls ? makeNullableSafe(expression_dag_node->result_type) : expression_dag_node->result_type;
                        available_columns_after_aggregation.emplace_back(nullptr, expression_type_after_aggregation, expression_dag_node->result_name);
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
        }
        else
        {
            for (auto & group_by_key_node : query_node.getGroupBy().getNodes())
            {
                auto is_constant_key = group_by_key_node->as<ConstantNode>() != nullptr;
                group_by_with_constant_keys |= is_constant_key;

                if (is_constant_key && !aggregates_descriptions.empty())
                    continue;

                auto expression_dag_nodes = actions_visitor.visit(before_aggregation_actions, group_by_key_node);
                aggregation_keys.reserve(expression_dag_nodes.size());

                for (auto & expression_dag_node : expression_dag_nodes)
                {
                    if (before_aggregation_actions_output_node_names.contains(expression_dag_node->result_name))
                        continue;

                    auto expression_type_after_aggregation = group_by_use_nulls ? makeNullableSafe(expression_dag_node->result_type) : expression_dag_node->result_type;
                    available_columns_after_aggregation.emplace_back(nullptr, expression_type_after_aggregation, expression_dag_node->result_name);
                    aggregation_keys.push_back(expression_dag_node->result_name);
                    before_aggregation_actions->getOutputs().push_back(expression_dag_node);
                    before_aggregation_actions_output_node_names.insert(expression_dag_node->result_name);
                }
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
    if (query_node.isGroupByWithRollup() || query_node.isGroupByWithCube() || query_node.isGroupByWithGroupingSets())
        available_columns_after_aggregation.emplace_back(nullptr, std::make_shared<DataTypeUInt64>(), "__grouping_set");

    /// Only aggregation keys and aggregates are available for next steps after GROUP BY step
    auto aggregate_step = std::make_unique<ActionsChainStep>(before_aggregation_actions,
        false /*use_actions_nodes_as_output_columns*/,
        available_columns_after_aggregation);
    actions_chain.addStep(std::move(aggregate_step));

    AggregationAnalysisResult aggregation_analysis_result;
    aggregation_analysis_result.before_aggregation_actions = before_aggregation_actions;
    aggregation_analysis_result.aggregation_keys = std::move(aggregation_keys);
    aggregation_analysis_result.aggregate_descriptions = std::move(aggregates_descriptions);
    aggregation_analysis_result.grouping_sets_parameters_list = std::move(grouping_sets_parameters_list);
    aggregation_analysis_result.group_by_with_constant_keys = group_by_with_constant_keys;

    return aggregation_analysis_result;
}

/** Construct window analysis result if query tree has window functions.
  * Actions before window functions are added into actions chain, if result is not null optional.
  */
std::optional<WindowAnalysisResult> analyzeWindow(const QueryTreeNodePtr & query_tree,
    const ColumnsWithTypeAndName & input_columns,
    const PlannerContextPtr & planner_context,
    ActionsChain & actions_chain)
{
    auto window_function_nodes = collectWindowFunctionNodes(query_tree);
    if (window_function_nodes.empty())
        return {};

    auto window_descriptions = extractWindowDescriptions(window_function_nodes, *planner_context);

    PlannerActionsVisitor actions_visitor(planner_context);

    ActionsDAGPtr before_window_actions = std::make_shared<ActionsDAG>(input_columns);
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
            window_functions_additional_columns.emplace_back(nullptr, window_function.aggregate_function->getResultType(), window_function.column_name);

    auto before_window_step = std::make_unique<ActionsChainStep>(before_window_actions,
        true /*use_actions_nodes_as_output_columns*/,
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
    const ColumnsWithTypeAndName & input_columns,
    const PlannerContextPtr & planner_context,
    ActionsChain & actions_chain)
{
    auto projection_actions = buildActionsDAGFromExpressionNode(query_node.getProjectionNode(), input_columns, planner_context);

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
            projection_columns_size,
            projection_outputs_size);

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
    const ColumnsWithTypeAndName & input_columns,
    const PlannerContextPtr & planner_context,
    ActionsChain & actions_chain)
{
    ActionsDAGPtr before_sort_actions = std::make_shared<ActionsDAG>(input_columns);
    auto & before_sort_actions_outputs = before_sort_actions->getOutputs();
    before_sort_actions_outputs.clear();

    PlannerActionsVisitor actions_visitor(planner_context);
    bool has_with_fill = false;
    std::unordered_set<std::string_view> before_sort_actions_dag_output_node_names;

    /** We add only sort node sort expression in before ORDER BY actions DAG.
      * WITH fill expressions must be constant nodes.
      */
    const auto & order_by_node_list = query_node.getOrderBy();
    for (const auto & sort_node : order_by_node_list.getNodes())
    {
        auto & sort_node_typed = sort_node->as<SortNode &>();
        auto expression_dag_nodes = actions_visitor.visit(before_sort_actions, sort_node_typed.getExpression());
        has_with_fill |= sort_node_typed.withFill();

        for (auto & action_dag_node : expression_dag_nodes)
        {
            if (before_sort_actions_dag_output_node_names.contains(action_dag_node->result_name))
                continue;

            before_sort_actions_outputs.push_back(action_dag_node);
            before_sort_actions_dag_output_node_names.insert(action_dag_node->result_name);
        }
    }

    if (has_with_fill)
    {
        for (auto & output_node : before_sort_actions_outputs)
            output_node = &before_sort_actions->materializeNode(*output_node);
    }

    /// We add only INPUT columns necessary for INTERPOLATE expression in before ORDER BY actions DAG
    if (query_node.hasInterpolate())
    {
        auto & interpolate_list_node = query_node.getInterpolate()->as<ListNode &>();

        PlannerActionsVisitor interpolate_actions_visitor(planner_context);
        auto interpolate_actions_dag = std::make_shared<ActionsDAG>();

        for (auto & interpolate_node : interpolate_list_node.getNodes())
        {
            auto & interpolate_node_typed = interpolate_node->as<InterpolateNode &>();
            interpolate_actions_visitor.visit(interpolate_actions_dag, interpolate_node_typed.getExpression());
            interpolate_actions_visitor.visit(interpolate_actions_dag, interpolate_node_typed.getInterpolateExpression());
        }

        std::unordered_map<std::string_view, const ActionsDAG::Node *> before_sort_actions_inputs_name_to_node;
        for (const auto & node : before_sort_actions->getInputs())
            before_sort_actions_inputs_name_to_node.emplace(node->result_name, node);

        for (const auto & node : interpolate_actions_dag->getNodes())
        {
            if (before_sort_actions_dag_output_node_names.contains(node.result_name) ||
                node.type != ActionsDAG::ActionType::INPUT)
                continue;

            auto input_node_it = before_sort_actions_inputs_name_to_node.find(node.result_name);
            if (input_node_it == before_sort_actions_inputs_name_to_node.end())
            {
                auto input_column = ColumnWithTypeAndName{node.column, node.result_type, node.result_name};
                const auto * input_node = &before_sort_actions->addInput(std::move(input_column));
                auto [it, _] = before_sort_actions_inputs_name_to_node.emplace(node.result_name, input_node);
                input_node_it = it;
            }

            before_sort_actions_outputs.push_back(input_node_it->second);
            before_sort_actions_dag_output_node_names.insert(node.result_name);
        }
    }

    auto actions_step_before_sort = std::make_unique<ActionsChainStep>(before_sort_actions);
    actions_chain.addStep(std::move(actions_step_before_sort));

    return SortAnalysisResult{std::move(before_sort_actions), has_with_fill};
}

/** Construct limit by analysis result.
  * Actions before limit by are added into actions chain.
  */
LimitByAnalysisResult analyzeLimitBy(const QueryNode & query_node,
    const ColumnsWithTypeAndName & input_columns,
    const PlannerContextPtr & planner_context,
    const NameSet & required_output_nodes_names,
    ActionsChain & actions_chain)
{
    auto before_limit_by_actions = buildActionsDAGFromExpressionNode(query_node.getLimitByNode(), input_columns, planner_context);

    NameSet limit_by_column_names_set;
    Names limit_by_column_names;
    limit_by_column_names.reserve(before_limit_by_actions->getOutputs().size());
    for (auto & output_node : before_limit_by_actions->getOutputs())
    {
        limit_by_column_names_set.insert(output_node->result_name);
        limit_by_column_names.push_back(output_node->result_name);
    }

    for (const auto & node : before_limit_by_actions->getNodes())
    {
        if (required_output_nodes_names.contains(node.result_name) &&
            !limit_by_column_names_set.contains(node.result_name))
            before_limit_by_actions->getOutputs().push_back(&node);
    }

    auto actions_step_before_limit_by = std::make_unique<ActionsChainStep>(before_limit_by_actions);
    actions_chain.addStep(std::move(actions_step_before_limit_by));

    return LimitByAnalysisResult{std::move(before_limit_by_actions), std::move(limit_by_column_names)};
}

}

PlannerExpressionsAnalysisResult buildExpressionAnalysisResult(const QueryTreeNodePtr & query_tree,
    const ColumnsWithTypeAndName & join_tree_input_columns,
    const PlannerContextPtr & planner_context,
    const PlannerQueryProcessingInfo & planner_query_processing_info)
{
    auto & query_node = query_tree->as<QueryNode &>();

    ActionsChain actions_chain;

    std::optional<FilterAnalysisResult> where_analysis_result_optional;
    std::optional<size_t> where_action_step_index_optional;

    ColumnsWithTypeAndName current_output_columns = join_tree_input_columns;

    if (query_node.hasWhere())
    {
        where_analysis_result_optional = analyzeFilter(query_node.getWhere(), current_output_columns, planner_context, actions_chain);
        where_action_step_index_optional = actions_chain.getLastStepIndex();
        current_output_columns = actions_chain.getLastStepAvailableOutputColumns();
    }

    auto aggregation_analysis_result_optional = analyzeAggregation(query_tree, current_output_columns, planner_context, actions_chain);
    if (aggregation_analysis_result_optional)
        current_output_columns = actions_chain.getLastStepAvailableOutputColumns();

    std::optional<FilterAnalysisResult> having_analysis_result_optional;
    std::optional<size_t> having_action_step_index_optional;

    if (query_node.hasHaving())
    {
        having_analysis_result_optional = analyzeFilter(query_node.getHaving(), current_output_columns, planner_context, actions_chain);
        having_action_step_index_optional = actions_chain.getLastStepIndex();
        current_output_columns = actions_chain.getLastStepAvailableOutputColumns();
    }

    auto window_analysis_result_optional = analyzeWindow(query_tree, current_output_columns, planner_context, actions_chain);
    if (window_analysis_result_optional)
        current_output_columns = actions_chain.getLastStepAvailableOutputColumns();

    auto projection_analysis_result = analyzeProjection(query_node, current_output_columns, planner_context, actions_chain);
    current_output_columns = actions_chain.getLastStepAvailableOutputColumns();

    std::optional<SortAnalysisResult> sort_analysis_result_optional;
    if (query_node.hasOrderBy())
    {
        sort_analysis_result_optional = analyzeSort(query_node, current_output_columns, planner_context, actions_chain);
        current_output_columns = actions_chain.getLastStepAvailableOutputColumns();
    }

    std::optional<LimitByAnalysisResult> limit_by_analysis_result_optional;

    if (query_node.hasLimitBy())
    {
        /** If we process only first stage of query and there is ORDER BY, we must preserve ORDER BY output columns
          * and put them into LIMIT BY output columns, to prevent removing of unused expressions during chain finalize.
          *
          * Example: SELECT 1 FROM remote('127.0.0.{2,3}', system.one) ORDER BY dummy LIMIT 1 BY 1;
          * In this example, LIMIT BY actions does not need `dummy` column, but we must preserve it, because
          * otherwise coordinator does not find it in block.
          */
        NameSet required_output_nodes_names;
        if (sort_analysis_result_optional.has_value() && !planner_query_processing_info.isSecondStage())
        {
            const auto & before_order_by_actions = sort_analysis_result_optional->before_order_by_actions;
            for (const auto & output_node : before_order_by_actions->getOutputs())
                required_output_nodes_names.insert(output_node->result_name);
        }

        limit_by_analysis_result_optional = analyzeLimitBy(query_node,
            current_output_columns,
            planner_context,
            required_output_nodes_names,
            actions_chain);
        current_output_columns = actions_chain.getLastStepAvailableOutputColumns();
    }

    const auto * chain_available_output_columns = actions_chain.getLastStepAvailableOutputColumnsOrNull();
    auto project_names_input = chain_available_output_columns ? *chain_available_output_columns : current_output_columns;
    bool has_with_fill = sort_analysis_result_optional.has_value() && sort_analysis_result_optional->has_with_fill;

    /** If there is WITH FILL we must use non constant projection columns.
      *
      * Example: SELECT 1 AS value ORDER BY value ASC WITH FILL FROM 0 TO 5 STEP 1;
      *
      * If there is DISTINCT we must preserve non constant projection output columns
      * in project names actions, to prevent removing of unused expressions during chain finalize.
      *
      * Example: SELECT DISTINCT id, 1 AS value FROM test_table ORDER BY id;
      */
    if (has_with_fill || query_node.isDistinct())
    {
        std::unordered_set<std::string_view> projection_column_names;

        if (query_node.isDistinct())
        {
            for (auto & [column_name, _] : projection_analysis_result.projection_column_names_with_display_aliases)
                projection_column_names.insert(column_name);
        }

        for (auto & column : project_names_input)
        {
            if (has_with_fill || projection_column_names.contains(column.name))
                column.column = nullptr;
        }
    }

    auto project_names_actions = std::make_shared<ActionsDAG>(project_names_input);
    project_names_actions->project(projection_analysis_result.projection_column_names_with_display_aliases);
    actions_chain.addStep(std::make_unique<ActionsChainStep>(project_names_actions));

    actions_chain.finalize();

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
        auto & having_analysis_result = *having_analysis_result_optional;
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
