#include <memory>
#include <optional>
#include <Planner/PlannerExpressionAnalysis.h>

#include <Columns/ColumnNullable.h>
#include <Columns/FilterDescription.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>

#include <Interpreters/Context.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/HashUtils.h>
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

#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool group_by_use_nulls;
}

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
std::optional<FilterAnalysisResult> analyzeFilter(
    const QueryTreeNodePtr & filter_expression_node,
    const ColumnsWithTypeAndName & input_columns,
    const PlannerContextPtr & planner_context,
    const ColumnNodePtrWithHashSet & correlated_columns_set,
    ActionsChain & actions_chain)
{
    FilterAnalysisResult result;

    auto [filter_expression_dag, correlated_subtrees] = buildActionsDAGFromExpressionNode(filter_expression_node, input_columns, planner_context, correlated_columns_set);

    result.filter_actions = std::make_shared<ActionsAndProjectInputsFlag>();
    result.filter_actions->dag = std::move(filter_expression_dag);
    result.correlated_subtrees = std::move(correlated_subtrees);

    const auto * output = result.filter_actions->dag.getOutputs().at(0);
    if (output->column && ConstantFilterDescription(*output->column).always_true)
        return {};

    result.filter_column_name = output->result_name;
    actions_chain.addStep(std::make_unique<ActionsChainStep>(result.filter_actions));

    return result;
}

bool canRemoveConstantFromGroupByKey(const ConstantNode & root)
{
    const auto & source_expression = root.getSourceExpression();
    if (!source_expression)
        return true;

    std::stack<const IQueryTreeNode *> nodes;
    nodes.push(source_expression.get());
    while (!nodes.empty())
    {
        const auto * node = nodes.top();
        nodes.pop();

        if (node->getNodeType() == QueryTreeNodeType::QUERY)
            /// Allow removing constants from scalar subqueries. We send them to all the shards.
            continue;

        const auto * constant_node = node->as<ConstantNode>();
        const auto * function_node = node->as<FunctionNode>();
        if (constant_node)
        {
            if (!canRemoveConstantFromGroupByKey(*constant_node))
                return false;
        }
        else if (function_node)
        {
            /// Do not allow removing constants like `hostName()`
            if (function_node->getFunctionOrThrow()->isServerConstant())
                return false;

            for (const auto & child : function_node->getArguments())
                nodes.push(child.get());
        }
        // else
        //     return false;
    }

    return true;
}

/** Construct aggregation analysis result if query tree has GROUP BY or aggregates.
  * Actions before aggregation are added into actions chain, if result is not null optional.
  */
std::optional<AggregationAnalysisResult> analyzeAggregation(
    const QueryTreeNodePtr & query_tree,
    const ColumnsWithTypeAndName & input_columns,
    const PlannerContextPtr & planner_context,
    const ColumnNodePtrWithHashSet & correlated_columns_set,
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

    ActionsAndProjectInputsFlagPtr before_aggregation_actions = std::make_shared<ActionsAndProjectInputsFlag>();
    /// Here it is OK to materialize const columns: if column is used in GROUP BY, it may be expected to become non-const
    /// See https://github.com/ClickHouse/ClickHouse/issues/70655 for example
    before_aggregation_actions->dag = ActionsDAG(input_columns, false);
    before_aggregation_actions->dag.getOutputs().clear();

    std::unordered_set<std::string_view> before_aggregation_actions_output_node_names;

    GroupingSetsParamsList grouping_sets_parameters_list;
    bool group_by_with_constant_keys = false;

    PlannerActionsVisitor actions_visitor(planner_context, correlated_columns_set);

    /// Add expressions from GROUP BY
    bool group_by_use_nulls = planner_context->getQueryContext()->getSettingsRef()[Setting::group_by_use_nulls]
        && (query_node.isGroupByWithGroupingSets() || query_node.isGroupByWithRollup() || query_node.isGroupByWithCube());

    bool is_secondary_query = planner_context->getQueryContext()->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY;
    bool is_distributed_query = planner_context->getQueryContext()->isDistributed();
    bool check_constants_for_group_by_key = is_secondary_query || is_distributed_query;

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
                    const auto * constant_key = grouping_set_key_node->as<ConstantNode>();
                    group_by_with_constant_keys |= (constant_key != nullptr);

                    if (constant_key && !aggregates_descriptions.empty() && (!check_constants_for_group_by_key || canRemoveConstantFromGroupByKey(*constant_key)))
                        continue;

                    auto [expression_dag_nodes, correlated_subtrees] = actions_visitor.visit(before_aggregation_actions->dag, grouping_set_key_node);
                    correlated_subtrees.assertEmpty("in aggregation keys");
                    aggregation_keys.reserve(expression_dag_nodes.size());

                    for (auto & expression_dag_node : expression_dag_nodes)
                    {
                        grouping_sets_parameters.used_keys.push_back(expression_dag_node->result_name);
                        if (before_aggregation_actions_output_node_names.contains(expression_dag_node->result_name))
                            continue;

                        auto expression_type_after_aggregation = group_by_use_nulls ? makeNullableSafe(expression_dag_node->result_type) : expression_dag_node->result_type;
                        auto column_after_aggregation = group_by_use_nulls && expression_dag_node->column != nullptr ? makeNullableSafe(expression_dag_node->column) : expression_dag_node->column;
                        available_columns_after_aggregation.emplace_back(std::move(column_after_aggregation), expression_type_after_aggregation, expression_dag_node->result_name);
                        aggregation_keys.push_back(expression_dag_node->result_name);
                        before_aggregation_actions->dag.getOutputs().push_back(expression_dag_node);
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
                const auto * constant_key = group_by_key_node->as<ConstantNode>();
                group_by_with_constant_keys |= (constant_key != nullptr);

                if (constant_key && !aggregates_descriptions.empty() && (!check_constants_for_group_by_key || canRemoveConstantFromGroupByKey(*constant_key)))
                    continue;

                auto [expression_dag_nodes, correlated_subtrees] = actions_visitor.visit(before_aggregation_actions->dag, group_by_key_node);
                correlated_subtrees.assertEmpty("in aggregation keys");
                aggregation_keys.reserve(expression_dag_nodes.size());

                for (auto & expression_dag_node : expression_dag_nodes)
                {
                    if (before_aggregation_actions_output_node_names.contains(expression_dag_node->result_name))
                        continue;

                    auto expression_type_after_aggregation = group_by_use_nulls ? makeNullableSafe(expression_dag_node->result_type) : expression_dag_node->result_type;
                    auto column_after_aggregation = group_by_use_nulls && expression_dag_node->column != nullptr ? makeNullableSafe(expression_dag_node->column) : expression_dag_node->column;

                    available_columns_after_aggregation.emplace_back(std::move(column_after_aggregation), expression_type_after_aggregation, expression_dag_node->result_name);
                    aggregation_keys.push_back(expression_dag_node->result_name);
                    before_aggregation_actions->dag.getOutputs().push_back(expression_dag_node);
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
            auto [expression_dag_nodes, correlated_subtrees] = actions_visitor.visit(before_aggregation_actions->dag, aggregate_function_node_argument);
            correlated_subtrees.assertEmpty("in aggregate function argument");
            for (auto & expression_dag_node : expression_dag_nodes)
            {
                if (before_aggregation_actions_output_node_names.contains(expression_dag_node->result_name))
                    continue;

                before_aggregation_actions->dag.getOutputs().push_back(expression_dag_node);
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
std::optional<WindowAnalysisResult> analyzeWindow(
    const QueryTreeNodePtr & query_tree,
    const ColumnsWithTypeAndName & input_columns,
    const PlannerContextPtr & planner_context,
    const ColumnNodePtrWithHashSet & correlated_columns_set,
    ActionsChain & actions_chain)
{
    auto window_function_nodes = collectWindowFunctionNodes(query_tree);
    if (window_function_nodes.empty())
        return {};

    auto window_descriptions = extractWindowDescriptions(window_function_nodes, *planner_context);

    PlannerActionsVisitor actions_visitor(planner_context, correlated_columns_set);

    ActionsAndProjectInputsFlagPtr before_window_actions = std::make_shared<ActionsAndProjectInputsFlag>();
    before_window_actions->dag = ActionsDAG(input_columns);
    before_window_actions->dag.getOutputs().clear();

    std::unordered_set<std::string_view> before_window_actions_output_node_names;

    for (auto & window_function_node : window_function_nodes)
    {
        auto & window_function_node_typed = window_function_node->as<FunctionNode &>();
        auto & window_node = window_function_node_typed.getWindowNode()->as<WindowNode &>();

        auto [expression_dag_nodes, correlated_subtrees] = actions_visitor.visit(before_window_actions->dag, window_function_node_typed.getArgumentsNode());
        correlated_subtrees.assertEmpty("in window function arguments");

        for (auto & expression_dag_node : expression_dag_nodes)
        {
            if (before_window_actions_output_node_names.contains(expression_dag_node->result_name))
                continue;

            before_window_actions->dag.getOutputs().push_back(expression_dag_node);
            before_window_actions_output_node_names.insert(expression_dag_node->result_name);
        }

        std::tie(expression_dag_nodes, correlated_subtrees) = actions_visitor.visit(before_window_actions->dag, window_node.getPartitionByNode());
        correlated_subtrees.assertEmpty("in window definition");

        for (auto & expression_dag_node : expression_dag_nodes)
        {
            if (before_window_actions_output_node_names.contains(expression_dag_node->result_name))
                continue;

            before_window_actions->dag.getOutputs().push_back(expression_dag_node);
            before_window_actions_output_node_names.insert(expression_dag_node->result_name);
        }

        /** We add only sort column sort expression in before WINDOW actions DAG.
          * WITH fill expressions must be constant nodes.
          */
        auto & order_by_node_list = window_node.getOrderBy();
        for (auto & sort_node : order_by_node_list.getNodes())
        {
            auto & sort_node_typed = sort_node->as<SortNode &>();
            std::tie(expression_dag_nodes, correlated_subtrees) = actions_visitor.visit(before_window_actions->dag, sort_node_typed.getExpression());
            correlated_subtrees.assertEmpty("in window order definition");

            for (auto & expression_dag_node : expression_dag_nodes)
            {
                if (before_window_actions_output_node_names.contains(expression_dag_node->result_name))
                    continue;

                before_window_actions->dag.getOutputs().push_back(expression_dag_node);
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
ProjectionAnalysisResult analyzeProjection(
    const QueryNode & query_node,
    const ColumnsWithTypeAndName & input_columns,
    const PlannerContextPtr & planner_context,
    const ColumnNodePtrWithHashSet & correlated_columns_set,
    ActionsChain & actions_chain)
{
    auto [projection_actions_dag, correlated_subtrees] = buildActionsDAGFromExpressionNode(
        query_node.getProjectionNode(),
        input_columns,
        planner_context,
        correlated_columns_set);

    auto projection_actions = std::make_shared<ActionsAndProjectInputsFlag>();
    projection_actions->dag = std::move(projection_actions_dag);

    auto projection_columns = query_node.getProjectionColumns();
    size_t projection_columns_size = projection_columns.size();

    Names projection_column_names;
    NamesWithAliases projection_column_names_with_display_aliases;
    projection_column_names_with_display_aliases.reserve(projection_columns_size);

    auto & projection_actions_outputs = projection_actions->dag.getOutputs();
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
    result.correlated_subtrees = std::move(correlated_subtrees);
    result.projection_column_names = std::move(projection_column_names);
    result.projection_column_names_with_display_aliases = std::move(projection_column_names_with_display_aliases);

    return result;
}

/** Construct sort analysis result.
  * Actions before sort are added into actions chain.
  */
SortAnalysisResult analyzeSort(
    const QueryNode & query_node,
    const ColumnsWithTypeAndName & input_columns,
    const PlannerContextPtr & planner_context,
    const ColumnNodePtrWithHashSet & correlated_columns_set,
    ActionsChain & actions_chain)
{
    auto before_sort_actions = std::make_shared<ActionsAndProjectInputsFlag>();
    before_sort_actions->dag = ActionsDAG(input_columns);
    auto & before_sort_actions_outputs = before_sort_actions->dag.getOutputs();
    before_sort_actions_outputs.clear();

    PlannerActionsVisitor actions_visitor(planner_context, correlated_columns_set);
    bool has_with_fill = false;
    std::unordered_set<std::string_view> before_sort_actions_dag_output_node_names;

    /** We add only sort node sort expression in before ORDER BY actions DAG.
      * WITH fill expressions must be constant nodes.
      */
    const auto & order_by_node_list = query_node.getOrderBy();
    for (const auto & sort_node : order_by_node_list.getNodes())
    {
        auto & sort_node_typed = sort_node->as<SortNode &>();
        auto [expression_dag_nodes, correlated_subtrees] = actions_visitor.visit(before_sort_actions->dag, sort_node_typed.getExpression());
        correlated_subtrees.assertEmpty("in ORDER BY");
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
            output_node = &before_sort_actions->dag.materializeNode(*output_node);
    }

    /// We add only INPUT columns necessary for INTERPOLATE expression in before ORDER BY actions DAG
    if (query_node.hasInterpolate())
    {
        auto & interpolate_list_node = query_node.getInterpolate()->as<ListNode &>();

        PlannerActionsVisitor interpolate_actions_visitor(planner_context, correlated_columns_set);
        ActionsDAG interpolate_actions_dag;

        for (auto & interpolate_node : interpolate_list_node.getNodes())
        {
            auto & interpolate_node_typed = interpolate_node->as<InterpolateNode &>();
            if (interpolate_node_typed.getExpression()->getNodeType() == QueryTreeNodeType::CONSTANT)
               continue;

            interpolate_actions_visitor.visit(interpolate_actions_dag, interpolate_node_typed.getInterpolateExpression());
        }

        std::unordered_map<std::string_view, const ActionsDAG::Node *> before_sort_actions_inputs_name_to_node;
        for (const auto & node : before_sort_actions->dag.getInputs())
            before_sort_actions_inputs_name_to_node.emplace(node->result_name, node);

        for (const auto & node : interpolate_actions_dag.getNodes())
        {
            if (before_sort_actions_dag_output_node_names.contains(node.result_name) ||
                node.type != ActionsDAG::ActionType::INPUT)
                continue;

            auto input_node_it = before_sort_actions_inputs_name_to_node.find(node.result_name);
            if (input_node_it == before_sort_actions_inputs_name_to_node.end())
            {
                auto input_column = ColumnWithTypeAndName{node.column, node.result_type, node.result_name};
                const auto * input_node = &before_sort_actions->dag.addInput(std::move(input_column));
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
    const ColumnNodePtrWithHashSet & correlated_columns_set,
    ActionsChain & actions_chain)
{
    auto [before_limit_by_actions_dag, correlated_subtrees] = buildActionsDAGFromExpressionNode(
        query_node.getLimitByNode(),
        input_columns,
        planner_context,
        correlated_columns_set);
    correlated_subtrees.assertEmpty("in LIMIT BY expression");

    auto before_limit_by_actions = std::make_shared<ActionsAndProjectInputsFlag>();
    before_limit_by_actions->dag = std::move(before_limit_by_actions_dag);

    NameSet limit_by_column_names_set;
    Names limit_by_column_names;
    limit_by_column_names.reserve(before_limit_by_actions->dag.getOutputs().size());
    for (auto & output_node : before_limit_by_actions->dag.getOutputs())
    {
        limit_by_column_names_set.insert(output_node->result_name);
        limit_by_column_names.push_back(output_node->result_name);
    }

    for (const auto & node : before_limit_by_actions->dag.getNodes())
    {
        if (required_output_nodes_names.contains(node.result_name) &&
            !limit_by_column_names_set.contains(node.result_name))
            before_limit_by_actions->dag.getOutputs().push_back(&node);
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

    ColumnsWithTypeAndName current_output_columns = join_tree_input_columns;

    auto correlated_columns_set = query_node.getCorrelatedColumnsSet();

    std::optional<FilterAnalysisResult> where_analysis_result_optional;
    std::optional<size_t> where_action_step_index_optional;

    if (query_node.hasWhere())
    {
        where_analysis_result_optional = analyzeFilter(
            query_node.getWhere(),
            current_output_columns,
            planner_context,
            correlated_columns_set,
            actions_chain);
        if (where_analysis_result_optional)
        {
            where_action_step_index_optional = actions_chain.getLastStepIndex();
            current_output_columns = actions_chain.getLastStepAvailableOutputColumns();
        }
    }

    auto aggregation_analysis_result_optional = analyzeAggregation(
        query_tree,
        current_output_columns,
        planner_context,
        correlated_columns_set,
        actions_chain);
    if (aggregation_analysis_result_optional)
        current_output_columns = actions_chain.getLastStepAvailableOutputColumns();

    std::optional<FilterAnalysisResult> having_analysis_result_optional;
    std::optional<size_t> having_action_step_index_optional;

    if (query_node.hasHaving())
    {
        having_analysis_result_optional = analyzeFilter(
            query_node.getHaving(),
            current_output_columns,
            planner_context,
            correlated_columns_set,
            actions_chain);
        if (having_analysis_result_optional)
        {
            having_action_step_index_optional = actions_chain.getLastStepIndex();
            current_output_columns = actions_chain.getLastStepAvailableOutputColumns();
        }
    }

    auto window_analysis_result_optional = analyzeWindow(
        query_tree,
        current_output_columns,
        planner_context,
        correlated_columns_set,
        actions_chain);
    if (window_analysis_result_optional)
        current_output_columns = actions_chain.getLastStepAvailableOutputColumns();

    std::optional<FilterAnalysisResult> qualify_analysis_result_optional;
    std::optional<size_t> qualify_action_step_index_optional;

    if (query_node.hasQualify())
    {
        qualify_analysis_result_optional = analyzeFilter(
            query_node.getQualify(),
            current_output_columns,
            planner_context,
            correlated_columns_set,
            actions_chain);
        if (qualify_analysis_result_optional)
        {
            qualify_action_step_index_optional = actions_chain.getLastStepIndex();
            current_output_columns = actions_chain.getLastStepAvailableOutputColumns();
        }
    }

    auto projection_analysis_result = analyzeProjection(
        query_node,
        current_output_columns,
        planner_context,
        correlated_columns_set,
        actions_chain);
    current_output_columns = actions_chain.getLastStepAvailableOutputColumns();

    std::optional<SortAnalysisResult> sort_analysis_result_optional;
    if (query_node.hasOrderBy())
    {
        sort_analysis_result_optional = analyzeSort(
            query_node,
            current_output_columns,
            planner_context,
            correlated_columns_set,
            actions_chain);
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
        if (sort_analysis_result_optional.has_value() && planner_query_processing_info.isFirstStage() && planner_query_processing_info.getToStage() != QueryProcessingStage::Complete)
        {
            const auto & before_order_by_actions = sort_analysis_result_optional->before_order_by_actions;
            for (const auto & output_node : before_order_by_actions->dag.getOutputs())
                required_output_nodes_names.insert(output_node->result_name);
        }

        limit_by_analysis_result_optional = analyzeLimitBy(
            query_node,
            current_output_columns,
            planner_context,
            required_output_nodes_names,
            correlated_columns_set,
            actions_chain);
        current_output_columns = actions_chain.getLastStepAvailableOutputColumns();
    }

    const auto * chain_available_output_columns = actions_chain.getLastStepAvailableOutputColumnsOrNull();
    auto project_names_input = chain_available_output_columns ? *chain_available_output_columns : current_output_columns;

    /** For distributed query `isToAggregationState`, we do not project names on shards/replicas.
      * However, constant columns from project_names_actions still can be required on the initiator.
      * For example, for query:
      *   SELECT hostName(), number from clusterAllReplicas(default, numbers_mt(3)) ORDER BY number;
      * executed to stage `WithMergeableStateAfterAggregationAndLimit` on replicas
      * we must send hostName() column to initiator.
      */
    if (planner_query_processing_info.isToAggregationState())
    {
        for (auto & column : project_names_input)
            column.column = nullptr;
    }

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

    auto project_names_actions = std::make_shared<ActionsAndProjectInputsFlag>();
    project_names_actions->dag = ActionsDAG(project_names_input);
    project_names_actions->dag.project(projection_analysis_result.projection_column_names_with_display_aliases);
    project_names_actions->project_input = true;
    actions_chain.addStep(std::make_unique<ActionsChainStep>(project_names_actions));

    actions_chain.finalize();

    projection_analysis_result.project_names_actions = std::move(project_names_actions);

    PlannerExpressionsAnalysisResult expressions_analysis_result(std::move(projection_analysis_result));

    if (where_analysis_result_optional && where_action_step_index_optional)
    {
        auto & where_analysis_result = *where_analysis_result_optional;
        auto & where_actions_chain_node = actions_chain.at(*where_action_step_index_optional);
        where_analysis_result.remove_filter_column = !where_actions_chain_node->getChildRequiredOutputColumnsNames().contains(where_analysis_result.filter_column_name);
        expressions_analysis_result.addWhere(std::move(where_analysis_result));
    }

    if (aggregation_analysis_result_optional)
        expressions_analysis_result.addAggregation(std::move(*aggregation_analysis_result_optional));

    if (having_analysis_result_optional && having_action_step_index_optional)
    {
        auto & having_analysis_result = *having_analysis_result_optional;
        auto & having_actions_chain_node = actions_chain.at(*having_action_step_index_optional);
        having_analysis_result.remove_filter_column = !having_actions_chain_node->getChildRequiredOutputColumnsNames().contains(having_analysis_result.filter_column_name);
        expressions_analysis_result.addHaving(std::move(having_analysis_result));
    }

    if (window_analysis_result_optional)
        expressions_analysis_result.addWindow(std::move(*window_analysis_result_optional));

    if (qualify_analysis_result_optional && qualify_action_step_index_optional)
    {
        auto & qualify_analysis_result = *qualify_analysis_result_optional;
        auto & qualify_actions_chain_node = actions_chain.at(*qualify_action_step_index_optional);
        qualify_analysis_result.remove_filter_column = !qualify_actions_chain_node->getChildRequiredOutputColumnsNames().contains(qualify_analysis_result.filter_column_name);
        expressions_analysis_result.addQualify(std::move(qualify_analysis_result));
    }

    if (sort_analysis_result_optional)
        expressions_analysis_result.addSort(std::move(*sort_analysis_result_optional));

    if (limit_by_analysis_result_optional)
        expressions_analysis_result.addLimitBy(std::move(*limit_by_analysis_result_optional));

    return expressions_analysis_result;
}

}
