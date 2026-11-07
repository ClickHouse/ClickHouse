#include <memory>
#include <optional>
#include <stack>
#include <Planner/PlannerExpressionAnalysis.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/FilterDescription.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>

#include <Interpreters/Context.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/Utils.h>
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
    extern const int UNEXPECTED_EXPRESSION;
}

namespace
{

/// The columns a step's correlated subqueries correlate on. They are read from the step's input, by the
/// join the subquery is planned into, so the child step has to keep them even though no expression of
/// this step reads them.
NameSet correlatedColumnsOfSubtrees(const CorrelatedSubtrees & correlated_subtrees)
{
    NameSet result;
    for (const auto & correlated_subquery : correlated_subtrees.subqueries)
        result.insert(
            correlated_subquery.correlated_column_identifiers.begin(), correlated_subquery.correlated_column_identifiers.end());
    return result;
}

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
    actions_chain.addStep(std::make_unique<ActionsChainStep>(
        result.filter_actions,
        true /*use_actions_nodes_as_output_columns*/,
        ColumnsWithTypeAndName{},
        correlatedColumnsOfSubtrees(result.correlated_subtrees)));

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
            if (!function_node->isOrdinaryFunction())
            {
                /// Non-ordinary functions (window, aggregate, lambda) cannot be server constants,
                /// so it is safe to remove them from GROUP BY. We cannot call getFunctionOrThrow()
                /// on them (it would throw), but we also don't need to — they won't produce
                /// server-specific values. Skip examining their children.
                continue;
            }

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

                        auto expression_type_after_aggregation = group_by_use_nulls ? makeNullableOrLowCardinalityNullableSafe(expression_dag_node->result_type) : expression_dag_node->result_type;
                        ColumnPtr column_after_aggregation = group_by_use_nulls && expression_dag_node->column != nullptr ? makeNullableOrLowCardinalityNullableSafe(expression_dag_node->column) : ColumnPtr(expression_dag_node->column);
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
            /// ROLLUP and CUBE derive their grouping sets from the number of aggregation keys, and
            /// `GROUPING` is resolved against the full key list, so dropping a constant key here would
            /// lose grouping levels: `GROUP BY CUBE(1, number)` would produce the sets of a single key.
            bool keep_constant_keys = query_node.isGroupByWithRollup() || query_node.isGroupByWithCube();

            for (auto & group_by_key_node : query_node.getGroupBy().getNodes())
            {
                const auto * constant_key = group_by_key_node->as<ConstantNode>();
                group_by_with_constant_keys |= (constant_key != nullptr);

                if (constant_key && !keep_constant_keys && !aggregates_descriptions.empty()
                    && (!check_constants_for_group_by_key || canRemoveConstantFromGroupByKey(*constant_key)))
                    continue;

                auto [expression_dag_nodes, correlated_subtrees] = actions_visitor.visit(before_aggregation_actions->dag, group_by_key_node);
                correlated_subtrees.assertEmpty("in aggregation keys");
                aggregation_keys.reserve(expression_dag_nodes.size());

                for (auto & expression_dag_node : expression_dag_nodes)
                {
                    if (before_aggregation_actions_output_node_names.contains(expression_dag_node->result_name))
                        continue;

                    auto expression_type_after_aggregation = group_by_use_nulls ? makeNullableOrLowCardinalityNullableSafe(expression_dag_node->result_type) : expression_dag_node->result_type;
                    ColumnPtr column_after_aggregation = group_by_use_nulls && expression_dag_node->column != nullptr ? makeNullableOrLowCardinalityNullableSafe(expression_dag_node->column) : ColumnPtr(expression_dag_node->column);

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

    if (aggregation_keys.empty() && aggregates_descriptions.empty() && grouping_sets_parameters_list.empty())
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

/** A lower bound of the number of bytes that a value of a type without a maximum size of value takes in a column.
  */
size_t getMinimumSizeOfUnboundedValue(const IDataType & type)
{
    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(&type))
        return 1 + getMinimumSizeOfUnboundedValue(*nullable_type->getNestedType());

    /// The columns of these types keep an offset for each value.
    if (isString(type) || isArray(type) || isMap(type))
        return sizeof(UInt64);

    /// For example, a `LowCardinality` column keeps only an index for each value, which can take one byte.
    return 0;
}

/** The expression that computes the values of a column: for a column of a subquery, its projection expression,
  * so that the columns of a subquery that are aliases of each other resolve to the same expression.
  */
QueryTreeNodePtr resolveColumnExpression(QueryTreeNodePtr node)
{
    static constexpr size_t max_steps = 100;
    for (size_t step = 0; step < max_steps; ++step)
    {
        const auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            break;

        if (column_node->hasExpression() && column_node->getExpression()->getNodeType() == QueryTreeNodeType::CONSTANT)
            return column_node->getExpression();

        auto column_source = column_node->getColumnSourceOrNull();
        const auto * subquery_node = column_source ? column_source->as<QueryNode>() : nullptr;
        if (!subquery_node)
            break;

        const auto & projection_columns = subquery_node->getProjectionColumns();
        const auto & projection_nodes = subquery_node->getProjection().getNodes();
        if (projection_columns.size() != projection_nodes.size())
            break;

        auto it = std::find_if(projection_columns.begin(), projection_columns.end(),
            [&](const auto & projection_column) { return projection_column.name == column_node->getColumnName(); });
        if (it == projection_columns.end())
            break;

        node = projection_nodes[it - projection_columns.begin()];
    }

    return node;
}

/// The grouping keys of a query with aggregation, which are, like the aggregate function results,
/// the input columns of the actions after aggregation.
struct AggregationKeys
{
    bool has_aggregation = false;
    QueryTreeNodePtrWithHashSet keys;
};

/** Whether the expression can be computed before the window functions instead of after them.
  * The window step keeps every row and only changes the order of rows and their split into blocks,
  * so an expression evaluated row by row has the same value on either side of it.
  * The columns, the grouping keys and the aggregate function results that the expression reads and whose values
  * have no maximum size, such as `String` or `Array`, are added to `unbounded_inputs`, except constant columns.
  * A column is added as the expression that computes it, see `resolveColumnExpression`.
  */
bool canBeComputedBeforeWindow(
    const QueryTreeNodePtr & node,
    const ColumnNodePtrWithHashSet & correlated_columns_set,
    const AggregationKeys & aggregation_keys,
    QueryTreeNodePtrWithHashSet & unbounded_inputs)
{
    if (aggregation_keys.keys.contains(node))
    {
        if (!node->getResultType()->haveMaximumSizeOfValue())
            unbounded_inputs.insert(node);
        return true;
    }

    switch (node->getNodeType())
    {
        case QueryTreeNodeType::CONSTANT:
            return true;
        case QueryTreeNodeType::COLUMN:
        {
            const auto & column_node = node->as<ColumnNode &>();
            auto column_source = column_node.getColumnSourceOrNull();
            bool is_lambda_argument = column_source && column_source->getNodeType() == QueryTreeNodeType::LAMBDA_ARGS;

            /// After aggregation, a column is read only as a part of a grouping key or of an aggregate function,
            /// so the expression could not be matched with the input columns of the actions.
            if (aggregation_keys.has_aggregation && !is_lambda_argument)
                return false;

            if (!is_lambda_argument && !column_node.getColumnType()->haveMaximumSizeOfValue())
            {
                auto column_expression = resolveColumnExpression(node);
                if (column_expression->getNodeType() != QueryTreeNodeType::CONSTANT)
                    unbounded_inputs.insert(column_expression);
            }
            return !correlated_columns_set.contains(std::static_pointer_cast<ColumnNode>(node));
        }
        case QueryTreeNodeType::LIST:
        {
            for (const auto & child : node->as<ListNode &>().getNodes())
                if (!canBeComputedBeforeWindow(child, correlated_columns_set, aggregation_keys, unbounded_inputs))
                    return false;
            return true;
        }
        case QueryTreeNodeType::LAMBDA:
            return canBeComputedBeforeWindow(node->as<LambdaNode &>().getExpression(), correlated_columns_set, aggregation_keys, unbounded_inputs);
        case QueryTreeNodeType::FUNCTION:
        {
            const auto & function_node = node->as<FunctionNode &>();
            if (function_node.isWindowFunction())
                return false;

            /// The result of an aggregate function is an input column of the actions after aggregation.
            if (function_node.isAggregateFunction())
            {
                if (!function_node.getResultType()->haveMaximumSizeOfValue())
                    unbounded_inputs.insert(node);
                return true;
            }

            /// `arrayJoin` in the projection multiplies the rows only after the window functions are computed.
            if (function_node.getFunctionName() == "arrayJoin")
                return false;

            /// Stateful functions (`rowNumberInBlock`, `runningDifference`, ...) depend on the order of rows,
            /// some functions that are not deterministic in the scope of the query (`blockSize`) depend on blocks,
            /// and the side effects of some functions (`sleepEachRow`) depend on blocks too.
            if (auto function = function_node.getFunction();
                function && (function->isStateful() || !function->isDeterministicInScopeOfQuery() || function->hasObservableSideEffects()))
                return false;

            return canBeComputedBeforeWindow(function_node.getArgumentsNode(), correlated_columns_set, aggregation_keys, unbounded_inputs);
        }
        default:
            /// Subqueries.
            return false;
    }
}

struct ExpressionBeforeWindowCandidate
{
    QueryTreeNodePtr node;
    QueryTreeNodePtrWithHashSet unbounded_inputs;
    size_t maximum_size_of_result = 0;
};

/** Collect the largest subexpressions of the projection that can be computed before the window functions,
  * read columns of unbounded size and have a result of a bounded size.
  * The arguments of short-circuit functions that may be executed lazily are not collected, as computing them
  * before the window step would execute them for all rows.
  */
void collectExpressionsComputableBeforeWindow(
    const QueryTreeNodePtr & node,
    const ColumnNodePtrWithHashSet & correlated_columns_set,
    const AggregationKeys & aggregation_keys,
    QueryTreeNodePtrWithHashSet & collected_nodes,
    std::vector<ExpressionBeforeWindowCandidate> & result)
{
    auto * function_node = node->as<FunctionNode>();
    if (!function_node || !function_node->isOrdinaryFunction())
        return;

    QueryTreeNodePtrWithHashSet unbounded_inputs;
    const auto & result_type = function_node->getResultType();
    if (canBeComputedBeforeWindow(node, correlated_columns_set, aggregation_keys, unbounded_inputs)
        && !unbounded_inputs.empty()
        && result_type->haveMaximumSizeOfValue())
    {
        if (collected_nodes.insert(node).second)
            result.push_back({node, std::move(unbounded_inputs), result_type->getMaximumSizeOfValueInMemory()});
        return;
    }

    const auto & arguments = function_node->getArguments().getNodes();

    IFunctionBase::ShortCircuitSettings short_circuit_settings;
    auto function = function_node->getFunction();
    bool is_short_circuit = function && function->isShortCircuit(short_circuit_settings, arguments.size());

    for (size_t i = 0; i < arguments.size(); ++i)
    {
        /// For commutative short-circuit functions (`and`, `or`) any argument may be executed lazily.
        if (is_short_circuit
            && (short_circuit_settings.arguments_are_commutative
                || short_circuit_settings.force_enable_lazy_execution
                || !short_circuit_settings.arguments_with_disabled_lazy_execution.contains(i)))
            continue;

        collectExpressionsComputableBeforeWindow(arguments[i], correlated_columns_set, aggregation_keys, collected_nodes, result);
    }
}

/** Collect the columns of tables, the grouping keys and the aggregate function results from which the values
  * of the expression are computed, following the columns of subqueries to their projection expressions and
  * the columns with expressions (`ALIAS` columns) to their expressions. Two values can share memory
  * (for example, if one is an identity `CAST` of the other) only if they are computed from common base columns.
  * The grouping keys are matched only in the expressions of the query itself, `aggregation_keys` is null
  * for the expressions of subqueries.
  */
void collectBaseColumns(
    const QueryTreeNodePtr & node,
    const AggregationKeys * aggregation_keys,
    QueryTreeNodePtrWithHashSet & base_columns,
    size_t depth = 0)
{
    static constexpr size_t max_depth = 100;
    if (depth > max_depth || (aggregation_keys && aggregation_keys->keys.contains(node)))
    {
        base_columns.insert(node);
        return;
    }

    if (node->getNodeType() == QueryTreeNodeType::CONSTANT)
        return;

    if (const auto * column_node = node->as<ColumnNode>())
    {
        auto column_source = column_node->getColumnSourceOrNull();
        if (column_source && column_source->getNodeType() == QueryTreeNodeType::LAMBDA_ARGS)
            return;

        if (column_node->hasExpression())
        {
            collectBaseColumns(column_node->getExpression(), nullptr, base_columns, depth + 1);
            return;
        }

        auto column_expression = resolveColumnExpression(node);
        if (column_expression.get() == node.get())
            base_columns.insert(node);
        else
            collectBaseColumns(column_expression, nullptr, base_columns, depth + 1);
        return;
    }

    if (const auto * function_node = node->as<FunctionNode>(); function_node && function_node->isAggregateFunction() && !function_node->isWindowFunction())
    {
        base_columns.insert(node);
        return;
    }

    for (const auto & child : node->getChildren())
        if (child)
            collectBaseColumns(child, aggregation_keys, base_columns, depth + 1);
}

/** Collect the base columns (see `collectBaseColumns`) of the values that are read after the window step
  * by anything except the expressions computed before it.
  * It includes the arguments of window functions, which the window step buffers anyway.
  */
void collectBaseColumnsReadAfterWindow(
    const QueryTreeNodePtr & node,
    const QueryTreeNodePtrWithHashSet & expressions_before_window,
    const AggregationKeys & aggregation_keys,
    QueryTreeNodePtrWithHashSet & base_columns)
{
    if (!node || expressions_before_window.contains(node))
        return;

    if (node->getNodeType() == QueryTreeNodeType::COLUMN || aggregation_keys.keys.contains(node))
    {
        collectBaseColumns(node, &aggregation_keys, base_columns);
        return;
    }

    if (const auto * function_node = node->as<FunctionNode>(); function_node && function_node->isAggregateFunction() && !function_node->isWindowFunction())
    {
        base_columns.insert(node);
        return;
    }

    for (const auto & child : node->getChildren())
        collectBaseColumnsReadAfterWindow(child, expressions_before_window, aggregation_keys, base_columns);
}

/** Select the expressions worth computing before the window functions.
  * Window functions buffer their input, so computing a projection expression before them lets the window step
  * buffer its result instead of the values it reads, but a value is freed only if nothing else read after
  * the window step can share its memory. This is assumed only if they have no common base columns, otherwise
  * the value is not credited. The expressions with common base columns are selected or rejected together:
  * only if the maximum size of their results is not larger than a lower bound of the size of the values they free,
  * so the buffered rows can not become larger.
  */
QueryTreeNodes selectExpressionsWorthComputingBeforeWindow(
    const QueryNode & query_node,
    const AggregationKeys & aggregation_keys,
    std::vector<ExpressionBeforeWindowCandidate> candidates)
{
    QueryTreeNodePtrWithHashSet candidate_nodes;
    for (const auto & candidate : candidates)
        candidate_nodes.insert(candidate.node);

    QueryTreeNodePtrWithHashSet base_columns_read_after_window;
    for (const auto * node : {&query_node.getProjectionNode(), &query_node.getOrderByNode(), &query_node.getInterpolate(), &query_node.getLimitByNode()})
        collectBaseColumnsReadAfterWindow(*node, candidate_nodes, aggregation_keys, base_columns_read_after_window);

    std::vector<QueryTreeNodePtrWithHashSet> candidates_base_columns(candidates.size());
    for (size_t i = 0; i < candidates.size(); ++i)
        for (const auto & input : candidates[i].unbounded_inputs)
            collectBaseColumns(input.node, &aggregation_keys, candidates_base_columns[i]);

    QueryTreeNodes result;
    std::vector<bool> visited(candidates.size(), false);

    for (size_t group_start = 0; group_start < candidates.size(); ++group_start)
    {
        if (visited[group_start])
            continue;

        /// The group of candidates connected by common base columns.
        std::vector<size_t> group{group_start};
        visited[group_start] = true;
        QueryTreeNodePtrWithHashSet group_inputs = candidates[group_start].unbounded_inputs;

        for (size_t i = 0; i < group.size(); ++i)
        {
            for (size_t other = 0; other < candidates.size(); ++other)
            {
                if (visited[other])
                    continue;

                bool has_common_base_columns = false;
                for (const auto & base_column : candidates_base_columns[other])
                    if (candidates_base_columns[group[i]].contains(base_column))
                        has_common_base_columns = true;

                if (!has_common_base_columns)
                    continue;

                visited[other] = true;
                group.push_back(other);
                group_inputs.insert(candidates[other].unbounded_inputs.begin(), candidates[other].unbounded_inputs.end());
            }
        }

        size_t minimum_size_of_freed_inputs = 0;
        QueryTreeNodePtrWithHashSet credited_base_columns;
        for (const auto & input : group_inputs)
        {
            QueryTreeNodePtrWithHashSet input_base_columns;
            collectBaseColumns(input.node, &aggregation_keys, input_base_columns);

            /// A value without base columns, such as `materialize('a')`, may share memory with anything.
            bool can_be_freed = !input_base_columns.empty();
            for (const auto & base_column : input_base_columns)
                if (base_columns_read_after_window.contains(base_column) || credited_base_columns.contains(base_column))
                    can_be_freed = false;

            if (!can_be_freed)
                continue;

            credited_base_columns.insert(input_base_columns.begin(), input_base_columns.end());
            minimum_size_of_freed_inputs += getMinimumSizeOfUnboundedValue(*input.node->getResultType());
        }

        size_t maximum_size_of_results = 0;
        for (size_t candidate_index : group)
            maximum_size_of_results += candidates[candidate_index].maximum_size_of_result;

        if (minimum_size_of_freed_inputs == 0 || maximum_size_of_results > minimum_size_of_freed_inputs)
            continue;

        for (size_t candidate_index : group)
            result.push_back(candidates[candidate_index].node);
    }

    return result;
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

    /// A window function's aggregate is created during analysis and keeps the argument types known then,
    /// so an argument whose actual type differs when the window step runs needs the aggregate re-created
    /// with that type.
    for (auto & window_description : window_descriptions)
    {
        for (auto & window_function : window_description.window_functions)
        {
            bool types_changed = false;
            DataTypes actual_argument_types;
            actual_argument_types.reserve(window_function.argument_names.size());

            for (size_t i = 0; i < window_function.argument_names.size(); ++i)
            {
                const auto * dag_node = before_window_actions->dag.tryFindInOutputs(window_function.argument_names[i]);
                if (dag_node && !window_function.argument_types[i]->equals(*dag_node->result_type))
                {
                    actual_argument_types.push_back(dag_node->result_type);
                    types_changed = true;
                }
                else
                {
                    actual_argument_types.push_back(window_function.argument_types[i]);
                }
            }

            if (types_changed)
            {
                AggregateFunctionProperties properties;
                auto new_function = AggregateFunctionFactory::instance().get(
                    window_function.aggregate_function->getName(),
                    NullsAction::EMPTY,
                    actual_argument_types,
                    window_function.function_parameters,
                    properties);
                window_function.aggregate_function = std::move(new_function);
                window_function.argument_types = std::move(actual_argument_types);
            }
        }
    }

    ColumnsWithTypeAndName window_functions_additional_columns;

    for (auto & window_description : window_descriptions)
        for (auto & window_function : window_description.window_functions)
            window_functions_additional_columns.emplace_back(nullptr, window_function.aggregate_function->getResultType(), window_function.column_name);

    /// With `QUALIFY`, the projection is computed only for the rows that pass the filter after the window functions,
    /// and with `LIMIT`, `LIMIT AFTER` or `LIMIT UNTIL` it may be computed only for the first blocks of the result.
    QueryTreeNodes expressions_before_window;
    const auto & query_node = query_tree->as<const QueryNode &>();
    if (!query_node.hasQualify() && !query_node.hasLimit() && !query_node.hasLimitAfter() && !query_node.hasLimitUntil())
    {
        AggregationKeys aggregation_keys;
        aggregation_keys.has_aggregation = query_node.hasGroupBy() || hasAggregateFunctionNodes(query_tree);
        if (query_node.hasGroupBy())
        {
            for (const auto & grouping_node : query_node.getGroupBy().getNodes())
            {
                if (query_node.isGroupByWithGroupingSets())
                {
                    for (const auto & key : grouping_node->as<ListNode &>().getNodes())
                        aggregation_keys.keys.insert(key);
                }
                else
                {
                    aggregation_keys.keys.insert(grouping_node);
                }
            }
        }

        QueryTreeNodePtrWithHashSet collected_nodes;
        std::vector<ExpressionBeforeWindowCandidate> candidates;
        for (const auto & projection_node : query_node.getProjection().getNodes())
            collectExpressionsComputableBeforeWindow(projection_node, correlated_columns_set, aggregation_keys, collected_nodes, candidates);

        expressions_before_window = selectExpressionsWorthComputingBeforeWindow(query_node, aggregation_keys, std::move(candidates));
    }

    WindowAnalysisResult result;

    if (expressions_before_window.empty())
    {
        actions_chain.addStep(std::make_unique<ActionsChainStep>(before_window_actions,
            true /*use_actions_nodes_as_output_columns*/,
            window_functions_additional_columns));

        result.before_window_actions = std::move(before_window_actions);
        result.window_descriptions = std::move(window_descriptions);
        return result;
    }

    actions_chain.addStep(std::make_unique<ActionsChainStep>(before_window_actions, true /*use_actions_nodes_as_output_columns*/));

    /** The projection expressions are computed in a separate step, which is executed on the initiator together with
      * the window functions, while the "Before WINDOW" actions may be executed on the shards.
      * Keeping them separate keeps the header of the blocks sent by the shards the same as without this step.
      */
    auto before_window_projection_actions = std::make_shared<ActionsAndProjectInputsFlag>();
    before_window_projection_actions->dag = ActionsDAG(actions_chain.getLastStepAvailableOutputColumns());

    auto & before_window_projection_outputs = before_window_projection_actions->dag.getOutputs();
    before_window_projection_outputs.clear();

    std::unordered_set<std::string_view> before_window_projection_output_node_names;

    std::unordered_map<std::string_view, const ActionsDAG::Node *> before_window_projection_inputs_by_name;
    for (const auto * input_node : before_window_projection_actions->dag.getInputs())
        before_window_projection_inputs_by_name.emplace(input_node->result_name, input_node);

    /// The columns read by the window step.
    for (const auto * before_window_output_node : before_window_actions->dag.getOutputs())
    {
        if (!before_window_projection_output_node_names.insert(before_window_output_node->result_name).second)
            continue;

        before_window_projection_outputs.push_back(before_window_projection_inputs_by_name.at(before_window_output_node->result_name));
    }

    for (const auto & expression : expressions_before_window)
    {
        auto [expression_dag_nodes, correlated_subtrees] = actions_visitor.visit(before_window_projection_actions->dag, expression);
        correlated_subtrees.assertEmpty("in projection computed before window functions");

        for (const auto * expression_dag_node : expression_dag_nodes)
        {
            if (expression_dag_node->column && isColumnConst(*expression_dag_node->column))
                continue;

            if (!before_window_projection_output_node_names.insert(expression_dag_node->result_name).second)
                continue;

            before_window_projection_outputs.push_back(expression_dag_node);
        }
    }

    actions_chain.addStep(std::make_unique<ActionsChainStep>(before_window_projection_actions,
        true /*use_actions_nodes_as_output_columns*/,
        window_functions_additional_columns));

    result.before_window_actions = std::move(before_window_actions);
    result.before_window_projection_actions = std::move(before_window_projection_actions);
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

    auto projection_actions_step = std::make_unique<ActionsChainStep>(
        projection_actions,
        true /*use_actions_nodes_as_output_columns*/,
        ColumnsWithTypeAndName{},
        correlatedColumnsOfSubtrees(correlated_subtrees));
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

    auto actions_step_before_sort = std::make_unique<ActionsChainStep>(before_sort_actions);
    actions_chain.addStep(std::move(actions_step_before_sort));

    ActionsAndProjectInputsFlagPtr before_interpolate_actions;
    NameToNameMap interpolated_column_names;
    NameToNameMap interpolate_target_column_names;

    /// We add only INPUT columns necessary for INTERPOLATE expression in before ORDER BY actions DAG
    if (query_node.hasInterpolate())
    {
        auto & interpolate_list_node = query_node.getInterpolate()->as<ListNode &>();

        PlannerActionsVisitor interpolate_actions_visitor(planner_context, correlated_columns_set);

        /// getLastStepAvailableOutputColumns should have been correct.
        /// However, we materialize ORDER BY columns in case of WITH FILL, and it causes a name-collision.
        /// If we take getLastStepAvailableOutputColumns list, it may return non-materialized constants,
        /// so here we add materialized ORDER BY columns manually, and append everything else after.
        ActionsDAG before_interpolate_actions_dag(before_sort_actions->dag.getResultColumns());
        for (const auto & out : actions_chain.getLastStepAvailableOutputColumns())
        {
            if (before_sort_actions_dag_output_node_names.contains(out.name))
                continue;

            /** The `Set` placeholder of an `IN` and the `Function` column of a lambda are not values: no
              * `INTERPOLATE` expression can name them (their names are internal), and they cannot be
              * materialized. Keeping one as an output carries it past the filter that consumes it into every
              * step above, including ones that build rows out of the whole header: the `FINAL` merge of a
              * `WITH FILL INTERPOLATE` query then failed with `CORRUPTED_DATA` ("Cannot get value from Set"),
              * and the virtual row of an in-order read with `NOT_IMPLEMENTED` ("Cannot insert element into
              * Set", #111831).
              */
            const WhichDataType which_type(out.type);
            if (which_type.isSet() || which_type.isFunction())
                continue;

            before_interpolate_actions_dag.getOutputs().push_back(&before_interpolate_actions_dag.addInput(out));
        }

        for (auto & interpolate_node : interpolate_list_node.getNodes())
        {
            auto & interpolate_node_typed = interpolate_node->as<InterpolateNode &>();

            auto [nodes, correlated_subtrees] = interpolate_actions_visitor.visit(before_interpolate_actions_dag, interpolate_node_typed.getExpression());
            correlated_subtrees.assertEmpty("in expression to interpolate");
            if (nodes.size() != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Interpolate expression must return single column");
            const auto * node = nodes.front();
            const auto & target_name = interpolate_node_typed.getExpressionName();
            interpolated_column_names.emplace(target_name, node->result_name);

            /// A target alias may be the name of another column, as in `count() AS c, n AS `count()``.
            String column_name = target_name;
            for (size_t i = 0; before_interpolate_actions_dag.tryFindInOutputs(column_name); ++i)
                column_name = fmt::format("__interpolate_{}_{}", i, target_name);

            interpolate_target_column_names.emplace(target_name, column_name);
            node = &before_interpolate_actions_dag.addAlias(*node, column_name);
            before_interpolate_actions_dag.getOutputs().push_back(node);
        }

        before_interpolate_actions = std::make_shared<ActionsAndProjectInputsFlag>();
        before_interpolate_actions->dag = std::move(before_interpolate_actions_dag);
    }

    /// before_interpolate_actions is intentionally not added to the chain here;
    /// buildExpressionAnalysisResult appends it after analyzeLimitBy so the chain order matches plan execution order.
    return SortAnalysisResult{
        std::move(before_sort_actions),
        has_with_fill,
        std::move(before_interpolate_actions),
        std::move(interpolated_column_names),
        std::move(interpolate_target_column_names)};
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

/** Construct LIMIT AFTER/UNTIL analysis result.
  * The boundary expressions may reference columns that are not in the SELECT list. `LimitRangeStep` runs
  * before "Project names" in the plan but is not part of the actions chain, so no chain step requires those
  * columns and chain finalization would prune them. This adds an identity step over every current value column:
  * all of its inputs are its outputs, so the whole stream, boundary columns included, stays alive up to the
  * range step, which builds its conditions from the resulting header.
  * `IN` set and lambda `Function` placeholders are not values and are not kept: a boundary expression builds its own.
  */
LimitRangeAnalysisResult analyzeLimitRange(const QueryNode & query_node,
    const ColumnsWithTypeAndName & input_columns,
    ActionsChain & actions_chain)
{
    for (const auto & boundary_node : {query_node.getLimitAfter(), query_node.getLimitUntil()})
    {
        if (boundary_node && hasFunctionNode(boundary_node, "arrayJoin"))
            throw Exception(ErrorCodes::UNEXPECTED_EXPRESSION, "`arrayJoin` is not allowed in LIMIT AFTER/UNTIL expressions");
    }

    ColumnsWithTypeAndName value_columns;
    value_columns.reserve(input_columns.size());
    for (const auto & column : input_columns)
    {
        const WhichDataType which_type(column.type);
        if (!which_type.isSet() && !which_type.isFunction())
            value_columns.push_back(column);
    }

    auto before_limit_range_actions = std::make_shared<ActionsAndProjectInputsFlag>();
    before_limit_range_actions->dag = ActionsDAG(value_columns);
    actions_chain.addStep(std::make_unique<ActionsChainStep>(before_limit_range_actions));

    return LimitRangeAnalysisResult{std::move(before_limit_range_actions)};
}

}

PlannerExpressionsAnalysisResult buildExpressionAnalysisResult(const QueryTreeNodePtr & query_tree,
    const ColumnsWithTypeAndName & join_tree_input_columns,
    const PlannerContextPtr & planner_context,
    const PlannerQueryProcessingInfo & planner_query_processing_info,
    const NameSet & source_constants)
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
          *
          * Similarly, when ORDER BY has WITH FILL, we must preserve the ORDER BY columns because
          * the filling step added later requires them in the block header.
          *
          * Example: SELECT 1 ORDER BY toDateTime(0) WITH FILL LIMIT 1 BY 1;
          *
          * The same holds for LIMIT WITH TIES: the limit step added after LIMIT BY compares the
          * ORDER BY columns of the last row against its successors, so they have to reach it. A
          * constant ORDER BY key is otherwise dropped here, because the projection above can create
          * the constant on its own.
          *
          * Example: SELECT 1 FROM numbers(10) ORDER BY ALL LIMIT 1 BY number LIMIT 4 WITH TIES;
          */
        NameSet required_output_nodes_names;
        if (sort_analysis_result_optional.has_value()
            && (sort_analysis_result_optional->has_with_fill
                || query_node.isLimitWithTies()
                || (planner_query_processing_info.isFirstStage()
                    && planner_query_processing_info.getToStage() != QueryProcessingStage::Complete)))
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

    /** Add before_interpolate_actions to the chain AFTER the limit by step.
      * This matches the query plan execution order where Before INTERPOLATE runs after LIMIT BY.
      * Previously it was added before LIMIT BY in the chain (inside analyzeSort), which caused
      * the LIMIT BY DAG to expect interpolated column names (e.g. 's') that don't exist yet
      * in the block at that point in the plan.
      */
    if (sort_analysis_result_optional.has_value() && sort_analysis_result_optional->before_interpolate_actions)
    {
        auto & before_interpolate_actions = sort_analysis_result_optional->before_interpolate_actions;
        auto actions_step_before_interpolate = std::make_unique<ActionsChainStep>(before_interpolate_actions);
        actions_chain.addStep(std::move(actions_step_before_interpolate));
        current_output_columns = actions_chain.getLastStepAvailableOutputColumns();
    }

    /** LIMIT AFTER/UNTIL boundary columns may not be in the SELECT list. The LimitRangeStep runs before
      * "Project names" in the plan, so keep its referenced columns alive past the projection by adding a
      * dedicated step here. This must also run when producing a mergeable aggregation state on a shard:
      * the range step itself is applied later on the initiator, but the shard must still carry the boundary
      * columns in its output (the Projection would otherwise prune them, leaving the initiator unable to
      * evaluate the range over a non-selected column in a distributed query).
      */
    std::optional<LimitRangeAnalysisResult> limit_range_analysis_result_optional;
    if (query_node.hasLimitAfter() || query_node.hasLimitUntil())
    {
        limit_range_analysis_result_optional = analyzeLimitRange(query_node, current_output_columns, actions_chain);
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

    /// Keep source-constant INPUTs (`source_constants`) so `project`/`removeUnusedActions` does not
    /// fold-and-drop them; they must keep flowing as required inputs to stay in the stream at
    /// distributed stage boundaries. Literals and re-creatable alias constants stay foldable.
    std::unordered_set<const ActionsDAG::Node *> keep_inputs;
    if (!source_constants.empty())
        for (const auto * input : project_names_actions->dag.getInputs())
            if (input->column && source_constants.contains(input->result_name))
                keep_inputs.insert(input);

    if (query_node.hasInterpolate() && sort_analysis_result_optional)
    {
        auto project_names = projection_analysis_result.projection_column_names_with_display_aliases;
        const auto & interpolated_column_names = sort_analysis_result_optional->interpolated_column_names;
        /// A result column takes the interpolated values only if it is the column the target interpolates.
        for (auto & [name, alias] : project_names)
            if (auto it = interpolated_column_names.find(alias); it != interpolated_column_names.end() && it->second == name)
                name = sort_analysis_result_optional->interpolate_target_column_names.at(alias);

        project_names_actions->dag.project(project_names, keep_inputs);
    }
    else
        project_names_actions->dag.project(projection_analysis_result.projection_column_names_with_display_aliases, keep_inputs);

    project_names_actions->project_input = true;
    actions_chain.addStep(std::make_unique<ActionsChainStep>(project_names_actions));

    actions_chain.finalize(source_constants);

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

    if (limit_range_analysis_result_optional)
        expressions_analysis_result.addLimitRange(std::move(*limit_range_analysis_result_optional));

    return expressions_analysis_result;
}

}
