#include <Processors/QueryPlan/Optimizations/joinRuntimeFilterKeyPath.h>

#include <DataTypes/DataTypeAggregateFunction.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAGTraversal.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/CommonSubplanStep.h>
#include <Processors/QueryPlan/CreateSetAndFilterOnTheFlyStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <array>

namespace DB::QueryPlanOptimizations
{

namespace
{

/// The filter push-down moves only conditions that are deterministic within the query.
bool isDeterministicInQuery(const ActionsDAG::Node & node)
{
    static constexpr auto changes_within_query = [](const IFunctionBase & function) { return !function.isDeterministicInScopeOfQuery(); };

    if (node.type == ActionsDAG::ActionType::ARRAY_JOIN)
        return false;
    if (node.type != ActionsDAG::ActionType::FUNCTION)
        return true;
    return node.function_base && !changes_within_query(*node.function_base)
        && !ActionsDAG::hasUnsafeHiddenLambdaBody(node, changes_within_query);
}

/// The input columns that `roots` are computed from, which a filter reading `roots` reads once it is merged into the DAG
/// that computes them. Nothing if that filter cannot be pushed down, or if it reads no column.
std::optional<NameSet> getSourceColumns(const ActionsDAG::NodeRawConstPtrs & roots)
{
    NameSet columns;
    const bool deterministic = visitReachableNodes(roots, [&](const ActionsDAG::Node * node)
    {
        if (!isDeterministicInQuery(*node))
            return VisitResult::Stop;
        if (node->type == ActionsDAG::ActionType::INPUT)
            columns.insert(node->result_name);
        return VisitResult::Continue;
    });

    if (!deterministic || columns.empty())
        return {};
    return columns;
}

bool hasColumns(const QueryPlan::Node & node, const NameSet & columns)
{
    const auto & header = *node.step->getOutputHeader();
    return std::ranges::all_of(columns, [&](const String & column) { return header.has(column); });
}

bool filterReachesInputAbove(const QueryPlan::Node * node, NameSet columns, const QueryPlanOptimizationSettings & settings);

/// Whether a filter that reads `columns` of the output of the join at `node`, pushed down into one of its inputs, reaches
/// an input estimated above `join_runtime_filter_min_probe_rows`.
bool filterReachesJoinInputAbove(
    const QueryPlan::Node & node, const JoinStepLogical & join, const NameSet & columns, const QueryPlanOptimizationSettings & settings)
{
    const auto & join_operator = join.getJoinOperator();
    const auto kind = join_operator.kind;
    if (node.children.size() != 2 || (kind != JoinKind::Inner && kind != JoinKind::Left && kind != JoinKind::Right))
        return false;

    const bool is_semi = join_operator.strictness == JoinStrictness::Semi;
    const bool can_push_to_right = join_operator.strictness != JoinStrictness::Asof
        && !typeid_cast<const JoinStepLogicalLookup *>(node.children[1]->step.get());

    /// The rules of `tryPushDownOverJoinStep`, per input (left, right). An input gets a filter on its own columns unless an
    /// outer join adds rows for its unmatched rows of the other input. A filter that reads only keys of the join is also
    /// pushed down as a filter on the equal keys, if the keys it reads count as equal for this join, to an input of which
    /// the join does not read only the matching rows: such a copy of a runtime filter could make an OUTER or ANTI join
    /// emit a row it would not.
    const std::array<bool, 2> gets_own_columns{kind != JoinKind::Right, kind != JoinKind::Left && can_push_to_right};
    const std::array<bool, 2> keys_are_equal{
        gets_own_columns[0] || (kind == JoinKind::Right && is_semi), gets_own_columns[1] || (kind == JoinKind::Left && is_semi)};
    const std::array<bool, 2> gets_equal_keys{kind != JoinKind::Right || is_semi, can_push_to_right && (kind != JoinKind::Left || is_semi)};

    /// Per input, its keys mapped to the equal keys of the other input. Only keys that are columns of the same type are
    /// followed here.
    std::array<NameToNameMap, 2> equal_keys;
    if (join.typeChangingSides().empty())
    {
        for (const auto & condition : join_operator.expression)
        {
            auto [predicate_op, lhs, rhs] = condition.asBinaryPredicate();
            if (predicate_op != JoinConditionOperator::Equals && predicate_op != JoinConditionOperator::NullSafeEquals)
                continue;
            if (lhs.fromRight() && rhs.fromLeft())
                std::swap(lhs, rhs);
            else if (!lhs.fromLeft() || !rhs.fromRight())
                continue;
            if (lhs.getNode()->type != ActionsDAG::ActionType::INPUT || rhs.getNode()->type != ActionsDAG::ActionType::INPUT
                || !lhs.getType()->equals(*rhs.getType()))
                continue;
            equal_keys[0].emplace(lhs.getColumnName(), rhs.getColumnName());
            equal_keys[1].emplace(rhs.getColumnName(), lhs.getColumnName());
        }
    }
    const bool reads_only_equal_keys = std::ranges::all_of(columns, [&](const String & column)
    {
        return (keys_are_equal[0] && equal_keys[0].contains(column)) || (keys_are_equal[1] && equal_keys[1].contains(column));
    });

    for (const size_t side : {0, 1})
    {
        const auto & input = *node.children[side];
        NameSet input_columns;
        if (gets_own_columns[side] && hasColumns(input, columns))
        {
            input_columns = columns;
        }
        else if (gets_equal_keys[side] && reads_only_equal_keys)
        {
            for (const auto & column : columns)
                input_columns.insert(equal_keys[side].contains(column) ? column : equal_keys[1 - side].at(column));
        }
        else
        {
            continue;
        }

        if (auto rows = join.getInputRowsEstimation(side == 0 ? JoinTableSide::Left : JoinTableSide::Right);
            rows && *rows > settings.join_runtime_filter_min_probe_rows)
            return true;
        if (filterReachesInputAbove(&input, std::move(input_columns), settings))
            return true;
    }
    return false;
}

/// The columns of the input of the single-input step at `node` that a filter reading `columns` of its output reads once
/// `tryPushDownFilter` pushed it down below the step. Nothing if it is not pushed down.
std::optional<NameSet> getColumnsBelowStep(const QueryPlan::Node & node, NameSet columns, const QueryPlanOptimizationSettings & settings)
{
    const IQueryPlanStep * step = node.step.get();

    /// The filter is merged with the step and then reads the columns the step computes its own from, and the columns it
    /// passes through, which are not outputs of its DAG.
    const ActionsDAG * dag = nullptr;
    if (const auto * expression = typeid_cast<const ExpressionStep *>(step))
        dag = &expression->getExpression();
    else if (const auto * filter = typeid_cast<const FilterStep *>(step))
        dag = &filter->getExpression();
    if (dag)
    {
        if (dag->hasStatefulFunctions() || dag->hasArrayJoin())
            return {};

        NameSet columns_below;
        ActionsDAG::NodeRawConstPtrs outputs;
        for (const auto & column : columns)
        {
            if (const auto * output = dag->tryFindInOutputs(column))
                outputs.push_back(output);
            else
                columns_below.insert(column);
        }

        if (!outputs.empty())
        {
            auto source_columns = getSourceColumns(outputs);
            if (!source_columns)
                return {};
            columns_below.merge(*source_columns);
        }
        return columns_below;
    }

    /// The other steps pass the columns the filter reads on unchanged, if it reads only those it may be pushed below them.
    auto if_reads_only = [&](const auto & allowed) -> std::optional<NameSet>
    {
        if (!std::ranges::all_of(columns, [&](const String & column) { return std::ranges::find(allowed, column) != allowed.end(); }))
            return {};
        return std::move(columns);
    };

    /// The keys, except for GROUPING SETS. An aggregation without keys returns a row for an empty input.
    if (const auto * aggregating = typeid_cast<const AggregatingStep *>(step))
        return aggregating->isGroupingSets() ? std::nullopt : if_reads_only(aggregating->getParams().keys);
    if (const auto * merging_aggregated = typeid_cast<const MergingAggregatedStep *>(step))
        return merging_aggregated->isGroupingSets() ? std::nullopt : if_reads_only(merging_aggregated->getParams().keys);

    /// The keys, if the step keeps a row of every group.
    if (const auto * limit_by = typeid_cast<const LimitByStep *>(step))
    {
        if (!settings.filter_push_down_below_limit_by || limit_by->getGroupOffset() != 0 || limit_by->getGroupLength() == 0)
            return {};
        return if_reads_only(limit_by->getColumns());
    }

    /// The columns that are not aggregate function states, without HAVING.
    if (const auto * totals_having = typeid_cast<const TotalsHavingStep *>(step))
    {
        if (totals_having->getActions())
            return {};
        Names keys;
        for (const auto & column : *totals_having->getInputHeaders().front())
            if (!typeid_cast<const DataTypeAggregateFunction *>(column.type.get()))
                keys.push_back(column.name);
        return if_reads_only(keys);
    }

    /// The columns that are not array joined.
    if (const auto * array_join = typeid_cast<const ArrayJoinStep *>(step))
    {
        const auto & array_joined = array_join->getColumns();
        if (std::ranges::any_of(columns, [&](const String & column) { return std::ranges::find(array_joined, column) != array_joined.end(); }))
            return {};
        return std::move(columns);
    }

    /// Every column.
    if (typeid_cast<const BuildRuntimeFilterStep *>(step) || typeid_cast<const DistinctStep *>(step)
        || typeid_cast<const SortingStep *>(step) || typeid_cast<const CreateSetAndFilterOnTheFlyStep *>(step)
        || typeid_cast<const DelayedCreatingSetsStep *>(step) || typeid_cast<const CommonSubplanStep *>(step))
        return std::move(columns);

    return {};
}

/// Whether a filter that reads `columns` of the output of `node`, pushed down into `node`, reaches a join input
/// estimated above `join_runtime_filter_min_probe_rows`.
bool filterReachesInputAbove(const QueryPlan::Node * node, NameSet columns, const QueryPlanOptimizationSettings & settings)
{
    while (true)
    {
        if (!hasColumns(*node, columns))
            return false;

        if (const auto * join = typeid_cast<const JoinStepLogical *>(node->step.get()))
            return filterReachesJoinInputAbove(*node, *join, columns, settings);

        /// A copy of the filter is pushed down to every input of a union, unless the union changes the structure of one.
        if (const auto * union_step = typeid_cast<const UnionStep *>(node->step.get()))
        {
            const auto & output = *union_step->getOutputHeader();
            if (!std::ranges::all_of(union_step->getInputHeaders(), [&](const auto & input) { return blocksHaveEqualStructure(*input, output); }))
                return false;
            return std::ranges::any_of(node->children, [&](const QueryPlan::Node * input) { return filterReachesInputAbove(input, columns, settings); });
        }

        /// The filter is pushed down to the first input, the others build sets.
        if (typeid_cast<const CreatingSetsStep *>(node->step.get()))
        {
            node = node->children.front();
            continue;
        }

        if (node->children.size() != 1)
            return false;

        auto columns_below = getColumnsBelowStep(*node, std::move(columns), settings);
        if (!columns_below)
            return false;
        columns = std::move(*columns_below);
        node = node->children.front();
    }
}

}

bool runtimeFilterReachesInputAbove(
    const QueryPlan::Node & join_node, bool check_left_does_not_contain, const QueryPlanOptimizationSettings & settings)
{
    const auto & join_operator = assert_cast<const JoinStepLogical &>(*join_node.step).getJoinOperator();

    /// The probe-side key of each filter, as `JoinStepLogical::preCalculateKeys` takes it.
    VectorWithMemoryTracking<ActionsDAG::NodeRawConstPtrs> filter_keys;
    for (const auto & condition : join_operator.expression)
    {
        auto [predicate_op, lhs, rhs] = condition.asBinaryPredicate();
        if (predicate_op != JoinConditionOperator::Equals)
            continue;
        if (lhs.fromLeft() && rhs.fromRight())
            filter_keys.push_back({lhs.getNode()});
        else if (lhs.fromRight() && rhs.fromLeft())
            filter_keys.push_back({rhs.getNode()});
    }

    /// A LEFT ANTI JOIN on several keys gets one filter on the tuple of them, see `tryAddJoinRuntimeFilter`.
    if (check_left_does_not_contain && filter_keys.size() > 1)
    {
        ActionsDAG::NodeRawConstPtrs all_keys;
        for (const auto & keys : filter_keys)
            all_keys.append_range(keys);
        filter_keys = {std::move(all_keys)};
    }

    return std::ranges::any_of(filter_keys, [&](const ActionsDAG::NodeRawConstPtrs & keys)
    {
        auto columns = getSourceColumns(keys);
        return columns && filterReachesInputAbove(join_node.children[0], std::move(*columns), settings);
    });
}

}
