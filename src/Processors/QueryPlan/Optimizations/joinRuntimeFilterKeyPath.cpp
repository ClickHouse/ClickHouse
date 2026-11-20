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
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Optimizations/RelationStatisticsEstimator.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Common/Exception.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <array>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace QueryPlanOptimizations
{

namespace
{

/// The filter push-down moves only conditions that are deterministic within the query and not stateful.
bool canBePushedDown(const ActionsDAG::Node & node)
{
    static constexpr auto blocks_push_down = [](const IFunctionBase & function)
    { return !function.isDeterministicInScopeOfQuery() || function.isStateful(); };

    if (node.type == ActionsDAG::ActionType::ARRAY_JOIN)
        return false;
    if (node.type != ActionsDAG::ActionType::FUNCTION)
        return true;
    return node.function_base && !blocks_push_down(*node.function_base)
        && !ActionsDAG::hasUnsafeHiddenLambdaBody(node, blocks_push_down);
}

/// The input columns `roots` are computed from, which a filter on `roots` reads once merged into their DAG. Nothing if such
/// a filter is not pushed down, or reads no column.
std::optional<NameSet> getSourceColumns(const ActionsDAG::NodeRawConstPtrs & roots)
{
    NameSet columns;
    const bool deterministic = visitReachableNodes(roots, [&](const ActionsDAG::Node * node)
    {
        if (!canBePushedDown(*node))
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

bool isEstimatedAbove(QueryPlan::Node & node, const QueryPlanOptimizationSettings & settings)
{
    auto rows = estimateReadRowsCount(node, RelationStatsOptions{.find_single_node_read = &settings.find_single_node_read}).estimated_rows;
    return rows && *rows > settings.join_runtime_filter_min_probe_rows;
}

bool filterReachesInputAbove(QueryPlan::Node * node, NameSet columns, bool rows_known, const QueryPlanOptimizationSettings & settings);

/// `filterReachesInputAbove` for the join at `node`.
bool filterReachesJoinInputAbove(
    QueryPlan::Node & node, const JoinStepLogical & join, const NameSet & columns, bool rows_known, const QueryPlanOptimizationSettings & settings)
{
    const auto & join_operator = join.getJoinOperator();
    const auto kind = join_operator.kind;
    if (node.children.size() != 2 || (kind != JoinKind::Inner && kind != JoinKind::Left && kind != JoinKind::Right))
        return !rows_known && isEstimatedAbove(node, settings);

    const bool is_semi = join_operator.strictness == JoinStrictness::Semi;
    const bool can_push_to_right = join_operator.strictness != JoinStrictness::Asof
        && !typeid_cast<const JoinStepLogicalLookup *>(node.children[1]->step.get());

    /// The rules of `tryPushDownOverJoinStep`, per input (left, right): whether it gets a filter on its own columns, whether
    /// its keys may be replaced by the equal keys of the other input, and whether it gets a filter on keys with them replaced
    /// by its own. A runtime filter is not copied to an input that an OUTER or ANTI join only matches against, see
    /// `splitActionsForJOINFilterPushDown`.
    const std::array<bool, 2> gets_own_columns{kind != JoinKind::Right, kind != JoinKind::Left && can_push_to_right};
    const std::array<bool, 2> keys_are_equal{
        gets_own_columns[0] || (kind == JoinKind::Right && is_semi), gets_own_columns[1] || (kind == JoinKind::Left && is_semi)};
    const std::array<bool, 2> gets_equal_keys{kind != JoinKind::Right || is_semi, can_push_to_right && (kind != JoinKind::Left || is_semi)};

    /// Per input, its keys mapped to the equal keys of the other input. Only same-typed column keys are followed here.
    /// A key equal to several same-typed columns or expressions is not followed: which of them the push-down substitutes is not
    /// known here.
    std::array<NameToNameMap, 2> equal_keys;
    if (join.typeChangingSides().empty())
    {
        std::array<NameToNameMap, 2> partners;
        std::array<NameSet, 2> ambiguous_keys;
        auto add_partner = [&](size_t side, const String & key, const String & partner)
        {
            auto [it, inserted] = partners[side].emplace(key, partner);
            if (!inserted && it->second != partner)
                ambiguous_keys[side].insert(key);
        };

        for (const auto & condition : join_operator.expression)
        {
            auto [predicate_op, lhs, rhs] = condition.asBinaryPredicate();
            if (predicate_op != JoinConditionOperator::Equals && predicate_op != JoinConditionOperator::NullSafeEquals)
                continue;
            if (lhs.fromRight() && rhs.fromLeft())
                std::swap(lhs, rhs);
            else if (!lhs.fromLeft() || !rhs.fromRight())
                continue;
            if (!lhs.getType()->equals(*rhs.getType()))
                continue;
            add_partner(0, lhs.getColumnName(), rhs.getColumnName());
            add_partner(1, rhs.getColumnName(), lhs.getColumnName());
            if (lhs.getNode()->type != ActionsDAG::ActionType::INPUT || rhs.getNode()->type != ActionsDAG::ActionType::INPUT)
                continue;
            equal_keys[0].emplace(lhs.getColumnName(), rhs.getColumnName());
            equal_keys[1].emplace(rhs.getColumnName(), lhs.getColumnName());
        }

        for (const size_t side : {0, 1})
            for (const auto & key : ambiguous_keys[side])
                equal_keys[side].erase(key);
    }
    const bool reads_only_equal_keys = std::ranges::all_of(columns, [&](const String & column)
    {
        return (keys_are_equal[0] && equal_keys[0].contains(column)) || (keys_are_equal[1] && equal_keys[1].contains(column));
    });

    bool pushed_down = false;
    for (const size_t side : {0, 1})
    {
        auto & input = *node.children[side];
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

        pushed_down = true;
        if (auto rows = join.getInputRowsEstimation(side == 0 ? JoinTableSide::Left : JoinTableSide::Right);
            rows && *rows > settings.join_runtime_filter_min_probe_rows)
            return true;
        if (filterReachesInputAbove(&input, std::move(input_columns), /*rows_known=*/ true, settings))
            return true;
    }
    return !pushed_down && !rows_known && isEstimatedAbove(node, settings);
}

/// The input columns of the single-input step at `node` that a filter on `columns` of its output reads once
/// `tryPushDownFilter` pushes it below the step, or nothing if it does not.
std::optional<NameSet> getColumnsBelowStep(const QueryPlan::Node & node, NameSet columns, const QueryPlanOptimizationSettings & settings)
{
    const IQueryPlanStep * step = node.step.get();

    /// Merged into the step, the filter reads the sources of the DAG outputs it reads, and the columns the step passes
    /// through, which are not DAG outputs.
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

    /// The other steps pass some columns on unchanged, and a filter that reads only those is pushed below them.
    auto if_reads_only = [&](const auto & allowed) -> std::optional<NameSet>
    {
        if (!std::ranges::all_of(columns, [&](const String & column) { return std::ranges::find(allowed, column) != allowed.end(); }))
            return {};
        return std::move(columns);
    };

    /// The keys, except for GROUPING SETS.
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

/// Whether a filter on `columns` of the output of `node`, pushed down into `node`, is applied to an input estimated above
/// `join_runtime_filter_min_probe_rows`. `rows_known`: the rows of `node` are those of the probe side or a join input,
/// compared already. Below a step that changes the number of rows, the step the filter stays above is estimated instead.
bool filterReachesInputAbove(QueryPlan::Node * node, NameSet columns, bool rows_known, const QueryPlanOptimizationSettings & settings)
{
    auto stays_above_node = [&] { return !rows_known && isEstimatedAbove(*node, settings); };

    while (true)
    {
        if (!hasColumns(*node, columns))
            return false;

        if (const auto * join = typeid_cast<const JoinStepLogical *>(node->step.get()))
            return filterReachesJoinInputAbove(*node, *join, columns, rows_known, settings);

        /// A copy of the filter is pushed down to every input of a union, unless the union changes the structure of one.
        if (const auto * union_step = typeid_cast<const UnionStep *>(node->step.get()))
        {
            const auto & output = *union_step->getOutputHeader();
            if (!std::ranges::all_of(union_step->getInputHeaders(), [&](const auto & input) { return blocksHaveEqualStructure(*input, output); }))
                return stays_above_node();
            return std::ranges::any_of(
                node->children, [&](QueryPlan::Node * input) { return filterReachesInputAbove(input, columns, rows_known, settings); });
        }

        /// The filter is pushed down to the first input, the others build sets.
        if (typeid_cast<const CreatingSetsStep *>(node->step.get()))
        {
            node = node->children.front();
            continue;
        }

        if (node->children.size() != 1)
            return stays_above_node();

        auto columns_below = getColumnsBelowStep(*node, std::move(columns), settings);
        if (!columns_below)
            return stays_above_node();

        const auto * transforming = dynamic_cast<const ITransformingStep *>(node->step.get());
        rows_known = rows_known && transforming && transforming->getTransformTraits().preserves_number_of_rows;
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
        return columns && filterReachesInputAbove(join_node.children[0], std::move(*columns), /*rows_known=*/ true, settings);
    });
}

void checkRuntimeFiltersKeptForKeyPath(
    QueryPlan::Node & root,
    const VectorWithMemoryTracking<RuntimeFiltersKeptForKeyPath> & kept,
    const QueryPlanOptimizationSettings & settings)
{
    if (kept.empty())
        return;

    UnorderedMapWithMemoryTracking<const IQueryPlanStep *, QueryPlan::Node *> join_nodes;
    {
        UnorderedSetWithMemoryTracking<const QueryPlan::Node *> visited;
        VectorWithMemoryTracking<QueryPlan::Node *> to_visit{&root};
        while (!to_visit.empty())
        {
            auto * node = to_visit.back();
            to_visit.pop_back();
            if (!visited.insert(node).second)
                continue;
            if (typeid_cast<const JoinStepLogical *>(node->step.get()))
                join_nodes.emplace(node->step.get(), node);
            to_visit.append_range(node->children);
        }
    }

    for (const auto & [join_step, filter_keys] : kept)
    {
        auto join_node_it = join_nodes.find(join_step);
        if (join_node_it == join_nodes.end())
            continue;
        const auto * join_node = join_node_it->second;

        const UnorderedSetWithMemoryTracking<String> keys(filter_keys.begin(), filter_keys.end());
        bool found = false;
        bool applied_to_larger_input = false;

        struct Frame
        {
            QueryPlan::Node * node;
            bool below_larger_join_input;
        };
        VectorWithMemoryTracking<Frame> stack{{join_node->children.front(), false}};
        while (!stack.empty() && !applied_to_larger_input)
        {
            auto [node, below_larger_join_input] = stack.back();
            stack.pop_back();

            if (const auto * filter = typeid_cast<const FilterStep *>(node->step.get()))
            {
                VectorWithMemoryTracking<RuntimeFilterApplication> applications;
                collectRuntimeFilterApplications(filter->getExpression(), applications);
                if (std::ranges::any_of(applications, [&](const auto & application) { return keys.contains(application.filter_key); }))
                {
                    found = true;
                    applied_to_larger_input = below_larger_join_input || isEstimatedAbove(*node->children.front(), settings);
                }
            }

            const auto * join = typeid_cast<const JoinStepLogical *>(node->step.get());
            for (size_t i = 0; i < node->children.size(); ++i)
            {
                bool below_larger_input = below_larger_join_input;
                if (join && node->children.size() == 2)
                {
                    auto rows = join->getInputRowsEstimation(i == 0 ? JoinTableSide::Left : JoinTableSide::Right);
                    below_larger_input = below_larger_input || (rows && *rows > settings.join_runtime_filter_min_probe_rows);
                }
                stack.push_back({node->children[i], below_larger_input});
            }
        }

        if (found && !applied_to_larger_input)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "The JOIN runtime filters of a probe side estimated at most {} rows were created because they are pushed down to a "
                "larger input, but they are not",
                settings.join_runtime_filter_min_probe_rows);
    }
}

}

}
