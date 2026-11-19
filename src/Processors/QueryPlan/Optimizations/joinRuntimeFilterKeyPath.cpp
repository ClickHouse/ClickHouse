#include <Processors/QueryPlan/Optimizations/joinRuntimeFilterKeyPath.h>

#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAGTraversal.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
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

bool filterReachesInputAbove(const QueryPlan::Node * node, NameSet columns, UInt64 limit);

/// Whether a filter that reads `columns` of the output of the join at `node`, pushed down into one of its inputs, reaches
/// an input estimated above `limit` rows.
bool filterReachesJoinInputAbove(const QueryPlan::Node & node, const JoinStepLogical & join, const NameSet & columns, UInt64 limit)
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

        if (auto rows = join.getInputRowsEstimation(side == 0 ? JoinTableSide::Left : JoinTableSide::Right); rows && *rows > limit)
            return true;
        if (filterReachesInputAbove(&input, std::move(input_columns), limit))
            return true;
    }
    return false;
}

/// Whether a filter that reads `columns` of the output of `node`, pushed down into `node`, reaches a join input
/// estimated above `limit` rows.
bool filterReachesInputAbove(const QueryPlan::Node * node, NameSet columns, UInt64 limit)
{
    if (!hasColumns(*node, columns))
        return false;

    while (node)
    {
        if (const auto * join = typeid_cast<const JoinStepLogical *>(node->step.get()))
            return filterReachesJoinInputAbove(*node, *join, columns, limit);

        if (node->children.size() != 1)
            return false;

        const ActionsDAG * dag = nullptr;
        if (const auto * expression = typeid_cast<const ExpressionStep *>(node->step.get()))
            dag = &expression->getExpression();
        else if (const auto * filter = typeid_cast<const FilterStep *>(node->step.get()))
            dag = &filter->getExpression();
        else if (!typeid_cast<const BuildRuntimeFilterStep *>(node->step.get()))
            return false;

        /// The filter is merged with the step and then reads the columns the step computes its own from.
        if (dag)
        {
            if (dag->hasStatefulFunctions() || dag->hasArrayJoin())
                return false;

            ActionsDAG::NodeRawConstPtrs outputs;
            for (const auto & column : columns)
            {
                const auto * output = dag->tryFindInOutputs(column);
                if (!output)
                    return false;
                outputs.push_back(output);
            }

            auto source_columns = getSourceColumns(outputs);
            if (!source_columns)
                return false;
            columns = std::move(*source_columns);
        }

        node = node->children.front();
        if (!hasColumns(*node, columns))
            return false;
    }
    return false;
}

}

bool runtimeFilterReachesInputAbove(const QueryPlan::Node & join_node, bool check_left_does_not_contain, UInt64 limit)
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
        return columns && filterReachesInputAbove(join_node.children[0], std::move(*columns), limit);
    });
}

}
