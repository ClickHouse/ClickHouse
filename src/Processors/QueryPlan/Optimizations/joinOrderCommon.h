#pragma once

#include <Processors/QueryPlan/Optimizations/joinOrder.h>

#include <algorithm>
#include <limits>

namespace DB
{

using PlanMemo = std::unordered_map<BitSet, DPJoinEntryPtr>;
using SelectivityCache = std::unordered_map<JoinActionRef, double>;

inline size_t getColumnStats(
    const QueryGraph & query_graph,
    const PlanMemo & dp_table,
    const BitSet & rels,
    const String & column_name)
{
    const auto & relation_stats = query_graph.relation_stats;
    auto rel_id = rels.getSingleBit();
    if (!rel_id.has_value())
    {
        /// Look up NDV from the dp_table entry's column_stats (propagated through joins).
        if (auto it = dp_table.find(rels); it != dp_table.end())
        {
            auto col_it = it->second->column_stats.find(column_name);
            if (col_it != it->second->column_stats.end())
                return col_it->second.num_distinct_values;
            return it->second->estimated_rows.value_or(0);
        }
        return 0;
    }

    const auto & relation_stat = relation_stats.at(rel_id.value());
    const auto & col_stats = relation_stat.column_stats;
    if (auto it = col_stats.find(column_name); it != col_stats.end())
        return it->second.num_distinct_values;
    return relation_stat.estimated_rows.value_or(0);
}

inline double computeEqualitySelectivity(
    const QueryGraph & query_graph, const PlanMemo & dp_table, const JoinActionRef & lhs, const JoinActionRef & rhs)
{
    UInt64 lhs_ndv = getColumnStats(query_graph, dp_table, lhs.getSourceRelations(), lhs.getColumnName());
    UInt64 rhs_ndv = getColumnStats(query_graph, dp_table, rhs.getSourceRelations(), rhs.getColumnName());
    UInt64 max_ndv = std::max(lhs_ndv, rhs_ndv);
    return max_ndv > 0 ? 1.0 / static_cast<double>(max_ndv) : 1.0;
}

inline double computeSelectivity(
    const QueryGraph & query_graph,
    const PlanMemo & dp_table,
    SelectivityCache & expression_selectivity,
    const JoinActionRef & edge)
{
    auto [it, inserted] = expression_selectivity.try_emplace(edge, 1.0);
    auto & selectivity = it->second;
    if (!inserted)
        return selectivity;

    auto [op, lhs, rhs] = edge.asBinaryPredicate();

    if (op != JoinConditionOperator::Equals && op != JoinConditionOperator::NullSafeEquals)
        return 1.0;

    selectivity = computeEqualitySelectivity(query_graph, dp_table, lhs, rhs);
    return selectivity;
}

/// An OR is a join on its branches only where every branch has a key between the two sides. It then matches a pair of
/// rows only where one branch does, and only where a key common to all branches does. Elsewhere it is a filter.
/// `is_across(lhs_sources, rhs_sources)` tells whether an equality's operands are on opposite sides of the join.
template <typename IsAcross>
double computeDisjunctionSelectivity(
    const QueryGraph & query_graph, const PlanMemo & dp_table, const JoinActionRef & disjunction, const IsAcross & is_across)
{
    using Key = std::pair<JoinActionRef, double>;
    double sum = 0.0;
    std::vector<Key> common_keys;
    bool is_first_branch = true;
    for (const auto & branch : disjunction.getArguments())
    {
        std::vector<JoinActionRef> conditions = branch.isFunction(JoinConditionOperator::And)
            ? branch.getArguments() : std::vector<JoinActionRef>{branch};
        std::vector<Key> keys;
        double branch_selectivity = 1.0;
        for (const auto & condition : conditions)
        {
            auto [op, lhs, rhs] = condition.asBinaryPredicate();
            if (op != JoinConditionOperator::Equals && op != JoinConditionOperator::NullSafeEquals)
                continue;
            if (!is_across(lhs.getSourceRelations(), rhs.getSourceRelations()))
                continue;
            double key_selectivity = computeEqualitySelectivity(query_graph, dp_table, lhs, rhs);
            keys.emplace_back(condition, key_selectivity);
            branch_selectivity = std::min(branch_selectivity, key_selectivity);
        }
        if (keys.empty())
            return 1.0;
        sum += branch_selectivity;
        if (is_first_branch)
            common_keys = std::move(keys);
        else
            std::erase_if(common_keys, [&](const Key & common)
                { return std::ranges::find(keys, common.first, &Key::first) == keys.end(); });
        is_first_branch = false;
    }
    double result = std::min(1.0, sum);
    for (const auto & [_, key_selectivity] : common_keys)
        result = std::min(result, key_selectivity);
    return result;
}

template <typename IsAcross>
double computeSelectivity(
    const QueryGraph & query_graph, const PlanMemo & dp_table, SelectivityCache & expression_selectivity,
    const std::vector<JoinActionRef *> & edges, const IsAcross & is_across, bool has_equivalence_key,
    bool has_prepared_storage_side, bool is_inner_step)
{
    auto is_disjunction = [](const JoinActionRef * edge) { return edge->isFunction(JoinConditionOperator::Or); };
    auto is_key = [&](const JoinActionRef * edge)
    {
        auto [op, lhs, rhs] = edge->asBinaryPredicate();
        return (op == JoinConditionOperator::Equals || op == JoinConditionOperator::NullSafeEquals)
            && is_across(lhs.getSourceRelations(), rhs.getSourceRelations());
    };
    auto is_inequality = [&](const JoinActionRef * edge)
    {
        auto [op, lhs, rhs] = edge->asBinaryPredicate();
        return (op == JoinConditionOperator::Less || op == JoinConditionOperator::LessOrEquals
                || op == JoinConditionOperator::Greater || op == JoinConditionOperator::GreaterOrEquals)
            && is_across(lhs.getSourceRelations(), rhs.getSourceRelations());
    };
    /// At an outer join step only the conditions of its own `ON` clause are join conditions; the others filter its result.
    auto in_join_condition = [&](const JoinActionRef * edge)
    {
        return is_inner_step || query_graph.outer_join_conditions.contains(*edge);
    };
    /// An OR is costed on its branches only where the join surely runs on them: `hash` is enabled, no side is a table
    /// looked up by its key (a `Join` engine table joins on nothing else), the OR is the only OR of the join, the join
    /// has no other key, and IEJoin does not take it on two inequalities.
    const bool keyed_disjunction = query_graph.hash_join_enabled && !has_prepared_storage_side && !has_equivalence_key
        && std::ranges::count_if(edges, is_disjunction) == 1 && std::ranges::none_of(edges, is_key)
        && !(query_graph.ie_join_enabled && std::ranges::count_if(edges, is_inequality) >= 2);
    double selectivity = 1.0;
    for (const auto & edge : edges)
    {
        double edge_selectivity = keyed_disjunction && is_disjunction(edge) && in_join_condition(edge)
            ? computeDisjunctionSelectivity(query_graph, dp_table, *edge, is_across)
            : computeSelectivity(query_graph, dp_table, expression_selectivity, *edge);
        selectivity = std::min(selectivity, edge_selectivity);
    }
    return selectivity;
}

/// Single source of truth for join cardinality estimation. For outer joins the result is
/// floored by the number of rows from the preserved side(s), since those are always emitted
/// (NULL-padded when there is no match): LEFT keeps all left rows, RIGHT all right rows, FULL both.
///
/// Semi/anti joins are filters on their preserved side (LEFT preserves the left input, RIGHT the
/// right), so they never expand and must NOT be floored at the preserved side's row count. A
/// semijoin keeps the fraction of preserved rows that have >= 1 match; an antijoin keeps the rest.
/// Estimating them like outer joins (row count >= preserved side) is what makes the optimizer
/// refuse to push a selective semi/anti join down.
inline std::optional<UInt64> estimateJoinCardinality(
    std::optional<UInt64> left_rows,
    std::optional<UInt64> right_rows,
    double selectivity,
    JoinKind join_kind,
    JoinStrictness strictness = JoinStrictness::All)
{
    if (!left_rows || !right_rows)
        return {};

    double lhs = static_cast<double>(*left_rows);
    double rhs = static_cast<double>(*right_rows);

    if (strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti)
    {
        /// Preserved side is the left input for LEFT (and Inner/Cross, defensively), the right
        /// input for RIGHT; the other side is only probed for existence.
        const bool preserve_left = !isRight(join_kind);
        const double preserved = preserve_left ? lhs : rhs;
        const double other = preserve_left ? rhs : lhs;
        /// Expected fraction of preserved rows with at least one match. `selectivity` is ~1/ndv,
        /// so `selectivity * other` approximates matches per preserved row; cap at 1.
        const double match_fraction = std::min(1.0, selectivity * other);
        const double kept = (strictness == JoinStrictness::Semi)
            ? preserved * match_fraction
            : preserved * (1.0 - match_fraction);
        const double semi_rows = std::max(kept, 1.0);
        if (semi_rows >= static_cast<double>(std::numeric_limits<UInt64>::max()))
            return std::numeric_limits<UInt64>::max();
        return static_cast<UInt64>(semi_rows);
    }

    double joined_rows = std::max(selectivity * lhs * rhs, 1.0);

    if (join_kind == JoinKind::Left)
        joined_rows = std::max(joined_rows, lhs);
    if (join_kind == JoinKind::Right)
        joined_rows = std::max(joined_rows, rhs);
    if (join_kind == JoinKind::Full)
        joined_rows = std::max(joined_rows, lhs + rhs);

    /// Use >= to avoid undefined behavior when joined_rows is very close to max UInt64
    /// Due to floating point precision, a value slightly less than max when compared
    /// as double could still overflow when cast to UInt64
    if (joined_rows >= static_cast<double>(std::numeric_limits<UInt64>::max()))
        return std::numeric_limits<UInt64>::max();
    if (joined_rows < 1)
        return 1;
    return static_cast<UInt64>(joined_rows);
}

inline std::optional<UInt64> estimateJoinCardinality(
    const DPJoinEntryPtr & left,
    const DPJoinEntryPtr & right,
    double selectivity,
    JoinKind join_kind = JoinKind::Inner)
{
    return estimateJoinCardinality(left->estimated_rows, right->estimated_rows, selectivity, join_kind);
}

inline double computeJoinCost(const DPJoinEntryPtr & left, const DPJoinEntryPtr & right, double selectivity)
{
    return left->cost + right->cost
        + selectivity * static_cast<double>(left->estimated_rows.value_or(1)) * static_cast<double>(right->estimated_rows.value_or(1));
}

}
