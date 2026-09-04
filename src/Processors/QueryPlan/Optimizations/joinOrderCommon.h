#pragma once

#include <Processors/QueryPlan/Optimizations/joinOrder.h>

#include <algorithm>
#include <limits>

namespace DB
{

using PlanMemo = std::unordered_map<BitSet, DPJoinEntryPtr>;

/// Result of estimating how much a set of join predicates reduces the cross product.
/// `reliable` tells whether `value` is backed by real column statistics; `has_equi` tells whether
/// an equality predicate connects the two sides at all. Both are needed because a missing NDV must
/// not be treated the same as a missing equi condition: the former still means a key lookup,
/// the latter means a cross product.
struct SelectivityEstimate
{
    double value = 1.0;
    bool reliable = false;
    bool has_equi = false;
};

using SelectivityCache = std::unordered_map<JoinActionRef, SelectivityEstimate>;

/// Number of distinct values of a join-key column, or nullopt when no real statistics are available.
inline std::optional<UInt64> getColumnStats(
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
        }
        return {};
    }

    const auto & col_stats = relation_stats.at(rel_id.value()).column_stats;
    if (auto it = col_stats.find(column_name); it != col_stats.end())
        return it->second.num_distinct_values;
    return {};
}

inline SelectivityEstimate computeSelectivity(
    const QueryGraph & query_graph,
    const PlanMemo & dp_table,
    SelectivityCache & expression_selectivity,
    const JoinActionRef & edge)
{
    auto [it, inserted] = expression_selectivity.try_emplace(edge);
    auto & estimate = it->second;
    if (!inserted)
        return estimate;

    auto [op, lhs, rhs] = edge.asBinaryPredicate();

    if (op != JoinConditionOperator::Equals && op != JoinConditionOperator::NullSafeEquals)
        return estimate;

    estimate.has_equi = true;
    auto lhs_ndv = getColumnStats(query_graph, dp_table, lhs.getSourceRelations(), lhs.getColumnName());
    auto rhs_ndv = getColumnStats(query_graph, dp_table, rhs.getSourceRelations(), rhs.getColumnName());
    UInt64 max_ndv = std::max(lhs_ndv.value_or(0), rhs_ndv.value_or(0));
    if (max_ndv > 0)
    {
        estimate.value = std::min(estimate.value, 1.0 / static_cast<double>(max_ndv));
        estimate.reliable = true;
    }
    return estimate;
}

inline SelectivityEstimate computeSelectivity(
    const QueryGraph & query_graph,
    const PlanMemo & dp_table,
    SelectivityCache & expression_selectivity,
    const std::vector<JoinActionRef *> & edges)
{
    SelectivityEstimate estimate;
    for (const auto & edge : edges)
    {
        auto edge_estimate = computeSelectivity(query_graph, dp_table, expression_selectivity, *edge);
        estimate.value = std::min(estimate.value, edge_estimate.value);
        estimate.reliable |= edge_estimate.reliable;
        estimate.has_equi |= edge_estimate.has_equi;
    }
    return estimate;
}

/// Single source of truth for join cardinality estimation. For outer joins the result is
/// floored by the number of rows from the preserved side(s), since those are always emitted
/// (NULL-padded when there is no match): LEFT keeps all left rows, RIGHT all right rows, FULL both.
inline std::optional<UInt64> estimateJoinCardinality(
    std::optional<UInt64> left_rows,
    std::optional<UInt64> right_rows,
    const SelectivityEstimate & selectivity,
    JoinKind join_kind)
{
    if (!left_rows && !right_rows)
        return {};

    double lhs = static_cast<double>(left_rows.value_or(0));
    double rhs = static_cast<double>(right_rows.value_or(0));

    double joined_rows = 1.0;
    if (!left_rows || !right_rows)
    {
        /// One side is unknown: for an equi join assume FK->PK, so the join keeps the known side.
        /// For a cross or range-only join the result is a product with an unknown multiplier;
        /// returning the known side would make the cross product look deceptively small.
        if (!selectivity.has_equi)
            return {};
        joined_rows = std::max(lhs, rhs);
    }
    else if (selectivity.reliable)
        joined_rows = std::max(selectivity.value * lhs * rhs, 1.0);
    else if (selectivity.has_equi)
        /// Equi-join, NDV unknown: assume the smaller side is a unique key (FK->PK), so the join keeps the larger side.
        joined_rows = std::max(lhs, rhs);
    else
        /// No equi condition (cross or range-only join): the result is the full product.
        joined_rows = std::max(lhs * rhs, 1.0);

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
    const SelectivityEstimate & selectivity,
    JoinKind join_kind = JoinKind::Inner)
{
    return estimateJoinCardinality(left->estimated_rows, right->estimated_rows, selectivity, join_kind);
}

inline double computeJoinCost(const DPJoinEntryPtr & left, const DPJoinEntryPtr & right, const SelectivityEstimate & selectivity)
{
    double lhs = static_cast<double>(left->estimated_rows.value_or(1));
    double rhs = static_cast<double>(right->estimated_rows.value_or(1));
    double joined_rows = 1.0;
    if (selectivity.reliable)
        joined_rows = selectivity.value * lhs * rhs;
    else if (selectivity.has_equi)
        joined_rows = std::max(lhs, rhs);
    else
        joined_rows = lhs * rhs;
    return left->cost + right->cost + joined_rows;
}

}
