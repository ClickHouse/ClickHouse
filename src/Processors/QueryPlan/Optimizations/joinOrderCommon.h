#pragma once

#include <Processors/QueryPlan/Optimizations/joinOrder.h>

#include <algorithm>
#include <bit>
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
    /// Like `value`, but a key column without NDV statistics uses the row count of its relation
    /// (an upper bound of its NDV) instead. Reported by `EXPLAIN` as `estimated (NDV)` and used for the
    /// match fraction of semi/anti joins; the inner join cost model does not use it, because it says
    /// nothing about which side is the key.
    double reported_value = 1.0;
    bool reliable = false;
    bool has_equi = false;
};

using SelectivityCache = std::unordered_map<JoinActionRef, SelectivityEstimate>;

struct ColumnDistinctValues
{
    /// Number of distinct values from real statistics, or nullopt when there are none.
    std::optional<UInt64> ndv;
    /// `ndv` if known, otherwise the row count of the column's relation; 0 when neither is known.
    UInt64 upper_bound = 0;
};

/// Number of distinct values of a join-key column.
inline ColumnDistinctValues getColumnStats(
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
                return {col_it->second.num_distinct_values, col_it->second.num_distinct_values};
            return {{}, it->second->estimated_rows.value_or(0)};
        }
        return {};
    }

    const auto & relation_stat = relation_stats.at(rel_id.value());
    if (auto it = relation_stat.column_stats.find(column_name); it != relation_stat.column_stats.end())
        return {it->second.num_distinct_values, it->second.num_distinct_values};
    return {{}, relation_stat.estimated_rows.value_or(0)};
}

/// Accounts for an equality between key columns with the largest NDV `max_ndv` (0 if unknown)
/// and the largest NDV upper bound `max_ndv_upper_bound` (0 if unknown), see `ColumnDistinctValues`.
inline void applyEquiKeyDistinctValues(SelectivityEstimate & estimate, UInt64 max_ndv, UInt64 max_ndv_upper_bound)
{
    estimate.has_equi = true;
    if (max_ndv > 0)
    {
        estimate.value = std::min(estimate.value, 1.0 / static_cast<double>(max_ndv));
        estimate.reliable = true;
    }
    if (max_ndv_upper_bound > 0)
        estimate.reported_value = std::min(estimate.reported_value, 1.0 / static_cast<double>(max_ndv_upper_bound));
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

    auto lhs_ndv = getColumnStats(query_graph, dp_table, lhs.getSourceRelations(), lhs.getColumnName());
    auto rhs_ndv = getColumnStats(query_graph, dp_table, rhs.getSourceRelations(), rhs.getColumnName());
    applyEquiKeyDistinctValues(
        estimate,
        std::max(lhs_ndv.ndv.value_or(0), rhs_ndv.ndv.value_or(0)),
        std::max(lhs_ndv.upper_bound, rhs_ndv.upper_bound));
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
        estimate.reported_value = std::min(estimate.reported_value, edge_estimate.reported_value);
        estimate.reliable |= edge_estimate.reliable;
        estimate.has_equi |= edge_estimate.has_equi;
    }
    return estimate;
}

/// Expected number of rows an inner join of `lhs_rows` x `rhs_rows` keeps under `selectivity`.
/// When a side has no row estimate we cannot tell which side is the key, so the join is costed as
/// before the FK->PK heuristic: the missing row estimate counts as 1 row and the selectivity is the
/// NDV upper bound (`reported_value`), whatever `reliable` says. A join with an unknown relation must
/// not look more expensive than that, otherwise the only relations with known sizes (for example,
/// those with hash-table statistics hints) are pushed to the end of the join order, and a key whose
/// relation has a known size must keep making the join with it cheap even when the other side is a
/// sub-join of unknown size. With both sizes known, an equi join without reliable NDV is assumed to
/// be FK->PK (the smaller side is a unique key), so it keeps the larger side. Without any equi
/// condition (cross or range-only join) the result is the full product.
inline double estimateJoinedRows(
    const SelectivityEstimate & selectivity, std::optional<UInt64> lhs_rows, std::optional<UInt64> rhs_rows)
{
    double lhs = static_cast<double>(lhs_rows.value_or(1));
    double rhs = static_cast<double>(rhs_rows.value_or(1));
    if (!lhs_rows || !rhs_rows)
        return selectivity.reported_value * lhs * rhs;
    if (selectivity.reliable)
        return selectivity.value * lhs * rhs;
    if (selectivity.has_equi)
        return std::max(lhs, rhs);
    return lhs * rhs;
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
    const SelectivityEstimate & selectivity,
    JoinKind join_kind,
    JoinStrictness strictness = JoinStrictness::All)
{
    /// With one side unknown the result is unknown too: even for an inner equi join we cannot tell
    /// whether the known side is the key, and for an outer join the preserved side may be the unknown one.
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
        /// Expected fraction of preserved rows with at least one match. The selectivity is ~1/ndv,
        /// so `ndv_selectivity * other` approximates matches per preserved row; cap at 1. Without
        /// reliable statistics use the row-count upper bound of NDV (`reported_value`), which treats
        /// the join as FK->PK: e.g. a semi join of 1000 rows against 10 keys keeps ~10 rows.
        const double ndv_selectivity = selectivity.reliable ? selectivity.value : selectivity.reported_value;
        const double match_fraction = std::min(1.0, ndv_selectivity * other);
        const double kept = (strictness == JoinStrictness::Semi)
            ? preserved * match_fraction
            : preserved * (1.0 - match_fraction);
        const double semi_rows = std::max(kept, 1.0);
        if (semi_rows >= static_cast<double>(std::numeric_limits<UInt64>::max()))
            return std::numeric_limits<UInt64>::max();
        return static_cast<UInt64>(semi_rows);
    }

    double joined_rows = std::max(estimateJoinedRows(selectivity, left_rows, right_rows), 1.0);

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
    return left->cost + right->cost + estimateJoinedRows(selectivity, left->estimated_rows, right->estimated_rows);
}

/// Per-row costs relative to a hash table probe, fitted on `parallel_hash` and `full_sorting_merge` timings of `MergeTree`
/// tables (release build with jemalloc, 32 threads, x86 and aarch64). One unit is about 0.75 ns.
constexpr double HASH_BUILD_ROW_COST = 6.8;
constexpr double HASH_PROBE_ROW_COST = 1.0;
/// A merge sharded by primary key ranges runs a merge per thread; an unsharded one is a single stream.
constexpr double MERGE_SHARDED_ROW_COST = 1.4;
constexpr double MERGE_UNSHARDED_ROW_COST = 25.0;
/// Paid once per merge join. Fitted at 3.9M on aarch64 (none on x86), with a margin.
constexpr double MERGE_JOIN_FIXED_COST = 15'000'000;
/// A sharded merge reads the shards of the probe side its build side overlaps, plus this share at their edges.
constexpr double MERGE_PROBE_READ_MARGIN = 0.06;
/// A full sort costs `SORT_ROW_COST * log2(rows)` per row, with the logarithm rounded up to an integer.
constexpr double SORT_ROW_COST = 0.8;

/// An input of a sort-merge join: the cost of its plan, its rows, and whether it is sorted by the merge key.
struct MergeJoinInput
{
    double cost = 0;
    UInt64 rows = 0;
    bool sorted = false;
    /// Sorted, and a sharded merge can split it by key ranges: a table read in order by its primary key, of a type the
    /// split supports, or the result of a sharded merge on the key.
    bool shardable = false;
    /// For a sorted input of a join on several keys that is sorted by the merge key only: the number of groups of equal
    /// merge keys the other keys are sorted within. 0 when it is sorted by all of them.
    double sort_groups = 0;
};

/// The work of a hash join: building the table from the smaller input, and probing it with the `probe_fraction` of the
/// larger one that its runtime filter lets it read. DPsub adds it to the cost of a hash join when merge joins compete.
inline double computeHashJoinWork(UInt64 left_rows, UInt64 right_rows, double probe_fraction)
{
    const auto build = static_cast<double>(std::min(left_rows, right_rows));
    const auto probe = static_cast<double>(std::max(left_rows, right_rows));
    return HASH_BUILD_ROW_COST * build + HASH_PROBE_ROW_COST * probe_fraction * probe;
}

/// A sort-merge join charges its own work, on the scale of a hash join with `computeHashJoinWork`, so plans of different
/// join trees compare, whichever methods they use. A sharded merge reads `probe_fraction` of the larger input, plus a margin.
inline double computeMergeJoinCost(
    const MergeJoinInput & left, const MergeJoinInput & right, const SelectivityEstimate & selectivity, bool sharded, double probe_fraction)
{
    const double row_cost = sharded ? MERGE_SHARDED_ROW_COST : MERGE_UNSHARDED_ROW_COST;
    const double probe_read = sharded ? std::min(1.0, probe_fraction + MERGE_PROBE_READ_MARGIN) : 1.0;
    const bool left_probes = left.rows >= right.rows;
    auto input_cost = [&](const MergeJoinInput & input, double read)
    {
        const auto rows = static_cast<double>(input.rows);
        double sort = 0;
        if (!input.sorted)
            sort = SORT_ROW_COST * rows * static_cast<double>(std::bit_width(input.rows));
        else if (input.sort_groups > 0)
            sort = SORT_ROW_COST * rows * static_cast<double>(std::bit_width(static_cast<UInt64>(rows / std::max(input.sort_groups, 1.0))));
        return sort + row_cost * read * rows;
    };
    const double merge_join = MERGE_JOIN_FIXED_COST + input_cost(left, left_probes ? probe_read : 1.0)
        + input_cost(right, left_probes ? 1.0 : probe_read);
    return left.cost + right.cost + estimateJoinedRows(selectivity, left.rows, right.rows) + merge_join;
}

}
