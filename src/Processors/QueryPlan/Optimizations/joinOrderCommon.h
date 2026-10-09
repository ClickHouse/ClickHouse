#pragma once

#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>
#include <Interpreters/ActionsDAG.h>

#include <algorithm>
#include <limits>

namespace DB
{

using PlanMemo = std::unordered_map<BitSet, DPJoinEntryPtr>;
using QueryPlanOptimizations::KeyDistinctValues;

/// What one join edge contributes to the keys of a join, computed once per edge: the distinct
/// values of its two sides, the key's domain, and the relations of the left side, to tell which
/// side of a join split each belongs to. Nothing for a non-equality edge.
struct EdgeSelectivity
{
    bool is_equality = false;
    KeyDistinctValues lhs_distinct_values;
    KeyDistinctValues rhs_distinct_values;
    UInt64 domain_distinct_values = 0;
    BitSet lhs_relations;
};
using SelectivityCache = std::unordered_map<JoinActionRef, EdgeSelectivity>;

/// The equality predicates of a join combined: the most selective key decides.
struct JoinKeyEstimate
{
    /// One over the largest key NDV, a key without an NDV counting its relation's rows. `EXPLAIN` reports it.
    double selectivity = 1.0;
    /// The same over the NDVs from statistics only; sizes the join when some key has one.
    double statistics_selectivity = 1.0;
    /// An equality connects the two sides.
    bool has_equality = false;
    /// Some key NDV comes from statistics.
    bool from_statistics = false;
    /// The probe side of a semi join was built from the preserved side's key values (a decorrelated
    /// subquery joined with the distinct correlation keys), so every probe key is a preserved key:
    /// the preserved side matches in the share of its keys the probe side has, whatever the key
    /// domain, and a row count stands in for a missing NDV. An anti join does not take this share:
    /// its complement of a share near one is unreliable, and the domain-based fraction errs towards
    /// keeping rows.
    bool probe_keys_within_preserved_side = false;
    std::optional<double> left_within_fraction = {};
    std::optional<double> right_within_fraction = {};
    /// Smallest share of a side's key values the other side also has, see `keyContainment`; known
    /// when both NDVs come from statistics, since a row count says nothing about shared values.
    std::optional<double> left_match_fraction;
    std::optional<double> right_match_fraction;

    void add(const KeyDistinctValues & left, const KeyDistinctValues & right, UInt64 domain_distinct_values)
    {
        has_equality = true;
        const UInt64 largest = std::max(left.orRows(), right.orRows());
        if (largest == 0)
            return;
        selectivity = std::min(selectivity, 1.0 / static_cast<double>(largest));
        if (const UInt64 largest_from_statistics = std::max(left.from_statistics, right.from_statistics))
        {
            statistics_selectivity = std::min(statistics_selectivity, 1.0 / static_cast<double>(largest_from_statistics));
            from_statistics = true;
        }
        if (probe_keys_within_preserved_side)
        {
            const double left_values = static_cast<double>(left.orRows());
            const double right_values = static_cast<double>(right.orRows());
            left_within_fraction = std::min(left_within_fraction.value_or(1.0), std::min(1.0, right_values / left_values));
            right_within_fraction = std::min(right_within_fraction.value_or(1.0), std::min(1.0, left_values / right_values));
        }
        if (!left.from_statistics || !right.from_statistics)
            return;
        left_match_fraction = std::min(left_match_fraction.value_or(1.0),
            QueryPlanOptimizations::keyContainment(left.from_statistics, right.from_statistics, domain_distinct_values).value_or(1.0));
        right_match_fraction = std::min(right_match_fraction.value_or(1.0),
            QueryPlanOptimizations::keyContainment(right.from_statistics, left.from_statistics, domain_distinct_values).value_or(1.0));
    }
};

/// Rows the cost model counts for an entry: its estimate; for a join without one, its selectivity
/// times its inputs' values (its bound is the product of its inputs' bounds, which would grow with
/// every join); for a relation without one, its row bound, else one row.
inline double searchRows(const DPJoinEntryPtr & entry, const QueryGraph & query_graph)
{
    if (entry->estimated_rows)
        return static_cast<double>(*entry->estimated_rows);
    if (!entry->isLeaf())
        return entry->selectivity * searchRows(entry->left, query_graph) * searchRows(entry->right, query_graph);
    if (entry->max_rows)
        return static_cast<double>(*entry->max_rows);
    return 1;
}

/// Distinct values of a column as the join order optimizer knows it: from the relation's
/// statistics and rows, or from the DP entry of a joined relation set, whose column statistics
/// the joins below narrowed and whose rows are the ones its cost counts, see `searchRows`.
inline KeyDistinctValues getColumnStats(
    const QueryGraph & query_graph,
    const PlanMemo & dp_table,
    const BitSet & rels,
    const String & column_name)
{
    auto rel_id = rels.getSingleBit();
    if (!rel_id.has_value())
    {
        KeyDistinctValues result;
        if (auto it = dp_table.find(rels); it != dp_table.end())
        {
            if (auto col_it = it->second->column_stats.find(column_name); col_it != it->second->column_stats.end())
                result.from_statistics = col_it->second.num_distinct_values;
            result.rows = roundToRowCount(searchRows(it->second, query_graph));
        }
        return result;
    }
    return QueryPlanOptimizations::keyDistinctValues(query_graph.relation_stats.at(rel_id.value()), column_name);
}

/// Domain of a column as the join order optimizer knows it; zero when unknown.
inline UInt64 getColumnDomain(
    const QueryGraph & query_graph,
    const PlanMemo & dp_table,
    const BitSet & rels,
    const String & column_name)
{
    auto rel_id = rels.getSingleBit();
    if (!rel_id.has_value())
    {
        if (auto it = dp_table.find(rels); it != dp_table.end())
            if (auto col_it = it->second->column_stats.find(column_name); col_it != it->second->column_stats.end())
                return col_it->second.domain_distinct_values;
        return 0;
    }
    return QueryPlanOptimizations::keyDomain(query_graph.relation_stats.at(rel_id.value()), column_name);
}

inline const EdgeSelectivity & computeEdgeSelectivity(
    const QueryGraph & query_graph,
    const PlanMemo & dp_table,
    SelectivityCache & expression_selectivity,
    const JoinActionRef & edge)
{
    auto [it, inserted] = expression_selectivity.try_emplace(edge);
    auto & result = it->second;
    if (!inserted)
        return result;

    auto [op, lhs, rhs] = edge.asBinaryPredicate();
    if (op != JoinConditionOperator::Equals && op != JoinConditionOperator::NullSafeEquals)
        return result;

    result.is_equality = true;
    result.lhs_distinct_values = getColumnStats(query_graph, dp_table, lhs.getSourceRelations(), lhs.getColumnName());
    result.rhs_distinct_values = getColumnStats(query_graph, dp_table, rhs.getSourceRelations(), rhs.getColumnName());
    result.domain_distinct_values = std::max(
        getColumnDomain(query_graph, dp_table, lhs.getSourceRelations(), lhs.getColumnName()),
        getColumnDomain(query_graph, dp_table, rhs.getSourceRelations(), rhs.getColumnName()));
    result.lhs_relations = lhs.getSourceRelations();
    return result;
}

/// Adds the equality edges between `left` and `right` to `keys`.
inline void collectEdgeKeys(
    const QueryGraph & query_graph,
    const PlanMemo & dp_table,
    SelectivityCache & expression_selectivity,
    const std::vector<JoinActionRef *> & edges,
    const BitSet & left,
    JoinKeyEstimate & keys)
{
    for (const auto * edge : edges)
    {
        const auto & edge_selectivity = computeEdgeSelectivity(query_graph, dp_table, expression_selectivity, *edge);
        if (!edge_selectivity.is_equality)
            continue;
        const bool lhs_on_left = isSubsetOf(edge_selectivity.lhs_relations, left);
        keys.add(
            lhs_on_left ? edge_selectivity.lhs_distinct_values : edge_selectivity.rhs_distinct_values,
            lhs_on_left ? edge_selectivity.rhs_distinct_values : edge_selectivity.lhs_distinct_values,
            edge_selectivity.domain_distinct_values);
    }
}

/// The input whose rows a join emits at most once each: the preserved side of a semi or anti
/// join, the side that takes one match per row in an `ANY` or `ASOF` join (the left side, or
/// the right side of a `RIGHT` join and of a right-any join). None for the other strictnesses.
inline std::optional<bool> singleMatchSideIsLeft(JoinKind join_kind, JoinStrictness strictness)
{
    switch (strictness)
    {
        case JoinStrictness::Semi:
        case JoinStrictness::Anti:
        case JoinStrictness::Any:
        case JoinStrictness::Asof:
            return !isRight(join_kind);
        case JoinStrictness::RightAny:
            return join_kind == JoinKind::Left;
        default:
            return {};
    }
}

/// Rows a join cannot exceed, from the bounds of its inputs by the join kind and strictness;
/// unknown when a needed input bound is. The arithmetic saturates, and a saturated bound is still a bound.
inline std::optional<UInt64> estimateJoinRowsUpperBound(
    std::optional<UInt64> left_max, std::optional<UInt64> right_max, JoinKind join_kind, JoinStrictness strictness)
{
    const bool left_empty = left_max && *left_max == 0;
    const bool right_empty = right_max && *right_max == 0;

    if (join_kind == JoinKind::Paste)
    {
        if (left_max && right_max)
            return std::min(*left_max, *right_max);
        return left_max ? left_max : right_max;
    }

    if (const auto single_side_is_left = singleMatchSideIsLeft(join_kind, strictness))
    {
        const auto & single_max = *single_side_is_left ? left_max : right_max;
        const bool other_empty = *single_side_is_left ? right_empty : left_empty;
        /// Only an anti join and an outer any or asof join keep rows the other side does not match.
        const bool keeps_unmatched = strictness == JoinStrictness::Anti
            || (strictness != JoinStrictness::Semi && join_kind != JoinKind::Inner);
        if (other_empty && !keeps_unmatched)
            return 0;
        return single_max;
    }

    /// An empty side decides on its own: an inner or cross join is empty, an outer join keeps the
    /// other side when that is the preserved one.
    if (left_empty || right_empty)
    {
        if (join_kind == JoinKind::Left)
            return left_empty ? std::optional<UInt64>(0) : left_max;
        if (join_kind == JoinKind::Right)
            return right_empty ? std::optional<UInt64>(0) : right_max;
        if (join_kind == JoinKind::Full)
            return left_empty ? right_max : left_max;
        return 0;
    }

    if (!left_max || !right_max)
        return {};

    switch (join_kind)
    {
        case JoinKind::Left:
            return saturatingMul(*left_max, std::max<UInt64>(1, *right_max));
        case JoinKind::Right:
            return saturatingMul(*right_max, std::max<UInt64>(1, *left_max));
        case JoinKind::Full:
            return saturatingAdd(saturatingMul(*left_max, std::max<UInt64>(1, *right_max)), *right_max);
        default:
            return saturatingMul(*left_max, *right_max);
    }
}

/// Rows an inner join of `lhs` and `rhs` rows keeps: the product over the key NDV from statistics;
/// without one, an equality is taken as a foreign key meeting a primary key and keeps the larger
/// side; without an equality, the product.
inline double estimateJoinedRows(const JoinKeyEstimate & keys, double lhs, double rhs)
{
    if (keys.from_statistics)
        return keys.statistics_selectivity * lhs * rhs;
    if (keys.has_equality)
        return std::max(lhs, rhs);
    return lhs * rhs;
}

/// Join cardinality for every planner, by the join kind and strictness, at least one row. An empty
/// side decides the result alone, as the bound does.
inline std::optional<UInt64> estimateJoinCardinality(
    std::optional<UInt64> left_rows,
    std::optional<UInt64> right_rows,
    const JoinKeyEstimate & keys,
    JoinKind join_kind,
    JoinStrictness strictness = JoinStrictness::All)
{
    const bool left_empty = left_rows && *left_rows == 0;
    const bool right_empty = right_rows && *right_rows == 0;
    if (join_kind == JoinKind::Paste || left_empty || right_empty)
        return estimateJoinRowsUpperBound(left_rows, right_rows, join_kind, strictness);

    if (!left_rows || !right_rows)
        return {};

    double lhs = static_cast<double>(*left_rows);
    double rhs = static_cast<double>(*right_rows);

    if (strictness == JoinStrictness::Any || strictness == JoinStrictness::RightAny || strictness == JoinStrictness::Asof)
    {
        const bool single_side_is_left = *singleMatchSideIsLeft(join_kind, strictness);
        const double single_side = single_side_is_left ? lhs : rhs;
        /// An outer join emits every row of that side once; an inner join only the matched ones.
        if (join_kind != JoinKind::Inner)
            return roundToRowCount(single_side);
        return roundToRowCount(std::max(std::min(estimateJoinedRows(keys, lhs, rhs), single_side), 1.0));
    }

    if (strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti)
    {
        /// Preserved side is the left input for LEFT (and Inner/Cross, defensively), the right
        /// input for RIGHT; the other side is only probed for existence.
        const bool preserve_left = !isRight(join_kind);
        const double preserved = preserve_left ? lhs : rhs;
        const double other = preserve_left ? rhs : lhs;
        /// Fraction of preserved rows with a match: the share of the preserved key values the other
        /// side has when that key's NDV is known, else the other side's rows per key value, capped at 1.
        const auto & within_fraction = preserve_left ? keys.left_within_fraction : keys.right_within_fraction;
        const auto & known_fraction = (strictness == JoinStrictness::Semi && within_fraction)
            ? within_fraction
            : (preserve_left ? keys.left_match_fraction : keys.right_match_fraction);
        const double rows_per_key = keys.from_statistics ? keys.statistics_selectivity : keys.selectivity;
        const double match_fraction = known_fraction.value_or(std::min(1.0, rows_per_key * other));
        const double kept = (strictness == JoinStrictness::Semi)
            ? preserved * match_fraction
            : preserved * (1.0 - match_fraction);
        return roundToRowCount(std::max(kept, 1.0));
    }

    double joined_rows = std::max(estimateJoinedRows(keys, lhs, rhs), 1.0);

    if (join_kind == JoinKind::Left)
        joined_rows = std::max(joined_rows, lhs);
    if (join_kind == JoinKind::Right)
        joined_rows = std::max(joined_rows, rhs);
    /// Every row of both sides appears at least once, matched or padded, so the result is at least
    /// the larger side. The sum of both sides is not a lower bound: matched rows appear once.
    if (join_kind == JoinKind::Full)
        joined_rows = std::max({joined_rows, lhs, rhs});

    return roundToRowCount(std::max(joined_rows, 1.0));
}

/// A join whose selectivity is given as one number, taken as backed by statistics;
/// `preserved_match_fraction` is the preserved side's share of matched key values, when known.
inline std::optional<UInt64> estimateJoinCardinality(
    std::optional<UInt64> left_rows,
    std::optional<UInt64> right_rows,
    double selectivity,
    JoinKind join_kind,
    JoinStrictness strictness = JoinStrictness::All,
    std::optional<double> preserved_match_fraction = {})
{
    JoinKeyEstimate keys;
    keys.selectivity = selectivity;
    keys.statistics_selectivity = selectivity;
    keys.has_equality = true;
    keys.from_statistics = true;
    keys.left_match_fraction = preserved_match_fraction;
    keys.right_match_fraction = preserved_match_fraction;
    return estimateJoinCardinality(left_rows, right_rows, keys, join_kind, strictness);
}

inline std::optional<UInt64> estimateJoinCardinality(
    const DPJoinEntryPtr & left,
    const DPJoinEntryPtr & right,
    const JoinKeyEstimate & keys,
    JoinKind join_kind,
    JoinStrictness strictness)
{
    return estimateJoinCardinality(left->estimated_rows, right->estimated_rows, keys, join_kind, strictness);
}

/// Rows a join emits for the cost model: `estimateJoinedRows` of two estimated inputs, otherwise
/// the reported selectivity times the inputs' search values.
inline double joinedRowsSearchValue(const QueryGraph & query_graph, const DPJoinEntryPtr & left, const DPJoinEntryPtr & right, const JoinKeyEstimate & keys)
{
    if (left->estimated_rows && right->estimated_rows)
        return estimateJoinedRows(keys, static_cast<double>(*left->estimated_rows), static_cast<double>(*right->estimated_rows));
    return keys.selectivity * searchRows(left, query_graph) * searchRows(right, query_graph);
}

/// The cost of a join: both inputs' costs plus the rows it emits.
inline double computeJoinCost(const QueryGraph & query_graph, const DPJoinEntryPtr & left, const DPJoinEntryPtr & right, const JoinKeyEstimate & keys)
{
    return left->cost + right->cost + joinedRowsSearchValue(query_graph, left, right, keys);
}

/// The estimate of a join of two relations, by the shared formulas: the key selectivity and
/// containment from the relations' column statistics, the rows and the bound by the join kind
/// and strictness, and the output column statistics with the key NDVs narrowed by the join.
struct JoinEstimate
{
    std::optional<UInt64> rows;
    std::optional<UInt64> max_rows;
    double selectivity = 1.0;
    std::unordered_map<String, ColumnStats> column_stats;
};

inline JoinEstimate estimateJoin(
    const RelationStats & left, const RelationStats & right, const JoinOperator & join_operator, bool probe_keys_within_preserved_side = false)
{
    auto left_entry = std::make_shared<DPJoinEntry>(0, left);
    auto right_entry = std::make_shared<DPJoinEntry>(1, right);

    JoinKeyEstimate keys;
    keys.probe_keys_within_preserved_side = probe_keys_within_preserved_side;
    for (const auto & predicate : join_operator.expression)
    {
        auto [op, lhs, rhs] = predicate.asBinaryPredicate();
        if (op != JoinConditionOperator::Equals && op != JoinConditionOperator::NullSafeEquals)
            continue;
        if (lhs.fromRight() && rhs.fromLeft())
            std::swap(lhs, rhs);
        if (!lhs.fromLeft() || !rhs.fromRight())
            continue;
        keys.add(
            QueryPlanOptimizations::keyDistinctValues(left, lhs.getColumnName()),
            QueryPlanOptimizations::keyDistinctValues(right, rhs.getColumnName()),
            std::max(QueryPlanOptimizations::keyDomain(left, lhs.getColumnName()), QueryPlanOptimizations::keyDomain(right, rhs.getColumnName())));
    }

    const auto rows = estimateJoinCardinality(left_entry, right_entry, keys, join_operator.kind, join_operator.strictness);
    DPJoinEntry joined(left_entry, right_entry, /*cost*/ 0.0, keys.selectivity, rows, join_operator);
    return JoinEstimate{
        .rows = joined.estimated_rows,
        .max_rows = joined.max_rows,
        .selectivity = keys.selectivity,
        .column_stats = std::move(joined.column_stats)};
}

}
