#pragma once

#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>
#include <Interpreters/ActionsDAG.h>

#include <algorithm>
#include <cmath>
#include <functional>
#include <limits>
#include <optional>
#include <vector>

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
    /// Some key NDV could be too low (rows without statistics can add values), so the rows rest on an assumption.
    bool imprecise_distinct_values = false;
    /// The probe side of a semi join was built from the preserved side's key values (a decorrelated
    /// subquery joined with the distinct correlation keys), so every probe key is a preserved key:
    /// the preserved side matches in the share of its keys the probe side has, whatever the key
    /// domain, and a row count stands in for a missing NDV. An anti join does not take this share:
    /// its complement of a share near one is unreliable, and the domain-based fraction errs towards
    /// keeping rows.
    bool probe_keys_within_preserved_side = false;
    std::optional<double> left_within_fraction = {};
    std::optional<double> right_within_fraction = {};
    /// With `probe_keys_within_preserved_side`, the share of the key domain the other side's values
    /// take: the fraction an anti join uses.
    std::optional<double> left_domain_fraction = {};
    std::optional<double> right_domain_fraction = {};
    /// Smallest share of a side's key values the other side also has, see `keyContainment`; known
    /// when both NDVs come from statistics, since a row count says nothing about shared values.
    std::optional<double> left_match_fraction;
    std::optional<double> right_match_fraction;

    void add(const KeyDistinctValues & left, const KeyDistinctValues & right, UInt64 domain_distinct_values)
    {
        has_equality = true;
        imprecise_distinct_values |= left.imprecise || right.imprecise;
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
            left_within_fraction = std::min({left_within_fraction.value_or(1.0), 1.0, right_values / left_values});
            right_within_fraction = std::min({right_within_fraction.value_or(1.0), 1.0, left_values / right_values});
        }
        if (!left.from_statistics || !right.from_statistics)
            return;
        if (probe_keys_within_preserved_side && domain_distinct_values >= std::max(left.from_statistics, right.from_statistics))
        {
            const double domain = static_cast<double>(domain_distinct_values);
            left_domain_fraction = std::min(left_domain_fraction.value_or(1.0), static_cast<double>(right.from_statistics) / domain);
            right_domain_fraction = std::min(right_domain_fraction.value_or(1.0), static_cast<double>(left.from_statistics) / domain);
        }
        left_match_fraction = std::min(left_match_fraction.value_or(1.0),
            QueryPlanOptimizations::keyContainment(left.from_statistics, right.from_statistics, domain_distinct_values).value_or(1.0));
        right_match_fraction = std::min(right_match_fraction.value_or(1.0),
            QueryPlanOptimizations::keyContainment(right.from_statistics, left.from_statistics, domain_distinct_values).value_or(1.0));
    }

    /// Keys counted once each for a key of several columns, see `countIndependentKey`.
    size_t independent_keys = 0;
    bool independent_keys_from_statistics = true;
    /// The NDVs of the independent keys on each side.
    std::vector<double> left_key_distinct_values = {};
    std::vector<double> right_key_distinct_values = {};
    /// Hashes of the columns of the independent keys on each side.
    std::vector<UInt64> left_key_columns = {};
    std::vector<UInt64> right_key_columns = {};

    /// Counts a key added with `add` for the estimate of a key of several columns. A key that shares a
    /// column with one counted before is not independent of it and is not counted.
    void countIndependentKey(
        const KeyDistinctValues & left, const KeyDistinctValues & right,
        const std::vector<UInt64> & left_columns, const std::vector<UInt64> & right_columns)
    {
        const auto seen = [](const std::vector<UInt64> & counted, const std::vector<UInt64> & columns)
        { return std::ranges::any_of(columns, [&](UInt64 column) { return std::ranges::find(counted, column) != counted.end(); }); };
        if (seen(left_key_columns, left_columns) || seen(right_key_columns, right_columns))
            return;
        left_key_columns.insert(left_key_columns.end(), left_columns.begin(), left_columns.end());
        right_key_columns.insert(right_key_columns.end(), right_columns.begin(), right_columns.end());
        ++independent_keys;
        if (!left.from_statistics || !right.from_statistics)
        {
            independent_keys_from_statistics = false;
            return;
        }
        left_key_distinct_values.push_back(static_cast<double>(left.from_statistics));
        right_key_distinct_values.push_back(static_cast<double>(right.from_statistics));
    }

    /// For two or more independent keys with statistics: the geometric mean of the selectivity of the
    /// most selective key and of the key combinations. The combinations are those of the side with
    /// more of them, each side having the product of its key NDVs, at most its `rows`. The single key
    /// counts too many rows when the other keys also filter, and the combinations too few when the key
    /// columns are correlated. The true size usually lies between the two, and the mean is then at most
    /// the square root of their ratio off either way.
    ///
    /// The setting `query_plan_join_multi_key_model` replaces the mean for experiments: the
    /// combinations of each side then follow `estimateDistinctCombinations`, and `largest` keeps the single key.
    /// `postgres` multiplies the selectivities of the keys, as PostgreSQL does for the clauses of a join.
    std::optional<double> multiColumnSelectivity(double left_rows, double right_rows) const
    {
        if (independent_keys < 2 || !independent_keys_from_statistics)
            return {};
        const auto model = QueryPlanOptimizations::getMultiColumnDistinctModel(QueryPlanOptimizations::MultiColumnDistinctModelConsumer::Join);
        if (model == QueryPlanOptimizations::MultiColumnDistinctModel::Largest
            || model == QueryPlanOptimizations::MultiColumnDistinctModel::PerTable)
            return {};
        if (model == QueryPlanOptimizations::MultiColumnDistinctModel::Postgres)
        {
            double product_selectivity = 1.0;
            for (size_t key = 0; key < left_key_distinct_values.size(); ++key)
                product_selectivity /= std::max(left_key_distinct_values[key], right_key_distinct_values[key]);
            /// At least one row of the join.
            return std::min(statistics_selectivity, std::max(product_selectivity, 1.0 / std::max(1.0, left_rows * right_rows)));
        }
        const bool keep_mean = model == QueryPlanOptimizations::MultiColumnDistinctModel::Default
            || model == QueryPlanOptimizations::MultiColumnDistinctModel::GeometricMean;
        const auto side_model = keep_mean ? QueryPlanOptimizations::MultiColumnDistinctModel::Product : model;
        const double combinations = std::max(
            QueryPlanOptimizations::estimateDistinctCombinations(left_key_distinct_values, left_rows, side_model),
            QueryPlanOptimizations::estimateDistinctCombinations(right_key_distinct_values, right_rows, side_model));
        if (combinations < 1.0)
            return {};
        const double combinations_selectivity = std::min(statistics_selectivity, 1.0 / combinations);
        if (!keep_mean)
            return combinations_selectivity;
        return std::sqrt(statistics_selectivity * combinations_selectivity);
    }

    /// Whether the estimate for inputs of `left_rows` and `right_rows` rests on the independence of the
    /// key columns, see `multiColumnSelectivity`.
    bool assumesIndependentKeys(double left_rows, double right_rows) const
    {
        const auto multi_column = multiColumnSelectivity(left_rows, right_rows);
        return multi_column && *multi_column < statistics_selectivity;
    }
};

/// Identifies a key column for `JoinKeyEstimate::countIndependentKey`.
inline UInt64 keyColumnId(const String & column_name)
{
    return std::hash<String>{}(column_name);
}

/// The input whose rows a join emits at most once each: the preserved side of a semi or anti
/// join, the side that takes one match per row in an `ANY` or `ASOF` join (the left side, or
/// the right side of a `RIGHT` join and of a right-any join). None for the other strictnesses
/// and for a `FULL` join, which also emits the unmatched rows of the other side.
inline std::optional<bool> singleMatchSideIsLeft(JoinKind join_kind, JoinStrictness strictness)
{
    if (join_kind == JoinKind::Full)
        return {};
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

/// Rows a join emits for the cost model when an input has no estimate: `selectivity` times the inputs'
/// search values, counted by the join kind and strictness as `estimateJoinCardinality` counts them. A
/// `SEMI` or `ANTI` join keeps a share of its preserved side; an `ANY` or `ASOF` join emits each row of
/// one side at most once, and every row of it when the join is outer.
inline double joinSearchRows(double selectivity, double left, double right, JoinKind join_kind, JoinStrictness strictness)
{
    if (strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti)
    {
        const bool preserve_left = !isRight(join_kind);
        const double preserved = preserve_left ? left : right;
        const double other = preserve_left ? right : left;
        const double match_fraction = std::min(1.0, selectivity * other);
        return strictness == JoinStrictness::Semi ? preserved * match_fraction : preserved * (1.0 - match_fraction);
    }
    if (const auto single_side_is_left = singleMatchSideIsLeft(join_kind, strictness))
    {
        const double single_side = *single_side_is_left ? left : right;
        if (join_kind != JoinKind::Inner)
            return single_side;
        return std::min(selectivity * left * right, single_side);
    }
    /// An outer join emits every row of its preserved sides at least once.
    const double joined = selectivity * left * right;
    if (join_kind == JoinKind::Left)
        return std::max(joined, left);
    if (join_kind == JoinKind::Right)
        return std::max(joined, right);
    if (join_kind == JoinKind::Full)
        return std::max({joined, left, right});
    return joined;
}

/// Rows the cost model counts for an entry: its estimate; for a join without one, `joinSearchRows` of
/// its inputs' values (its bound is the product of its inputs' bounds, which would grow with every
/// join); for a relation without one, its row bound, else one row.
inline double searchRows(const DPJoinEntryPtr & entry, const QueryGraph & query_graph)
{
    if (entry->estimated_rows)
        return static_cast<double>(*entry->estimated_rows);
    if (!entry->isLeaf())
        return joinSearchRows(entry->selectivity, searchRows(entry->left, query_graph), searchRows(entry->right, query_graph),
            entry->join_operator.kind, entry->join_operator.strictness);
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
            {
                result.from_statistics = col_it->second.num_distinct_values;
                result.imprecise = col_it->second.hasImpreciseDistinctValues();
            }
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
        const auto & left_values = lhs_on_left ? edge_selectivity.lhs_distinct_values : edge_selectivity.rhs_distinct_values;
        const auto & right_values = lhs_on_left ? edge_selectivity.rhs_distinct_values : edge_selectivity.lhs_distinct_values;
        keys.add(left_values, right_values, edge_selectivity.domain_distinct_values);

        /// An edge inside an equivalence class is counted as a key of several columns with its class.
        auto [op, lhs, rhs] = edge->asBinaryPredicate();
        const auto equivalence_class = query_graph.column_equivalences.getClass(lhs);
        if (equivalence_class && std::ranges::find(*equivalence_class, rhs) != equivalence_class->end())
            continue;
        const UInt64 lhs_column = keyColumnId(lhs.getColumnName());
        const UInt64 rhs_column = keyColumnId(rhs.getColumnName());
        keys.countIndependentKey(left_values, right_values, {lhs_on_left ? lhs_column : rhs_column}, {lhs_on_left ? rhs_column : lhs_column});
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
        return keys.multiColumnSelectivity(lhs, rhs).value_or(keys.statistics_selectivity) * lhs * rhs;
    if (keys.has_equality)
        return std::max(lhs, rhs);
    return lhs * rhs;
}

/// Whether the rows of a join of inputs with these estimates rest on an assumption, so they are a guess: an equality
/// without key NDVs is taken as a foreign key meeting a primary key (see `estimateJoinedRows`), and the key columns
/// of several keys as independent (see `JoinKeyEstimate::multiColumnSelectivity`).
inline bool joinRowsRestOnAssumption(const JoinKeyEstimate & keys, std::optional<UInt64> left_rows, std::optional<UInt64> right_rows)
{
    if (!left_rows || !right_rows)
        return false;
    if (keys.imprecise_distinct_values)
        return true;
    if (!keys.from_statistics)
        return keys.has_equality;
    return keys.assumesIndependentKeys(static_cast<double>(*left_rows), static_cast<double>(*right_rows));
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

    const auto single_side_is_left = singleMatchSideIsLeft(join_kind, strictness);
    if (single_side_is_left && strictness != JoinStrictness::Semi && strictness != JoinStrictness::Anti)
    {
        const double single_side = *single_side_is_left ? lhs : rhs;
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
        const auto & domain_fraction = preserve_left ? keys.left_domain_fraction : keys.right_domain_fraction;
        const auto & known_fraction = (strictness == JoinStrictness::Semi && within_fraction)
            ? within_fraction
            : ((strictness == JoinStrictness::Anti && domain_fraction)
                ? domain_fraction
                : (preserve_left ? keys.left_match_fraction : keys.right_match_fraction));
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

/// Rows a join emits for the cost model, for every solver: with two estimated inputs, the estimate of
/// the join; otherwise `joinSearchRows` over the inputs' search values.
inline double joinedRowsSearchValue(
    const JoinKeyEstimate & keys,
    std::optional<UInt64> left_rows,
    std::optional<UInt64> right_rows,
    double left_search_rows,
    double right_search_rows,
    JoinKind join_kind,
    JoinStrictness strictness)
{
    if (left_rows && right_rows)
        if (const auto rows = estimateJoinCardinality(left_rows, right_rows, keys, join_kind, strictness))
            return static_cast<double>(*rows);
    return joinSearchRows(keys.selectivity, left_search_rows, right_search_rows, join_kind, strictness);
}

/// The cost of a join: both inputs' costs plus the rows it emits.
inline double computeJoinCost(
    const QueryGraph & query_graph, const DPJoinEntryPtr & left, const DPJoinEntryPtr & right, const JoinKeyEstimate & keys,
    JoinKind join_kind, JoinStrictness strictness)
{
    return left->cost + right->cost
        + joinedRowsSearchValue(keys, left->estimated_rows, right->estimated_rows, searchRows(left, query_graph), searchRows(right, query_graph),
            join_kind, strictness);
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
    /// An input estimate or the join's own rests on a default or an assumption.
    bool estimate_from_defaults = false;
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
        const auto left_values = QueryPlanOptimizations::keyDistinctValues(left, lhs.getColumnName());
        const auto right_values = QueryPlanOptimizations::keyDistinctValues(right, rhs.getColumnName());
        keys.add(left_values, right_values,
            std::max(QueryPlanOptimizations::keyDomain(left, lhs.getColumnName()), QueryPlanOptimizations::keyDomain(right, rhs.getColumnName())));
        keys.countIndependentKey(left_values, right_values, {keyColumnId(lhs.getColumnName())}, {keyColumnId(rhs.getColumnName())});
    }

    const auto rows = estimateJoinCardinality(left_entry, right_entry, keys, join_operator.kind, join_operator.strictness);
    DPJoinEntry joined(left_entry, right_entry, /*cost*/ 0.0, keys.selectivity, rows, join_operator);
    joined.estimate_from_defaults |= joinRowsRestOnAssumption(keys, left.estimated_rows, right.estimated_rows);
    return JoinEstimate{
        .rows = joined.estimated_rows,
        .max_rows = joined.max_rows,
        .selectivity = keys.selectivity,
        .column_stats = std::move(joined.column_stats),
        .estimate_from_defaults = joined.estimate_from_defaults};
}

}
