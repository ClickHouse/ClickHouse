#pragma once

#include <Processors/QueryPlan/Optimizations/joinOrder.h>

#include <algorithm>
#include <limits>
#include <optional>

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

    UInt64 lhs_ndv = getColumnStats(query_graph, dp_table, lhs.getSourceRelations(), lhs.getColumnName());
    UInt64 rhs_ndv = getColumnStats(query_graph, dp_table, rhs.getSourceRelations(), rhs.getColumnName());
    UInt64 max_ndv = std::max(lhs_ndv, rhs_ndv);
    if (max_ndv > 0)
        selectivity = std::min(selectivity, 1.0 / static_cast<double>(max_ndv));
    return selectivity;
}

inline double computeSelectivity(
    const QueryGraph & query_graph,
    const PlanMemo & dp_table,
    SelectivityCache & expression_selectivity,
    const std::vector<JoinActionRef *> & edges)
{
    double selectivity = 1.0;
    for (const auto & edge : edges)
        selectivity = std::min(selectivity, computeSelectivity(query_graph, dp_table, expression_selectivity, *edge));
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

/// `upper_bound` is a proven canonical cardinality cap of the join output, if any: the local cost
/// of the join must not exceed the number of rows it can produce.
inline double computeJoinCost(
    const DPJoinEntryPtr & left, const DPJoinEntryPtr & right, double selectivity, std::optional<UInt64> upper_bound = {})
{
    double local_cost
        = selectivity * static_cast<double>(left->estimated_rows.value_or(1)) * static_cast<double>(right->estimated_rows.value_or(1));
    if (upper_bound)
        local_cost = std::min(local_cost, static_cast<double>(*upper_bound));
    return left->cost + right->cost + local_cost;
}

/// Checks that a relation id fits a native `UInt32` relation mask (used by DPsub).
UInt32 checkedRelationBit32(size_t relation);

struct JoinOrderPropertyOptions
{
    bool proven_uniqueness_enabled = false;
    bool dphyp_proven_edges_enabled = false;
    bool transitive_predicates_enabled = false;
    bool diagnostics_enabled = false;
};

/// Canonical data-property state of one join-order optimization, shared by every algorithm of
/// the fallback chain: admission of transitively-connected candidates, proven canonical
/// cardinality caps, and their diagnostics. With the properties disabled (no provider), every
/// candidate is assessed exactly like before the feature: caps are `Disabled` and transitive
/// connectivity follows only the independent `enable_join_transitive_predicates` setting.
class JoinOrderPropertyContext
{
public:
    JoinOrderPropertyContext(
        const QueryGraph & query_graph_,
        JoinOrderPropertyOptions options,
        std::unique_ptr<JoinOrderCanonicalProperties> canonical_properties_,
        JoinOrderOptimizationDebugInfo * debug_info_);

    const bool proven_uniqueness_enabled;
    const bool dphyp_proven_edges_enabled;
    const bool transitive_predicates_enabled;
    const bool data_property_diagnostics_enabled;

    const JoinOrderCanonicalProperties * canonicalProperties() const { return canonical_properties.get(); }
    JoinOrderOptimizationDebugInfo * debugInfo() const { return debug_info; }

    bool costingPropertiesEnabled() const { return proven_uniqueness_enabled && canonical_properties; }

    void recordCanonicalCapAssessment(const JoinOrderCardinalityCap & cap) const;

    /// `Subset` is a `BitSet` or a native `UInt32` relation mask; the native instantiation keeps
    /// the DPsub hot path free of `BitSet` allocations by using the provider's native group lookup.
    template <typename Subset>
    JoinOrderCardinalityCap getCanonicalCap(
        const Subset & left_relations,
        const Subset & right_relations,
        std::optional<UInt64> left_rows,
        std::optional<UInt64> right_rows) const
    {
        if (!costingPropertiesEnabled())
            return JoinOrderNoCardinalityCapReason::Disabled;
        return canonical_properties->inferInnerAllCardinalityCap(left_relations, right_relations, left_rows, right_rows);
    }

    /// Ordinary estimate, clamped by a proven canonical cap. Proven caps exist only for
    /// `INNER ALL` regions, so a cap is applied only to an `INNER ALL` join.
    JoinOrderCardinalityEstimate estimateCardinality(
        std::optional<UInt64> left_rows,
        std::optional<UInt64> right_rows,
        double selectivity,
        JoinKind join_kind,
        JoinStrictness strictness,
        const JoinOrderCardinalityCap & canonical_cap) const;

    /// Assessment of a predicate-free transitively-connected pair for the DPsub acceptor and
    /// the DPhyp synthetic hyperedges. The independent transitive setting admits every such
    /// pair; with the setting off only a proven canonical assessment may authorize
    /// proven-uniqueness-gated transitive connectivity. Every other outcome fails closed.
    struct TransitivePairAssessment
    {
        bool admitted = false;
        JoinOrderCardinalityCap canonical_cap;
    };
    template <typename Subset>
    TransitivePairAssessment assessTransitivePair(
        const Subset & left_relations,
        const Subset & right_relations,
        std::optional<UInt64> left_rows,
        std::optional<UInt64> right_rows) const
    {
        if (!query_graph.areTransitivelyConnected(toRelationBitSet(left_relations), toRelationBitSet(right_relations)))
            return {};

        if (transitive_predicates_enabled)
            return {.admitted = true, .canonical_cap = {}};

        const auto cap = getCanonicalCap(left_relations, right_relations, left_rows, right_rows);
        recordCanonicalCapAssessment(cap);
        return {.admitted = getProvenCap(cap) != nullptr, .canonical_cap = cap};
    }

    /// How a candidate pair is connected before canonical assessment. `legacy_connected`
    /// means applicable predicates exist; `has_cross_split_predicate` means at least one of
    /// them references both sides of this particular split.
    struct JoinCandidateConnectivity
    {
        bool legacy_connected = false;
        bool has_cross_split_predicate = false;
    };

    struct JoinCandidateAssessment
    {
        bool legacy_connected = false;
        bool has_cross_split_predicate = false;
        bool independently_transitive_connected = false;
        bool proof_gated_transitive_connected = false;
        bool equivalence_selectivity_allowed = false;
        JoinOrderCardinalityCap canonical_cap;

        bool connected() const { return legacy_connected || independently_transitive_connected || proof_gated_transitive_connected; }
    };

    JoinCandidateAssessment assessCandidate(
        const BitSet & left_relations,
        const BitSet & right_relations,
        std::optional<UInt64> left_rows,
        std::optional<UInt64> right_rows,
        JoinKind join_kind,
        JoinCandidateConnectivity connectivity) const;

private:
    static BitSet toRelationBitSet(const BitSet & subset) { return subset; }
    static BitSet toRelationBitSet(UInt32 subset) { return BitSet::fromUInt(subset); }

    const QueryGraph & query_graph;
    std::unique_ptr<JoinOrderCanonicalProperties> canonical_properties;
    JoinOrderOptimizationDebugInfo * debug_info;
    LoggerPtr log;
};

}
