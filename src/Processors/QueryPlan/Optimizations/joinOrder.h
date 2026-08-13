#pragma once

#include <concepts>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Core/Joins.h>
#include <Common/EquivalenceClasses.h>
#include <Common/logger_useful.h>
#include <base/types.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Processors/QueryPlan/Optimizations/DataProperties.h>
#include <Processors/QueryPlan/Optimizations/joinOrderCanonicalProperties.h>
#include <Processors/QueryPlan/Optimizations/joinOrderDataPropertyCatalog.h>
#include <Processors/QueryPlan/RelationEstimateInfo.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>

namespace DB
{

struct DPJoinEntry;
using DPJoinEntryPtr = std::shared_ptr<DPJoinEntry>;

enum class JoinMethod : UInt8
{
    None,
    Hash,
    Merge,
};

struct JoinOrderCardinalityEstimate
{
    std::optional<UInt64> rows;
    std::optional<UInt64> upper_bound;
};

JoinOrderPredicatePropertyBinding bindJoinOrderPredicate(const JoinActionRef & predicate, const JoinOrderDataPropertyCatalog & catalog);

template <std::unsigned_integral T>
inline String toBinaryString(T value)
{
    return toString(BitSet::fromUInt(value));
}

struct DPJoinEntry
{
    BitSet relations;

    DPJoinEntryPtr left;
    DPJoinEntryPtr right;

    double cost = 0.0;
    std::optional<UInt64> estimated_rows = {};
    std::unordered_map<String, ColumnStats> column_stats = {};

    /// For join nodes
    JoinOperator join_operator;
    JoinMethod join_method = JoinMethod::None;

    /// Whether this join's estimate was clamped by a proven canonical cardinality cap.
    /// `cleanupJoinPredicates` must then materialize the equality cut the cap assumed.
    bool used_canonical_cap = false;
    /// Obligations of the cap's proofs (`JoinOrderCardinalityCapProof::obligation_classes`):
    /// equality classes whose links must be enforced below this join by synthesis.
    UInt64 canonical_cap_obligations = 0;

    /// For leaf nodes
    int relation_id = -1;

    /// Constructor for a leaf node (base relation)
    DPJoinEntry(size_t id, std::optional<UInt64> rows, std::unordered_map<String, ColumnStats> column_stats_ = {});

    /// Constructor for a join node
    DPJoinEntry(DPJoinEntryPtr lhs,
                DPJoinEntryPtr rhs,
                double cost_,
                std::optional<UInt64> cardinality_,
                JoinOperator join_operator_,
                JoinMethod join_method_ = JoinMethod::Hash);

    bool isLeaf() const;

    String dump() const;
};

struct RelationStats
{
    std::optional<UInt64> estimated_rows = {};
    std::optional<Float64> avg_row_bytes = {};
    std::unordered_map<String, ColumnStats> column_stats = {};

    String table_name;

    bool imprecise_estimate = false;

    /// Diagnostic annotation of where `estimated_rows` came from; see `RowEstimateSource`.
    /// `NoSource` means the producer of the estimate did not track it; set it wherever it is known.
    RowEstimateSource source = RowEstimateSource::NoSource;
};

struct QueryGraph
{
    std::vector<RelationStats> relation_stats;
    std::shared_ptr<const JoinOrderDataPropertyCatalog> data_property_catalog;
    std::optional<JoinOrderPropertyUnsupportedReason> canonical_property_region_rejection;

    std::vector<JoinActionRef> edges;

    /// Restriction for a null-supplying relation of an outer join.
    /// Maps (relation id) -> (set of relations referenced by the outer join's ON clause, join kind).
    /// The relation may be joined (as a singleton side) only against a set that contains all
    /// relations its ON clause depends on; the remaining relations may be joined outside.
    /// Only non-`INNER ALL` joins are recorded, so an empty map means an all-inner region.
    std::unordered_map<size_t, std::pair<BitSet, JoinKind>> join_kinds;

    /// Predicates from the ON clause of an outer join, mapped to the id of the null-supplying
    /// relation. Such a predicate must be applied in the ON clause of the join step that joins
    /// this relation: it affects matching, not filtering (rows of the preserved side are kept
    /// even when the predicate doesn't hold).
    /// All other predicates are filters: they may be applied at any step where all their source
    /// relations are available, but they must not be merged into an outer join's ON clause -
    /// they go to the post-join `residual_filter` instead.
    std::unordered_map<JoinActionRef, size_t> outer_join_conditions;

    /// Column equivalence classes derived from equi-join edges (e.g., A.x = B.x AND B.x = C.x
    /// implies A.x, B.x, C.x are all equivalent). Used by the join order optimizer to detect
    /// transitive connectivity between relations without synthesizing extra edges.
    /// Stored as alias-resolved JoinActionRef-s pointing to INPUT nodes.
    EquivalenceClasses<JoinActionRef> column_equivalences;
    /// One relation mask per equivalence class, precomputed by `buildColumnEquivalences`,
    /// so `areTransitivelyConnected` costs one bitset intersection per class instead of
    /// rescanning every class member for every enumerated candidate pair.
    std::vector<BitSet> equivalence_class_relations;

    /// Build equivalence classes from existing edges. Call after all edges are populated.
    void buildColumnEquivalences();

    /// Check if two relation sets are transitively connected through column equivalences
    /// (i.e., there exists at least one equivalence class with members in both sets).
    bool areTransitivelyConnected(const BitSet & left, const BitSet & right) const;
};

struct QueryPlanOptimizationSettings;

struct JoinOrderCanonicalCapAssessmentMetrics
{
    UInt64 proven = 0;
    UInt64 missing_input_rows = 0;
    UInt64 not_proven = 0;
    UInt64 unsupported = 0;
};

struct JoinOrderOptimizationDebugInfo
{
    std::optional<JoinOrderCanonicalMetrics> canonical_metrics;
    JoinOrderCanonicalCapAssessmentMetrics cap_assessments;
};

DPJoinEntryPtr optimizeJoinOrder(
    QueryGraph query_graph,
    const QueryPlanOptimizationSettings & optimization_settings,
    JoinOrderOptimizationDebugInfo * debug_info = nullptr);

namespace QueryPlanOptimizations
{

/// Propagate per-column statistics through `actions`, rekeying the map in place by output name.
/// An output inherits an input's stats when it is that input, an alias of it, or a deterministic
/// single-argument function of it (which cannot increase the distinct count).
void remapColumnStats(std::unordered_map<String, ColumnStats> & mapped, const ActionsDAG & actions);

}

}
