#pragma once

#include <concepts>
#include <vector>
#include <Core/Joins.h>
#include <Common/EquivalenceClasses.h>
#include <Common/logger_useful.h>
#include <base/types.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/JoinExpressionActions.h>
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

/// One binary join operator captured verbatim from the original (pre-flattening) join tree.
/// Used only by the optional conflict detector for DPsub (CD-A or CD-C; see conflictDetector.h).
///   - `left` / `right`: the relation sets of the operator's two input subtrees;
///   - `nel`: the relations referenced by the operator's ON clause (its SES);
///   - `kind`: the operator's join kind.
/// Relation ids are in the final (global) QueryGraph numbering.
struct ConflictJoinOp
{
    BitSet left;
    BitSet right;
    BitSet nel;
    /// Relations on whose attributes the ON predicate rejects nulls (Definition 1 of the paper);
    /// a subset of `nel`. Enables the null-rejection-dependent reorderability entries.
    /// See `ConflictOpMask::nr_rels`.
    BitSet nr_rels;
    JoinKind kind = JoinKind::Inner;
    /// Strictness distinguishes plain joins (All) from semi/anti joins, which the detectors model as
    /// distinct operator types with their own reorderability (assoc / l-asscom / r-asscom) rules.
    JoinStrictness strictness = JoinStrictness::All;
};

struct QueryGraph
{
    std::vector<RelationStats> relation_stats;

    std::vector<JoinActionRef> edges;

    /// Operators of the original join tree, in tree (not enumeration) order. Populated during
    /// `buildQueryGraph` and consumed by the conflict detector (CD-A/CD-C) when it is enabled; empty
    /// otherwise. See `ConflictJoinOp`.
    std::vector<ConflictJoinOp> conflict_ops;

    /// When either is true, DPsub builds its reordering constraints from the conflict detector
    /// (see conflictDetector.h) over `conflict_ops` instead of the per-relation `join_kinds`
    /// restrictions. CD-C takes precedence over CD-A when both are set. Set from settings in
    /// `optimizeJoinOrder`; affects only the DPsub algorithm.
    bool use_cd_a_conflict_detector = false;
    bool use_cd_c_conflict_detector = false;

    /// Restriction for a null-supplying relation of an outer join.
    /// Maps (relation id) -> (set of relations referenced by the outer join's ON clause, join kind).
    /// The relation may be joined (as a singleton side) only against a set that contains all
    /// relations its ON clause depends on; the remaining relations may be joined outside.
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

    /// Build equivalence classes from existing edges. Call after all edges are populated.
    void buildColumnEquivalences();

    /// Check if two relation sets are transitively connected through column equivalences
    /// (i.e., there exists at least one equivalence class with members in both sets).
    bool areTransitivelyConnected(const BitSet & left, const BitSet & right) const;
};

struct QueryPlanOptimizationSettings;

DPJoinEntryPtr optimizeJoinOrder(QueryGraph query_graph, const QueryPlanOptimizationSettings & optimization_settings);

namespace QueryPlanOptimizations
{

/// Propagate per-column statistics through `actions`, rekeying the map in place by output name.
/// An output inherits an input's stats when it is that input, an alias of it, or a deterministic
/// single-argument function of it (which cannot increase the distinct count).
void remapColumnStats(std::unordered_map<String, ColumnStats> & mapped, const ActionsDAG & actions);

}

}
