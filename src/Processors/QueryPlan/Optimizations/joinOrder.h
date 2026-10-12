#pragma once

#include <concepts>
#include <vector>
#include <Core/Joins.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/JoinOperator.h>
#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>
#include <base/types.h>
#include <Common/EquivalenceClasses.h>
#include <Common/logger_useful.h>

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
    /// One over the largest key NDV of the join predicates, a key without an NDV counting its relation's
    /// rows; reported by `EXPLAIN` as `estimated (NDV)`. See `JoinKeyEstimate::selectivity`.
    double selectivity = 0.0;
    std::optional<UInt64> estimated_rows = {};
    /// Rows the result cannot exceed; see `RelationStats::max_rows` and `estimateJoinRowsUpperBound`.
    std::optional<UInt64> max_rows = {};
    /// A default selectivity stood in for a predicate in some relation of this entry.
    bool estimate_from_defaults = false;
    std::unordered_map<String, ColumnStats> column_stats = {};

    /// For join nodes
    JoinOperator join_operator;
    JoinMethod join_method = JoinMethod::None;

    /// For leaf nodes
    int relation_id = -1;

    /// Constructor for a leaf node (base relation)
    DPJoinEntry(size_t id, const RelationStats & relation_stats);

    /// Constructor for a join node
    DPJoinEntry(DPJoinEntryPtr lhs,
                DPJoinEntryPtr rhs,
                double cost_,
                double selectivity_,
                std::optional<UInt64> cardinality_,
                JoinOperator join_operator_,
                JoinMethod join_method_ = JoinMethod::Hash);

    bool isLeaf() const;

    String dump() const;
};

/// One binary join operator captured verbatim from the original (pre-flattening) join tree.
/// Used only by the optional conflict detector for DPsub (CD-A or CD-C; see conflictDetector.h).
///   - `left` / `right`: the relation sets of the operator's two input subtrees;
///   - `nel`: the relations referenced by the operator's ON clause;
///   - `kind`: the operator's join kind.
/// Relation ids are in the final (global) QueryGraph numbering.
struct ConflictJoinOp
{
    BitSet left;
    BitSet right;
    BitSet nel;
    /// Relations on whose attributes the ON predicate rejects nulls; a subset of `nel`. Enables the
    /// null-rejection-dependent reorderability entries. See `ConflictOpMask::nr_rels`.
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

    /// The graph is one semi or anti join whose probe side was built from the preserved side's
    /// key values, see `JoinStepLogical::probeKeysWithinPreservedSide`.
    bool probe_keys_within_preserved_side = false;

    /// Operators of the original join tree, in tree (not enumeration) order. Populated during
    /// `buildQueryGraph` and consumed by the conflict detector (CD-A/CD-C) when it is enabled; empty
    /// otherwise. See `ConflictJoinOp`.
    std::vector<ConflictJoinOp> conflict_ops;

    /// When not `NONE`, DPsub builds its reordering constraints from the selected conflict detector
    /// (see conflictDetector.h) over `conflict_ops` instead of the per-relation `join_kinds`
    /// restrictions. Set from settings in `optimizeJoinOrder`; affects only the DPsub algorithm.
    JoinOrderConflictDetector conflict_detector = JoinOrderConflictDetector::NONE;


    /// Strictness of every join of this graph (a graph mixes strictnesses only under a conflict
    /// detector, which carries the strictness per operator). A semi or anti join estimated as an
    /// outer join would keep its whole preserved side.
    JoinStrictness join_strictness = JoinStrictness::All;

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

    /// Relations read through a prepared join storage (`Join` engine, key-value storage), which is
    /// looked up by a fixed key.
    BitSet prepared_storage_relations;

    /// When set, the DP solvers keep `full_set_runner_up`: the cheapest plan over all relations whose
    /// top join splits them differently from the best plan, not just with its inputs swapped.
    bool keep_runner_up = false;
    mutable DPJoinEntryPtr full_set_runner_up;

    /// Build equivalence classes from existing edges. Call after all edges are populated.
    void buildColumnEquivalences();

    /// Check if two relation sets are transitively connected through column equivalences
    /// (i.e., there exists at least one equivalence class with members in both sets).
    bool areTransitivelyConnected(const BitSet & left, const BitSet & right) const;
};

struct QueryPlanOptimizationSettings;

struct JoinOrderResult
{
    DPJoinEntryPtr best;
    /// Another complete order, see `QueryGraph::full_set_runner_up`; null when not asked for or not found.
    DPJoinEntryPtr runner_up;
};

JoinOrderResult optimizeJoinOrder(QueryGraph query_graph, const QueryPlanOptimizationSettings & optimization_settings);

}
