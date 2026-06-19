#pragma once

#include <vector>
#include <Core/Joins.h>
#include <Common/EquivalenceClasses.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/JoinExpressionActions.h>
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

/// Quality of a row-count value held in `estimated_rows`.
enum class RowCountKind : UInt8
{
    /// Nothing is known; `estimated_rows` is empty.
    Unknown,
    /// A guaranteed exact count (e.g. an unfiltered table size or an in-memory row count). Being
    /// exact, it is also a valid lower bound, which the build-side choice requires before trusting
    /// it against the other side (see `chooseJoinOrder`).
    Exact,
    /// A guaranteed upper bound but not exact (e.g. a residual filter the primary index cannot use
    /// leaves only the scanned-row count, or `LIMIT [WITH TIES]` over an inexact child).
    UpperBound,
    /// A heuristic estimate with no guarantee in either direction (NDV-based aggregation result,
    /// join cardinality, statistics estimator). Usable for cost/ordering but never as proof of
    /// relative size for the swap.
    Estimate,
    /// A measured row count from the runtime hash-table-stats cache (`HashTablesStatistics`): the
    /// actual number of rows a previous execution fed into this build. It is not a guaranteed
    /// bound (the cache is max-like and can be stale), but it is a real measurement the optimizer
    /// already relies on for join reordering (`use_hash_table_stats_for_join_reordering`), so the
    /// upper-bound swap accepts it on the opposite side -- unlike the purely-derived `Estimate`.
    Cached,
};

/// The row count only when it is a point estimate (exact, heuristic or measured), i.e. not a bare
/// upper bound. This is what the heuristic `lhs < rhs` swap comparison and result reporting use.
inline std::optional<UInt64> pointEstimate(std::optional<UInt64> rows, RowCountKind kind)
{
    return (kind == RowCountKind::Exact || kind == RowCountKind::Estimate || kind == RowCountKind::Cached) ? rows : std::nullopt;
}

/// Whether a right-side value may anchor an upper-bound-driven swap (`upperBound(left) < this`).
/// Exact is a true lower bound; Cached is a measured size the optimizer already trusts for
/// reordering (see `RowCountKind::Cached`). A derived `Estimate` may be wildly off (e.g. an
/// NDV-less aggregation reporting its input row count), so it is excluded.
inline bool canAnchorUpperBoundSwap(RowCountKind kind)
{
    return kind == RowCountKind::Exact || kind == RowCountKind::Cached;
}

struct DPJoinEntry
{
    BitSet relations;

    DPJoinEntryPtr left;
    DPJoinEntryPtr right;

    double cost = 0.0;
    /// The best known row-count value; empty iff `estimated_rows_kind == Unknown`. Its guarantee
    /// is described by `estimated_rows_kind`. Used directly as a size proxy for cost/ordering;
    /// the build-side swap additionally inspects the kind (see `chooseJoinOrder`).
    std::optional<UInt64> estimated_rows = {};
    RowCountKind estimated_rows_kind = RowCountKind::Unknown;
    std::unordered_map<String, ColumnStats> column_stats = {};

    std::optional<UInt64> pointEstimate() const { return DB::pointEstimate(estimated_rows, estimated_rows_kind); }

    /// For join nodes
    JoinOperator join_operator;
    JoinMethod join_method = JoinMethod::None;

    /// For leaf nodes
    int relation_id = -1;

    /// Constructor for a leaf node (base relation)
    DPJoinEntry(size_t id, std::optional<UInt64> rows, std::unordered_map<String, ColumnStats> column_stats_ = {}, RowCountKind rows_kind = RowCountKind::Unknown);

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
    /// See `DPJoinEntry::estimated_rows_kind`.
    RowCountKind estimated_rows_kind = RowCountKind::Unknown;
    std::unordered_map<String, ColumnStats> column_stats = {};

    String table_name;

    std::optional<UInt64> pointEstimate() const { return DB::pointEstimate(estimated_rows, estimated_rows_kind); }
};

struct QueryGraph
{
    std::vector<RelationStats> relation_stats;

    std::vector<JoinActionRef> edges;

    /// Shape constraint for a null-supplying relation.
    /// Example: `(A LEFT JOIN B) JOIN C ON B.y=C.y` registers for B:
    ///   required_partners  = {A}   — at `{X} ⋈ {B}`, X must include A.
    ///   forbidden_partners = {C}   — at `{X} ⋈ {B}`, X must not include C
    ///                                (C was pulled across the boundary by `B.y=C.y`;
    ///                                allowing `{A,C} ⋈ {B}` would drag the predicate
    ///                                into the LEFT JOIN's ON clause). It's still fine
    ///                                for C to sit opposite a subtree that *contains*
    ///                                B (e.g. `{A,B} ⋈ {C}`) — the check doesn't fire.
    ///   kind               = LEFT  — kind to return when the shape is valid.
    struct OuterJoinRestriction
    {
        BitSet required_partners;
        BitSet forbidden_partners;
        JoinKind kind{};
    };
    std::unordered_map<size_t, OuterJoinRestriction> join_kinds;

    /// Each predicate may require a set of relations to be already joined before it becomes applicable
    std::unordered_map<JoinActionRef, BitSet> pinned;

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

}
