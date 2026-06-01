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
    std::unordered_map<String, ColumnStats> column_stats = {};

    String table_name;
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
