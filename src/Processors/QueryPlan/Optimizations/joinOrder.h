#pragma once

#include <concepts>
#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>
#include <Core/Joins.h>
#include <Core/Names.h>
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

/// Every split of a set of relations that the DPsub search accepted, with what is needed to build a join over each of
/// them. The Cascades optimizer gets one memo group per set and one join expression per split, see `JoinGraphStep`.
struct RecordedJoinSplits
{
    struct Split
    {
        UInt64 left = 0;
        UInt64 right = 0;
        /// Positions in `edges` of the predicates that DPsub places at this split.
        std::vector<size_t> edges;
        /// The DPsub cost of this join without the costs of its two inputs: the cost of a join order is the sum of
        /// these over its joins.
        double local_cost = 0;
    };
    /// In the order the search accepted them.
    std::vector<Split> splits;
    /// Calls of the acceptor; the splits are the calls it accepted.
    size_t considered_splits = 0;

    /// The edges of the graph, and per edge the relations that must be joined before it applies.
    std::vector<JoinActionRef> edges;
    std::vector<UInt64> edge_relations;

    /// The column equivalences of the graph; empty when transitive predicates are off.
    EquivalenceClasses<JoinActionRef> column_equivalences;

    /// The DPsub winner of every set that has a split. It carries the estimates of the set.
    std::unordered_map<UInt64, DPJoinEntryPtr> winners;
    /// With `query_plan_join_order_distribution`: per set, the left inputs of the top joins of the plans DPsub keeps for
    /// it, its cheapest plan first.
    std::unordered_map<UInt64, std::vector<UInt64>> frontier_lefts;

    /// Set when DPsub planned the graph; otherwise nothing above is complete.
    bool complete = false;
};

struct QueryGraph
{
    std::vector<RelationStats> relation_stats;

    std::vector<JoinActionRef> edges;

    /// The graph is one semi or anti join whose probe side was built from the preserved side's
    /// key values, see `JoinStepLogical::probeKeysWithinPreservedSide`.
    bool probe_keys_within_preserved_side = false;

    /// For each of `edges`, the index in `conflict_ops` of the join step whose ON clause it comes
    /// from. An inner join's ON clause is split into conjuncts placed on their own, and a conjunct
    /// placed on its own still has to obey the conflict rules of its join step.
    std::vector<size_t> edge_conflict_ops;

    /// Operators of the original join tree, in tree (not enumeration) order. Populated during
    /// `buildQueryGraph` and consumed by the conflict detector (CD-A/CD-C) when it is enabled; empty
    /// otherwise. See `ConflictJoinOp`.
    std::vector<ConflictJoinOp> conflict_ops;

    /// When not `NONE`, DPsub builds its reordering constraints from the selected conflict detector
    /// (see conflictDetector.h) over `conflict_ops` instead of the per-relation `join_kinds`
    /// restrictions. Set from settings in `optimizeJoinOrder`; affects only the DPsub algorithm.
    JoinOrderConflictDetector conflict_detector = JoinOrderConflictDetector::NONE;

    /// Whether a semi/anti join actually joined this set of tables. `conflict_ops` lists every join
    /// of the original query whatever the settings, so it cannot answer this on its own.
    bool semi_anti_flattened = false;

    /// `query_plan_join_swap_table`: empty when the planner may pick the build side itself, set when
    /// the query pinned it. Only when it is empty may a join's inputs be put round the cheaper way.
    std::optional<bool> join_swap_table;

    /// The strictness `chooseJoinOrder` stamps onto every join of this graph (unless a conflict
    /// detector reordered a semi/anti join, which keeps its own). DP entries are built as `All`
    /// whatever it is, so an `ANY`/`SEMI`/`ANTI` graph is only recognisable from here. A semi or
    /// anti join estimated as an outer join would keep its whole preserved side.
    JoinStrictness join_strictness = JoinStrictness::All;

    /// Restriction for a null-supplying relation of an outer join.
    /// Maps (relation id) -> (set of relations referenced by the outer join's ON clause, join kind).
    /// The relation may be joined (as a singleton side) only against a set that contains all
    /// relations its ON clause depends on; the remaining relations may be joined outside.
    std::unordered_map<size_t, std::pair<BitSet, JoinKind>> join_kinds;

    /// Predicates from the ON clause of an outer join, mapped to the null-supplying side of that
    /// join (both sides for `FULL`). Such a predicate must be applied in the ON clause of the join
    /// step that joins this side: it affects matching, not filtering (rows of the preserved side
    /// are kept even when the predicate doesn't hold). The side is a single relation unless the
    /// graph was built for the conflict detector (see `requires_conflict_detector`).
    /// All other predicates are filters: they may be applied at any step where all their source
    /// relations are available, but they must not be merged into an outer join's ON clause -
    /// they go to the post-join `residual_filter` instead.
    std::unordered_map<JoinActionRef, BitSet> outer_join_conditions;

    /// Relations on the null-supplying side of an outer join that `join_kinds` cannot hold, because
    /// that side has more than one relation. They are kept out of column equivalences like the
    /// relations in `join_kinds`.
    BitSet null_supplying_subtree_relations;

    /// Set when the graph holds a join that only DPsub with a conflict detector can reorder: a
    /// semi/anti join, an outer join whose null-supplying side has more than one relation, or a
    /// `FULL` join among more than two relations. `join_kinds` does not describe such a graph, so no
    /// other algorithm may plan it, and DPsub must not turn it down.
    bool requires_conflict_detector = false;

    /// The two input subtrees of every cross product of the original tree (`CROSS JOIN`, a comma join,
    /// or an inner join without any condition). A cross product joins on nothing, so it adds no edge,
    /// and a graph that only cross products hold together would otherwise fall apart. Each one links
    /// its two sides instead, as a join with selectivity 1: the reordered join it ends up as still has
    /// no condition, so it stays `Cross`.
    std::vector<std::pair<BitSet, BitSet>> cross_product_links;

    /// Column equivalence classes derived from equi-join edges (e.g., A.x = B.x AND B.x = C.x
    /// implies A.x, B.x, C.x are all equivalent). Used by the join order optimizer to detect
    /// transitive connectivity between relations without synthesizing extra edges.
    /// Stored as alias-resolved JoinActionRef-s pointing to INPUT nodes.
    EquivalenceClasses<JoinActionRef> column_equivalences;

    /// Relations read through a prepared join storage (`Join` engine, key-value storage), which is
    /// looked up by a fixed key.
    BitSet prepared_storage_relations;

    /// How many complete orders besides the best one the DP solvers keep: the cheapest plans over all
    /// relations whose top joins split them in different ways (a swap of the two inputs is the same split).
    size_t max_join_order_alternatives = 0;
    /// The kept plans by cost, the best plan among them; filled while a solver runs.
    mutable std::vector<DPJoinEntryPtr> full_set_alternatives;
    /// When set, DPsub records every split it accepts here.
    std::shared_ptr<RecordedJoinSplits> recorded_splits;

    /// DPsub chooses between hash and sort-merge joins by the sort order of their inputs; set from the graph's join settings.
    bool use_interesting_orders = false;
    /// The order of a sort-merge join result is used by the sort of a later join (`optimize_sorting_by_input_stream_properties`).
    bool reuse_join_output_order = false;
    /// A hash join's runtime filter prunes the granules of the probe side by its primary key (`enable_join_runtime_filters_index_analysis`).
    bool runtime_filter_prunes_probe = false;
    /// A sort-merge join of inputs read in order by their primary key runs sharded by key ranges (`query_plan_join_shard_by_pk_ranges`).
    bool shard_merge_by_pk_ranges = false;

    /// With `query_plan_join_order_distribution`: the nodes the query runs on, 0 when DPsub ignores how the data is
    /// spread over them. DPsub then costs joins as local, shuffle or broadcast joins, see `CostConfig`.
    size_t distribution_node_count = 0;
    /// The input columns of the graph, for the bytes an exchange of a set of relations moves: the columns of its relations
    /// that the graph outputs or that a join with a relation outside it compares.
    struct DistributionColumn
    {
        size_t relation = 0;
        double bytes = 0;
        bool output = false;
        /// The relations of the predicates and equivalence classes that use the column.
        UInt64 compared_with = 0;
    };
    std::vector<DistributionColumn> distribution_columns;
    /// Input columns that the step above the graph would like its input partitioned by, see
    /// `JoinStepLogical::getPartitionKeysAbove`. A plan of all relations partitioned by one of them saves that step an
    /// exchange, so DPsub keeps such plans.
    std::vector<JoinActionRef> partition_columns_above;
    /// The step above aggregates before it exchanges its input.
    bool partition_above_aggregates = false;
    /// The step above needs all rows on one node.
    bool single_node_above = false;
    /// Where the rows of a relation are when its plan ends, per relation.
    enum class LeafState : uint8_t
    {
        Read,         /// A table read: partitioned, on one node or on every node, each with the cost of its read.
        SingleNode,   /// A result that ends on one node, like a `UNION ALL` or an aggregation without keys.
        Either,       /// An aggregation with keys, which Cascades may leave partitioned or gather.
        Partitioned,  /// Any other result: partitioned, on one node after a gather.
    };
    std::vector<LeafState> relation_leaf_states;
    /// The bytes a broadcast may replicate to every node, see `distributed_plan_max_bytes_to_broadcast`; 0 for no limit.
    UInt64 max_bytes_to_broadcast = 0;

    /// How a relation can be read in order: the longest sequence of its `candidates`, and how many of its leading columns
    /// are the leading primary key columns themselves, which a sharded merge splits by (`optimizeJoinByShards`).
    struct ReadInOrder
    {
        Names prefix;
        size_t primary_key_prefix = 0;
    };
    /// Per relation. Called by DPsub only when it uses interesting orders, since it probes the storage order of every input.
    std::function<std::vector<ReadInOrder>(const std::vector<NameSet> & candidates)> find_read_in_order_prefixes;

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
    /// Other complete orders by cost, see `QueryGraph::max_join_order_alternatives`. They share the entries of their
    /// common subtrees with `best` and with each other.
    std::vector<DPJoinEntryPtr> alternatives;
};

JoinOrderResult optimizeJoinOrder(QueryGraph query_graph, const QueryPlanOptimizationSettings & optimization_settings);

}
