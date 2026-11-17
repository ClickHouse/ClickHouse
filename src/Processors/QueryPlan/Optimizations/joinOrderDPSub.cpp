#include <Processors/QueryPlan/Optimizations/joinOrderAlgorithms.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Processors/QueryPlan/Optimizations/joinOrderCommon.h>
#include <Processors/QueryPlan/Optimizations/joinEnum.h>
#include <Processors/QueryPlan/Optimizations/dpTable.h>
#include <Processors/QueryPlan/Optimizations/enumeratorChecker.h>
#include <Processors/QueryPlan/Optimizations/conflictDetector.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/JoinOperator.h>
#include <Common/Exception.h>

#include <algorithm>
#include <bit>
#include <limits>
#include <map>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <Common/MapWithMemoryTracking.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Pack a `BitSet` (boost::dynamic_bitset) of relation ids into a native 32-bit mask.
/// Used by the DPsub hot path, where the relation count is guaranteed below 32 so every
/// set bit fits, allowing the per-CCP work to run on native integers instead of allocating
/// a `dynamic_bitset` for every plan it considers.
UInt32 toMask(const BitSet & bits)
{
    UInt32 mask = 0;
    for (auto bit : bits)
    {
        chassert(bit < std::numeric_limits<UInt32>::digits);
        mask |= (static_cast<UInt32>(1) << bit);
    }
    return mask;
}

/// Whether the DP entries of a graph carry the real strictness of their joins. They do for an `ALL`
/// graph. Otherwise the strictness is stamped on the joins later and the entries read `All`, except
/// in a semi/anti graph flattened for the conflict detector, where each semi/anti join is an
/// operator of its own and its entry carries its strictness.
bool entriesCarryStrictness(const QueryGraph & query_graph)
{
    if (query_graph.join_strictness == JoinStrictness::All)
        return true;
    const bool is_semi_or_anti = query_graph.join_strictness == JoinStrictness::Semi
        || query_graph.join_strictness == JoinStrictness::Anti;
    return query_graph.semi_anti_flattened && is_semi_or_anti;
}

/// A numeric statistics value as a double, for estimating the share of a key range.
std::optional<double> toNumber(const std::optional<Field> & value)
{
    if (!value)
        return std::nullopt;
    switch (value->getType())
    {
        case Field::Types::UInt64: return static_cast<double>(value->safeGet<UInt64>());
        case Field::Types::Int64: return static_cast<double>(value->safeGet<Int64>());
        case Field::Types::Float64: return value->safeGet<Float64>();
        default: return std::nullopt;
    }
}

/// Whether a join may have its inputs swapped. Inner and cross joins commute, and an `ALL`
/// outer join is mirrored through `reverseJoinKind` (`Left` <-> `Right`), the same equivalence the
/// enumeration uses. Semi/anti joins keep their sides, and so do `ANY` joins: `ANY LEFT` keeps one
/// match per left row, `ANY RIGHT` one per right row.
bool canSwap(JoinKind kind, JoinStrictness strictness)
{
    if (isInner(kind) || isCrossOrComma(kind))
        return true;
    return strictness == JoinStrictness::All && (isLeft(kind) || isRight(kind) || isFull(kind));
}

class DPSubJoinOrderOptimizer
{
public:
    explicit DPSubJoinOrderOptimizer(QueryGraph & query_graph_)
        : query_graph(query_graph_)
    {
        if (query_graph.distribution_plan_pair_limit)
            plan_pair_limit = std::min(plan_pair_limit, query_graph.distribution_plan_pair_limit);
        if (query_graph.distribution_partition_state_limit)
            partition_state_limit = std::min(partition_state_limit, query_graph.distribution_partition_state_limit);
    }

    DPJoinEntryPtr solve();

private:
    /// `plan_index` selects a plan of `S` from `ordered`, -1 is its cheapest plan.
    template <typename DPTable, std::unsigned_integral TUInt>
    DPJoinEntryPtr buildPhysicalPlan(const DPTable & dptable, const TUInt & S, Int32 plan_index = -1) const;

    template <class TDPTable, class TOptimizer>
    friend class DB::EnumeratorCheckerWithCosts;

    [[nodiscard]] std::optional<UInt64> estimateCardinality(
        std::optional<UInt64> left_rows, std::optional<UInt64> right_rows, const SelectivityEstimate & selectivity, JoinKind join_kind,
        JoinStrictness strictness = JoinStrictness::All) const;

    /// Native-mask counterparts used exclusively by the DPsub acceptor.
    void initDPsubScratch();
    /// With `useDistribution`: the types of the classes and the shuffle key of each edge.
    void initShuffleKeys(const UnorderedMapWithMemoryTracking<const EquivalenceClasses<JoinActionRef>::Class *, UInt32> & class_index);
    [[nodiscard]] std::optional<JoinKind> isValidJoinOrderMask(UInt32 left_mask, UInt32 right_mask) const;

    /// Conflict-detector (CD) variant: decide validity and the resulting (kind, strictness) using the
    /// per-operator descriptors in `dpsub_data.conflict_operators` (CD-A or CD-C), which support
    /// outer and semi/anti reordering. Returns nullopt to reject the split.
    [[nodiscard]] std::optional<std::pair<JoinKind, JoinStrictness>> isValidJoinOrderMaskConflict(UInt32 left_mask, UInt32 right_mask) const;

    /// Dispatch used by the DPsub acceptor: routes to the per-operator conflict-detector check when
    /// a conflict detector (CD-A or CD-C) is enabled, otherwise to the per-relation
    /// `isValidJoinOrderMask` (with strictness fixed to All, its only supported case).
    [[nodiscard]] std::optional<std::pair<JoinKind, JoinStrictness>> resolveJoinMask(UInt32 left_mask, UInt32 right_mask) const;

    /// True when an inner-join condition joins the two sides here: all its tables are present and it
    /// mentions both sides. Skips pinned conditions as those belong to an outer or semi/anti join's
    [[nodiscard]] bool hasInnerEdgeAcross(UInt32 left_mask, UInt32 right_mask) const;

    /// The join kind as the query wrote it (`Left` or `Right`) of the semi/anti join step applied where
    /// `left_mask` and `right_mask` are joined, if there is one.
    [[nodiscard]] std::optional<JoinKind> writtenSemiAntiKind(UInt32 left_mask, UInt32 right_mask, JoinStrictness strictness) const;

    /// True when a column equivalence ties the two sides together: some equivalence class has a
    /// member on each side. The mask counterpart of `QueryGraph::areTransitivelyConnected`.
    [[nodiscard]] bool areTransitivelyConnectedMask(UInt32 left_mask, UInt32 right_mask) const;

    /// Equivalence classes (a bit mask) on which an inner equi-join of the two sides may run as `full_sorting_merge`.
    [[nodiscard]] UInt64 mergeKeyClasses(
        const VectorWithMemoryTracking<JoinActionRef *> & edges, UInt32 left_mask, UInt32 right_mask, JoinKind kind, JoinStrictness strictness) const;
    [[nodiscard]] bool isReadInOrder(UInt32 relation_mask, Int32 order_class) const;

    /// Cost model of a sort-merge join of two input plans on the equivalence class, see `computeMergeJoinCost`.
    [[nodiscard]] double mergeJoinCost(
        MergeJoinInput left, UInt32 left_mask, MergeJoinInput right, UInt32 right_mask, Int32 order_class,
        const SelectivityEstimate & selectivity) const;
    /// Whether a merge of the two inputs runs sharded by primary key ranges, as `optimizeJoinByShards` does.
    [[nodiscard]] bool isShardedMerge(const MergeJoinInput & left, UInt32 left_mask, const MergeJoinInput & right, UInt32 right_mask) const;
    /// Whether the statistics show that a table splits into one key range for a merge on the classes: each leading primary
    /// key column it is joined on has a single distinct value.
    [[nodiscard]] bool hasConstantShardKey(UInt32 relation_mask, UInt64 key_classes) const;
    /// Whether a single relation read in order by the class can feed a sharded merge on it.
    [[nodiscard]] bool isShardableLeaf(UInt32 relation_mask, Int32 order_class) const
    {
        return isReadInOrder(relation_mask, order_class) && (relation_mask & dpsub_data.shardable_relations);
    }
    /// The equivalence classes with columns on both sides: one key of the join each.
    [[nodiscard]] UInt64 spanningClasses(UInt32 left_mask, UInt32 right_mask) const;
    /// The spanning classes that a merge join gets an added key of: those of one type. In a class of several types two
    /// columns can differ (`Int32` 0 and 1 joined through `Enum8('a' = 0)`, `String` and `Enum8('a' = 1)`), and the join
    /// keeps the edges of such a class as its keys instead.
    [[nodiscard]] UInt64 mergeKeysClasses(UInt32 left_mask, UInt32 right_mask) const;
    /// The classes of all keys of a join, in the order a single relation can be read in, starting with the merge key;
    /// empty if it cannot be read in an order that covers them.
    [[nodiscard]] VectorWithMemoryTracking<Int32> keyOrder(UInt32 relation_mask, UInt64 key_classes, Int32 order_class) const;
    /// The number of distinct values of the class's columns in a set of relations, at most `rows`; 1 when unknown.
    [[nodiscard]] double distinctValues(UInt32 relations, Int32 order_class, UInt64 rows) const;
    /// The work of a hash join of the two sides, see `computeHashJoinWork`. 0 without interesting orders, where costs
    /// count output rows only.
    [[nodiscard]] double hashJoinWork(UInt32 left_mask, std::optional<UInt64> left_rows, UInt32 right_mask, std::optional<UInt64> right_rows) const;
    /// The overlap of the key range of the build side with the key range of a probe table sorted by a key of the join: what
    /// a hash join's runtime filter lets it read, and what a sharded merge reads. 1 when unknown.
    [[nodiscard]] double probeReadFraction(UInt32 probe_mask, UInt32 build_mask) const;
    /// Whether a later join may still use a result sorted by the equivalence class.
    [[nodiscard]] bool isInterestingOrder(Int32 order_class, UInt32 relations) const;
    /// The column of the class that the result of a plan is sorted by, as `applyOrder` sees it: a merge join on the class
    /// is sorted by the key of its left input. For any other plan, the first column of the class in its relations.
    template <typename DPTable, std::unsigned_integral TUInt>
    [[nodiscard]] const JoinActionRef & orderColumn(const DPTable & dptable, TUInt S, Int32 plan_index, Int32 order_class) const;

    [[nodiscard]] bool useInterestingOrders() const { return dpsub_data.interesting_orders; }

    /// Whether joins are costed by the data they move between nodes, see `QueryGraph::distribution_node_count`.
    [[nodiscard]] bool useDistribution() const { return dpsub_data.distribution; }
    /// The search keeps a plan per partitioning state, so its work grows with the product of the plans of two sides. It
    /// costs distributed joins only for graphs with at most this many classes that join two relations or more. After
    /// this many pairs of plans it adds no more distributed plans, and the graph is planned again without distribution.
    static constexpr size_t max_distribution_classes = 16;
    static constexpr size_t max_distributed_plan_pairs = 10'000'000;
    /// A set keeps a plan for each state, and its plans are numbered with `Int8`: the keyed states and the three negative
    /// ones must fit into the positive range.
    static constexpr size_t max_partition_states = std::numeric_limits<Int8>::max() - 3;
    /// Counts the pairs of plans of a distributed join; false when the search is over the budget and must stop.
    bool spendDistributedPlanPairs(size_t pairs)
    {
        distributed_plan_pairs += pairs;
        if (distributed_plan_pairs > plan_pair_limit)
            distribution_stopped = true;
        return !distribution_stopped;
    }
    /// The states of a plan's rows over the nodes (`DPPlan::partition_state`): an index in `DPSubData::partition_states`
    /// is partitioned as that entry says, and the negative values are the states below.
    static constexpr Int8 any_partitioning_state = -1; /// Partitioned in a way that no later join uses.
    static constexpr Int8 replicated_state = -2;       /// A full copy on every node.
    static constexpr Int8 single_node_state = -3;      /// All rows on one node.
    static constexpr Int8 no_state = -4;               /// A state not looked up yet, see `partitionState`.
    /// The equivalence classes with columns on both sides: the keys both sides may be shuffled by.
    [[nodiscard]] UInt64 partitionKeyClasses(UInt32 left_mask, UInt32 right_mask) const;
    /// An equality of two columns of a class, as the key of a shuffle join.
    struct ShuffleKey
    {
        Int32 key_class = -1;     /// -1 when the edge is no equality of two columns of a class
        UInt8 lhs_relation = 0;
        UInt8 rhs_relation = 0;
        bool raw = false;         /// the two columns have one type, so the join hashes their values as they are
        bool hashable = false;    /// the two types have a common type to hash at, as `extractRoutingEquiKeys` requires
        UInt32 lhs_relations = 0; /// the relations of the two sides of the equality
        UInt32 rhs_relations = 0;
    };
    [[nodiscard]] const ShuffleKey & shuffleKey(const JoinActionRef * edge) const
    {
        return dpsub_data.shuffle_keys[edge - query_graph.edges.data()];
    }
    /// Whether predicate cleanup adds an equality of the class to a join with no edge of the class, see `cleanupJoinPredicates`.
    [[nodiscard]] bool hasSynthesizedShuffleKey(UInt32 key_class) const
    {
        return ((dpsub_data.shuffle_classes >> key_class) & 1) && dpsub_data.class_single_type[key_class];
    }
    /// The state of rows that a shuffle by a raw key of the class hashed: Cascades advertises them as hashed by the two
    /// columns of the key, the columns of the class in `relations`. `any_partitioning_state` when there is no such state,
    /// for example when a relation has two columns in the class.
    Int8 partitionState(UInt32 key_class, UInt32 relations, Int32 type = -1);
    /// Whether the search chooses the key of a shuffle by the class itself, instead of taking an edge: for a class of one
    /// type up to `Nullable` and `LowCardinality` with one column per relation, any two columns of the class are equal,
    /// and the join gets the chosen equality first (see `buildPhysicalPlan`).
    [[nodiscard]] bool choosesShuffleKey(UInt32 key_class) const
    {
        return hasSynthesizedShuffleKey(key_class) && dpsub_data.class_has_states[key_class];
    }
    /// The exact types (bits of indexes in `DPSubData::class_type_relations`) that both sides have columns of: a shuffle
    /// by the class on one of them hashes the key as it is.
    [[nodiscard]] UInt64 shuffleKeyTypes(UInt32 key_class, UInt32 left_mask, UInt32 right_mask) const;
    /// The exact type of the columns of the class that rows in the state are hashed by, -1 for another state.
    [[nodiscard]] Int32 stateType(Int8 state, UInt32 key_class) const
    {
        if (state < 0 || dpsub_data.partition_states[state].key_class != key_class)
            return -1;
        return dpsub_data.partition_states[state].type;
    }
    /// The relations whose column of the class, of the exact type, the result of a plan is hashed by: the two key
    /// columns of a shuffle by the class, or those of the partitioned side of a broadcast.
    template <typename DPTable, std::unsigned_integral TUInt>
    [[nodiscard]] UInt32 routedRelations(const DPTable & dptable, TUInt S, Int32 plan_index, UInt32 key_class, Int32 type) const;
    /// The two relations of the columns of the shuffle key that the search chose for a plan.
    template <typename DPTable, std::unsigned_integral TUInt>
    [[nodiscard]] std::pair<UInt32, UInt32> shuffleKeyRelations(const DPTable & dptable, TUInt S, Int32 plan_index) const;
    /// The column of the class in the relation.
    [[nodiscard]] const JoinActionRef & classColumn(UInt32 key_class, UInt32 relation) const;
    /// Whether rows in the state are hashed by the column of the class in the relation, so that a shuffle by a raw key
    /// with that column moves no rows.
    [[nodiscard]] bool isPartitionedBy(Int8 state, UInt32 key_class, UInt32 relation) const
    {
        if (state < 0)
            return false;
        const auto & partition_state = dpsub_data.partition_states[state];
        return partition_state.key_class == key_class && ((partition_state.relations >> relation) & 1);
    }
    /// Whether a later join may still use a result in the partitioning state.
    [[nodiscard]] bool isInterestingPartition(Int8 partition_state, UInt32 relations) const
    {
        return dpsub_data.class_relations[dpsub_data.partition_states[partition_state].key_class] & ~relations;
    }
    /// Whether a set keeps its cheapest plan in a state besides its cheapest plan overall. A copy on every node of all
    /// relations is no use, since nothing joins it any more.
    [[nodiscard]] bool isInterestingState(Int8 state, UInt32 relations) const
    {
        if (state >= 0)
            return isInterestingPartition(state, relations);
        const UInt32 all_relations = (static_cast<UInt32>(1) << query_graph.relation_stats.size()) - 1;
        return state != replicated_state || relations != all_relations;
    }

    /// The costs of the hash joins of two sides on the nodes, with the cost functions of Cascades (`exchangeTransferCost`,
    /// `hashJoinWorkCost`). Each side is first moved to the state a join needs with the exchanges Cascades uses for it.
    struct DistributedJoinCost
    {
        const CostConfig & config;
        double nodes = 1;

        static constexpr double impossible = std::numeric_limits<double>::infinity();
        /// A shuffle among the nodes, or a gather or a scatter, which send every row through one node.
        [[nodiscard]] double shuffle(double rows, double bytes) const
        {
            return exchangeTransferCost(rows, bytes, nodes, 0, config).total(config);
        }
        [[nodiscard]] double funnel(double rows, double bytes) const
        {
            return exchangeTransferCost(rows, bytes, 1, rows, config).total(config);
        }

        /// To partitioned in any way.
        [[nodiscard]] double toPartitioned(Int8 state, double rows, double bytes) const
        {
            return state == replicated_state || state == single_node_state ? toNewPartitioning(state, rows, bytes) : 0;
        }
        /// To a partitioning that no plan has yet: a shuffle, or a scatter from one node. Copies cannot be split without
        /// multiplying the rows.
        [[nodiscard]] double toNewPartitioning(Int8 state, double rows, double bytes) const
        {
            if (state == replicated_state)
                return impossible;
            return state == single_node_state ? funnel(rows, bytes) : shuffle(rows, bytes);
        }
        /// To a copy on every node: a broadcast from one node, after a gather when the rows are on several nodes.
        [[nodiscard]] double toReplicated(Int8 state, double rows, double bytes) const
        {
            if (state == replicated_state)
                return 0;
            const double broadcast = exchangeTransferCost(rows, bytes, 1, 0, config).total(config);
            return state == single_node_state ? broadcast : broadcast + toSingleNode(state, rows, bytes);
        }
        /// To one node: a gather.
        [[nodiscard]] double toSingleNode(Int8 state, double rows, double bytes) const
        {
            if (state == single_node_state)
                return 0;
            if (state == replicated_state)
                return impossible;
            return funnel(rows, bytes);
        }
        [[nodiscard]] double joinWork(
            double probe_rows, double build_rows, double build_bytes, double joined_rows, double probe_parallelism,
            double build_parallelism) const
        {
            return hashJoinWorkCost(probe_rows, build_rows, build_bytes, joined_rows, probe_parallelism, build_parallelism, config)
                .total(config);
        }
    };
    [[nodiscard]] DistributedJoinCost distributedJoinCost() const
    {
        return {*query_graph.distribution_cost_config, static_cast<double>(query_graph.distribution_node_count)};
    }
    /// The bytes of a row of the join of the relations.
    [[nodiscard]] double rowBytes(UInt32 relations) const;
    /// Adds the plans of a single relation besides its partitioned one: on one node, and on every node where Cascades
    /// may read it there.
    template <typename Entry>
    void addLeafStates(UInt32 relation_mask, Entry & leaf) const;

    [[nodiscard]] bool useConflictDetector() const
    {
        return query_graph.conflict_detector != JoinOrderConflictDetector::NONE;
    }
    [[nodiscard]] ConflictDetector conflictDetectorKind() const
    {
        return query_graph.conflict_detector == JoinOrderConflictDetector::CD_C ? ConflictDetector::CDC : ConflictDetector::CDA;
    }

    const VectorWithMemoryTracking<JoinActionRef *> & collectJoinEdgesMask(UInt32 left_mask, UInt32 right_mask);
    SelectivityEstimate computeSelectivityMask(const VectorWithMemoryTracking<JoinActionRef *> & edges, UInt32 left_mask, UInt32 right_mask);
    /// The limits of the distributed search: the defaults above, or lower ones from `QueryGraph`.
    size_t plan_pair_limit = max_distributed_plan_pairs;
    size_t partition_state_limit = max_partition_states;
    size_t distributed_plan_pairs = 0;
    /// Set when the distributed search went over its budget: its plans are not complete, so its result is not used.
    bool distribution_stopped = false;
    /// Set for the second search of a graph, after the distributed one stopped.
    bool distribution_disabled = false;

    QueryGraph & query_graph;
    SelectivityCache expression_selectivity;
    PlanMemo dp_table;

    /** Precomputed native-mask view of the query graph for the DPsub hot path. Populated once per
    * `solve` run by `initDPsubScratch`. Each `BitSet` is packed into a `UInt` type, the outer-join
    * restrictions are indexed by relation, and column-equivalence classes are indexed by relation so
    * `computeSelectivityMask` only visits the classes incident to the left side instead of rescanning
    * the whole equivalence map. The trailing fields are reusable scratch (no per-CCP allocation)
    */
    template <std::unsigned_integral TUInt>
    struct DPsubMaskData
    {
        VectorWithMemoryTracking<TUInt> edge_source_mask;   /// per edge: source relations
        VectorWithMemoryTracking<TUInt> edge_pin_mask;      /// per edge: relations that must all be present (if pinned)
        VectorWithMemoryTracking<char> edge_pinned;         /// per edge: whether a pin applies

        struct Restriction
        {
            TUInt required = 0;
            JoinKind kind = JoinKind::Inner;
            bool present = false;
        };
        VectorWithMemoryTracking<Restriction> restriction_by_rel; /// indexed by relation id

        /// A member of an equivalence class, resolved once: its relation and the distinct values of
        /// its column, which do not change during the search.
        struct EquivMember
        {
            TUInt relation_bit = 0;
            std::optional<UInt64> ndv;
            UInt64 ndv_upper_bound = 0;
        };
        VectorWithMemoryTracking<VectorWithMemoryTracking<EquivMember>> equiv_classes; /// members of each distinct equivalence class
        VectorWithMemoryTracking<VectorWithMemoryTracking<TUInt>> rel_to_classes; /// relation id -> indices into equiv_classes
        VectorWithMemoryTracking<TUInt> class_relations;             /// per equivalence class: relations of its members

        VectorWithMemoryTracking<UInt64> class_visited;        /// generation stamp per equivalence class
        UInt64 equiv_generation = 0;              /// bumped on each computeSelectivityMask call
        VectorWithMemoryTracking<JoinActionRef *> applicable_scratch; /// reused output of collectJoinEdgesMask

        /// Whether the relations fall apart once cross products are set aside, computed in
        /// `initDPsubScratch` from the masks below. DPsub builds the full set out of connected
        /// pieces, so such a graph is one it cannot plan.
        bool disconnected_graph = false;

        /// Per-operator conflict descriptors (CD-A or CD-C), populated in `initDPsubScratch` only
        /// when a conflict detector is enabled. When non-empty, `isValidJoinOrderMaskConflict` uses
        /// these (per-operator required-set + conflict rules) instead of the per-relation
        /// `restriction_by_rel`, which lets non-commutative outer and semi/anti joins be reordered.
        VectorWithMemoryTracking<ConflictOperator> conflict_operators;

        /// Per edge, with a conflict detector: the index in `conflict_operators` of the join step
        /// whose ON clause it comes from, and, for an inner join's conjunct, its outstanding relations:
        /// the relations besides its own that must be present before it may be placed. See
        /// `initDPsubScratch`.
        VectorWithMemoryTracking<size_t> edge_operator;
        VectorWithMemoryTracking<TUInt> edge_outstanding_mask;

        /// The columns of each equivalence class.
        VectorWithMemoryTracking<VectorWithMemoryTracking<JoinActionRef>> class_members;

        /// Only with `use_interesting_orders`, which needs the classes to fit into a 64-bit mask.
        bool interesting_orders = false;
        /// Only with `QueryGraph::distribution_node_count`, instead of `interesting_orders`. Needs the classes to fit into a
        /// 64-bit mask too.
        bool distribution = false;
        VectorWithMemoryTracking<char> class_single_type;   /// per class: one type up to `Nullable` and `LowCardinality`
        /// With `distribution`: the partitioning states that a later join can use, added as the search finds them.
        struct PartitionState
        {
            UInt32 key_class = 0; /// rows with equal values of the columns of the class are on one node
            UInt32 relations = 0; /// the relations whose column of the class Cascades knows the rows are hashed by
            /// For a class whose key the search chooses: the exact type of the columns the rows are hashed by, and
            /// `relations` is 0. The search finds these columns when it builds the plan, see `routedRelations`.
            Int32 type = -1;
        };
        VectorWithMemoryTracking<PartitionState> partition_states;
        UnorderedMapWithMemoryTracking<UInt64, Int8> partition_state_index; /// (class, relations) to its index in `partition_states`
        /// Per class whose key the search chooses: the relations of its columns of each exact type, and the state of rows
        /// hashed by the columns of each type (`no_state` until the search finds it).
        VectorWithMemoryTracking<VectorWithMemoryTracking<UInt32>> class_type_relations;
        VectorWithMemoryTracking<VectorWithMemoryTracking<Int8>> class_type_states;
        VectorWithMemoryTracking<ShuffleKey> shuffle_keys;    /// per edge
        VectorWithMemoryTracking<char> class_has_states;      /// per class: each relation has at most one column in the class
        UInt64 shuffle_classes = 0;              /// the classes whose columns have a common type to hash at
        VectorWithMemoryTracking<VectorWithMemoryTracking<Int32>> rel_prefix_classes; /// per relation: the classes it can be read in order of, in order
        VectorWithMemoryTracking<Int32> edge_class;          /// per edge: the class of an equality of two columns, or -1
        TUInt prepared_storage_mask = 0;
        /// Relations a sharded merge can split by primary key ranges on the class they are read in order of.
        TUInt shardable_relations = 0;
        /// Per relation: how many leading classes of its read order are its leading primary key columns.
        VectorWithMemoryTracking<UInt8> rel_primary_key_prefix;

        /// Per relation and class (`relation * num_classes + class`): the value range of its column of the class, if known.
        struct KeyRange
        {
            double min = 0;
            double max = 0;
            bool known = false;
        };
        VectorWithMemoryTracking<KeyRange> key_ranges;
    };
    DPsubMaskData<UInt32> dpsub_data;

    LoggerPtr log = DB::getJoinOrderOptimizerLogger();
};

std::optional<UInt64> DPSubJoinOrderOptimizer::estimateCardinality(
    std::optional<UInt64> left_rows, std::optional<UInt64> right_rows, const SelectivityEstimate & selectivity, JoinKind join_kind,
    JoinStrictness strictness) const
{
    return estimateJoinCardinality(left_rows, right_rows, selectivity, join_kind, strictness);
}

void DPSubJoinOrderOptimizer::initDPsubScratch()
{
    using EquivClass = EquivalenceClasses<JoinActionRef>::Class;

    const size_t num_relations = query_graph.relation_stats.size();
    const auto & edges = query_graph.edges;

    /// Edges: precompute the source-relation mask and any pin mask once per edge
    auto n = edges.size();
    dpsub_data.edge_source_mask.assign(n, 0);
    dpsub_data.edge_pin_mask.assign(n, 0);
    dpsub_data.edge_pinned.assign(n, 0);
    for (size_t i = 0; i < n; ++i)
    {
        const auto & edge = edges[i];
        if (!edge)
            continue;
        dpsub_data.edge_source_mask[i] = toMask(edge.getSourceRelations());
        /// ON-clause predicates of an outer join are pinned to its null-supplying side; the pin
        /// becomes applicable exactly when that whole side is joined.
        if (auto pin_it = query_graph.outer_join_conditions.find(edge); pin_it != query_graph.outer_join_conditions.end())
        {
            dpsub_data.edge_pinned[i] = 1;
            dpsub_data.edge_pin_mask[i] = toMask(pin_it->second);
        }
    }

    /// Outer-join reordering constraints. Two mutually exclusive representations:
    ///  - default: per-relation ON-clause restrictions from `query_graph.join_kinds`, consumed by
    ///    `isValidJoinOrderMask`. Handles inner + outer joins only.
    ///  - conflict detector enabled: per-operator descriptors from CD-A or CD-C, consumed by
    ///    `isValidJoinOrderMaskConflict`. Handles inner/outer *and* semi/anti joins, with orientation.
    dpsub_data.restriction_by_rel.assign(num_relations, {});
    dpsub_data.conflict_operators.clear();
    dpsub_data.edge_operator.assign(n, 0);
    dpsub_data.edge_outstanding_mask.assign(n, 0);
    if (useConflictDetector())
    {
        VectorWithMemoryTracking<ConflictOpMask> ops;
        ops.reserve(query_graph.conflict_ops.size());
        for (const auto & op : query_graph.conflict_ops)
            ops.push_back(ConflictOpMask{toMask(op.left), toMask(op.right), toMask(op.nel), toMask(op.nr_rels), op.kind, op.strictness});

        dpsub_data.conflict_operators = computeConflictOperators(ops, conflictDetectorKind(), log);

        /// An inner join step does not fix the order of its inputs, and its ON clause is split into
        /// conjuncts, filters placed each at the lowest join where it can be evaluated. Neither may
        /// get ahead of a join nested under the step whose padded side it reads: below an outer,
        /// semi or anti join, a filter may only go onto the preserved side, or it would remove rows
        /// before that join pads or matches them. So such a nested join must have been applied first:
        /// the relations locating it are outstanding relations of the step, and of each of its
        /// conjuncts, which may only be applied once they are present.
        ///
        /// Exactly those, and not all relations of the nested join, keep a conjunct that reads two
        /// relations spanning the join it is placed at: the last of its relations and its outstanding
        /// relations to be joined is either one of its own, or one locating the nested join, which is
        /// then applied at that same join and takes the conjunct as a filter after it. More outstanding
        /// relations would place it higher, with both of its relations on one input, as a one-sided
        /// condition of an inner join that some join algorithms (`full_sorting_merge`) cannot execute.
        auto & operators = dpsub_data.conflict_operators;
        auto outstanding_relations = [&](UInt32 reads, size_t step) -> UInt32
        {
            const UInt32 step_relations = operators[step].relations;
            UInt32 outstanding = 0;
            for (const auto & nested : operators)
            {
                const bool is_nested = (nested.relations & ~step_relations) == 0 && nested.relations != step_relations;
                if (is_nested && (reads & nested.nullSupplying()))
                    outstanding |= nested.locatedBy();
            }
            return outstanding;
        };

        /// The inner steps first, from the ON clauses as written: `nel` is where `isAppliedAt` puts them.
        VectorWithMemoryTracking<UInt32> step_outstanding(operators.size(), 0);
        for (size_t step = 0; step < operators.size(); ++step)
            if (operators[step].freely_reorderable && !operators[step].degenerate)
                step_outstanding[step] = outstanding_relations(operators[step].nel, step);
        for (size_t step = 0; step < operators.size(); ++step)
            operators[step].nel |= step_outstanding[step];

        /// Where each condition is placed follows from the join step it comes from, see
        /// `collectJoinEdgesMask`: an outer, semi or anti join's condition goes where that join is
        /// applied, an inner join's conjunct as low as its relations and its outstanding relations allow.
        if (query_graph.edge_conflict_ops.size() != edges.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Join graph has {} conditions, but join steps are recorded for {} of them",
                edges.size(), query_graph.edge_conflict_ops.size());

        for (size_t i = 0; i < edges.size(); ++i)
        {
            if (!edges[i])
                continue;
            const size_t op_index = query_graph.edge_conflict_ops[i];
            if (op_index >= operators.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Join condition {} refers to join step {}, but there are only {}",
                    i, op_index, operators.size());
            dpsub_data.edge_operator[i] = op_index;
            if (dpsub_data.edge_pinned[i])
                continue;
            /// A constant conjunct reads no relation, but it filters the result of its own step: all
            /// relations of the step are outstanding for it, so it is neither applied below a join
            /// nested under the step nor to relations outside of it.
            const UInt32 sources = dpsub_data.edge_source_mask[i];
            dpsub_data.edge_outstanding_mask[i] = sources ? outstanding_relations(sources, op_index) : operators[op_index].relations;
        }
        LOG_TRACE(log, "DPsub: using {} conflict detector over {} captured join operators",
                  toString(query_graph.conflict_detector), dpsub_data.conflict_operators.size());
    }
    else
    {
        for (const auto & [rel, restriction] : query_graph.join_kinds)
        {
            if (rel >= num_relations)
                continue;
            auto & native = dpsub_data.restriction_by_rel[rel];
            native.required = toMask(restriction.first);
            native.kind = restriction.second;
            native.present = true;
        }
    }

    /// Column-equivalence classes: build a per-relation incidence list so selectivity only
    /// visits the classes touching the left side instead of rescanning the whole member map
    dpsub_data.equiv_classes.clear();
    dpsub_data.class_relations.clear();
    dpsub_data.class_members.clear();
    dpsub_data.rel_to_classes.assign(num_relations, {});
    UnorderedMapWithMemoryTracking<const EquivClass *, UInt32> class_index;
    for (const auto & [member, class_ptr] : query_graph.column_equivalences.getMemberToClassMap())
    {
        if (!class_ptr)
            continue;
        auto rel = member.getSourceRelations().getSingleBit();
        if (!rel || *rel >= num_relations)
            continue;

        auto [it, inserted] = class_index.try_emplace(class_ptr.get(), static_cast<UInt32>(dpsub_data.equiv_classes.size()));
        if (inserted)
        {
            auto & members = dpsub_data.equiv_classes.emplace_back();
            auto & relations = dpsub_data.class_relations.emplace_back(0);
            auto & columns = dpsub_data.class_members.emplace_back();
            for (const auto & equiv_member : *class_ptr)
            {
                const auto & sources = equiv_member.getSourceRelations();
                auto relation = sources.getSingleBit();
                if (!relation || *relation >= num_relations)
                    continue;
                auto ndv = getColumnStats(query_graph, dp_table, sources, equiv_member.getColumnName());
                members.push_back({static_cast<UInt32>(1) << *relation, ndv.ndv, ndv.upper_bound});
                relations |= members.back().relation_bit;
                columns.push_back(equiv_member);
            }
        }
        dpsub_data.rel_to_classes[*rel].push_back(it->second);
    }
    dpsub_data.class_visited.assign(dpsub_data.equiv_classes.size(), 0);
    dpsub_data.equiv_generation = 0;

    const size_t num_classes = dpsub_data.class_members.size();
    const auto joining_classes
        = std::ranges::count_if(dpsub_data.class_relations, [](UInt32 relations) { return std::popcount(relations) > 1; });
    dpsub_data.distribution = query_graph.distribution_node_count > 1 && query_graph.distribution_cost_config && !distribution_disabled
        && num_classes <= std::numeric_limits<UInt64>::digits && static_cast<size_t>(joining_classes) <= max_distribution_classes;
    dpsub_data.interesting_orders = !dpsub_data.distribution && query_graph.use_interesting_orders
        && num_classes <= std::numeric_limits<UInt64>::digits;
    dpsub_data.class_single_type.assign(num_classes, 0);
    dpsub_data.partition_states.clear();
    dpsub_data.partition_state_index.clear();
    dpsub_data.class_type_relations.assign(num_classes, {});
    dpsub_data.class_type_states.assign(num_classes, {});
    dpsub_data.shuffle_keys.assign(n, {});
    dpsub_data.class_has_states.assign(num_classes, 0);
    dpsub_data.shuffle_classes = 0;
    if (dpsub_data.distribution)
        initShuffleKeys(class_index);
    dpsub_data.rel_prefix_classes.assign(num_relations, {});
    dpsub_data.edge_class.assign(n, -1);
    dpsub_data.prepared_storage_mask = toMask(query_graph.prepared_storage_relations);
    dpsub_data.shardable_relations = 0;
    dpsub_data.rel_primary_key_prefix.assign(num_relations, 0);
    dpsub_data.key_ranges.assign(dpsub_data.interesting_orders ? num_relations * num_classes : 0, {});
    if (dpsub_data.interesting_orders)
    {
        /// The columns of each relation that can be merge keys; a relation with two columns in a class is not sorted by the
        /// class until they are equated.
        VectorWithMemoryTracking<NameSet> candidates(num_relations);
        MapWithMemoryTracking<std::pair<size_t, String>, Int32> column_class;
        for (size_t c = 0; c < num_classes; ++c)
        {
            const auto & columns = dpsub_data.class_members[c];
            auto type = removeLowCardinalityAndNullable(columns.front().getType());
            dpsub_data.class_single_type[c] = std::ranges::all_of(columns, [&](const JoinActionRef & column)
            {
                return removeLowCardinalityAndNullable(column.getType())->equals(*type);
            });

            UnorderedMapWithMemoryTracking<size_t, size_t> columns_per_relation;
            for (const auto & column : columns)
                ++columns_per_relation[*column.getSourceRelations().getSingleBit()];
            for (const auto & column : columns)
            {
                const size_t relation = *column.getSourceRelations().getSingleBit();
                if (columns_per_relation[relation] != 1)
                    continue;
                candidates[relation].insert(column.getColumnName());
                column_class.emplace(std::make_pair(relation, column.getColumnName()), static_cast<Int32>(c));

                const auto & column_stats = query_graph.relation_stats[relation].column_stats;
                if (auto it = column_stats.find(column.getColumnName()); it != column_stats.end())
                {
                    auto min = toNumber(it->second.min_value);
                    auto max = toNumber(it->second.max_value);
                    if (min && max && *min <= *max)
                        dpsub_data.key_ranges[relation * num_classes + c] = {*min, *max, true};
                }
            }
        }

        /// Probing the storage order is not free, so it waits until DPsub plans this graph with interesting orders.
        if (query_graph.find_read_in_order_prefixes)
        {
            const auto prefixes = query_graph.find_read_in_order_prefixes(candidates);
            for (size_t relation = 0; relation < num_relations && relation < prefixes.size(); ++relation)
            {
                for (const auto & name : prefixes[relation].prefix)
                    dpsub_data.rel_prefix_classes[relation].push_back(column_class.at({relation, name}));
                const size_t primary_key_prefix = std::min(prefixes[relation].primary_key_prefix, prefixes[relation].prefix.size());
                dpsub_data.rel_primary_key_prefix[relation] = static_cast<UInt8>(primary_key_prefix);
                if (primary_key_prefix > 0)
                    dpsub_data.shardable_relations |= static_cast<UInt32>(1) << relation;
            }
        }

        for (size_t i = 0; i < n; ++i)
        {
            if (!edges[i])
                continue;
            auto [op, lhs, rhs] = edges[i].asBinaryPredicate();
            if (op != JoinConditionOperator::Equals)
                continue;
            auto lhs_class = query_graph.column_equivalences.getClass(lhs.resolveAliases());
            auto rhs_class = query_graph.column_equivalences.getClass(rhs.resolveAliases());
            if (!lhs_class || lhs_class != rhs_class)
                continue;
            if (auto it = class_index.find(lhs_class.get()); it != class_index.end())
                dpsub_data.edge_class[i] = static_cast<Int32>(it->second);
        }

        /// Without an input sorted by a join key no merge join beats a hash join, so costs stay as they are.
        dpsub_data.interesting_orders = std::ranges::any_of(dpsub_data.rel_prefix_classes, [](const auto & prefix) { return !prefix.empty(); });
    }

    /// Connectivity: every predicate over two or more relations, every cross product (it links its two
    /// sides, see `QueryGraph::cross_product_links`), plus, with a conflict detector, one link per
    /// operator whose predicate does not span its two sides. What is still disconnected after that
    /// cannot be stitched together by DPsub.
    VectorWithMemoryTracking<size_t> component(num_relations);
    for (size_t i = 0; i < num_relations; ++i)
        component[i] = i;

    auto find = [&component](size_t x)
    {
        while (component[x] != x)
        {
            component[x] = component[component[x]];
            x = component[x];
        }
        return x;
    };
    auto unite = [&](UInt32 a_mask, UInt32 b_mask)
    {
        if (!a_mask || !b_mask)
            return;
        const size_t ra = find(static_cast<size_t>(std::countr_zero(a_mask)));
        const size_t rb = find(static_cast<size_t>(std::countr_zero(b_mask)));
        if (ra != rb)
            component[rb] = ra;
    };

    /// A condition over three or more tables (`a.x + b.x = c.x`) ties all of them together, just as a
    /// two-table one ties its pair.
    for (const UInt32 sources : dpsub_data.edge_source_mask)
    {
        if (std::popcount(sources) < 2)
            continue;
        const UInt32 lowest = sources & (~sources + 1);
        for (UInt32 rest = sources & ~lowest; rest; rest &= rest - 1)
            unite(lowest, rest & (~rest + 1));
    }

    /// An operator counts here only when degenerate - its predicate does not span its two sides. A
    /// spanning predicate is already linked by its edge above, and uniting the operator's whole
    /// subtrees instead would attach a relation that only a nested cross product holds on (`t1` in
    /// `t1 CROSS JOIN t2 JOIN t3 ON t2.k = t3.k`); that one is linked by its cross product below.
    for (const auto & op : dpsub_data.conflict_operators)
    {
        if (isCrossOrComma(op.kind) || !op.degenerate)
            continue;
        unite(op.left_relations, op.relations & ~op.left_relations);
    }

    for (const auto & [left, right] : query_graph.cross_product_links)
        unite(toMask(left), toMask(right));

    dpsub_data.disconnected_graph = false;
    for (size_t i = 1; i < num_relations; ++i)
    {
        if (find(i) != find(0))
        {
            dpsub_data.disconnected_graph = true;
            break;
        }
    }
}

std::optional<JoinKind> DPSubJoinOrderOptimizer::isValidJoinOrderMask(UInt32 left_mask, UInt32 right_mask) const
{
    auto check = [&](UInt32 lhs, UInt32 rhs) -> std::optional<JoinKind>
    {
        if (std::popcount(lhs) == 1)
        {
            const auto & restriction = dpsub_data.restriction_by_rel[std::countr_zero(lhs)];
            if (restriction.present)
            {
                /// If there are any bits set in `restriction.required`
                /// that are not set in `rhs`, the bitwise AND (&) results in a non-zero value
                if (restriction.required & ~rhs)
                    return {};
                return restriction.kind;
            }
        }
        return JoinKind::Inner;
    };

    JoinKind left_join_type = JoinKind::Inner;
    JoinKind right_join_type = JoinKind::Inner;

    if (auto res = check(left_mask, right_mask))
        left_join_type = isLeftOrFull(res.value()) ? reverseJoinKind(res.value()) : res.value();
    else
        return {};

    if (auto res = check(right_mask, left_mask))
        right_join_type = isRightOrFull(res.value()) ? reverseJoinKind(res.value()) : res.value();
    else
        return {};

    if (left_join_type == JoinKind::Inner)
        return right_join_type;
    if (right_join_type == JoinKind::Inner)
        return left_join_type;
    if (left_join_type == JoinKind::Full && right_join_type == JoinKind::Full)
        return JoinKind::Full;

    /// Conflicting outer-join constraints, the order is not possible
    return {};
}

std::optional<std::pair<JoinKind, JoinStrictness>>
DPSubJoinOrderOptimizer::isValidJoinOrderMaskConflict(UInt32 left_mask, UInt32 right_mask) const
{
    /// Decide whether this (left_mask, right_mask) split is a legal join and what kind/strictness it
    /// produces. Every operator applied across the split must pass: its required-left relations must
    /// sit in S1 and required-right in S2 (or mirrored), and each conflict rule T1 -> T2 must hold
    /// (if any table of T1 is joined, all of T2 must be too). Inner operators are checked too, since
    /// a nested operator's conflict with its parent is recorded on the parent, which may be inner.
    /// The single non-inner operator that crosses fixes the kind; two cannot share one node.
    const UInt32 combined = left_mask | right_mask;
    auto subset_of = [](const UInt32 a, const UInt32 b) { return (a & ~b) == 0; };

    JoinKind kind = JoinKind::Inner;
    JoinStrictness strictness = JoinStrictness::All;
    bool have_non_inner = false;
    bool any_involved = false;
    /// Whether anything applied here actually ties the two sides together, as opposed to a cross
    /// product, which is applied across the split while joining the sides on nothing.
    bool have_connecting_op = false;

    for (const auto & op : dpsub_data.conflict_operators)
    {
        /// The same notion places the operator's ON condition, see `collectJoinEdgesMask`.
        if (!op.isAppliedAt(left_mask, right_mask))
            continue;
        any_involved = true;

        /// Conflict rules (CD-C; empty for CD-A). A rule T1 -> T2 is disobeyed when some table of T1
        /// is already in the joined set S but not all of T2 is -- that ordering would apply this
        /// operator before a conflicting operand is in place.
        for (const auto & rule : op.rules)
            if ((rule.t1 & combined) && (rule.t2 & ~combined))
                return std::nullopt;

        /// Required-set containment. `forward` keeps the operator's (left, right) inputs aligned
        /// with (left_mask, right_mask); `mirrored` swaps them: a valid equivalence for any of our
        /// operators via `reverseJoinKind` (Left<->Right, Full/Inner unchanged; for semi/anti it
        /// flips the preserved side). The two required sides are disjoint and, for a non-degenerate
        /// predicate, both non-empty, so at most one orientation can hold.
        bool forward = subset_of(op.required_left, left_mask) && subset_of(op.required_right, right_mask);
        bool mirrored = subset_of(op.required_left, right_mask) && subset_of(op.required_right, left_mask);

        /// A one-sided/cross-product operator has an empty required side, so the containment above
        /// cannot orient it. Require each whole input subtree on its own side instead: this orients
        /// it and rejects a fragmented split that pulls part of a subtree across the outer-join
        /// boundary (an invalid plan). This is a very rare condition.
        if (op.degenerate)
        {
            const UInt32 right_relations = op.relations & ~op.left_relations;
            forward = forward && subset_of(op.left_relations, left_mask) && subset_of(right_relations, right_mask);
            mirrored = mirrored && subset_of(op.left_relations, right_mask) && subset_of(right_relations, left_mask);
        }

        if (!forward && !mirrored)
            return std::nullopt;

        /// Inner joins impose no join kind and are commutative; only their gate matters. A cross
        /// product is equally reorderable, but unlike an inner join its kind is worth keeping, so
        /// note it and read it off below. It must not go through the kind-fixing path underneath:
        /// that rejects a second such operator at one node, and two cross products there are legal.
        if (op.freely_reorderable)
        {
            if (!isCrossOrComma(op.kind))
                have_connecting_op = true;
            continue;
        }

        /// A non-inner operator fixes the kind. Two of them at one node -> impossible order.
        if (have_non_inner)
            return std::nullopt;
        have_non_inner = true;

        /// A non-inner operator whose orientation is still ambiguous (both sides admissible) would
        /// have its preserved side guessed; fail closed instead (see `reverseJoinKind`, which flips
        /// Left<->Right / the semi/anti preserved side while `buildPhysicalPlan` keeps the child order).
        if (forward && mirrored)
            return std::nullopt;

        kind = forward ? op.kind : reverseJoinKind(op.kind);
        strictness = op.strictness;
    }

    /// No operator is applied across this split. A real predicate always maps to an operator, so the
    /// only legitimate no-operator split is a transitive inner join (two sides tied by a column
    /// equivalence, no direct predicate). Anything else reaching here is the synthetic cross-product
    /// connectivity with no operator spanning it, thus reject rather than invent an inner join.
    if (!any_involved
        && !hasInnerEdgeAcross(left_mask, right_mask)
        && !areTransitivelyConnectedMask(left_mask, right_mask))
        return std::nullopt;

    /// Only cross products were applied, and no operator fixed a kind, so the two sides are joined
    /// on nothing. Report that rather than the `Inner` this started as: an unconditioned join is a
    /// cross product, and `applyParallelReplicas` and `EXPLAIN` both go by the kind.
    if (kind == JoinKind::Inner && any_involved && !have_non_inner && !have_connecting_op
        && !hasInnerEdgeAcross(left_mask, right_mask)
        && !areTransitivelyConnectedMask(left_mask, right_mask))
        kind = JoinKind::Cross;

    return std::make_pair(kind, strictness);
}

bool DPSubJoinOrderOptimizer::hasInnerEdgeAcross(UInt32 left_mask, UInt32 right_mask) const
{
    const UInt32 combined = left_mask | right_mask;
    for (size_t i = 0; i < dpsub_data.edge_source_mask.size(); ++i)
    {
        if (dpsub_data.edge_pinned[i])
            continue;
        const UInt32 sources = dpsub_data.edge_source_mask[i];
        if ((sources | dpsub_data.edge_outstanding_mask[i]) & ~combined)
            continue;
        if (!(sources & left_mask) || !(sources & right_mask))
            continue;
        /// A conjunct joining the two sides applies part of its join step here, so the step's conflict
        /// rules must hold for the joined relations, or the conjunct does not join them.
        bool rule_violated = false;
        for (const auto & rule : dpsub_data.conflict_operators[dpsub_data.edge_operator[i]].rules)
            if ((rule.t1 & combined) && (rule.t2 & ~combined))
                rule_violated = true;
        if (!rule_violated)
            return true;
    }
    return false;
}

std::optional<JoinKind> DPSubJoinOrderOptimizer::writtenSemiAntiKind(UInt32 left_mask, UInt32 right_mask, JoinStrictness strictness) const
{
    const auto & operators = dpsub_data.conflict_operators;
    for (size_t i = 0; i < operators.size(); ++i)
    {
        if (operators[i].strictness == strictness && operators[i].isAppliedAt(left_mask, right_mask))
            return query_graph.conflict_ops[i].kind;
    }
    return std::nullopt;
}

bool DPSubJoinOrderOptimizer::areTransitivelyConnectedMask(UInt32 left_mask, UInt32 right_mask) const
{
    for (const UInt32 relations : dpsub_data.class_relations)
    {
        if ((relations & left_mask) && (relations & right_mask))
            return true;
    }
    return false;
}

UInt64 DPSubJoinOrderOptimizer::mergeKeyClasses(
    const VectorWithMemoryTracking<JoinActionRef *> & edges, UInt32 left_mask, UInt32 right_mask, JoinKind kind, JoinStrictness strictness) const
{
    if (!dpsub_data.interesting_orders || kind != JoinKind::Inner || strictness != JoinStrictness::All
        || query_graph.join_strictness != JoinStrictness::All)
        return 0;

    /// `full_sorting_merge` reads neither from a prepared join storage nor evaluates other conditions.
    for (const UInt32 side : {left_mask, right_mask})
        if (std::popcount(side) == 1 && (side & dpsub_data.prepared_storage_mask))
            return 0;
    for (const auto * edge : edges)
        if (dpsub_data.edge_class[edge - query_graph.edges.data()] < 0)
            return 0;

    UInt64 classes = 0;
    for (size_t c = 0; c < dpsub_data.class_relations.size(); ++c)
    {
        const UInt32 relations = dpsub_data.class_relations[c];
        if (dpsub_data.class_single_type[c] && (relations & left_mask) && (relations & right_mask))
            classes |= static_cast<UInt64>(1) << c;
    }
    return classes;
}

UInt64 DPSubJoinOrderOptimizer::partitionKeyClasses(UInt32 left_mask, UInt32 right_mask) const
{
    UInt64 classes = 0;
    for (size_t c = 0; c < dpsub_data.class_relations.size(); ++c)
    {
        const UInt32 relations = dpsub_data.class_relations[c];
        if ((relations & left_mask) && (relations & right_mask))
            classes |= static_cast<UInt64>(1) << c;
    }
    return classes;
}

void DPSubJoinOrderOptimizer::initShuffleKeys(
    const UnorderedMapWithMemoryTracking<const EquivalenceClasses<JoinActionRef>::Class *, UInt32> & class_index)
{
    for (size_t c = 0; c < dpsub_data.class_members.size(); ++c)
    {
        const auto & columns = dpsub_data.class_members[c];
        DataTypes types;
        UnorderedSetWithMemoryTracking<size_t> relations;
        for (const auto & column : columns)
        {
            types.push_back(column.getType());
            relations.insert(*column.getSourceRelations().getSingleBit());
        }
        const auto type = removeLowCardinalityAndNullable(types.front());
        dpsub_data.class_single_type[c] = std::ranges::all_of(types, [&](const DataTypePtr & column_type)
        {
            return removeLowCardinalityAndNullable(column_type)->equals(*type);
        });
        if (tryGetLeastSupertype(types))
            dpsub_data.shuffle_classes |= static_cast<UInt64>(1) << c;
        dpsub_data.class_has_states[c] = relations.size() == columns.size();

        if (choosesShuffleKey(static_cast<UInt32>(c)))
        {
            auto & type_relations = dpsub_data.class_type_relations[c];
            VectorWithMemoryTracking<DataTypePtr> group_types;
            for (const auto & column : columns)
            {
                const auto relation_bit = static_cast<UInt32>(1) << *column.getSourceRelations().getSingleBit();
                const auto group = std::ranges::find_if(
                    group_types, [&](const DataTypePtr & group_type) { return group_type->equals(*column.getType()); });
                if (group == group_types.end())
                {
                    group_types.push_back(column.getType());
                    type_relations.push_back(relation_bit);
                }
                else
                    type_relations[group - group_types.begin()] |= relation_bit;
            }
            dpsub_data.class_type_states[c].assign(type_relations.size(), no_state);
        }
    }

    for (size_t i = 0; i < query_graph.edges.size(); ++i)
    {
        const auto & edge = query_graph.edges[i];
        if (!edge)
            continue;
        auto [op, lhs, rhs] = edge.asBinaryPredicate();
        if (op != JoinConditionOperator::Equals)
            continue;
        auto & key = dpsub_data.shuffle_keys[i];
        key.raw = lhs.getType()->equals(*rhs.getType());
        key.hashable = key.raw || tryGetLeastSupertype(DataTypes{lhs.getType(), rhs.getType()});
        key.lhs_relations = toMask(lhs.getSourceRelations());
        key.rhs_relations = toMask(rhs.getSourceRelations());

        const auto lhs_class = query_graph.column_equivalences.getClass(lhs.resolveAliases());
        if (!lhs_class || lhs_class != query_graph.column_equivalences.getClass(rhs.resolveAliases()))
            continue;
        const auto it = class_index.find(lhs_class.get());
        const auto lhs_relation = lhs.getSourceRelations().getSingleBit();
        const auto rhs_relation = rhs.getSourceRelations().getSingleBit();
        if (it == class_index.end() || !lhs_relation || !rhs_relation)
            continue;
        key.key_class = static_cast<Int32>(it->second);
        key.lhs_relation = static_cast<UInt8>(*lhs_relation);
        key.rhs_relation = static_cast<UInt8>(*rhs_relation);
    }
}

Int8 DPSubJoinOrderOptimizer::partitionState(UInt32 key_class, UInt32 relations, Int32 type)
{
    if (!dpsub_data.class_has_states[key_class])
        return any_partitioning_state;
    auto & states = dpsub_data.partition_states;
    Int8 * known = nullptr;
    if (type >= 0)
    {
        known = &dpsub_data.class_type_states[key_class][type];
        if (*known != no_state)
            return *known;
    }
    else
    {
        const UInt64 key = (static_cast<UInt64>(key_class) << 32) | relations;
        if (auto it = dpsub_data.partition_state_index.find(key); it != dpsub_data.partition_state_index.end())
            return it->second;
    }
    Int8 state = any_partitioning_state;
    if (states.size() < partition_state_limit)
    {
        states.push_back({.key_class = key_class, .relations = relations, .type = type});
        state = static_cast<Int8>(states.size() - 1);
    }
    if (known)
        *known = state;
    else
        dpsub_data.partition_state_index.emplace((static_cast<UInt64>(key_class) << 32) | relations, state);
    return state;
}

UInt64 DPSubJoinOrderOptimizer::shuffleKeyTypes(UInt32 key_class, UInt32 left_mask, UInt32 right_mask) const
{
    UInt64 types = 0;
    const auto & type_relations = dpsub_data.class_type_relations[key_class];
    for (size_t type = 0; type < type_relations.size() && type < 64; ++type)
        if ((type_relations[type] & left_mask) && (type_relations[type] & right_mask))
            types |= static_cast<UInt64>(1) << type;
    return types;
}

const JoinActionRef & DPSubJoinOrderOptimizer::classColumn(UInt32 key_class, UInt32 relation) const
{
    for (const auto & column : dpsub_data.class_members[key_class])
        if (*column.getSourceRelations().getSingleBit() == relation)
            return column;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Relation {} has no column of equivalence class {}", relation, key_class);
}

double DPSubJoinOrderOptimizer::rowBytes(UInt32 relations) const
{
    double bytes = 0;
    for (UInt32 remaining = relations; remaining; remaining &= remaining - 1)
        bytes += query_graph.distribution_inputs[std::countr_zero(remaining)].row_bytes;
    return std::max(bytes, 1.0);
}

template <typename Entry>
void DPSubJoinOrderOptimizer::addLeafStates(UInt32 relation_mask, Entry & leaf) const
{
    const auto & input = query_graph.distribution_inputs[std::countr_zero(relation_mask)];
    const double rows = leaf.estimated_rows ? static_cast<double>(*leaf.estimated_rows) : std::max(input.scanned_rows, 1.0);
    leaf.search_rows = rows;
    const double bytes = rowBytes(relation_mask);
    const auto join_cost = distributedJoinCost();
    typename Entry::Plan plan = leaf;

    if (input.placement != QueryGraph::InputPlacement::Read)
    {
        /// The plan of a subquery is costed by Cascades the same way in every join order, so only moving its result is
        /// costed here.
        leaf.cost = 0;
        if (input.placement == QueryGraph::InputPlacement::SingleNode)
        {
            leaf.partition_state = single_node_state;
            plan.partition_state = any_partitioning_state;
            plan.cost = join_cost.toPartitioned(single_node_state, rows, bytes);
        }
        else
        {
            leaf.partition_state = any_partitioning_state;
            plan.partition_state = single_node_state;
            plan.cost = join_cost.toSingleNode(any_partitioning_state, rows, bytes);
        }
        leaf.ordered.push_back(plan);
        return;
    }

    /// Cascades costs a read by the bytes it scans: 1/N of them per node for a partitioned read, all of them for the
    /// others. A filter that the primary key does not prune still scans the granules it selects.
    const double read = query_graph.distribution_cost_config->work_weight * std::max(rows, input.scanned_rows) * bytes;
    leaf.cost = read / static_cast<double>(query_graph.distribution_node_count);
    leaf.partition_state = any_partitioning_state;

    plan.cost = read;
    plan.partition_state = single_node_state;
    leaf.ordered.push_back(plan);
    if (input.can_read_on_every_node)
    {
        plan.partition_state = replicated_state;
        leaf.ordered.push_back(plan);
    }
}

bool DPSubJoinOrderOptimizer::isReadInOrder(UInt32 relation_mask, Int32 order_class) const
{
    const auto & prefix = dpsub_data.rel_prefix_classes[std::countr_zero(relation_mask)];
    return !prefix.empty() && prefix.front() == order_class;
}

double DPSubJoinOrderOptimizer::mergeJoinCost(
    MergeJoinInput left, UInt32 left_mask, MergeJoinInput right, UInt32 right_mask, Int32 order_class,
    const SelectivityEstimate & selectivity) const
{
    /// On several keys an input sorted by the merge key only is still sorted by the other keys within its groups.
    /// The keys follow the order of the left input if it covers them (see `buildPhysicalPlan`), else of the right one.
    const UInt64 key_classes = spanningClasses(left_mask, right_mask);
    if (std::popcount(key_classes) > 1)
    {
        const auto left_order = left.sorted ? keyOrder(left_mask, key_classes, order_class) : VectorWithMemoryTracking<Int32>{};
        const auto right_order = right.sorted ? keyOrder(right_mask, key_classes, order_class) : VectorWithMemoryTracking<Int32>{};
        const auto & emitted_order = left_order.empty() ? right_order : left_order;
        if (left.sorted && (left_order.empty() || left_order != emitted_order))
            left.sort_groups = distinctValues(left_mask, order_class, left.rows);
        if (right.sorted && (right_order.empty() || right_order != emitted_order))
            right.sort_groups = distinctValues(right_mask, order_class, right.rows);
    }
    const bool sharded = isShardedMerge(left, left_mask, right, right_mask);
    double probe_fraction = 1.0;
    if (sharded)
        probe_fraction = left.rows >= right.rows ? probeReadFraction(left_mask, right_mask) : probeReadFraction(right_mask, left_mask);
    return computeMergeJoinCost(left, right, selectivity, sharded, probe_fraction);
}

bool DPSubJoinOrderOptimizer::isShardedMerge(
    const MergeJoinInput & left, UInt32 left_mask, const MergeJoinInput & right, UInt32 right_mask) const
{
    if (!query_graph.shard_merge_by_pk_ranges || !left.shardable || !right.shardable)
        return false;
    /// One key range is one shard, whatever the other side holds.
    const UInt64 key_classes = spanningClasses(left_mask, right_mask);
    for (const UInt32 side : {left_mask, right_mask})
        if (std::popcount(side) == 1 && hasConstantShardKey(side, key_classes))
            return false;
    return true;
}

bool DPSubJoinOrderOptimizer::hasConstantShardKey(UInt32 relation_mask, UInt64 key_classes) const
{
    const size_t relation = std::countr_zero(relation_mask);
    const auto & prefix = dpsub_data.rel_prefix_classes[relation];
    size_t used = 0;
    for (; used < dpsub_data.rel_primary_key_prefix[relation] && ((key_classes >> prefix[used]) & 1); ++used)
    {
        const auto & members = dpsub_data.equiv_classes[prefix[used]];
        const bool constant = std::ranges::any_of(members, [&](const auto & member)
        {
            return (member.relation_bit & relation_mask) && member.ndv && *member.ndv <= 1;
        });
        if (!constant)
            return false;
    }
    return used > 0;
}

UInt64 DPSubJoinOrderOptimizer::mergeKeysClasses(UInt32 left_mask, UInt32 right_mask) const
{
    UInt64 key_classes = 0;
    for (UInt64 classes = spanningClasses(left_mask, right_mask); classes; classes &= classes - 1)
    {
        const auto key_class = std::countr_zero(classes);
        if (dpsub_data.class_single_type[key_class])
            key_classes |= static_cast<UInt64>(1) << key_class;
    }
    return key_classes;
}

UInt64 DPSubJoinOrderOptimizer::spanningClasses(UInt32 left_mask, UInt32 right_mask) const
{
    UInt64 classes = 0;
    for (size_t c = 0; c < dpsub_data.class_relations.size(); ++c)
    {
        const UInt32 relations = dpsub_data.class_relations[c];
        if ((relations & left_mask) && (relations & right_mask))
            classes |= static_cast<UInt64>(1) << c;
    }
    return classes;
}

VectorWithMemoryTracking<Int32> DPSubJoinOrderOptimizer::keyOrder(UInt32 relation_mask, UInt64 key_classes, Int32 order_class) const
{
    if (std::popcount(relation_mask) != 1)
        return {};
    const auto & prefix = dpsub_data.rel_prefix_classes[std::countr_zero(relation_mask)];
    const auto num_keys = static_cast<size_t>(std::popcount(key_classes));
    if (prefix.size() < num_keys || prefix.front() != order_class)
        return {};
    UInt64 covered = 0;
    for (size_t i = 0; i < num_keys; ++i)
        covered |= static_cast<UInt64>(1) << prefix[i];
    if (covered != key_classes)
        return {};
    return {prefix.begin(), prefix.begin() + num_keys};
}

double DPSubJoinOrderOptimizer::distinctValues(UInt32 relations, Int32 order_class, UInt64 rows) const
{
    UInt64 ndv = 0;
    for (const auto & member : dpsub_data.equiv_classes[order_class])
        if ((member.relation_bit & relations) && member.ndv)
            ndv = std::max(ndv, *member.ndv);
    return static_cast<double>(std::clamp<UInt64>(ndv, 1, std::max<UInt64>(rows, 1)));
}

double DPSubJoinOrderOptimizer::hashJoinWork(
    UInt32 left_mask, std::optional<UInt64> left_rows, UInt32 right_mask, std::optional<UInt64> right_rows) const
{
    if (!dpsub_data.interesting_orders)
        return 0;
    const UInt64 left = left_rows.value_or(0);
    const UInt64 right = right_rows.value_or(0);
    double probe_fraction = 1.0;
    if (query_graph.runtime_filter_prunes_probe)
        probe_fraction = left < right ? probeReadFraction(right_mask, left_mask) : probeReadFraction(left_mask, right_mask);
    return computeHashJoinWork(left, right, probe_fraction);
}

double DPSubJoinOrderOptimizer::probeReadFraction(UInt32 probe_mask, UInt32 build_mask) const
{
    if (std::popcount(probe_mask) != 1)
        return 1.0;
    const auto & prefix = dpsub_data.rel_prefix_classes[std::countr_zero(probe_mask)];
    if (prefix.empty() || !(dpsub_data.class_relations[prefix.front()] & build_mask))
        return 1.0;
    const Int32 order_class = prefix.front();

    const size_t num_classes = dpsub_data.class_members.size();
    const auto & probe = dpsub_data.key_ranges[std::countr_zero(probe_mask) * num_classes + order_class];
    if (!probe.known || probe.max <= probe.min)
        return 1.0;

    /// The keys of the build side lie in the range of each of its columns of the class.
    double min = -std::numeric_limits<double>::infinity();
    double max = std::numeric_limits<double>::infinity();
    bool known = false;
    for (UInt32 remaining = build_mask; remaining; remaining &= remaining - 1)
    {
        const auto & range = dpsub_data.key_ranges[std::countr_zero(remaining) * num_classes + order_class];
        if (!range.known)
            continue;
        min = std::max(min, range.min);
        max = std::min(max, range.max);
        known = true;
    }
    if (!known)
        return 1.0;
    const double overlap = std::max(0.0, std::min(max, probe.max) - std::max(min, probe.min));
    return std::clamp(overlap / (probe.max - probe.min), 0.0, 1.0);
}

bool DPSubJoinOrderOptimizer::isInterestingOrder(Int32 order_class, UInt32 relations) const
{
    return query_graph.reuse_join_output_order && (dpsub_data.class_relations[order_class] & ~relations);
}

template <typename DPTable, std::unsigned_integral TUInt>
const JoinActionRef & DPSubJoinOrderOptimizer::orderColumn(const DPTable & dptable, TUInt S, Int32 plan_index, Int32 order_class) const
{
    const auto & table_entry = dptable[S];
    const auto & plan = plan_index < 0 ? table_entry : table_entry.ordered.at(plan_index);
    if (plan.method == JoinMethod::Merge && plan.merge_class == order_class)
        return orderColumn(dptable, plan.left, plan.left_plan, order_class);

    for (const auto & column : dpsub_data.class_members[order_class])
        if ((static_cast<UInt32>(1) << *column.getSourceRelations().getSingleBit()) & S)
            return column;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Equivalence class {} has no column in relations {}", order_class, toBinaryString(S));
}

template <typename DPTable, std::unsigned_integral TUInt>
UInt32 DPSubJoinOrderOptimizer::routedRelations(const DPTable & dptable, TUInt S, Int32 plan_index, UInt32 key_class, Int32 type) const
{
    const auto & table_entry = dptable[S];
    const auto & plan = plan_index < 0 ? table_entry : table_entry.ordered.at(plan_index);
    /// Only a result in the state of the class and type is hashed so: a later shuffle by another key hashes it again.
    if (!plan.left || !plan.right || plan.method != JoinMethod::Hash || plan.partition_state < 0)
        return 0;
    const auto & state = dpsub_data.partition_states[plan.partition_state];
    if (state.key_class != key_class || state.type != type)
        return 0;
    if (plan.shuffle_class == static_cast<Int8>(key_class))
    {
        const auto [left_relation, right_relation] = shuffleKeyRelations(dptable, S, plan_index);
        return (static_cast<UInt32>(1) << left_relation) | (static_cast<UInt32>(1) << right_relation);
    }
    /// A broadcast join keeps the rows of its partitioned side where they are. When that side is in another state, the
    /// plan shuffles it into this one before the join (see `prunePartitionedPlans`), by its columns of the class.
    const TUInt side = plan.partitioned_side == 0 ? plan.left : plan.right;
    const Int32 side_plan = plan.partitioned_side == 0 ? plan.left_plan : plan.right_plan;
    if (const UInt32 routed = routedRelations(dptable, side, side_plan, key_class, type))
        return routed;
    return dpsub_data.class_type_relations[key_class][type] & static_cast<UInt32>(side);
}

template <typename DPTable, std::unsigned_integral TUInt>
std::pair<UInt32, UInt32> DPSubJoinOrderOptimizer::shuffleKeyRelations(const DPTable & dptable, TUInt S, Int32 plan_index) const
{
    const auto & table_entry = dptable[S];
    const auto & plan = plan_index < 0 ? table_entry : table_entry.ordered.at(plan_index);
    const auto key_class = static_cast<UInt32>(plan.shuffle_class);
    const UInt32 class_relations = dpsub_data.class_relations[key_class];
    auto key_relation = [&](TUInt side, Int8 side_plan)
    {
        UInt32 columns = class_relations & static_cast<UInt32>(side);
        if (plan.shuffle_type >= 0)
        {
            columns &= dpsub_data.class_type_relations[key_class][plan.shuffle_type];
            if (const UInt32 routed = routedRelations(dptable, side, side_plan, key_class, plan.shuffle_type) & columns)
                columns = routed;
        }
        return static_cast<UInt32>(std::countr_zero(columns));
    };
    return {key_relation(plan.left, plan.left_plan), key_relation(plan.right, plan.right_plan)};
}

std::optional<std::pair<JoinKind, JoinStrictness>>
DPSubJoinOrderOptimizer::resolveJoinMask(UInt32 left_mask, UInt32 right_mask) const
{
    if (useConflictDetector())
        return isValidJoinOrderMaskConflict(left_mask, right_mask);

    if (auto kind = isValidJoinOrderMask(left_mask, right_mask))
        return std::make_pair(*kind, JoinStrictness::All);
    return std::nullopt;
}

const VectorWithMemoryTracking<JoinActionRef *> & DPSubJoinOrderOptimizer::collectJoinEdgesMask(UInt32 left_mask, UInt32 right_mask)
{
    auto & out = dpsub_data.applicable_scratch;
    out.clear();

    const UInt32 joined = left_mask | right_mask;
    const bool two_relations = std::popcount(joined) == 2;
    const bool by_operator = useConflictDetector();
    auto & edges = query_graph.edges;

    for (size_t i = 0; i < edges.size(); ++i)
    {
        auto & edge = edges[i];
        if (!edge)
            continue;

        /// With a conflict detector, the ON condition of an outer, semi or anti join is placed exactly
        /// where that join is applied, the join whose kind it fixes (see `isValidJoinOrderMaskConflict`),
        /// and so it stays in that join's ON clause, whatever the search does around it.
        if (by_operator && dpsub_data.edge_pinned[i])
        {
            if (dpsub_data.conflict_operators[dpsub_data.edge_operator[i]].isAppliedAt(left_mask, right_mask))
                out.push_back(&edge);
            continue;
        }

        const UInt32 sources = dpsub_data.edge_source_mask[i];
        if (sources & ~joined) /// edge sources not proper subset of joined relations
            continue;
        if (dpsub_data.edge_pinned[i] && (dpsub_data.edge_pin_mask[i] & ~joined))
            continue;

        /// For an outer join the `pin` mask encodes relations that must be present for the predicate
        /// to be applicable; for an inner join it is just the predicate's source relations (pin empty).
        /// For a single-table conjunct of an outer join's ON
        /// clause (e.g. `t2.value = 'x'` in `... LEFT JOIN t3 ON t2.id = t3.id AND t2.value = 'x'`),
        /// `sources` is only `{t2}` but the pin is `{t3}`: the predicate belongs to the ON condition of
        /// the join that brings in `t3`, not to `t2` as a base-table filter. Placing it by `sources`
        /// alone would push it below (or drop it from) that join, silently changing outer-join
        /// semantics, since `t2` may already have been NULL-extended (or default-filled) by an
        /// earlier join. Without a conflict detector, a null-supplying side is a single relation, which
        /// makes this placement exact. With one, the pin is unused, but an inner join's conjunct with
        /// outstanding relations is placed the same way (see `initDPsubScratch`).
        const UInt32 pin = dpsub_data.edge_pinned[i] ? dpsub_data.edge_pin_mask[i] : dpsub_data.edge_outstanding_mask[i];
        if (pin & ~joined)
            continue;
        const UInt32 applicable = sources | pin;

        if (std::popcount(applicable) <= 1)
        {
            /// Base-relation filter or constant predicate (the edge references at most one relation).
            const bool relation_introduced = applicable != 0 && (left_mask == applicable || right_mask == applicable);
            const bool constant_at_earliest_join = applicable == 0 && two_relations;
            if (relation_introduced || constant_at_earliest_join)
                out.push_back(&edge);
        }
        else if ((applicable & ~left_mask) && (applicable & ~right_mask))
        {
            /// The predicate spans the split (a connecting equi-predicate, or a single-table ON-clause
            /// conjunct pinned to the opposite side): neither side alone contains all the relations it
            /// needs. This join is the lowest one that makes it applicable, so attach it here — into the
            /// correct join's ON condition.
            out.push_back(&edge);
        }
    }
    return out;
}

SelectivityEstimate DPSubJoinOrderOptimizer::computeSelectivityMask(
    const VectorWithMemoryTracking<JoinActionRef *> & edges, UInt32 left_mask, UInt32 right_mask)
{
    auto estimate = DB::computeSelectivity(query_graph, dp_table, expression_selectivity, edges);

    /// Account for transitively-equivalent columns spanning both sides, visiting only the classes
    /// incident to the left relations. A generation stamp deduplicates classes without allocating
    const UInt64 generation = ++dpsub_data.equiv_generation;

    for (UInt32 remaining = left_mask; remaining;)
    {
        const UInt32 rel = std::countr_zero(remaining);
        remaining &= remaining - 1;

        for (UInt32 class_idx : dpsub_data.rel_to_classes[rel])
        {
            if (dpsub_data.class_visited[class_idx] == generation)
                continue;
            dpsub_data.class_visited[class_idx] = generation;

            UInt64 max_ndv = 0;
            UInt64 max_ndv_upper_bound = 0;
            bool has_left = false;
            bool has_right = false;
            for (const auto & equiv_member : dpsub_data.equiv_classes[class_idx])
            {
                if (left_mask & equiv_member.relation_bit)
                    has_left = true;
                else if (right_mask & equiv_member.relation_bit)
                    has_right = true;
                else
                    continue;
                max_ndv = std::max(max_ndv, equiv_member.ndv.value_or(0));
                max_ndv_upper_bound = std::max(max_ndv_upper_bound, equiv_member.ndv_upper_bound);
            }
            if (has_left && has_right)
                applyEquiKeyDistinctValues(estimate, max_ndv, max_ndv_upper_bound);
        }
    }

    return estimate;
}

template <typename DPTable, std::unsigned_integral TUInt>
std::shared_ptr<DPJoinEntry> DPSubJoinOrderOptimizer::buildPhysicalPlan(const DPTable & dptable, const TUInt & S, Int32 plan_index) const
{
    const auto & table_entry = dptable[S];
    const auto & entry = plan_index < 0 ? table_entry : table_entry.ordered.at(plan_index);
    if (!entry.left && !entry.right)
        return std::make_shared<DPJoinEntry>(std::countr_zero(S), table_entry.estimated_rows, table_entry.column_stats);

    /// `entry.strictness` is All for every DP entry except semi/anti joins admitted by the
    /// conflict detector (CD-A/CD-C), which must keep their strictness in the reordered tree.
    JoinOperator join_operator(entry.kind, entry.strictness, JoinLocality::Unspecified);
    /// A filter predicate applied at an outer join step must not go to the ON clause, where it
    /// would affect matching instead of filtering and let non-matching rows of the preserved side
    /// survive NULL-extended. Apply it after the join instead (see `solveGreedy`).
    /// Semi/anti joins keep their ON predicates in the ON clause (they are in
    /// `outer_join_conditions`), so this only diverts genuine post-join filters.
    bool is_inner_step = isInner(entry.kind) || isCrossOrComma(entry.kind);

    /// A shuffle key that the search chose goes first, and `cleanupJoinPredicates` drops the other conditions of its class.
    if (entry.method == JoinMethod::Hash && entry.shuffle_class >= 0)
    {
        const auto key_class = static_cast<UInt32>(entry.shuffle_class);
        const auto [left_relation, right_relation] = shuffleKeyRelations(dptable, S, plan_index);
        join_operator.expression.push_back(JoinActionRef::transform(
            {classColumn(key_class, left_relation), classColumn(key_class, right_relation)},
            JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    }

    /// The merge key goes first, on the columns the inputs are sorted by; `cleanupJoinPredicates` drops the conditions it makes redundant.
    if (entry.method == JoinMethod::Merge)
    {
        auto add_key = [&](Int32 key_class)
        {
            const auto & left_column = orderColumn(dptable, entry.left, entry.left_plan, key_class);
            const auto & right_column = orderColumn(dptable, entry.right, entry.right_plan, key_class);
            join_operator.expression.push_back(
                JoinActionRef::transform({left_column, right_column}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
        };
        add_key(entry.merge_class);

        /// On several keys, the others follow in the order an input is sorted by, as `mergeJoinCost` assumed.
        const UInt64 key_classes = mergeKeysClasses(entry.left, entry.right);
        if (std::popcount(key_classes) > 1)
        {
            auto key_order = keyOrder(entry.left, key_classes, entry.merge_class);
            if (key_order.empty())
                key_order = keyOrder(entry.right, key_classes, entry.merge_class);
            for (size_t i = 1; i < key_order.size(); ++i)
                add_key(key_order[i]);
        }
    }

    for (const auto * e : entry.edges)
    {
        if (is_inner_step || query_graph.outer_join_conditions.contains(*e))
            join_operator.expression.push_back(*e);
        else
            join_operator.residual_filter.push_back(*e);
    }

    auto left = buildPhysicalPlan(dptable, entry.left, entry.left_plan);
    auto right = buildPhysicalPlan(dptable, entry.right, entry.right_plan);

    /// A semi/anti join leaves the search in whichever orientation the enumeration reached first:
    /// mirroring it changes neither its cost nor its result. Emit it the way the query wrote it, so the
    /// join algorithms that execute the query as written also execute the reordered plan (partial merge
    /// join runs `LEFT SEMI` but not `RIGHT SEMI`). Which reorderings are valid is decided by the conflict
    /// detector alone; the orientation is a physical detail that must not make a query fail.
    if (entry.strictness == JoinStrictness::Semi || entry.strictness == JoinStrictness::Anti)
    {
        if (auto written_kind = writtenSemiAntiKind(entry.left, entry.right, entry.strictness);
            written_kind && *written_kind != join_operator.kind)
        {
            left.swap(right);
            join_operator.kind = reverseJoinKind(join_operator.kind);
        }
    }

    /// The search costs a join the same whichever way round its inputs go, so `entry.right` is just
    /// the side the enumeration reached first. The right side builds the hash table, so give that
    /// role to the smaller estimate.
    ///
    /// Leave the sides alone when the query pins `query_plan_join_swap_table`, and in a graph whose
    /// entries do not carry the real strictness: the swap in `optimizeJoin` decides those later.
    /// A merge join keeps its sides: its result is sorted by the key of the left input, which `orderColumn` relies on.
    const bool can_swap = entriesCarryStrictness(query_graph) && canSwap(entry.kind, entry.strictness) && entry.method != JoinMethod::Merge;
    if (!query_graph.join_swap_table && can_swap)
    {
        const auto & left_rows = left->estimated_rows;
        const auto & right_rows = right->estimated_rows;
        /// On equal estimates, or when an input has none (e.g. a `UNION ALL` subquery), put the input
        /// with more relations on the left: the enumeration reaches a lone relation against the rest
        /// first, and would otherwise build the hash table from a whole subtree. Two single
        /// relations without an estimate keep their sides.
        const bool fewer_relations_left = std::popcount(entry.left) < std::popcount(entry.right);
        const bool swap_sides = (left_rows && right_rows)
            ? (*left_rows < *right_rows || (*left_rows == *right_rows && fewer_relations_left))
            : fewer_relations_left;

        if (swap_sides)
        {
            left.swap(right);
            join_operator.kind = reverseJoinKind(join_operator.kind);
        }
    }

    auto result = std::make_shared<DPJoinEntry>(
        left, right, entry.rows_cost, entry.sel, entry.estimated_rows, std::move(join_operator), entry.method);
    result->distributed_costs = useDistribution();
    result->costing_rows = entry.search_rows;
    return result;
}

std::shared_ptr<DPJoinEntry> DPSubJoinOrderOptimizer::solve()
{
    /// DPsub uses the generic memo only for composite-expression statistics. Clear any
    /// partial state left by an earlier algorithm in the fallback chain so this attempt
    /// has the same cost model as a standalone DPsub run.
    dp_table.clear();
    expression_selectivity.clear();

    const size_t n = query_graph.relation_stats.size();
    using Bitvector = UInt32; // choose UInt64 or even UInt128 for larger sets

    /// Check the table count up front rather than stopping midway through: a half-finished search
    /// throws away the time it already spent and still ends up handing the query to greedy.
    static_assert(DPSUB_MAX_RELATIONS < std::numeric_limits<Bitvector>::digits);

    if (n > DPSUB_MAX_RELATIONS)
    {
        LOG_TRACE(log,
            "Number of relations {} exceeds the DPsub threshold {}, skipping DPsub optimization and falling back to the next algorithm",
            n, DPSUB_MAX_RELATIONS);
        return nullptr;
    }

    struct DPPlan
    {
        Bitvector left{0};
        Bitvector right{0};
        std::optional<UInt64> estimated_rows = {};
        double cost{.0};
        /// What `EXPLAIN` reports: the output rows of its joins, as `EXPLAIN ANALYZE` measures them. `cost` adds the work
        /// of the join methods when merge joins compete.
        double rows_cost{.0};
        double sel{.0};
        JoinKind kind{JoinKind::Inner};
        JoinStrictness strictness{JoinStrictness::All};
        JoinMethod method{JoinMethod::Hash};
        bool sharded{false};   /// a merge join sharded by primary key ranges
        Int8 merge_class{-1};  /// equivalence class of the merge key
        Int8 order_class{-1};  /// equivalence class the result is sorted by, if a later join may use it
        Int8 left_plan{-1};    /// plan of `left` in its `ordered`, -1 for its cheapest plan
        Int8 right_plan{-1};
        Int8 partition_state{-1}; /// how the result is spread over the nodes, see `DPSubJoinOrderOptimizer::replicated_state`
        /// A shuffle join by a key of a class that the search chose, see `shuffleKeyTypes`: the class, and the exact type
        /// of the two key columns (an index in `DPSubData::class_type_relations`, -1 when they have no common one). -1
        /// for other joins.
        Int8 shuffle_class{-1};
        Int8 shuffle_type{-1};
        /// A broadcast join: the side whose rows stay where they are, 0 for the left one, 1 for the right one.
        UInt8 partitioned_side{0};
        /// With `useDistribution`: the rows the costs take, `estimated_rows` when known, else the rows before the filters of
        /// the inputs, joined as `estimateJoinedRows` joins them.
        double search_rows{1.0};
        VectorWithMemoryTracking<JoinActionRef*> edges; // needed for physical plan generation
    };
    struct DPEntry : DPPlan
    {
        using Plan = DPPlan;
        Bitvector neighbor{0};
        UnorderedMapWithMemoryTracking<String, ColumnStats> column_stats = {};
        /// The cheapest plan per interesting sort order (or partitioning, see `useDistribution`), kept even when the
        /// cheapest plan overall is another one.
        VectorWithMemoryTracking<DPPlan> ordered;
        /// With `useDistribution`: `ordered` keeps only the plans that a later join may prefer, see `prunePartitionedPlans`.
        bool pruned{false};
        /// The states of the plans that `prunePartitionedPlans` dropped.
        VectorWithMemoryTracking<Int8> pruned_states;
    };
    using DPTable = DPTable<DPEntry, Bitvector>;
    using Checker = EnumeratorCheckerWithCosts<DPTable, DPSubJoinOrderOptimizer>;
    using Enumerator = EnumCcpSub<Checker, DPTable, QueryGraph>;

    /// Precompute the native-mask view of the query graph so the acceptor's per-CCP work
    /// (validity, edge collection, selectivity) runs without allocating a `BitSet` each time
    /// That is, we don't have to convert Bitvector -> BitSet -> Bitvector for every subset S
    /// and its subcomponents S1, S2
    initDPsubScratch();

    /// Turn down a disconnected graph before enumerating anything: DPsub cannot stitch its
    /// components, so the next algorithm in the chain plans the query instead. Cross products do not
    /// disconnect a graph: each one links its two sides (see `QueryGraph::cross_product_links`),
    /// whatever the conflict detector, and the acceptor reports such a join as `Cross`. What is left
    /// is a join whose condition does not span its sides without a conflict detector to link it.
    /// A graph only DPsub may plan is never turned down.
    if (dpsub_data.disconnected_graph && !query_graph.requires_conflict_detector)
    {
        LOG_TRACE(log, "Join graph is disconnected apart from cross products, leaving it to the next algorithm");
        return nullptr;
    }

    /// Returns nothing when the distributed search went over its budget.
    auto search = [&](Checker & checker) -> std::optional<DPJoinEntryPtr>
    {
        if (useDistribution())
        {
            for (size_t i = 0; i < n; ++i)
            {
                auto & leaf = checker.getDPTable()[static_cast<Bitvector>(1) << i];
                leaf.estimated_rows = query_graph.relation_stats[i].estimated_rows;
                addLeafStates(static_cast<UInt32>(1) << i, leaf);
            }
        }
        Enumerator enumerator(n, log);
        enumerator.enumerate(checker, query_graph);
        if (distribution_stopped)
            return std::nullopt;

        const Bitvector full_set = (static_cast<Bitvector>(1) << n) - 1;
        const auto & dptable = checker.getDPTable();

        /// The full set is assembled only if the join graph is connected. When it is not e.g.
        /// cross products, or predicates that reference a single relation or a constant and thus
        /// create no binary edge (`... LEFT JOIN t ON t.x = 5`): DPsub cannot stitch the
        /// disconnected components, so the full-set entry is missing (or was never given a join)
        const bool full_built = dptable.isConnected(full_set)
                                && (dptable[full_set].left != 0 || dptable[full_set].right != 0);
        if (!full_built)
        {
            LOG_TRACE(log, "DPsub: join graph is disconnected");
            return nullptr;
        }

        if (!useDistribution())
            return buildPhysicalPlan(dptable, full_set);

        /// The result of the joins goes to one node in the end, so a plan pays the gather of its rows too. This is the cost
        /// of the plan as the root of the query, not under an aggregation, which needs less.
        const auto & root = dptable[full_set];
        const auto join_cost = distributedJoinCost();
        const double root_bytes = rowBytes(full_set);
        auto root_cost = [&](const DPPlan & plan)
        {
            return plan.cost + join_cost.toSingleNode(plan.partition_state, plan.search_rows, root_bytes);
        };
        Int32 root_index = -1;
        for (size_t i = 0; i < root.ordered.size(); ++i)
            if (root_cost(root.ordered[i]) < root_cost(root_index < 0 ? root : root.ordered[root_index]))
                root_index = static_cast<Int32>(i);
        return buildPhysicalPlan(dptable, full_set, root_index);
    };

    {
        Checker checker(n, *this);
        auto result = search(checker);
        if (useDistribution())
            LOG_TRACE(log, "DPsub: the distribution-aware search visited {} pairs of plans", distributed_plan_pairs);
        if (result)
            return *result;
    }
    LOG_TRACE(log, "DPsub: the distribution-aware search visited more than {} pairs of plans, planning the graph without distribution",
        plan_pair_limit);
    dp_table.clear();
    expression_selectivity.clear();
    distribution_disabled = true;
    distribution_stopped = false;
    initDPsubScratch();
    Checker checker(n, *this);
    return search(checker).value();
}

}

DPJoinEntryPtr solveDPSubJoinOrder(QueryGraph & query_graph)
{
    return DPSubJoinOrderOptimizer(query_graph).solve();
}

}
