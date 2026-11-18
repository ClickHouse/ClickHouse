#pragma once

#include <Interpreters/JoinExpressionActions.h>
#include <Processors/QueryPlan/Optimizations/joinOrderCommon.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <limits>
#include <vector>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

/** This a helper class that is used to check the plans before storing them
* into DP dp_table. No plan costing here.
*/
template <class TDPTable>
class EnumeratorChecker
{
    using DPTable = TDPTable;
    using UInt = typename DPTable::Key;
    using Consumer = EnumeratorChecker;
public:
    explicit EnumeratorChecker(const size_t nr_relations_) : nr_relations(nr_relations_), dp_table(nr_relations_) {}

    void accept(UInt result_subset_, UInt lhs_subset_, UInt rhs_subset_) { dp_table.insert(result_subset_, lhs_subset_, rhs_subset_); }

    DPTable & getDPTable() { return dp_table; }
    const DPTable & getDPTable() const { return dp_table; }

    [[nodiscard]] size_t numRelations() const { return nr_relations; }
private:
    DPTable dp_table;
    const size_t nr_relations;
};

/** This a helper class that is used to check the plans before storing them
* into DP dp_table. Plans are costed here as well.
*/
template <class TDPTable, class TOptimizer>
class EnumeratorCheckerWithCosts
{
    using DPTable = TDPTable;
    using UInt = typename DPTable::Key;
    using Consumer = EnumeratorCheckerWithCosts;
    using Optimizer = TOptimizer;
public:
    EnumeratorCheckerWithCosts(const size_t num_relations_, Optimizer & optimizer_)
        : dp_table(num_relations_), optimizer(optimizer_), num_relations(num_relations_) {}

    void accept(UInt result_subset_, UInt lhs_subset_, UInt rhs_subset_);

    DPTable & getDPTable() { return dp_table; }
    [[nodiscard]] const DPTable & getDPTable() const { return dp_table; }

    [[nodiscard]] size_t numRelations() const { return num_relations; }
private:
    using Plan = typename DPTable::DPEntry::Plan;

    /// Offers the plans of a distributed hash join of the two sides, for every pair of their plans and every way to
    /// distribute it, see `DPSubJoinOrderOptimizer::useDistribution`.
    void acceptDistributedJoins(UInt result_subset, UInt lhs_subset, UInt rhs_subset, JoinKind kind, JoinStrictness strictness,
        const SelectivityEstimate & selectivity, const VectorWithMemoryTracking<JoinActionRef *> & edges);
    /// Keeps the plan as the cheapest plan of the subset and as its cheapest plan in its state, where that state is
    /// interesting.
    void offerPlan(UInt subset, const Plan & plan);
    /// Drops the plans of a set in a partitioning state that cost at least as much as its cheapest plan shuffled into
    /// that state, and have at least its rows: a shuffle join by the state's key moves the rows of the cheapest plan for
    /// less. A broadcast join keeps the state of its partitioned side, so it is also offered on the cheapest plan shuffled
    /// into a dropped state (`DPEntry::pruned_states`). Done once, when a larger set first uses the set, since DPsub
    /// plans all subsets of a set before the set itself.
    void prunePartitionedPlans(UInt subset);

    /// A shuffle of one split by an equality of a class: it depends on the split only, not on the plans of its sides.
    struct SplitShuffle
    {
        UInt32 key_class = 0;
        bool chosen = false;        /// the search chooses the key by the states of the sides, see `shuffleKeyTypes`
        bool raw = false;           /// the key hashes its values as they are; false for a cast key or an added one
        UInt32 left_relation = 0;   /// with `raw`: the relations of the key columns on the two sides
        UInt32 right_relation = 0;
        Int8 state = Optimizer::any_partitioning_state; /// the state of the result
    };
    VectorWithMemoryTracking<SplitShuffle> split_shuffles; /// scratch of `acceptDistributedJoins`

    /// Offers the sort-merge joins of the two sides, one per equivalence class they may be merged on.
    void acceptMergeJoins(UInt result_subset, UInt lhs_subset, UInt rhs_subset, JoinKind kind, JoinStrictness strictness,
        const SelectivityEstimate & selectivity, const VectorWithMemoryTracking<JoinActionRef *> & edges);

    struct MergeInputCandidate
    {
        Int8 plan = -1;
        MergeJoinInput input;
    };
    struct MergeInputCandidates
    {
        std::array<MergeInputCandidate, 2> items;
        size_t size = 0;
        [[nodiscard]] const MergeInputCandidate * begin() const { return items.data(); }
        [[nodiscard]] const MergeInputCandidate * end() const { return items.data() + size; }
    };
    /// The plans of the subset that may feed a merge join on the equivalence class: its cheapest plan, and its
    /// cheapest plan sorted by the class if it is another one. Plans without a row estimate are left out.
    MergeInputCandidates mergeInputCandidates(UInt subset, Int32 key) const;
    /// The cheapest plan of the subset for -1, else its plan in `ordered`.
    [[nodiscard]] const Plan & planAt(UInt subset, Int8 index) const { return index < 0 ? dp_table[subset] : dp_table[subset].ordered[index]; }

    DPTable dp_table;
    Optimizer & optimizer;
    const size_t num_relations;
};

template <class TDPTable, class TOptimizer>
void
EnumeratorCheckerWithCosts<TDPTable, TOptimizer>::accept(const UInt result_subset, const UInt lhs_subset, const UInt rhs_subset)
{
    auto logger = optimizer.log;

    /// A child is usable only if it is a base relation or a subset for which a valid join
    /// was already recorded. The enumerator's `setTableNeighbor` creates a DP entry for
    /// every connected subset to propagate neighbor info, but for non-Inner joins `accept`
    /// may have rejected the only ordering that builds that subset (e.g. {t2, t3} when t2's
    /// LEFT join requires t1). Such a polluted entry has no recorded join (left == right == 0)
    /// and must not be used as a building block, otherwise `buildPhysicalPlan` mistakes it for
    /// a leaf and emits an incomplete tree
    auto is_buildable = [&](UInt s)
    {
        return std::popcount(s) == 1 || (dp_table.isConnected(s) && (dp_table[s].left != 0 || dp_table[s].right != 0));
    };
    if (!is_buildable(lhs_subset) || !is_buildable(rhs_subset))
        return;

    const UInt32 left_mask = static_cast<UInt32>(lhs_subset);
    const UInt32 right_mask = static_cast<UInt32>(rhs_subset);

    /// Resolve validity + the resulting (kind, strictness). With a conflict detector (CD-A/CD-C)
    /// this uses the per-operator check (which can admit semi/anti joins), otherwise the
    /// per-relation outer-join check with strictness fixed to All.
    auto resolved = optimizer.resolveJoinMask(left_mask, right_mask);
    if (!resolved)
        return;

    auto kind = resolved->first;
    auto strictness = resolved->second;

    /// `edge` aliases an internal scratch buffer that the next `collectJoinEdgesMask` call overwrites
    /// it is only read below and copied into the DP entry, so the aliasing is safe.
    const auto & edge = optimizer.collectJoinEdgesMask(left_mask, right_mask);

    /// Decide whether this pair is a real join or a cross product, the way greedy does. `initDPTable`
    /// links the two sides of a join that has no two-table condition, so the acceptor is reached for
    /// pairs that nothing actually joins; those have to stay `Cross`, because that is what keeps a
    /// cross product local in `applyParallelReplicas`.
    const bool connected = !edge.empty()
        || optimizer.areTransitivelyConnectedMask(left_mask, right_mask);
    if (kind == JoinKind::Cross && connected)
        kind = JoinKind::Inner;
    else if (kind == JoinKind::Inner && !connected)
        kind = JoinKind::Cross;

    auto selectivity = optimizer.computeSelectivityMask(edge, left_mask, right_mask);
    const auto & left_entry = dp_table[lhs_subset];
    const auto & right_entry = dp_table[rhs_subset];
    const double joined_rows = estimateJoinedRows(selectivity, left_entry.estimated_rows, right_entry.estimated_rows);
    const double rows_cost = left_entry.rows_cost + right_entry.rows_cost + joined_rows;
    const double plan_cost = left_entry.cost + right_entry.cost + joined_rows
        + optimizer.hashJoinWork(left_mask, left_entry.estimated_rows, right_mask, right_entry.estimated_rows);

    LOG_TEST(logger, "selectivity: {} costs: {}, lhs est. rows: {}, rhs est. rows: {}",
             selectivity.value,
             plan_cost,
             dp_table[lhs_subset].estimated_rows.value_or(0),
             dp_table[rhs_subset].estimated_rows.value_or(0));

    if (optimizer.useDistribution())
    {
        acceptDistributedJoins(result_subset, lhs_subset, rhs_subset, kind, strictness, selectivity, edge);
        return;
    }

    if (!dp_table.isConnected(result_subset) || plan_cost < dp_table[result_subset].cost)
    {
        auto & entry = dp_table[result_subset];
        entry.left = lhs_subset;
        entry.right = rhs_subset;
        entry.cost = plan_cost;
        entry.rows_cost = rows_cost;
        entry.sel = selectivity.reported_value;
        entry.kind = kind;
        entry.strictness = strictness;
        entry.estimated_rows = optimizer.estimateCardinality(dp_table[lhs_subset].estimated_rows, dp_table[rhs_subset].estimated_rows, selectivity, kind, strictness);
        entry.edges.assign(edge.begin(), edge.end());
        entry.method = JoinMethod::Hash;
        entry.sharded = false;
        entry.merge_class = -1;
        entry.order_class = -1;
        entry.left_plan = -1;
        entry.right_plan = -1;
    }

    if (optimizer.useInterestingOrders())
        acceptMergeJoins(result_subset, lhs_subset, rhs_subset, kind, strictness, selectivity, edge);
}

template <class TDPTable, class TOptimizer>
void EnumeratorCheckerWithCosts<TDPTable, TOptimizer>::offerPlan(const UInt subset, const Plan & plan)
{
    auto & entry = dp_table[subset];
    if (optimizer.isInterestingState(plan.partition_state, static_cast<UInt32>(subset)))
    {
        auto it = std::ranges::find(entry.ordered, plan.partition_state, &Plan::partition_state);
        if (it == entry.ordered.end())
            entry.ordered.push_back(plan);
        else if (plan.cost < it->cost)
            *it = plan;
    }
    const bool has_plan = entry.left != 0 || entry.right != 0;
    if (!has_plan || plan.cost < entry.cost)
    {
        static_cast<Plan &>(entry) = plan;
        /// The partitioning of the cheapest plan matters only while a later join may use it.
        if (entry.partition_state >= 0 && !optimizer.isInterestingPartition(entry.partition_state, static_cast<UInt32>(subset)))
            entry.partition_state = Optimizer::any_partitioning_state;
    }
}

template <class TDPTable, class TOptimizer>
void EnumeratorCheckerWithCosts<TDPTable, TOptimizer>::prunePartitionedPlans(const UInt subset)
{
    auto & entry = dp_table[subset];
    if (entry.pruned)
        return;
    entry.pruned = true;
    const auto join_cost = optimizer.distributedJoinCost();
    const double cheapest_shuffled = entry.cost
        + join_cost.toNewPartitioning(entry.partition_state, entry.search_rows, optimizer.rowBytes(static_cast<UInt32>(subset)));
    std::erase_if(entry.ordered, [&](const Plan & plan)
    {
        if (plan.partition_state < 0 || plan.cost < cheapest_shuffled || plan.search_rows < entry.search_rows)
            return false;
        entry.pruned_states.push_back(plan.partition_state);
        return true;
    });
}

template <class TDPTable, class TOptimizer>
void EnumeratorCheckerWithCosts<TDPTable, TOptimizer>::acceptDistributedJoins(
    const UInt result_subset, const UInt lhs_subset, const UInt rhs_subset, const JoinKind kind, const JoinStrictness strictness,
    const SelectivityEstimate & selectivity, const VectorWithMemoryTracking<JoinActionRef *> & edges)
{
    /// The plans of a side: its cheapest plan (-1) and its plans in the other states.
    prunePartitionedPlans(lhs_subset);
    prunePartitionedPlans(rhs_subset);
    const auto left_plans = static_cast<Int8>(dp_table[lhs_subset].ordered.size());
    const auto right_plans = static_cast<Int8>(dp_table[rhs_subset].ordered.size());
    if (optimizer.distribution_stopped
        || !optimizer.spendDistributedPlanPairs(static_cast<size_t>(left_plans + 1) * static_cast<size_t>(right_plans + 1)))
        return;
    const auto left_mask = static_cast<UInt32>(lhs_subset);
    const auto right_mask = static_cast<UInt32>(rhs_subset);
    const UInt64 key_classes = optimizer.partitionKeyClasses(left_mask, right_mask);
    const auto join_cost = optimizer.distributedJoinCost();
    const double nodes = join_cost.nodes;
    const double left_bytes = optimizer.rowBytes(left_mask);
    const double right_bytes = optimizer.rowBytes(right_mask);
    constexpr Int8 any_partitioning = Optimizer::any_partitioning_state;
    constexpr Int8 replicated = Optimizer::replicated_state;
    constexpr Int8 single_node = Optimizer::single_node_state;

    /// Shuffle: both sides hashed by one equality of a class. After a shuffle by two columns of one type, Cascades knows
    /// the rows are hashed by these two columns, so a later shuffle by one of them moves no rows of that side. For a class
    /// of one type the search chooses the two columns, see `shuffleKeyTypes`. For another class, predicate cleanup keeps
    /// the first edge of the class, and when the join has no edge of the class, it adds none. An edge cast before hashing
    /// gives rows that no later join reuses.
    split_shuffles.clear();
    for (UInt64 classes = key_classes; classes; classes &= classes - 1)
    {
        const auto key_class = static_cast<UInt32>(std::countr_zero(classes));
        if (optimizer.choosesShuffleKey(key_class))
        {
            split_shuffles.push_back({.key_class = key_class, .chosen = true});
            continue;
        }
        const auto edge = std::ranges::find_if(edges, [&](const JoinActionRef * candidate)
        {
            return optimizer.shuffleKey(candidate).key_class == static_cast<Int32>(key_class);
        });
        if (edge == edges.end())
        {
            if (optimizer.hasSynthesizedShuffleKey(key_class))
                split_shuffles.push_back({.key_class = key_class, .state = any_partitioning});
            continue;
        }
        const auto & key = optimizer.shuffleKey(*edge);
        if (!key.hashable)
            continue;
        const bool lhs_on_left = (left_mask >> key.lhs_relation) & 1;
        split_shuffles.push_back({
            .key_class = key_class,
            .raw = key.raw,
            .left_relation = lhs_on_left ? key.lhs_relation : key.rhs_relation,
            .right_relation = lhs_on_left ? key.rhs_relation : key.lhs_relation,
            .state = key.raw
                ? optimizer.partitionState(
                    key_class, (static_cast<UInt32>(1) << key.lhs_relation) | (static_cast<UInt32>(1) << key.rhs_relation))
                : any_partitioning});
    }

    /// An equality of two columns of different types, or of expressions, is in no class: Cascades shuffles by it with the
    /// keys cast to a common type, and no later join reuses the result. The cost is the same for each such edge.
    const bool has_direct_shuffle_key = std::ranges::any_of(edges, [&](const JoinActionRef * edge)
    {
        const auto & key = optimizer.shuffleKey(edge);
        auto within = [](UInt32 relations, UInt32 mask) { return relations && (relations & ~mask) == 0; };
        return key.key_class < 0 && key.hashable
            && ((within(key.lhs_relations, left_mask) && within(key.rhs_relations, right_mask))
                || (within(key.lhs_relations, right_mask) && within(key.rhs_relations, left_mask)));
    });
    if (has_direct_shuffle_key)
        split_shuffles.push_back({.state = any_partitioning});

    /// The fields of the plans of the split that do not depend on the plans of its sides: the edges are copied once.
    Plan plan;
    plan.left = lhs_subset;
    plan.right = rhs_subset;
    plan.sel = selectivity.reported_value;
    plan.kind = kind;
    plan.strictness = strictness;
    plan.edges.assign(edges.begin(), edges.end());
    plan.method = JoinMethod::Hash;

    for (Int8 left_index = -1; left_index < left_plans; ++left_index)
    {
        const Plan & left = planAt(lhs_subset, left_index);
        const double left_rows = left.search_rows;
        const Int8 left_state = left.partition_state;
        for (Int8 right_index = -1; right_index < right_plans; ++right_index)
        {
            const Plan & right = planAt(rhs_subset, right_index);
            const double right_rows = right.search_rows;
            const Int8 right_state = right.partition_state;
            /// Rows far above any real count only rank plans: the cap keeps their conversion to `UInt64` in range.
            static constexpr double max_search_rows = 1e18;
            auto to_rows = [](double rows) { return static_cast<UInt64>(std::min(rows, max_search_rows)); };
            const double joined_rows = std::clamp(
                estimateJoinedRows(selectivity, to_rows(left_rows), to_rows(right_rows)), 1.0, max_search_rows);
            const double inputs_cost = left.cost + right.cost;

            plan.left_plan = left_index;
            plan.right_plan = right_index;
            plan.rows_cost = left.rows_cost + right.rows_cost + joined_rows;
            plan.estimated_rows = optimizer.estimateCardinality(left.estimated_rows, right.estimated_rows, selectivity, kind, strictness);
            plan.search_rows = plan.estimated_rows ? static_cast<double>(*plan.estimated_rows) : joined_rows;

            auto offer = [&](double cost, Int8 state)
            {
                if (!std::isfinite(cost))
                    return;
                plan.cost = inputs_cost + cost;
                plan.partition_state = state;
                offerPlan(result_subset, plan);
            };
            plan.shuffle_class = -1;

            /// Where both sides are split the same way, Cascades may swap the sides of an inner join: the cheaper side builds
            /// the hash table.
            auto join_work = [&](double parallelism)
            {
                return std::min(
                    join_cost.joinWork(left_rows, right_rows, right_bytes, joined_rows, parallelism, parallelism),
                    join_cost.joinWork(right_rows, left_rows, left_bytes, joined_rows, parallelism, parallelism));
            };

            /// Broadcast: one side copied to every node, the other one partitioned; its partitioning is kept, a side on
            /// one node is scattered first.
            plan.partitioned_side = 0;
            offer(join_cost.toPartitioned(left_state, left_rows, left_bytes) + join_cost.toReplicated(right_state, right_rows, right_bytes)
                    + join_cost.joinWork(left_rows, right_rows, right_bytes, joined_rows, nodes, 1),
                left_state == single_node ? any_partitioning : left_state);
            plan.partitioned_side = 1;
            offer(join_cost.toPartitioned(right_state, right_rows, right_bytes) + join_cost.toReplicated(left_state, left_rows, left_bytes)
                    + join_cost.joinWork(right_rows, left_rows, left_bytes, joined_rows, nodes, 1),
                right_state == single_node ? any_partitioning : right_state);
            /// The cheapest plan of a side shuffled into a state whose own plan was dropped, then broadcast to.
            if (right_index == -1)
                for (const Int8 state : dp_table[rhs_subset].pruned_states)
                    offer(join_cost.toNewPartitioning(right_state, right_rows, right_bytes)
                            + join_cost.toReplicated(left_state, left_rows, left_bytes)
                            + join_cost.joinWork(right_rows, left_rows, left_bytes, joined_rows, nodes, 1),
                        state);
            plan.partitioned_side = 0;
            if (left_index == -1)
                for (const Int8 state : dp_table[lhs_subset].pruned_states)
                    offer(join_cost.toNewPartitioning(left_state, left_rows, left_bytes)
                            + join_cost.toReplicated(right_state, right_rows, right_bytes)
                            + join_cost.joinWork(left_rows, right_rows, right_bytes, joined_rows, nodes, 1),
                        state);

            for (const auto & shuffle : split_shuffles)
            {
                auto offer_shuffle = [&](bool left_routed, bool right_routed, Int8 state)
                {
                    offer((left_routed ? 0.0 : join_cost.toNewPartitioning(left_state, left_rows, left_bytes))
                            + (right_routed ? 0.0 : join_cost.toNewPartitioning(right_state, right_rows, right_bytes)) + join_work(nodes),
                        state);
                };
                if (!shuffle.chosen)
                {
                    offer_shuffle(
                        shuffle.raw && optimizer.isPartitionedBy(left_state, shuffle.key_class, shuffle.left_relation),
                        shuffle.raw && optimizer.isPartitionedBy(right_state, shuffle.key_class, shuffle.right_relation),
                        shuffle.state);
                    continue;
                }
                /// One shuffle per exact type that both sides have columns of: a side hashed by a column of that type
                /// moves no rows. Without such a type, the key is cast before hashing.
                plan.shuffle_class = static_cast<Int8>(shuffle.key_class);
                const Int32 left_type = optimizer.stateType(left_state, shuffle.key_class);
                const Int32 right_type = optimizer.stateType(right_state, shuffle.key_class);
                const UInt64 types = optimizer.shuffleKeyTypes(shuffle.key_class, left_mask, right_mask);
                for (UInt64 remaining = types; remaining; remaining &= remaining - 1)
                {
                    const auto type = static_cast<Int32>(std::countr_zero(remaining));
                    plan.shuffle_type = static_cast<Int8>(type);
                    offer_shuffle(left_type == type, right_type == type, optimizer.partitionState(shuffle.key_class, 0, type));
                }
                if (!types)
                {
                    plan.shuffle_type = -1;
                    offer_shuffle(false, false, any_partitioning);
                }
                plan.shuffle_class = -1;
            }

            /// Local: both sides on one node.
            offer(join_cost.toSingleNode(left_state, left_rows, left_bytes) + join_cost.toSingleNode(right_state, right_rows, right_bytes)
                    + join_work(1),
                single_node);

            /// Replicated: every node joins its copies of both sides, so its result is a copy too.
            offer(join_cost.toReplicated(left_state, left_rows, left_bytes) + join_cost.toReplicated(right_state, right_rows, right_bytes)
                    + join_work(1),
                replicated);
        }
    }
}

template <class TDPTable, class TOptimizer>
typename EnumeratorCheckerWithCosts<TDPTable, TOptimizer>::MergeInputCandidates
EnumeratorCheckerWithCosts<TDPTable, TOptimizer>::mergeInputCandidates(const UInt subset, const Int32 key) const
{
    const auto & entry = dp_table[subset];
    MergeInputCandidates candidates;
    auto add = [&](const Plan & plan, Int8 plan_index, bool sorted, bool shardable)
    {
        if (plan.estimated_rows)
            candidates.items[candidates.size++] = {plan_index, {plan.cost, *plan.estimated_rows, sorted, shardable}};
    };

    if (std::popcount(subset) == 1)
    {
        const auto relation = static_cast<UInt32>(subset);
        add(entry, -1, optimizer.isReadInOrder(relation, key), optimizer.isShardableLeaf(relation, key));
        return candidates;
    }
    add(entry, -1, entry.order_class == key, entry.order_class == key && entry.sharded);
    /// `ordered` has at most one plan per class.
    for (size_t i = 0; i < entry.ordered.size(); ++i)
    {
        if (entry.ordered[i].order_class == key)
        {
            add(entry.ordered[i], static_cast<Int8>(i), true, entry.ordered[i].sharded);
            break;
        }
    }
    return candidates;
}

template <class TDPTable, class TOptimizer>
void EnumeratorCheckerWithCosts<TDPTable, TOptimizer>::acceptMergeJoins(
    const UInt result_subset, const UInt lhs_subset, const UInt rhs_subset, const JoinKind kind, const JoinStrictness strictness,
    const SelectivityEstimate & selectivity, const VectorWithMemoryTracking<JoinActionRef *> & edges)
{
    const auto left_mask = static_cast<UInt32>(lhs_subset);
    const auto right_mask = static_cast<UInt32>(rhs_subset);
    const UInt64 merge_keys = optimizer.mergeKeyClasses(edges, left_mask, right_mask, kind, strictness);
    if (!merge_keys)
        return;
    const UInt64 key_classes = optimizer.joinKeyClasses(edges, left_mask, right_mask);
    for (UInt64 keys = merge_keys; keys; keys &= keys - 1)
    {
        const auto key = static_cast<Int32>(std::countr_zero(keys));
        const MergeInputCandidate * left = nullptr;
        const MergeInputCandidate * right = nullptr;
        double cost = std::numeric_limits<double>::infinity();
        const auto left_candidates = mergeInputCandidates(lhs_subset, key);
        const auto right_candidates = mergeInputCandidates(rhs_subset, key);
        for (const auto & left_candidate : left_candidates)
        {
            for (const auto & right_candidate : right_candidates)
            {
                const double candidate_cost = optimizer.mergeJoinCost(
                    left_candidate.input, left_mask, right_candidate.input, right_mask, key, key_classes, selectivity);
                if (candidate_cost < cost)
                {
                    cost = candidate_cost;
                    left = &left_candidate;
                    right = &right_candidate;
                }
            }
        }
        if (!left)
            continue;

        /// Decide where the plan goes before building it, a plan that is not kept costs no allocation.
        auto & entry = dp_table[result_subset];
        const bool keeps_order = optimizer.isInterestingOrder(key, static_cast<UInt32>(result_subset));
        auto ordered_it = keeps_order ? std::ranges::find(entry.ordered, key, &Plan::order_class) : entry.ordered.end();
        const bool new_ordered = keeps_order && ordered_it == entry.ordered.end();
        const bool better_ordered = keeps_order && !new_ordered && cost < ordered_it->cost;
        const bool better_cheapest = cost < entry.cost;
        if (!new_ordered && !better_ordered && !better_cheapest)
            continue;

        Plan plan;
        plan.left = lhs_subset;
        plan.right = rhs_subset;
        plan.left_plan = left->plan;
        plan.right_plan = right->plan;
        plan.cost = cost;
        plan.rows_cost = planAt(lhs_subset, left->plan).rows_cost + planAt(rhs_subset, right->plan).rows_cost
            + estimateJoinedRows(selectivity, left->input.rows, right->input.rows);
        plan.sel = selectivity.reported_value;
        plan.kind = kind;
        plan.strictness = strictness;
        plan.estimated_rows = optimizer.estimateCardinality(left->input.rows, right->input.rows, selectivity, kind, strictness);
        plan.edges.assign(edges.begin(), edges.end());
        plan.method = JoinMethod::Merge;
        plan.sharded = optimizer.isShardedMerge(left->input, left_mask, right->input, right_mask, key_classes);
        plan.merge_class = static_cast<Int8>(key);
        plan.order_class = keeps_order ? static_cast<Int8>(key) : -1;

        if (new_ordered)
            entry.ordered.push_back(plan);
        else if (better_ordered)
            *ordered_it = plan;
        if (better_cheapest)
            static_cast<Plan &>(entry) = std::move(plan);
    }
}
}
