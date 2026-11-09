#pragma once

#include <Interpreters/JoinExpressionActions.h>
#include <Processors/QueryPlan/Optimizations/joinOrderCommon.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <array>
#include <bit>
#include <functional>
#include <limits>
#include <vector>

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

    size_t numRelations() const { return nr_relations; }
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
    const DPTable & getDPTable() const { return dp_table; }

    size_t numRelations() const { return num_relations; }
private:
    using Plan = typename DPTable::DPEntry::Plan;

    /// The search value of the rows of a subset, which stands in for an unknown estimate, as `searchRows` counts it.
    double searchRows(UInt relations) const;

    /// Offers the sort-merge joins of the two sides, one per equivalence class they may be merged on.
    void acceptMergeJoins(UInt result_subset, UInt lhs_subset, UInt rhs_subset, JoinKind kind, JoinStrictness strictness,
        const JoinKeyEstimate & keys, const std::vector<JoinActionRef *> & edges);

    struct MergeInputCandidate
    {
        Int8 plan = -1;
        MergeJoinInput input;
    };
    struct MergeInputCandidates
    {
        std::array<MergeInputCandidate, 2> items;
        size_t size = 0;
        const MergeInputCandidate * begin() const { return items.data(); }
        const MergeInputCandidate * end() const { return items.data() + size; }
    };
    /// The plans of the subset that may feed a merge join on the equivalence class: its cheapest plan, and its
    /// cheapest plan sorted by the class if it is another one. Plans without a row estimate are left out.
    MergeInputCandidates mergeInputCandidates(UInt subset, Int32 key) const;
    /// Offers the local, shuffle and broadcast hash joins of the plans of the two sides, see `DistributedJoinCost`. Returns
    /// the cost of the cheapest one.
    double acceptDistributedJoins(UInt result_subset, UInt lhs_subset, UInt rhs_subset, JoinKind kind, JoinStrictness strictness,
        const JoinKeyEstimate & keys, const std::vector<JoinActionRef *> & edges);
    /// Keeps the plan as the cheapest plan of the subset, and as its plan partitioned by the class, if it is cheaper.
    void offerPlan(UInt subset, const Plan & plan);

    /// The cheapest plan of the subset for -1, else its plan in `ordered`.
    const Plan & planAt(UInt subset, Int8 index) const { return index < 0 ? dp_table[subset] : dp_table[subset].ordered[index]; }

    DPTable dp_table;
    Optimizer & optimizer;
    const size_t num_relations;
};

template <class TDPTable, class TOptimizer>
double EnumeratorCheckerWithCosts<TDPTable, TOptimizer>::searchRows(const UInt relations) const
{
    const auto & entry = dp_table[relations];
    if (entry.estimated_rows)
        return static_cast<double>(*entry.estimated_rows);
    if (entry.left != 0 || entry.right != 0)
        return joinSearchRows(entry.sel, searchRows(entry.left), searchRows(entry.right), entry.kind, entry.strictness);
    if (entry.max_rows)
        return static_cast<double>(*entry.max_rows);
    return 1;
}

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

    auto & recorded_splits = optimizer.query_graph.recorded_splits;
    if (recorded_splits)
        ++recorded_splits->considered_splits;

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

    auto keys = optimizer.computeSelectivityMask(edge, left_mask, right_mask);
    const auto & left_entry = dp_table[lhs_subset];
    const auto & right_entry = dp_table[rhs_subset];
    const double left_search_rows = searchRows(lhs_subset);
    const double right_search_rows = searchRows(rhs_subset);
    const double joined_rows = joinedRowsSearchValue(
        keys, left_entry.estimated_rows, right_entry.estimated_rows, left_search_rows, right_search_rows, kind, strictness);
    const double rows_cost = left_entry.rows_cost + right_entry.rows_cost + joined_rows;
    /// The hash work counts an input without an estimate by its search value, as the rows do.
    double plan_cost = left_entry.cost + right_entry.cost + joined_rows
        + optimizer.hashJoinWork(left_mask, left_search_rows, right_mask, right_search_rows);

    if (recorded_splits)
    {
        RecordedJoinSplits::Split split{.left = lhs_subset, .right = rhs_subset, .edges = {}, .local_cost = joined_rows};
        for (const auto * recorded_edge : edge)
            split.edges.push_back(recorded_edge - optimizer.query_graph.edges.data());
        recorded_splits->splits.push_back(std::move(split));
    }

    LOG_TEST(logger, "selectivity: {} costs: {}, lhs est. rows: {}, rhs est. rows: {}",
             keys.selectivity,
             plan_cost,
             dp_table[lhs_subset].estimated_rows.value_or(0),
             dp_table[rhs_subset].estimated_rows.value_or(0));

    if (optimizer.useDistribution())
        plan_cost = acceptDistributedJoins(result_subset, lhs_subset, rhs_subset, kind, strictness, keys, edge);

    /// The full set is enumerated last, so the plans of its parts are final: every costed split of it can be kept as
    /// another join order.
    const UInt full_set = (static_cast<UInt>(1) << num_relations) - 1;
    if (result_subset == full_set && optimizer.query_graph.max_join_order_alternatives > 0)
    {
        optimizer.keepFullSetAlternative(left_mask, plan_cost, [&]
        {
            return typename Optimizer::FullSetAlternative{
                .left = left_mask,
                .right = right_mask,
                .cost = plan_cost,
                .rows_cost = rows_cost,
                .sel = keys.selectivity,
                .estimated_rows = optimizer.estimateCardinality(left_entry.estimated_rows, right_entry.estimated_rows, keys, kind, strictness),
                .rows_rest_on_assumption = joinRowsRestOnAssumption(keys, left_entry.estimated_rows, right_entry.estimated_rows),
                .kind = kind,
                .strictness = strictness,
                .edges = {edge.begin(), edge.end()}};
        });
    }

    if (optimizer.useDistribution())
        return;

    if (!dp_table.isConnected(result_subset) || plan_cost < dp_table[result_subset].cost)
    {
        auto & entry = dp_table[result_subset];
        entry.left = lhs_subset;
        entry.right = rhs_subset;
        entry.cost = plan_cost;
        entry.rows_cost = rows_cost;
        entry.sel = keys.selectivity;
        entry.kind = kind;
        entry.strictness = strictness;
        entry.estimated_rows = optimizer.estimateCardinality(dp_table[lhs_subset].estimated_rows, dp_table[rhs_subset].estimated_rows, keys, kind, strictness);
        entry.rows_rest_on_assumption = joinRowsRestOnAssumption(keys, dp_table[lhs_subset].estimated_rows, dp_table[rhs_subset].estimated_rows);
        entry.max_rows = estimateJoinRowsUpperBound(dp_table[lhs_subset].max_rows, dp_table[rhs_subset].max_rows, kind, strictness);
        entry.edges.assign(edge.begin(), edge.end());
        entry.method = JoinMethod::Hash;
        entry.sharded = false;
        entry.merge_class = -1;
        entry.order_class = -1;
        entry.left_plan = -1;
        entry.right_plan = -1;
        entry.partition_class = -1;
    }

    if (optimizer.useInterestingOrders())
        acceptMergeJoins(result_subset, lhs_subset, rhs_subset, kind, strictness, keys, edge);
}

template <class TDPTable, class TOptimizer>
void EnumeratorCheckerWithCosts<TDPTable, TOptimizer>::offerPlan(const UInt subset, const Plan & plan)
{
    auto & entry = dp_table[subset];
    if (plan.partition_class >= 0 && optimizer.isInterestingPartition(plan.partition_class, static_cast<UInt32>(subset)))
    {
        auto it = std::ranges::find(entry.ordered, plan.partition_class, &Plan::partition_class);
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
        if (entry.partition_class >= 0 && !optimizer.isInterestingPartition(entry.partition_class, static_cast<UInt32>(subset)))
            entry.partition_class = -1;
    }
}

template <class TDPTable, class TOptimizer>
double EnumeratorCheckerWithCosts<TDPTable, TOptimizer>::acceptDistributedJoins(
    const UInt result_subset, const UInt lhs_subset, const UInt rhs_subset, const JoinKind kind, const JoinStrictness strictness,
    const JoinKeyEstimate & keys, const std::vector<JoinActionRef *> & edges)
{
    const auto left_mask = static_cast<UInt32>(lhs_subset);
    const auto right_mask = static_cast<UInt32>(rhs_subset);
    const UInt64 key_classes = optimizer.partitionKeyClasses(left_mask, right_mask);
    const auto join_cost = optimizer.distributedJoinCost(left_mask, right_mask);

    /// The plans of a side: its cheapest plan and its plans partitioned by a class.
    auto side_plans = [&](UInt subset)
    {
        std::vector<Int8> plans{-1};
        for (size_t i = 0; i < dp_table[subset].ordered.size(); ++i)
            plans.push_back(static_cast<Int8>(i));
        return plans;
    };
    const auto left_plans = side_plans(lhs_subset);
    const auto right_plans = side_plans(rhs_subset);

    double best_cost = std::numeric_limits<double>::infinity();
    for (const Int8 left_index : left_plans)
    {
        const Plan & left = planAt(lhs_subset, left_index);
        const double left_rows = left.estimated_rows ? static_cast<double>(*left.estimated_rows) : searchRows(lhs_subset);
        for (const Int8 right_index : right_plans)
        {
            const Plan & right = planAt(rhs_subset, right_index);
            const double right_rows = right.estimated_rows ? static_cast<double>(*right.estimated_rows) : searchRows(rhs_subset);
            const double joined_rows = joinedRowsSearchValue(
                keys, left.estimated_rows, right.estimated_rows, left_rows, right_rows, kind, strictness);
            const double inputs_cost = left.cost + right.cost;

            Plan plan;
            plan.left = lhs_subset;
            plan.right = rhs_subset;
            plan.left_plan = left_index;
            plan.right_plan = right_index;
            plan.rows_cost = left.rows_cost + right.rows_cost + joined_rows;
            plan.sel = keys.selectivity;
            plan.kind = kind;
            plan.strictness = strictness;
            plan.estimated_rows = optimizer.estimateCardinality(left.estimated_rows, right.estimated_rows, keys, kind, strictness);
            plan.rows_rest_on_assumption = joinRowsRestOnAssumption(keys, left.estimated_rows, right.estimated_rows);
            plan.imprecise_rows = left.imprecise_rows || right.imprecise_rows || plan.rows_rest_on_assumption || !plan.estimated_rows;
            plan.max_rows = estimateJoinRowsUpperBound(left.max_rows, right.max_rows, kind, strictness);
            plan.edges.assign(edges.begin(), edges.end());
            plan.method = JoinMethod::Hash;

            auto offer = [&](double cost, Int8 partition_class)
            {
                plan.cost = inputs_cost + cost;
                plan.partition_class = partition_class;
                best_cost = std::min(best_cost, plan.cost);
                offerPlan(result_subset, plan);
            };

            /// A broadcast of one side keeps the partitioning of the other.
            /// A join without keys has no other way to run, so its right side is broadcast whatever its size.
            const bool broadcast_right = optimizer.canBroadcast(right_mask, right.imprecise_rows, right.max_rows, right_rows);
            const bool broadcast_left = optimizer.canBroadcast(left_mask, left.imprecise_rows, left.max_rows, left_rows);
            if (broadcast_right || (key_classes == 0 && !broadcast_left))
                offer(join_cost.broadcastRight(left_rows, right_rows, joined_rows), left.partition_class);
            if (broadcast_left)
                offer(join_cost.broadcastLeft(left_rows, right_rows, joined_rows), right.partition_class);

            /// Both sides partitioned by a key: the sides that are not partitioned by it yet are shuffled.
            for (UInt64 classes = key_classes; classes; classes &= classes - 1)
            {
                const auto key = static_cast<Int8>(std::countr_zero(classes));
                const bool shuffle_left = left.partition_class != key;
                const bool shuffle_right = right.partition_class != key;
                offer(join_cost.partitioned(left_rows, shuffle_left, right_rows, shuffle_right, joined_rows), key);
            }
        }
    }
    return best_cost;
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
    const JoinKeyEstimate & keys, const std::vector<JoinActionRef *> & edges)
{
    const auto left_mask = static_cast<UInt32>(lhs_subset);
    const auto right_mask = static_cast<UInt32>(rhs_subset);
    for (UInt64 key_classes = optimizer.mergeKeyClasses(edges, left_mask, right_mask, kind, strictness); key_classes; key_classes &= key_classes - 1)
    {
        const auto key = static_cast<Int32>(std::countr_zero(key_classes));
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
                    left_candidate.input, left_mask, right_candidate.input, right_mask, key, keys);
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
            + estimateJoinedRows(keys, static_cast<double>(left->input.rows), static_cast<double>(right->input.rows));
        plan.sel = keys.selectivity;
        plan.kind = kind;
        plan.strictness = strictness;
        plan.estimated_rows = optimizer.estimateCardinality(left->input.rows, right->input.rows, keys, kind, strictness);
        plan.rows_rest_on_assumption = joinRowsRestOnAssumption(keys, left->input.rows, right->input.rows);
        plan.max_rows = estimateJoinRowsUpperBound(
            planAt(lhs_subset, left->plan).max_rows, planAt(rhs_subset, right->plan).max_rows, kind, strictness);
        plan.edges.assign(edges.begin(), edges.end());
        plan.method = JoinMethod::Merge;
        plan.sharded = optimizer.isShardedMerge(left->input, left_mask, right->input, right_mask);
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
