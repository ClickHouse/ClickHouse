#pragma once

#include <Interpreters/JoinExpressionActions.h>
#include <Processors/QueryPlan/Optimizations/joinOrderCommon.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <array>
#include <bit>
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

    /// Offers the sort-merge joins of the two sides, one per equivalence class they may be merged on.
    void acceptMergeJoins(UInt result_subset, UInt lhs_subset, UInt rhs_subset, JoinKind kind, JoinStrictness strictness,
        const SelectivityEstimate & selectivity, const std::vector<JoinActionRef *> & edges);

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
    /// The cheapest plan of the subset for -1, else its plan in `ordered`.
    const Plan & planAt(UInt subset, Int8 index) const { return index < 0 ? dp_table[subset] : dp_table[subset].ordered[index]; }

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
        entry.merge_class = -1;
        entry.order_class = -1;
        entry.left_plan = -1;
        entry.right_plan = -1;
    }

    if (optimizer.useInterestingOrders())
        acceptMergeJoins(result_subset, lhs_subset, rhs_subset, kind, strictness, selectivity, edge);
}

template <class TDPTable, class TOptimizer>
typename EnumeratorCheckerWithCosts<TDPTable, TOptimizer>::MergeInputCandidates
EnumeratorCheckerWithCosts<TDPTable, TOptimizer>::mergeInputCandidates(const UInt subset, const Int32 key) const
{
    const auto & entry = dp_table[subset];
    MergeInputCandidates candidates;
    auto add = [&](const Plan & plan, Int8 plan_index, bool sorted)
    {
        if (plan.estimated_rows)
            candidates.items[candidates.size++] = {plan_index, {plan.cost, *plan.estimated_rows, sorted}};
    };

    const bool is_leaf = std::popcount(subset) == 1;
    add(entry, -1, is_leaf ? optimizer.isReadInOrder(static_cast<UInt32>(subset), key) : entry.order_class == key);
    /// `ordered` has at most one plan per class.
    for (size_t i = 0; i < entry.ordered.size(); ++i)
    {
        if (entry.ordered[i].order_class == key)
        {
            add(entry.ordered[i], static_cast<Int8>(i), true);
            break;
        }
    }
    return candidates;
}

template <class TDPTable, class TOptimizer>
void EnumeratorCheckerWithCosts<TDPTable, TOptimizer>::acceptMergeJoins(
    const UInt result_subset, const UInt lhs_subset, const UInt rhs_subset, const JoinKind kind, const JoinStrictness strictness,
    const SelectivityEstimate & selectivity, const std::vector<JoinActionRef *> & edges)
{
    const auto left_mask = static_cast<UInt32>(lhs_subset);
    const auto right_mask = static_cast<UInt32>(rhs_subset);
    for (UInt64 keys = optimizer.mergeKeyClasses(edges, left_mask, right_mask, kind, strictness); keys; keys &= keys - 1)
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
                    left_candidate.input, left_mask, right_candidate.input, right_mask, key, selectivity);
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
