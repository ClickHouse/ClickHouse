#pragma once

#include <Processors/QueryPlan/Optimizations/joinOrderCommon.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Common/logger_useful.h>

#include <functional>

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
    double computeJoinCost(UInt lhs, UInt rhs, const JoinKeyEstimate & keys, JoinKind join_kind, JoinStrictness strictness) const;

    void accept(UInt result_subset_, UInt lhs_subset_, UInt rhs_subset_);

    DPTable & getDPTable() { return dp_table; }
    const DPTable & getDPTable() const { return dp_table; }

    size_t numRelations() const { return num_relations; }
private:
    DPTable dp_table;
    Optimizer & optimizer;
    const size_t num_relations;
};

template <class TDPTable, class TOptimizer>
double
EnumeratorCheckerWithCosts<TDPTable, TOptimizer>::computeJoinCost(const UInt lhs,
                                                                  const UInt rhs,
                                                                  const JoinKeyEstimate & keys,
                                                                  JoinKind join_kind,
                                                                  JoinStrictness strictness) const
{
    /// The search value of an entry as `searchRows` counts it, with the same rule for each join below.
    std::function<double(UInt)> search_rows = [&](UInt relations) -> double
    {
        const auto & entry = dp_table[relations];
        if (entry.estimated_rows)
            return static_cast<double>(*entry.estimated_rows);
        if (entry.left != 0 || entry.right != 0)
            return joinSearchRows(entry.sel, search_rows(entry.left), search_rows(entry.right), entry.kind, entry.strictness);
        if (entry.max_rows)
            return static_cast<double>(*entry.max_rows);
        return 1;
    };
    const auto & left_entry = dp_table[lhs];
    const auto & right_entry = dp_table[rhs];
    const double joined_rows = joinedRowsSearchValue(
        keys, left_entry.estimated_rows, right_entry.estimated_rows, search_rows(lhs), search_rows(rhs), join_kind, strictness);
    return left_entry.cost + right_entry.cost + joined_rows;
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
    auto plan_cost = computeJoinCost(lhs_subset, rhs_subset, keys, kind, strictness);

    LOG_TEST(logger, "selectivity: {} costs: {}, lhs est. rows: {}, rhs est. rows: {}",
             keys.selectivity,
             plan_cost,
             dp_table[lhs_subset].estimated_rows.value_or(0),
             dp_table[rhs_subset].estimated_rows.value_or(0));

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
                .sel = keys.selectivity,
                .estimated_rows = optimizer.estimateCardinality(
                    dp_table[lhs_subset].estimated_rows, dp_table[rhs_subset].estimated_rows, keys, kind, strictness),
                .rows_rest_on_assumption = joinRowsRestOnAssumption(keys, dp_table[lhs_subset].estimated_rows, dp_table[rhs_subset].estimated_rows),
                .kind = kind,
                .strictness = strictness,
                .edges = {edge.begin(), edge.end()}};
        });
    }

    if (!dp_table.isConnected(result_subset) || plan_cost < dp_table[result_subset].cost)
    {
        auto & entry = dp_table[result_subset];
        entry.left = lhs_subset;
        entry.right = rhs_subset;
        entry.cost = plan_cost;
        entry.sel = keys.selectivity;
        entry.kind = kind;
        entry.strictness = strictness;
        entry.estimated_rows = optimizer.estimateCardinality(dp_table[lhs_subset].estimated_rows, dp_table[rhs_subset].estimated_rows, keys, kind, strictness);
        entry.rows_rest_on_assumption = joinRowsRestOnAssumption(keys, dp_table[lhs_subset].estimated_rows, dp_table[rhs_subset].estimated_rows);
        entry.max_rows = estimateJoinRowsUpperBound(dp_table[lhs_subset].max_rows, dp_table[rhs_subset].max_rows, kind, strictness);
        entry.edges.assign(edge.begin(), edge.end());
    }
}
}
