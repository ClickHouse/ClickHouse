#pragma once

#include <Interpreters/JoinExpressionActions.h>
#include <Common/logger_useful.h>

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
    double computeJoinCost(UInt lhs, UInt rhs, double selectivity) const;

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
                                                                  const double selectivity) const
{
    return dp_table[lhs].cost + dp_table[rhs].cost
        + selectivity * static_cast<double>(dp_table[lhs].estimated_rows.value_or(1))
        * static_cast<double>(dp_table[rhs].estimated_rows.value_or(1));
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

    auto join_kind = optimizer.isValidJoinOrderMask(left_mask, right_mask);
    if (!join_kind)
        return;

    auto kind = *join_kind;

    /// `edge` aliases an internal scratch buffer that the next `collectJoinEdgesMask` call overwrites
    /// it is only read below and copied into the DP entry, so the aliasing is safe.
    const auto & edge = optimizer.collectJoinEdgesMask(left_mask, right_mask);

    /// The enumerator only invokes the acceptor for connected pairs, so a `Cross`
    /// kind here is a connected join that should be treated as `Inner` (mirrors the
    /// normalization done by the greedy and DPsize solvers).
    if (kind == JoinKind::Cross)
        kind = JoinKind::Inner;

    auto selectivity = optimizer.computeSelectivityMask(edge, left_mask, right_mask);
    auto plan_cost = computeJoinCost(lhs_subset, rhs_subset, selectivity);

    LOG_TEST(logger, "selectivity: {} costs: {}, lhs est. rows: {}, rhs est. rows: {}",
             selectivity,
             plan_cost,
             dp_table[lhs_subset].estimated_rows.value_or(0),
             dp_table[rhs_subset].estimated_rows.value_or(0));

    if (!dp_table.isConnected(result_subset) || plan_cost < dp_table[result_subset].cost)
    {
        auto & entry = dp_table[result_subset];
        entry.left = lhs_subset;
        entry.right = rhs_subset;
        entry.cost = plan_cost;
        entry.sel = selectivity;
        entry.kind = kind;
        entry.estimated_rows = optimizer.estimateCardinality(dp_table[lhs_subset].estimated_rows, dp_table[rhs_subset].estimated_rows, selectivity, kind);
        entry.edges.assign(edge.begin(), edge.end());
    }
}
}
