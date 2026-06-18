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
    using TUInt = typename DPTable::Key;
    using Consumer = EnumeratorChecker;
public:
    explicit EnumeratorChecker(const size_t nr_relations_) : nr_relations(nr_relations_), dp_table(nr_relations_) {}
    void accept(TUInt S_, TUInt S1_, TUInt S2_) { dp_table.insert(S_, S1_, S2_); }
    size_t numRelations() const { return nr_relations; }
    DPTable & getDPTable() { return dp_table; }
    const DPTable & getDPTable() const { return dp_table; }
    DPTable * getDPTablePtr() { return (&dp_table); }
private:
    const size_t nr_relations;
    DPTable dp_table;
};

/** This a helper class that is used to check the plans before storing them
* into DP dp_table. Plans are costed here as well.
*/
template <class TDPTable, class TOptimizer>
class EnumeratorCheckerWithCosts
{
    using DPTable = TDPTable;
    using TUInt = typename DPTable::Key;
    using Consumer = EnumeratorCheckerWithCosts;
    using Optimizer = TOptimizer;
public:
    EnumeratorCheckerWithCosts(const size_t nr_relations_, Optimizer & optimizer_)
        : num_relations(nr_relations_), dp_table(nr_relations_), optimizer(optimizer_) {}
    double computeJoinCost(TUInt S1, TUInt S2, double selectivity) const;
    void accept(TUInt S, TUInt S1, TUInt S2);
    size_t numRelations() const { return num_relations; }
    DPTable & getDPTable() { return dp_table; }
    const DPTable & getDPTable() const { return dp_table; }
    DPTable * getDPTablePtr() { return (&dp_table); }
private:
    const size_t num_relations;
    DPTable dp_table;
    Optimizer & optimizer;
};

template <class TDPTable, class TOptimizer>
double
EnumeratorCheckerWithCosts<TDPTable, TOptimizer>::computeJoinCost(const TUInt S1,
                                                                  const TUInt S2,
                                                                  const double selectivity) const
{
    return dp_table[S1].cost + dp_table[S2].cost
        + selectivity * static_cast<double>(dp_table[S1].estimated_rows.value_or(1))
        * static_cast<double>(dp_table[S2].estimated_rows.value_or(1));
}


template <class TDPTable, class TOptimizer>
void
EnumeratorCheckerWithCosts<TDPTable, TOptimizer>::accept(const TUInt S, const TUInt S1, const TUInt S2)
{
    auto logger = optimizer.log;
    auto lhs = BitSet::fromUInt(S1);
    auto rhs = BitSet::fromUInt(S2);

    /// A child is usable only if it is a base relation or a subset for which a valid join
    /// was already recorded. The enumerator's `setTableNeighbor` creates a DP entry for
    /// every connected subset to propagate neighbor info, but for non-Inner joins `accept`
    /// may have rejected the only ordering that builds that subset (e.g. {t2, t3} when t2's
    /// LEFT join requires t1). Such a polluted entry has no recorded join (left == right == 0)
    /// and must not be used as a building block, otherwise `buildPhysicalPlan` mistakes it for
    /// a leaf and emits an incomplete tree.
    auto is_buildable = [&](TUInt s)
    {
        return std::popcount(s) == 1 || (dp_table.map().contains(s) && (dp_table[s].left != 0 || dp_table[s].right != 0));
    };
    if (!is_buildable(S1) || !is_buildable(S2))
        return;

    auto join_kind = optimizer.isValidJoinOrder(lhs, rhs);
    if (!join_kind)
        return;

    auto kind = *join_kind;

    auto applicable_edge = optimizer.getApplicableExpressions(lhs, rhs);
    std::vector<JoinActionRef *> edge;
    for (auto & edge_it : applicable_edge)
    {
        LOG_TEST(
            logger, "Bitset Rep. left: {}, right: {}, edge: {}", toString(lhs), toString(rhs), toString(edge_it->getSourceRelations()));

        if (connects(edge_it, lhs, rhs))
        {
            edge.push_back(edge_it);
        }
        else if ((edge_it->fromLeft() || edge_it->fromRight() || edge_it->fromNone()) && std::popcount(S) == 2)
        {
            /// If a predicate does not connect tables we add it at the earliest stage - when joining just 2 tables
            edge.push_back(edge_it);
        }
        else
        {
            LOG_TEST(logger, "Skipping non-connecting predicate for {} and {} : {}", toString(lhs), toString(rhs), edge_it->dump());
        }
    }

    /// The enumerator only invokes the acceptor for connected pairs, so a `Cross`
    /// kind here is a connected join that should be treated as `Inner` (mirrors the
    /// normalization done by the greedy and DPsize solvers).
    if (kind == JoinKind::Cross)
        kind = JoinKind::Inner;

    auto selectivity = optimizer.computeSelectivity(edge, lhs, rhs);
    auto plan_cost = computeJoinCost(S1, S2, selectivity);

    if (!dp_table.map().contains(S) || plan_cost < dp_table[S].cost)
    {
        auto & entry = dp_table[S];
        entry.left = S1;
        entry.right = S2;
        entry.cost = plan_cost;
        entry.sel = selectivity;
        entry.kind = kind;
        entry.estimated_rows = optimizer.estimateCardinality(dp_table[S1].estimated_rows, dp_table[S2].estimated_rows, selectivity, kind);
        entry.edges = edge;
    }
}
}
