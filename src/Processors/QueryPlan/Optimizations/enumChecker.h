#pragma once

#include <Interpreters/JoinExpressionActions.h>
#include <Common/logger_useful.h>

namespace DB
{

/**
* This a helper class that is used to check the plans before storing them
* into DP table. No plan costing here. 
*/
template <class TDptable, std::unsigned_integral TUint>
class EnumeratorChecker
{
    using DPtable = TDptable;
    using Consumser = EnumeratorChecker;
public:
    using acceptor_fn = void (EnumeratorChecker::*)(TUint, TUint, TUint);
    explicit EnumeratorChecker(const size_t nr_relations_) : nr_relations(nr_relations_), dptab(nr_relations_) {}
    void accept(TUint S_, TUint S1_, TUint S2_) { dptab.insert(S_, S1_, S2_); }
    inline size_t n() const { return nr_relations; }
    inline DPtable & dptable() { return dptab; }
    inline const DPtable & dptable() const { return dptab; }
    inline DPtable * dptablePtr() { return (&dptab); }
private:
    const size_t nr_relations;
    DPtable dptab;
};

/**
* This a helper class that is used to check the plans before storing them
* into DP table. Plans are costed here as well.
*/
template <class TDptable, class TOptimizer, std::unsigned_integral TUint>
class EnumeratorCheckerWithCosts
{
    using Dptable = TDptable;
    using Consumser = EnumeratorCheckerWithCosts;
    using Optimizer = TOptimizer;
public:
    using AcceptorFN = void (EnumeratorCheckerWithCosts::*)(TUint, TUint, TUint);
    EnumeratorCheckerWithCosts(const size_t nr_relations_, Optimizer & optimizer_)
        : nr_relations(nr_relations_), dptab(nr_relations_), optimizer(optimizer_) {}
    double computeJoinCost(TUint S1, TUint S2, double selectivity) const;
    void accept(TUint S, TUint S1, TUint S2);
    size_t n() const { return nr_relations; }
    Dptable & dptable() { return dptab; }
    const Dptable & dptable() const { return dptab; }
    Dptable * dptablePtr() { return (&dptab); }
private:
    const size_t nr_relations;
    Dptable dptab;
    Optimizer & optimizer;
};

template <class TDptable, class TOptimizer, std::unsigned_integral TUint>
double
EnumeratorCheckerWithCosts<TDptable, TOptimizer, TUint>::computeJoinCost(const TUint S1,
                                                                         const TUint S2,
                                                                         const double selectivity) const
{
    return dptab[S1].cost + dptab[S2].cost
        + selectivity * static_cast<double>(dptab[S1].estimated_rows.value_or(1))
        * static_cast<double>(dptab[S2].estimated_rows.value_or(1));
}


template <class TDptable, class TOptimizer, std::unsigned_integral TUint>
void
EnumeratorCheckerWithCosts<TDptable, TOptimizer, TUint>::accept(const TUint S, const TUint S1, const TUint S2)
{
    auto logger = optimizer.log;
    auto lhs = BitSet::fromUint(S1);
    auto rhs = BitSet::fromUint(S2);

    /// A child is usable only if it is a base relation or a subset for which a valid join
    /// was already recorded. The enumerator's `setTableNeighbor` creates a DP entry for
    /// every connected subset to propagate neighbor info, but for non-Inner joins `accept`
    /// may have rejected the only ordering that builds that subset (e.g. {t2, t3} when t2's
    /// LEFT join requires t1). Such a polluted entry has no recorded join (left == right == 0)
    /// and must not be used as a building block, otherwise `buildPhysicalPlan` mistakes it for
    /// a leaf and emits an incomplete tree.
    auto is_buildable = [&](TUint s)
    {
        return std::popcount(s) == 1 || (dptab.map().contains(s) && (dptab[s].left != 0 || dptab[s].right != 0));
    };
    if (!is_buildable(S1) || !is_buildable(S2))
        return;

    auto join_kind = optimizer.isValidJoinOrder(lhs, rhs);
    if (!join_kind)
        return;

    /// `isValidJoinOrder` returns the kind relative to the (S1, S2) orientation we were
    /// handed, so we can use it directly without swapping the inputs.
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

    if (!dptab.map().contains(S) || plan_cost < dptab[S].cost)
    {
        dptab.insert(S, S1, S2);

        auto & entry = dptab[S];
        entry.left = S1;
        entry.right = S2;
        entry.cost = plan_cost;
        entry.sel = selectivity;
        entry.kind = kind;
        entry.estimated_rows = optimizer.estimateCardinality(dptab[S1].estimated_rows, dptab[S2].estimated_rows, selectivity, kind);
        entry.edges = edge;
    }
}
}
