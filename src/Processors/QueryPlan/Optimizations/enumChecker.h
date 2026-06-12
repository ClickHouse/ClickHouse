#pragma once

#include <Interpreters/JoinExpressionActions.h>
#include <Common/logger_useful.h>

namespace DB
{

template <class Tdptable, std::unsigned_integral Tuint>
class EnumeratorChecker
{
    using dptable_t = Tdptable;
    using consumser_t = EnumeratorChecker;
public:
    using acceptor_fn = void (EnumeratorChecker::*)(Tuint, Tuint, Tuint);
    explicit EnumeratorChecker(const size_t nr_relations_) : nr_relations(nr_relations_), dptab(nr_relations_) {}
    void accept(Tuint S, Tuint S1, Tuint S2) { dptab.insert(S, S1, S2); }
    inline size_t n() const { return nr_relations; }
    inline dptable_t & dptable() { return dptab; }
    inline const dptable_t & dptable() const { return dptab; }
    inline dptable_t * dptablePtr() { return (&dptab); }
private:
    const size_t nr_relations;
    dptable_t dptab;
};

template <class Tdptable, class Toptimizer, std::unsigned_integral Tuint>
class EnumeratorCheckerWithCosts
{
    using dptable_t = Tdptable;
    using consumser_t = EnumeratorCheckerWithCosts;
    using optimizer_t = Toptimizer;
public:
    using acceptor_fn = void (EnumeratorCheckerWithCosts::*)(Tuint, Tuint, Tuint);
    EnumeratorCheckerWithCosts(const size_t nr_relations_, optimizer_t & optimizer_)
        : nr_relations(nr_relations_), dptab(nr_relations_), optimizer(optimizer_) {}
    double computeJoinCost(Tuint S1, Tuint S2, double selectivity) const;
    void accept(Tuint S, Tuint S1, Tuint S2);
    inline size_t n() const { return nr_relations; }
    inline dptable_t & dptable() { return dptab; }
    inline const dptable_t & dptable() const { return dptab; }
    inline dptable_t * dptablePtr() { return (&dptab); }
private:
    const size_t nr_relations;
    dptable_t dptab;
    optimizer_t & optimizer;
};

template <class Tdptable, class Toptimizer, std::unsigned_integral Tuint>
double
EnumeratorCheckerWithCosts<Tdptable, Toptimizer, Tuint>::computeJoinCost(const Tuint S1,
                                                                         const Tuint S2,
                                                                         const double selectivity) const
{
    return dptab[S1].cost + dptab[S2].cost
        + selectivity * static_cast<double>(dptab[S1].estimated_rows.value_or(1))
        * static_cast<double>(dptab[S2].estimated_rows.value_or(1));
}


template <class Tdptable, class Toptimizer, std::unsigned_integral Tuint>
void
EnumeratorCheckerWithCosts<Tdptable, Toptimizer, Tuint>::accept(const Tuint S, const Tuint S1, const Tuint S2)
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
    auto is_buildable = [&](Tuint s)
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
