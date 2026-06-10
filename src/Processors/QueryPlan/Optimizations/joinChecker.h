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
    EnumeratorChecker(const size_t nr_relations_) : nr_relations(nr_relations_), dptab(nr_relations_) {}
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
    double computeCardinality(Tuint S1, Tuint S2, double selectivity) const;
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
double
EnumeratorCheckerWithCosts<Tdptable, Toptimizer, Tuint>::computeCardinality(const Tuint S1,
                                                                            const Tuint S2,
                                                                            const double selectivity) const
{
    return std::max(selectivity * static_cast<double>(dptab[S1].estimated_rows.value_or(1)) *
                    static_cast<double>(dptab[S2].estimated_rows.value_or(1)),
                    1.0);
}

template <class Tdptable, class Toptimizer, std::unsigned_integral Tuint>
void
EnumeratorCheckerWithCosts<Tdptable, Toptimizer, Tuint>::accept(const Tuint S, const Tuint S1, const Tuint S2)
{
    auto logger = optimizer.log;
    auto lhs = BitSet::fromUint(S1);
    auto rhs = BitSet::fromUint(S2);
    /// Keep the edges that connect left and right
    /// The following loop is unnecessary, since we already have connectedness information in the DP table
    /// FIXME: we need a more efficient way to get the edges that connect left and right

    auto join_kind = optimizer.isValidJoinOrder(lhs, rhs);
    if (!join_kind)
        return;

    /// FIXME: Restrict to Inner joins for now because isValidJoinOrder seems to not handle non-Inner joins with swapped inputs correctly
    if (*join_kind != JoinKind::Inner)
        return;

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

    auto selectivity = optimizer.computeSelectivity(edge, lhs, rhs);

    // LOG_TEST(logger, "Selectivity for {} and {}: [{}, {}]", toString(lhs), toString(rhs), selectivity, dptab[S1].sel * dptab[S2].sel);

    auto plan_cost = computeJoinCost(S1, S2, selectivity);

    // LOG_TEST(logger, "Selectivity for {} and {}: [{}, {}], plan cost: {}", toString(lhs), toString(rhs), selectivity, dptab[S1].sel * dptab[S2].sel, plan_cost);

    if (!dptab.map().contains(S) || plan_cost < dptab[S].cost)
    {
        dptab.insert(S, S1, S2);

        /// Cache the entry reference: insert() guarantees S is present, and
        /// computeCardinality only reads the S1/S2 entries, so the reference
        /// stays valid (unordered_map preserves references across rehashing).
        auto & entry = dptab[S];
        entry.left = S1;
        entry.right = S2;
        entry.cost = plan_cost;
        entry.sel = selectivity;
        entry.estimated_rows = computeCardinality(S1, S2, selectivity);
        entry.edges = edge;
    }
}
}
