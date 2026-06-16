#pragma once

#include <bit>
#include <concepts>
#include <vector>
#include <Common/logger_useful.h>
#include <base/types.h>
#include <Processors/QueryPlan/Optimizations/joinOrder.h>

namespace DB
{

/** This an efficient iterator that iterates over all (strict) non-empty subsets of a given
* input set S (excluding the empty subset).
*/
template <std::unsigned_integral TUint>
class NonEmptySubmasks
{
public:
    using Bitvector = TUint;
    constexpr explicit NonEmptySubmasks(const Bitvector start_) noexcept : start(start_) {}
    constexpr TUint getFullSet() const noexcept { return start; }
    class Iterator
    {
    public:
        using value_type = TUint;
        using difference_type = std::ptrdiff_t;

        constexpr Iterator(TUint start_, TUint current_) noexcept
            : start(start_), current(current_) {}

        constexpr TUint operator*() const noexcept { return current; }

        constexpr Iterator& operator++() noexcept
        {
            current = (start & (current - start));
            return *this;
        }

        constexpr Iterator operator++(int) noexcept
        {
            Iterator temp = *this;
            ++(*this);
            return temp;
        }

        constexpr bool operator==(const Iterator& other) const noexcept
        {
            return current == other.current;
        }
    private:
        TUint start;
        TUint current;
    };
    constexpr Iterator begin() const noexcept { return Iterator(start, start & (-start)); }
    constexpr Iterator end() const noexcept   { return Iterator(start, start); }
private:
    Bitvector start;
};

template <class TConsumer, class TDptable, class TQueryGraph, std::unsigned_integral TUint>
class EnumCcpSub
{
    using Consumer = TConsumer;
    using AcceptorFn = Consumer::AcceptorFN;
    using Dptable = TDptable;
    using Graph = TQueryGraph;
    using Uint = TUint;
public:
    EnumCcpSub(UInt64 nr_relations_, UInt64 budget_, LoggerPtr log_);
    UInt64 n() const { return nr_relations; }
    void initDPTable(Dptable & dp_table, const Graph & query_graph);
    bool isConnected(const Dptable & dp_table, Uint S1, Uint S2) const;
    void setTableNeighbor(Dptable & dp_table, Uint S1, Uint S2) const;
    void enumerate(Consumer & consumer, AcceptorFn acceptor, const Graph & query_graph);
private:
    UInt64 nr_relations{0};
    UInt64 budget{0}; // budget cap on nr. of connected components considered by DPsub to avoid excessive optimization time on large join graphs.
    LoggerPtr log;
};


template <class TConsumer, class TDptable, class TQueryGraph, std::unsigned_integral TUint>
EnumCcpSub<TConsumer, TDptable, TQueryGraph, TUint>::EnumCcpSub(UInt64 nr_relations_, UInt64 budget_, LoggerPtr log_)
    : nr_relations(nr_relations_)
    , budget(budget_)
    , log(log_)
{
}

template <class TConsumer, class TDptable, class TQueryGraph, std::unsigned_integral TUint>
void EnumCcpSub<TConsumer, TDptable, TQueryGraph, TUint>::initDPTable(TDptable & dp_table, const TQueryGraph & query_graph)
{
    for (auto & edge : query_graph.edges)
    {
        if (!edge)
            continue;

        const auto & edge_sources = edge.getSourceRelations();

        if (edge_sources.count() != 2) // we are only interested in binary predicates for DP initialization
            continue;

        LOG_TEST(log, "Edge contains relations: {}", toString(edge_sources));

        std::vector<TUint> relations;
        // Fill relations with bit positions set in edge_sources
        for (auto relation : edge_sources)
            relations.push_back(static_cast<TUint>(relation));

        TUint left_mask = (static_cast<TUint>(1) << relations[0]);
        TUint right_mask = (static_cast<TUint>(1) << relations[1]);

        LOG_TEST(log, "Initializing DP table with edge between relations {} and {}", toBinaryString(left_mask), toBinaryString(right_mask));

        dp_table[left_mask].neighbor |= right_mask;
        dp_table[left_mask].estimated_rows = query_graph.relation_stats[relations[0]].estimated_rows;
        dp_table[left_mask].sel = 1.0; // selectivity of a base relation is trivially 1.0
        dp_table[left_mask].column_stats = query_graph.relation_stats[relations[0]].column_stats;

        dp_table[right_mask].neighbor |= left_mask;
        dp_table[right_mask].estimated_rows = query_graph.relation_stats[relations[1]].estimated_rows;
        dp_table[right_mask].sel = 1.0; // selectivity of a base relation is trivially 1.0
        dp_table[right_mask].column_stats = query_graph.relation_stats[relations[1]].column_stats;
    }
}

template <class TConsumer, class TDptable, class TQueryGraph, std::unsigned_integral TUint>
bool EnumCcpSub<TConsumer, TDptable, TQueryGraph, TUint>::isConnected(const TDptable & dp_table, const TUint S1, const TUint S2) const
{
    return (dp_table[S1].neighbor & S2) || (dp_table[S2].neighbor & S1);
}

template <class TConsumer, class TDptable, class TQueryGraph, std::unsigned_integral TUint>
void EnumCcpSub<TConsumer, TDptable, TQueryGraph, TUint>::setTableNeighbor(TDptable & dp_table, const TUint S1, const TUint S2) const
{
    dp_table.insert(S1 | S2, S1, S2);
}

template <class TConsumer, class TDptable, class TQueryGraph, std::unsigned_integral TUint>
void EnumCcpSub<TConsumer, TDptable, TQueryGraph, TUint>::enumerate(TConsumer & consumer,
    typename EnumCcpSub<TConsumer, TDptable, TQueryGraph, TUint>::AcceptorFn acceptor,
    const TQueryGraph & query_graph)
{
    const TUint full_set_mask = (static_cast<TUint>(1) << n()) - 1;

    initDPTable(consumer.dptable(), query_graph);

    /** The integer `s` induces the current subset `S` via its binary representation.
    * Taken as bitvectors, integers in the range [1, 2^n - 1] map exactly to all
    * non-empty subsets of {R_0, ..., R_{n-1}}.
    * Iterating in strictly ascending order guarantees a valid sequence for dynamic
    * programming: for any given subset, all of its proper subsets are guaranteed to
    * be evaluated before the subset itself.
    * This approach is highly performant because subset generation is driven by
    * a native CPU integer increment operation.
    */
    for (TUint s = 1; s <= full_set_mask; ++s)
    {
        if (std::popcount(s) <= 1)
            continue;

        // If the query is large/complex break out of the optimization early
        if (consumer.dptable().noCcp() > budget)
            return;

        NonEmptySubmasks<TUint> subsets(s);
        for (auto s_iter : subsets)
        {
            const TUint lhs = s_iter;
            const TUint rhs = (s ^ lhs);
            LOG_TEST(log, "Enumerating subset S: {}, lhs: {}, rhs: {}", toBinaryString(s), toBinaryString(lhs), toBinaryString(rhs));

            // only generate non-symmetric ccps
            if (lhs > rhs)
                continue;

            if (!(consumer.dptable().isConnected(lhs)))
            {
                LOG_TEST(log, "lhs not connected");
                continue;
            }

            if (!(consumer.dptable().isConnected(rhs)))
            {
                LOG_TEST(log, "rhs not connected");
                continue;
            }

            if (isConnected(consumer.dptable(), lhs, rhs))
            {
                setTableNeighbor(consumer.dptable(), lhs, rhs);
                (consumer.*acceptor)(lhs | rhs, lhs, rhs);
                LOG_TEST(log, "accepted lhs-rhs connected.");
            }
            else if (query_graph.areTransitivelyConnected(BitSet::fromUint(lhs), BitSet::fromUint(rhs)))
            {
                setTableNeighbor(consumer.dptable(), lhs, rhs);
                (consumer.*acceptor)(lhs | rhs, lhs, rhs);
                LOG_TEST(log, "lhs-rhs transitively connected through equivalences, but not directly connected.");
            }
            else
            {
                LOG_TEST(log, "lhs-rhs not connected.");
            }
        }
    }
}

}
