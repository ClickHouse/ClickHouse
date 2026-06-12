#pragma once

#include <bit>
#include <concepts>
#include <vector>
#include <Common/logger_useful.h>
#include "base/types.h"
#include "joinOrder.h"

namespace DB
{

// generates S subset X, where S is not empty
template <std::unsigned_integral Tuint>
class NonEmptySubmasks
{
public:
    using bitvector_t = Tuint;
    
    constexpr explicit NonEmptySubmasks(const bitvector_t x) noexcept : start(x) {}
    
    constexpr Tuint getFullSet() const noexcept { return start; }

    class Iterator 
    {
    public:
        using value_type = Tuint;
        using difference_type = std::ptrdiff_t;

        constexpr Iterator(Tuint start_, Tuint current_) noexcept
            : start(start_), current(current_) {}

        constexpr Tuint operator*() const noexcept { return current; }

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
        Tuint start;
        Tuint current;
    };

    constexpr Iterator begin() const noexcept { return Iterator(start, start & (-start)); }
    constexpr Iterator end() const noexcept   { return Iterator(start, start); } 

private:
    bitvector_t start;
};

template <class TConsumer, class TDptable, class TQueryGraph, std::unsigned_integral Tuint>
class EnumCcpSub
{
    using consumer_t = TConsumer;
    using acceptor_fn = consumer_t::acceptor_fn;
    using dptable_t = TDptable;
    using graph_t = TQueryGraph;
    using uint_t = Tuint;
public:
    EnumCcpSub(UInt64 nr_relations_, LoggerPtr log_);
    UInt64 n() const { return nr_relations; }
    void initDPTable(dptable_t & dp_table, const graph_t & query_graph);
    bool isConnected(const dptable_t & dp_table, const uint_t S1, const uint_t S2) const;
    void setTableNeighbor(dptable_t & dp_table, const uint_t S1, const uint_t S2) const;
    void enumerate(consumer_t & consumer, acceptor_fn acceptor, const graph_t & query_graph);
private:
    UInt64 nr_relations{0};
    LoggerPtr log;
};


template <class TConsumer, class TDptable, class TQueryGraph, std::unsigned_integral Tuint>
EnumCcpSub<TConsumer, TDptable, TQueryGraph, Tuint>::EnumCcpSub(UInt64 nr_relations_, LoggerPtr log_)
    : nr_relations(nr_relations_)
    , log(log_)
{
}

template <class TConsumer, class TDptable, class TQueryGraph, std::unsigned_integral Tuint>
void EnumCcpSub<TConsumer, TDptable, TQueryGraph, Tuint>::initDPTable(TDptable & dp_table, const TQueryGraph & query_graph)
{
    for (auto & edge : query_graph.edges)
    {
        if (!edge)
            continue;

        const auto & edge_sources = edge.getSourceRelations();

        if (edge_sources.count() != 2) // we are only interested in binary predicates for DP initialization
            continue;

        LOG_TEST(log, "Edge contains relations: {}", toString(edge_sources));

        std::vector<Tuint> relations;
        // Fill relations with bit positions set in edge_sources
        for (auto relation : edge_sources)
            relations.push_back(static_cast<Tuint>(relation));

        Tuint left_mask = (static_cast<Tuint>(1) << relations[0]);
        Tuint right_mask = (static_cast<Tuint>(1) << relations[1]);

        LOG_TEST(log, "Initializing DP table with edge between relations {} and {}", toBinaryString(left_mask), toBinaryString(right_mask));

        dp_table[left_mask].neighbor |= right_mask;
        dp_table[left_mask].estimated_rows = query_graph.relation_stats[relations[0]].estimated_rows.value_or(1);
        dp_table[left_mask].sel = 1.0; // selectivity of a base relation is trivially 1.0
        dp_table[left_mask].column_stats = query_graph.relation_stats[relations[0]].column_stats;

        dp_table[right_mask].neighbor |= left_mask;
        dp_table[right_mask].estimated_rows = query_graph.relation_stats[relations[1]].estimated_rows.value_or(1);
        dp_table[right_mask].sel = 1.0; // selectivity of a base relation is trivially 1.0
        dp_table[right_mask].column_stats = query_graph.relation_stats[relations[1]].column_stats;
    }
}

template <class TConsumer, class TDptable, class TQueryGraph, std::unsigned_integral Tuint>
bool EnumCcpSub<TConsumer, TDptable, TQueryGraph, Tuint>::isConnected(const TDptable & dp_table, const Tuint S1, const Tuint S2) const
{
    return (dp_table[S1].neighbor & S2) || (dp_table[S2].neighbor & S1);
}

template <class TConsumer, class TDptable, class TQueryGraph, std::unsigned_integral Tuint>
void EnumCcpSub<TConsumer, TDptable, TQueryGraph, Tuint>::setTableNeighbor(TDptable & dp_table, const Tuint S1, const Tuint S2) const
{
    dp_table.insert(S1 | S2, S1, S2);
}

template <class TConsumer, class TDptable, class TQueryGraph, std::unsigned_integral Tuint>
void EnumCcpSub<TConsumer, TDptable, TQueryGraph, Tuint>::enumerate(
    TConsumer & consumer,
    typename EnumCcpSub<TConsumer, TDptable, TQueryGraph, Tuint>::acceptor_fn acceptor,
    const TQueryGraph & query_graph)
{
    const Tuint full_set_mask = (static_cast<Tuint>(1) << n()) - 1;

    initDPTable(consumer.dptable(), query_graph);

    for (Tuint s = 1; s <= full_set_mask; ++s)
    {
        if (std::popcount(s) <= 1)
            continue;

        NonEmptySubmasks<Tuint> subsets(s);
        for (auto s_iter : subsets)
        {
            const Tuint lhs = s_iter;
            const Tuint rhs = (s ^ lhs);
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

} // namespace DB
