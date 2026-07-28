#pragma once

#include <bit>
#include <concepts>
#include <vector>
#include <Common/logger_useful.h>
#include <base/types.h>
#include <Processors/QueryPlan/Optimizations/joinOrder.h>

namespace DB
{

/** Iterator that iterates over all (strict) non-empty subsets of a given
* input set S (excluding the empty subset).
*/
template <std::unsigned_integral UInt>
class NonEmptySubmasks
{
public:
    using Bitvector = UInt;
    constexpr explicit NonEmptySubmasks(const Bitvector start_) noexcept : start(start_) {}
    constexpr UInt getFullSet() const noexcept { return start; }
    class Iterator
    {
    public:
        using value_type = UInt;
        using difference_type = std::ptrdiff_t;

        constexpr Iterator(UInt start_, UInt current_) noexcept
            : start(start_), current(current_) {}

        constexpr UInt operator*() const noexcept { return current; }

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
        UInt start;
        UInt current;
    };
    constexpr Iterator begin() const noexcept { return Iterator(start, start & (-start)); }
    constexpr Iterator end() const noexcept   { return Iterator(start, start); }
private:
    Bitvector start;
};

template <class TConsumer, class TDPTable, class TQueryGraph>
class EnumCcpSub
{
    using Consumer = TConsumer;
    using Dptable = TDPTable;
    using Graph = TQueryGraph;
    using UInt = TDPTable::Key;
public:
    EnumCcpSub(UInt64 nr_relations_, UInt64 budget_, LoggerPtr log_);
    UInt64 n() const { return nr_relations; }
    void initDPTable(Dptable & dp_table, const Graph & query_graph);
    bool isConnected(const Dptable & dp_table, UInt S1, UInt S2) const;
    void setTableNeighbor(Dptable & dp_table, UInt S1, UInt S2) const;
    void enumerate(Consumer & consumer, const Graph & query_graph);
private:
    UInt64 nr_relations{0};
    UInt64 budget{0}; // budget cap on # of ccps enumerated by DPsub
    LoggerPtr log;
};


template <class TConsumer, class TDPTable, class TQueryGraph>
EnumCcpSub<TConsumer, TDPTable, TQueryGraph>::EnumCcpSub(UInt64 nr_relations_, UInt64 budget_, LoggerPtr log_)
    : nr_relations(nr_relations_)
    , budget(budget_)
    , log(log_)
{
}

template <class TConsumer, class TDPTable, class TQueryGraph>
void EnumCcpSub<TConsumer, TDPTable, TQueryGraph>::initDPTable(TDPTable & dp_table, const TQueryGraph & query_graph)
{
    for (auto & edge : query_graph.edges)
    {
        if (!edge)
            continue;

        const auto & edge_sources = edge.getSourceRelations();

        if (edge_sources.count() != 2) // we are only interested in binary predicates for DP initialization
            continue;

        LOG_TEST(log, "Edge contains relations: {} edge info: {}", toString(edge_sources), edge.dump());

        std::vector<UInt> relations;
        relations.reserve(edge_sources.count());

        // Fill relations with bit positions set in edge_sources
        for (auto relation : edge_sources)
            relations.push_back(static_cast<UInt>(relation));

        UInt left_mask = (static_cast<UInt>(1) << relations[0]);
        UInt right_mask = (static_cast<UInt>(1) << relations[1]);

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

template <class TConsumer, class TDPTable, class TQueryGraph>
bool EnumCcpSub<TConsumer, TDPTable, TQueryGraph>::isConnected(const TDPTable & dp_table, const UInt S1, const UInt S2) const
{
    return (dp_table[S1].neighbor & S2) || (dp_table[S2].neighbor & S1);
}

template <class TConsumer, class TDPTable, class TQueryGraph>
void EnumCcpSub<TConsumer, TDPTable, TQueryGraph>::setTableNeighbor(TDPTable & dp_table, const UInt S1, const UInt S2) const
{
    dp_table.insert(S1 | S2, S1, S2);
}

template <class TConsumer, class TDPTable, class TQueryGraph>
void EnumCcpSub<TConsumer, TDPTable, TQueryGraph>::enumerate(TConsumer & consumer, const TQueryGraph & query_graph)
{
    const UInt full_set_mask = (static_cast<UInt>(1) << n()) - 1;

    initDPTable(consumer.getDPTable(), query_graph);

    /** The integer `s` induces the current subset `S` via its binary representation.
    * Taken as bitvectors, integers in the range [1, 2^n - 1] map exactly to all
    * non-empty subsets of {R_0, ..., R_{n-1}}.
    * Iterating in strictly ascending order guarantees a valid sequence for dynamic
    * programming: for any given subset, all of its proper subsets are guaranteed to
    * be evaluated before the subset itself.
    * This approach is highly performant because subset generation is driven by
    * a native CPU integer increment operation.
    */
    for (UInt s = 1; s <= full_set_mask; ++s)
    {
        if (std::popcount(s) <= 1)
            continue;

        auto & dp_table = consumer.getDPTable();
        // If the query is large/complex break out of the optimization early
        if (dp_table.noCCP() > budget)
            return;

        NonEmptySubmasks<UInt> subsets(s);
        for (auto s_iter : subsets)
        {
            const UInt lhs = s_iter;
            const UInt rhs = (s ^ lhs);

            // only generate non-symmetric ccps
            if (lhs > rhs)
                continue;

            LOG_TEST(log, "Enumerating subset S: {}, lhs: {}, rhs: {}", toBinaryString(s), toBinaryString(lhs), toBinaryString(rhs));

            if (!(dp_table.isConnected(lhs)))
            {
                LOG_TEST(log, "lhs subset '{}' not connected", toBinaryString(lhs));
                continue;
            }

            if (!(dp_table.isConnected(rhs)))
            {
                LOG_TEST(log, "rhs subset '{}' not connected", toBinaryString(rhs));
                continue;
            }

            if (isConnected(dp_table, lhs, rhs))
            {
                setTableNeighbor(dp_table, lhs, rhs);
                consumer.accept(lhs | rhs, lhs, rhs);
                LOG_TEST(log, "accepted lhs-rhs connected.");
            }
            else if (query_graph.areTransitivelyConnected(BitSet::fromUInt(lhs), BitSet::fromUInt(rhs)))
            {
                setTableNeighbor(dp_table, lhs, rhs);
                consumer.accept(lhs | rhs, lhs, rhs);
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
