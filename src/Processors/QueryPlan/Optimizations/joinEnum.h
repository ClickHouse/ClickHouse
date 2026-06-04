#pragma once

#include <bit>
#include <concepts>
#include <vector>
#include <Common/logger_useful.h>
#include "base/types.h"
#include "dpTable.h"
#include "joinOrder.h"

namespace DB
{

template <class TConsumer, class TDptable, class TQueryGraph, std::unsigned_integral Tuint>
class EnumCcpSub
{
    using consumer_t = TConsumer;
    using acceptor_fn = consumer_t::acceptor_fn;
    using dptable_t = TDptable;
    using graph_t = TQueryGraph;
    using uint_t = Tuint;
public:
    EnumCcpSub(UInt64 nr_relations_, LoggerPtr log_) : nr_relations(nr_relations_), log(log_) { }
    UInt64 n() const { return nr_relations; }
    void initDPTable(dptable_t & dp_table, const graph_t & query_graph)
    {
        for (auto & edge : query_graph.edges)
        {
            if (!edge)
                continue;

            const auto & edge_sources = edge.getSourceRelations();

            if (edge_sources.count() != 2) // we are only interested in binary predicates for DP initialization
                continue;

            LOG_TEST(log, "Edge contains relations: {}", toString(edge_sources));

            std::vector<UInt32> relations;
            // Fill relations with bit positions set in edge_sources
            for (auto relation : edge_sources)
                relations.push_back(static_cast<UInt32>(relation));


            UInt32 left_mask = (1u << relations[0]);
            UInt32 right_mask = (1u << relations[1]);

            LOG_TEST(log, "Initializing DP table with edge between relations {} and {}",
                 toBinaryString(left_mask), toBinaryString(right_mask));

            dp_table[left_mask].neighbor |= right_mask;
            dp_table[right_mask].neighbor |= left_mask;
        }
    }

    bool isConnected(const dptable_t & dp_table, const uint_t S1, const uint_t S2) const
    {
        return (dp_table[S1].neighbor & S2) || (dp_table[S2].neighbor & S1);
    }

    void setTableNeighbor(dptable_t & dp_table, const uint_t S1, const uint_t S2) const
    {
        const auto full_mask = S1 | S2;

        if (dp_table[full_mask].neighbor == 0)
        {
            dp_table[full_mask].neighbor = (dp_table[S1].neighbor | dp_table[S2].neighbor) & (~full_mask);
        }
    }

    void enumerate(consumer_t & consumer, acceptor_fn acceptor, const graph_t & query_graph)
    {
        const uint_t full_set_mask = (static_cast<Tuint>(1) << n()) - 1;

        initDPTable(consumer.dptable(), query_graph);

        for (Tuint s = 1; s <= full_set_mask; ++s)
        {
            if (std::popcount(s) <= 1)
                continue;

            for (NonEmptySubsetIterator<uint_t> s_iter(s); s_iter.isValid(); ++s_iter)
            {
                const Tuint lhs = s_iter.get();
                const Tuint rhs = (s ^ lhs);
                LOG_TEST(log, "Considering subset S: {}, lhs: {}, rhs: {}",
                                toBinaryString(s), toBinaryString(lhs), toBinaryString(rhs));

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

private:
    UInt64 nr_relations{0};
    LoggerPtr log;
};

} // namespace DB
