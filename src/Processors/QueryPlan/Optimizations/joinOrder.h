#pragma once

#include <bit>
#include <concepts>
#include <vector>
#include <Core/Joins.h>
#include <Common/EquivalenceClasses.h>
#include <Common/logger_useful.h>
#include "base/types.h"
#include <Interpreters/JoinOperator.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>

namespace DB
{

struct DPJoinEntry;
using DPJoinEntryPtr = std::shared_ptr<DPJoinEntry>;

enum class JoinMethod : UInt8
{
    None,
    Hash,
    Merge,
};

template <std::unsigned_integral T>
inline String toBinaryString(T value)
{
    return std::bitset<sizeof(T) * 8>(value).to_string();
}

struct DPJoinEntry
{
    BitSet relations;

    DPJoinEntryPtr left;
    DPJoinEntryPtr right;

    double cost = 0.0;
    std::optional<UInt64> estimated_rows = {};
    std::unordered_map<String, ColumnStats> column_stats = {};

    /// For join nodes
    JoinOperator join_operator;
    JoinMethod join_method = JoinMethod::None;

    /// For leaf nodes
    int relation_id = -1;

    /// Constructor for a leaf node (base relation)
    DPJoinEntry(size_t id, std::optional<UInt64> rows, std::unordered_map<String, ColumnStats> column_stats_ = {});

    /// Constructor for a join node
    DPJoinEntry(DPJoinEntryPtr lhs,
                DPJoinEntryPtr rhs,
                double cost_,
                std::optional<UInt64> cardinality_,
                JoinOperator join_operator_,
                JoinMethod join_method_ = JoinMethod::Hash);

    bool isLeaf() const;

    String dump() const;
};

template<std::unsigned_integral Tuint, class Tentry>
class DpTable {
  public:
    struct DPEntry : Tentry
    {
      uint64_t count{0};
      DPEntry() : Tentry() {}
    };
    using map_t = std::unordered_map<Tuint, DPEntry>;
    DpTable(size_t nrRelations) : nr_relations(nrRelations), nr_ccp(0), nr_reordered(0)
    {
        dp_tab.reserve(static_cast<Tuint>(1) << n());

        for (Tuint i = 1; i <= n(); ++i)
            dp_tab[(static_cast<Tuint>(1) << i)].count = 1;
    }
    void insert(Tuint S, Tuint S1, Tuint S2)
    {
        chassert(0 == (S1 & S2));
        chassert(S == (S1 | S2));
        if (S1 > S2)
        {
            ++nr_reordered;
        }
        ++(dp_tab[S].count);
        ++nr_ccp;
    }
    const DPEntry& operator[](Tuint x) const { return (*dp_tab.find(x)).second; }
    DPEntry& operator[](Tuint x) { return dp_tab[x]; }
    bool isConnected(const Tuint S) const { return dp_tab.count(S); }
    inline size_t     n() const { return nr_relations; }
    inline uint64_t noCcp() const { return nr_ccp; }
    inline uint64_t noCsg() const { return map().size(); }
    inline uint64_t noReordered() const { return nr_reordered; }
    void printStatistics(std::ostream& os) const
    {
        os << "DP Table Statistics:\n";
        os << "Number of CSGs: " << noCsg() << "\n";
        os << "Number of CCPs: " << noCcp() << "\n";
        os << "Number of reordered joins: " << noReordered() << "\n";
    }
    inline const map_t& map() const { return dp_tab; }
  private:
    const size_t nr_relations; 
    map_t    dp_tab;
    UInt64   nr_ccp;
    UInt64   nr_reordered;
};

template <class Tuint, class Tdptable>
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

            LOG_TEST(log, "Initializing DP table with edge between relations {} and {}",
                 toBinaryString(relations[0]), toBinaryString(relations[1]));

            UInt32 left_mask = (1u << relations[0]);
            UInt32 right_mask = (1u << relations[1]);

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
        const uint_t all_sets = (static_cast<Tuint>(1) << n()) - 1;

        initDPTable(consumer.dptable(), query_graph);

        for (Tuint s = 1; s <= all_sets; ++s)
        {
            if (std::popcount(s) <= 1)
                continue;

            LOG_TEST(log, "Considering subset S: {}", toBinaryString(s));

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
                else
                {
                    LOG_TEST(log, "lhs-rhs not connected.");
                }
            }
        }
    }

private:
    UInt64  nr_relations{0};
    LoggerPtr log;
};

struct RelationStats
{
    std::optional<UInt64> estimated_rows = {};
    std::unordered_map<String, ColumnStats> column_stats = {};

    String table_name;
};

struct QueryGraph
{
    std::vector<RelationStats> relation_stats;

    std::vector<JoinActionRef> edges;

    /// Shape constraint for a null-supplying relation.
    /// Example: `(A LEFT JOIN B) JOIN C ON B.y=C.y` registers for B:
    ///   required_partners  = {A}   — at `{X} ⋈ {B}`, X must include A.
    ///   forbidden_partners = {C}   — at `{X} ⋈ {B}`, X must not include C
    ///                                (C was pulled across the boundary by `B.y=C.y`;
    ///                                allowing `{A,C} ⋈ {B}` would drag the predicate
    ///                                into the LEFT JOIN's ON clause). It's still fine
    ///                                for C to sit opposite a subtree that *contains*
    ///                                B (e.g. `{A,B} ⋈ {C}`) — the check doesn't fire.
    ///   kind               = LEFT  — kind to return when the shape is valid.
    struct OuterJoinRestriction
    {
        BitSet required_partners;
        BitSet forbidden_partners;
        JoinKind kind{};
    };
    std::unordered_map<size_t, OuterJoinRestriction> join_kinds;

    /// Each predicate may require a set of relations to be already joined before it becomes applicable
    std::unordered_map<JoinActionRef, BitSet> pinned;

    /// Column equivalence classes derived from equi-join edges (e.g., A.x = B.x AND B.x = C.x
    /// implies A.x, B.x, C.x are all equivalent). Used by the join order optimizer to detect
    /// transitive connectivity between relations without synthesizing extra edges.
    /// Stored as alias-resolved JoinActionRef-s pointing to INPUT nodes.
    EquivalenceClasses<JoinActionRef> column_equivalences;

    /// Build equivalence classes from existing edges. Call after all edges are populated.
    void buildColumnEquivalences();

    /// Check if two relation sets are transitively connected through column equivalences
    /// (i.e., there exists at least one equivalence class with members in both sets).
    bool areTransitivelyConnected(const BitSet & left, const BitSet & right) const;
};

struct QueryPlanOptimizationSettings;

DPJoinEntryPtr optimizeJoinOrder(QueryGraph query_graph, const QueryPlanOptimizationSettings & optimization_settings);

}
