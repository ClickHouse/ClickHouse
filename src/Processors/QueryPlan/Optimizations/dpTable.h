#pragma once

#include <base/types.h>
#include <concepts>
#include <limits>
#include <unordered_map>
#include <Common/logger_useful.h>

namespace DB
{

/**
* DP table for storing query plans and fetching them.
* Besides storing plans, it also computes `csg` & `ccp` statistics:
* `csg`: (non-empty) connected subgraph,
* `ccp`: `csg` and its complement pair.
*/
template <class TEntry, std::unsigned_integral TUint>
class DpTable
{
public:
    struct DPEntry : TEntry
    {
        UInt64 count{0};
    };

    using Map = std::unordered_map<TUint, DPEntry>;

    explicit DpTable(size_t nrRelations_);

    void insert(TUint S, TUint S1, TUint S2);

    const DPEntry & operator[](TUint x) const { return dp_tab.at(x); }
    DPEntry & operator[](TUint x) { return dp_tab[x]; }

    bool isConnected(TUint S) const { return dp_tab.contains(S); }
    size_t n() const { return nr_relations; }
    UInt64 noCcp() const { return nr_ccp; }
    UInt64 noCsg() const { return map().size(); }
    const Map & map() const { return dp_tab; }

    void printStatistics() const;
private:
    const size_t nr_relations;
    Map dp_tab;
    UInt64 nr_ccp;
};

template <class TEntry, std::unsigned_integral TUint>
DpTable<TEntry, TUint>::DpTable(size_t nrRelations_)
    : nr_relations(nrRelations_)
    , nr_ccp(0)
{
    for (TUint i = 0; i < n(); ++i)
        dp_tab[static_cast<TUint>(1) << i].count = 1;
}

template <class TEntry, std::unsigned_integral TUint>
void DpTable<TEntry, TUint>::insert(TUint S, TUint S1, TUint S2)
{
    chassert(0 == (S1 & S2));
    chassert(S == (S1 | S2));

    auto & entry = dp_tab[S];
    if (entry.count == 0)
    {
        /// First time we see this subset: propagate the children's neighbor info,
        /// masking out bits belonging to S, and set the cost to +inf so the first
        /// real plan replaces it.
        entry.neighbor = (dp_tab[S1].neighbor | dp_tab[S2].neighbor) & (~S);
        entry.cost = std::numeric_limits<double>::infinity();
    }

    ++entry.count;
    ++nr_ccp;
}

template <class TEntry, std::unsigned_integral TUint>
void DpTable<TEntry, TUint>::printStatistics() const
{
    auto * logger = &Poco::Logger::get("JoinOrderOptimizer");
    LOG_INFO(logger, "DP Table Statistics:");
    LOG_INFO(logger, "Number of CSGs: {}", noCsg());
    LOG_INFO(logger, "Number of CCPs: {}", noCcp());

    for (const auto & [key, entry] : map())
    {
        LOG_INFO(logger, "Subset: {}, Count: {}, Cost: {}, Estimated Rows: {}, Selectivity: {}",
                 toBinaryString(key), entry.count, entry.cost, entry.estimated_rows.value_or(0), entry.sel);
    }
}

}
