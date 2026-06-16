#pragma once

#include <base/types.h>
#include <concepts>
#include <limits>
#include <unordered_map>
#include <Common/logger_useful.h>

namespace DB
{

/** DP table for storing query plans and fetching them.
* Besides storing plans, it also computes `csg` & `ccp` statistics:
* `csg`: (non-empty) connected subgraph,
* `ccp`: `csg` and its complement pair.
*/
template <class TEntry, std::unsigned_integral TUInt>
class DPTable
{
public:
    struct DPEntry : TEntry
    {
        UInt64 count{0};
    };

    using Key = TUInt;
    using Map = std::unordered_map<Key, DPEntry>;

    explicit DPTable(size_t numRelations_);

    void insert(TUInt S, TUInt S1, TUInt S2);

    const DPEntry & operator[](TUInt x) const { return table.at(x); }
    DPEntry & operator[](TUInt x) { return table[x]; }

    bool isConnected(TUInt S) const { return table.contains(S); }
    size_t numRelations() const { return num_relations; }
    UInt64 noCcp() const { return nr_ccp; }
    UInt64 noCsg() const { return map().size(); }
    const Map & map() const { return table; }

    void printStatistics() const;
private:
    const size_t num_relations;
    Map table;
    UInt64 nr_ccp;
};

template <class TEntry, std::unsigned_integral TUInt>
DPTable<TEntry, TUInt>::DPTable(size_t numRelations_)
    : num_relations(numRelations_)
    , nr_ccp(0)
{
    for (TUInt i = 0; i < numRelations(); ++i)
        table[static_cast<TUInt>(1) << i].count = 1;
}

template <class TEntry, std::unsigned_integral TUInt>
void DPTable<TEntry, TUInt>::insert(TUInt S, TUInt S1, TUInt S2)
{
    chassert(0 == (S1 & S2));
    chassert(S == (S1 | S2));

    auto & entry = table[S];
    if (entry.count == 0)
    {
        /// First time we see this subset: propagate the children's neighbor info,
        /// masking out bits belonging to S, and set the cost to +inf so the first
        /// real plan replaces it.
        entry.neighbor = (table[S1].neighbor | table[S2].neighbor) & (~S);
        entry.cost = std::numeric_limits<double>::infinity();
    }

    ++entry.count;
    ++nr_ccp;
}

template <class TEntry, std::unsigned_integral TUInt>
void DPTable<TEntry, TUInt>::printStatistics() const
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
