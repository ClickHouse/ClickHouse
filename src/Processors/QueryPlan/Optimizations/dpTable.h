#pragma once

#include <base/types.h>
#include <Common/logger_useful.h>
#include <concepts>
#include <limits>

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
        size_t count{0};
    };

    using Key = TUInt;
    using Map = std::vector<DPEntry>;

    explicit DPTable(size_t numRelations_);

    void insert(TUInt S, TUInt S1, TUInt S2);
    const DPEntry & operator[](TUInt x) const { return table.at(x); }
    DPEntry & operator[](TUInt x) { return table[x]; }

    bool isConnected(TUInt S) const { return table[S].count; }
    size_t numRelations() const { return num_relations; }
    size_t noCCP() const { return nr_ccp; }
    size_t noCSG() const { return map().size(); }
    const Map & map() const { return table; }

    void printStatistics() const;
private:
    const size_t num_relations;
    size_t nr_ccp;
    Map table;
};

template <class TEntry, std::unsigned_integral TUInt>
DPTable<TEntry, TUInt>::DPTable(size_t numRelations_)
    : num_relations(numRelations_),
    nr_ccp(0),
    table(static_cast<TUInt>(1) << numRelations_, DPEntry{}) /// reserve space for all subsets of relations
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
    auto logger = getLogger("JoinOrderOptimizer");
    LOG_INFO(logger, "DP Table Statistics:");
    LOG_INFO(logger, "Number of CSGs: {}", noCSG());
    LOG_INFO(logger, "Number of CCPs: {}", noCCP());

    for (Key i = 1; i < (static_cast<TUInt>(1) << numRelations()); ++i)
    {
        const auto & entry = table[i];
        LOG_INFO(logger, "Subset: {}, Count: {}, Cost: {}, Estimated Rows: {}, Selectivity: {}",
                 toBinaryString(i), entry.count, entry.cost, entry.estimated_rows.value_or(0), entry.sel);
    }
}

}
