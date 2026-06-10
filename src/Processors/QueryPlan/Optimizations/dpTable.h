#pragma once

#include <base/types.h>
#include <concepts>
#include <limits>
#include <unordered_map>
#include <Common/logger_useful.h>

namespace DB
{

template <class Tentry, std::unsigned_integral Tuint>
class DpTable
{
public:
    struct DPEntry : Tentry
    {
        UInt64 count{0};
    };

    using map_t = std::unordered_map<Tuint, DPEntry>;

    explicit DpTable(size_t nrRelations);

    void insert(Tuint S, Tuint S1, Tuint S2);

    const DPEntry & operator[](Tuint x) const { return dp_tab[x]; }
    DPEntry & operator[](Tuint x) { return dp_tab[x]; }

    bool isConnected(Tuint S) const { return dp_tab.contains(S); }

    size_t n() const { return nr_relations; }
    UInt64 noCcp() const { return nr_ccp; }
    UInt64 noCsg() const { return map().size(); }
    const map_t & map() const { return dp_tab; }

    void printStatistics() const;

private:
    const size_t nr_relations;
    map_t dp_tab;
    UInt64 nr_ccp;
};

template <class Tentry, std::unsigned_integral Tuint>
DpTable<Tentry, Tuint>::DpTable(size_t nrRelations)
    : nr_relations(nrRelations)
    , nr_ccp(0)
{
    /// The DPsub enumerator visits every connected subset, so the table can hold up to 2^n entries.
    /// Compute the bound in size_t to avoid undefined behaviour when shifting a Tuint by its own width.
    dp_tab.reserve(static_cast<size_t>(1) << n());

    for (Tuint i = 0; i < n(); ++i)
        dp_tab[static_cast<Tuint>(1) << i].count = 1;
}

template <class Tentry, std::unsigned_integral Tuint>
void DpTable<Tentry, Tuint>::insert(Tuint S, Tuint S1, Tuint S2)
{
    chassert(0 == (S1 & S2));
    chassert(S == (S1 | S2));

    /// std::unordered_map keeps references to elements valid across rehashing
    /// (only iterators are invalidated), so caching `entry` before touching S1/S2 is safe.
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

template <class Tentry, std::unsigned_integral Tuint>
void DpTable<Tentry, Tuint>::printStatistics() const
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
