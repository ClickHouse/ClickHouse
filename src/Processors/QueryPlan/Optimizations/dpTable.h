#pragma once

#include "base/types.h"
#include <concepts>
#include <unordered_map>
#include <Common/logger_useful.h>

namespace DB
{

template<class Tentry, std::unsigned_integral Tuint>
class DpTable {
  public:
    struct DPEntry : Tentry
    {
      UInt64 count{0};
      DPEntry() : Tentry() {}
    };
    using map_t = std::unordered_map<Tuint, DPEntry>;
    DpTable(size_t nrRelations);
    void insert(Tuint S, Tuint S1, Tuint S2);
    const DPEntry& operator[](Tuint x) const { return (*dp_tab.find(x)).second; }
    DPEntry& operator[](Tuint x) { return dp_tab[x]; }
    bool isConnected(const Tuint S) const { return dp_tab.count(S); }
    inline size_t     n() const { return nr_relations; }
    inline UInt64 noCcp() const { return nr_ccp; }
    inline UInt64 noCsg() const { return map().size(); }
    inline UInt64 noReordered() const { return nr_reordered; }
    void printStatistics() const;
    inline const map_t& map() const { return dp_tab; }
  private:
    const size_t nr_relations; 
    map_t    dp_tab;
    UInt64   nr_ccp;
    UInt64   nr_reordered;
};

template<class Tentry, std::unsigned_integral Tuint>
DpTable<Tentry, Tuint>::DpTable(size_t nrRelations)
    : nr_relations(nrRelations)
    , nr_ccp(0)
    , nr_reordered(0)
{
    dp_tab.reserve(static_cast<Tuint>(1) << n());

    for (Tuint i = 0; i < n(); ++i)
        dp_tab[(static_cast<Tuint>(1) << i)].count = 1;
}

template<class Tentry, std::unsigned_integral Tuint>
void DpTable<Tentry, Tuint>::insert(Tuint S, Tuint S1, Tuint S2)
{
    chassert(0 == (S1 & S2));
    chassert(S == (S1 | S2));
    if (S1 > S2)
        ++nr_reordered;
  // If this entry does not yet exist (count == 0), initialize fields
  // that other code expects to be non-default so that subsequent
  // comparisons against stored cost behave correctly.
  if (dp_tab[S].count == 0)
  {
    // Propagate neighbor info from children, masking out bits belonging to S.
    dp_tab[S].neighbor = (dp_tab[S1].neighbor | dp_tab[S2].neighbor) & (~S);
    // Initialize cost to +inf so the first real plan will replace it.
    dp_tab[S].cost = std::numeric_limits<double>::infinity();
  }

  ++(dp_tab[S].count);
  ++nr_ccp;
}

template<class Tentry, std::unsigned_integral Tuint>
void DpTable<Tentry, Tuint>::printStatistics() const
{
    auto logger = &Poco::Logger::get("JoinOrderOptimizer");
    LOG_INFO(logger, "DP Table Statistics:");
    LOG_INFO(logger, "Number of CSGs: {}", noCsg());
    LOG_INFO(logger, "Number of CCPs: {}", noCcp());
    LOG_INFO(logger, "Number of reordered joins: {}", noReordered());


    for (const auto & [key, entry] : map())
    {
        LOG_INFO(logger, "Subset: {}, Count: {}, Cost: {}, Estimated Rows: {}, Selectivity: {}",
                 toBinaryString(key), entry.count, entry.cost, entry.estimated_rows.value_or(0), entry.sel);
    }
}


}
