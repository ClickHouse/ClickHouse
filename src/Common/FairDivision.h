#pragma once

#include <base/types.h>
#include <base/extended_types.h>
#include <boost/core/noncopyable.hpp>
#include <algorithm>

namespace DB {

/*
 * Solver for max-min fair allocation problem.
 */
template <class T>
struct FairDivision : boost::noncopyable
{
    using Amount = Int64;

    struct Demand
    {
        T item;
        Amount committed;
        Amount soft_limit; // acts like weight for fair shares
        Amount fair_share;

        Demand(T item_, Amount committed_, Amount soft_limit_)
            : item(item_), committed(committed_), soft_limit(soft_limit_), fair_share(0)
        {}

        friend bool operator<(Demand const & lhs, Demand const & rhs) noexcept
        {
            Int128 lhs_committed = lhs.committed, lhs_soft_limit = lhs.soft_limit;
            Int128 rhs_committed = rhs.committed, rhs_soft_limit = rhs.soft_limit;
            // (a / b < c / d) <=> (a * d < c * b)
            return (lhs_committed * rhs_soft_limit) < (rhs_committed * lhs_soft_limit)
                || (lhs_soft_limit == 0 && rhs_soft_limit > 0)
                || (lhs_committed == 0 && rhs_committed == 0 && lhs_soft_limit > rhs_soft_limit);
        }
    };

    void addDemand(T item, Amount committed, Amount soft_limit)
    {
        demands.emplace_back(item, committed, soft_limit);
        total_soft_limit += soft_limit;
    }

    template <class Func>
    void computeFairShares(Amount limit, Func callback)
    {
        Int128 limit_left = limit;
        Int128 soft_limit_left = total_soft_limit;
        std::sort(demands.begin(), demands.end());
        for (Demand & demand : demands)
        {
            demand.fair_share = std::min(limit_left * demand.soft_limit / soft_limit_left, demand.committed);
            limit_left -= demand.fair_share;
            soft_limit_left -= demand.soft_limit;
            callback(demand.item, demand.committed, demand.soft_limit, demand.fair_share);
        }
    }

    void clear()
    {
        demands.clear();
        total_soft_limit = 0;
    }

private:
    std::vector<Demand> demands;
    Amount total_soft_limit = 0;
};

}
