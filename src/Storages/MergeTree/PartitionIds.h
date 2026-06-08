#pragma once

#include <boost/container/flat_set.hpp>
#include <base/types.h>

namespace DB
{

/// Set of partition IDs affected by a partition-scoped (`IN PARTITION`) mutation.
/// Most of the code assumes `PartitionIds` to be sorted (it is a `flat_set`) and
/// suitable for binary search. An empty set means the mutation is global, i.e. it
/// applies to all partitions.
using PartitionIds = boost::container::flat_set<String>;

inline bool containsInPartitionIdsOrEmpty(const PartitionIds & haystack, const String & needle)
{
    return haystack.empty() || haystack.contains(needle);
}

/// Whether two partition-scoped mutations affect at least one common partition.
/// An empty set means the mutation is global (affects all partitions), so it overlaps
/// with everything. Both sets are sorted (`flat_set`), so a linear merge suffices.
inline bool partitionIdsOverlap(const PartitionIds & lhs, const PartitionIds & rhs)
{
    if (lhs.empty() || rhs.empty())
        return true;

    auto lhs_it = lhs.cbegin();
    auto rhs_it = rhs.cbegin();
    while (lhs_it != lhs.cend() && rhs_it != rhs.cend())
    {
        if (*lhs_it < *rhs_it)
            ++lhs_it;
        else if (*rhs_it < *lhs_it)
            ++rhs_it;
        else
            return true;
    }
    return false;
}

}
