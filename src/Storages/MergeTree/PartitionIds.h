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

}
