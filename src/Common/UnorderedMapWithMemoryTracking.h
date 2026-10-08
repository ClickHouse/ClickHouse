#pragma once

#include <Common/AllocatorWithMemoryTracking.h>

#include <unordered_map>

namespace DB
{

/// We do track memory allocations from standard containers by default, but we do it through
/// non-throwing methods of the `MemoryTracker` (see `trackMemory` in src/Common/memory.h).
/// The following scenario is possible:
///     1. Memory consumption is already very close to the limit
///     2. We're trying to create a huge vector/list (e.g. inside some aggregate function)
///     3. The allocation succeeds (potentially, OS overcommits memory)
///     4. We go over the limit
///     5. OOM killer rightfully kills the server process
/// To prevent this, we provide these `-WithMemoryTracking` aliases to standard containers that use the
/// `AllocatorWithMemoryTracking`, which tracks memory using throwing methods of the `MemoryTracker`.

template <typename K, typename V, typename Hash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
using UnorderedMapWithMemoryTracking = std::unordered_map<K, V, Hash, KeyEqual, AllocatorWithMemoryTracking<std::pair<const K, V>>>;

template <typename K, typename V, typename Hash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
using UnorderedMultiMapWithMemoryTracking
    = std::unordered_multimap<K, V, Hash, KeyEqual, AllocatorWithMemoryTracking<std::pair<const K, V>>>;

}
