#pragma once

#include <Common/AllocatorWithMemoryTracking.h>

#include <deque>
#include <list>
#include <map>
#include <queue>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>

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
/// To prevent this, we provide these `Strict-` aliases to standard containers that use the
/// `AllocatorWithMemoryTracking`, which tracks memory using throwing methods of the `MemoryTracker`.

template <typename T>
using StrictList = std::list<T, AllocatorWithMemoryTracking<T>>;

template <typename T>
using StrictVector = std::vector<T, AllocatorWithMemoryTracking<T>>;

template <typename T>
using StrictDeque = std::deque<T, AllocatorWithMemoryTracking<T>>;

template <typename T>
using StrictQueue = std::queue<T, AllocatorWithMemoryTracking<T>>;

template <typename K, typename V>
using StrictMap = std::map<K, V, std::less<K>, AllocatorWithMemoryTracking<std::pair<const K, V>>>;

template <typename K, typename V>
using StrictMultiMap = std::multimap<K, V, std::less<K>, AllocatorWithMemoryTracking<std::pair<const K, V>>>;

template <typename K>
using StrictSet = std::set<K, std::less<K>, AllocatorWithMemoryTracking<K>>;

template <typename K>
using StrictMultiSet = std::multiset<K, std::less<K>, AllocatorWithMemoryTracking<K>>;

template <typename K, typename V, typename Hash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
using StrictUnorderedMap = std::unordered_map<K, V, Hash, KeyEqual, AllocatorWithMemoryTracking<std::pair<const K, V>>>;

template <typename K, typename V, typename Hash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
using StrictUnorderedMultiMap = std::unordered_multimap<K, V, Hash, KeyEqual, AllocatorWithMemoryTracking<std::pair<const K, V>>>;

template <typename K, typename Hash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
using StrictUnorderedSet = std::unordered_set<K, Hash, KeyEqual, AllocatorWithMemoryTracking<K>>;

template <typename K, typename Hash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
using StrictUnorderedMultiSet = std::unordered_multiset<K, Hash, KeyEqual, AllocatorWithMemoryTracking<K>>;

}
