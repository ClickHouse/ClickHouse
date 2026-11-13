#pragma once

#include <Common/AllocatorWithMemoryTracking.h>

#include <absl/container/flat_hash_map.h>

namespace DB
{

/// `absl::flat_hash_map` with `AllocatorWithMemoryTracking`, see the comment in `UnorderedMapWithMemoryTracking.h`.
/// Unlike `std::unordered_map`, elements move on rehash and `erase(iterator)` returns nothing.
template <typename K, typename V, typename Hash = absl::DefaultHashContainerHash<K>, typename KeyEqual = absl::DefaultHashContainerEq<K>>
using FlatHashMapWithMemoryTracking = absl::flat_hash_map<K, V, Hash, KeyEqual, AllocatorWithMemoryTracking<std::pair<const K, V>>>;

}
