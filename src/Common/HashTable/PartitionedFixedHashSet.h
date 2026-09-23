#pragma once

#include <Common/HashTable/FixedHashSet.h>
#include <Common/HashTable/PartitionedFixedHashTable.h>
#include <Common/HashTable/TwoLevelHashTable.h>


/// Set counterpart of `PartitionedFixedHashMap`, for a caller that only records which keys exist.
/// The cell holds nothing but the presence flag, so routing uses the cache line of that narrower cell.
template <typename Key, size_t size_bits = sizeof(Key) * 8, size_t BITS_FOR_BUCKET = DEFAULT_BITS_FOR_BUCKET>
using PartitionedFixedHashSet = PartitionedFixedHashTable<FixedHashSetWithSizeBits<Key, size_bits>, BITS_FOR_BUCKET>;
