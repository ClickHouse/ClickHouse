#pragma once

#include <type_traits>
#include <Common/HashTable/FixedHashSet.h>
#include <Common/HashTable/TwoLevelHashTable.h>


template <typename Key, typename Allocator, size_t size_bits>
struct IsFixedRangeTable<FixedHashSet<Key, Allocator, size_bits>> : std::true_type
{
};

/// Set counterpart of `PartitionedFixedHashMap`, for a caller that only records which keys exist.
/// The cell holds nothing but the presence flag, so routing uses the cache line of that narrower cell.
template <typename Key, size_t size_bits = sizeof(Key) * 8, size_t BITS_FOR_BUCKET = DEFAULT_BITS_FOR_BUCKET>
using PartitionedFixedHashSet = TwoLevelHashTable<
    Key,
    FixedHashTableCell<Key>,
    TrivialHash,
    TwoLevelHashTableGrower<>,
    HashTableAllocator,
    FixedHashSet<Key, HashTableAllocator, size_bits>,
    BITS_FOR_BUCKET,
    std::conditional_t<BITS_FOR_BUCKET == 0, void, FixedRangeBucketHash<sizeof(FixedHashTableCell<Key>)>>>;
