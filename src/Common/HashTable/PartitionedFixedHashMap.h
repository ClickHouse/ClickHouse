#pragma once

#include <type_traits>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/TwoLevelHashTable.h>


template <typename Key, typename Mapped, typename Cell, typename Size, typename Allocator, size_t size_bits>
struct IsFixedRangeTable<FixedHashMap<Key, Mapped, Cell, Size, Allocator, size_bits>> : std::true_type
{
};

template <typename Key, typename Mapped, size_t size_bits = sizeof(Key) * 8, Int32 bits_for_bucket = DEFAULT_BITS_FOR_BUCKET>
using PartitionedFixedHashMap = TwoLevelHashTable<
    Key,
    FixedHashMapCell<Key, Mapped>,
    TrivialHash,
    TwoLevelHashTableGrower<>,
    HashTableAllocator,
    FixedHashMap<
        Key,
        Mapped,
        FixedHashMapCell<Key, Mapped>,
        FixedHashTableStoredSize<FixedHashMapCell<Key, Mapped>>,
        HashTableAllocator,
        size_bits>,
    bits_for_bucket,
    std::conditional_t<bits_for_bucket == 0, void, FixedRangeBucketHash<sizeof(FixedHashMapCell<Key, Mapped>)>>>;
