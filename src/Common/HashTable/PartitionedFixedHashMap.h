#pragma once

#include <type_traits>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/TwoLevelHashTable.h>


/// Only with the atomic size counter: several threads insert into the one buffer at the same time.
template <typename Key, typename Mapped, typename Cell, typename Allocator, size_t size_bits>
struct IsFixedRangeTable<FixedHashMap<Key, Mapped, Cell, FixedHashTableStoredSize<Cell>, Allocator, size_bits>> : std::true_type
{
};

/// A `FixedHashMap` whose keys are partitioned into buckets for a caller that fills it from several
/// threads. The buckets share the one flat table; they only decide which lock a key is inserted
/// under, so the cells, their offsets and iteration are those of the plain `FixedHashMap`. Keys are
/// routed by the cache line of their cell, not by the key's high bits, so a dense key range spreads
/// over the buckets. With `bits_for_bucket = 0` it is the plain map with no routing at all.
template <typename Key, typename Mapped, size_t size_bits = sizeof(Key) * 8, size_t bits_for_bucket = DEFAULT_BITS_FOR_BUCKET>
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
