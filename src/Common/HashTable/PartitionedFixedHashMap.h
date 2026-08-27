#pragma once

#include <type_traits>
#include <Common/CacheLine.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/TwoLevelHashTable.h>


/// A `FixedHashMap` places by the key itself, so routing on the high bits of that would put a dense
/// range in one bucket. Hashes the cache line the cell starts on, so two keys whose cells start on
/// the same line cannot sit under two locks. `key >> log2(line/sizeof(Cell))` only matches that
/// when `sizeof(Cell)` divides the line; `MapsOne` cells are 12 bytes (`bool` plus 8-byte `RowRef`).
template <size_t cell_size>
struct FixedRangeBucketHash
{
    template <typename Key>
    size_t ALWAYS_INLINE operator()(Key key) const
    {
        const UInt64 line = (static_cast<UInt64>(key) * cell_size) / DB::CH_CACHE_LINE_SIZE;
        return static_cast<size_t>((line * 0x9E3779B97F4A7C15ULL) >> 32);
    }
};

template <typename Key, typename Mapped, typename Cell, typename Size, typename Allocator, size_t size_bits>
struct IsDirectAddressedTable<FixedHashMap<Key, Mapped, Cell, Size, Allocator, size_bits>> : std::true_type
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
