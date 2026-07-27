#pragma once

#include <bit>
#include <type_traits>
#include <Common/CacheLine.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/TwoLevelHashTable.h>


/// A `FixedHashMap` places by the key itself, so routing on the high bits of that would put a dense
/// range in one bucket. Hashes the key's cache line rather than the key, which also keeps two keys
/// sharing a line out of two different locks.
template <UInt32 block_shift>
struct FixedRangeBucketHash
{
    template <typename Key>
    size_t ALWAYS_INLINE operator()(Key key) const
    {
        const UInt64 block = static_cast<UInt64>(key) >> block_shift;
        return static_cast<size_t>((block * 0x9E3779B97F4A7C15ULL) >> 32);
    }
};

template <typename Cell>
constexpr UInt32 fixedRangeBlockShift()
{
    constexpr size_t cells_per_line = std::bit_floor(std::max<size_t>(1, DB::CH_CACHE_LINE_SIZE / sizeof(Cell)));
    return static_cast<UInt32>(std::countr_zero(cells_per_line));
}

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
    std::conditional_t<bits_for_bucket == 0, void, FixedRangeBucketHash<fixedRangeBlockShift<FixedHashMapCell<Key, Mapped>>()>>>;
