#pragma once

#include <Common/BitHelpers.h>
#include <Common/HashTable/Hash.h>
#include <Common/HyperLogLogCounter.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/types.h>

#include <optional>
#include <span>

namespace DB
{

/// Bloom filters of the blocks of a set on disk, which the set keeps in memory. A lookup checks the filter of
/// the only block that can hold its key, and reads the block only when the filter lets the key pass.
///
/// The filters share one array of bits. Each block takes a range of it in proportion to its keys, and each
/// key sets a few bits in the range of its block. A single filter for all keys would take as much memory, but
/// a key would set bits anywhere in it, leading to writes at random places all over the array. For a large
/// set, the array is far larger than the cache, so each such write would miss the cache and the TLB, and
/// these misses would slow down writing the set, because the set fills the filters on the single thread that
/// writes its blocks. The range of a block is about as small as the block, so it stays in the cache while the
/// set writes the block. Lookups check the ranges in the order of the blocks, as they read the blocks.
template <typename Key>
class DiskSetBlockFilters
{
public:
    /// Counts the distinct keys of a set as they arrive, so that the bloom filters can be sized before the set
    /// writes its blocks. With 2^20 buckets, the estimate is within about 0.1% and the counter takes only 768 KiB.
    class DistinctKeyCounter
    {
    public:
        void add(std::span<const Key> keys)
        {
            for (const Key & key : keys)
                counter.insert(hashKey(key));
        }

        size_t getCount() const { return counter.size(); }

    private:
        HyperLogLogCounter<20, UInt64, TrivialHash, UInt64> counter;
    };

    /// Creates the filters for about `expected_keys` keys. They take 8 bits per key, which lets about 3% of
    /// the keys that the set does not have pass. When that does not fit in `max_bytes` bytes with the ranges
    /// of the blocks, they take fewer bits per key, so that they fit: with 2 bits per key, about 40% of these
    /// keys pass, and with 1 bit per key, about 63%. Every block but the last holds at least
    /// `min_keys_per_block` keys, which bounds the number of ranges. Returns nothing when fewer than 1 bit
    /// per key fits, because such filters let so many keys pass that they do not pay for filling them.
    static std::optional<DiskSetBlockFilters> create(size_t expected_keys, size_t min_keys_per_block, size_t max_bytes);

    /// Returns the memory that `create` reserves with the same arguments, or 0 when it creates no filters.
    static size_t estimateBytes(size_t expected_keys, size_t min_keys_per_block, size_t max_bytes);

    /// Returns the bits of the filters per key: 8, or fewer when they fit a smaller budget.
    double getBitsPerKey() const { return static_cast<double>(bits_per_1024_keys) / 1024; }

    /// Adds the filter of the next block of the set, which holds `block_keys`.
    void addBlock(std::span<const Key> block_keys);

    /// Returns false when `block` cannot hold `key`, and true when it may.
    bool mayContain(size_t block, const Key & key) const
    {
        const size_t range_begin = range_begins[block];
        const size_t range_bits = range_begins[block + 1] - range_begin;
        const UInt64 hash = hashKey(key);
        for (size_t i = 0; i < positions_per_key; ++i)
        {
            const size_t bit = range_begin + getBitPosition(hash, i, range_bits);
            if (!((words[bit / 64] >> (bit % 64)) & 1))
                return false;
        }
        return true;
    }

    /// Returns the memory of the bits and of the ranges of the blocks.
    size_t allocatedBytes() const;

private:
    /// A key sets the bits at most at this many positions in the range of its block. Each position costs a
    /// memory access when a lookup checks a key that the set has, while more positions let only slightly
    /// fewer other keys pass: with 8 bits per key, 3 positions let about 3.1% of them pass, and 6 positions,
    /// the most effective number, about 2.2%.
    static constexpr size_t max_positions_per_key = 3;

    /// Each position takes this many bits of the hash of a key, and the positions of a key fit in its 64
    /// bits. A range holds far fewer bits, because a block holds at most a few thousand keys.
    static constexpr size_t hash_bits_per_position = 21;
    static_assert(max_positions_per_key * hash_bits_per_position <= 64);

    /// The bits and the positions per key of the filters, and the words of bits and the ranges that they
    /// reserve, which `computeLayout` derives from the arguments of `create`.
    struct Layout
    {
        size_t bits_per_1024_keys;
        size_t positions_per_key;
        size_t reserved_words;
        size_t reserved_ranges;
    };

    /// Returns the layout of the filters that `create` creates, or nothing when it creates none.
    static std::optional<Layout> computeLayout(size_t expected_keys, size_t min_keys_per_block, size_t max_bytes);

    explicit DiskSetBlockFilters(const Layout & layout);

    /// Returns position `i` of the bits of a key with `hash` in a range of `range_bits` bits.
    static size_t getBitPosition(UInt64 hash, size_t i, size_t range_bits)
    {
        const UInt64 position_hash = (hash >> (i * hash_bits_per_position)) & maskLowBits<UInt64>(hash_bits_per_position);
        return (position_hash * range_bits) >> hash_bits_per_position;
    }

    /// Returns a hash of all bits of `key`.
    static UInt64 hashKey(const Key & key)
    {
        if constexpr (sizeof(Key) <= sizeof(UInt64))
            return intHash64(key);
        else if constexpr (sizeof(Key) == sizeof(UInt128))
            return UInt128Hash{}(key);
        else
            return UInt256Hash{}(key);
    }

    /// The bits per key, counted per 1024 keys, so that the ranges of the blocks are computed exactly in
    /// integers.
    size_t bits_per_1024_keys;

    /// Each key sets the bits at this many positions, the number that lets the fewest keys that the set does
    /// not have pass at the bits per key, but at most `max_positions_per_key`.
    size_t positions_per_key;

    /// The number of keys of the blocks added so far. The range of a block ends at the bit that the keys of
    /// the blocks up to it take in total, so neighboring blocks can share a word, and the bits add up to
    /// exactly the bits per key.
    size_t added_keys = 0;
    VectorWithMemoryTracking<UInt64> words;

    /// Block `b` takes the bits from `range_begins[b]` to `range_begins[b + 1]`.
    VectorWithMemoryTracking<size_t> range_begins{0};
};

}
