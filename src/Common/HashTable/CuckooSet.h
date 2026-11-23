#pragma once

#include <Common/HashTable/Hash.h>
#include <Common/PODArray.h>
#include <base/sanitizer_defs.h>
#include <base/types.h>

#include <utility>


namespace DB
{

/// A cuckoo hash table of a fixed set of keys, looked up without data-dependent branches. Each key is in one of its two
/// slots, and every slot without a key holds a copy of some key of the set, so a value is in the set if and only if it
/// equals one of its two slots. Instantiated for `UInt32`, `UInt64` and `UInt128`.
template <typename Key>
class CuckooSet
{
public:
    /// The peak memory of building the smallest table for `num_keys` keys, held in an array of `keys_capacity` keys.
    static size_t minPeakBytes(size_t keys_capacity, size_t num_keys);

    /// Builds nothing if the slots would take more than `max_slot_bytes` or a placement more than `memory_budget`.
    bool tryBuild(const PODArray<Key> & keys, size_t max_slot_bytes, size_t memory_budget, size_t & attempts);

    [[nodiscard]] bool empty() const { return slots.empty(); }
    [[nodiscard]] size_t allocatedBytes() const { return slots.allocated_bytes(); }

    /// Sets `res[i]` to whether `get_key(i)` is in the set, for each of the `rows` rows.
    template <typename GetKey>
    void findEach(size_t rows, UInt8 * res, GetKey && get_key) const
    {
        if (mixed_hash)
        {
            for (size_t i = 0; i < rows; ++i)
                res[i] = contains<true>(get_key(i));
        }
        else
        {
            for (size_t i = 0; i < rows; ++i)
                res[i] = contains<false>(get_key(i));
        }
    }

    /// The same for the `rows` keys stored at `values`, which need not be aligned.
    void find(const char * values, size_t rows, UInt8 * res) const requires (sizeof(Key) <= sizeof(UInt64));

    /// The same for keys that mostly repeat the previous row's: only a changed key is looked up.
    void findRepeated(const char * values, size_t rows, UInt8 * res) const;

    /// Whether the key changes every `n >= 2` rows, judged by the three runs after each of four rows spread over the block.
    static bool keysChangeRegularly(const char * values, size_t rows);

private:
    PODArray<Key> slots;
    /// One per 64-bit word of a key (see `places`).
    UInt64 multipliers1[2] = {};
    UInt64 multipliers2[2] = {};
    bool mixed_hash = false;
    size_t shift = 0;
    UInt64 mask = 0;

    template <bool mixed>
    [[nodiscard]] NO_SANITIZE_UNSIGNED_OVERFLOW std::pair<size_t, size_t> places(const Key & key) const
    {
        static constexpr size_t num_words = sizeof(Key) <= sizeof(UInt64) ? 1 : sizeof(Key) / sizeof(UInt64);
        auto word = [&](size_t i) -> UInt64
        {
            if constexpr (sizeof(Key) <= sizeof(UInt64))
                return static_cast<UInt64>(key);
            else
                return key.items[i];
        };

        if constexpr (mixed && sizeof(Key) <= sizeof(UInt64))
        {
            const UInt64 hash = intHashCRC32(static_cast<UInt64>(key));
            return {(hash * multipliers1[0]) >> shift, (hash * multipliers2[0]) >> shift};
        }
        else if constexpr (mixed)
        {
            UInt64 hash = 0;
            for (size_t i = 0; i < num_words; ++i)
                hash += intHash64(word(i) ^ multipliers1[i]);
            return {hash >> shift, hash & mask};
        }
        else
        {
            UInt64 hash1 = 0;
            UInt64 hash2 = 0;
            for (size_t i = 0; i < num_words; ++i)
            {
                hash1 += word(i) * multipliers1[i];
                hash2 += word(i) * multipliers2[i];
            }
            return {hash1 >> shift, hash2 >> shift};
        }
    }

    template <bool mixed>
    [[nodiscard]] UInt8 contains(const Key & key) const
    {
        const Key * data = slots.data();
        const auto [place1, place2] = places<mixed>(key);
        return static_cast<UInt8>(static_cast<UInt8>(data[place1] == key) | static_cast<UInt8>(data[place2] == key));
    }

    template <bool mixed>
    bool tryPlaceKeys(const PODArray<Key> & keys, size_t bits);

    template <bool mixed>
    void findImpl(const char * values, size_t rows, UInt8 * res) const;

    template <bool mixed>
    void findRepeatedImpl(const char * values, size_t rows, UInt8 * res) const;
};

extern template class CuckooSet<UInt32>;
extern template class CuckooSet<UInt64>;
extern template class CuckooSet<UInt128>;

}
