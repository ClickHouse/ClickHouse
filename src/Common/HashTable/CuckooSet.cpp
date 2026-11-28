#include <Common/HashTable/CuckooSet.h>

#include <base/unaligned.h>

#include <algorithm>
#include <iterator>


namespace DB
{

namespace
{

constexpr size_t minSlots(size_t num_keys) { return 2 * num_keys + num_keys / 4; }

size_t minBits(size_t num_keys)
{
    size_t bits = 4;
    while ((1ULL << bits) < minSlots(num_keys))
        ++bits;
    return bits;
}

size_t occupancyWords(size_t bits) { return ((1ULL << bits) + 63) / 64; }

/// The peak memory of a placement into `1 << bits` slots: the keys, the slots and their occupancy bits, or, while
/// the slots of the previous placement (none if `previous_bits` is 0) are reallocated, both blocks of slots.
size_t attemptPeakBytes(size_t num_keys, size_t key_size, size_t bits, size_t previous_bits)
{
    const size_t slot_bytes = (1ULL << bits) * key_size;
    const size_t previous_slot_bytes = previous_bits ? (1ULL << previous_bits) * key_size : 0;
    const size_t occupancy_bytes = occupancyWords(bits) * sizeof(UInt64);
    return num_keys * key_size + std::max(previous_slot_bytes + slot_bytes, slot_bytes + occupancy_bytes);
}

NO_SANITIZE_UNSIGNED_OVERFLOW UInt64 splitMix64(UInt64 & state)
{
    UInt64 z = (state += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

}

template <typename Key>
size_t CuckooSet<Key>::minPeakBytes(size_t keys_capacity, size_t num_keys)
{
    return attemptPeakBytes(keys_capacity, sizeof(Key), minBits(num_keys), 0);
}

template <typename Key>
bool CuckooSet<Key>::tryBuild(const PODArray<Key> & keys, size_t max_slot_bytes, size_t memory_budget, size_t & attempts)
{
    if (keys.empty())
        return false;

    size_t bits = minBits(keys.size());
    size_t previous_bits = 0;
    UInt64 random_state = keys.size();
    for (size_t attempt = 0; attempt < 16; ++attempt)
    {
        if (attempt > 0 && attempt % 4 == 0)
            ++bits;
        /// A retry of the same size reuses the slots in place.
        const size_t reallocated_bits = bits > previous_bits ? previous_bits : 0;
        if ((1ULL << bits) * sizeof(Key) > max_slot_bytes || attemptPeakBytes(keys.capacity(), sizeof(Key), bits, reallocated_bits) > memory_budget)
            break;
        for (size_t i = 0; i < std::size(multipliers1); ++i)
        {
            multipliers1[i] = splitMix64(random_state) | 1;
            multipliers2[i] = splitMix64(random_state) | 1;
        }
        ++attempts;
        mixed_hash = attempt > 0;
        if (mixed_hash ? tryPlaceKeys<true>(keys, bits) : tryPlaceKeys<false>(keys, bits))
            return true;
        previous_bits = bits;
    }
    slots.clear();
    return false;
}

template <typename Key>
template <bool mixed>
bool CuckooSet<Key>::tryPlaceKeys(const PODArray<Key> & keys, size_t bits)
{
    shift = 64 - bits;
    mask = (1ULL << bits) - 1;
    const size_t num_slots = 1ULL << bits;
    /// The slots that get no key keep this copy of a key of the set.
    slots.assign(num_slots, keys[0]);
    PODArray<UInt64> occupied(occupancyWords(bits), 0);

    /// The moves would never end if more than two keys had the same two slots.
    static constexpr size_t max_moves = 100;
    static constexpr size_t prefetch_distance = 16;
    const size_t num_keys = keys.size();
    for (size_t i = 0; i < num_keys; ++i)
    {
        if (i + prefetch_distance < num_keys)
            __builtin_prefetch(&slots[places<mixed>(keys[i + prefetch_distance]).first], 1);

        Key key = keys[i];
        size_t current = places<mixed>(key).first;
        for (size_t moves = 0;; ++moves)
        {
            UInt64 & word = occupied[current / 64];
            const UInt64 bit = 1ULL << (current % 64);
            if (!(word & bit))
            {
                word |= bit;
                slots[current] = key;
                break;
            }
            if (moves == max_moves)
                return false;
            std::swap(key, slots[current]);
            const auto [first, second] = places<mixed>(key);
            current = current == first ? second : first;
        }
    }

    return true;
}

template <typename Key>
bool CuckooSet<Key>::keysChangeRegularly(const char * values, size_t rows)
{
    static constexpr size_t num_starts = 4;
    static constexpr size_t runs_per_start = 3;
    static constexpr size_t max_run = 64;
    static constexpr size_t rows_per_start = (runs_per_start + 1) * max_run;
    if (rows < num_starts * rows_per_start)
        return false;
    size_t run = 0;
    for (size_t start = 0; start < num_starts; ++start)
    {
        const size_t begin = start * (rows / num_starts);
        /// The run that contains `begin` may have started before it, so it is not counted.
        size_t last_change = 0;
        size_t runs = 0;
        for (size_t i = begin + 1; i < begin + rows_per_start && runs < runs_per_start; ++i)
        {
            if (unalignedLoad<Key>(values + i * sizeof(Key)) == unalignedLoad<Key>(values + (i - 1) * sizeof(Key)))
                continue;
            if (last_change)
            {
                const size_t length = i - last_change;
                if (length < 2 || (run && length != run))
                    return false;
                run = length;
                ++runs;
            }
            last_change = i;
        }
        if (runs < runs_per_start)
            return false;
    }
    return true;
}

template <typename Key>
void CuckooSet<Key>::findRepeated(const char * values, size_t rows, UInt8 * res) const
{
    if (mixed_hash)
        findRepeatedImpl<true>(values, rows, res);
    else
        findRepeatedImpl<false>(values, rows, res);
}

template <typename Key>
template <bool mixed>
void CuckooSet<Key>::findRepeatedImpl(const char * values, size_t rows, UInt8 * res) const
{
    if (rows == 0)
        return;
    Key previous = unalignedLoad<Key>(values);
    UInt8 found = contains<mixed>(previous);
    res[0] = found;
    for (size_t i = 1; i < rows; ++i)
    {
        const Key key = unalignedLoad<Key>(values + i * sizeof(Key));
        if (key != previous)
        {
            previous = key;
            found = contains<mixed>(key);
        }
        res[i] = found;
    }
}

template <typename Key>
void CuckooSet<Key>::find(const char * values, size_t rows, UInt8 * res) const requires (sizeof(Key) <= sizeof(UInt64))
{
    if (mixed_hash)
        findImpl<true>(values, rows, res);
    else
        findImpl<false>(values, rows, res);
}

template <typename Key>
template <bool mixed>
void CuckooSet<Key>::findImpl(const char * values, size_t rows, UInt8 * res) const
{
    const Key * data = slots.data();
    const UInt64 m1 = multipliers1[0];
    [[maybe_unused]] const UInt64 m2 = multipliers2[0];
    const size_t s = shift;
    [[maybe_unused]] const UInt64 m = mask;
    /// Must match `places`.
    auto places_of = [&](Key v) NO_SANITIZE_UNSIGNED_OVERFLOW -> std::pair<size_t, size_t>
    {
        if constexpr (mixed)
        {
            const UInt64 hash = intHashCRC32(static_cast<UInt64>(v));
            return {(hash * m1) >> s, (hash * m2) >> s};
        }
        else
            return {(static_cast<UInt64>(v) * m1) >> s, (static_cast<UInt64>(v) * m2) >> s};
    };

    size_t i = 0;
    for (; i + 4 <= rows; i += 4)
    {
#if defined(__x86_64__)
        /// On x86 this loop stalls on keys that are not in the cache (e.g. those of a `Memory` table) unless prefetched.
        static constexpr size_t prefetch_distance = 2048 / sizeof(Key);
        if (i + prefetch_distance < rows)
            __builtin_prefetch(values + (i + prefetch_distance) * sizeof(Key));
#endif
        const Key v0 = unalignedLoad<Key>(values + (i + 0) * sizeof(Key));
        const Key v1 = unalignedLoad<Key>(values + (i + 1) * sizeof(Key));
        const Key v2 = unalignedLoad<Key>(values + (i + 2) * sizeof(Key));
        const Key v3 = unalignedLoad<Key>(values + (i + 3) * sizeof(Key));
        const auto [p0, q0] = places_of(v0);
        const auto [p1, q1] = places_of(v1);
        const auto [p2, q2] = places_of(v2);
        const auto [p3, q3] = places_of(v3);
        const Key a0 = data[p0];
        const Key a1 = data[p1];
        const Key a2 = data[p2];
        const Key a3 = data[p3];
        const Key b0 = data[q0];
        const Key b1 = data[q1];
        const Key b2 = data[q2];
        const Key b3 = data[q3];
        res[i + 0] = static_cast<UInt8>((a0 == v0) | (b0 == v0));
        res[i + 1] = static_cast<UInt8>((a1 == v1) | (b1 == v1));
        res[i + 2] = static_cast<UInt8>((a2 == v2) | (b2 == v2));
        res[i + 3] = static_cast<UInt8>((a3 == v3) | (b3 == v3));
    }

    for (; i < rows; ++i)
        res[i] = contains<mixed>(unalignedLoad<Key>(values + i * sizeof(Key)));
}

template class CuckooSet<UInt32>;
template class CuckooSet<UInt64>;
template class CuckooSet<UInt128>;

}
