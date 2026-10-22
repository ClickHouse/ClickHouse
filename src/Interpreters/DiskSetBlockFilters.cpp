#include <Interpreters/DiskSetBlockFilters.h>

#include <algorithm>
#include <cmath>
#include <numbers>

namespace DB
{

namespace
{

constexpr double max_bits_per_key = 8;
constexpr double min_bits_per_key = 1;

/// The filters reserve their memory for 1% more keys than expected, which covers the error of
/// `DistinctKeyCounter`, so that their arrays are not reallocated while the set writes its blocks.
constexpr double reserved_keys_ratio = 1.01;

}

template <typename Key>
std::optional<typename DiskSetBlockFilters<Key>::Layout>
DiskSetBlockFilters<Key>::computeLayout(size_t expected_keys, size_t min_keys_per_block, size_t max_bytes)
{
    chassert(expected_keys && max_bytes);
    const double reserved_keys = reserved_keys_ratio * static_cast<double>(expected_keys);

    /// The ranges take a begin for each block and one more for the end of the last block, and the bits take
    /// the rest of the memory. The ranges can take all of it, which leaves no bits.
    const size_t reserved_ranges = static_cast<size_t>(reserved_keys) / min_keys_per_block + 2;
    const double max_bits = 8.0 * (static_cast<double>(max_bytes) - static_cast<double>(reserved_ranges * sizeof(size_t)));
    const double bits_per_key = std::min(max_bits_per_key, max_bits / reserved_keys);
    if (bits_per_key < min_bits_per_key)
        return {};

    /// A Bloom filter lets the fewest keys pass when each of its keys sets about `bits_per_key * ln 2` bits,
    /// so that about half of its bits are set.
    const auto positions_per_key
        = std::clamp<size_t>(static_cast<size_t>(std::lround(bits_per_key * std::numbers::ln2)), 1, max_positions_per_key);

    const auto bits_per_1024_keys = static_cast<size_t>(bits_per_key * 1024);
    const auto reserved_words = static_cast<size_t>(std::ceil(reserved_keys * static_cast<double>(bits_per_1024_keys) / 1024 / 64));
    return Layout{bits_per_1024_keys, positions_per_key, reserved_words, reserved_ranges};
}

template <typename Key>
std::optional<DiskSetBlockFilters<Key>> DiskSetBlockFilters<Key>::create(size_t expected_keys, size_t min_keys_per_block, size_t max_bytes)
{
    if (const auto layout = computeLayout(expected_keys, min_keys_per_block, max_bytes))
        return DiskSetBlockFilters(*layout);
    return {};
}

template <typename Key>
size_t DiskSetBlockFilters<Key>::estimateBytes(size_t expected_keys, size_t min_keys_per_block, size_t max_bytes)
{
    const auto layout = computeLayout(expected_keys, min_keys_per_block, max_bytes);
    return layout ? layout->reserved_words * sizeof(UInt64) + layout->reserved_ranges * sizeof(size_t) : 0;
}

template <typename Key>
DiskSetBlockFilters<Key>::DiskSetBlockFilters(const Layout & layout)
    : bits_per_1024_keys(layout.bits_per_1024_keys)
    , positions_per_key(layout.positions_per_key)
{
    words.reserve(layout.reserved_words);
    range_begins.reserve(layout.reserved_ranges);
}

template <typename Key>
void DiskSetBlockFilters<Key>::addBlock(std::span<const Key> block_keys)
{
    const size_t range_begin = range_begins.back();
    added_keys += block_keys.size();
    const size_t range_end = added_keys * bits_per_1024_keys / 1024;
    range_begins.push_back(range_end);

    /// With at least 1 bit per key, the range of each block holds at least one bit for each key of the block,
    /// so no range is empty.
    const size_t range_bits = range_end - range_begin;
    chassert(range_bits >= block_keys.size());

    words.resize((range_end + 63) / 64);
    for (const Key & key : block_keys)
    {
        const UInt64 hash = hashKey(key);
        for (size_t i = 0; i < positions_per_key; ++i)
        {
            const size_t bit = range_begin + getBitPosition(hash, i, range_bits);
            words[bit / 64] |= UInt64(1) << (bit % 64);
        }
    }
}

template <typename Key>
size_t DiskSetBlockFilters<Key>::allocatedBytes() const
{
    return words.capacity() * sizeof(UInt64) + range_begins.capacity() * sizeof(size_t);
}

template class DiskSetBlockFilters<UInt8>;
template class DiskSetBlockFilters<UInt16>;
template class DiskSetBlockFilters<UInt32>;
template class DiskSetBlockFilters<UInt64>;
template class DiskSetBlockFilters<UInt128>;
template class DiskSetBlockFilters<UInt256>;

}
