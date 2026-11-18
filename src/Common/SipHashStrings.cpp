#include <Common/SipHash.h>
#include <Common/SipHashStrings.h>
#include <Common/SipHashStringsImpl.h>

#include <algorithm>

#if (defined(__aarch64__) && defined(__ARM_NEON)) || defined(__AVX2__)

namespace
{

using SipHashStrings::chunk_rows;
using SipHashStrings::group_size;

/// The `rows` argument of `hashRows` for hashing all the rows of a chunk: 0, 1, ..., `chunk_rows - 1`.
constexpr auto all_rows = []
{
    std::array<UInt32, chunk_rows> rows{};
    for (size_t r = 0; r < chunk_rows; ++r)
        rows[r] = static_cast<UInt32>(r);
    return rows;
}();

/// The hashes of at most `chunk_rows` rows, `offsets` as in `SipHashStrings::hashRows`.
void hashChunk(const char * chars, const UInt64 * offsets, size_t num_rows, UInt128 * hashes)
{
    uint8_t buckets[chunk_rows + 32];
    SipHashStrings::lengthBuckets(offsets, num_rows, buckets);
    SipHashStrings::hashRows(chars, offsets, all_rows.data(), buckets, num_rows, hashes);
}

}

void sipHash128PaddedStrings(const char * chars, const UInt64 * offsets, size_t num_rows, UInt128 * hashes)
{
    for (size_t begin = 0; begin < num_rows; begin += chunk_rows)
        hashChunk(chars, offsets + begin - 1, std::min(num_rows - begin, chunk_rows), hashes + begin);
}

void sipHash128PaddedFixedStrings(const char * chars, size_t n, size_t num_rows, UInt128 * hashes)
{
    size_t r = 0;
    for (; r + group_size <= num_rows; r += group_size)
    {
        const char * group_chars = chars + r * n;
        UInt128 * group_hashes = hashes + r;
        SipHashStrings::hashGroup<0>(
            [&](size_t i) ALWAYS_INLINE { return std::pair{group_chars + i * n, n}; },
            [&](size_t i) ALWAYS_INLINE { return group_hashes + i; });
    }
    for (; r < num_rows; ++r)
        hashes[r] = sipHash128Padded(chars + r * n, n);
}

void sipHash128PaddedStringRows(const char * chars, const UInt64 * offsets, const UInt32 * rows, size_t num_rows, UInt128 * hashes)
{
    for (size_t begin = 0; begin < num_rows; begin += chunk_rows)
    {
        const size_t count = std::min(num_rows - begin, chunk_rows);
        uint8_t buckets[chunk_rows + 32];
        for (size_t i = 0; i < count; ++i)
        {
            const ssize_t r = rows[begin + i];
            buckets[i] = static_cast<uint8_t>(std::min<UInt64>((offsets[r] - offsets[r - 1]) / 8, SipHashStrings::exact_buckets));
        }
        /// Row `r` of `hashRows` is [offsets[r - 1], offsets[r]).
        SipHashStrings::hashRows(chars, offsets - 1, rows + begin, buckets, count, hashes);
    }
}

void sipHash128PaddedFixedStringRows(const char * chars, size_t n, const UInt32 * rows, size_t num_rows, UInt128 * hashes)
{
    size_t j = 0;
    for (; j + group_size <= num_rows; j += group_size)
    {
        const UInt32 * group_rows = rows + j;
        SipHashStrings::hashGroup<0>(
            [&](size_t i) ALWAYS_INLINE { return std::pair{chars + group_rows[i] * n, n}; },
            [&](size_t i) ALWAYS_INLINE { return hashes + group_rows[i]; });
    }
    for (; j < num_rows; ++j)
        hashes[rows[j]] = sipHash128Padded(chars + rows[j] * n, n);
}

#else

void sipHash128PaddedStrings(const char * chars, const UInt64 * offsets, size_t num_rows, UInt128 * hashes)
{
    for (size_t i = 0; i < num_rows; ++i)
        hashes[i] = sipHash128Padded(chars + offsets[ssize_t(i) - 1], offsets[i] - offsets[ssize_t(i) - 1]);
}

void sipHash128PaddedFixedStrings(const char * chars, size_t n, size_t num_rows, UInt128 * hashes)
{
    for (size_t i = 0; i < num_rows; ++i)
        hashes[i] = sipHash128Padded(chars + i * n, n);
}

void sipHash128PaddedStringRows(const char * chars, const UInt64 * offsets, const UInt32 * rows, size_t num_rows, UInt128 * hashes)
{
    for (size_t i = 0; i < num_rows; ++i)
    {
        const ssize_t r = rows[i];
        hashes[r] = sipHash128Padded(chars + offsets[r - 1], offsets[r] - offsets[r - 1]);
    }
}

void sipHash128PaddedFixedStringRows(const char * chars, size_t n, const UInt32 * rows, size_t num_rows, UInt128 * hashes)
{
    for (size_t i = 0; i < num_rows; ++i)
        hashes[rows[i]] = sipHash128Padded(chars + rows[i] * n, n);
}

#endif
