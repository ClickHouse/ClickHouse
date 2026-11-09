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

#endif
