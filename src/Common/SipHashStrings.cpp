#include <Common/SipHash.h>
#include <Common/SipHashStrings.h>
#include <Common/SipHashStringsImpl.h>
#include <base/unaligned.h>

#include <algorithm>
#include <cstring>

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

bool sipHashManyRowsIsVectorized()
{
    return true;
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

bool sipHashManyRowsIsVectorized()
{
    return false;
}

#endif

namespace
{

/// The functions below hash at most this many rows at a time, from buffers on the stack.
constexpr size_t stack_rows = 256;

/// `sipHash64` and `sipHash128` of the same bytes run the same rounds, and `sipHash64` is the XOR of the two halves of the
/// 128-bit hash.
void fold(const UInt128 * hashes128, size_t num_rows, UInt64 * hashes)
{
    for (size_t i = 0; i < num_rows; ++i)
        hashes[i] = hashes128[i].items[0] ^ hashes128[i].items[1];
}

void storeLittleEndian(char * pos, UInt64 hash)
{
    unalignedStoreLittleEndian<UInt64>(pos, hash);
}

void storeLittleEndian(char * pos, UInt128 hash)
{
    unalignedStoreLittleEndian<UInt64>(pos, hash.items[UInt128::_impl::little(0)]);
    unalignedStoreLittleEndian<UInt64>(pos + 8, hash.items[UInt128::_impl::little(1)]);
}

/// Sets `hashes[i]` to the hash of `hashes[i]` and `rhs(i)`.
template <typename T, typename Rhs>
void combine(T * hashes, Rhs && rhs, size_t num_rows)
{
    constexpr size_t pair_size = 2 * sizeof(T);
    /// 8 bytes before the first pair and after the last one, as `sipHash128PaddedFixedStrings` requires.
    char pairs[8 + stack_rows * pair_size + 8];
    memset(pairs, 0, 8);
    [[maybe_unused]] UInt128 hashes128[std::is_same_v<T, UInt64> ? stack_rows : 1];
    for (size_t begin = 0; begin < num_rows; begin += stack_rows)
    {
        const size_t rows = std::min(num_rows - begin, stack_rows);
        for (size_t i = 0; i < rows; ++i)
        {
            storeLittleEndian(pairs + 8 + i * pair_size, hashes[begin + i]);
            storeLittleEndian(pairs + 8 + i * pair_size + sizeof(T), rhs(begin + i));
        }
        memset(pairs + 8 + rows * pair_size, 0, 8);
        if constexpr (std::is_same_v<T, UInt128>)
        {
            sipHash128PaddedFixedStrings(pairs + 8, pair_size, rows, hashes + begin);
        }
        else
        {
            sipHash128PaddedFixedStrings(pairs + 8, pair_size, rows, hashes128);
            fold(hashes128, rows, hashes + begin);
        }
    }
}

}

void sipHash64PaddedStrings(const char * chars, const UInt64 * offsets, size_t num_rows, UInt64 * hashes)
{
    UInt128 hashes128[stack_rows];
    for (size_t begin = 0; begin < num_rows; begin += stack_rows)
    {
        const size_t rows = std::min(num_rows - begin, stack_rows);
        sipHash128PaddedStrings(chars, offsets + begin, rows, hashes128);
        fold(hashes128, rows, hashes + begin);
    }
}

void sipHash64PaddedFixedStrings(const char * chars, size_t n, size_t num_rows, UInt64 * hashes)
{
    UInt128 hashes128[stack_rows];
    for (size_t begin = 0; begin < num_rows; begin += stack_rows)
    {
        const size_t rows = std::min(num_rows - begin, stack_rows);
        sipHash128PaddedFixedStrings(chars + begin * n, n, rows, hashes128);
        fold(hashes128, rows, hashes + begin);
    }
}

void sipHash128Combine(UInt128 * hashes, const UInt128 * rhs, size_t num_rows)
{
    combine(hashes, [rhs](size_t i) { return rhs[i]; }, num_rows);
}

void sipHash128CombineWith(UInt128 * hashes, UInt128 rhs, size_t num_rows)
{
    combine(hashes, [rhs](size_t) { return rhs; }, num_rows);
}

void sipHash64Combine(UInt64 * hashes, const UInt64 * rhs, size_t num_rows)
{
    combine(hashes, [rhs](size_t i) { return rhs[i]; }, num_rows);
}

void sipHash64CombineWith(UInt64 * hashes, UInt64 rhs, size_t num_rows)
{
    combine(hashes, [rhs](size_t) { return rhs; }, num_rows);
}
