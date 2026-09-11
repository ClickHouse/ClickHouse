#pragma once

#include <base/defines.h>
#include <base/types.h>

#include <bit>
#include <cstddef>
#include <cstring>

#if defined(__SSSE3__)
#    include <tmmintrin.h>
#endif
#if defined(__aarch64__)
#    include <arm_neon.h>
#endif

namespace DB
{

/// A set of bytes with an O(1) membership test and a vectorized search for the first byte that
/// is (or is not) in the set. Building a set is cheap, so it fits delimiter sets that come from
/// user input as well as character classes fixed at compile time.
///
/// The vectorized search can only look up 16-entry tables and therefore classifies a byte by
/// its two nibbles:
///
/// - Every high nibble that occurs in the set gets its own bit (`high_nibble_bit`).
/// - `high_nibble_table[high]` is that bit, or 0 if no member has this high nibble.
/// - `low_nibble_table[low]` is the union of the bits of all high nibbles that are
///   combined with this low nibble in some member.
/// - A byte is in the set iff the two lookups share a bit:
///   `low_nibble_table[low] & high_nibble_table[high] != 0`.
///
/// The bits fit in one byte, so at most 8 distinct high nibbles are supported. That is
/// enough for any set of ASCII characters; a set that needs more is searched by the
/// scalar loop only.
class ByteSetLookup
{
public:
    void add(char c)
    {
        auto byte = static_cast<UInt8>(c);
        if (table[byte])
            return;

        table[byte] = true;

        UInt8 high = byte >> 4;
        UInt8 low = byte & 0x0F;

        if (!high_nibble_bit[high])
        {
            if (num_high_nibbles == 8)
            {
                vectorized = false;
                return;
            }

            high_nibble_bit[high] = static_cast<UInt8>(1u << num_high_nibbles);
            ++num_high_nibbles;
        }

        low_nibble_table[low] |= high_nibble_bit[high];
        high_nibble_table[high] = high_nibble_bit[high];
    }

    ALWAYS_INLINE bool contains(char c) const
    {
        return table[static_cast<UInt8>(c)];
    }

    /// Returns the first byte in [begin, end) whose membership in the set equals `positive`, or `end`.
    template <bool positive>
    const char * find(const char * begin, const char * end) const
    {
        const char * pos = begin;

        /// Most tokens are short, so the first bytes are checked one by one
        /// before paying for a vector iteration that would mostly look past the token.
        const char * scalar_end = end - pos > SCALAR_PREFIX ? pos + SCALAR_PREFIX : end;
        for (; pos < scalar_end; ++pos)
        {
            if (contains(*pos) == positive)
                return pos;
        }

#if defined(__SSSE3__) || defined(__aarch64__)
        if (vectorized)
            pos = findVectorized<positive>(pos, end);
#endif

        for (; pos < end; ++pos)
        {
            if (contains(*pos) == positive)
                return pos;
        }

        return end;
    }

private:
    static constexpr ptrdiff_t SCALAR_PREFIX = 16;
    static constexpr ptrdiff_t VECTOR_SIZE = 16;

#if defined(__SSSE3__) || defined(__aarch64__)
    /// GCC/Clang vector extensions lower each lane operation to one SSE/NEON instruction.
    /// Only the byte permutation has no portable spelling.
    using UInt8x16 = UInt8 __attribute__((vector_size(16)));
    using UInt64x2 = UInt64 __attribute__((vector_size(16)));

    /// Returns a vector with `table[indexes[i]]` in lane `i`; every index must be less than 16.
    static ALWAYS_INLINE UInt8x16 lookupBytes(UInt8x16 table, UInt8x16 indexes)
    {
#if defined(__SSSE3__)
        return std::bit_cast<UInt8x16>(_mm_shuffle_epi8(std::bit_cast<__m128i>(table), std::bit_cast<__m128i>(indexes)));
#else
        return std::bit_cast<UInt8x16>(vqtbl1q_u8(std::bit_cast<uint8x16_t>(table), std::bit_cast<uint8x16_t>(indexes)));
#endif
    }

    /// Scans whole 16-byte blocks. Returns the position of the first byte matching the search,
    /// or the position from which fewer than 16 bytes remain.
    template <bool positive>
    const char * findVectorized(const char * pos, const char * end) const
    {
        const auto low_table = std::bit_cast<UInt8x16>(low_nibble_table);
        const auto high_table = std::bit_cast<UInt8x16>(high_nibble_table);

        for (; end - pos >= VECTOR_SIZE; pos += VECTOR_SIZE)
        {
            UInt8x16 bytes;
            memcpy(&bytes, pos, VECTOR_SIZE);

            UInt8x16 low = lookupBytes(low_table, bytes & 0x0F);
            UInt8x16 high = lookupBytes(high_table, bytes >> 4);

            /// 0xFF in the lanes of the bytes that match the search, 0x00 in the others.
            auto match = (low & high) != UInt8x16{};
            if constexpr (!positive)
                match = ~match;

            /// The first matching byte is the lowest non-zero byte, little-endian.
            const auto halves = std::bit_cast<UInt64x2>(match);
            if (halves[0])
                return pos + std::countr_zero(halves[0]) / 8;
            if (halves[1])
                return pos + 8 + std::countr_zero(halves[1]) / 8;
        }

        return pos;
    }
#endif

    bool table[256]{};
    UInt8 low_nibble_table[16]{};
    UInt8 high_nibble_table[16]{};
    UInt8 high_nibble_bit[16]{};
    size_t num_high_nibbles = 0;
    bool vectorized = true;
};

}
