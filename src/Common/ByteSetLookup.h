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
/// is (or is not) in the set. Building a set is cheap and works in constant expressions, so it fits
/// both character classes fixed in the code (declare them `constexpr`, then the dispatch on the
/// set's properties below folds away) and delimiter sets coming from user input.
///
/// The vectorized search classifies 16 bytes at once with two nibble lookups (`pshufb` / `tbl`):
///
/// - Every high nibble that occurs in the set gets its own bit (`high_nibble_bit`).
/// - `high_nibble_table[high]` is that bit, or 0 if no member has this high nibble.
/// - `low_nibble_table[low]` is the union of the bits of all high nibbles that are
///   combined with this low nibble in some member.
/// - A byte is in the set iff the two lookups share a bit:
///   `low_nibble_table[low] & high_nibble_table[high] != 0`.
///
/// The bits fit in one byte, so at most 8 distinct high nibbles are supported. That is enough
/// for any set of ASCII characters; a set that needs more is searched by the scalar loop only.
class ByteSetLookup
{
public:
    static constexpr size_t BLOCK_SIZE = 16;

    constexpr ByteSetLookup() = default;

    /// The set of all bytes for which `predicate` returns true.
    template <typename Predicate>
    static constexpr ByteSetLookup fromPredicate(Predicate && predicate)
    {
        ByteSetLookup set;
        for (int c = 0; c < 256; ++c)
            if (predicate(static_cast<char>(c)))
                set.add(static_cast<char>(c));
        return set;
    }

    constexpr void add(char c)
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

    ALWAYS_INLINE constexpr bool contains(char c) const
    {
        return table[static_cast<UInt8>(c)];
    }

    /// Returns the first byte in [begin, end) whose membership in the set equals `positive`, or `end`.
    template <bool positive>
    ALWAYS_INLINE const char * find(const char * begin, const char * end) const
    {
        const char * pos = begin;

        if (end - pos >= SCALAR_PREFIX)
        {
            /// Most tokens are short, so the first bytes are checked one by one before paying
            /// for a vector iteration that would mostly look past the token. The constant trip
            /// count lets the compiler unroll this loop into a straight sequence of table lookups.
            for (ptrdiff_t i = 0; i < SCALAR_PREFIX; ++i)
            {
                if (contains(pos[i]) == positive)
                    return pos + i;
            }
            pos += SCALAR_PREFIX;

#if defined(__SSSE3__) || defined(__aarch64__)
            if (vectorized)
                pos = findVectorized<positive>(pos, end);
#endif
        }

        for (; pos < end; ++pos)
        {
            if (contains(*pos) == positive)
                return pos;
        }

        return end;
    }

    /// Classifies the `BLOCK_SIZE` bytes at `pos`: bit `i` of the result is set iff byte `i` is in the set.
    /// All `BLOCK_SIZE` bytes must be readable.
    ALWAYS_INLINE UInt32 matchBlock(const char * pos) const
    {
#if defined(__SSSE3__) || defined(__aarch64__)
        if (vectorized)
        {
            UInt8x16 bytes;
            memcpy(&bytes, pos, BLOCK_SIZE);
            return blockMask(classify(bytes));
        }
#endif
        UInt32 mask = 0;
        for (size_t i = 0; i < BLOCK_SIZE; ++i)
            mask |= static_cast<UInt32>(contains(pos[i])) << i;
        return mask;
    }

private:
    static constexpr ptrdiff_t SCALAR_PREFIX = 16;

#if defined(__SSSE3__) || defined(__aarch64__)
    /// GCC/Clang vector extensions lower each lane operation to one SSE/NEON instruction;
    /// only the byte permutation has no portable spelling.
    using UInt8x16 = UInt8 __attribute__((vector_size(16)));
    using Int8x16 = Int8 __attribute__((vector_size(16)));
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

    /// 0xFF in the lanes of the bytes that are in the set, 0x00 in the others.
    ALWAYS_INLINE Int8x16 classify(UInt8x16 bytes) const
    {
        const auto low_table = std::bit_cast<UInt8x16>(low_nibble_table);
        const auto high_table = std::bit_cast<UInt8x16>(high_nibble_table);

        UInt8x16 low = lookupBytes(low_table, bytes & 0x0F);
        UInt8x16 high = lookupBytes(high_table, bytes >> 4);
        return (low & high) != UInt8x16{};
    }

    /// Compresses a 0x00/0xFF lane vector into a bit per lane.
    static ALWAYS_INLINE UInt32 blockMask(Int8x16 lanes)
    {
#if defined(__SSSE3__)
        return static_cast<UInt32>(_mm_movemask_epi8(std::bit_cast<__m128i>(lanes)));
#else
        /// Keep one distinct bit per lane within each 8-lane half, then sum the halves pairwise.
        const uint8x16_t bit_per_lane = {1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128};
        uint8x16_t bits = vandq_u8(std::bit_cast<uint8x16_t>(lanes), bit_per_lane);
        uint64x2_t halves = vpaddlq_u32(vpaddlq_u16(vpaddlq_u8(bits)));
        return static_cast<UInt32>(vgetq_lane_u64(halves, 0) | (vgetq_lane_u64(halves, 1) << 8));
#endif
    }

    /// Scans whole blocks. Returns the position of the first byte matching the search,
    /// or the position from which fewer than `BLOCK_SIZE` bytes remain.
    template <bool positive>
    const char * findVectorized(const char * pos, const char * end) const
    {
        for (; end - pos >= static_cast<ptrdiff_t>(BLOCK_SIZE); pos += BLOCK_SIZE)
        {
            UInt8x16 bytes;
            memcpy(&bytes, pos, BLOCK_SIZE);

            auto match = classify(bytes);
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
