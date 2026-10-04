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

/// A set of bytes (i.e. at most 256 elements) with two operations:
///   - `contains`: an O(1) membership test of a single byte;
///   - `find`: the position of the first byte in a range that is in the set (or, alternatively, the first byte that is not in the set).
///
/// `find` is vectorized: the membership of 16 consecutive bytes is tested at once,
/// with two table lookups per byte (`pshufb` on x86, `tbl` on ARM).
/// Each lookup is indexed by one nibble (half of the byte), because these instructions look up a 16-entry table.
/// Think of all 256 bytes as a 16x16 grid: the high nibble (the upper 4 bits) picks the row, the low nibble (the lower 4 bits) picks the column.
/// Every row that has at least one member is assigned its own bit.
/// `high_nibble_table[row]` holds the bit of the row.
/// `low_nibble_table[column]` holds the bits of all rows that have a member in the column.
/// A byte is in the set iff `high_nibble_table[row] & low_nibble_table[column]` is non-zero, i.e. iff the bit of its row is present in its column.
///
/// A table entry is one byte and holds at most 8 bits, so at most 8 rows may have members.
/// That is enough for any set of ASCII characters, which occupy the rows 0 to 7.
/// A set with members in more than 8 rows is searched by the scalar loop only.
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
        {
            if (predicate(static_cast<char>(c)))
                set.add(static_cast<char>(c));
        }
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

        /// See the class comment: `high` is the row of the byte, `low` is the column.
        /// The rows are assigned their bits in the order of their first appearance, because there are 16 rows,
        /// but a table entry has only 8 bits, so `1 << high` would not fit.
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
            /// The callers mostly search short ranges where the byte is found within the first few positions,
            /// so these are checked one by one before paying for a vector iteration that would mostly look past the found byte.
            /// The constant trip count lets the compiler unroll this loop into a straight sequence of table lookups.
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

    /// Tests the membership of the `BLOCK_SIZE` bytes at `pos`:
    /// bit `i` of the result is set iff byte `i` is in the set.
    /// All `BLOCK_SIZE` bytes must be readable.
    ALWAYS_INLINE UInt32 matchBlock(const char * pos) const
    {
#if defined(__SSSE3__) || defined(__aarch64__)
        if (vectorized)
        {
            UInt8x16 bytes;
            memcpy(&bytes, pos, BLOCK_SIZE);
            return blockMask(containsVector(bytes));
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
    /// GCC/Clang vector extensions lower each lane operation to one SSE/NEON instruction.
    /// Only the byte permutation has no portable spelling.
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

    /// Compresses a 0x00/0xFF lane vector into a bit per lane.
    static ALWAYS_INLINE UInt32 blockMask(Int8x16 lanes)
    {
#if defined(__SSSE3__)
        return static_cast<UInt32>(_mm_movemask_epi8(std::bit_cast<__m128i>(lanes)));
#else
        /// Keep one distinct bit per lane within each 8-lane half, so that summing a half ORs its bits.
        /// Three rounds of `addp` of the vector with itself leave the sums of the halves in lanes 0 and 1, the two bytes of the mask.
        const uint8x16_t bit_per_lane = {1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128};
        uint8x16_t sums = vandq_u8(std::bit_cast<uint8x16_t>(lanes), bit_per_lane);
        sums = vpaddq_u8(sums, sums);
        sums = vpaddq_u8(sums, sums);
        sums = vpaddq_u8(sums, sums);
        return vgetq_lane_u16(vreinterpretq_u16_u8(sums), 0);
#endif
    }

    /// Tests the membership of 16 bytes at once: 0xFF in the lanes of the bytes that are in the set, 0x00 in the others.
    ALWAYS_INLINE Int8x16 containsVector(UInt8x16 bytes) const
    {
        const auto low_table = std::bit_cast<UInt8x16>(low_nibble_table);
        const auto high_table = std::bit_cast<UInt8x16>(high_nibble_table);

        UInt8x16 low = lookupBytes(low_table, bytes & 0x0F);
        UInt8x16 high = lookupBytes(high_table, bytes >> 4);
        return (low & high) != UInt8x16{};
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

            auto match = containsVector(bytes);
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
