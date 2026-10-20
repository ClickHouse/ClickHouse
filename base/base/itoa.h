#pragma once

#include <array>
#include <bit>
#include <cstring>
#include <limits>
#include <string_view>
#include <base/defines.h>
#include <base/extended_types.h>
#include <base/unaligned.h>

#include <type_traits>

#if defined(__aarch64__)
#    include <arm_neon.h>
#endif

/// The text of every `UInt8` value, 4 bytes each: the digits from the lowest address on, and the length in the last byte.
extern const std::array<UInt32, 256> one_byte_to_string_lookup_table;

/// Not instrumented: the XRay entry and exit sleds are a measurable part of calls this short.
#pragma clang attribute push([[clang::xray_never_instrument]], apply_to = function)

/// ----------------------------------------------------------------------------------------------------
/// 8 bits
/// `one_byte_to_string_lookup_table` has the text of every value: one load, one 4-byte store and no branch.
/// `Int8` uses it too, with the sign written in front of the magnitude.
/// ----------------------------------------------------------------------------------------------------

namespace itoa_impl
{
/// The shift that places byte `i` of an integer's in-memory representation.
template <typename T>
constexpr UInt32 byteShift(UInt32 i)
{
    return 8 * (std::endian::native == std::endian::little ? i : sizeof(T) - 1 - i);
}

/// `value` is below 256. `UInt32`, not `UInt8`, so that callers computing it are not narrowed to 8-bit instructions.
ALWAYS_INLINE inline char * writeUInt8Text(UInt32 value, char * p)
{
    UInt32 entry = one_byte_to_string_lookup_table[value];
    /// One 4-byte store instead of the exact length: storing 2 + 1 bytes is 1.35x slower, and storing only the
    /// text needs a branch on the length. The bytes after the text are the rest of the entry (zeros and the length),
    /// to be overwritten by whatever is written next; `max_int_width<UInt8>` reserves the extra byte.
    memcpy(p, &entry, 4);
    return p + ((entry >> byteShift<UInt32>(3)) & 0xFF);
}

ALWAYS_INLINE inline char * writeInt8Text(Int32 value, char * p)
{
    Int32 sign = value >> 31;
    *p = '-';
    /// The magnitude as `(value ^ sign) - sign`, not `std::abs`: clang narrows that to 8-bit instructions, which
    /// is about two cycles slower. `p - sign` is one past the '-' when there is one.
    return writeUInt8Text((value ^ sign) - sign, p - sign);
}
}

inline char * itoa(UInt8 i, char * p)
{
    return itoa_impl::writeUInt8Text(i, p);
}

inline char * itoa(uint8_t i, char * p)
{
    return itoa_impl::writeUInt8Text(UInt8(i), p);
}

inline char * itoa(Int8 i, char * p)
{
    return itoa_impl::writeInt8Text(i, p);
}

inline char * itoa(int8_t i, char * p)
{
    return itoa_impl::writeInt8Text(i, p);
}

/// ----------------------------------------------------------------------------------------------------
/// 16 bits: SWAR (SIMD within a register), no table. The quotients of the value by 10000, 1000, 100 and 10 are
/// computed in parallel, a byte apart in one register, and one multiplication turns them into digits.
/// ----------------------------------------------------------------------------------------------------

namespace itoa_impl
{
/// `value` is below 65536. One 8-byte store whatever the length.
ALWAYS_INLINE inline char * writeUInt16Text(UInt32 value, char * p)
{
    /// As in `writeUInt32Text`: short values usually come in runs, where this branch is predicted.
    if (value < 256)
        return writeUInt8Text(value, p);

    /// [n / 10000][n / 1000][n / 100][n / 10][n], a byte apart, all computed from `n` directly. The larger quotients
    /// overflow into the following bytes, so they are added, not OR-ed.
    /// The first three in one vector multiplication. Each lane divides by multiplying with a rounded-up reciprocal,
    /// as compilers do for a division by a constant (these are the constants clang emits for a 16-bit `n`):
    /// d = 2^a * o with o odd, and n / d = ((n >> a) * m) >> b with m = ceil(2^b / o):
    ///     10000 = 2^4 * 625: m = ceil(2^19 / 625) = 839
    ///      1000 = 2^3 * 125: m = ceil(2^20 / 125) = 8389
    ///       100 = 2^2 * 25:  m = ceil(2^17 / 25) = 5243
    /// `b` is the smallest shift for which rounding `m` up stays exact for every `n` below 2^16, and all products fit
    /// in 32 bits. The fourth lane is unused. Then one shuffle gathers the quotients a byte apart; `n / 100` is wider
    /// than a byte, so it takes two.
    using UInt32x4 = UInt32 __attribute__((vector_size(16)));
    using UInt8x16 = uint8_t __attribute__((vector_size(16)));
    UInt32x4 lanes = (UInt32x4{value, value, value, value} >> UInt32x4{4, 3, 2, 0}) * UInt32x4{839, 8389, 5243, 0}
        >> UInt32x4{19, 20, 17, 0};
    /// Only the first four bytes are used. A full 16-byte shuffle is one instruction (`pshufb`, `tbl`), extracting
    /// four bytes is several on ARM.
    UInt8x16 lane_bytes = std::bit_cast<UInt8x16>(lanes);
    UInt8x16 gathered;
    if constexpr (std::endian::native == std::endian::little)
        gathered = __builtin_shufflevector(lane_bytes, lane_bytes, 0, 4, 8, 9, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1);
    else
        gathered = __builtin_shufflevector(lane_bytes, lane_bytes, 10, 11, 7, 3, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1);
    UInt32 low_quotients = std::bit_cast<UInt32x4>(gathered)[0];
    UInt64 quotients = low_quotients + (UInt64(value / 10) << 24) + (UInt64(value) << 32);
    /// Subtracting ten times the previous quotient leaves one digit per byte, and the overflow cancels out:
    /// byte k becomes (n / 10^(4 - k)) - 10 * (n / 10^(5 - k)), the digit, and the rest goes above byte 4.
    UInt64 digits = quotients - 10 * (quotients << 8);
    /// The leading zero digits, in parallel with the multiplication: the lowest nonzero quotient is in byte k when the
    /// first k digits are zero, and it is at most 65 there, so its lowest set bit stays in that byte. In the low
    /// 32 bits, so that no quotient is nonzero there for `n` below 10, including 0: 32 / 8 is 4 zeros.
    UInt32 zeros = std::countr_zero(static_cast<UInt32>(quotients)) / 8;
    UInt64 text = (digits + 0x3030303030) >> (8 * zeros);
    /// The first digit is in the lowest byte of `text`.
    unalignedStoreLittleEndian<UInt64>(p, text);
    return p + 5 - zeros;
}

ALWAYS_INLINE inline char * writeInt16Text(Int32 value, char * p)
{
    Int32 sign = value >> 31;
    *p = '-';
    /// As for `Int8`.
    return writeUInt16Text((value ^ sign) - sign, p - sign);
}
}

inline char * itoa(UInt16 i, char * p)
{
    return itoa_impl::writeUInt16Text(i, p);
}

inline char * itoa(Int16 i, char * p)
{
    return itoa_impl::writeInt16Text(i, p);
}

/// ----------------------------------------------------------------------------------------------------
/// 32 bits: as zmij (https://github.com/vitaut/zmij) does, with SWAR (SIMD within a register) and two small tables.
/// ----------------------------------------------------------------------------------------------------

namespace itoa_impl
{
/// Kendall Willets' digit count, as in {fmt}: indexed by the leading zeros of `n`, 0 to 32 (for 0), `(n + entry) >> 32`
/// is the digit count of that bit length, plus one if `n` reaches the next power of ten.
/// Together with `reading_order` in one object, so that one register holds the address of both.
struct UInt32Tables
{
    std::array<UInt64, 33> digit_count_increments;
    std::array<uint8_t, 26> reading_order;
};

constexpr std::array<UInt64, 33> makeDigitCountIncrements()
{
    std::array<UInt64, 33> table{};
    for (UInt32 leading_zeros = 0; leading_zeros <= 32; ++leading_zeros)
    {
        UInt32 bit = leading_zeros == 32 ? 0 : 31 - leading_zeros;
        UInt64 digits = 1;
        UInt64 power = 10;
        for (; power <= (UInt64(1) << bit); power *= 10)
            ++digits;
        table[leading_zeros] = (digits << 32) + (power < (UInt64(1) << 32) ? (UInt64(1) << 32) - power : 0);
    }
    return table;
}

/// The digits come out of `writeUInt32Text` with digits 5 to 8 first, and reversed in each 32-bit lane on
/// little-endian: d8 d7 d6 d5, d4 d3 d2 d1, d10 d9. 16 bytes of `reading_order` from offset `k` are the shuffle that
/// puts them in reading order without the first `k`.
inline constexpr UInt32Tables uint32_tables = {
    makeDigitCountIncrements(),
    std::endian::native == std::endian::little ? std::array<uint8_t, 26>{7, 6, 5, 4, 3, 2, 1, 0, 9, 8}
                                               : std::array<uint8_t, 26>{4, 5, 6, 7, 0, 1, 2, 3, 10, 11}};

using UInt8x16 = uint8_t __attribute__((vector_size(16)));

/// `result[i] = bytes[order[i]]` with every `order[i]` below 16: a byte shuffle with a runtime mask, the one operation
/// without a vector operator. Elsewhere than x86 and ARM, it is done one byte at a time.
ALWAYS_INLINE inline UInt8x16 shuffleBytes(UInt8x16 bytes, UInt8x16 order)
{
#if defined(__SSSE3__)
    /// The builtin of `_mm_shuffle_epi8`: `<immintrin.h>` is too large for a header included everywhere.
    using Int8x16 = char __attribute__((vector_size(16)));
    return std::bit_cast<UInt8x16>(__builtin_ia32_pshufb128(std::bit_cast<Int8x16>(bytes), std::bit_cast<Int8x16>(order)));
#elif defined(__aarch64__)
    return std::bit_cast<UInt8x16>(vqtbl1q_u8(std::bit_cast<uint8x16_t>(bytes), std::bit_cast<uint8x16_t>(order)));
#else
    UInt8x16 result{};
    for (size_t i = 0; i < 16; ++i)
        result[i] = bytes[order[i]];
    return result;
#endif
}

/// The value is split into groups of four, four and two digits, one per 32-bit lane, each lane into digit pairs,
/// then each pair into digits, as zmij does. One shuffle puts the digits in reading order without the leading zeros,
/// and one 16-byte store writes them.
ALWAYS_INLINE inline char * writeUInt32Text(UInt32 value, char * p)
{
    /// Short values are common and usually come in runs, where this branch is predicted: the 8-bit table costs one
    /// load and one store, much less than the vector code below.
    if (value < 256)
        return writeUInt8Text(value, p);

    UInt32 length = static_cast<UInt32>((value + uint32_tables.digit_count_increments[std::countl_zero(value)]) >> 32);

    using UInt64x2 = UInt64 __attribute__((vector_size(16)));
    using UInt32x4 = UInt32 __attribute__((vector_size(16)));
    using UInt16x8 = UInt16 __attribute__((vector_size(16)));

    /// The groups, from n / 100 and n / 10^6 computed at once in two 64-bit lanes (the constants clang uses for them):
    /// next_four = n / 100 - 10000 * (n / 10^6), first_four = n / 10^6, last_two = n - 100 * (n / 100).
    /// On big-endian, the high half of a 64-bit lane is the first 32-bit lane.
    constexpr bool little_endian = std::endian::native == std::endian::little;
    /// Converted from 32-bit lanes, so that the compiler knows the high halves are 0 and multiplies 32 x 32 to 64 bits
    /// (`vpmuludq`, `umull`), also where this is inlined into code that already knows `value` fits in 32 bits.
    using UInt32x2 = UInt32 __attribute__((vector_size(8)));
    UInt64x2 wide = __builtin_convertvector(UInt32x2{value, value}, UInt64x2);
    UInt32x4 quotients = std::bit_cast<UInt32x4>((wide * UInt64x2{0x51eb851f, 0x431bde83}) >> UInt64x2{37, 50});
    UInt32x4 terms;
    UInt32x4 subtracted;
    /// Shuffles that are one instruction on x86 and ARM: the low halves of the 64-bit lanes of both vectors, and
    /// the two 64-bit lanes swapped. The fourth lane gets `n`, which is never written out.
    if constexpr (little_endian)
    {
        terms = __builtin_shufflevector(quotients, std::bit_cast<UInt32x4>(wide), 0, 2, 4, 6);
        subtracted = __builtin_shufflevector(quotients, quotients, 2, 3, 0, 1);
    }
    else
    {
        terms = __builtin_shufflevector(quotients, std::bit_cast<UInt32x4>(wide), 1, 3, 5, 7);
        subtracted = __builtin_shufflevector(quotients, quotients, 3, 2, 1, 0);
    }
    UInt32x4 groups = terms + subtracted * UInt32x4{static_cast<UInt32>(-10000), 0, static_cast<UInt32>(-100), 0};

    /// A lane v < 10^4 becomes (v % 100) | (v / 100) << 16: v + (v / 100) * (2^16 - 100). The last lane is 0; the
    /// different multipliers there make clang load each constant as a vector, instead of broadcasting one element
    /// with an extra instruction.
    UInt32x4 pairs = groups + ((groups * UInt32x4{5243, 5243, 5243, 0}) >> 19) * UInt32x4{65536 - 100, 65536 - 100, 65536 - 100, 0};
    /// Each 16-bit pair p < 100 becomes the characters (p % 10) | (p / 10) << 8: p + (p / 10) * (2^8 - 10), with
    /// p / 10 = (p * 205) >> 11, exact for p < 100.
    UInt16x8 pair_lanes = std::bit_cast<UInt16x8>(pairs);
    UInt16x8 tens = (pair_lanes * UInt16(205)) >> 11;
    UInt8x16 text = std::bit_cast<UInt8x16>(pair_lanes + 0x3030 + tens * UInt16(256 - 10));

    UInt8x16 order;
    memcpy(&order, uint32_tables.reading_order.data() + 10 - length, 16);
    text = shuffleBytes(text, order);
    memcpy(p, &text, 16);
    return p + length;
}
}

inline char * itoa(UInt32 i, char * p)
{
    return itoa_impl::writeUInt32Text(i, p);
}

inline char * itoa(Int32 i, char * p)
{
    UInt32 sign = static_cast<UInt32>(i >> 31);
    *p = '-';
    /// As for `Int8`, in unsigned arithmetic: the magnitude of `INT32_MIN` does not fit in `Int32`.
    return itoa_impl::writeUInt32Text((static_cast<UInt32>(i) ^ sign) - sign, p - static_cast<Int32>(sign));
}

/// ----------------------------------------------------------------------------------------------------
/// 64, 128 and 256 bits: the values that fit in a smaller type are written here, the others in `itoa.cpp`.
/// ----------------------------------------------------------------------------------------------------

namespace itoa_impl
{
/// Out of line in `itoa.cpp`: the values that need them are rare, and their code is large.
char * writeLargeUInt64Text(UInt64 value, char * p);
char * writeLargeUInt128Text(unsigned __int128 value, char * p);
char * writeLargeUInt256Text(unsigned __int128 high, unsigned __int128 low, char * p);

/// Most values are small, and the 32-bit code is faster for them. The rest is out of line, so that the callers of this
/// function do not get both bodies.
ALWAYS_INLINE inline char * writeUInt64Text(UInt64 value, char * p)
{
    if (likely(value <= std::numeric_limits<UInt32>::max()))
        return writeUInt32Text(static_cast<UInt32>(value), p);
    return writeLargeUInt64Text(value, p);
}

ALWAYS_INLINE inline char * writeUInt128Text(unsigned __int128 value, char * p)
{
    /// Most values fit in 64 bits.
    if (likely(value >> 64 == 0))
        return writeUInt64Text(static_cast<UInt64>(value), p);
    return writeLargeUInt128Text(value, p);
}
}

inline char * itoa(UInt64 i, char * p)
{
    return itoa_impl::writeUInt64Text(i, p);
}

inline char * itoa(Int64 i, char * p)
{
    UInt64 sign = static_cast<UInt64>(i >> 63);
    *p = '-';
    /// As for `Int32`.
    return itoa_impl::writeUInt64Text((static_cast<UInt64>(i) ^ sign) - sign, p - static_cast<Int64>(sign));
}

inline char * itoa(UInt128 i, char * p)
{
    return itoa_impl::writeUInt128Text(
        (static_cast<unsigned __int128>(i.items[UInt128::_impl::little(1)]) << 64) | i.items[UInt128::_impl::little(0)], p);
}

inline char * itoa(Int128 i, char * p)
{
    unsigned __int128 value = (static_cast<unsigned __int128>(static_cast<UInt64>(i.items[Int128::_impl::little(1)])) << 64)
        | static_cast<UInt64>(i.items[Int128::_impl::little(0)]);
    unsigned __int128 sign = static_cast<unsigned __int128>(static_cast<__int128>(value) >> 127);
    *p = '-';
    /// As for `Int32`.
    value = (value ^ sign) - sign;
    p += static_cast<UInt64>(sign) & 1;
    return itoa_impl::writeUInt128Text(value, p);
}

inline char * itoa(const UInt256 & i, char * p)
{
    unsigned __int128 high = (static_cast<unsigned __int128>(i.items[UInt256::_impl::little(3)]) << 64)
        | i.items[UInt256::_impl::little(2)];
    unsigned __int128 low = (static_cast<unsigned __int128>(i.items[UInt256::_impl::little(1)]) << 64)
        | i.items[UInt256::_impl::little(0)];
    /// Most values fit in 128 bits.
    if (likely(high == 0))
        return itoa_impl::writeUInt128Text(low, p);
    return itoa_impl::writeLargeUInt256Text(high, low, p);
}

inline char * itoa(const Int256 & i, char * p)
{
    unsigned __int128 high = (static_cast<unsigned __int128>(static_cast<UInt64>(i.items[Int256::_impl::little(3)])) << 64)
        | static_cast<UInt64>(i.items[Int256::_impl::little(2)]);
    unsigned __int128 low = (static_cast<unsigned __int128>(static_cast<UInt64>(i.items[Int256::_impl::little(1)])) << 64)
        | static_cast<UInt64>(i.items[Int256::_impl::little(0)]);
    unsigned __int128 sign = static_cast<unsigned __int128>(static_cast<__int128>(high) >> 127);
    *p = '-';
    p += static_cast<UInt64>(sign) & 1;
    /// As for `Int32`, in two halves: the negation of the low half carries into the high one when the low half is 0.
    high = (high ^ sign) + (sign & (low == 0));
    low = (low ^ sign) - sign;
    if (likely(high == 0))
        return itoa_impl::writeUInt128Text(low, p);
    return itoa_impl::writeLargeUInt256Text(high, low, p);
}

/// ----------------------------------------------------------------------------------------------------
/// `long` where it is a distinct type: the same as the fixed-width type of its size. After the others, which it calls.
/// Naming the type is the whole point here, so `google-runtime-int` has nothing to suggest.
/// ----------------------------------------------------------------------------------------------------

// NOLINTBEGIN(google-runtime-int)
#if defined(LONG_IS_A_DISTINCT_TYPE)
static_assert(
    !std::is_same_v<long, Int32> && !std::is_same_v<long, Int64>,
    "`long` is one of the fixed-width types here, so it must not have overloads of its own");
static_assert(sizeof(long) == 4 || sizeof(long) == 8);

inline char * itoa(unsigned long i, char * p)
{
    if constexpr (sizeof(unsigned long) == 8)
        return itoa(UInt64(i), p);
    else
        return itoa(UInt32(i), p);
}

inline char * itoa(long i, char * p)
{
    if constexpr (sizeof(long) == 8)
        return itoa(Int64(i), p);
    else
        return itoa(Int32(i), p);
}
#else
static_assert(
    std::is_same_v<long, Int32> || std::is_same_v<long, Int64>,
    "`long` is a distinct type here and needs overloads of its own");
#endif
// NOLINTEND(google-runtime-int)

#pragma clang attribute pop

char * writeFixedDigits(UInt64 value, UInt32 width, char * p);
char * writeFixedDigits(UInt128 value, UInt32 width, char * p);
char * writeFixedDigits(const UInt256 & value, UInt32 width, char * p);

/// The text of `value`, pointing into `one_byte_to_string_lookup_table`.
inline std::string_view oneByteToString(UInt8 value)
{
    const UInt32 & entry = one_byte_to_string_lookup_table[value];
    size_t length = std::endian::native == std::endian::little ? entry >> 24 : entry & 0xFF;
    return {reinterpret_cast<const char *>(&entry), length};
}

void setUseAVX512ItoaForTests(bool value);
bool getUseAVX512ItoaForTests();
