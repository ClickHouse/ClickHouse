#include <array>
#include <bit>
#include <cstring>
#include <type_traits>
#include <base/defines.h>
#include <base/extended_types.h>
#include <base/itoa.h>
#include <base/unaligned.h>

#if defined(__aarch64__)
#    include <arm_neon.h>
#endif

#if defined(__x86_64__)
#    include <cpuid.h>
#    include <immintrin.h>
#endif

/// ----------------------------------------------------------------------------------------------------
/// 8 bits
/// `one_byte_to_string_lookup_table` has the text of every value: one load, one 4-byte store and no branch.
/// `Int8` uses it too, with the sign written in front of the magnitude.
/// ----------------------------------------------------------------------------------------------------

namespace
{
/// The shift that places byte `i` of an integer's in-memory representation.
template <typename T>
constexpr UInt32 byteShift(UInt32 i)
{
    return 8 * (std::endian::native == std::endian::little ? i : sizeof(T) - 1 - i);
}
}

constexpr std::array<UInt32, 256> one_byte_to_string_lookup_table = []
{
    std::array<UInt32, 256> table{};
    for (UInt32 value = 0; value < 256; ++value)
    {
        UInt32 length = value >= 100 ? 3 : (value >= 10 ? 2 : 1);
        UInt32 entry = length << byteShift<UInt32>(3);
        for (UInt32 i = length, rest = value; i > 0; --i, rest /= 10)
            entry |= ('0' + rest % 10) << byteShift<UInt32>(i - 1);
        table[value] = entry;
    }
    return table;
}();

namespace
{
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

char * itoa(UInt8 i, char * p)
{
    return writeUInt8Text(i, p);
}

char * itoa(uint8_t i, char * p)
{
    return writeUInt8Text(UInt8(i), p);
}

char * itoa(Int8 i, char * p)
{
    return writeInt8Text(i, p);
}

char * itoa(int8_t i, char * p)
{
    return writeInt8Text(i, p);
}

/// ----------------------------------------------------------------------------------------------------
/// 16 bits: SWAR (SIMD within a register), no table. The quotients of the value by 10000, 1000, 100 and 10 are
/// computed in parallel, a byte apart in one register, and one multiplication turns them into digits.
/// ----------------------------------------------------------------------------------------------------

namespace
{
/// `value` is below 65536. One 8-byte store whatever the length.
ALWAYS_INLINE inline char * writeUInt16Text(UInt32 value, char * p)
{
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

char * itoa(UInt16 i, char * p)
{
    return writeUInt16Text(i, p);
}

char * itoa(Int16 i, char * p)
{
    return writeInt16Text(i, p);
}

/// ----------------------------------------------------------------------------------------------------
/// 32 bits: as zmij (https://github.com/vitaut/zmij) does, with SWAR (SIMD within a register) and two small tables.
/// ----------------------------------------------------------------------------------------------------

namespace
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
constexpr UInt32Tables uint32_tables = {
    makeDigitCountIncrements(),
    std::endian::native == std::endian::little ? std::array<uint8_t, 26>{7, 6, 5, 4, 3, 2, 1, 0, 9, 8}
                                               : std::array<uint8_t, 26>{4, 5, 6, 7, 0, 1, 2, 3, 10, 11}};

using UInt8x16 = uint8_t __attribute__((vector_size(16)));

/// `result[i] = bytes[order[i]]` with every `order[i]` below 16: a byte shuffle with a runtime mask, the one operation
/// without a vector operator. Elsewhere than x86 and ARM, it is done one byte at a time.
ALWAYS_INLINE inline UInt8x16 shuffleBytes(UInt8x16 bytes, UInt8x16 order)
{
#if defined(__SSSE3__)
    return std::bit_cast<UInt8x16>(_mm_shuffle_epi8(std::bit_cast<__m128i>(bytes), std::bit_cast<__m128i>(order)));
#elif defined(__aarch64__)
    return std::bit_cast<UInt8x16>(vqtbl1q_u8(std::bit_cast<uint8x16_t>(bytes), std::bit_cast<uint8x16_t>(order)));
#else
    UInt8x16 result;
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
    UInt32 length = static_cast<UInt32>((value + uint32_tables.digit_count_increments[std::countl_zero(value)]) >> 32);

    using UInt64x2 = UInt64 __attribute__((vector_size(16)));
    using UInt32x4 = UInt32 __attribute__((vector_size(16)));
    using UInt32x8 = UInt32 __attribute__((vector_size(32)));
    using UInt16x8 = UInt16 __attribute__((vector_size(16)));

    /// The groups, from n / 100 and n / 10^6 computed at once in two 64-bit lanes (the constants clang uses for them):
    /// next_four = n / 100 - 10000 * (n / 10^6), first_four = n / 10^6, last_two = n - 100 * (n / 100).
    /// On big-endian, the high half of a 64-bit lane is the first 32-bit lane.
    constexpr bool little_endian = std::endian::native == std::endian::little;
    /// The zeros are explicit, so that the multiplication is `vpmuludq` (32 x 32 to 64 bits) also where this is inlined
    /// into code that already knows `value` fits in 32 bits, and would otherwise lose that in the vector.
    UInt64x2 wide;
    if constexpr (little_endian)
        wide = std::bit_cast<UInt64x2>(UInt32x4{value, 0, value, 0});
    else
        wide = std::bit_cast<UInt64x2>(UInt32x4{0, value, 0, value});
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
    /// p / 10 = (p * 6554) >> 16, the high half of a 16-bit multiplication.
    UInt16x8 pair_lanes = std::bit_cast<UInt16x8>(pairs);
    UInt16x8 tens = __builtin_convertvector((__builtin_convertvector(pair_lanes, UInt32x8) * 6554) >> 16, UInt16x8);
    UInt8x16 text = std::bit_cast<UInt8x16>(pair_lanes + 0x3030 + tens * UInt16(256 - 10));

    UInt8x16 order;
    memcpy(&order, uint32_tables.reading_order.data() + 10 - length, 16);
    text = shuffleBytes(text, order);
    memcpy(p, &text, 16);
    return p + length;
}
}

char * itoa(UInt32 i, char * p)
{
    return writeUInt32Text(i, p);
}

char * itoa(Int32 i, char * p)
{
    UInt32 sign = static_cast<UInt32>(i >> 31);
    *p = '-';
    /// As for `Int8`, in unsigned arithmetic: the magnitude of `INT32_MIN` does not fit in `Int32`.
    return writeUInt32Text((static_cast<UInt32>(i) ^ sign) - sign, p - static_cast<Int32>(sign));
}

/// ----------------------------------------------------------------------------------------------------
/// Shared by several sizes
/// The digit pair table, fixed blocks of eight digits and the sign.
/// ----------------------------------------------------------------------------------------------------

namespace
{
// Using a lookup table to convert binary numbers from 0 to 99
// into ascii characters as described by Andrei Alexandrescu in
// https://www.facebook.com/notes/facebook-engineering/three-optimization-tips-for-c/10151361643253920/
constexpr char digits[201] = "00010203040506070809"
                             "10111213141516171819"
                             "20212223242526272829"
                             "30313233343536373839"
                             "40414243444546474849"
                             "50515253545556575859"
                             "60616263646566676869"
                             "70717273747576777879"
                             "80818283848586878889"
                             "90919293949596979899";
ALWAYS_INLINE inline char * outTwoDigits(char * p, uint8_t value)
{
    memcpy(p, &digits[value * 2], 2);
    p += 2;
    return p;
}

ALWAYS_INLINE inline void writeEightFixedDigits(char * p, uint32_t z)
{
    constexpr UInt64 mask32 = (UInt64(1) << 32) - 1;
    auto f0 = (UInt64((1ull << 48ull) / 1e6 + 1) * z >> 16) + 1;
    outTwoDigits(p, static_cast<uint8_t>(f0 >> 32));
    auto f2 = (f0 & mask32) * 100;
    outTwoDigits(p + 2, static_cast<uint8_t>(f2 >> 32));
    auto f4 = (f2 & mask32) * 100;
    outTwoDigits(p + 4, static_cast<uint8_t>(f4 >> 32));
    auto f6 = (f4 & mask32) * 100;
    outTwoDigits(p + 6, static_cast<uint8_t>(f6 >> 32));
}

ALWAYS_INLINE inline char * writeLeadingMinus(char * pos)
{
    *pos = '-';
    return pos + 1;
}
}

/// ----------------------------------------------------------------------------------------------------
/// `long` where it is a distinct type
/// ----------------------------------------------------------------------------------------------------

#if defined(LONG_IS_A_DISTINCT_TYPE)
/// The same as the fixed-width type of its size.
char * itoa(unsigned long i, char * p)
{
    static_assert(sizeof(unsigned long) == 4 || sizeof(unsigned long) == 8);
    if constexpr (sizeof(unsigned long) == 8)
        return itoa(UInt64(i), p);
    else
        return itoa(UInt32(i), p);
}

char * itoa(long i, char * p)
{
    static_assert(sizeof(long) == 4 || sizeof(long) == 8);
    if constexpr (sizeof(long) == 8)
        return itoa(Int64(i), p);
    else
        return itoa(Int32(i), p);
}
#endif

/// ----------------------------------------------------------------------------------------------------
/// 64 bits
/// The AVX-512 IFMA path also serves `writeFixedDigits`.
/// ----------------------------------------------------------------------------------------------------

namespace
{
#if defined(__x86_64__)

/// Champagne Gareau, J. and Lemire, D., "Converting an Integer to a Decimal String in Under
/// Two Nanoseconds", Software: Practice and Experience 56(8), 2026, doi:10.1002/spe.70079.
namespace avx512ifma
{

#define ITOA_IFMA_TARGET __attribute__((target("avx512f,avx512vl,avx512bw,avx512dq,avx512ifma,avx512vbmi")))

ALWAYS_INLINE inline char * shiftedPointer(char * p, Int32 offset)
{
    return reinterpret_cast<char *>(reinterpret_cast<uintptr_t>(p) + static_cast<uintptr_t>(static_cast<intptr_t>(offset)));
}

ITOA_IFMA_TARGET ALWAYS_INLINE inline __m512i digitsOfEight(UInt64 n)
{
    constexpr UInt64 two_to_52 = 1ULL << 52;
    const __m512i c = _mm512_setr_epi64(
        two_to_52 / 100000000,
        two_to_52 / 10000000,
        two_to_52 / 1000000,
        two_to_52 / 100000,
        two_to_52 / 10000,
        two_to_52 / 1000,
        two_to_52 / 100,
        two_to_52 / 10);
    const __m512i low = _mm512_madd52lo_epu64(c, _mm512_set1_epi64(static_cast<Int64>(n)), c);
    return _mm512_madd52hi_epu64(_mm512_set1_epi64('0'), _mm512_set1_epi64(10), low);
}

ITOA_IFMA_TARGET ALWAYS_INLINE inline __m128i digitsOfSixteen(UInt64 n)
{
    const __m512i high = digitsOfEight(n / 100000000);
    const __m512i low = digitsOfEight(n % 100000000);
    /// Only the low 16 indexes matter, but the whole vector is spelled out as a constant on purpose:
    /// widening a 128-bit constant with `_mm512_castsi128_si512` leaves the upper lanes undefined, which makes
    /// the index a non-constant value in the IR, and MemorySanitizer in clang 21 falsely reports
    /// use-of-uninitialized-value for `vpermi2b` with a non-constant index. See
    /// https://github.com/llvm/llvm-project/pull/148785 - the fix is not in clang 21.
    const __m512i indexes = _mm512_set_epi8(
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0x78, 0x70, 0x68, 0x60, 0x58, 0x50, 0x48, 0x40, 0x38, 0x30, 0x28, 0x20, 0x18, 0x10, 0x08, 0x00);
    return _mm512_castsi512_si128(_mm512_permutex2var_epi8(high, indexes, low));
}

/// Writes the last `count` of the eight digits in `digits` at `p`.
///
/// MemorySanitizer does not model `_mm512_mask_cvtusepi64_storeu_epi8`: the bytes are written, but their shadow
/// is left untouched, so under MSan every number formatted here looks uninitialized to whatever reads it next.
/// Converting in a register and storing with `_mm_mask_storeu_epi8` is modelled correctly and still propagates
/// the shadow of `digits`, so genuinely uninitialized input is still caught. It needs one extra instruction,
/// hence it is only used under MSan.
ITOA_IFMA_TARGET ALWAYS_INLINE inline void storeDigitsOfEight(char * p, UInt32 count, __m512i digits)
{
    char * destination = shiftedPointer(p, static_cast<Int32>(count) - 8);
#if defined(MEMORY_SANITIZER)
    _mm_mask_storeu_epi8(
        destination, static_cast<__mmask16>((0xff00u >> count) & 0xffu), _mm512_cvtusepi64_epi8(digits));
#else
    _mm512_mask_cvtusepi64_storeu_epi8(destination, static_cast<__mmask8>(0xff00 >> count), digits);
#endif
}

ITOA_IFMA_TARGET char * fixedDigits(char * p, UInt64 value, UInt32 width)
{
    if (width <= 8)
    {
        if (unlikely(value >= 100000000ULL))
            value %= 100000000ULL;

        storeDigitsOfEight(p, width, digitsOfEight(value));
        return p + width;
    }

    if (width <= 16)
    {
        if (unlikely(value >= 10000000000000000ULL))
            value %= 10000000000000000ULL;

        _mm_mask_storeu_epi8(
            shiftedPointer(p, static_cast<Int32>(width) - 16),
            static_cast<__mmask16>(0xffffu << (16 - width)),
            digitsOfSixteen(value));
        return p + width;
    }

    const UInt64 quotient = value / 10000000000000000ULL;
    const UInt32 quotient_digits = width - 16;
    storeDigitsOfEight(p, quotient_digits, digitsOfEight(quotient));
    _mm_storeu_si128(
        reinterpret_cast<__m128i *>(p + quotient_digits), digitsOfSixteen(value - quotient * 10000000000000000ULL));
    return p + width;
}

#undef ITOA_IFMA_TARGET

}

bool detectAVX512IFMA()
{
    UInt32 eax = 0;
    UInt32 ebx = 0;
    UInt32 ecx = 0;
    UInt32 edx = 0;

    if (!__get_cpuid(1, &eax, &ebx, &ecx, &edx))
        return false;

    if ((ecx & (1u << 27)) == 0)
        return false;

    UInt32 xcr0_low = 0;
    UInt32 xcr0_high = 0;
    __asm__ volatile("xgetbv" : "=a"(xcr0_low), "=d"(xcr0_high) : "c"(0));

    if ((xcr0_low & 0x6u) != 0x6u || ((xcr0_low >> 5) & 0x7u) != 0x7u)
        return false;

    if (!__get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx))
        return false;

    constexpr UInt32 have_avx512f = 1u << 16;
    constexpr UInt32 have_avx512dq = 1u << 17;
    constexpr UInt32 have_avx512ifma = 1u << 21;
    constexpr UInt32 have_avx512bw = 1u << 30;
    constexpr UInt32 have_avx512vl = 1u << 31;
    constexpr UInt32 have_avx512vbmi = 1u << 1;

    return (ebx & (have_avx512f | have_avx512dq | have_avx512ifma | have_avx512bw | have_avx512vl))
        == (have_avx512f | have_avx512dq | have_avx512ifma | have_avx512bw | have_avx512vl)
        && (ecx & have_avx512vbmi) != 0;
}

bool has_avx512_ifma = detectAVX512IFMA();

#endif

/// Together in one object, so that one register holds the address of all of them. C arrays, not `std::array`: an
/// index loaded from a table cannot be proven in range, and the libc++ hardening would check it on every call.
struct UInt64Tables
{
    /// fmt's digit count (https://github.com/fmtlib/fmt): an estimate from the leading zeros, then one less if `n` is
    /// below the power of ten of that estimate.
    uint8_t estimated_length[65];
    UInt64 length_threshold[21];
    uint8_t reading_order[32];
    /// The same for the 16 digits of a chunk of `writeLargeUInt128Text`.
    uint8_t chunk_order[32];
    /// 16 bytes from offset `20 - length` put the first 4 digits in reading order without the leading zeros.
    uint8_t top_order[32];
    /// By length: the digits of the first 4 written, and where the shuffle of the other 16 starts in `reading_order`.
    uint8_t top_length[21];
    uint8_t order_offset[21];
};

constexpr UInt64Tables uint64_tables = []
{
    UInt64Tables tables{};
    for (UInt32 leading_zeros = 0; leading_zeros <= 64; ++leading_zeros)
    {
        /// The digits of the largest value with this many leading zeros.
        UInt64 largest = leading_zeros == 64 ? 0 : ~UInt64(0) >> leading_zeros;
        uint8_t length = 1;
        for (UInt64 rest = largest; rest >= 10; rest /= 10)
            ++length;
        tables.estimated_length[leading_zeros] = length;
    }
    UInt64 power = 1;
    for (UInt32 length = 2; length <= 20; ++length, power *= 10)
        tables.length_threshold[length] = power * 10;
    for (uint8_t length = 0; length <= 20; ++length)
    {
        tables.top_length[length] = length > 16 ? length - 16 : 0;
        tables.order_offset[length] = length > 16 ? 0 : 16 - length;
    }
    /// The 16 digits come out of `writeUInt64Text` as the 4-digit groups 2, 4, 3, 5 in the 32-bit lanes, reversed in
    /// each lane on little-endian. 16 bytes from offset `k` put them in reading order without the first `k`.
    constexpr uint8_t group_lane[4] = {0, 2, 1, 3};
    for (uint8_t i = 0; i < 16; ++i)
    {
        if constexpr (std::endian::native == std::endian::little)
            tables.reading_order[i] = 4 * group_lane[i / 4] + 3 - i % 4;
        else
            tables.reading_order[i] = 4 * group_lane[i / 4] + i % 4;
    }
    /// A chunk has its two halves of 8 digits in the 64-bit lanes, each as the last 4 digits in the low 32 bits and the
    /// first 4 in the high 32 bits: the groups 2, 1, 4, 3 on little-endian, 1 to 4 on big-endian.
    constexpr uint8_t chunk_group_lane[4] = {1, 0, 3, 2};
    for (uint8_t i = 0; i < 16; ++i)
    {
        if constexpr (std::endian::native == std::endian::little)
            tables.chunk_order[i] = 4 * chunk_group_lane[i / 4] + 3 - i % 4;
        else
            tables.chunk_order[i] = i;
    }
    /// The first 4 digits are in the first 32-bit lane, the first digit in the highest byte. Only the first
    /// `length - 16` bytes of the shuffle are kept, the others are any index in range.
    for (uint8_t i = 0; i < 32; ++i)
    {
        if constexpr (std::endian::native == std::endian::little)
            tables.top_order[i] = (3 - i) & 15;
        else
            tables.top_order[i] = i & 15;
    }
    return tables;
}();

using UInt32x4 = UInt32 __attribute__((vector_size(16)));
using UInt32x8 = UInt32 __attribute__((vector_size(32)));
using UInt8x32 = uint8_t __attribute__((vector_size(32)));

/// As for 32 bits: `n / 10^16`, at most 1844, is the first 4 digits, and the other 16 are four groups of 4 in the
/// 32-bit lanes of one vector, in the order of `reading_order`.
struct DigitGroups
{
    UInt32x4 groups;
    UInt32 top;
};

ALWAYS_INLINE inline DigitGroups digitGroups(UInt64 value)
{
    using UInt64x2 = UInt64 __attribute__((vector_size(16)));

    /// As zmij does: n * ceil(2^90 / 10^8) has n / 10^8 above bit 90, and the remainder as a fraction of 10^8 in the 32
    /// bits below, rounded up. The same for that quotient gives the first 4 digits, n / 10^16, and digits 5 to 12.
    constexpr UInt64 reciprocal = 12379400392853802749ULL;
    unsigned __int128 low_product = static_cast<unsigned __int128>(value) * reciprocal;
    UInt64 first_twelve = static_cast<UInt64>(low_product >> 90);
    /// `first_twelve` is below 2^38, so it can be multiplied by 2^6 first: then n / 10^16 is in the high 32 bits of the
    /// high 64, and the fraction in the low 32, with no shift across the two halves.
    UInt64 high_half = static_cast<UInt64>((static_cast<unsigned __int128>(first_twelve << 6) * reciprocal) >> 64);

    /// Multiplying a fraction by 10^4 puts the next 4 digits in the high 32 bits and leaves the rest as a fraction in
    /// the low 32: twice for each fraction gives the groups 2 to 5.
    UInt64x2 fractions = {static_cast<UInt32>(high_half) + UInt64(1), static_cast<UInt32>(low_product >> 58) + UInt64(1)};
    UInt64x2 first = fractions * 10000;
    UInt64x2 second = (first & 0xFFFFFFFF) * 10000;
    DigitGroups result;
    if constexpr (std::endian::native == std::endian::little)
        result.groups = __builtin_shufflevector(std::bit_cast<UInt32x4>(first), std::bit_cast<UInt32x4>(second), 1, 3, 5, 7);
    else
        result.groups = __builtin_shufflevector(std::bit_cast<UInt32x4>(first), std::bit_cast<UInt32x4>(second), 0, 2, 4, 6);
    result.top = static_cast<UInt32>(high_half >> 32);
    return result;
}

/// Groups of 4 digits to their characters, each lane with the first in the highest byte, whatever the endianness.
ALWAYS_INLINE inline UInt8x32 textOfGroups(UInt32x8 groups)
{
    using UInt16x16 = UInt16 __attribute__((vector_size(32)));
    using UInt32x16 = UInt32 __attribute__((vector_size(64)));
    UInt32x8 pairs = groups + ((groups * 5243) >> 19) * (65536 - 100);
    UInt16x16 pair_lanes = std::bit_cast<UInt16x16>(pairs);
    UInt16x16 tens = __builtin_convertvector((__builtin_convertvector(pair_lanes, UInt32x16) * 6554) >> 16, UInt16x16);
    return std::bit_cast<UInt8x32>(pair_lanes + 0x3030 + tens * UInt16(256 - 10));
}

ALWAYS_INLINE inline UInt8x16 lowHalf(UInt8x32 text)
{
    return __builtin_shufflevector(text, text, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
}

ALWAYS_INLINE inline UInt8x16 highHalf(UInt8x32 text)
{
    return __builtin_shufflevector(text, text, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31);
}

ALWAYS_INLINE inline UInt32 digitCount(UInt64 value)
{
    UInt32 estimated = uint64_tables.estimated_length[std::countl_zero(value)];
    return estimated - (value < uint64_tables.length_threshold[estimated]);
}

/// Writes the last `length` of the 16 digits in `text`, which are in the order of `order_table`.
ALWAYS_INLINE inline void writeLastDigits(UInt8x16 text, UInt32 length, const uint8_t * order_table, char * p)
{
    UInt8x16 order;
    memcpy(&order, order_table + uint64_tables.order_offset[length], 16);
    text = shuffleBytes(text, order);
    memcpy(p, &text, 16);
}

/// The first 4 digits, the fifth lane, are written with one 4-byte store, then the other 16 with one 16-byte store over
/// the part of those 4 bytes that is not digits.
NO_INLINE char * writeLargeUInt64Text(UInt64 value, char * p)
{
    UInt32 length = digitCount(value);
    DigitGroups groups = digitGroups(value);
    UInt8x32 text = textOfGroups(__builtin_shufflevector(groups.groups, UInt32x4{groups.top, 0, 0, 0}, 0, 1, 2, 3, 4, 5, 6, 7));

    UInt8x16 top_order;
    memcpy(&top_order, uint64_tables.top_order + 20 - length, 16);
    UInt8x16 top_text = shuffleBytes(highHalf(text), top_order);
    /// Stored as a 32-bit lane: a vector of 4 bytes is not a native type on AArch64, and is widened to store it.
    unalignedStore<UInt32>(p, std::bit_cast<UInt32x4>(top_text)[0]);
    writeLastDigits(lowHalf(text), length, uint64_tables.reading_order, p + uint64_tables.top_length[length]);
    return p + length;
}

/// The digits of `head` and `tail`, both below 10^16, in the two halves of one vector, in the order of `chunk_order`.
/// As zmij does: not `digitGroups`, which also makes the first 4 of 20 digits. Each chunk is split into two halves
/// of 8 digits, and all four halves into groups of 4 at once.
ALWAYS_INLINE inline UInt8x32 textOfTwo(UInt64 head, UInt64 tail)
{
    using UInt64x4 = UInt64 __attribute__((vector_size(32)));
    auto head_first = static_cast<UInt32>(head / 100000000);
    auto tail_first = static_cast<UInt32>(tail / 100000000);
    auto head_last = static_cast<UInt32>(head % 100000000);
    auto tail_last = static_cast<UInt32>(tail % 100000000);
    /// The zeros are explicit, so that the compiler knows the halves fit in 32 bits, and multiplies 32 x 32 to 64 bits.
    UInt64x4 halves;
    if constexpr (std::endian::native == std::endian::little)
        halves = std::bit_cast<UInt64x4>(UInt32x8{head_first, 0, head_last, 0, tail_first, 0, tail_last, 0});
    else
        halves = std::bit_cast<UInt64x4>(UInt32x8{0, head_first, 0, head_last, 0, tail_first, 0, tail_last});
    /// The last 4 digits of each half in its low 32 bits, the first 4 in its high 32 bits. Not `halves / 10000`: clang
    /// divides 64-bit lanes as if they could be any 64-bit value, with two multiplications. A half is below 10^8, and
    /// for that n * ceil(2^40 / 10^4) / 2^40 is n / 10^4, as zmij does.
    constexpr UInt64 reciprocal = (UInt64(1) << 40) / 10000 + 1;
    UInt64x4 groups = halves + ((halves * reciprocal) >> 40) * ((UInt64(1) << 32) - 10000);
    return textOfGroups(std::bit_cast<UInt32x8>(groups));
}

/// All the 32 digits of `textOfTwo` in reading order: on little-endian each 64-bit lane is reversed, as `chunk_order`.
ALWAYS_INLINE inline UInt8x32 chunksInReadingOrder(UInt8x32 text)
{
    if constexpr (std::endian::native == std::endian::little)
        return __builtin_shufflevector(
            text, text, 7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8,
            23, 22, 21, 20, 19, 18, 17, 16, 31, 30, 29, 28, 27, 26, 25, 24);
    else
        return text;
}

/// Most values are small, and the 32-bit code is faster for them. The rest is out of line, so that the callers of this
/// function do not get both bodies.
ALWAYS_INLINE inline char * writeUInt64Text(UInt64 value, char * p)
{
    if (likely(value <= std::numeric_limits<UInt32>::max()))
        return writeUInt32Text(static_cast<UInt32>(value), p);
    return writeLargeUInt64Text(value, p);
}
}

char * itoa(UInt64 i, char * p)
{
    return writeUInt64Text(i, p);
}

char * itoa(Int64 i, char * p)
{
    UInt64 sign = static_cast<UInt64>(i >> 63);
    *p = '-';
    /// As for `Int32`.
    return writeUInt64Text((static_cast<UInt64>(i) ^ sign) - sign, p - static_cast<Int64>(sign));
}

/// ----------------------------------------------------------------------------------------------------
/// 128 and 256 bits
/// They share the signed path and the division by 10^18.
/// ----------------------------------------------------------------------------------------------------

namespace
{
constexpr uint64_t max_multiple_of_hundred_that_fits_in_64_bits = 1'00'00'00'00'00'00'00'00'00ull;
static_assert(max_multiple_of_hundred_that_fits_in_64_bits % 100 == 0);

/// Divide a 128-bit unsigned integer by 10^18 using Barrett reduction.
/// Returns the quotient and stores the remainder in `remainder`.
/// This replaces the expensive `__udivti3` compiler runtime call with
/// a few multiplications and one correction step.
///
/// Barrett reduction: q ≈ floor(n * M / 2^128) where M = floor(2^128 / 10^18).
/// The approximation may be off by 1, corrected by checking the remainder.
ALWAYS_INLINE inline unsigned __int128 divmod_1e18(unsigned __int128 n, uint64_t & remainder)
{
    /// M = floor(2^128 / 10^18) = 340282366920938463463 (69 bits)
    /// Split as M_hi:M_lo where M = M_hi * 2^64 + M_lo.
    constexpr uint64_t M_lo = 0x725DD1D243ABA0E7ULL;
    constexpr uint64_t M_hi = 0x12ULL;

    /// Compute q = (n * M) >> 128 using schoolbook 64-bit multiplication.
    ///
    /// n * M = (n_hi * 2^64 + n_lo) * (M_hi * 2^64 + M_lo)
    ///       = n_hi*M_hi * 2^128 + (n_hi*M_lo + n_lo*M_hi) * 2^64 + n_lo*M_lo
    ///
    /// We need the bits at position 128 and above.
    uint64_t n_lo = static_cast<uint64_t>(n);
    uint64_t n_hi = static_cast<uint64_t>(n >> 64);

    /// Carry from n_lo * M_lo (upper 64 bits of 128-bit product)
    unsigned __int128 c = static_cast<unsigned __int128>(n_lo) * M_lo;
    uint64_t c_hi = static_cast<uint64_t>(c >> 64);

    /// Middle terms + carry, computed in 128 bits to capture overflow
    unsigned __int128 mid = static_cast<unsigned __int128>(n_hi) * M_lo
                          + static_cast<unsigned __int128>(n_lo) * M_hi
                          + c_hi;

    /// High part: n_hi * M_hi + carry from mid. This is the quotient approximation.
    /// n_hi * M_hi can exceed 64 bits (up to 68 bits), so use 128-bit arithmetic.
    unsigned __int128 q = static_cast<unsigned __int128>(n_hi) * M_hi
                        + static_cast<uint64_t>(mid >> 64);

    /// Correct: Barrett approximation may be off by 1.
    unsigned __int128 r = n - q * max_multiple_of_hundred_that_fits_in_64_bits;
    if (r >= max_multiple_of_hundred_that_fits_in_64_bits)
    {
        q++;
        r -= max_multiple_of_hundred_that_fits_in_64_bits;
    }
    remainder = static_cast<uint64_t>(r);
    return q;
}

/// Divide a 256-bit value by 10^18 in place and return the remainder, one limb at a time from the
/// most significant one down. `wide::integer` divides bit by bit, which is ~70 times slower.
/// The quotient of every step fits into a limb because the previous remainder is below 10^18.
ALWAYS_INLINE inline uint64_t divmod_1e18(UInt256 & value)
{
    uint64_t remainder = 0;
    for (unsigned i = 4; i > 0; --i)
    {
        unsigned __int128 current
            = (static_cast<unsigned __int128>(remainder) << 64) | value.items[UInt256::_impl::little(i - 1)];
        value.items[UInt256::_impl::little(i - 1)]
            = static_cast<uint64_t>(current / max_multiple_of_hundred_that_fits_in_64_bits);
        remainder = static_cast<uint64_t>(current % max_multiple_of_hundred_that_fits_in_64_bits);
    }
    return remainder;
}

/// Divides a 256-bit unsigned integer by 10^18, one limb per step. Returns the quotient and stores
/// the remainder in `remainder`.
///
/// Every step is a 128 / 64 division by a constant, which `divmod_1e18` does with multiplications
/// only. The quotient of a step fits in a limb because the remainder carried in from the previous
/// step is below 10^18.
///
/// This is why the digit blocks are not stripped off with _BitInt(256) division: for that the
/// compiler emits a generic bignum sequence, while here the divisor is a known constant spanning a
/// single limb. It also produces the quotient and the remainder at once, so a block costs one
/// division instead of two.
ALWAYS_INLINE inline UInt256 divmod_1e18_256(UInt256 x, uint64_t & remainder)
{
    UInt256 quotient{};
    uint64_t r = 0;
    for (int i = 3; i >= 0; --i)
    {
        const unsigned __int128 current = (static_cast<unsigned __int128>(r) << 64) | x.items[UInt256::_impl::little(i)];
        quotient.items[UInt256::_impl::little(i)] = static_cast<uint64_t>(divmod_1e18(current, r));
    }
    remainder = r;
    return quotient;
}

/// Extract up to 9 digit pairs from a u64 value into the provided output buffer.
ALWAYS_INLINE inline void extractDigitPairs(uint64_t remainder, uint8_t * two_values)
{
    for (int i = 0; i < 9; ++i)
    {
        two_values[i] = uint8_t(remainder % 100);
        remainder /= 100;
    }
}

/// Write `count` digit pairs from `two_values` (in reverse order) to the output buffer.
ALWAYS_INLINE inline char * writeDigitPairs(char * p, const uint8_t * two_values, int count)
{
    for (int i = count - 1; i >= 0; --i)
    {
        outTwoDigits(p, two_values[i]);
        p += 2;
    }
    return p;
}

/// As zmij does: 16 digits at a time. `value` is at least 2^64, so the part above the last 16 digits is at least 1844.
NO_INLINE char * writeLargeUInt128Text(unsigned __int128 value, char * p)
{
    constexpr UInt64 ten_to_16 = 10000000000000000ULL;
    unsigned __int128 high = value / ten_to_16;
    /// In 64 bits: the remainder is below 10^16, and the high half of the product is not needed.
    UInt64 low = static_cast<UInt64>(value) - static_cast<UInt64>(high) * ten_to_16;
    if (likely(high < ten_to_16))
    {
        /// The head without leading zeros at `p`, the 16 digits of `low` right after it.
        UInt32 length = digitCount(static_cast<UInt64>(high));
        UInt8x32 text = textOfTwo(static_cast<UInt64>(high), low);
        writeLastDigits(lowHalf(text), length, uint64_tables.chunk_order, p);
        UInt8x16 tail_text = highHalf(chunksInReadingOrder(text));
        memcpy(p + length, &tail_text, 16);
        return p + length + 16;
    }

    /// `high` is below 2^75, and 10^16 is 2^16 * 5^16: the division by 5^16 of `high / 2^16` is a 64-bit one.
    UInt32 top = static_cast<UInt32>(static_cast<UInt64>(high >> 16) / 152587890625ULL);
    UInt64 middle = static_cast<UInt64>(high) - top * ten_to_16;
    p = writeUInt32Text(top, p);
    UInt8x32 text = textOfTwo(middle, low);
    text = chunksInReadingOrder(text);
    memcpy(p, &text, 32);
    return p + 32;
}

ALWAYS_INLINE inline char * writeUIntText(UInt128 x, char * p)
{
    /// Most values fit in 64 bits.
    if (likely(x.items[UInt128::_impl::little(1)] == 0))
        return writeUInt64Text(x.items[UInt128::_impl::little(0)], p);
    return writeLargeUInt128Text(
        (static_cast<unsigned __int128>(x.items[UInt128::_impl::little(1)]) << 64) | x.items[UInt128::_impl::little(0)], p);
}

/// Only values above the 128-bit range reach here. Out of line so the digit buffer, and the
/// `-fstack-protector-strong` canary that any local array forces, stay off the common path.
NO_INLINE char * writeUIntTextAbove128(const UInt256 & _x, char * p)
{
    /// Similar to writeUIntText(UInt128) only that in this case we will stop as soon as we reach the largest u128
    /// and switch to that function.
    uint8_t two_values[39] = {0}; // 78 Max characters / 2
    int current_pos = 0;

    UInt256 x = _x;
    /// The loop condition is `x > std::numeric_limits<UInt128>::max()`, spelled out on the limbs
    /// to avoid a full 256-bit comparison.
    while (x.items[UInt256::_impl::little(3)] != 0 || x.items[UInt256::_impl::little(2)] != 0)
    {
        uint64_t block = 0;
        x = divmod_1e18_256(x, block);
        extractDigitPairs(block, two_values + current_pos);
        current_pos += 9;
    }

    UInt128 pending{x.items[UInt256::_impl::little(0)], x.items[UInt256::_impl::little(1)]};

    char * out = writeUIntText(pending, p);
    return writeDigitPairs(out, two_values, current_pos);
}

/// By reference: by value a 256-bit integer is a `byval` aggregate holding an array, and that alone
/// puts a `-fstack-protector-strong` canary on the function.
ALWAYS_INLINE inline char * writeUIntText(const UInt256 & _x, char * p)
{
    /// If possible, treat it as a smaller integer as they are much faster to print
    if (likely(_x.items[UInt256::_impl::little(3)] == 0 && _x.items[UInt256::_impl::little(2)] == 0))
        return writeUIntText(UInt128{_x.items[UInt256::_impl::little(0)], _x.items[UInt256::_impl::little(1)]}, p);

    return writeUIntTextAbove128(_x, p);
}

/// Out of line: everything but the fast path needs a 256-bit local, which holds an array and so gets a
/// `-fstack-protector-strong` canary.
NO_INLINE char * writeSInt256TextSlow(const Int256 & x, char * pos)
{
    if (x < 0)
        return writeUIntText(UInt256(-x), writeLeadingMinus(pos));

    return writeUIntText(UInt256(x), pos);
}

ALWAYS_INLINE inline char * writeSIntText(const Int256 & x, char * pos)
{
    constexpr Int256 min_int = UInt256(1) << 255;
    if (unlikely(x == min_int))
    {
        const char * res = "-57896044618658097711785492504343953926634992332820282019728792003956564819968";
        memcpy(pos, res, strlen(res)); /// NOLINT(bugprone-not-null-terminated-result)
        return pos + strlen(res);
    }

    /// Clear top limbs mean the value fits into 128 bits, the common case. Reading through the
    /// reference avoids a copy, and a copy would cost a `-fstack-protector-strong` canary.
    if (likely(x.items[Int256::_impl::little(3)] == 0 && x.items[Int256::_impl::little(2)] == 0))
        return writeUIntText(UInt128{x.items[Int256::_impl::little(0)], x.items[Int256::_impl::little(1)]}, pos);

    return writeSInt256TextSlow(x, pos);
}
}

char * itoa(UInt128 i, char * p)
{
    return writeUIntText(i, p);
}

char * itoa(Int128 i, char * p)
{
    unsigned __int128 value = (static_cast<unsigned __int128>(static_cast<UInt64>(i.items[Int128::_impl::little(1)])) << 64)
        | static_cast<UInt64>(i.items[Int128::_impl::little(0)]);
    unsigned __int128 sign = static_cast<unsigned __int128>(static_cast<__int128>(value) >> 127);
    *p = '-';
    /// As for `Int32`.
    value = (value ^ sign) - sign;
    p += static_cast<UInt64>(sign) & 1;
    if (likely(value >> 64 == 0))
        return writeUInt64Text(static_cast<UInt64>(value), p);
    return writeLargeUInt128Text(value, p);
}

char * itoa(const UInt256 & i, char * p)
{
    return writeUIntText(i, p);
}

char * itoa(const Int256 & i, char * p)
{
    return writeSIntText(i, p);
}

/// ----------------------------------------------------------------------------------------------------
/// `writeFixedDigits`: the fractional part of `Decimal`
/// ----------------------------------------------------------------------------------------------------

namespace
{
char * writeFixedDigitsPortable(char * p, UInt64 value, UInt32 width)
{
    UInt32 rest = width;

    while (rest > 8)
    {
        writeEightFixedDigits(p + rest - 8, static_cast<uint32_t>(value % 100000000ULL));
        value /= 100000000ULL;
        rest -= 8;

        if (value == 0)
        {
            memset(p, '0', rest);
            return p + width;
        }
    }

    if (rest == 0)
        return p;

    if (rest <= 2)
    {
        if (rest == 2)
            outTwoDigits(p, static_cast<uint8_t>(value % 100));
        else
            *p = static_cast<char>('0' + value % 10);
        return p + width;
    }

    char tail[8];
    writeEightFixedDigits(tail, static_cast<uint32_t>(value % 100000000ULL));

    switch (rest)
    {
        case 8: memcpy(p, tail, 8); break;
        case 7: memcpy(p, tail + 1, 7); break;
        case 6: memcpy(p, tail + 2, 6); break;
        case 5: memcpy(p, tail + 3, 5); break;
        case 4: memcpy(p, tail + 4, 4); break;
        default: memcpy(p, tail + 5, 3); break;
    }
    return p + width;
}
}

char * writeFixedDigits(UInt64 value, UInt32 width, char * p)
{
    chassert(width <= std::numeric_limits<UInt256>::digits10);
#if defined(__x86_64__)
    if (has_avx512_ifma && width >= 1 && width <= 19)
        return avx512ifma::fixedDigits(p, value, width);
#endif
    return writeFixedDigitsPortable(p, value, width);
}

char * writeFixedDigits(UInt128 value, UInt32 width, char * p)
{
    char * const end = p + width;

    if (likely(value.items[UInt128::_impl::little(1)] == 0))
    {
        writeFixedDigits(UInt64(value.items[UInt128::_impl::little(0)]), width, p);
        return end;
    }

    using T = unsigned __int128;
    T x = (T(value.items[UInt128::_impl::little(1)]) << 64) + T(value.items[UInt128::_impl::little(0)]);

    while (width > 18)
    {
        uint64_t block = 0;
        x = divmod_1e18(x, block);
        writeFixedDigits(block, 18, p + width - 18);
        width -= 18;

        if (x == 0)
        {
            memset(p, '0', width);
            return end;
        }
    }

    constexpr T largest_uint64 = std::numeric_limits<uint64_t>::max();
    if (unlikely(x > largest_uint64))
        x %= max_multiple_of_hundred_that_fits_in_64_bits;

    writeFixedDigits(uint64_t(x), width, p);
    return end;
}

/// Only values above the 128-bit range reach here. Out of line: the running quotient is a local 256-bit
/// value, which holds an array and so gets a `-fstack-protector-strong` canary.
static NO_INLINE char * writeFixedDigitsAbove128(UInt256 value, UInt32 width, char * p)
{
    char * const end = p + width;

    while (width > 18)
    {
        uint64_t block = divmod_1e18(value);
        writeFixedDigits(block, 18, p + width - 18);
        width -= 18;

        if (value == 0)
        {
            memset(p, '0', width);
            return end;
        }

        if (value.items[UInt256::_impl::little(3)] == 0 && value.items[UInt256::_impl::little(2)] == 0)
        {
            writeFixedDigits(UInt128{value.items[UInt256::_impl::little(0)], value.items[UInt256::_impl::little(1)]}, width, p);
            return end;
        }
    }

    if (unlikely(value > UInt256(std::numeric_limits<uint64_t>::max())))
    {
        uint64_t block = divmod_1e18(value);
        writeFixedDigits(block, width, p);
        return end;
    }

    writeFixedDigits(static_cast<uint64_t>(value), width, p);
    return end;
}

char * writeFixedDigits(const UInt256 & value, UInt32 width, char * p)
{
    if (likely(value.items[UInt256::_impl::little(3)] == 0 && value.items[UInt256::_impl::little(2)] == 0))
    {
        writeFixedDigits(UInt128{value.items[UInt256::_impl::little(0)], value.items[UInt256::_impl::little(1)]}, width, p);
        return p + width;
    }

    return writeFixedDigitsAbove128(value, width, p);
}

/// ----------------------------------------------------------------------------------------------------
/// Test hooks
/// ----------------------------------------------------------------------------------------------------

void setUseAVX512ItoaForTests([[maybe_unused]] bool value)
{
#if defined(__x86_64__)
    has_avx512_ifma = value && detectAVX512IFMA();
#endif
}

bool getUseAVX512ItoaForTests()
{
#if defined(__x86_64__)
    return has_avx512_ifma;
#else
    return false;
#endif
}
