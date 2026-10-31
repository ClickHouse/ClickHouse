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

/// Not instrumented: the XRay entry and exit sleds are a measurable part of calls this short.
#pragma clang attribute push([[clang::xray_never_instrument]], apply_to = function)

using namespace itoa_impl;

/// ----------------------------------------------------------------------------------------------------
/// 8 bits
/// ----------------------------------------------------------------------------------------------------

constexpr std::array<UInt32, 256> one_byte_to_string_lookup_table = []
{
    std::array<UInt32, 256> table{};
    for (UInt32 value = 0; value < 256; ++value)
    {
        UInt32 length = value >= 100 ? 3 : (value >= 10 ? 2 : 1);
        UInt32 entry = length << itoa_impl::byteShift<UInt32>(3);
        for (UInt32 i = length, rest = value; i > 0; --i, rest /= 10)
            entry |= ('0' + rest % 10) << itoa_impl::byteShift<UInt32>(i - 1);
        table[value] = entry;
    }
    return table;
}();

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
}

/// ----------------------------------------------------------------------------------------------------
/// 64 bits
/// Where the CPU has AVX-512 IFMA, it writes the values above 2^32 of up to 16 digits, and serves `writeFixedDigits`.
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

/// `value` is at least 2^32, with `length` digits, at most 16: one `vpmadd52` sequence and one masked store, fewer
/// micro-ops than the AVX2 code of `writeLargeUInt64Text`. Longer values need another division and more stores, and
/// are slower than the AVX2 code on Zen 4.
ITOA_IFMA_TARGET char * toChars(UInt64 value, char * p, UInt32 length)
{
    _mm_mask_storeu_epi8(
        shiftedPointer(p, static_cast<Int32>(length) - 16), static_cast<__mmask16>(0xffffu << (16 - length)), digitsOfSixteen(value));
    return p + length;
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
    DigitGroups result{};
    if constexpr (std::endian::native == std::endian::little)
        result.groups = __builtin_shufflevector(std::bit_cast<UInt32x4>(first), std::bit_cast<UInt32x4>(second), 1, 3, 5, 7);
    else
        result.groups = __builtin_shufflevector(std::bit_cast<UInt32x4>(first), std::bit_cast<UInt32x4>(second), 0, 2, 4, 6);
    result.top = static_cast<UInt32>(high_half >> 32);
    return result;
}

/// Groups of 4 digits to their characters, each lane with the first in the highest byte, whatever the endianness.
/// 32-byte vectors use `__builtin_bit_cast`: `std::bit_cast` is a call that returns them in AVX registers, an ABI change without AVX.
ALWAYS_INLINE inline UInt8x32 textOfGroups(UInt32x8 groups)
{
    using UInt16x16 = UInt16 __attribute__((vector_size(32)));
    using UInt32x16 = UInt32 __attribute__((vector_size(64)));
    UInt32x8 pairs = groups + ((groups * 5243) >> 19) * (65536 - 100);
    UInt16x16 pair_lanes = __builtin_bit_cast(UInt16x16, pairs);
    UInt16x16 tens = __builtin_convertvector((__builtin_convertvector(pair_lanes, UInt32x16) * 6554) >> 16, UInt16x16);
    return __builtin_bit_cast(UInt8x32, pair_lanes + 0x3030 + tens * UInt16(256 - 10));
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

}

/// The first 4 digits, the fifth lane, are written with one 4-byte store, then the other 16 with one 16-byte store over
/// the part of those 4 bytes that is not digits.
NO_INLINE char * itoa_impl::writeLargeUInt64Text(UInt64 value, char * p)
{
    UInt32 length = digitCount(value);
#if defined(__x86_64__)
    if (has_avx512_ifma && length <= 16)
        return avx512ifma::toChars(value, p, length);
#endif
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

namespace
{
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
        halves = __builtin_bit_cast(UInt64x4, UInt32x8{head_first, 0, head_last, 0, tail_first, 0, tail_last, 0});
    else
        halves = __builtin_bit_cast(UInt64x4, UInt32x8{0, head_first, 0, head_last, 0, tail_first, 0, tail_last});
    /// The last 4 digits of each half in its low 32 bits, the first 4 in its high 32 bits. Not `halves / 10000`: clang
    /// divides 64-bit lanes as if they could be any 64-bit value, with two multiplications. A half is below 10^8, and
    /// for that n * ceil(2^40 / 10^4) / 2^40 is n / 10^4, as zmij does.
    constexpr UInt64 reciprocal = (UInt64(1) << 40) / 10000 + 1;
    UInt64x4 groups = halves + ((halves * reciprocal) >> 40) * ((UInt64(1) << 32) - 10000);
    return textOfGroups(__builtin_bit_cast(UInt32x8, groups));
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

/// Exactly 16 digits of `value` below 10^16, with leading zeros.
ALWAYS_INLINE inline void writeSixteenDigits(UInt64 value, char * p)
{
    UInt8x16 text = lowHalf(chunksInReadingOrder(textOfTwo(value, 0)));
    memcpy(p, &text, 16);
}

/// Exactly 32 digits: those of `head` then those of `tail`, both below 10^16, with leading zeros.
ALWAYS_INLINE inline void writeThirtyTwoDigits(UInt64 head, UInt64 tail, char * p)
{
    UInt8x32 text = chunksInReadingOrder(textOfTwo(head, tail));
    memcpy(p, &text, 32);
}

/// `head` without leading zeros, then the 16 digits of `tail`, both below 10^16, converted in the two halves of one vector.
ALWAYS_INLINE inline char * writeHeadAndSixteenDigits(UInt64 head, UInt64 tail, char * p)
{
    UInt32 length = digitCount(head);
    UInt8x32 text = textOfTwo(head, tail);
    writeLastDigits(lowHalf(text), length, uint64_tables.chunk_order, p);
    UInt8x16 tail_text = highHalf(chunksInReadingOrder(text));
    memcpy(p + length, &tail_text, 16);
    return p + length + 16;
}

}

/// ----------------------------------------------------------------------------------------------------
/// 128 and 256 bits
/// They share the chunks of 16 digits.
/// ----------------------------------------------------------------------------------------------------

namespace
{
/// The high 128 bits of the product of `a` and `b`, from four 64 x 64 bit multiplications.
ALWAYS_INLINE inline unsigned __int128 multiplyHigh(unsigned __int128 a, unsigned __int128 b)
{
    unsigned __int128 low_low = static_cast<unsigned __int128>(static_cast<UInt64>(a)) * static_cast<UInt64>(b);
    unsigned __int128 low_high = static_cast<unsigned __int128>(static_cast<UInt64>(a)) * static_cast<UInt64>(b >> 64);
    unsigned __int128 high_low = static_cast<unsigned __int128>(static_cast<UInt64>(a >> 64)) * static_cast<UInt64>(b);
    unsigned __int128 high_high = static_cast<unsigned __int128>(static_cast<UInt64>(a >> 64)) * static_cast<UInt64>(b >> 64);
    unsigned __int128 middle = (low_low >> 64) + static_cast<UInt64>(low_high) + static_cast<UInt64>(high_low);
    return high_high + (low_high >> 64) + (high_low >> 64) + (middle >> 64);
}

/// The divisions of 128 bits by a constant are written as the multiplications by the reciprocal that clang 23 emits for
/// them: clang 22 calls `__udivti3` instead, which is several times slower. The reciprocals are rounded up, and exact
/// for every value in the range given (Granlund and Montgomery's condition).

/// Any 128-bit `value` divided by 10^16: the high half of the product by ceil(2^179 / 10^16), as zmij's `divmod_1e16`.
ALWAYS_INLINE inline unsigned __int128 divideByTenToSixteen(unsigned __int128 value)
{
    constexpr unsigned __int128 reciprocal = (static_cast<unsigned __int128>(0x39a5652fb1137856ULL) << 64) | 0xd30baf9a1e626a6dULL;
    return multiplyHigh(value, reciprocal) >> 51;
}

/// `value` below 2^118 divided by 5^32: the high half of the product by ceil(2^192 / 5^32), shifted by 64.
ALWAYS_INLINE inline UInt64 divideByFiveToThirtyTwo(unsigned __int128 value)
{
    constexpr unsigned __int128 reciprocal = (static_cast<unsigned __int128>(0x0033ec47ab514e65ULL) << 64) | 0x2e99f7863b696053ULL;
    return static_cast<UInt64>(multiplyHigh(value, reciprocal) >> 64);
}
}

/// As zmij does: 16 digits at a time. `value` is at least 2^64, so the part above the last 16 digits is at least 1844.
NO_INLINE char * itoa_impl::writeLargeUInt128Text(unsigned __int128 value, char * p)
{
    constexpr UInt64 ten_to_16 = 10000000000000000ULL;
    unsigned __int128 high = divideByTenToSixteen(value);
    /// In 64 bits: the remainder is below 10^16, and the high half of the product is not needed.
    UInt64 low = static_cast<UInt64>(value) - static_cast<UInt64>(high) * ten_to_16;
    if (likely(high < ten_to_16))
        return writeHeadAndSixteenDigits(static_cast<UInt64>(high), low, p);

    /// `high` is below 2^75, and 10^16 is 2^16 * 5^16: the division by 5^16 of `high / 2^16` is a 64-bit one.
    UInt32 top = static_cast<UInt32>(static_cast<UInt64>(high >> 16) / 152587890625ULL);
    UInt64 middle = static_cast<UInt64>(high) - top * ten_to_16;
    p = writeUInt32Text(top, p);
    writeThirtyTwoDigits(middle, low, p);
    return p + 32;
}

namespace
{
constexpr unsigned __int128 ten_to_32 = static_cast<unsigned __int128>(10000000000000000ULL) * 10000000000000000ULL;

/// `high` and `low` divided by 10^32, as for 128 bits: the product by ceil(2^362 / 10^32), from bit 362, which is exact
/// for every 256-bit value. Clang divides 256 bits by a constant with a loop over the bits.
ALWAYS_INLINE inline void divideByTenToThirtyTwo(unsigned __int128 high, unsigned __int128 low, unsigned __int128 & quotient_high, unsigned __int128 & quotient_low)
{
    constexpr UInt64 reciprocal[4] = {0x59e338e387ad8e29ULL, 0x0b5b1aa028ccd99eULL, 0x67de18eda5814af2ULL, 0xcfb11ead453994baULL};
    const UInt64 value[4] = {static_cast<UInt64>(low), static_cast<UInt64>(low >> 64), static_cast<UInt64>(high), static_cast<UInt64>(high >> 64)};
    UInt64 product[8] = {};
    for (size_t i = 0; i < 4; ++i)
    {
        UInt64 carry = 0;
        for (size_t j = 0; j < 4; ++j)
        {
            unsigned __int128 term = static_cast<unsigned __int128>(value[i]) * reciprocal[j] + product[i + j] + carry;
            product[i + j] = static_cast<UInt64>(term);
            carry = static_cast<UInt64>(term >> 64);
        }
        product[i + 4] = carry;
    }
    quotient_high = product[7] >> 42;
    quotient_low = (static_cast<unsigned __int128>(product[7]) << 86) | (static_cast<unsigned __int128>(product[6]) << 22) | (product[5] >> 42);
}

}

/// The value in `high` and `low` is at least 2^128. One division by 10^32 leaves a remainder of 32 digits and a quotient
/// below 2^150. When the quotient does not fit in 128 bits, a second division by 10^32 splits it into at most 14 digits
/// and 32 more.
NO_INLINE char * itoa_impl::writeLargeUInt256Text(unsigned __int128 high, unsigned __int128 low, char * p)
{
    unsigned __int128 quotient_high = 0;
    unsigned __int128 quotient_low = 0;
    divideByTenToThirtyTwo(high, low, quotient_high, quotient_low);

    /// In 128 bits: the remainder is below 10^32, and the high half of the product is not needed.
    unsigned __int128 rest = low - quotient_low * ten_to_32;
    constexpr UInt64 ten_to_16 = 10000000000000000ULL;
    UInt64 middle = static_cast<UInt64>(divideByTenToSixteen(rest));
    UInt64 last = static_cast<UInt64>(rest) - middle * ten_to_16;

    if (quotient_high == 0)
    {
        p = writeUInt128Text(quotient_low, p);
        writeThirtyTwoDigits(middle, last, p);
        return p + 32;
    }

    /// The quotient is below 2^150, and 10^32 is 2^32 * 5^32: the division by 5^32 of the quotient / 2^32 is one of
    /// 128 bits.
    unsigned __int128 quotient = (quotient_high << 96) | (quotient_low >> 32);
    UInt64 top = divideByFiveToThirtyTwo(quotient);
    unsigned __int128 next = quotient_low - top * ten_to_32;
    UInt64 second = static_cast<UInt64>(divideByTenToSixteen(next));
    UInt64 third = static_cast<UInt64>(next) - second * ten_to_16;

    p = writeHeadAndSixteenDigits(top, second, p);
    writeThirtyTwoDigits(third, middle, p);
    writeSixteenDigits(last, p + 32);
    return p + 48;
}

/// ----------------------------------------------------------------------------------------------------
/// `writeFixedDigits`: the fractional part of `Decimal`
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

#pragma clang attribute pop
