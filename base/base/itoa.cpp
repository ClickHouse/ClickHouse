#include <bit>
#include <cstring>
#include <type_traits>
#include <base/defines.h>
#include <base/extended_types.h>
#include <base/itoa.h>

#if defined(__x86_64__)
#    include <cpuid.h>
#    include <immintrin.h>
#endif

namespace
{
ALWAYS_INLINE inline char * outOneDigit(char * p, uint8_t value)
{
    *p = '0' + value;
    return p + 1;
}

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

namespace jeaiii
{
/*
    MIT License

    Copyright (c) 2022 James Edward Anhalt III - https://github.com/jeaiii/itoa

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/
struct pair
{
    char dd[2];
    constexpr pair(char c) : dd{c, '\0'} { } /// NOLINT(google-explicit-constructor)
    constexpr pair(int n) : dd{"0123456789"[n / 10], "0123456789"[n % 10]} { } /// NOLINT(google-explicit-constructor)
};

constexpr struct
{
    pair dd[100]{
        0,  1,  2,  3,  4,  5,  6,  7,  8,  9, //
        10, 11, 12, 13, 14, 15, 16, 17, 18, 19, //
        20, 21, 22, 23, 24, 25, 26, 27, 28, 29, //
        30, 31, 32, 33, 34, 35, 36, 37, 38, 39, //
        40, 41, 42, 43, 44, 45, 46, 47, 48, 49, //
        50, 51, 52, 53, 54, 55, 56, 57, 58, 59, //
        60, 61, 62, 63, 64, 65, 66, 67, 68, 69, //
        70, 71, 72, 73, 74, 75, 76, 77, 78, 79, //
        80, 81, 82, 83, 84, 85, 86, 87, 88, 89, //
        90, 91, 92, 93, 94, 95, 96, 97, 98, 99, //
    };
    pair fd[100]{
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', //
        10,  11,  12,  13,  14,  15,  16,  17,  18,  19, //
        20,  21,  22,  23,  24,  25,  26,  27,  28,  29, //
        30,  31,  32,  33,  34,  35,  36,  37,  38,  39, //
        40,  41,  42,  43,  44,  45,  46,  47,  48,  49, //
        50,  51,  52,  53,  54,  55,  56,  57,  58,  59, //
        60,  61,  62,  63,  64,  65,  66,  67,  68,  69, //
        70,  71,  72,  73,  74,  75,  76,  77,  78,  79, //
        80,  81,  82,  83,  84,  85,  86,  87,  88,  89, //
        90,  91,  92,  93,  94,  95,  96,  97,  98,  99, //
    };
} digits;

constexpr UInt64 mask24 = (UInt64(1) << 24) - 1;
constexpr UInt64 mask32 = (UInt64(1) << 32) - 1;
constexpr UInt64 mask57 = (UInt64(1) << 57) - 1;

template <bool, class, class F>
struct _cond
{
    using type = F;
};
template <class T, class F>
struct _cond<true, T, F>
{
    using type = T;
};
template <bool B, class T, class F>
using cond = typename _cond<B, T, F>::type;

template <class T>
inline ALWAYS_INLINE char * to_text_from_integer(char * b, T i)
{
    constexpr auto q = sizeof(T);
    using U = cond<q == 1, char8_t, cond<q <= sizeof(UInt16), UInt16, cond<q <= sizeof(UInt32), UInt32, UInt64>>>;

    // convert bool to int before test with unary + to silence warning if T happens to be bool
    U const n = +i < 0 ? *b++ = '-', U(0) - U(i) : U(i);

    if (n < U(1e2))
    {
        /// This is changed from the original jeaiii implementation
        /// For small numbers the extra branch to call outOneDigit() is worth it as it saves some instructions
        /// and a memory access (no need to read digits.fd[n])
        /// This is not true for pure random numbers, but that's not the common use case of a database
        /// Original jeaii code
        //      *reinterpret_cast<pair *>(b) = digits.fd[n];
        //      return n < 10 ? b + 1 : b + 2;
        return n < 10 ? outOneDigit(b, static_cast<uint8_t>(n)) : outTwoDigits(b, static_cast<uint8_t>(n));
    }
    if (n < UInt32(1e6))
    {
        if (sizeof(U) == 1 || n < U(1e4))
        {
            auto f0 = UInt32(10 * (1 << 24) / 1e3 + 1) * n;
            *reinterpret_cast<pair *>(b) = digits.fd[f0 >> 24];
            if constexpr (sizeof(U) == 1)
                b -= 1;
            else
                b -= n < U(1e3);
            auto f2 = (f0 & mask24) * 100;
            *reinterpret_cast<pair *>(b + 2) = digits.dd[f2 >> 24];
            return b + 4;
        }
        auto f0 = UInt64(10 * (1ull << 32ull) / 1e5 + 1) * n;
        *reinterpret_cast<pair *>(b) = digits.fd[f0 >> 32];
        if constexpr (sizeof(U) == 2)
            b -= 1;
        else
            b -= n < U(1e5);
        auto f2 = (f0 & mask32) * 100;
        *reinterpret_cast<pair *>(b + 2) = digits.dd[f2 >> 32];
        auto f4 = (f2 & mask32) * 100;
        *reinterpret_cast<pair *>(b + 4) = digits.dd[f4 >> 32];
        return b + 6;
    }
    if (sizeof(U) == 4 || n < UInt64(1ull << 32ull))
    {
        if (n < U(1e8))
        {
            auto f0 = UInt64(10 * (1ull << 48ull) / 1e7 + 1) * n >> 16;
            *reinterpret_cast<pair *>(b) = digits.fd[f0 >> 32];
            b -= n < U(1e7);
            auto f2 = (f0 & mask32) * 100;
            *reinterpret_cast<pair *>(b + 2) = digits.dd[f2 >> 32];
            auto f4 = (f2 & mask32) * 100;
            *reinterpret_cast<pair *>(b + 4) = digits.dd[f4 >> 32];
            auto f6 = (f4 & mask32) * 100;
            *reinterpret_cast<pair *>(b + 6) = digits.dd[f6 >> 32];
            return b + 8;
        }
        auto f0 = UInt64(10 * (1ull << 57ull) / 1e9 + 1) * n;
        *reinterpret_cast<pair *>(b) = digits.fd[f0 >> 57];
        b -= n < UInt32(1e9);
        auto f2 = (f0 & mask57) * 100;
        *reinterpret_cast<pair *>(b + 2) = digits.dd[f2 >> 57];
        auto f4 = (f2 & mask57) * 100;
        *reinterpret_cast<pair *>(b + 4) = digits.dd[f4 >> 57];
        auto f6 = (f4 & mask57) * 100;
        *reinterpret_cast<pair *>(b + 6) = digits.dd[f6 >> 57];
        auto f8 = (f6 & mask57) * 100;
        *reinterpret_cast<pair *>(b + 8) = digits.dd[f8 >> 57];
        return b + 10;
    }

    // if we get here U must be UInt64 but some compilers don't know that, so reassign n to a UInt64 to avoid warnings
    UInt32 z = n % UInt32(1e8);
    UInt64 u = n / UInt32(1e8);

    if (u < UInt32(1e2))
    {
        // u can't be 1 digit (if u < 10 it would have been handled above as a 9 digit 32bit number)
        *reinterpret_cast<pair *>(b) = digits.dd[u];
        b += 2;
    }
    else if (u < UInt32(1e6))
    {
        if (u < UInt32(1e4))
        {
            auto f0 = UInt32(10 * (1 << 24) / 1e3 + 1) * u;
            *reinterpret_cast<pair *>(b) = digits.fd[f0 >> 24];
            b -= u < UInt32(1e3);
            auto f2 = (f0 & mask24) * 100;
            *reinterpret_cast<pair *>(b + 2) = digits.dd[f2 >> 24];
            b += 4;
        }
        else
        {
            auto f0 = UInt64(10 * (1ull << 32ull) / 1e5 + 1) * u;
            *reinterpret_cast<pair *>(b) = digits.fd[f0 >> 32];
            b -= u < UInt32(1e5);
            auto f2 = (f0 & mask32) * 100;
            *reinterpret_cast<pair *>(b + 2) = digits.dd[f2 >> 32];
            auto f4 = (f2 & mask32) * 100;
            *reinterpret_cast<pair *>(b + 4) = digits.dd[f4 >> 32];
            b += 6;
        }
    }
    else if (u < UInt32(1e8))
    {
        auto f0 = UInt64(10 * (1ull << 48ull) / 1e7 + 1) * u >> 16;
        *reinterpret_cast<pair *>(b) = digits.fd[f0 >> 32];
        b -= u < UInt32(1e7);
        auto f2 = (f0 & mask32) * 100;
        *reinterpret_cast<pair *>(b + 2) = digits.dd[f2 >> 32];
        auto f4 = (f2 & mask32) * 100;
        *reinterpret_cast<pair *>(b + 4) = digits.dd[f4 >> 32];
        auto f6 = (f4 & mask32) * 100;
        *reinterpret_cast<pair *>(b + 6) = digits.dd[f6 >> 32];
        b += 8;
    }
    else if (u < UInt64(1ull << 32ull))
    {
        auto f0 = UInt64(10 * (1ull << 57ull) / 1e9 + 1) * u;
        *reinterpret_cast<pair *>(b) = digits.fd[f0 >> 57];
        b -= u < UInt32(1e9);
        auto f2 = (f0 & mask57) * 100;
        *reinterpret_cast<pair *>(b + 2) = digits.dd[f2 >> 57];
        auto f4 = (f2 & mask57) * 100;
        *reinterpret_cast<pair *>(b + 4) = digits.dd[f4 >> 57];
        auto f6 = (f4 & mask57) * 100;
        *reinterpret_cast<pair *>(b + 6) = digits.dd[f6 >> 57];
        auto f8 = (f6 & mask57) * 100;
        *reinterpret_cast<pair *>(b + 8) = digits.dd[f8 >> 57];
        b += 10;
    }
    else
    {
        UInt32 y = u % UInt32(1e8);
        u /= UInt32(1e8);

        // u is 2, 3, or 4 digits (if u < 10 it would have been handled above)
        if (u < UInt32(1e2))
        {
            *reinterpret_cast<pair *>(b) = digits.dd[u];
            b += 2;
        }
        else
        {
            auto f0 = UInt32(10 * (1 << 24) / 1e3 + 1) * u;
            *reinterpret_cast<pair *>(b) = digits.fd[f0 >> 24];
            b -= u < UInt32(1e3);
            auto f2 = (f0 & mask24) * 100;
            *reinterpret_cast<pair *>(b + 2) = digits.dd[f2 >> 24];
            b += 4;
        }
        // do 8 digits
        auto f0 = (UInt64((1ull << 48ull) / 1e6 + 1) * y >> 16) + 1;
        *reinterpret_cast<pair *>(b) = digits.dd[f0 >> 32];
        auto f2 = (f0 & mask32) * 100;
        *reinterpret_cast<pair *>(b + 2) = digits.dd[f2 >> 32];
        auto f4 = (f2 & mask32) * 100;
        *reinterpret_cast<pair *>(b + 4) = digits.dd[f4 >> 32];
        auto f6 = (f4 & mask32) * 100;
        *reinterpret_cast<pair *>(b + 6) = digits.dd[f6 >> 32];
        b += 8;
    }
    // do 8 digits
    auto f0 = (UInt64((1ull << 48ull) / 1e6 + 1) * z >> 16) + 1;
    *reinterpret_cast<pair *>(b) = digits.dd[f0 >> 32];
    auto f2 = (f0 & mask32) * 100;
    *reinterpret_cast<pair *>(b + 2) = digits.dd[f2 >> 32];
    auto f4 = (f2 & mask32) * 100;
    *reinterpret_cast<pair *>(b + 4) = digits.dd[f4 >> 32];
    auto f6 = (f4 & mask32) * 100;
    *reinterpret_cast<pair *>(b + 6) = digits.dd[f6 >> 32];
    return b + 8;
}
}

ALWAYS_INLINE inline void writeEightFixedDigits(char * p, uint32_t z)
{
    auto f0 = (UInt64((1ull << 48ull) / 1e6 + 1) * z >> 16) + 1;
    outTwoDigits(p, static_cast<uint8_t>(f0 >> 32));
    auto f2 = (f0 & jeaiii::mask32) * 100;
    outTwoDigits(p + 2, static_cast<uint8_t>(f2 >> 32));
    auto f4 = (f2 & jeaiii::mask32) * 100;
    outTwoDigits(p + 4, static_cast<uint8_t>(f4 >> 32));
    auto f6 = (f4 & jeaiii::mask32) * 100;
    outTwoDigits(p + 6, static_cast<uint8_t>(f6 >> 32));
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

ALWAYS_INLINE inline UInt32 digitCount(UInt64 x)
{
    static constexpr uint8_t digit_count[65]
        = {19, 19, 19, 19, 18, 18, 18, 17, 17, 17, 16, 16, 16, 16, 15, 15, 15, 14, 14, 14, 13, 13,
           13, 13, 12, 12, 12, 11, 11, 11, 10, 10, 10, 10, 9,  9,  9,  8,  8,  8,  7,  7,  7,  7,
           6,  6,  6,  5,  5,  5,  4,  4,  4,  4,  3,  3,  3,  2,  2,  2,  1,  1,  1,  1,  1};
    static constexpr UInt64 lower_bound[65]
        = {9999999999999999999ULL, 9999999999999999999ULL, 9999999999999999999ULL, 9999999999999999999ULL,
           999999999999999999ULL,  999999999999999999ULL,  999999999999999999ULL,  99999999999999999ULL,
           99999999999999999ULL,   99999999999999999ULL,   9999999999999999ULL,    9999999999999999ULL,
           9999999999999999ULL,    9999999999999999ULL,    999999999999999ULL,     999999999999999ULL,
           999999999999999ULL,     99999999999999ULL,      99999999999999ULL,      99999999999999ULL,
           9999999999999ULL,       9999999999999ULL,       9999999999999ULL,       9999999999999ULL,
           999999999999ULL,        999999999999ULL,        999999999999ULL,        99999999999ULL,
           99999999999ULL,         99999999999ULL,         9999999999ULL,          9999999999ULL,
           9999999999ULL,          9999999999ULL,          999999999ULL,           999999999ULL,
           999999999ULL,           99999999ULL,            99999999ULL,            99999999ULL,
           9999999ULL,             9999999ULL,             9999999ULL,             9999999ULL,
           999999ULL,              999999ULL,              999999ULL,              99999ULL,
           99999ULL,               99999ULL,               9999ULL,                9999ULL,
           9999ULL,                9999ULL,                999ULL,                 999ULL,
           999ULL,                 99ULL,                  99ULL,                  99ULL,
           9ULL,                   9ULL,                   9ULL,                   9ULL,
           0ULL};

    int leading_zeros = std::countl_zero(x);
    return static_cast<UInt32>(x > lower_bound[leading_zeros]) + digit_count[leading_zeros];
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

ITOA_IFMA_TARGET char * toChars(char * p, UInt64 value)
{
    if (value < 100000000ULL)
    {
        const __m512i digits_of_eight = digitsOfEight(value);
        const UInt32 n = digitCount(value);
        storeDigitsOfEight(p, n, digits_of_eight);
        return p + n;
    }

    if (unlikely(value >= 10000000000000000ULL))
    {
        const UInt32 n = digitCount(value);
        const UInt64 quotient = value / 10000;
        const UInt32 quotient_digits = n - 4;
        _mm_mask_storeu_epi8(
            shiftedPointer(p, static_cast<Int32>(quotient_digits) - 16),
            static_cast<__mmask16>(0xffffu << (16 - quotient_digits)),
            digitsOfSixteen(quotient));
        auto remainder = static_cast<uint32_t>(value - quotient * 10000);
        outTwoDigits(p + quotient_digits, static_cast<uint8_t>(remainder / 100));
        outTwoDigits(p + quotient_digits + 2, static_cast<uint8_t>(remainder % 100));
        return p + n;
    }

    const __m128i digits_of_sixteen = digitsOfSixteen(value);
    const UInt32 n = digitCount(value);
    _mm_mask_storeu_epi8(
        shiftedPointer(p, static_cast<Int32>(n) - 16), static_cast<__mmask16>(0xffffu << (16 - n)), digits_of_sixteen);
    return p + n;
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

ALWAYS_INLINE inline char * writeUInt64Text(UInt64 value, char * p)
{
#if defined(__x86_64__)
    if (has_avx512_ifma)
        return avx512ifma::toChars(p, value);
#endif
    return jeaiii::to_text_from_integer(p, value);
}

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

ALWAYS_INLINE inline char * writeEighteenFixedDigits(char * p, UInt64 value)
{
    writeEightFixedDigits(p + 10, static_cast<uint32_t>(value % 100000000ULL));
    value /= 100000000ULL;
    writeEightFixedDigits(p + 2, static_cast<uint32_t>(value % 100000000ULL));
    outTwoDigits(p, static_cast<uint8_t>(value / 100000000ULL));
    return p + 18;
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

ALWAYS_INLINE inline char * writeUIntText(UInt128 _x, char * p)
{
    /// If the highest 64-bit item is empty, we can print just the lowest item as u64.
    /// Even though technically there are more numbers in the range where this isn't true, in real-life data this isn't the case
    if (likely(_x.items[UInt128::_impl::little(1)] == 0))
        return writeUInt64Text(_x.items[UInt128::_impl::little(0)], p);

    /// Doing operations using __int128 is faster and we already rely on this feature.
    using T = unsigned __int128;
    T x = (T(_x.items[UInt128::_impl::little(1)]) << 64) + T(_x.items[UInt128::_impl::little(0)]);

    /// Split into blocks of up to 18 digits (10^18 per block) using Barrett reduction.
    /// UInt128 max is ~3.4e38, so at most 2 divisions are needed.
    /// Unrolled: first division always needed (x > uint64 max since high item != 0),
    /// second division only if quotient still exceeds uint64 max.
    uint64_t low_block = 0;
    x = divmod_1e18(x, low_block);

    constexpr T largest_uint64 = std::numeric_limits<uint64_t>::max();
    if (unlikely(x > largest_uint64))
    {
        uint64_t middle_block = 0;
        x = divmod_1e18(x, middle_block);

        char * out = writeUInt64Text(uint64_t(x), p);
        out = writeEighteenFixedDigits(out, middle_block);
        return writeEighteenFixedDigits(out, low_block);
    }

    char * out = writeUInt64Text(uint64_t(x), p);
    return writeEighteenFixedDigits(out, low_block);
}

ALWAYS_INLINE inline char * writeUIntText(UInt256 _x, char * p)
{
    /// If possible, treat it as a smaller integer as they are much faster to print
    if (likely(_x.items[UInt256::_impl::little(3)] == 0 && _x.items[UInt256::_impl::little(2)] == 0))
        return writeUIntText(UInt128{_x.items[UInt256::_impl::little(0)], _x.items[UInt256::_impl::little(1)]}, p);

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

ALWAYS_INLINE inline char * writeLeadingMinus(char * pos)
{
    *pos = '-';
    return pos + 1;
}

template <typename T>
ALWAYS_INLINE inline char * writeSIntText(T x, char * pos)
{
    static_assert(std::is_same_v<T, Int128> || std::is_same_v<T, Int256>);

    using UnsignedT = make_unsigned_t<T>;
    constexpr T min_int = UnsignedT(1) << (sizeof(T) * 8 - 1);

    if (unlikely(x == min_int))
    {
        if constexpr (std::is_same_v<T, Int128>)
        {
            const char * res = "-170141183460469231731687303715884105728";
            memcpy(pos, res, strlen(res)); /// NOLINT(bugprone-not-null-terminated-result)
            return pos + strlen(res);
        }
        else if constexpr (std::is_same_v<T, Int256>)
        {
            const char * res = "-57896044618658097711785492504343953926634992332820282019728792003956564819968";
            memcpy(pos, res, strlen(res)); /// NOLINT(bugprone-not-null-terminated-result)
            return pos + strlen(res);
        }
    }

    if (x < 0)
    {
        x = -x;
        pos = writeLeadingMinus(pos);
    }
    return writeUIntText(UnsignedT(x), pos);
}
}

char * itoa(UInt8 i, char * p)
{
    return jeaiii::to_text_from_integer(p, uint8_t(i));
}

char * itoa(Int8 i, char * p)
{
    return jeaiii::to_text_from_integer(p, int8_t(i));
}

char * itoa(UInt128 i, char * p)
{
    return writeUIntText(i, p);
}

char * itoa(Int128 i, char * p)
{
    return writeSIntText(i, p);
}

char * itoa(UInt256 i, char * p)
{
    return writeUIntText(i, p);
}

char * itoa(Int256 i, char * p)
{
    return writeSIntText(i, p);
}

#define DEFAULT_ITOA(T) \
    char * itoa(T i, char * p) \
    { \
        return jeaiii::to_text_from_integer(p, i); \
    }

#define FOR_MISSING_INTEGER_TYPES(M) \
    M(uint8_t) \
    M(UInt16) \
    M(UInt32) \
    M(int8_t) \
    M(Int16) \
    M(Int32)

FOR_MISSING_INTEGER_TYPES(DEFAULT_ITOA)

/// `long` is not covered by the list above where it is a distinct type.
#if defined(LONG_IS_A_DISTINCT_TYPE)
DEFAULT_ITOA(unsigned long)
DEFAULT_ITOA(long)
#endif

#undef FOR_MISSING_INTEGER_TYPES
#undef DEFAULT_ITOA

char * itoa(UInt64 i, char * p)
{
    return writeUInt64Text(i, p);
}

char * itoa(Int64 i, char * p)
{
    if (i < 0)
        return writeUInt64Text(0 - static_cast<UInt64>(i), writeLeadingMinus(p));
    return writeUInt64Text(static_cast<UInt64>(i), p);
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

char * writeFixedDigits(UInt256 value, UInt32 width, char * p)
{
    char * const end = p + width;

    if (likely(value.items[UInt256::_impl::little(3)] == 0 && value.items[UInt256::_impl::little(2)] == 0))
    {
        writeFixedDigits(UInt128{value.items[UInt256::_impl::little(0)], value.items[UInt256::_impl::little(1)]}, width, p);
        return end;
    }

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
