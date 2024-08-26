// Based on https://github.com/amdn/itoa and combined with our optimizations
//
//=== itoa.cpp - Fast integer to ascii conversion                 --*- C++ -*-//
//
// The MIT License (MIT)
// Copyright (c) 2016 Arturo Martin-de-Nicolas
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
//     The above copyright notice and this permission notice shall be included
//     in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//===----------------------------------------------------------------------===//

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>
#include <base/defines.h>
#include <base/extended_types.h>
#include <base/itoa.h>

namespace
{
template <typename T>
ALWAYS_INLINE inline constexpr T pow10(size_t x)
{
    return x ? 10 * pow10<T>(x - 1) : 1;
}

// Division by a power of 10 is implemented using a multiplicative inverse.
// This strength reduction is also done by optimizing compilers, but
// presently the fastest results are produced by using the values
// for the multiplication and the shift as given by the algorithm
// described by Agner Fog in "Optimizing Subroutines in Assembly Language"
//
// http://www.agner.org/optimize/optimizing_assembly.pdf
//
// "Integer division by a constant (all processors)
// A floating point number can be divided by a constant by multiplying
// with the reciprocal. If we want to do the same with integers, we have
// to scale the reciprocal by 2n and then shift the product to the right
// by n. There are various algorithms for finding a suitable value of n
// and compensating for rounding errors. The algorithm described below
// was invented by Terje Mathisen, Norway, and not published elsewhere."

/// Division by constant is performed by:
/// 1. Adding 1 if needed;
/// 2. Multiplying by another constant;
/// 3. Shifting right by another constant.
template <typename UInt, bool add_, UInt multiplier_, unsigned shift_>
struct Division
{
    static constexpr bool add{add_};
    static constexpr UInt multiplier{multiplier_};
    static constexpr unsigned shift{shift_};
};

/// Select a type with appropriate number of bytes from the list of types.
/// First parameter is the number of bytes requested. Then goes a list of types with 1, 2, 4, ... number of bytes.
/// Example: SelectType<4, uint8_t, uint16_t, uint32_t, uint64_t> will select uint32_t.
template <size_t N, typename T, typename... Ts>
struct SelectType
{
    using Result = typename SelectType<N / 2, Ts...>::Result;
};

template <typename T, typename... Ts>
struct SelectType<1, T, Ts...>
{
    using Result = T;
};


/// Division by 10^N where N is the size of the type.
template <size_t N>
using DivisionBy10PowN = typename SelectType<
    N,
    Division<uint8_t, false, 205U, 11>, /// divide by 10
    Division<uint16_t, true, 41943U, 22>, /// divide by 100
    Division<uint32_t, false, 3518437209U, 45>, /// divide by 10000
    Division<uint64_t, false, 12379400392853802749ULL, 90> /// divide by 100000000
    >::Result;

template <size_t N>
using UnsignedOfSize = typename SelectType<N, uint8_t, uint16_t, uint32_t, uint64_t, __uint128_t>::Result;

/// Holds the result of dividing an unsigned N-byte variable by 10^N resulting in
template <size_t N>
struct QuotientAndRemainder
{
    UnsignedOfSize<N> quotient; // quotient with fewer than 2*N decimal digits
    UnsignedOfSize<N / 2> remainder; // remainder with at most N decimal digits
};

template <size_t N>
QuotientAndRemainder<N> inline split(UnsignedOfSize<N> value)
{
    constexpr DivisionBy10PowN<N> division;

    UnsignedOfSize<N> quotient = (division.multiplier * (UnsignedOfSize<2 * N>(value) + division.add)) >> division.shift;
    UnsignedOfSize<N / 2> remainder = static_cast<UnsignedOfSize<N / 2>>(value - quotient * pow10<UnsignedOfSize<N / 2>>(N));

    return {quotient, remainder};
}

ALWAYS_INLINE inline char * outDigit(char * p, uint8_t value)
{
    *p = '0' + value;
    ++p;
    return p;
}

// Using a lookup table to convert binary numbers from 0 to 99
// into ascii characters as described by Andrei Alexandrescu in
// https://www.facebook.com/notes/facebook-engineering/three-optimization-tips-for-c/10151361643253920/

const char digits[201] = "00010203040506070809"
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

namespace convert
{
template <typename UInt, size_t N = sizeof(UInt)>
char * head(char * p, UInt u);
template <typename UInt, size_t N = sizeof(UInt)>
char * tail(char * p, UInt u);

//===----------------------------------------------------------===//
//     head: find most significant digit, skip leading zeros
//===----------------------------------------------------------===//

// "x" contains quotient and remainder after division by 10^N
// quotient is less than 10^N
template <size_t N>
ALWAYS_INLINE inline char * head(char * p, QuotientAndRemainder<N> x)
{
    p = head(p, UnsignedOfSize<N / 2>(x.quotient));
    p = tail(p, x.remainder);
    return p;
}

// "u" is less than 10^2*N
template <typename UInt, size_t N>
ALWAYS_INLINE inline char * head(char * p, UInt u)
{
    return u < pow10<UnsignedOfSize<N>>(N) ? head(p, UnsignedOfSize<N / 2>(u)) : head<N>(p, split<N>(u));
}

// recursion base case, selected when "u" is one byte
template <>
ALWAYS_INLINE inline char * head<UnsignedOfSize<1>, 1>(char * p, UnsignedOfSize<1> u)
{
    return u < 10 ? outDigit(p, u) : outTwoDigits(p, u);
}

//===----------------------------------------------------------===//
//     tail: produce all digits including leading zeros
//===----------------------------------------------------------===//

// recursive step, "u" is less than 10^2*N
template <typename UInt, size_t N>
ALWAYS_INLINE inline char * tail(char * p, UInt u)
{
    QuotientAndRemainder<N> x = split<N>(u);
    p = tail(p, UnsignedOfSize<N / 2>(x.quotient));
    p = tail(p, x.remainder);
    return p;
}

// recursion base case, selected when "u" is one byte
template <>
ALWAYS_INLINE inline char * tail<UnsignedOfSize<1>, 1>(char * p, UnsignedOfSize<1> u)
{
    return outTwoDigits(p, u);
}

//===----------------------------------------------------------===//
// large values are >= 10^2*N
// where x contains quotient and remainder after division by 10^N
//===----------------------------------------------------------===//
template <size_t N>
ALWAYS_INLINE inline char * large(char * p, QuotientAndRemainder<N> x)
{
    QuotientAndRemainder<N> y = split<N>(x.quotient);
    p = head(p, UnsignedOfSize<N / 2>(y.quotient));
    p = tail(p, y.remainder);
    p = tail(p, x.remainder);
    return p;
}

//===----------------------------------------------------------===//
// handle values of "u" that might be >= 10^2*N
// where N is the size of "u" in bytes
//===----------------------------------------------------------===//
template <typename UInt, size_t N = sizeof(UInt)>
ALWAYS_INLINE inline char * uitoa(char * p, UInt u)
{
    if (u < pow10<UnsignedOfSize<N>>(N))
        return head(p, UnsignedOfSize<N / 2>(u));
    QuotientAndRemainder<N> x = split<N>(u);

    return u < pow10<UnsignedOfSize<N>>(2 * N) ? head<N>(p, x) : large<N>(p, x);
}

// selected when "u" is one byte
template <>
ALWAYS_INLINE inline char * uitoa<UnsignedOfSize<1>, 1>(char * p, UnsignedOfSize<1> u)
{
    if (u < 10)
        return outDigit(p, u);
    else if (u < 100)
        return outTwoDigits(p, u);
    else
    {
        p = outDigit(p, u / 100);
        p = outTwoDigits(p, u % 100);
        return p;
    }
}

//===----------------------------------------------------------===//
//     handle unsigned and signed integral operands
//===----------------------------------------------------------===//

// itoa: handle unsigned integral operands (selected by SFINAE)
template <typename U>
requires(!std::is_signed_v<U> && std::is_integral_v<U>)
ALWAYS_INLINE inline char * itoa(U u, char * p)
{
    return convert::uitoa(p, u);
}

// itoa: handle signed integral operands (selected by SFINAE)
template <typename I, size_t N = sizeof(I)>
requires(std::is_signed_v<I> && std::is_integral_v<I>)
ALWAYS_INLINE inline char * itoa(I i, char * p)
{
    // Need "mask" to be filled with a copy of the sign bit.
    // If "i" is a negative value, then the result of "operator >>"
    // is implementation-defined, though usually it is an arithmetic
    // right shift that replicates the sign bit.
    // Use a conditional expression to be portable,
    // a good optimizing compiler generates an arithmetic right shift
    // and avoids the conditional branch.
    UnsignedOfSize<N> mask = i < 0 ? ~UnsignedOfSize<N>(0) : 0;
    // Now get the absolute value of "i" and cast to unsigned type UnsignedOfSize<N>.
    // Cannot use std::abs() because the result is undefined
    // in 2's complement systems for the most-negative value.
    // Want to avoid conditional branch for performance reasons since
    // CPU branch prediction will be ineffective when negative values
    // occur randomly.
    // Let "u" be "i" cast to unsigned type UnsignedOfSize<N>.
    // Subtract "u" from 2*u if "i" is positive or 0 if "i" is negative.
    // This yields the absolute value with the desired type without
    // using a conditional branch and without invoking undefined or
    // implementation defined behavior:
    UnsignedOfSize<N> u = ((2 * UnsignedOfSize<N>(i)) & ~mask) - UnsignedOfSize<N>(i);
    // Unconditionally store a minus sign when producing digits
    // in a forward direction and increment the pointer only if
    // the value is in fact negative.
    // This avoids a conditional branch and is safe because we will
    // always produce at least one digit and it will overwrite the
    // minus sign when the value is not negative.
    *p = '-';
    p += (mask & 1);
    p = convert::uitoa(p, u);
    return p;
}
}

const uint64_t max_multiple_of_hundred_that_fits_in_64_bits = 1'00'00'00'00'00'00'00'00'00ull;
const int max_multiple_of_hundred_blocks = 9;
static_assert(max_multiple_of_hundred_that_fits_in_64_bits % 100 == 0);

ALWAYS_INLINE inline char * writeUIntText(UInt128 _x, char * p)
{
    /// If we the highest 64bit item is empty, we can print just the lowest item as u64
    if (_x.items[UInt128::_impl::little(1)] == 0)
        return convert::itoa(_x.items[UInt128::_impl::little(0)], p);

    /// Doing operations using __int128 is faster and we already rely on this feature
    using T = unsigned __int128;
    T x = (T(_x.items[UInt128::_impl::little(1)]) << 64) + T(_x.items[UInt128::_impl::little(0)]);

    /// We are going to accumulate blocks of 2 digits to print until the number is small enough to be printed as u64
    /// To do this we could do: x / 100, x % 100
    /// But these would mean doing many iterations with long integers, so instead we divide by a much longer integer
    /// multiple of 100 (100^9) and then get the blocks out of it (as u64)
    /// Once we reach u64::max we can stop and use the fast method to print that in the front
    static const T large_divisor = max_multiple_of_hundred_that_fits_in_64_bits;
    static const T largest_uint64 = std::numeric_limits<uint64_t>::max();
    uint8_t two_values[20] = {0}; // 39 Max characters / 2

    int current_block = 0;
    while (x > largest_uint64)
    {
        uint64_t u64_remainder = uint64_t(x % large_divisor);
        x /= large_divisor;

        int pos = current_block;
        while (u64_remainder)
        {
            two_values[pos] = uint8_t(u64_remainder % 100);
            pos++;
            u64_remainder /= 100;
        }
        current_block += max_multiple_of_hundred_blocks;
    }

    char * highest_part_print = convert::itoa(uint64_t(x), p);
    for (int i = 0; i < current_block; i++)
    {
        outTwoDigits(highest_part_print, two_values[current_block - 1 - i]);
        highest_part_print += 2;
    }

    return highest_part_print;
}

ALWAYS_INLINE inline char * writeUIntText(UInt256 _x, char * p)
{
    /// If possible, treat it as a smaller integer as they are much faster to print
    if (_x.items[UInt256::_impl::little(3)] == 0 && _x.items[UInt256::_impl::little(2)] == 0)
        return writeUIntText(UInt128{_x.items[UInt256::_impl::little(0)], _x.items[UInt256::_impl::little(1)]}, p);

    /// If available (x86) we transform from our custom class to _BitInt(256) which has better support in the compiler
    /// and produces better code
    using T =
#if defined(__x86_64__)
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wbit-int-extension"
        unsigned _BitInt(256)
#    pragma clang diagnostic pop
#else
        UInt256
#endif
        ;

#if defined(__x86_64__)
    T x = (T(_x.items[UInt256::_impl::little(3)]) << 192) + (T(_x.items[UInt256::_impl::little(2)]) << 128)
        + (T(_x.items[UInt256::_impl::little(1)]) << 64) + T(_x.items[UInt256::_impl::little(0)]);
#else
    T x = _x;
#endif

    /// Similar to writeUIntText(UInt128) only that in this case we will stop as soon as we reach the largest u128
    /// and switch to that function
    uint8_t two_values[39] = {0}; // 78 Max characters / 2
    int current_pos = 0;

    static const T large_divisor = max_multiple_of_hundred_that_fits_in_64_bits;
    static const T largest_uint128 = T(std::numeric_limits<uint64_t>::max()) << 64 | T(std::numeric_limits<uint64_t>::max());

    while (x > largest_uint128)
    {
        uint64_t u64_remainder = uint64_t(x % large_divisor);
        x /= large_divisor;

        int pos = current_pos;
        while (u64_remainder)
        {
            two_values[pos] = uint8_t(u64_remainder % 100);
            pos++;
            u64_remainder /= 100;
        }
        current_pos += max_multiple_of_hundred_blocks;
    }

#if defined(__x86_64__)
    UInt128 pending{uint64_t(x), uint64_t(x >> 64)};
#else
    UInt128 pending{x.items[UInt256::_impl::little(0)], x.items[UInt256::_impl::little(1)]};
#endif

    char * highest_part_print = writeUIntText(pending, p);
    for (int i = 0; i < current_pos; i++)
    {
        outTwoDigits(highest_part_print, two_values[current_pos - 1 - i]);
        highest_part_print += 2;
    }

    return highest_part_print;
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
    static constexpr T min_int = UnsignedT(1) << (sizeof(T) * 8 - 1);

    if (unlikely(x == min_int))
    {
        if constexpr (std::is_same_v<T, Int128>)
        {
            const char * res = "-170141183460469231731687303715884105728";
            memcpy(pos, res, strlen(res));
            return pos + strlen(res);
        }
        else if constexpr (std::is_same_v<T, Int256>)
        {
            const char * res = "-57896044618658097711785492504343953926634992332820282019728792003956564819968";
            memcpy(pos, res, strlen(res));
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
    return convert::itoa(uint8_t(i), p);
}

char * itoa(Int8 i, char * p)
{
    return convert::itoa(int8_t(i), p);
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
        return convert::itoa(i, p); \
    }

#define FOR_MISSING_INTEGER_TYPES(M) \
    M(uint8_t) \
    M(UInt16) \
    M(UInt32) \
    M(UInt64) \
    M(int8_t) \
    M(Int16) \
    M(Int32) \
    M(Int64)

FOR_MISSING_INTEGER_TYPES(DEFAULT_ITOA)

#if defined(OS_DARWIN)
DEFAULT_ITOA(unsigned long)
DEFAULT_ITOA(long)
#endif

#undef FOR_MISSING_INTEGER_TYPES
#undef DEFAULT_ITOA
