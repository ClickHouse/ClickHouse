#pragma once

// Based on https://github.com/amdn/itoa and combined with our optimizations
//
//=== itoa.h - Fast integer to ascii conversion                   --*- C++ -*-//
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

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <type_traits>

using int128_t = __int128;
using uint128_t = unsigned __int128;

namespace impl
{

template <typename T>
static constexpr T pow10(size_t x)
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
using DivisionBy10PowN = typename SelectType
<
    N,
    Division<uint8_t, 0, 205U, 11>,                           /// divide by 10
    Division<uint16_t, 1, 41943U, 22>,                        /// divide by 100
    Division<uint32_t, 0, 3518437209U, 45>,                   /// divide by 10000
    Division<uint64_t, 0, 12379400392853802749ULL, 90>        /// divide by 100000000
>::Result;

template <size_t N>
using UnsignedOfSize = typename SelectType
<
    N,
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t,
    uint128_t
>::Result;

/// Holds the result of dividing an unsigned N-byte variable by 10^N resulting in
template <size_t N>
struct QuotientAndRemainder
{
    UnsignedOfSize<N> quotient; // quotient with fewer than 2*N decimal digits
    UnsignedOfSize<N / 2> remainder; // remainder with at most N decimal digits
};

template <size_t N>
QuotientAndRemainder<N> static inline split(UnsignedOfSize<N> value)
{
    constexpr DivisionBy10PowN<N> division;

    UnsignedOfSize<N> quotient = (division.multiplier * (UnsignedOfSize<2 * N>(value) + division.add)) >> division.shift;
    UnsignedOfSize<N / 2> remainder = value - quotient * pow10<UnsignedOfSize<N / 2>>(N);

    return {quotient, remainder};
}


static inline char * outDigit(char * p, uint8_t value)
{
    *p = '0' + value;
    ++p;
    return p;
}

// Using a lookup table to convert binary numbers from 0 to 99
// into ascii characters as described by Andrei Alexandrescu in
// https://www.facebook.com/notes/facebook-engineering/three-optimization-tips-for-c/10151361643253920/

static const char digits[201] = "00010203040506070809"
                                "10111213141516171819"
                                "20212223242526272829"
                                "30313233343536373839"
                                "40414243444546474849"
                                "50515253545556575859"
                                "60616263646566676869"
                                "70717273747576777879"
                                "80818283848586878889"
                                "90919293949596979899";

static inline char * outTwoDigits(char * p, uint8_t value)
{
    memcpy(p, &digits[value * 2], 2);
    p += 2;
    return p;
}


namespace convert
{
    template <typename UInt, size_t N = sizeof(UInt)> static char * head(char * p, UInt u);
    template <typename UInt, size_t N = sizeof(UInt)> static char * tail(char * p, UInt u);

    //===----------------------------------------------------------===//
    //     head: find most significant digit, skip leading zeros
    //===----------------------------------------------------------===//

    // "x" contains quotient and remainder after division by 10^N
    // quotient is less than 10^N
    template <size_t N>
    static inline char * head(char * p, QuotientAndRemainder<N> x)
    {
        p = head(p, UnsignedOfSize<N / 2>(x.quotient));
        p = tail(p, x.remainder);
        return p;
    }

    // "u" is less than 10^2*N
    template <typename UInt, size_t N>
    static inline char * head(char * p, UInt u)
    {
        return u < pow10<UnsignedOfSize<N>>(N)
            ? head(p, UnsignedOfSize<N / 2>(u))
            : head<N>(p, split<N>(u));
    }

    // recursion base case, selected when "u" is one byte
    template <>
    inline char * head<UnsignedOfSize<1>, 1>(char * p, UnsignedOfSize<1> u)
    {
        return u < 10
            ? outDigit(p, u)
            : outTwoDigits(p, u);
    }

    //===----------------------------------------------------------===//
    //     tail: produce all digits including leading zeros
    //===----------------------------------------------------------===//

    // recursive step, "u" is less than 10^2*N
    template <typename UInt, size_t N>
    static inline char * tail(char * p, UInt u)
    {
        QuotientAndRemainder<N> x = split<N>(u);
        p = tail(p, UnsignedOfSize<N / 2>(x.quotient));
        p = tail(p, x.remainder);
        return p;
    }

    // recursion base case, selected when "u" is one byte
    template <>
    inline char * tail<UnsignedOfSize<1>, 1>(char * p, UnsignedOfSize<1> u)
    {
        return outTwoDigits(p, u);
    }

    //===----------------------------------------------------------===//
    // large values are >= 10^2*N
    // where x contains quotient and remainder after division by 10^N
    //===----------------------------------------------------------===//

    template <size_t N>
    static inline char * large(char * p, QuotientAndRemainder<N> x)
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
    static inline char * uitoa(char * p, UInt u)
    {
        if (u < pow10<UnsignedOfSize<N>>(N))
            return head(p, UnsignedOfSize<N / 2>(u));
        QuotientAndRemainder<N> x = split<N>(u);

        return u < pow10<UnsignedOfSize<N>>(2 * N)
            ? head<N>(p, x)
            : large<N>(p, x);
    }

    // selected when "u" is one byte
    template <>
    inline char * uitoa<UnsignedOfSize<1>, 1>(char * p, UnsignedOfSize<1> u)
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
    template <typename U, std::enable_if_t<!std::is_signed_v<U> && std::is_integral_v<U>> * = nullptr>
    static inline char * itoa(U u, char * p)
    {
        return convert::uitoa(p, u);
    }

    // itoa: handle signed integral operands (selected by SFINAE)
    template <typename I, size_t N = sizeof(I), std::enable_if_t<std::is_signed_v<I> && std::is_integral_v<I>> * = nullptr>
    static inline char * itoa(I i, char * p)
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

static inline int digits10(uint128_t x)
{
    if (x < 10ULL)
        return 1;
    if (x < 100ULL)
        return 2;
    if (x < 1000ULL)
        return 3;

    if (x < 1000000000000ULL)
    {
        if (x < 100000000ULL)
        {
            if (x < 1000000ULL)
            {
                if (x < 10000ULL)
                    return 4;
                else
                    return 5 + (x >= 100000ULL);
            }

            return 7 + (x >= 10000000ULL);
        }

        if (x < 10000000000ULL)
            return 9 + (x >= 1000000000ULL);

        return 11 + (x >= 100000000000ULL);
    }

    return 12 + digits10(x / 1000000000000ULL);
}

static inline char * writeUIntText(uint128_t x, char * p)
{
    int len = digits10(x);
    auto pp = p + len;
    while (x >= 100)
    {
        const auto i = x % 100;
        x /= 100;
        pp -= 2;
        outTwoDigits(pp, i);
    }
    if (x < 10)
        *p = '0' + x;
    else
        outTwoDigits(p, x);
    return p + len;
}

static inline char * writeLeadingMinus(char * pos)
{
    *pos = '-';
    return pos + 1;
}

static inline char * writeSIntText(int128_t x, char * pos)
{
    static constexpr int128_t min_int128 = uint128_t(1) << 127;

    if (unlikely(x == min_int128))
    {
        memcpy(pos, "-170141183460469231731687303715884105728", 40);
        return pos + 40;
    }

    if (x < 0)
    {
        x = -x;
        pos = writeLeadingMinus(pos);
    }
    return writeUIntText(static_cast<uint128_t>(x), pos);
}

}

template <typename I>
char * itoa(I i, char * p)
{
    return impl::convert::itoa(i, p);
}

template <>
inline char * itoa(char8_t i, char * p)
{
    return impl::convert::itoa(uint8_t(i), p);
}

template <>
inline char * itoa<uint128_t>(uint128_t i, char * p)
{
    return impl::writeUIntText(i, p);
}

template <>
inline char * itoa<int128_t>(int128_t i, char * p)
{
    return impl::writeSIntText(i, p);
}
