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
#include <type_traits>
#include "likely.h"
#include "unaligned.h"

using uint128_t = unsigned __int128;

namespace impl
{
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

static inline uint16_t const & dd(uint8_t u)
{
    return reinterpret_cast<uint16_t const *>(digits)[u];
}

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

template <typename UInt, bool A, UInt M, unsigned S>
struct MulInv
{
    using type = UInt;
    static constexpr bool a{A};
    static constexpr UInt m{M};
    static constexpr unsigned s{S};
};
template <int, int, class...>
struct UT;
template <int N, class T, class... Ts>
struct UT<N, N, T, Ts...>
{
    using U = T;
};
template <int N, int M, class T, class... Ts>
struct UT<N, M, T, Ts...>
{
    using U = typename UT<N, 2 * M, Ts...>::U;
};
template <int N>
using MI = typename UT<
    N,
    1,
    MulInv<uint8_t, 0, 205U, 11>,
    MulInv<uint16_t, 1, 41943U, 22>,
    MulInv<uint32_t, 0, 3518437209U, 45>,
    MulInv<uint64_t, 0, 12379400392853802749U, 90>,
    MulInv<uint128_t, 0, 0, 0>>::U;
template <int N>
using U = typename MI<N>::type;

// struct QR holds the result of dividing an unsigned N-byte variable
// by 10^N resulting in
template <size_t N>
struct QR
{
    U<N> q; // quotient with fewer than 2*N decimal digits
    U<N / 2> r; // remainder with at most N decimal digits
};
template <size_t N>
QR<N> static inline split(U<N> u)
{
    constexpr MI<N> mi{};
    U<N> q = (mi.m * (U<2 * N>(u) + mi.a)) >> mi.s;
    return {q, U<N / 2>(u - q * pow10<U<N / 2>>(N))};
}

template <typename T>
static inline char * out(char * p, T && obj)
{
    memcpy(p, reinterpret_cast<const void *>(&obj), sizeof(T));
    p += sizeof(T);
    return p;
}

struct convert
{
    //===----------------------------------------------------------===//
    //     head: find most significant digit, skip leading zeros
    //===----------------------------------------------------------===//

    // "x" contains quotient and remainder after division by 10^N
    // quotient is less than 10^N
    template <size_t N>
    static inline char * head(char * p, QR<N> x)
    {
        return tail(head(p, U<N / 2>(x.q)), x.r);
    }

    // "u" is less than 10^2*N
    template <typename UInt, size_t N = sizeof(UInt)>
    static inline char * head(char * p, UInt u)
    {
        return (u < pow10<U<N>>(N) ? (head(p, U<N / 2>(u))) : (head<N>(p, split<N>(u))));
    }

    // recursion base case, selected when "u" is one byte
    static inline char * head(char * p, U<1> u) { return (u < 10 ? (out<char>(p, '0' + u)) : (out(p, dd(u)))); }

    //===----------------------------------------------------------===//
    //     tail: produce all digits including leading zeros
    //===----------------------------------------------------------===//

    // recursive step, "u" is less than 10^2*N
    template <typename UInt, size_t N = sizeof(UInt)>
    static inline char * tail(char * p, UInt u)
    {
        QR<N> x = split<N>(u);
        return tail(tail(p, U<N / 2>(x.q)), x.r);
    }

    // recursion base case, selected when "u" is one byte
    static inline char * tail(char * p, U<1> u) { return out(p, dd(u)); }

    //===----------------------------------------------------------===//
    // large values are >= 10^2*N
    // where x contains quotient and remainder after division by 10^N
    //===----------------------------------------------------------===//

    template <size_t N>
    static inline char * large(char * p, QR<N> x)
    {
        QR<N> y = split<N>(x.q);
        return tail(tail(head(p, U<N / 2>(y.q)), y.r), x.r);
    }

    //===----------------------------------------------------------===//
    // handle values of "u" that might be >= 10^2*N
    // where N is the size of "u" in bytes
    //===----------------------------------------------------------===//

    template <typename UInt, size_t N = sizeof(UInt)>
    static inline char * itoa(char * p, UInt u)
    {
        if (u < pow10<U<N>>(N))
            return head(p, U<N / 2>(u));
        QR<N> x = split<N>(u);
        return (u < pow10<U<N>>(2 * N) ? (head<N>(p, x)) : (large<N>(p, x)));
    }

    // selected when "u" is one byte
    static inline char * itoa(char * p, U<1> u)
    {
        if (u < 10)
            return out<char>(p, '0' + u);
        if (u < 100)
            return out(p, dd(u));
        return out(out<char>(p, '0' + u / 100), dd(u % 100));
    }

    //===----------------------------------------------------------===//
    //     handle unsigned and signed integral operands
    //===----------------------------------------------------------===//

    // itoa: handle unsigned integral operands (selected by SFINAE)
    template <typename U, std::enable_if_t<not std::is_signed<U>::value && std::is_integral<U>::value> * = nullptr>
    static inline char * itoa(U u, char * p)
    {
        return convert::itoa(p, u);
    }

    // itoa: handle signed integral operands (selected by SFINAE)
    template <typename I, size_t N = sizeof(I), std::enable_if_t<std::is_signed<I>::value && std::is_integral<I>::value> * = nullptr>
    static inline char * itoa(I i, char * p)
    {
        // Need "mask" to be filled with a copy of the sign bit.
        // If "i" is a negative value, then the result of "operator >>"
        // is implementation-defined, though usually it is an arithmetic
        // right shift that replicates the sign bit.
        // Use a conditional expression to be portable,
        // a good optimizing compiler generates an arithmetic right shift
        // and avoids the conditional branch.
        U<N> mask = i < 0 ? ~U<N>(0) : 0;
        // Now get the absolute value of "i" and cast to unsigned type U<N>.
        // Cannot use std::abs() because the result is undefined
        // in 2's complement systems for the most-negative value.
        // Want to avoid conditional branch for performance reasons since
        // CPU branch prediction will be ineffective when negative values
        // occur randomly.
        // Let "u" be "i" cast to unsigned type U<N>.
        // Subtract "u" from 2*u if "i" is positive or 0 if "i" is negative.
        // This yields the absolute value with the desired type without
        // using a conditional branch and without invoking undefined or
        // implementation defined behavior:
        U<N> u = ((2 * U<N>(i)) & ~mask) - U<N>(i);
        // Unconditionally store a minus sign when producing digits
        // in a forward direction and increment the pointer only if
        // the value is in fact negative.
        // This avoids a conditional branch and is safe because we will
        // always produce at least one digit and it will overwrite the
        // minus sign when the value is not negative.
        *p = '-';
        p += (mask & 1);
        p = convert::itoa(p, u);
        return p;
    }
};

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
        unalignedStore(pp, dd(i));
    }
    if (x < 10)
        *p = '0' + x;
    else
        unalignedStore(p, dd(x));
    return p + len;
}

static inline char * writeLeadingMinus(char * pos)
{
    *pos = '-';
    return pos + 1;
}

static inline char * writeSIntText(__int128 x, char * pos)
{
    static const __int128 min_int128 = __int128(0x8000000000000000ll) << 64;

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
    return writeUIntText(static_cast<unsigned __int128>(x), pos);
}

}

template <typename I>
char * itoa(I i, char * p)
{
    return impl::convert::itoa(i, p);
}

static inline char * itoa(uint128_t i, char * p)
{
    return impl::writeUIntText(i, p);
}

static inline char * itoa(__int128 i, char * p)
{
    return impl::writeSIntText(i, p);
}
