#include <type_traits>
#include <base/defines.h>
#include <base/extended_types.h>
#include <base/itoa.h>

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

constexpr uint64_t max_multiple_of_hundred_that_fits_in_64_bits = 1'00'00'00'00'00'00'00'00'00ull;
constexpr int max_multiple_of_hundred_blocks = 9;
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

/// Extract up to 9 digit pairs from a u64 value into the provided output buffer.
ALWAYS_INLINE inline void extractDigitPairs(uint64_t remainder, uint8_t * two_values)
{
    for (int i = 0; i < max_multiple_of_hundred_blocks; ++i)
    {
        two_values[i] = uint8_t(remainder % 100);
        remainder /= 100;
    }
}

/// Write `count` digit pairs from `two_values` (in reverse order) to the output buffer.
template <int count>
ALWAYS_INLINE inline char * writeDigitPairs(char * p, const uint8_t * two_values)
{
    for (int i = count - 1; i >= 0; --i)
    {
        outTwoDigits(p, two_values[i]);
        p += 2;
    }
    return p;
}

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
        return jeaiii::to_text_from_integer(p, _x.items[UInt128::_impl::little(0)]);

    /// Doing operations using __int128 is faster and we already rely on this feature.
    using T = unsigned __int128;
    T x = (T(_x.items[UInt128::_impl::little(1)]) << 64) + T(_x.items[UInt128::_impl::little(0)]);

    /// Split into blocks of up to 18 digits (10^18 per block) using Barrett reduction.
    /// UInt128 max is ~3.4e38, so at most 2 divisions are needed.
    /// Unrolled: first division always needed (x > uint64 max since high item != 0),
    /// second division only if quotient still exceeds uint64 max.
    uint8_t two_values[18] = {0};

    uint64_t r1;
    x = divmod_1e18(x, r1);
    extractDigitPairs(r1, two_values);

    constexpr T largest_uint64 = std::numeric_limits<uint64_t>::max();
    if (unlikely(x > largest_uint64))
    {
        uint64_t r2;
        x = divmod_1e18(x, r2);
        extractDigitPairs(r2, two_values + max_multiple_of_hundred_blocks);

        char * out = jeaiii::to_text_from_integer(p, uint64_t(x));
        return writeDigitPairs<2 * max_multiple_of_hundred_blocks>(out, two_values);
    }

    char * out = jeaiii::to_text_from_integer(p, uint64_t(x));
    return writeDigitPairs<max_multiple_of_hundred_blocks>(out, two_values);
}

ALWAYS_INLINE inline char * writeUIntText(UInt256 _x, char * p)
{
    /// If possible, treat it as a smaller integer as they are much faster to print
    if (likely(_x.items[UInt256::_impl::little(3)] == 0 && _x.items[UInt256::_impl::little(2)] == 0))
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

    constexpr T large_divisor = max_multiple_of_hundred_that_fits_in_64_bits;
    constexpr T largest_uint128 = T(std::numeric_limits<uint64_t>::max()) << 64 | T(std::numeric_limits<uint64_t>::max());

    while (x > largest_uint128)
    {
        uint64_t u64_remainder = uint64_t(x % large_divisor);
        x /= large_divisor;
        extractDigitPairs(u64_remainder, two_values + current_pos);
        current_pos += max_multiple_of_hundred_blocks;
    }

#if defined(__x86_64__)
    UInt128 pending{uint64_t(x), uint64_t(x >> 64)};
#else
    UInt128 pending{x.items[UInt256::_impl::little(0)], x.items[UInt256::_impl::little(1)]};
#endif

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
