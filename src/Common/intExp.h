#pragma once

#include <cstdint>
#include <limits>

#include <base/extended_types.h>

// Also defined in Core/Defines.h
#if !defined(NO_SANITIZE_UNDEFINED)
#if defined(__clang__)
    #define NO_SANITIZE_UNDEFINED __attribute__((__no_sanitize__("undefined")))
#else
    #define NO_SANITIZE_UNDEFINED
#endif
#endif


/// On overflow, the function returns unspecified value.
inline NO_SANITIZE_UNDEFINED uint64_t intExp2(int x)
{
    return 1ULL << x;
}

constexpr inline uint64_t intExp10(int x)
{
    if (x < 0)
        return 0;
    if (x > 19)
        return std::numeric_limits<uint64_t>::max();

    constexpr uint64_t table[20] =
    {
        1ULL,                   10ULL,                   100ULL,
        1000ULL,                10000ULL,                100000ULL,
        1000000ULL,             10000000ULL,             100000000ULL,
        1000000000ULL,          10000000000ULL,          100000000000ULL,
        1000000000000ULL,       10000000000000ULL,       100000000000000ULL,
        1000000000000000ULL,    10000000000000000ULL,    100000000000000000ULL,
        1000000000000000000ULL, 10000000000000000000ULL
    };

    return table[x];
}

namespace common
{

constexpr inline int exp10_i32(int x)
{
    constexpr int values[] =
    {
        1,
        10,
        100,
        1000,
        10000,
        100000,
        1000000,
        10000000,
        100000000,
        1000000000
    };
    return values[x];
}

constexpr inline int64_t exp10_i64(int x)
{
    constexpr int64_t values[] =
    {
        1LL,
        10LL,
        100LL,
        1000LL,
        10000LL,
        100000LL,
        1000000LL,
        10000000LL,
        100000000LL,
        1000000000LL,
        10000000000LL,
        100000000000LL,
        1000000000000LL,
        10000000000000LL,
        100000000000000LL,
        1000000000000000LL,
        10000000000000000LL,
        100000000000000000LL,
        1000000000000000000LL
    };
    return values[x];
}

constexpr inline Int128 exp10_i128(int x)
{
    constexpr Int128 values[] =
    {
        static_cast<Int128>(1LL),
        static_cast<Int128>(10LL),
        static_cast<Int128>(100LL),
        static_cast<Int128>(1000LL),
        static_cast<Int128>(10000LL),
        static_cast<Int128>(100000LL),
        static_cast<Int128>(1000000LL),
        static_cast<Int128>(10000000LL),
        static_cast<Int128>(100000000LL),
        static_cast<Int128>(1000000000LL),
        static_cast<Int128>(10000000000LL),
        static_cast<Int128>(100000000000LL),
        static_cast<Int128>(1000000000000LL),
        static_cast<Int128>(10000000000000LL),
        static_cast<Int128>(100000000000000LL),
        static_cast<Int128>(1000000000000000LL),
        static_cast<Int128>(10000000000000000LL),
        static_cast<Int128>(100000000000000000LL),
        static_cast<Int128>(1000000000000000000LL),
        static_cast<Int128>(1000000000000000000LL) * 10LL,
        static_cast<Int128>(1000000000000000000LL) * 100LL,
        static_cast<Int128>(1000000000000000000LL) * 1000LL,
        static_cast<Int128>(1000000000000000000LL) * 10000LL,
        static_cast<Int128>(1000000000000000000LL) * 100000LL,
        static_cast<Int128>(1000000000000000000LL) * 1000000LL,
        static_cast<Int128>(1000000000000000000LL) * 10000000LL,
        static_cast<Int128>(1000000000000000000LL) * 100000000LL,
        static_cast<Int128>(1000000000000000000LL) * 1000000000LL,
        static_cast<Int128>(1000000000000000000LL) * 10000000000LL,
        static_cast<Int128>(1000000000000000000LL) * 100000000000LL,
        static_cast<Int128>(1000000000000000000LL) * 1000000000000LL,
        static_cast<Int128>(1000000000000000000LL) * 10000000000000LL,
        static_cast<Int128>(1000000000000000000LL) * 100000000000000LL,
        static_cast<Int128>(1000000000000000000LL) * 1000000000000000LL,
        static_cast<Int128>(1000000000000000000LL) * 10000000000000000LL,
        static_cast<Int128>(1000000000000000000LL) * 100000000000000000LL,
        static_cast<Int128>(1000000000000000000LL) * 100000000000000000LL * 10LL,
        static_cast<Int128>(1000000000000000000LL) * 100000000000000000LL * 100LL,
        static_cast<Int128>(1000000000000000000LL) * 100000000000000000LL * 1000LL
    };
    return values[x];
}


inline Int256 exp10_i256(int x)
{
    using Int256 = Int256;
    static constexpr Int256 i10e18{1000000000000000000ll};
    static const Int256 values[] = {
        static_cast<Int256>(1ll),
        static_cast<Int256>(10ll),
        static_cast<Int256>(100ll),
        static_cast<Int256>(1000ll),
        static_cast<Int256>(10000ll),
        static_cast<Int256>(100000ll),
        static_cast<Int256>(1000000ll),
        static_cast<Int256>(10000000ll),
        static_cast<Int256>(100000000ll),
        static_cast<Int256>(1000000000ll),
        static_cast<Int256>(10000000000ll),
        static_cast<Int256>(100000000000ll),
        static_cast<Int256>(1000000000000ll),
        static_cast<Int256>(10000000000000ll),
        static_cast<Int256>(100000000000000ll),
        static_cast<Int256>(1000000000000000ll),
        static_cast<Int256>(10000000000000000ll),
        static_cast<Int256>(100000000000000000ll),
        i10e18,
        i10e18 * 10ll,
        i10e18 * 100ll,
        i10e18 * 1000ll,
        i10e18 * 10000ll,
        i10e18 * 100000ll,
        i10e18 * 1000000ll,
        i10e18 * 10000000ll,
        i10e18 * 100000000ll,
        i10e18 * 1000000000ll,
        i10e18 * 10000000000ll,
        i10e18 * 100000000000ll,
        i10e18 * 1000000000000ll,
        i10e18 * 10000000000000ll,
        i10e18 * 100000000000000ll,
        i10e18 * 1000000000000000ll,
        i10e18 * 10000000000000000ll,
        i10e18 * 100000000000000000ll,
        i10e18 * 100000000000000000ll * 10ll,
        i10e18 * 100000000000000000ll * 100ll,
        i10e18 * 100000000000000000ll * 1000ll,
        i10e18 * 100000000000000000ll * 10000ll,
        i10e18 * 100000000000000000ll * 100000ll,
        i10e18 * 100000000000000000ll * 1000000ll,
        i10e18 * 100000000000000000ll * 10000000ll,
        i10e18 * 100000000000000000ll * 100000000ll,
        i10e18 * 100000000000000000ll * 1000000000ll,
        i10e18 * 100000000000000000ll * 10000000000ll,
        i10e18 * 100000000000000000ll * 100000000000ll,
        i10e18 * 100000000000000000ll * 1000000000000ll,
        i10e18 * 100000000000000000ll * 10000000000000ll,
        i10e18 * 100000000000000000ll * 100000000000000ll,
        i10e18 * 100000000000000000ll * 1000000000000000ll,
        i10e18 * 100000000000000000ll * 10000000000000000ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll * 10ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll * 100ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll * 1000ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll * 10000ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll * 100000ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll * 1000000ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll * 10000000ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll * 1000000000ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll * 10000000000ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll * 1000000000000ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll * 10000000000000ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll * 1000000000000000ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll * 10000000000000000ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000000ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000000ll * 10ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000000ll * 100ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000000ll * 1000ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000000ll * 10000ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000000ll * 100000ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000000ll * 1000000ll,
        i10e18 * 100000000000000000ll * 100000000000000000ll * 100000000000000000ll * 10000000ll,
    };
    return values[x];
}

}


/// intExp10 returning the type T.
template <typename T>
constexpr inline T intExp10OfSize(int x)
{
    if constexpr (sizeof(T) <= 8)
        return intExp10(x);
    else if constexpr (sizeof(T) <= 16)
        return common::exp10_i128(x);
    else
        return common::exp10_i256(x);
}
