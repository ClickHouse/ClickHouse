#pragma once

#include <cstdint>
#include <limits>

#include <base/extended_types.h>
#include <base/defines.h>


constexpr inline uint64_t intExp2(int x)
{
    if (x < 0)
        return 0;
    if (x > 63)
        return std::numeric_limits<uint64_t>::max();
    return 1ULL << x;
}

extern const uint64_t intExp10_table[];

constexpr uint64_t intExp10(int x)
{
    if (x < 0)
        return 0;
    if (x > 19)
        return std::numeric_limits<uint64_t>::max();

    return intExp10_table[x];
}

namespace common
{

extern const int exp10_i32_table[];
constexpr int exp10_i32(int x)
{
    if (x < 0)
        return 0;
    if (x > 9)
        return std::numeric_limits<int>::max();

    return exp10_i32_table[x];
}

extern const int64_t exp10_i64_table[];
constexpr int64_t exp10_i64(int x)
{
    if (x < 0)
        return 0;
    if (x > 18)
        return std::numeric_limits<int64_t>::max();

    return exp10_i64_table[x];
}

extern const Int128 exp10_i128_table[];
constexpr Int128 exp10_i128(int x)
{
    if (x < 0)
        return 0;
    if (x > 38)
        return std::numeric_limits<Int128>::max();

    return exp10_i128_table[x];
}

extern const Int256 exp10_i256_table[];
constexpr Int256 exp10_i256(int x)
{
    if (x < 0)
        return 0;
    if (x > 76)
        return std::numeric_limits<Int256>::max();

    return exp10_i256_table[x];
}

}


/// intExp10 returning the type T.
template <typename T>
constexpr T intExp10OfSize(int x)
{
    if constexpr (sizeof(T) <= 4)
        return static_cast<T>(common::exp10_i32(x));
    else if constexpr (sizeof(T) <= 8)
        return common::exp10_i64(x);
    else if constexpr (sizeof(T) <= 16)
        return common::exp10_i128(x);
    else
        return common::exp10_i256(x);
}
