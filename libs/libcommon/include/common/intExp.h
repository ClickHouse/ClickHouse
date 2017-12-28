#pragma once

#include <cstdint>
#include <limits>


/// On overlow, the function returns unspecified value.

inline uint64_t intExp2(int x)
{
    return 1ULL << x;
}

inline uint64_t intExp10(int x)
{
    if (x < 0)
        return 0;
    if (x > 19)
        return std::numeric_limits<uint64_t>::max();

    static const uint64_t table[20] =
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
