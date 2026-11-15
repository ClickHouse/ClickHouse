#pragma once

#include <cstdint>
#include <limits>

constexpr uint64_t intExp10(int x)
{
    if (x < 0)
        return 0;
    if (x > 19)
        return std::numeric_limits<uint64_t>::max();

    constexpr uint64_t intExp10_table[]
        = {1ULL,
           10ULL,
           100ULL,
           1000ULL,
           10000ULL,
           100000ULL,
           1000000ULL,
           10000000ULL,
           100000000ULL,
           1000000000ULL,
           10000000000ULL,
           100000000000ULL,
           1000000000000ULL,
           10000000000000ULL,
           100000000000000ULL,
           1000000000000000ULL,
           10000000000000000ULL,
           100000000000000000ULL,
           1000000000000000000ULL,
           10000000000000000000ULL};
    return intExp10_table[x];
}
