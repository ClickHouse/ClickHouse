#pragma once

#include <base/defines.h>

#include <cstdint>

/// On overflow, the function returns unspecified value.
constexpr NO_SANITIZE_UNDEFINED uint64_t intExp2(int x)
{
    if (x < 0)
        return 0;
    if (x > 63)
        return std::numeric_limits<uint64_t>::max();
    return 1ULL << x;
}
