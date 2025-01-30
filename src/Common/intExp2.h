#pragma once

#include <base/defines.h>

#include <cstdint>

/// On overflow, the function returns unspecified value.
constexpr NO_SANITIZE_UNDEFINED uint64_t intExp2(int x)
{
    return 1ULL << x;
}
