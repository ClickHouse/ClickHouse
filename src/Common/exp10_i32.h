#pragma once

namespace common
{

constexpr int exp10_i32(int x)
{
    if (x < 0)
        return 0;
    if (x > 9)
        return std::numeric_limits<int>::max();

    constexpr int exp10_i32_table[10] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};
    return exp10_i32_table[x];  /// NOLINT(clang-analyzer-security.ArrayBound)
}
}
