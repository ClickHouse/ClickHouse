#pragma once

/// Can be removed after -std=c++26
template <class T>
constexpr T saturating_sub(T x, T y) noexcept
{
    return __builtin_elementwise_sub_sat(x, y);
}
