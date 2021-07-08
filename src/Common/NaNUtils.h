#pragma once

#include <cmath>
#include <limits>
#include <type_traits>

#include <common/extended_types.h>


/// To be sure, that this function is zero-cost for non-floating point types.
template <typename T>
inline std::enable_if_t<std::is_floating_point_v<T>, bool> isNaN(T x)
{
    return std::isnan(x);
}

template <typename T>
inline std::enable_if_t<!std::is_floating_point_v<T>, bool> isNaN(T)
{
    return false;
}

template <typename T>
inline std::enable_if_t<std::is_floating_point_v<T>, bool> isFinite(T x)
{
    return std::isfinite(x);
}

template <typename T>
inline std::enable_if_t<!std::is_floating_point_v<T>, bool> isFinite(T)
{
    return true;
}

template <typename T>
std::enable_if_t<std::is_floating_point_v<T>, T> NaNOrZero()
{
    return std::numeric_limits<T>::quiet_NaN();
}

template <typename T>
std::enable_if_t<is_integer_v<T>, T> NaNOrZero()
{
    return T{0};
}

template <typename T>
std::enable_if_t<std::is_class_v<T> && !is_integer_v<T>, T> NaNOrZero()
{
    return T{};
}
