#pragma once

#include <cmath>
#include <limits>
#include <type_traits>


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
std::enable_if_t<std::numeric_limits<T>::is_integer, T> NaNOrZero()
{
    return 0;
}

template <typename T>
std::enable_if_t<std::is_class_v<T>, T> NaNOrZero()
{
    return T{};
}

#if 1 /// __int128
template <typename T>
std::enable_if_t<std::is_same_v<T, __int128>, __int128> NaNOrZero()
{
    return __int128(0);
}
#endif
