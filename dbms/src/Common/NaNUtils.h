#pragma once

#include <cmath>
#include <limits>
#include <type_traits>


/// To be sure, that this function is zero-cost for non-floating point types.
template <typename T>
inline typename std::enable_if<std::is_floating_point<T>::value, bool>::type isNaN(T x)
{
    return std::isnan(x);
}

template <typename T>
inline typename std::enable_if<!std::is_floating_point<T>::value, bool>::type isNaN(T x)
{
    return false;
}

template <typename T>
inline typename std::enable_if<std::is_floating_point<T>::value, bool>::type isFinite(T x)
{
    return std::isfinite(x);
}

template <typename T>
inline typename std::enable_if<!std::is_floating_point<T>::value, bool>::type isFinite(T x)
{
    return true;
}

template <typename T>
typename std::enable_if<std::is_floating_point<T>::value, T>::type NaNOrZero()
{
    return std::numeric_limits<T>::quiet_NaN();
}

template <typename T>
typename std::enable_if<std::numeric_limits<T>::is_integer, T>::type NaNOrZero()
{
    return 0;
}

template <typename T>
typename std::enable_if<std::is_class<T>::value, T>::type NaNOrZero()
{
    return T{};
}
