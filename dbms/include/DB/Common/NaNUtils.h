#pragma once

#include <cmath>
#include <limits>
#include <type_traits>


/// To be sure, that this function is zero-cost for non-floating point types.
template <typename T>
inline bool isNaN(T x)
{
    return std::is_floating_point<T>::value ? std::isnan(x) : false;
}

template <typename T>
inline bool isFinite(T x)
{
    return std::is_floating_point<T>::value ? std::isfinite(x) : true;
}


template <typename T>
typename std::enable_if<std::is_floating_point<T>::value, T>::type NaNOrZero()
{
    return std::numeric_limits<T>::quiet_NaN();
}

template <typename T>
typename std::enable_if<!std::is_floating_point<T>::value, T>::type NaNOrZero()
{
    return 0;
}
