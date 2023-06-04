#pragma once

#include <cmath>
#include <limits>
#include <type_traits>
#include <base/extended_types.h>


template <typename T>
inline bool isNaN(T x)
{
    /// To be sure, that this function is zero-cost for non-floating point types.
    if constexpr (is_floating_point<T>)
        return std::isnan(x);
    else
        return false;
}


template <typename T>
inline bool isFinite(T x)
{
    if constexpr (std::is_floating_point_v<T>)
        return std::isfinite(x);
    else
        return true;
}


template <typename T>
T NaNOrZero()
{
    if constexpr (std::is_floating_point_v<T>)
        return std::numeric_limits<T>::quiet_NaN();
    else
        return {};
}
