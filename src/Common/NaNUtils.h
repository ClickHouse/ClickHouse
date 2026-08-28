#pragma once

#include <cmath>
#include <limits>
#include <type_traits>
#include <base/DecomposedFloat.h>


template <typename T>
inline bool isNaN(T x)
{
    /// To be sure, that this function is zero-cost for non-floating point types.
    if constexpr (is_floating_point<T>)
        return DecomposedFloat<T>(x).isNaN();
    else
        return false;
}

template <typename T>
inline bool isFinite(T x)
{
    if constexpr (is_floating_point<T>)
        return DecomposedFloat<T>(x).isFinite();
    else
        return true;
}

template <typename T>
bool canConvertTo(Float64 x)
{
    if constexpr (is_floating_point<T>)
        return true;
    if (!isFinite(x))
        return false;
    if (x > Float64(std::numeric_limits<T>::max()) || x < Float64(std::numeric_limits<T>::lowest()))
        return false;

    return true;
}

template <typename T>
T NaNOrZero()
{
    if constexpr (std::is_floating_point_v<T>)
        return std::numeric_limits<T>::quiet_NaN();
    if constexpr (std::is_same_v<T, BFloat16>)
        return BFloat16(std::numeric_limits<Float32>::quiet_NaN());
    return {};
}

template <typename T>
bool signBit(T x)
{
    if constexpr (is_floating_point<T>)
        return DecomposedFloat<T>(x).isNegative();
    else
        return x < 0;
}
