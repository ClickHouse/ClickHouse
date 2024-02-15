#pragma once

#include <base/defines.h>
#include <base/types.h>

#include <cmath>

namespace DB
{

/** Stuff for comparing numbers.
  * Integer values are compared as usual.
  * Floating-point numbers are compared this way that NaNs always end up at the end
  *  (if you don't do this, the sort would not work at all).
  */
template <class T, class U = T>
struct CompareHelper
{
    static constexpr bool less(T a, U b, int /*nan_direction_hint*/) { return a < b; }
    static constexpr bool greater(T a, U b, int /*nan_direction_hint*/) { return a > b; }
    static constexpr bool equals(T a, U b, int /*nan_direction_hint*/) { return a == b; }

    /** Compares two numbers. Returns a number less than zero, equal to zero, or greater than zero if a < b, a == b, a > b, respectively.
      * If one of the values is NaN, then
      * - if nan_direction_hint == -1 - NaN are considered less than all numbers;
      * - if nan_direction_hint == 1 - NaN are considered to be larger than all numbers;
      * Essentially: nan_direction_hint == -1 says that the comparison is for sorting in descending order.
      */
    static constexpr int compare(T a, U b, int /*nan_direction_hint*/) { return a > b ? 1 : (a < b ? -1 : 0); }
};

template <class T>
struct FloatCompareHelper
{
    static constexpr bool less(T a, T b, int nan_direction_hint)
    {
        const bool isnan_a = std::isnan(a);
        const bool isnan_b = std::isnan(b);

        if (isnan_a && isnan_b)
            return false;
        if (isnan_a)
            return nan_direction_hint < 0;
        if (isnan_b)
            return nan_direction_hint > 0;

        return a < b;
    }

    static constexpr bool greater(T a, T b, int nan_direction_hint)
    {
        const bool isnan_a = std::isnan(a);
        const bool isnan_b = std::isnan(b);

        if (isnan_a && isnan_b)
            return false;
        if (isnan_a)
            return nan_direction_hint > 0;
        if (isnan_b)
            return nan_direction_hint < 0;

        return a > b;
    }

    static constexpr bool equals(T a, T b, int nan_direction_hint) { return compare(a, b, nan_direction_hint) == 0; }

    static constexpr int compare(T a, T b, int nan_direction_hint)
    {
        const bool isnan_a = std::isnan(a);
        const bool isnan_b = std::isnan(b);

        if (unlikely(isnan_a || isnan_b))
        {
            if (isnan_a && isnan_b)
                return 0;

            return isnan_a ? nan_direction_hint : -nan_direction_hint;
        }

        return (T(0) < (a - b)) - ((a - b) < T(0));
    }
};

template <typename U>
struct CompareHelper<Float32, U> : public FloatCompareHelper<Float32>
{
};
template <typename U>
struct CompareHelper<Float64, U> : public FloatCompareHelper<Float64>
{
};

}
