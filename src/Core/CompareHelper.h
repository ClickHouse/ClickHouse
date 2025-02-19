#pragma once

#include <base/defines.h>
#include <base/types.h>
#include <Common/NaNUtils.h>


namespace DB
{

/** Stuff for comparing numbers.
  * Integer values are compared as usual.
  * Floating-point numbers are compared this way that NaNs always end up at the end
  *  (if you don't do this, the sort would not work at all).
  */
template <typename T, typename U = T>
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

template <typename T>
struct FloatCompareHelper
{
    /// NaN semantics follow Apache Spark: https://spark.apache.org/docs/3.5.3/sql-ref-datatypes.html#nan-semantics.
    static constexpr bool less(T a, T b, int )
    {
        const bool isnan_a = isNaN(a);
        const bool isnan_b = isNaN(b);

        if (isnan_a && isnan_b)
            return false;
        if (isnan_a)
            return false;
        if (isnan_b)
            return true;

        return a < b;
    }

    static constexpr bool greater(T a, T b, int )
    {
        const bool isnan_a = isNaN(a);
        const bool isnan_b = isNaN(b);

        if (isnan_a && isnan_b)
            return false;
        if (isnan_a)
            return true;
        if (isnan_b)
            return false;

        return a > b;
    }

    static constexpr bool equals(T a, T b, int nan_direction_hint) { return compare(a, b, nan_direction_hint) == 0; }

    static constexpr int compare(T a, T b, int )
    {
        const bool isnan_a = isNaN(a);
        const bool isnan_b = isNaN(b);

        if (unlikely(isnan_a || isnan_b))
        {
            if (isnan_a && isnan_b)
                return 0;

            return isnan_a ? 1 : -1;
        }

        return (T(0) < (a - b)) - ((a - b) < T(0));
    }
};

template <typename U>
struct CompareHelper<BFloat16, U> : public FloatCompareHelper<BFloat16>
{
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
