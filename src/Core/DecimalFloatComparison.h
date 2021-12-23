#pragma once

#include <base/DecomposedFloat.h>

namespace DB
{
struct DecimalFloatComparison
{
    template <typename Float, typename Int>
    static int compare(Float a, Int b, Int scale)
    {
        /// TODO need to implement comparison
        if (a)
            return -1;
        if (b)
            return 0;
        if (scale)
            return 1;
        return 0;
    }

    template <typename Float, typename Int>
    static bool equals(Float a, Int b, Int scale)
    {
        return compare(a, b, scale) == 0;
    }

    template <typename Float, typename Int>
    static bool notEquals(Float a, Int b, Int scale)
    {
        return compare(a, b, scale) != 0;
    }

    template <typename Float, typename Int>
    static bool less(Float a, Int b, Int scale)
    {
        return compare(a, b, scale) < 0;
    }

    template <typename Float, typename Int>
    static bool greater(Float a, Int b, Int scale)
    {
        return compare(a, b, scale) > 0;
    }

    template <typename Float, typename Int>
    static bool lessOrEquals(Float a, Int b, Int scale)
    {
        return compare(a, b, scale) <= 0;
    }

    template <typename Float, typename Int>
    static bool greaterOrEquals(Float a, Int b, Int scale)
    {
        return compare(a, b, scale) >= 0;
    }
};
}
