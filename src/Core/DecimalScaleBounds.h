#pragma once

#include <base/arithmeticOverflow.h>

#include <limits>

namespace DB
{

/// The values whose product with a positive `multiplier` fits into `T`. Unlike `common::mulOverflow`, works for big integers.
template <typename T>
struct DecimalScaleBounds
{
    T multiplier;
    T max_multiplicand;
    T min_multiplicand;

    explicit DecimalScaleBounds(T multiplier_)
        : multiplier(multiplier_)
        , max_multiplicand(std::numeric_limits<T>::max() / multiplier_)
        , min_multiplicand(std::numeric_limits<T>::min() / multiplier_)
    {
    }

    [[nodiscard]] bool fits(T value) const { return value >= min_multiplicand && value <= max_multiplicand; }

    /// Same contract as `common::mulOverflow(value, multiplier, res)`.
    bool mulOverflow(T value, T & res) const
    {
        if constexpr (is_big_int_v<T>)
        {
            res = common::mulIgnoreOverflow(value, multiplier);
            return !fits(value);
        }
        else
            return common::mulOverflow(value, multiplier, res);
    }
};

}
