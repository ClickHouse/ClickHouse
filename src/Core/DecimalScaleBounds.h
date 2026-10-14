#pragma once

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

    bool fits(T value) const { return value >= min_multiplicand && value <= max_multiplicand; }
};

}
