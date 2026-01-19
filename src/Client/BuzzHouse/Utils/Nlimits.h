#pragma once

#include <limits>

namespace BuzzHouse
{

template <class T>
struct NumericLimits
{
    static constexpr T minimum() { return std::numeric_limits<T>::lowest(); }
    static constexpr T maximum() { return std::numeric_limits<T>::max(); }
};

}
