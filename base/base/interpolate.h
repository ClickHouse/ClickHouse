#pragma once
#include <cmath>
#include <base/defines.h>

/** Linear interpolation in logarithmic coordinates.
  * Exponential interpolation is related to linear interpolation
  * exactly in same way as geometric mean is related to arithmetic mean.
  */
constexpr double interpolateExponential(double min, double max, double ratio)
{
    chassert(min > 0 && ratio >= 0 && ratio <= 1);
    return min * std::pow(max / min, ratio);
}

constexpr double interpolateLinear(double min, double max, double ratio)
{
    return std::lerp(min, max, ratio);
}
