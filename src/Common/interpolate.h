#pragma once

#include <cmath>


inline double interpolateLinear(double min, double max, double ratio)
{
    return min + (max - min) * ratio;
}


/** It is linear interpolation in logarithmic coordinates.
  * Exponential interpolation is related to linear interpolation
  *  exactly in same way as geometric mean is related to arithmetic mean.
  * 'min' must be greater than zero, 'ratio' must be from 0 to 1.
  */
inline double interpolateExponential(double min, double max, double ratio)
{
    return min * std::pow(max / min, ratio);
}
