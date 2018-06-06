#pragma once

#include <common/Types.h>

/** Almost the same as x = x * exp10(exponent), but gives more accurate result.
  * Example:
  *  5 * 1e-11 = 4.9999999999999995e-11
  *  !=
  *  5e-11 = shift10(5.0, -11)
  */

double shift10(double x, int exponent);
float shift10(float x, int exponent);

double shift10(UInt64 x, int exponent);
double shift10(Int64 x, int exponent);
