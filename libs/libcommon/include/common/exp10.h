#pragma once

/** exp10 from GNU libm fails to give precise result for integer arguments.
  * For example, exp10(3) gives 1000.0000000000001
  *  despite the fact that 1000 is exactly representable in double and float.
  * Better to always use implementation from MUSL.
  */

#include <stdlib.h> /// for __THROW

// freebsd have no __THROW
#if !defined(__THROW)
#define __THROW
#endif

extern "C"
{

double exp10(double x) __THROW;
double pow10(double x) __THROW;
float exp10f(float x) __THROW;
float pow10f(float x) __THROW;

}
