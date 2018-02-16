#pragma once

/** exp10 from GNU libm fails to give precise result for integer arguments.
  * For example, exp10(3) gives 1000.0000000000001
  *  despite the fact that 1000 is exactly representable in double and float.
  * Better to always use implementation from MUSL.
  * Note: the musl_ prefix added to function names to avoid confusion with symbols from the system libm.
  */

#include <stdlib.h> /// for __THROW

// freebsd have no __THROW
#if !defined(__THROW)
#define __THROW
#endif

extern "C"
{

double musl_exp10(double x) __THROW;
double musl_pow10(double x) __THROW;
float musl_exp10f(float x) __THROW;
float musl_pow10f(float x) __THROW;

}
