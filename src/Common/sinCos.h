#pragma once

#include <cmath>

namespace DB
{

/// Computes `std::sin(x)` and `std::cos(x)` with one call to `sincos` where the C library has it: the two share
/// one argument reduction. Compilers merge such a pair into `sincos` by themselves only for some targets (glibc,
/// but not musl). With llvm-libc's correctly rounded `sincos` the results are the same as from separate calls.
inline void sinCos(double x, double & sin_x, double & cos_x)
{
#if defined(OS_LINUX) || defined(OS_FREEBSD)
    ::sincos(x, &sin_x, &cos_x);
#else
    sin_x = std::sin(x);
    cos_x = std::cos(x);
#endif
}

}
