#pragma once

#include <cmath>

namespace DB
{

/// Computes `std::sin(x)` and `std::cos(x)` with one call to `sincos` where the C library has it: the two share
/// one argument reduction. Compilers merge such a pair into `sincos` by themselves only for some targets (glibc,
/// but not musl). With llvm-libc's correctly rounded `sincos` the results are the same as from separate calls.
struct SinCos
{
    double sin;
    double cos;
};

inline SinCos sinCos(double x)
{
    SinCos result{};
#if defined(OS_LINUX) || defined(OS_FREEBSD)
    ::sincos(x, &result.sin, &result.cos);
#else
    result.sin = std::sin(x);
    result.cos = std::cos(x);
#endif
    return result;
}

}
