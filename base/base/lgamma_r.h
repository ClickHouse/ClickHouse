#pragma once

#include <cmath>

#if defined(OS_WINDOWS)

/// `lgamma_r` is `lgamma` with the sign of the gamma function returned through a pointer instead
/// of through the global `signgam`, which makes it thread-safe. It is a GNU extension; the Windows
/// CRT has only `lgamma`, whose `signgam` is per-thread there rather than global - so the sign can
/// be read back safely, and this wrapper is reentrant for the same reason the GNU one is.
inline double lgamma_r(double x, int * signp)
{
    const double result = std::lgamma(x);
    *signp = signgam;
    return result;
}

#endif
