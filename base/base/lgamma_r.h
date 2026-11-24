#pragma once

#include <cmath>

#if defined(OS_WINDOWS)

/// `lgamma_r` is `lgamma` with the sign of the gamma function returned through a pointer instead
/// of through the global `signgam`, which makes it thread-safe. It is a GNU extension that the
/// Windows CRT lacks, and it cannot be emulated on top of `lgamma`: mingw-w64's `signgam` is a
/// process-global variable, so concurrent calls race on it, and its `lgamma` reads that shared sign
/// back for the overflow result as well. The implementation is musl's, compiled into `common` on
/// Windows from `base/glibc-compatibility/musl/lgamma.c`; it never touches `signgam`.
extern "C" double lgamma_r(double x, int * signgamp);

#endif
