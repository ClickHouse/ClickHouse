#pragma once

#include <cmath>

double musl_exp10(double x);

#if defined(__FreeBSD__)
#define exp10 musl_exp10
#endif

#ifdef __APPLE__
#define exp10 __exp10
#endif
