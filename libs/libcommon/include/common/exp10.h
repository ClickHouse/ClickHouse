#pragma once

#include <cmath>

double musl_exp10(double x);

#if defined(__FreeBSD__)
#define exp10 musl_exp10
#endif
