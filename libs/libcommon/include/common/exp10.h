#pragma once

double musl_exp10(double x);

#if defined(__FreeBSD__)
//const auto& exp10 = musl_exp10; // it must be the name of a function with external linkage
#define exp10 musl_exp10
#endif
