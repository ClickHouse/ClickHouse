#ifndef _COMPLEX_IMPL_H
#define _COMPLEX_IMPL_H

#include <complex.h>
#include "libm.h"

#undef __CMPLX
#undef CMPLX
#undef CMPLXF
#undef CMPLXL

#define __CMPLX(x, y, t) \
	((union { _Complex t __z; t __xy[2]; }){.__xy = {(x),(y)}}.__z)

#define CMPLX(x, y) __CMPLX(x, y, double)
#define CMPLXF(x, y) __CMPLX(x, y, float)
#define CMPLXL(x, y) __CMPLX(x, y, long double)

hidden double complex __ldexp_cexp(double complex,int);
hidden float complex __ldexp_cexpf(float complex,int);

#endif
