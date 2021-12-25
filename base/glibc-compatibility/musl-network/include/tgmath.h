#ifndef _TGMATH_H
#define _TGMATH_H

/*
the return types are only correct with gcc (__GNUC__)
otherwise they are long double or long double complex

the long double version of a function is never chosen when
sizeof(double) == sizeof(long double)
(but the return type is set correctly with gcc)
*/

#include <math.h>
#include <complex.h>

#define __IS_FP(x) (sizeof((x)+1ULL) == sizeof((x)+1.0f))
#define __IS_CX(x) (__IS_FP(x) && sizeof(x) == sizeof((x)+I))
#define __IS_REAL(x) (__IS_FP(x) && 2*sizeof(x) == sizeof((x)+I))

#define __FLT(x) (__IS_REAL(x) && sizeof(x) == sizeof(float))
#define __LDBL(x) (__IS_REAL(x) && sizeof(x) == sizeof(long double) && sizeof(long double) != sizeof(double))

#define __FLTCX(x) (__IS_CX(x) && sizeof(x) == sizeof(float complex))
#define __DBLCX(x) (__IS_CX(x) && sizeof(x) == sizeof(double complex))
#define __LDBLCX(x) (__IS_CX(x) && sizeof(x) == sizeof(long double complex) && sizeof(long double) != sizeof(double))

/* return type */

#ifdef __GNUC__
/*
the result must be casted to the right type
(otherwise the result type is determined by the conversion
rules applied to all the function return types so it is long
double or long double complex except for integral functions)

this cannot be done in c99, so the typeof gcc extension is
used and that the type of ?: depends on wether an operand is
a null pointer constant or not
(in c11 _Generic can be used)

the c arguments below must be integer constant expressions
so they can be in null pointer constants
(__IS_FP above was carefully chosen this way)
*/
/* if c then t else void */
#define __type1(c,t) __typeof__(*(0?(t*)0:(void*)!(c)))
/* if c then t1 else t2 */
#define __type2(c,t1,t2) __typeof__(*(0?(__type1(c,t1)*)0:(__type1(!(c),t2)*)0))
/* cast to double when x is integral, otherwise use typeof(x) */
#define __RETCAST(x) ( \
	__type2(__IS_FP(x), __typeof__(x), double))
/* 2 args case, should work for complex types (cpow) */
#define __RETCAST_2(x, y) ( \
	__type2(__IS_FP(x) && __IS_FP(y), \
		__typeof__((x)+(y)), \
		__typeof__((x)+(y)+1.0)))
/* 3 args case (fma only) */
#define __RETCAST_3(x, y, z) ( \
	__type2(__IS_FP(x) && __IS_FP(y) && __IS_FP(z), \
		__typeof__((x)+(y)+(z)), \
		__typeof__((x)+(y)+(z)+1.0)))
/* drop complex from the type of x */
/* TODO: wrong when sizeof(long double)==sizeof(double) */
#define __RETCAST_REAL(x) (  \
	__type2(__IS_FP(x) && sizeof((x)+I) == sizeof(float complex), float, \
	__type2(sizeof((x)+1.0+I) == sizeof(double complex), double, \
		long double)))
/* add complex to the type of x */
#define __RETCAST_CX(x) (__typeof__(__RETCAST(x)0+I))
#else
#define __RETCAST(x)
#define __RETCAST_2(x, y)
#define __RETCAST_3(x, y, z)
#define __RETCAST_REAL(x)
#define __RETCAST_CX(x)
#endif

/* function selection */

#define __tg_real_nocast(fun, x) ( \
	__FLT(x) ? fun ## f (x) : \
	__LDBL(x) ? fun ## l (x) : \
	fun(x) )

#define __tg_real(fun, x) (__RETCAST(x)__tg_real_nocast(fun, x))

#define __tg_real_2_1(fun, x, y) (__RETCAST(x)( \
	__FLT(x) ? fun ## f (x, y) : \
	__LDBL(x) ? fun ## l (x, y) : \
	fun(x, y) ))

#define __tg_real_2(fun, x, y) (__RETCAST_2(x, y)( \
	__FLT(x) && __FLT(y) ? fun ## f (x, y) : \
	__LDBL((x)+(y)) ? fun ## l (x, y) : \
	fun(x, y) ))

#define __tg_complex(fun, x) (__RETCAST_CX(x)( \
	__FLTCX((x)+I) && __IS_FP(x) ? fun ## f (x) : \
	__LDBLCX((x)+I) ? fun ## l (x) : \
	fun(x) ))

#define __tg_complex_retreal(fun, x) (__RETCAST_REAL(x)( \
	__FLTCX((x)+I) && __IS_FP(x) ? fun ## f (x) : \
	__LDBLCX((x)+I) ? fun ## l (x) : \
	fun(x) ))

#define __tg_real_complex(fun, x) (__RETCAST(x)( \
	__FLTCX(x) ? c ## fun ## f (x) : \
	__DBLCX(x) ? c ## fun (x) : \
	__LDBLCX(x) ? c ## fun ## l (x) : \
	__FLT(x) ? fun ## f (x) : \
	__LDBL(x) ? fun ## l (x) : \
	fun(x) ))

/* special cases */

#define __tg_real_remquo(x, y, z) (__RETCAST_2(x, y)( \
	__FLT(x) && __FLT(y) ? remquof(x, y, z) : \
	__LDBL((x)+(y)) ? remquol(x, y, z) : \
	remquo(x, y, z) ))

#define __tg_real_fma(x, y, z) (__RETCAST_3(x, y, z)( \
	__FLT(x) && __FLT(y) && __FLT(z) ? fmaf(x, y, z) : \
	__LDBL((x)+(y)+(z)) ? fmal(x, y, z) : \
	fma(x, y, z) ))

#define __tg_real_complex_pow(x, y) (__RETCAST_2(x, y)( \
	__FLTCX((x)+(y)) && __IS_FP(x) && __IS_FP(y) ? cpowf(x, y) : \
	__FLTCX((x)+(y)) ? cpow(x, y) : \
	__DBLCX((x)+(y)) ? cpow(x, y) : \
	__LDBLCX((x)+(y)) ? cpowl(x, y) : \
	__FLT(x) && __FLT(y) ? powf(x, y) : \
	__LDBL((x)+(y)) ? powl(x, y) : \
	pow(x, y) ))

#define __tg_real_complex_fabs(x) (__RETCAST_REAL(x)( \
	__FLTCX(x) ? cabsf(x) : \
	__DBLCX(x) ? cabs(x) : \
	__LDBLCX(x) ? cabsl(x) : \
	__FLT(x) ? fabsf(x) : \
	__LDBL(x) ? fabsl(x) : \
	fabs(x) ))

/* suppress any macros in math.h or complex.h */

#undef acos
#undef acosh
#undef asin
#undef asinh
#undef atan
#undef atan2
#undef atanh
#undef carg
#undef cbrt
#undef ceil
#undef cimag
#undef conj
#undef copysign
#undef cos
#undef cosh
#undef cproj
#undef creal
#undef erf
#undef erfc
#undef exp
#undef exp2
#undef expm1
#undef fabs
#undef fdim
#undef floor
#undef fma
#undef fmax
#undef fmin
#undef fmod
#undef frexp
#undef hypot
#undef ilogb
#undef ldexp
#undef lgamma
#undef llrint
#undef llround
#undef log
#undef log10
#undef log1p
#undef log2
#undef logb
#undef lrint
#undef lround
#undef nearbyint
#undef nextafter
#undef nexttoward
#undef pow
#undef remainder
#undef remquo
#undef rint
#undef round
#undef scalbln
#undef scalbn
#undef sin
#undef sinh
#undef sqrt
#undef tan
#undef tanh
#undef tgamma
#undef trunc

/* tg functions */

#define acos(x)         __tg_real_complex(acos, (x))
#define acosh(x)        __tg_real_complex(acosh, (x))
#define asin(x)         __tg_real_complex(asin, (x))
#define asinh(x)        __tg_real_complex(asinh, (x))
#define atan(x)         __tg_real_complex(atan, (x))
#define atan2(x,y)      __tg_real_2(atan2, (x), (y))
#define atanh(x)        __tg_real_complex(atanh, (x))
#define carg(x)         __tg_complex_retreal(carg, (x))
#define cbrt(x)         __tg_real(cbrt, (x))
#define ceil(x)         __tg_real(ceil, (x))
#define cimag(x)        __tg_complex_retreal(cimag, (x))
#define conj(x)         __tg_complex(conj, (x))
#define copysign(x,y)   __tg_real_2(copysign, (x), (y))
#define cos(x)          __tg_real_complex(cos, (x))
#define cosh(x)         __tg_real_complex(cosh, (x))
#define cproj(x)        __tg_complex(cproj, (x))
#define creal(x)        __tg_complex_retreal(creal, (x))
#define erf(x)          __tg_real(erf, (x))
#define erfc(x)         __tg_real(erfc, (x))
#define exp(x)          __tg_real_complex(exp, (x))
#define exp2(x)         __tg_real(exp2, (x))
#define expm1(x)        __tg_real(expm1, (x))
#define fabs(x)         __tg_real_complex_fabs(x)
#define fdim(x,y)       __tg_real_2(fdim, (x), (y))
#define floor(x)        __tg_real(floor, (x))
#define fma(x,y,z)      __tg_real_fma((x), (y), (z))
#define fmax(x,y)       __tg_real_2(fmax, (x), (y))
#define fmin(x,y)       __tg_real_2(fmin, (x), (y))
#define fmod(x,y)       __tg_real_2(fmod, (x), (y))
#define frexp(x,y)      __tg_real_2_1(frexp, (x), (y))
#define hypot(x,y)      __tg_real_2(hypot, (x), (y))
#define ilogb(x)        __tg_real_nocast(ilogb, (x))
#define ldexp(x,y)      __tg_real_2_1(ldexp, (x), (y))
#define lgamma(x)       __tg_real(lgamma, (x))
#define llrint(x)       __tg_real_nocast(llrint, (x))
#define llround(x)      __tg_real_nocast(llround, (x))
#define log(x)          __tg_real_complex(log, (x))
#define log10(x)        __tg_real(log10, (x))
#define log1p(x)        __tg_real(log1p, (x))
#define log2(x)         __tg_real(log2, (x))
#define logb(x)         __tg_real(logb, (x))
#define lrint(x)        __tg_real_nocast(lrint, (x))
#define lround(x)       __tg_real_nocast(lround, (x))
#define nearbyint(x)    __tg_real(nearbyint, (x))
#define nextafter(x,y)  __tg_real_2(nextafter, (x), (y))
#define nexttoward(x,y) __tg_real_2(nexttoward, (x), (y))
#define pow(x,y)        __tg_real_complex_pow((x), (y))
#define remainder(x,y)  __tg_real_2(remainder, (x), (y))
#define remquo(x,y,z)   __tg_real_remquo((x), (y), (z))
#define rint(x)         __tg_real(rint, (x))
#define round(x)        __tg_real(round, (x))
#define scalbln(x,y)    __tg_real_2_1(scalbln, (x), (y))
#define scalbn(x,y)     __tg_real_2_1(scalbn, (x), (y))
#define sin(x)          __tg_real_complex(sin, (x))
#define sinh(x)         __tg_real_complex(sinh, (x))
#define sqrt(x)         __tg_real_complex(sqrt, (x))
#define tan(x)          __tg_real_complex(tan, (x))
#define tanh(x)         __tg_real_complex(tanh, (x))
#define tgamma(x)       __tg_real(tgamma, (x))
#define trunc(x)        __tg_real(trunc, (x))

#endif
