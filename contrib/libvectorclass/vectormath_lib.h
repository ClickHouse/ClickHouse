/****************************  vectormath_lib.h   *****************************
| Author:        Agner Fog
| Date created:  2012-05-30
* Last modified: 2014-04-23
| Version:       1.16
| Project:       vector classes
| Description:
| Header file defining mathematical functions on floating point vectors
| May use Intel SVML library or AMD LIBM library
|
| Instructions:
| Define VECTORMATH to one of the following values:
|   0:  Use ordinary math library (slow)
|   1:  Use AMD LIBM library
|   2:  Use Intel SVML library with any compiler
|   3:  Use Intel SVML library with Intel compiler
|
| For detailed instructions, see VectorClass.pdf
|
| (c) Copyright 2012-2014 GNU General Public License http://www.gnu.org/licenses
\*****************************************************************************/

// check combination of header files
#ifndef VECTORMATH_LIB_H
#define VECTORMATH_LIB_H

#include "vectorf128.h"

#ifndef VECTORMATH
#ifdef __INTEL_COMPILER
#define VECTORMATH 3
#else
#define VECTORMATH 0
#endif // __INTEL_COMPILER
#endif // VECTORMATH

/*****************************************************************************
*
*      VECTORMATH = 0. Use ordinary library (scalar)
*
*****************************************************************************/
#if VECTORMATH == 0
#include <math.h>

#ifndef VECTORMATH_COMMON_H
// exponential and power functions
static inline Vec4f exp (Vec4f const & x) {
    float xx[4];
    x.store(xx);
    return Vec4f(expf(xx[0]), expf(xx[1]), expf(xx[2]), expf(xx[3]));
}
static inline Vec2d exp (Vec2d const & x) {
    double xx[4];
    x.store(xx);
    return Vec2d(exp(xx[0]), exp(xx[1]));
}

// There is no certain way to know which functions are available, but at least some (Gnu)
// compilers have defines to specify this
#ifdef HAVE_EXPM1
static inline Vec4f expm1 (Vec4f const & x) {
    float xx[4];
    x.store(xx);
    return Vec4f(expm1(xx[0]), expm1(xx[1]), expm1(xx[2]), expm1(xx[3]));
}
static inline Vec2d expm1 (Vec2d const & x) {
    double xx[4];
    x.store(xx);
    return Vec2d(expm1(xx[0]), expm1(xx[1]));
}
#endif

#ifdef HAVE_EXP2
static inline Vec4f exp2 (Vec4f const & x) {
    float xx[4];
    x.store(xx);
    return Vec4f(exp2(xx[0]), exp2(xx[1]), exp2(xx[2]), exp2(xx[3]));
}
static inline Vec2d exp2 (Vec2d const & x) {
    double xx[4];
    x.store(xx);
    return Vec2d(exp2(xx[0]), exp2(xx[1]));
}
#else
static inline Vec4f exp2 (Vec4f const & x) {
    return exp(x*Vec4f(0.693147180559945309417f /* log(2) */));
}
static inline Vec2d exp2 (Vec2d const & x) {
    return exp(x*Vec2d(0.693147180559945309417  /* log(2) */));
}
#endif

static inline Vec4f exp10 (Vec4f const & x) {
    return exp(x*Vec4f(2.30258509299404568402f /* log(10) */));
}
static inline Vec2d exp10 (Vec2d const & x) {
    return exp(x*Vec2d(2.30258509299404568402  /* log(10) */));
}

static inline Vec4f pow (Vec4f const & a, Vec4f const & b) {
    float aa[4], bb[4];
    a.store(aa);  b.store(bb);
    return Vec4f(powf(aa[0],bb[0]), powf(aa[1],bb[1]), powf(aa[2],bb[2]), powf(aa[3],bb[3]));
}
static inline Vec2d pow (Vec2d const & a, Vec2d const & b) {
    double aa[4], bb[4];
    a.store(aa);  b.store(bb);
    return Vec2d(pow(aa[0],bb[0]), pow(aa[1],bb[1]));
}

static inline Vec4f log (Vec4f const & x) {
    float xx[4];
    x.store(xx);
    return Vec4f(log(xx[0]), log(xx[1]), log(xx[2]), log(xx[3]));
}
static inline Vec2d log (Vec2d const & x) {
    double xx[4];
    x.store(xx);
    return Vec2d(log(xx[0]), log(xx[1]));
}

#ifdef HAVE_LOG1P
static inline Vec4f log1p (Vec4f const & x) {
    float xx[4];
    x.store(xx);
    return Vec4f(log1p(xx[0]), log1p(xx[1]), log1p(xx[2]), log1p(xx[3]));
}
static inline Vec2d log1p (Vec2d const & x) {
    double xx[4];
    x.store(xx);
    return Vec2d(log1p(xx[0]), log1p(xx[1]));
}
#endif

static inline Vec4f log2 (Vec4f const & x) {   // logarithm base 2
    return log(x)*Vec4f(1.44269504088896340736f/* log2(e) */);
}
static inline Vec2d log2 (Vec2d const & x) {   // logarithm base 2
    return log(x)*Vec2d(1.44269504088896340736 /* log2(e) */);
}

static inline Vec4f log10 (Vec4f const & x) {  // logarithm base 10
    float xx[4];
    x.store(xx);
    return Vec4f(log10f(xx[0]), log10f(xx[1]), log10f(xx[2]), log10f(xx[3]));
}
static inline Vec2d log10 (Vec2d const & x) {  // logarithm base 10
    double xx[4];
    x.store(xx);
    return Vec2d(log10(xx[0]), log10(xx[1]));
}

// trigonometric functions
static inline Vec4f sin(Vec4f const & x) {
    float xx[4];
    x.store(xx);
    return Vec4f(sinf(xx[0]), sinf(xx[1]), sinf(xx[2]), sinf(xx[3]));
}
static inline Vec2d sin (Vec2d const & x) {
    double xx[4];
    x.store(xx);
    return Vec2d(sin(xx[0]), sin(xx[1]));
}

static inline Vec4f cos(Vec4f const & x) {
    float xx[4];
    x.store(xx);
    return Vec4f(cosf(xx[0]), cosf(xx[1]), cosf(xx[2]), cosf(xx[3]));
}
static inline Vec2d cos (Vec2d const & x) {
    double xx[4];
    x.store(xx);
    return Vec2d(cos(xx[0]), cos(xx[1]));
}

static inline Vec4f sincos (Vec4f * pcos, Vec4f const & x) {   // sine and cosine. sin(x) returned, cos(x) in pcos
    *pcos = cos(x);
    return sin(x);
}
static inline Vec2d sincos (Vec2d * pcos, Vec2d const & x) {   // sine and cosine. sin(x) returned, cos(x) in pcos
    *pcos = cos(x);
    return sin(x);
}

static inline Vec4f tan(Vec4f const & x) {
    float xx[4];
    x.store(xx);
    return Vec4f(tanf(xx[0]), tanf(xx[1]), tanf(xx[2]), tanf(xx[3]));
}
static inline Vec2d tan (Vec2d const & x) {
    double xx[4];
    x.store(xx);
    return Vec2d(tan(xx[0]), tan(xx[1]));
}

// inverse trigonometric functions
static inline Vec4f asin(Vec4f const & x) {
    float xx[4];
    x.store(xx);
    return Vec4f(asinf(xx[0]), asinf(xx[1]), asinf(xx[2]), asinf(xx[3]));
}
static inline Vec2d asin (Vec2d const & x) {
    double xx[4];
    x.store(xx);
    return Vec2d(asin(xx[0]), asin(xx[1]));
}

static inline Vec4f acos(Vec4f const & x) {
    float xx[4];
    x.store(xx);
    return Vec4f(acosf(xx[0]), acosf(xx[1]), acosf(xx[2]), acosf(xx[3]));
}
static inline Vec2d acos (Vec2d const & x) {
    double xx[4];
    x.store(xx);
    return Vec2d(acos(xx[0]), acos(xx[1]));
}

static inline Vec4f atan(Vec4f const & x) {
    float xx[4];
    x.store(xx);
    return Vec4f(atanf(xx[0]), atanf(xx[1]), atanf(xx[2]), atanf(xx[3]));
}
static inline Vec2d atan (Vec2d const & x) {
    double xx[4];
    x.store(xx);
    return Vec2d(atan(xx[0]), atan(xx[1]));
}

static inline Vec4f atan2 (Vec4f const & a, Vec4f const & b) {   // inverse tangent of a/b
    float aa[4], bb[4];
    a.store(aa);  b.store(bb);
    return Vec4f(atan2f(aa[0],bb[0]), atan2f(aa[1],bb[1]), atan2f(aa[2],bb[2]), atan2f(aa[3],bb[3]));
}
static inline Vec2d atan2 (Vec2d const & a, Vec2d const & b) {   // inverse tangent of a/b
    double aa[4], bb[4];
    a.store(aa);  b.store(bb);
    return Vec2d(atan2(aa[0],bb[0]), atan2(aa[1],bb[1]));
}
#endif // VECTORMATH_COMMON_H

// hyperbolic functions
static inline Vec4f sinh(Vec4f const & x) {   // hyperbolic sine
    float xx[4];
    x.store(xx);
    return Vec4f(sinhf(xx[0]), sinhf(xx[1]), sinhf(xx[2]), sinhf(xx[3]));
}
static inline Vec2d sinh (Vec2d const & x) {
    double xx[4];
    x.store(xx);
    return Vec2d(sinh(xx[0]), sinh(xx[1]));
}

static inline Vec4f cosh(Vec4f const & x) {   // hyperbolic cosine
    float xx[4];
    x.store(xx);
    return Vec4f(coshf(xx[0]), coshf(xx[1]), coshf(xx[2]), coshf(xx[3]));
}
static inline Vec2d cosh (Vec2d const & x) {
    double xx[4];
    x.store(xx);
    return Vec2d(cosh(xx[0]), cosh(xx[1]));
}

static inline Vec4f tanh(Vec4f const & x) {   // hyperbolic tangent
    float xx[4];
    x.store(xx);
    return Vec4f(tanhf(xx[0]), tanhf(xx[1]), tanhf(xx[2]), tanhf(xx[3]));
}
static inline Vec2d tanh (Vec2d const & x) {
    double xx[4];
    x.store(xx);
    return Vec2d(tanh(xx[0]), tanh(xx[1]));
}

// error function
#ifdef HAVE_ERF
static inline Vec4f erf(Vec4f const & x) {
    float xx[4];
    x.store(xx);
    return Vec4f(erf(xx[0]), erf(xx[1]), erf(xx[2]), erf(xx[3]));
}
static inline Vec2d erf (Vec2d const & x) {
    double xx[4];
    x.store(xx);
    return Vec2d(erf(xx[0]), erf(xx[1]));
}
#endif

#ifdef HAVE_ERFC
static inline Vec4f erfc(Vec4f const & x) {
    float xx[4];
    x.store(xx);
    return Vec4f(erfc(xx[0]), erfc(xx[1]), erfc(xx[2]), erfc(xx[3]));
}
static inline Vec2d erfc (Vec2d const & x) {
    double xx[4];
    x.store(xx);
    return Vec2d(erfc(xx[0]), erfc(xx[1]));
}
#endif

// complex exponential function (real part in even numbered elements, imaginary part in odd numbered elements)
static inline Vec4f cexp (Vec4f const & x) {   // complex exponential function
    float xx[4], ee[2];
    x.store(xx);
    Vec4f z(cosf(xx[1]),sinf(xx[1]),cosf(xx[3]),sinf(xx[3])); 
    ee[0] = expf(xx[0]);  ee[1] = expf(xx[2]);
    return z * Vec4f(ee[0],ee[0],ee[1],ee[1]);
}

static inline Vec2d cexp (Vec2d const & x) {   // complex exponential function
    double xx[2];
    x.store(xx);
    Vec2d z(cos(xx[1]), sin(xx[1]));
    return z * exp(xx[0]);
}

#if defined (VECTORF256_H)  // 256 bit vectors defined

#ifndef VECTORMATH_COMMON_H

// exponential and power functions
static inline Vec8f exp (Vec8f const & x) {   // exponential function
    return Vec8f(exp(x.get_low()), exp(x.get_high()));
}
static inline Vec4d exp (Vec4d const & x) {   // exponential function
    return Vec4d(exp(x.get_low()), exp(x.get_high()));
}
#ifdef HAVE_EXPM1
static inline Vec8f expm1 (Vec8f const & x) {   // exp(x)-1
    return Vec8f(expm1(x.get_low()), expm1(x.get_high()));
}
static inline Vec4d expm1 (Vec4d const & x) {   // exp(x)-1
    return Vec4d(expm1(x.get_low()), expm1(x.get_high()));
}
#endif

static inline Vec8f exp2 (Vec8f const & x) {   // pow(2,x)
    return Vec8f(exp2(x.get_low()), exp2(x.get_high()));
}
static inline Vec4d exp2 (Vec4d const & x) {   // pow(2,x)
    return Vec4d(exp2(x.get_low()), exp2(x.get_high()));
}

static inline Vec8f exp10 (Vec8f const & x) {   // pow(10,x)
    return Vec8f(exp10(x.get_low()), exp10(x.get_high()));
}
static inline Vec4d exp10 (Vec4d const & x) {   // pow(10,x)
    return Vec4d(exp10(x.get_low()), exp10(x.get_high()));
}

static inline Vec8f pow (Vec8f const & a, Vec8f const & b) {   // pow(a,b) = a to the power of b
    return Vec8f(pow(a.get_low(),b.get_low()), pow(a.get_high(),b.get_high()));
}
static inline Vec4d pow (Vec4d const & a, Vec4d const & b) {   // pow(a,b) = a to the power of b
    return Vec4d(pow(a.get_low(),b.get_low()), pow(a.get_high(),b.get_high()));
}

// logarithms
static inline Vec8f log (Vec8f const & x) {   // natural logarithm
    return Vec8f(log(x.get_low()), log(x.get_high()));
}
static inline Vec4d log (Vec4d const & x) {   // natural logarithm
    return Vec4d(log(x.get_low()), log(x.get_high()));
}
#ifdef HAVE_LOG1P
static inline Vec8f log1p (Vec8f const & x) {   // log(1+x). Avoids loss of precision if 1+x is close to 1
    return Vec8f(log1p(x.get_low()), log1p(x.get_high()));
}
static inline Vec4d log1p (Vec4d const & x) {   // log(1+x). Avoids loss of precision if 1+x is close to 1
    return Vec4d(log1p(x.get_low()), log1p(x.get_high()));
}
#endif

static inline Vec8f log2 (Vec8f const & x) {   // logarithm base 2
    return Vec8f(log2(x.get_low()), log2(x.get_high()));
}
static inline Vec4d log2 (Vec4d const & x) {   // logarithm base 2
    return Vec4d(log2(x.get_low()), log2(x.get_high()));
}

static inline Vec8f log10 (Vec8f const & x) {   // logarithm base 10
    return Vec8f(log10(x.get_low()), log10(x.get_high()));
}
static inline Vec4d log10 (Vec4d const & x) {   // logarithm base 10
    return Vec4d(log10(x.get_low()), log10(x.get_high()));
}

// trigonometric functions (angles in radians)
static inline Vec8f sin (Vec8f const & x) {   // sine
    return Vec8f(sin(x.get_low()), sin(x.get_high()));
}
static inline Vec4d sin (Vec4d const & x) {   // sine
    return Vec4d(sin(x.get_low()), sin(x.get_high()));
}

static inline Vec8f cos (Vec8f const & x) {   // cosine
    return Vec8f(cos(x.get_low()), cos(x.get_high()));
}
static inline Vec4d cos (Vec4d const & x) {   // cosine
    return Vec4d(cos(x.get_low()), cos(x.get_high()));
}

static inline Vec8f sincos (Vec8f * pcos, Vec8f const & x) {   // sine and cosine. sin(x) returned, cos(x) in pcos
    *pcos = Vec8f(cos(x.get_low()), cos(x.get_high())); 
    return Vec8f(sin(x.get_low()), sin(x.get_high()));
}
static inline Vec4d sincos (Vec4d * pcos, Vec4d const & x) {   // sine and cosine. sin(x) returned, cos(x) in pcos
    *pcos = Vec4d(cos(x.get_low()), cos(x.get_high())); 
    return Vec4d(sin(x.get_low()), sin(x.get_high()));
}

static inline Vec8f tan (Vec8f const & x) {   // tangent
    return Vec8f(tan(x.get_low()), tan(x.get_high()));
}
static inline Vec4d tan (Vec4d const & x) {   // tangent
    return Vec4d(tan(x.get_low()), tan(x.get_high()));
}

// inverse trigonometric functions
static inline Vec8f asin (Vec8f const & x) {   // inverse sine
    return Vec8f(asin(x.get_low()), asin(x.get_high()));
}
static inline Vec4d asin (Vec4d const & x) {   // inverse sine
    return Vec4d(asin(x.get_low()), asin(x.get_high()));
}

static inline Vec8f acos (Vec8f const & x) {   // inverse cosine
    return Vec8f(acos(x.get_low()), acos(x.get_high()));
}
static inline Vec4d acos (Vec4d const & x) {   // inverse cosine
    return Vec4d(acos(x.get_low()), acos(x.get_high()));
}

static inline Vec8f atan (Vec8f const & x) {   // inverse tangent
    return Vec8f(atan(x.get_low()), atan(x.get_high()));
}
static inline Vec4d atan (Vec4d const & x) {   // inverse tangent
    return Vec4d(atan(x.get_low()), atan(x.get_high()));
}

static inline Vec8f atan (Vec8f const & a, Vec8f const & b) {   // inverse tangent of a/b
    return Vec8f(atan(a.get_low(),b.get_low()), atan(a.get_high(),b.get_high()));
}
static inline Vec4d atan (Vec4d const & a, Vec4d const & b) {   // inverse tangent of a/b
    return Vec4d(atan(a.get_low(),b.get_low()), atan(a.get_high(),b.get_high()));
}
#endif // VECTORMATH_COMMON_H

// hyperbolic functions and inverse hyperbolic functions
static inline Vec8f sinh (Vec8f const & x) {   // hyperbolic sine
    return Vec8f(sinh(x.get_low()), sinh(x.get_high()));
}
static inline Vec4d sinh (Vec4d const & x) {   // hyperbolic sine
    return Vec4d(sinh(x.get_low()), sinh(x.get_high()));
}

static inline Vec8f cosh (Vec8f const & x) {   // hyperbolic cosine
    return Vec8f(cosh(x.get_low()), cosh(x.get_high()));
}
static inline Vec4d cosh (Vec4d const & x) {   // hyperbolic cosine
    return Vec4d(cosh(x.get_low()), cosh(x.get_high()));
}

static inline Vec8f tanh (Vec8f const & x) {   // hyperbolic tangent
    return Vec8f(tanh(x.get_low()), tanh(x.get_high()));
}
static inline Vec4d tanh (Vec4d const & x) {   // hyperbolic tangent
    return Vec4d(tanh(x.get_low()), tanh(x.get_high()));
}

// error function
#ifdef HAVE_ERF
static inline Vec8f erf (Vec8f const & x) {   // error function
    return Vec8f(erf(x.get_low()), erf(x.get_high()));
}
static inline Vec4d erf (Vec4d const & x) {   // error function
    return Vec4d(erf(x.get_low()), erf(x.get_high()));
}
#endif
#ifdef HAVE_ERFC
static inline Vec8f erfc (Vec8f const & x) {   // error function complement
    return Vec8f(erfc(x.get_low()), erfc(x.get_high()));
}
static inline Vec4d erfc (Vec4d const & x) {   // error function complement
    return Vec4d(erfc(x.get_low()), erfc(x.get_high()));
}
#endif

// complex exponential function (real part in even numbered elements, imaginary part in odd numbered elements)
static inline Vec8f cexp (Vec8f const & x) {   // complex exponential function
    return Vec8f(cexp(x.get_low()), cexp(x.get_high()));
}
static inline Vec4d cexp (Vec4d const & x) {   // complex exponential function
    return Vec4d(cexp(x.get_low()), cexp(x.get_high()));
}

#endif // VECTORF256_H == 1


/*****************************************************************************
*
*      VECTORMATH = 1. Use AMD LIBM library
*
*****************************************************************************/
#elif VECTORMATH == 1
//#include <amdlibm.h> 
#include "amdlibm.h" // if header file is in current directory

#ifndef VECTORMATH_COMMON_H

// exponential and power functions
static inline Vec4f exp (Vec4f const & x) {   // exponential function
    return amd_vrs4_expf(x);
}
static inline Vec2d exp (Vec2d const & x) {   // exponential function
    return amd_vrd2_exp(x);
}

static inline Vec4f expm1 (Vec4f const & x) {   // exp(x)-1. Avoids loss of precision if x is close to 1
    return amd_vrs4_expm1f(x);
}
static inline Vec2d expm1 (Vec2d const & x) {   // exp(x)-1. Avoids loss of precision if x is close to 1
    return amd_vrd2_expm1(x);
}

static inline Vec4f exp2 (Vec4f const & x) {   // pow(2,x)
    return amd_vrs4_exp2f(x);
}
static inline Vec2d exp2 (Vec2d const & x) {   // pow(2,x)
    return amd_vrd2_exp2(x);
}

static inline Vec4f exp10 (Vec4f const & x) {   // pow(10,x)
    return amd_vrs4_exp10f(x);
}
static inline Vec2d exp10 (Vec2d const & x) {   // pow(10,x)
    return amd_vrd2_exp10(x);
}

static inline Vec4f pow (Vec4f const & a, Vec4f const & b) {   // pow(a,b) = a to the power of b
    return amd_vrs4_powf(a,b);
}
static inline Vec2d pow (Vec2d const & a, Vec2d const & b) {   // pow(a,b) = a to the power of b
    return amd_vrd2_pow(a,b);
}

static inline Vec4f cbrt (Vec4f const & x) {   // pow(x,1/3)
    return amd_vrs4_cbrtf(x);
}
static inline Vec2d cbrt (Vec2d const & x) {   // pow(x,1/3)
    return amd_vrd2_cbrt(x);
}

// logarithms
static inline Vec4f log (Vec4f const & x) {   // natural logarithm
    return amd_vrs4_logf(x);
}
static inline Vec2d log (Vec2d const & x) {   // natural logarithm
    return amd_vrd2_log(x);
}

static inline Vec4f log1p (Vec4f const & x) {   // log(1+x). Avoids loss of precision if 1+x is close to 1
    return amd_vrs4_log1pf(x);
}
static inline Vec2d log1p (Vec2d const & x) {   // log(1+x). Avoids loss of precision if 1+x is close to 1
    return amd_vrd2_log1p(x);
}

static inline Vec4f log2 (Vec4f const & x) {   // logarithm base 2
    return amd_vrs4_log2f(x);
}
static inline Vec2d log2 (Vec2d const & x) {   // logarithm base 2
    return amd_vrd2_log2(x);
}

static inline Vec4f log10 (Vec4f const & x) {   // logarithm base 10
    return amd_vrs4_log10f(x);
}
static inline Vec2d log10 (Vec2d const & x) {   // logarithm base 10
    return amd_vrd2_log10(x);
}

// trigonometric functions (angles in radians)
static inline Vec4f sin (Vec4f const & x) {   // sine
    return amd_vrs4_sinf(x);
}
static inline Vec2d sin (Vec2d const & x) {   // sine
    return amd_vrd2_sin(x);
}

static inline Vec4f cos (Vec4f const & x) {   // cosine
    return amd_vrs4_cosf(x);
}
static inline Vec2d cos (Vec2d const & x) {   // cosine
    return amd_vrd2_cos(x);
}

static inline Vec4f sincos (Vec4f * pcos, Vec4f const & x) {   // sine and cosine. sin(x) returned, cos(x) in pcos
    __m128 r_sin;
    amd_vrs4_sincosf(x, &r_sin, (__m128*)pcos);
    return r_sin;
}
static inline Vec2d sincos (Vec2d * pcos, Vec2d const & x) {   // sine and cosine. sin(x) returned, cos(x) in pcos
    __m128d r_sin;
    amd_vrd2_sincos(x, &r_sin, (__m128d*)pcos);
    return r_sin;
}

static inline Vec4f tan (Vec4f const & x) {   // tangent
    return amd_vrs4_tanf(x);
}
static inline Vec2d tan (Vec2d const & x) {   // tangent
    return amd_vrd2_tan(x);
}

// inverse trigonometric functions not supported

#endif // VECTORMATH_COMMON_H

// hyperbolic functions and inverse hyperbolic functions not supported

// error function not supported

// complex exponential function not supported

#ifdef VECTORF256_H

// Emulate 256 bit vector functions with two 128-bit vectors

#ifndef VECTORMATH_COMMON_H

// exponential and power functions
static inline Vec8f exp (Vec8f const & x) {   // exponential function
    return Vec8f(exp(x.get_low()), exp(x.get_high()));
}
static inline Vec4d exp (Vec4d const & x) {   // exponential function
    return Vec4d(exp(x.get_low()), exp(x.get_high()));
}

static inline Vec8f expm1 (Vec8f const & x) {   // exp(x)-1. Avoids loss of precision if x is close to 1
    return Vec8f(expm1(x.get_low()), expm1(x.get_high()));
}
static inline Vec4d expm1 (Vec4d const & x) {   // exp(x)-1. Avoids loss of precision if x is close to 1
    return Vec4d(expm1(x.get_low()), expm1(x.get_high()));
}

static inline Vec8f exp2 (Vec8f const & x) {   // pow(2,x)
    return Vec8f(exp2(x.get_low()), exp2(x.get_high()));
}
static inline Vec4d exp2 (Vec4d const & x) {   // pow(2,x)
    return Vec4d(exp2(x.get_low()), exp2(x.get_high()));
}

static inline Vec8f exp10 (Vec8f const & x) {   // pow(10,x)
    return Vec8f(exp10(x.get_low()), exp10(x.get_high()));
}
static inline Vec4d exp10 (Vec4d const & x) {   // pow(10,x)
    return Vec4d(exp10(x.get_low()), exp10(x.get_high()));
}

static inline Vec8f pow (Vec8f const & a, Vec8f const & b) {   // pow(a,b) = a to the power of b
    return Vec8f(pow(a.get_low(),b.get_low()), pow(a.get_high(),b.get_high()));
}
static inline Vec4d pow (Vec4d const & a, Vec4d const & b) {   // pow(a,b) = a to the power of b
    return Vec4d(pow(a.get_low(),b.get_low()), pow(a.get_high(),b.get_high()));
}

static inline Vec8f cbrt (Vec8f const & x) {   // pow(x,1/3)
    return Vec8f(cbrt(x.get_low()), cbrt(x.get_high()));
}
static inline Vec4d cbrt (Vec4d const & x) {   // pow(x,1/3)
    return Vec4d(cbrt(x.get_low()), cbrt(x.get_high()));
}


// logarithms
static inline Vec8f log (Vec8f const & x) {   // natural logarithm
    return Vec8f(log(x.get_low()), log(x.get_high()));
}
static inline Vec4d log (Vec4d const & x) {   // natural logarithm
    return Vec4d(log(x.get_low()), log(x.get_high()));
}

static inline Vec8f log1p (Vec8f const & x) {   // log(1+x). Avoids loss of precision if 1+x is close to 1
    return Vec8f(log1p(x.get_low()), log1p(x.get_high()));
}
static inline Vec4d log1p (Vec4d const & x) {   // log(1+x). Avoids loss of precision if 1+x is close to 1
    return Vec4d(log1p(x.get_low()), log1p(x.get_high()));
}

static inline Vec8f log2 (Vec8f const & x) {   // logarithm base 2
    return Vec8f(log2(x.get_low()), log2(x.get_high()));
}
static inline Vec4d log2 (Vec4d const & x) {   // logarithm base 2
    return Vec4d(log2(x.get_low()), log2(x.get_high()));
}

static inline Vec8f log10 (Vec8f const & x) {   // logarithm base 10
    return Vec8f(log10(x.get_low()), log10(x.get_high()));
}
static inline Vec4d log10 (Vec4d const & x) {   // logarithm base 10
    return Vec4d(log10(x.get_low()), log10(x.get_high()));
}

// trigonometric functions (angles in radians)
static inline Vec8f sin (Vec8f const & x) {   // sine
    return Vec8f(sin(x.get_low()), sin(x.get_high()));
}
static inline Vec4d sin (Vec4d const & x) {   // sine
    return Vec4d(sin(x.get_low()), sin(x.get_high()));
}

static inline Vec8f cos (Vec8f const & x) {   // cosine
    return Vec8f(cos(x.get_low()), cos(x.get_high()));
}
static inline Vec4d cos (Vec4d const & x) {   // cosine
    return Vec4d(cos(x.get_low()), cos(x.get_high()));
}

static inline Vec8f sincos (Vec8f * pcos, Vec8f const & x) {   // sine and cosine. sin(x) returned, cos(x) in pcos
    Vec4f r_sin0, r_sin1, r_cos0, r_cos1;
    r_sin0 = sincos(&r_cos0, x.get_low()); 
    r_sin1 = sincos(&r_cos1, x.get_high());
    *pcos = Vec8f(r_cos0, r_cos1);
    return Vec8f(r_sin0, r_sin1); 
}
static inline Vec4d sincos (Vec4d * pcos, Vec4d const & x) {   // sine and cosine. sin(x) returned, cos(x) in pcos
    Vec2d r_sin0, r_sin1, r_cos0, r_cos1;
    r_sin0 = sincos(&r_cos0, x.get_low()); 
    r_sin1 = sincos(&r_cos1, x.get_high());
    *pcos = Vec4d(r_cos0, r_cos1);
    return Vec4d(r_sin0, r_sin1); 
}

static inline Vec8f tan (Vec8f const & x) {   // tangent
    return Vec8f(tan(x.get_low()), tan(x.get_high()));
}
static inline Vec4d tan (Vec4d const & x) {   // tangent
    return Vec4d(tan(x.get_low()), tan(x.get_high()));
}

#endif // VECTORMATH_COMMON_H

#endif // VECTORF256_H == 1


/*****************************************************************************
*
*      VECTORMATH = 2. Use Intel SVML library with any compiler
*
*****************************************************************************/
#elif VECTORMATH == 2 

extern "C" {
extern __m128  __svml_expf4       (__m128);
extern __m128d __svml_exp2        (__m128d);
extern __m128  __svml_expm1f4     (__m128);
extern __m128d __svml_expm12      (__m128d);
extern __m128  __svml_exp2f4      (__m128);
extern __m128d __svml_exp22       (__m128d);
extern __m128  __svml_exp10f4     (__m128);
extern __m128d __svml_exp102      (__m128d);
extern __m128  __svml_powf4       (__m128,  __m128);
extern __m128d __svml_pow2        (__m128d, __m128d);
extern __m128  __svml_cbrtf4      (__m128);
extern __m128d __svml_cbrt2       (__m128d);
extern __m128  __svml_invsqrtf4   (__m128);
extern __m128d __svml_invsqrt2    (__m128d);
extern __m128  __svml_logf4       (__m128);
extern __m128d __svml_log2        (__m128d);
extern __m128  __svml_log1pf4     (__m128);
extern __m128d __svml_log1p2      (__m128d);
extern __m128  __svml_log2f4      (__m128);
extern __m128d __svml_log22       (__m128d);
extern __m128  __svml_log10f4     (__m128);
extern __m128d __svml_log102      (__m128d);
extern __m128  __svml_sinf4       (__m128);
extern __m128d __svml_sin2        (__m128d);
extern __m128  __svml_cosf4       (__m128);
extern __m128d __svml_cos2        (__m128d);
extern __m128  __svml_sincosf4    (__m128);  // cos returned in xmm1
extern __m128d __svml_sincos2     (__m128d); // cos returned in xmm1
extern __m128  __svml_tanf4       (__m128);
extern __m128d __svml_tan2        (__m128d);
extern __m128  __svml_asinf4      (__m128);
extern __m128d __svml_asin2       (__m128d);
extern __m128  __svml_acosf4      (__m128);
extern __m128d __svml_acos2       (__m128d);
extern __m128  __svml_atanf4      (__m128);
extern __m128d __svml_atan2       (__m128d);
extern __m128  __svml_atan2f4     (__m128,  __m128);
extern __m128d __svml_atan22      (__m128d, __m128d);
extern __m128  __svml_sinhf4      (__m128);
extern __m128d __svml_sinh2       (__m128d);
extern __m128  __svml_coshf4      (__m128);
extern __m128d __svml_cosh2       (__m128d);
extern __m128  __svml_tanhf4      (__m128);
extern __m128d __svml_tanh2       (__m128d);
extern __m128  __svml_asinhf4     (__m128);
extern __m128d __svml_asinh2      (__m128d);
extern __m128  __svml_acoshf4     (__m128);
extern __m128d __svml_acosh2      (__m128d);
extern __m128  __svml_atanhf4     (__m128);
extern __m128d __svml_atanh2      (__m128d);
extern __m128  __svml_erff4       (__m128);
extern __m128d __svml_erf2        (__m128d);
extern __m128  __svml_erfcf4      (__m128);
extern __m128d __svml_erfc2       (__m128d);
extern __m128  __svml_erfinvf4    (__m128);
extern __m128d __svml_erfinv2     (__m128d);
extern __m128  __svml_cdfnorminvf4(__m128);
extern __m128d __svml_cdfnorminv2 (__m128d);
extern __m128  __svml_cdfnormf4   (__m128);
extern __m128d __svml_cdfnorm2    (__m128d);
extern __m128  __svml_cexpf4      (__m128);
extern __m128d __svml_cexp2       (__m128d);
}

#ifndef VECTORMATH_COMMON_H

// exponential and power functions
static inline Vec4f exp (Vec4f const & x) {   // exponential function
    return __svml_expf4(x);
}
static inline Vec2d exp (Vec2d const & x) {   // exponential function
    return __svml_exp2(x);
}

static inline Vec4f expm1 (Vec4f const & x) {   // exp(x)-1. Avoids loss of precision if x is close to 1
    return __svml_expm1f4(x);
}
static inline Vec2d expm1 (Vec2d const & x) {   // exp(x)-1. Avoids loss of precision if x is close to 1
    return __svml_expm12(x);
}

static inline Vec4f exp2 (Vec4f const & x) {   // pow(2,x)
    return __svml_exp2f4(x);
}
static inline Vec2d exp2 (Vec2d const & x) {   // pow(2,x)
    return __svml_exp22(x);
}

static inline Vec4f exp10 (Vec4f const & x) {   // pow(10,x)
    return __svml_exp10f4(x);
}
static inline Vec2d exp10 (Vec2d const & x) {   // pow(10,x)
    return __svml_exp102(x);
}

static inline Vec4f pow (Vec4f const & a, Vec4f const & b) {   // pow(a,b) = a to the power of b
    return __svml_powf4(a,b);
}
static inline Vec2d pow (Vec2d const & a, Vec2d const & b) {   // pow(a,b) = a to the power of b
    return __svml_pow2(a,b);
}

static inline Vec4f cbrt (Vec4f const & x) {   // pow(x,1/3)
    return __svml_cbrtf4(x);
}
static inline Vec2d cbrt (Vec2d const & x) {   // pow(x,1/3)
    return __svml_cbrt2(x);
}

// logarithms
static inline Vec4f log (Vec4f const & x) {   // natural logarithm
    return __svml_logf4(x);
}
static inline Vec2d log (Vec2d const & x) {   // natural logarithm
    return __svml_log2(x);
}

static inline Vec4f log1p (Vec4f const & x) {   // log(1+x). Avoids loss of precision if 1+x is close to 1
    return __svml_log1pf4(x);
}
static inline Vec2d log1p (Vec2d const & x) {   // log(1+x). Avoids loss of precision if 1+x is close to 1
    return __svml_log1p2(x);
}

static inline Vec4f log2 (Vec4f const & x) {   // logarithm base 2
    return __svml_log2f4(x);
}
static inline Vec2d log2 (Vec2d const & x) {   // logarithm base 2
    return __svml_log22(x);
}

static inline Vec4f log10 (Vec4f const & x) {   // logarithm base 10
    return __svml_log10f4(x);
}
static inline Vec2d log10 (Vec2d const & x) {   // logarithm base 10
    return __svml_log102(x);
}

// trigonometric functions (angles in radians)
static inline Vec4f sin (Vec4f const & x) {   // sine
    return __svml_sinf4(x);
}
static inline Vec2d sin (Vec2d const & x) {   // sine
    return __svml_sin2(x);
}

static inline Vec4f cos (Vec4f const & x) {   // cosine
    return __svml_cosf4(x);
}
static inline Vec2d cos (Vec2d const & x) {   // cosine
    return __svml_cos2(x);
}

#if defined(__unix__) || defined(__INTEL_COMPILER) || !defined(__x86_64__) || !defined(_MSC_VER)
// no inline assembly in 64 bit MS compiler
static inline Vec4f sincos (Vec4f * pcos, Vec4f const & x) {   // sine and cosine. sin(x) returned, cos(x) in pcos
    __m128 r_sin, r_cos;
    r_sin = __svml_sincosf4(x);
#if defined(__unix__) || defined(__GNUC__)
    //   __asm__ ( "call __svml_sincosf4 \n movaps %%xmm0, %0 \n movaps %%xmm1, %1" : "=m"(r_sin), "=m"(r_cos) : "xmm0"(x) );
     __asm__ __volatile__ ( "movaps %%xmm1, %0":"=m"(r_cos));
#else // Windows
    _asm movaps r_cos, xmm1;
#endif
    *pcos = r_cos;
    return r_sin;
}
static inline Vec2d sincos (Vec2d * pcos, Vec2d const & x) {   // sine and cosine. sin(x) returned, cos(x) in pcos
    __m128d r_sin, r_cos;
    r_sin = __svml_sincos2(x);
#if defined(__unix__) || defined(__GNUC__)
     __asm__ __volatile__ ( "movaps %%xmm1, %0":"=m"(r_cos));
#else // Windows
    _asm movapd r_cos, xmm1;
#endif
    *pcos = r_cos;
    return r_sin;
}
#endif // inline assembly available

static inline Vec4f tan (Vec4f const & x) {   // tangent
    return __svml_tanf4(x);
}
static inline Vec2d tan (Vec2d const & x) {   // tangent
    return __svml_tan2(x);
}

// inverse trigonometric functions
static inline Vec4f asin (Vec4f const & x) {   // inverse sine
    return __svml_asinf4(x);
}
static inline Vec2d asin (Vec2d const & x) {   // inverse sine
    return __svml_asin2(x);
}

static inline Vec4f acos (Vec4f const & x) {   // inverse cosine
    return __svml_acosf4(x);
}
static inline Vec2d acos (Vec2d const & x) {   // inverse cosine
    return __svml_acos2(x);
}

static inline Vec4f atan (Vec4f const & x) {   // inverse tangent
    return __svml_atanf4(x);
}
static inline Vec2d atan (Vec2d const & x) {   // inverse tangent
    return __svml_atan2(x);
}

static inline Vec4f atan2 (Vec4f const & a, Vec4f const & b) {   // inverse tangent of a/b
    return __svml_atan2f4(a,b);
}
static inline Vec2d atan2 (Vec2d const & a, Vec2d const & b) {   // inverse tangent of a/b
    return __svml_atan22(a,b);
}

#endif // VECTORMATH_COMMON_H

// hyperbolic functions and inverse hyperbolic functions
static inline Vec4f sinh (Vec4f const & x) {   // hyperbolic sine
    return __svml_sinhf4(x);
}
static inline Vec2d sinh (Vec2d const & x) {   // hyperbolic sine
    return __svml_sinh2(x);
}

static inline Vec4f cosh (Vec4f const & x) {   // hyperbolic cosine
    return __svml_coshf4(x);
}
static inline Vec2d cosh (Vec2d const & x) {   // hyperbolic cosine
    return __svml_cosh2(x);
}

static inline Vec4f tanh (Vec4f const & x) {   // hyperbolic tangent
    return __svml_tanhf4(x);
}
static inline Vec2d tanh (Vec2d const & x) {   // hyperbolic tangent
    return __svml_tanh2(x);
}

static inline Vec4f asinh (Vec4f const & x) {   // inverse hyperbolic sine
    return __svml_asinhf4(x);
}
static inline Vec2d asinh (Vec2d const & x) {   // inverse hyperbolic sine
    return __svml_asinh2(x);
}

static inline Vec4f acosh (Vec4f const & x) {   // inverse hyperbolic cosine
    return __svml_acoshf4(x);
}
static inline Vec2d acosh (Vec2d const & x) {   // inverse hyperbolic cosine
    return __svml_acosh2(x);
}

static inline Vec4f atanh (Vec4f const & x) {   // inverse hyperbolic tangent
    return __svml_atanhf4(x);
}
static inline Vec2d atanh (Vec2d const & x) {   // inverse hyperbolic tangent
    return __svml_atanh2(x);
}

// error function
static inline Vec4f erf (Vec4f const & x) {   // error function
    return __svml_erff4(x);
}
static inline Vec2d erf (Vec2d const & x) {   // error function
    return __svml_erf2(x);
}

static inline Vec4f erfc (Vec4f const & x) {   // error function complement
    return __svml_erfcf4(x);
}
static inline Vec2d erfc (Vec2d const & x) {   // error function complement
    return __svml_erfc2(x);
}

static inline Vec4f erfinv (Vec4f const & x) {   // inverse error function
    return __svml_erfinvf4(x);
}
static inline Vec2d erfinv (Vec2d const & x) {   // inverse error function
    return __svml_erfinv2(x);
}

static inline Vec4f cdfnorm (Vec4f const & x) {   // cumulative normal distribution function
    return __svml_cdfnormf4(x);
}
static inline Vec2d cdfnorm (Vec2d const & x) {   // cumulative normal distribution function
    return __svml_cdfnorm2(x);
}

static inline Vec4f cdfnorminv (Vec4f const & x) {   // inverse cumulative normal distribution function
    return __svml_cdfnorminvf4(x);
}
static inline Vec2d cdfnorminv (Vec2d const & x) {   // inverse cumulative normal distribution function
    return __svml_cdfnorminv2(x);
}

// complex exponential function (real part in even numbered elements, imaginary part in odd numbered elements)
static inline Vec4f cexp (Vec4f const & x) {   // complex exponential function
    return __svml_cexpf4(x);
}
static inline Vec2d cexp (Vec2d const & x) {   // complex exponential function
    return __svml_cexp2(x);
}


#if defined (VECTORF256_H) && VECTORF256_H >= 2  
// AVX gives 256 bit vectors

extern "C" {
extern __m256  __svml_expf8       (__m256);
extern __m256d __svml_exp4        (__m256d);
extern __m256  __svml_expm1f8     (__m256);
extern __m256d __svml_expm14      (__m256d);
extern __m256  __svml_exp2f8      (__m256);
extern __m256d __svml_exp24       (__m256d);
extern __m256  __svml_exp10f8     (__m256);
extern __m256d __svml_exp104      (__m256d);
extern __m256  __svml_powf8       (__m256,  __m256);
extern __m256d __svml_pow4        (__m256d, __m256d);
extern __m256  __svml_cbrtf8      (__m256);
extern __m256d __svml_cbrt4       (__m256d);
extern __m256  __svml_invsqrtf8   (__m256);
extern __m256d __svml_invsqrt4    (__m256d);
extern __m256  __svml_logf8       (__m256);
extern __m256d __svml_log4        (__m256d);
extern __m256  __svml_log1pf8     (__m256);
extern __m256d __svml_log1p4      (__m256d);
extern __m256  __svml_log2f8      (__m256);
extern __m256d __svml_log24       (__m256d);
extern __m256  __svml_log10f8     (__m256);
extern __m256d __svml_log104      (__m256d);
extern __m256  __svml_sinf8       (__m256);
extern __m256d __svml_sin4        (__m256d);
extern __m256  __svml_cosf8       (__m256);
extern __m256d __svml_cos4        (__m256d);
extern __m256  __svml_sincosf8    (__m256);  // cos returned in ymm1
extern __m256d __svml_sincos4     (__m256d); // cos returned in ymm1
extern __m256  __svml_tanf8       (__m256);
extern __m256d __svml_tan4        (__m256d);
extern __m256  __svml_asinf8      (__m256);
extern __m256d __svml_asin4       (__m256d);
extern __m256  __svml_acosf8      (__m256);
extern __m256d __svml_acos4       (__m256d);
extern __m256  __svml_atanf8      (__m256);
extern __m256d __svml_atan4       (__m256d);
extern __m256  __svml_atan2f8     (__m256, __m256);
extern __m256d __svml_atan24      (__m256d, __m256d);
extern __m256  __svml_sinhf8      (__m256);
extern __m256d __svml_sinh4       (__m256d);
extern __m256  __svml_coshf8      (__m256);
extern __m256d __svml_cosh4       (__m256d);
extern __m256  __svml_tanhf8      (__m256);
extern __m256d __svml_tanh4       (__m256d);
extern __m256  __svml_asinhf8     (__m256);
extern __m256d __svml_asinh4      (__m256d);
extern __m256  __svml_acoshf8     (__m256);
extern __m256d __svml_acosh4      (__m256d);
extern __m256  __svml_atanhf8     (__m256);
extern __m256d __svml_atanh4      (__m256d);
extern __m256  __svml_erff8       (__m256);
extern __m256d __svml_erf4        (__m256d);
extern __m256  __svml_erfcf8      (__m256);
extern __m256d __svml_erfc4       (__m256d);
extern __m256  __svml_erfinvf8    (__m256);
extern __m256d __svml_erfinv4     (__m256d);
extern __m256  __svml_cdfnorminvf8(__m256);
extern __m256d __svml_cdfnorminv4 (__m256d);
extern __m256  __svml_cdfnormf8   (__m256);
extern __m256d __svml_cdfnorm4    (__m256d);
//extern __m256  __svml_cexpf8      (__m256); // missing in current version of SVML (jan 2012)
//extern __m256d __svml_cexp4       (__m256d);
}

#ifndef VECTORMATH_COMMON_H

// exponential and power functions
static inline Vec8f exp (Vec8f const & x) {   // exponential function
    return __svml_expf8(x);
}
static inline Vec4d exp (Vec4d const & x) {   // exponential function
    return __svml_exp4(x);
}

static inline Vec8f expm1 (Vec8f const & x) {   // exp(x)-1. Avoids loss of precision if x is close to 1
    return __svml_expm1f8(x);
}
static inline Vec4d expm1 (Vec4d const & x) {   // exp(x)-1. Avoids loss of precision if x is close to 1
    return __svml_expm14(x);
}

static inline Vec8f exp2 (Vec8f const & x) {   // pow(2,x)
    return __svml_exp2f8(x);
}
static inline Vec4d exp2 (Vec4d const & x) {   // pow(2,x)
    return __svml_exp24(x);
}

static inline Vec8f exp10 (Vec8f const & x) {   // pow(10,x)
    return __svml_exp10f8(x);
}
static inline Vec4d exp10 (Vec4d const & x) {   // pow(10,x)
    return __svml_exp104(x);
}

static inline Vec8f pow (Vec8f const & a, Vec8f const & b) {   // pow(a,b) = a to the power of b
    return __svml_powf8(a,b);
}
static inline Vec4d pow (Vec4d const & a, Vec4d const & b) {   // pow(a,b) = a to the power of b
    return __svml_pow4(a,b);
}

static inline Vec8f cbrt (Vec8f const & x) {   // pow(x,1/3)
    return __svml_cbrtf8(x);
}
static inline Vec4d cbrt (Vec4d const & x) {   // pow(x,1/3)
    return __svml_cbrt4(x);
}

// logarithms
static inline Vec8f log (Vec8f const & x) {   // natural logarithm
    return __svml_logf8(x);
}
static inline Vec4d log (Vec4d const & x) {   // natural logarithm
    return __svml_log4(x);
}

static inline Vec8f log1p (Vec8f const & x) {   // log(1+x). Avoids loss of precision if 1+x is close to 1
    return __svml_log1pf8(x);
}
static inline Vec4d log1p (Vec4d const & x) {   // log(1+x). Avoids loss of precision if 1+x is close to 1
    return __svml_log1p4(x);
}

static inline Vec8f log2 (Vec8f const & x) {   // logarithm base 2
    return __svml_log2f8(x);
}
static inline Vec4d log2 (Vec4d const & x) {   // logarithm base 2
    return __svml_log24(x);
}

static inline Vec8f log10 (Vec8f const & x) {   // logarithm base 10
    return __svml_log10f8(x);
}
static inline Vec4d log10 (Vec4d const & x) {   // logarithm base 10
    return __svml_log104(x);
}

// trigonometric functions (angles in radians)
static inline Vec8f sin (Vec8f const & x) {   // sine
    return __svml_sinf8(x);
}
static inline Vec4d sin (Vec4d const & x) {   // sine
    return __svml_sin4(x);
}

static inline Vec8f cos (Vec8f const & x) {   // cosine
    return __svml_cosf8(x);
}
static inline Vec4d cos (Vec4d const & x) {   // cosine
    return __svml_cos4(x);
}

#if defined(__unix__) || defined(__INTEL_COMPILER) || !defined(__x86_64__) || !defined(_MSC_VER)
// no inline assembly in 64 bit MS compiler
static inline Vec8f sincos (Vec8f * pcos, Vec8f const & x) {   // sine and cosine. sin(x) returned, cos(x) in pcos
    __m256 r_sin, r_cos;
    r_sin = __svml_sincosf8(x);
#if defined(__unix__) || defined(__GNUC__)
    __asm__ __volatile__ ( "vmovaps %%ymm1, %0":"=m"(r_cos));
#else // Windows
    _asm vmovaps r_cos, ymm1;
#endif
    *pcos = r_cos;
    return r_sin;
}
static inline Vec4d sincos (Vec4d * pcos, Vec4d const & x) {   // sine and cosine. sin(x) returned, cos(x) in pcos
    __m256d r_sin, r_cos;
    r_sin = __svml_sincos4(x);
#if defined(__unix__) || defined(__GNUC__)
     __asm__ __volatile__ ( "vmovaps %%ymm1, %0":"=m"(r_cos));
#else // Windows
    _asm vmovapd r_cos, ymm1;
#endif
    *pcos = r_cos;
    return r_sin;
}
#endif // inline assembly available

static inline Vec8f tan (Vec8f const & x) {   // tangent
    return __svml_tanf8(x);
}
static inline Vec4d tan (Vec4d const & x) {   // tangent
    return __svml_tan4(x);
}

// inverse trigonometric functions
static inline Vec8f asin (Vec8f const & x) {   // inverse sine
    return __svml_asinf8(x);
}
static inline Vec4d asin (Vec4d const & x) {   // inverse sine
    return __svml_asin4(x);
}

static inline Vec8f acos (Vec8f const & x) {   // inverse cosine
    return __svml_acosf8(x);
}
static inline Vec4d acos (Vec4d const & x) {   // inverse cosine
    return __svml_acos4(x);
}

static inline Vec8f atan (Vec8f const & x) {   // inverse tangent
    return __svml_atanf8(x);
}
static inline Vec4d atan (Vec4d const & x) {   // inverse tangent
    return __svml_atan4(x);
}

static inline Vec8f atan2 (Vec8f const & a, Vec8f const & b) {   // inverse tangent of a/b
    return __svml_atan2f8(a,b);
}
static inline Vec4d atan2 (Vec4d const & a, Vec4d const & b) {   // inverse tangent of a/b
    return __svml_atan24(a,b);
}

#endif // VECTORMATH_COMMON_H

// hyperbolic functions and inverse hyperbolic functions
static inline Vec8f sinh (Vec8f const & x) {   // hyperbolic sine
    return __svml_sinhf8(x);
}
static inline Vec4d sinh (Vec4d const & x) {   // hyperbolic sine
    return __svml_sinh4(x);
}

static inline Vec8f cosh (Vec8f const & x) {   // hyperbolic cosine
    return __svml_coshf8(x);
}
static inline Vec4d cosh (Vec4d const & x) {   // hyperbolic cosine
    return __svml_cosh4(x);
}

static inline Vec8f tanh (Vec8f const & x) {   // hyperbolic tangent
    return __svml_tanhf8(x);
}
static inline Vec4d tanh (Vec4d const & x) {   // hyperbolic tangent
    return __svml_tanh4(x);
}

static inline Vec8f asinh (Vec8f const & x) {   // inverse hyperbolic sine
    return __svml_asinhf8(x);
}
static inline Vec4d asinh (Vec4d const & x) {   // inverse hyperbolic sine
    return __svml_asinh4(x);
}

static inline Vec8f acosh (Vec8f const & x) {   // inverse hyperbolic cosine
    return __svml_acoshf8(x);
}
static inline Vec4d acosh (Vec4d const & x) {   // inverse hyperbolic cosine
    return __svml_acosh4(x);
}

static inline Vec8f atanh (Vec8f const & x) {   // inverse hyperbolic tangent
    return __svml_atanhf8(x);
}
static inline Vec4d atanh (Vec4d const & x) {   // inverse hyperbolic tangent
    return __svml_atanh4(x);
}

// error function
static inline Vec8f erf (Vec8f const & x) {   // error function
    return __svml_erff8(x);
}
static inline Vec4d erf (Vec4d const & x) {   // error function
    return __svml_erf4(x);
}

static inline Vec8f erfc (Vec8f const & x) {   // error function complement
    return __svml_erfcf8(x);
}
static inline Vec4d erfc (Vec4d const & x) {   // error function complement
    return __svml_erfc4(x);
}

static inline Vec8f erfinv (Vec8f const & x) {   // inverse error function
    return __svml_erfinvf8(x);
}
static inline Vec4d erfinv (Vec4d const & x) {   // inverse error function
    return __svml_erfinv4(x);
}

static inline Vec8f cdfnorm (Vec8f const & x) {   // cumulative normal distribution function
    return __svml_cdfnormf8(x);
}
static inline Vec4d cdfnorm (Vec4d const & x) {   // cumulative normal distribution function
    return __svml_cdfnorm4(x);
}

static inline Vec8f cdfnorminv (Vec8f const & x) {   // inverse cumulative normal distribution function
    return __svml_cdfnorminvf8(x);
}
static inline Vec4d cdfnorminv (Vec4d const & x) {   // inverse cumulative normal distribution function
    return __svml_cdfnorminv4(x);
}

// complex exponential function (real part in even numbered elements, imaginary part in odd numbered elements)
// 256-bit version missing in current version of SVML (jan 2012). Use 128 bit version
static inline Vec8f cexp (Vec8f const & x) {   // complex exponential function
    return Vec8f(cexp(x.get_low()), cexp(x.get_high()));
}
static inline Vec4d cexp (Vec4d const & x) {   // complex exponential function
    return Vec4d(cexp(x.get_low()), cexp(x.get_high()));
}

#endif // VECTORF256_H == 2


/*****************************************************************************
*
*      VECTORMATH = 3. Use Intel SVML library with Intel compiler
*
*****************************************************************************/
#elif VECTORMATH == 3 
#include <ia32intrin.h>    // intel svml functions defined in Intel version of immintrin.h

// 128 bit vectors

#ifndef VECTORMATH_COMMON_H

// exponential and power functions
static inline Vec4f exp (Vec4f const & x) {   // exponential function
    return _mm_exp_ps(x);
}
static inline Vec2d exp (Vec2d const & x) {   // exponential function
    return _mm_exp_pd(x);
}

static inline Vec4f expm1 (Vec4f const & x) {   // exp(x)-1. Avoids loss of precision if x is close to 1
    return _mm_expm1_ps(x);
}
static inline Vec2d expm1 (Vec2d const & x) {   // exp(x)-1. Avoids loss of precision if x is close to 1
    return _mm_expm1_pd(x);
}

static inline Vec4f exp2 (Vec4f const & x) {   // pow(2,x)
    return _mm_exp2_ps(x);
}
static inline Vec2d exp2 (Vec2d const & x) {   // pow(2,x)
    return _mm_exp2_pd(x);
}

static inline Vec4f exp10 (Vec4f const & x) {   // pow(10,x)
    return _mm_exp10_ps(x);
}
static inline Vec2d exp10 (Vec2d const & x) {   // pow(10,x)
    return _mm_exp10_pd(x);
}

static inline Vec4f pow (Vec4f const & a, Vec4f const & b) {   // pow(a,b) = a to the power of b
    return _mm_pow_ps(a,b);
}
static inline Vec2d pow (Vec2d const & a, Vec2d const & b) {   // pow(a,b) = a to the power of b
    return _mm_pow_pd(a,b);
}

static inline Vec4f cbrt (Vec4f const & x) {   // pow(x,1/3)
    return _mm_cbrt_ps(x);
}
static inline Vec2d cbrt (Vec2d const & x) {   // pow(x,1/3)
    return _mm_cbrt_pd(x);
}

// logarithms
static inline Vec4f log (Vec4f const & x) {   // natural logarithm
    return _mm_log_ps(x);
}
static inline Vec2d log (Vec2d const & x) {   // natural logarithm
    return _mm_log_pd(x);
}

static inline Vec4f log1p (Vec4f const & x) {   // log(1+x). Avoids loss of precision if 1+x is close to 1
    return _mm_log1p_ps(x);
}
static inline Vec2d log1p (Vec2d const & x) {   // log(1+x). Avoids loss of precision if 1+x is close to 1
    return _mm_log1p_pd(x);
}

static inline Vec4f log2 (Vec4f const & x) {   // logarithm base 2
    return _mm_log2_ps(x);
}
static inline Vec2d log2 (Vec2d const & x) {   // logarithm base 2
    return _mm_log2_pd(x);
}

static inline Vec4f log10 (Vec4f const & x) {   // logarithm base 10
    return _mm_log10_ps(x);
}
static inline Vec2d log10 (Vec2d const & x) {   // logarithm base 10
    return _mm_log10_pd(x);
}

// trigonometric functions
static inline Vec4f sin (Vec4f const & x) {   // sine
    return _mm_sin_ps(x);
}
static inline Vec2d sin (Vec2d const & x) {   // sine
    return _mm_sin_pd(x);
}

static inline Vec4f cos (Vec4f const & x) {   // cosine
    return _mm_cos_ps(x);
}
static inline Vec2d cos (Vec2d const & x) {   // cosine
    return _mm_cos_pd(x);
}

static inline Vec4f sincos (Vec4f * pcos, Vec4f const & x) {   // sine and cosine. sin(x) returned, cos(x) in pcos
    __m128 r_sin, r_cos;
    r_sin = _mm_sincos_ps(&r_cos, x);
    *pcos = r_cos;
    return r_sin;
}
static inline Vec2d sincos (Vec2d * pcos, Vec2d const & x) {   // sine and cosine. sin(x) returned, cos(x) in pcos
    __m128d r_sin, r_cos;
    r_sin = _mm_sincos_pd(&r_cos, x);
    *pcos = r_cos;
    return r_sin;
}

static inline Vec4f tan (Vec4f const & x) {   // tangent
    return _mm_tan_ps(x);
}
static inline Vec2d tan (Vec2d const & x) {   // tangent
    return _mm_tan_pd(x);
}

// inverse trigonometric functions
static inline Vec4f asin (Vec4f const & x) {   // inverse sine
    return _mm_asin_ps(x);
}
static inline Vec2d asin (Vec2d const & x) {   // inverse sine
    return _mm_asin_pd(x);
}

static inline Vec4f acos (Vec4f const & x) {   // inverse cosine
    return _mm_acos_ps(x);
}
static inline Vec2d acos (Vec2d const & x) {   // inverse cosine
    return _mm_acos_pd(x);
}

static inline Vec4f atan (Vec4f const & x) {   // inverse tangent
    return _mm_atan_ps(x);
}
static inline Vec2d atan (Vec2d const & x) {   // inverse tangent
    return _mm_atan_pd(x);
}

static inline Vec4f atan2 (Vec4f const & a, Vec4f const & b) {   // inverse tangent of a/b
    return _mm_atan2_ps(a,b);
}
static inline Vec2d atan2 (Vec2d const & a, Vec2d const & b) {   // inverse tangent of a/b
    return _mm_atan2_pd(a,b);
}

#endif // VECTORMATH_COMMON_H

// hyperbolic functions and inverse hyperbolic functions
static inline Vec4f sinh (Vec4f const & x) {   // hyperbolic sine
    return _mm_sinh_ps(x);
}
static inline Vec2d sinh (Vec2d const & x) {   // hyperbolic sine
    return _mm_sinh_pd(x);
}

static inline Vec4f cosh (Vec4f const & x) {   // hyperbolic cosine
    return _mm_cosh_ps(x);
}
static inline Vec2d cosh (Vec2d const & x) {   // hyperbolic cosine
    return _mm_cosh_pd(x);
}

static inline Vec4f tanh (Vec4f const & x) {   // hyperbolic tangent
    return _mm_tanh_ps(x);
}
static inline Vec2d tanh (Vec2d const & x) {   // hyperbolic tangent
    return _mm_tanh_pd(x);
}

static inline Vec4f asinh (Vec4f const & x) {   // inverse hyperbolic sine
    return _mm_asinh_ps(x);
}
static inline Vec2d asinh (Vec2d const & x) {   // inverse hyperbolic sine
    return _mm_asinh_pd(x);
}

static inline Vec4f acosh (Vec4f const & x) {   // inverse hyperbolic cosine
    return _mm_acosh_ps(x);
}
static inline Vec2d acosh (Vec2d const & x) {   // inverse hyperbolic cosine
    return _mm_acosh_pd(x);
}

static inline Vec4f atanh (Vec4f const & x) {   // inverse hyperbolic tangent
    return _mm_atanh_ps(x);
}
static inline Vec2d atanh (Vec2d const & x) {   // inverse hyperbolic tangent
    return _mm_atanh_pd(x);
}

// error function
static inline Vec4f erf (Vec4f const & x) {   // error function
    return _mm_erf_ps(x);
}
static inline Vec2d erf (Vec2d const & x) {   // error function
    return _mm_erf_pd(x);
}

static inline Vec4f erfc (Vec4f const & x) {   // error function complement
    return _mm_erfc_ps(x);
}
static inline Vec2d erfc (Vec2d const & x) {   // error function complement
    return _mm_erfc_pd(x);
}

static inline Vec4f erfinv (Vec4f const & x) {   // inverse error function
    return _mm_erfinv_ps(x);
}
static inline Vec2d erfinv (Vec2d const & x) {   // inverse error function
    return _mm_erfinv_pd(x);
}

extern "C" {
extern __m128 __svml_cdfnormf4(__m128);  // not in immintrin.h
extern __m128d __svml_cdfnorm2(__m128d); // not in immintrin.h
}

static inline Vec4f cdfnorm (Vec4f const & x) {   // cumulative normal distribution function
    return __svml_cdfnormf4(x);
}
static inline Vec2d cdfnorm (Vec2d const & x) {   // cumulative normal distribution function
    return __svml_cdfnorm2(x);
}

static inline Vec4f cdfnorminv (Vec4f const & x) {   // inverse cumulative normal distribution function
    return _mm_cdfnorminv_ps(x);
}
static inline Vec2d cdfnorminv (Vec2d const & x) {   // inverse cumulative normal distribution function
    return _mm_cdfnorminv_pd(x);
}

// complex functions
extern "C" {
extern __m128  __svml_cexpf2(__m128);   // not in immintrin.h
extern __m128  __svml_cexpf4(__m128);   // not in immintrin.h
extern __m128d __svml_cexp2(__m128d);   // not in immintrin.h
}

static inline Vec4f cexp (Vec4f const & x) {   // complex exponential function
    return __svml_cexpf4(x);
}
static inline Vec2d cexp (Vec2d const & x) {   // complex exponential function
    return __svml_cexp2(x);
}

#if defined (VECTORF256_H) && VECTORF256_H >= 2

// 256 bit vectors

#ifndef VECTORMATH_COMMON_H

// exponential and power functions
static inline Vec8f exp (Vec8f const & x) {   // exponential function
    return _mm256_exp_ps(x);
}
static inline Vec4d exp (Vec4d const & x) {   // exponential function
    return _mm256_exp_pd(x);
}

static inline Vec8f expm1 (Vec8f const & x) {   // exp(x)-1. Avoids loss of precision if x is close to 1
    return _mm256_expm1_ps(x);
}
static inline Vec4d expm1 (Vec4d const & x) {   // exp(x)-1. Avoids loss of precision if x is close to 1
    return _mm256_expm1_pd(x);
}

static inline Vec8f exp2 (Vec8f const & x) {   // pow(2,x)
    return _mm256_exp2_ps(x);
}
static inline Vec4d exp2 (Vec4d const & x) {   // pow(2,x)
    return _mm256_exp2_pd(x);
}

static inline Vec8f exp10 (Vec8f const & x) {   // pow(10,x)
    return _mm256_exp10_ps(x);
}
static inline Vec4d exp10 (Vec4d const & x) {   // pow(10,x)
    return _mm256_exp10_pd(x);
}

static inline Vec8f pow (Vec8f const & a, Vec8f const & b) {   // pow(a,b) = a to the power of b
    return _mm256_pow_ps(a,b);
}
static inline Vec4d pow (Vec4d const & a, Vec4d const & b) {   // pow(a,b) = a to the power of b
    return _mm256_pow_pd(a,b);
}

static inline Vec8f cbrt (Vec8f const & x) {   // pow(x,1/3)
    return _mm256_cbrt_ps(x);
}
static inline Vec4d cbrt (Vec4d const & x) {   // pow(x,1/3)
    return _mm256_cbrt_pd(x);
}

// logarithms
static inline Vec8f log (Vec8f const & x) {   // natural logarithm
    return _mm256_log_ps(x);
}
static inline Vec4d log (Vec4d const & x) {   // natural logarithm
    return _mm256_log_pd(x);
}

static inline Vec8f log1p (Vec8f const & x) {   // log(1+x). Avoids loss of precision if 1+x is close to 1
    return _mm256_log1p_ps(x);
}
static inline Vec4d log1p (Vec4d const & x) {   // log(1+x). Avoids loss of precision if 1+x is close to 1
    return _mm256_log1p_pd(x);
}

static inline Vec8f log2 (Vec8f const & x) {   // logarithm base 2
    return _mm256_log2_ps(x);
}
static inline Vec4d log2 (Vec4d const & x) {   // logarithm base 2
    return _mm256_log2_pd(x);
}

static inline Vec8f log10 (Vec8f const & x) {   // logarithm base 10
    return _mm256_log10_ps(x);
}
static inline Vec4d log10 (Vec4d const & x) {   // logarithm base 10
    return _mm256_log10_pd(x);
}

// trigonometric functions
static inline Vec8f sin (Vec8f const & x) {   // sine
    return _mm256_sin_ps(x);
}
static inline Vec4d sin (Vec4d const & x) {   // sine
    return _mm256_sin_pd(x);
}

static inline Vec8f cos (Vec8f const & x) {   // cosine
    return _mm256_cos_ps(x);
}
static inline Vec4d cos (Vec4d const & x) {   // cosine
    return _mm256_cos_pd(x);
}

static inline Vec8f sincos (Vec8f * pcos, Vec8f const & x) {   // sine and cosine. sin(x) returned, cos(x) in pcos
    __m256 r_sin, r_cos;
    r_sin = _mm256_sincos_ps(&r_cos, x);
    *pcos = r_cos;
    return r_sin;
}
static inline Vec4d sincos (Vec4d * pcos, Vec4d const & x) {   // sine and cosine. sin(x) returned, cos(x) in pcos
    __m256d r_sin, r_cos;
    r_sin = _mm256_sincos_pd(&r_cos, x);
    *pcos = r_cos;
    return r_sin;
}

static inline Vec8f tan (Vec8f const & x) {   // tangent
    return _mm256_tan_ps(x);
}
static inline Vec4d tan (Vec4d const & x) {   // tangent
    return _mm256_tan_pd(x);
}

// inverse trigonometric functions
static inline Vec8f asin (Vec8f const & x) {   // inverse sine
    return _mm256_asin_ps(x);
}
static inline Vec4d asin (Vec4d const & x) {   // inverse sine
    return _mm256_asin_pd(x);
}

static inline Vec8f acos (Vec8f const & x) {   // inverse cosine
    return _mm256_acos_ps(x);
}
static inline Vec4d acos (Vec4d const & x) {   // inverse cosine
    return _mm256_acos_pd(x);
}

static inline Vec8f atan (Vec8f const & x) {   // inverse tangent
    return _mm256_atan_ps(x);
}
static inline Vec4d atan (Vec4d const & x) {   // inverse tangent
    return _mm256_atan_pd(x);
}

static inline Vec8f atan2 (Vec8f const & a, Vec8f const & b) {   // inverse tangent of a/b
    return _mm256_atan2_ps(a,b);
}
static inline Vec4d atan2 (Vec4d const & a, Vec4d const & b) {   // inverse tangent of a/b
    return _mm256_atan2_pd(a,b);
}

#endif // VECTORMATH_COMMON_H

// hyperbolic functions and inverse hyperbolic functions
static inline Vec8f sinh (Vec8f const & x) {   // hyperbolic sine
    return _mm256_sinh_ps(x);
}
static inline Vec4d sinh (Vec4d const & x) {   // hyperbolic sine
    return _mm256_sinh_pd(x);
}

static inline Vec8f cosh (Vec8f const & x) {   // hyperbolic cosine
    return _mm256_cosh_ps(x);
}
static inline Vec4d cosh (Vec4d const & x) {   // hyperbolic cosine
    return _mm256_cosh_pd(x);
}

static inline Vec8f tanh (Vec8f const & x) {   // hyperbolic tangent
    return _mm256_tanh_ps(x);
}
static inline Vec4d tanh (Vec4d const & x) {   // hyperbolic tangent
    return _mm256_tanh_pd(x);
}

static inline Vec8f asinh (Vec8f const & x) {   // inverse hyperbolic sine
    return _mm256_asinh_ps(x);
}
static inline Vec4d asinh (Vec4d const & x) {   // inverse hyperbolic sine
    return _mm256_asinh_pd(x);
}

static inline Vec8f acosh (Vec8f const & x) {   // inverse hyperbolic cosine
    return _mm256_acosh_ps(x);
}
static inline Vec4d acosh (Vec4d const & x) {   // inverse hyperbolic cosine
    return _mm256_acosh_pd(x);
}

static inline Vec8f atanh (Vec8f const & x) {   // inverse hyperbolic tangent
    return _mm256_atanh_ps(x);
}
static inline Vec4d atanh (Vec4d const & x) {   // inverse hyperbolic tangent
    return _mm256_atanh_pd(x);
}

// error function
static inline Vec8f erf (Vec8f const & x) {   // error function
    return _mm256_erf_ps(x);
}
static inline Vec4d erf (Vec4d const & x) {   // error function
    return _mm256_erf_pd(x);
}

static inline Vec8f erfc (Vec8f const & x) {   // error function complement
    return _mm256_erfc_ps(x);
}
static inline Vec4d erfc (Vec4d const & x) {   // error function complement
    return _mm256_erfc_pd(x);
}

static inline Vec8f erfinv (Vec8f const & x) {   // inverse error function
    return _mm256_erfinv_ps(x);
}
static inline Vec4d erfinv (Vec4d const & x) {   // inverse error function
    return _mm256_erfinv_pd(x);
}

extern "C" {
extern __m256 __svml_cdfnormf8(__m256);  // not in immintrin.h
extern __m256d __svml_cdfnorm4(__m256d); // not in immintrin.h
}
static inline Vec8f cdfnorm (Vec8f const & x) {   // cumulative normal distribution function
    return __svml_cdfnormf8(x);
}
static inline Vec4d cdfnorm (Vec4d const & x) {   // cumulative normal distribution function
    return __svml_cdfnorm4(x);
}

static inline Vec8f cdfnorminv (Vec8f const & x) {   // inverse cumulative normal distribution function
    return _mm256_cdfnorminv_ps(x);
}
static inline Vec4d cdfnorminv (Vec4d const & x) {   // inverse cumulative normal distribution function
    return _mm256_cdfnorminv_pd(x);
}

// complex exponential function (real part in even numbered elements, imaginary part in odd numbered elements)
static inline Vec8f cexp (Vec8f const & x) {   // complex exponential function
    return Vec8f(cexp(x.get_low()), cexp(x.get_high()));
}
static inline Vec4d cexp (Vec4d const & x) {   // complex exponential function
    return Vec4d(cexp(x.get_low()), cexp(x.get_high()));
}

#endif // VECTORF256_H >= 2

#else
#error unknown value of VECTORMATH
#endif // VECTORMATH


#if defined (VECTORF256_H) && VECTORF256_H == 1 && (VECTORMATH == 2 || VECTORMATH == 3)
/*****************************************************************************
*
*      VECTORF256_H == 1. 256 bit vectors emulated as two 128-bit vectors,
*      SVML library
*
*****************************************************************************/

#ifndef VECTORMATH_COMMON_H

// exponential and power functions
static inline Vec8f exp (Vec8f const & x) {   // exponential function
    return Vec8f(exp(x.get_low()), exp(x.get_high()));
}
static inline Vec4d exp (Vec4d const & x) {   // exponential function
    return Vec4d(exp(x.get_low()), exp(x.get_high()));
}

static inline Vec8f expm1 (Vec8f const & x) {   // exp(x)-1. Avoids loss of precision if x is close to 1
    return Vec8f(expm1(x.get_low()), expm1(x.get_high()));
}
static inline Vec4d expm1 (Vec4d const & x) {   // exp(x)-1. Avoids loss of precision if x is close to 1
    return Vec4d(expm1(x.get_low()), expm1(x.get_high()));
}

static inline Vec8f exp2 (Vec8f const & x) {   // pow(2,x)
    return Vec8f(exp2(x.get_low()), exp2(x.get_high()));
}
static inline Vec4d exp2 (Vec4d const & x) {   // pow(2,x)
    return Vec4d(exp2(x.get_low()), exp2(x.get_high()));
}

static inline Vec8f exp10 (Vec8f const & x) {   // pow(10,x)
    return Vec8f(exp10(x.get_low()), exp10(x.get_high()));
}
static inline Vec4d exp10 (Vec4d const & x) {   // pow(10,x)
    return Vec4d(exp10(x.get_low()), exp10(x.get_high()));
}

static inline Vec8f pow (Vec8f const & a, Vec8f const & b) {   // pow(a,b) = a to the power of b
    return Vec8f(pow(a.get_low(),b.get_low()), pow(a.get_high(),b.get_high()));
}
static inline Vec4d pow (Vec4d const & a, Vec4d const & b) {   // pow(a,b) = a to the power of b
    return Vec4d(pow(a.get_low(),b.get_low()), pow(a.get_high(),b.get_high()));
}

static inline Vec8f cbrt (Vec8f const & x) {   // pow(x,1/3)
    return Vec8f(cbrt(x.get_low()), cbrt(x.get_high()));
}
static inline Vec4d cbrt (Vec4d const & x) {   // pow(x,1/3)
    return Vec4d(cbrt(x.get_low()), cbrt(x.get_high()));
}

// logarithms
static inline Vec8f log (Vec8f const & x) {   // natural logarithm
    return Vec8f(log(x.get_low()), log(x.get_high()));
}
static inline Vec4d log (Vec4d const & x) {   // natural logarithm
    return Vec4d(log(x.get_low()), log(x.get_high()));
}

static inline Vec8f log1p (Vec8f const & x) {   // log(1+x). Avoids loss of precision if 1+x is close to 1
    return Vec8f(log1p(x.get_low()), log1p(x.get_high()));
}
static inline Vec4d log1p (Vec4d const & x) {   // log(1+x). Avoids loss of precision if 1+x is close to 1
    return Vec4d(log1p(x.get_low()), log1p(x.get_high()));
}

static inline Vec8f log2 (Vec8f const & x) {   // logarithm base 2
    return Vec8f(log2(x.get_low()), log2(x.get_high()));
}
static inline Vec4d log2 (Vec4d const & x) {   // logarithm base 2
    return Vec4d(log2(x.get_low()), log2(x.get_high()));
}

static inline Vec8f log10 (Vec8f const & x) {   // logarithm base 10
    return Vec8f(log10(x.get_low()), log10(x.get_high()));
}
static inline Vec4d log10 (Vec4d const & x) {   // logarithm base 10
    return Vec4d(log10(x.get_low()), log10(x.get_high()));
}

// trigonometric functions (angles in radians)
static inline Vec8f sin (Vec8f const & x) {   // sine
    return Vec8f(sin(x.get_low()), sin(x.get_high()));
}
static inline Vec4d sin (Vec4d const & x) {   // sine
    return Vec4d(sin(x.get_low()), sin(x.get_high()));
}

static inline Vec8f cos (Vec8f const & x) {   // cosine
    return Vec8f(cos(x.get_low()), cos(x.get_high()));
}
static inline Vec4d cos (Vec4d const & x) {   // cosine
    return Vec4d(cos(x.get_low()), cos(x.get_high()));
}

#if defined(__unix__) || defined(__INTEL_COMPILER) || !defined(__x86_64__) || !defined(_MSC_VER)
// no inline assembly in 64 bit MS compiler
static inline Vec8f sincos (Vec8f * pcos, Vec8f const & x) {   // sine and cosine. sin(x) returned, cos(x) in pcos
    Vec4f r_sin0, r_sin1, r_cos0, r_cos1;
    r_sin0 = sincos(&r_cos0, x.get_low()); 
    r_sin1 = sincos(&r_cos1, x.get_high());
    *pcos = Vec8f(r_cos0, r_cos1);
    return Vec8f(r_sin0, r_sin1); 
}
static inline Vec4d sincos (Vec4d * pcos, Vec4d const & x) {   // sine and cosine. sin(x) returned, cos(x) in pcos
    Vec2d r_sin0, r_sin1, r_cos0, r_cos1;
    r_sin0 = sincos(&r_cos0, x.get_low()); 
    r_sin1 = sincos(&r_cos1, x.get_high());
    *pcos = Vec4d(r_cos0, r_cos1);
    return Vec4d(r_sin0, r_sin1); 
}
#endif // inline assembly available

static inline Vec8f tan (Vec8f const & x) {   // tangent
    return Vec8f(tan(x.get_low()), tan(x.get_high()));
}
static inline Vec4d tan (Vec4d const & x) {   // tangent
    return Vec4d(tan(x.get_low()), tan(x.get_high()));
}

// inverse trigonometric functions
static inline Vec8f asin (Vec8f const & x) {   // inverse sine
    return Vec8f(asin(x.get_low()), asin(x.get_high()));
}
static inline Vec4d asin (Vec4d const & x) {   // inverse sine
    return Vec4d(asin(x.get_low()), asin(x.get_high()));
}

static inline Vec8f acos (Vec8f const & x) {   // inverse cosine
    return Vec8f(acos(x.get_low()), acos(x.get_high()));
}
static inline Vec4d acos (Vec4d const & x) {   // inverse cosine
    return Vec4d(acos(x.get_low()), acos(x.get_high()));
}

static inline Vec8f atan (Vec8f const & x) {   // inverse tangent
    return Vec8f(atan(x.get_low()), atan(x.get_high()));
}
static inline Vec4d atan (Vec4d const & x) {   // inverse tangent
    return Vec4d(atan(x.get_low()), atan(x.get_high()));
}

static inline Vec8f atan2 (Vec8f const & a, Vec8f const & b) {   // inverse tangent of a/b
    return Vec8f(atan2(a.get_low(),b.get_low()), atan2(a.get_high(),b.get_high()));
}
static inline Vec4d atan2 (Vec4d const & a, Vec4d const & b) {   // inverse tangent of a/b
    return Vec4d(atan2(a.get_low(),b.get_low()), atan2(a.get_high(),b.get_high()));
}

#endif // VECTORMATH_COMMON_H

// hyperbolic functions and inverse hyperbolic functions
static inline Vec8f sinh (Vec8f const & x) {   // hyperbolic sine
    return Vec8f(sinh(x.get_low()), sinh(x.get_high()));
}
static inline Vec4d sinh (Vec4d const & x) {   // hyperbolic sine
    return Vec4d(sinh(x.get_low()), sinh(x.get_high()));
}

static inline Vec8f cosh (Vec8f const & x) {   // hyperbolic cosine
    return Vec8f(cosh(x.get_low()), cosh(x.get_high()));
}
static inline Vec4d cosh (Vec4d const & x) {   // hyperbolic cosine
    return Vec4d(cosh(x.get_low()), cosh(x.get_high()));
}

static inline Vec8f tanh (Vec8f const & x) {   // hyperbolic tangent
    return Vec8f(tanh(x.get_low()), tanh(x.get_high()));
}
static inline Vec4d tanh (Vec4d const & x) {   // hyperbolic tangent
    return Vec4d(tanh(x.get_low()), tanh(x.get_high()));
}

static inline Vec8f asinh (Vec8f const & x) {   // inverse hyperbolic sine
    return Vec8f(asinh(x.get_low()), asinh(x.get_high()));
}
static inline Vec4d asinh (Vec4d const & x) {   // inverse hyperbolic sine
    return Vec4d(asinh(x.get_low()), asinh(x.get_high()));
}

static inline Vec8f acosh (Vec8f const & x) {   // inverse hyperbolic cosine
    return Vec8f(acosh(x.get_low()), acosh(x.get_high()));
}
static inline Vec4d acosh (Vec4d const & x) {   // inverse hyperbolic cosine
    return Vec4d(acosh(x.get_low()), acosh(x.get_high()));
}

static inline Vec8f atanh (Vec8f const & x) {   // inverse hyperbolic tangent
    return Vec8f(atanh(x.get_low()), atanh(x.get_high()));
}
static inline Vec4d atanh (Vec4d const & x) {   // inverse hyperbolic tangent
    return Vec4d(atanh(x.get_low()), atanh(x.get_high()));
}

// error function
static inline Vec8f erf (Vec8f const & x) {   // error function
    return Vec8f(erf(x.get_low()), erf(x.get_high()));
}
static inline Vec4d erf (Vec4d const & x) {   // error function
    return Vec4d(erf(x.get_low()), erf(x.get_high()));
}

static inline Vec8f erfc (Vec8f const & x) {   // error function complement
    return Vec8f(erfc(x.get_low()), erfc(x.get_high()));
}
static inline Vec4d erfc (Vec4d const & x) {   // error function complement
    return Vec4d(erfc(x.get_low()), erfc(x.get_high()));
}

static inline Vec8f erfinv (Vec8f const & x) {   // inverse error function
    return Vec8f(erfinv(x.get_low()), erfinv(x.get_high()));
}
static inline Vec4d erfinv (Vec4d const & x) {   // inverse error function
    return Vec4d(erfinv(x.get_low()), erfinv(x.get_high()));
}

static inline Vec8f cdfnorm (Vec8f const & x) {   // cumulative normal distribution function
    return Vec8f(cdfnorm(x.get_low()), cdfnorm(x.get_high()));
}
static inline Vec4d cdfnorm (Vec4d const & x) {   // cumulative normal distribution function
    return Vec4d(cdfnorm(x.get_low()), cdfnorm(x.get_high()));
}

static inline Vec8f cdfnorminv (Vec8f const & x) {   // inverse cumulative normal distribution function
    return Vec8f(cdfnorminv(x.get_low()), cdfnorminv(x.get_high()));
}
static inline Vec4d cdfnorminv (Vec4d const & x) {   // inverse cumulative normal distribution function
    return Vec4d(cdfnorminv(x.get_low()), cdfnorminv(x.get_high()));
}

// complex exponential function (real part in even numbered elements, imaginary part in odd numbered elements)
static inline Vec8f cexp (Vec8f const & x) {   // complex exponential function
    return Vec8f(cexp(x.get_low()), cexp(x.get_high()));
}
static inline Vec4d cexp (Vec4d const & x) {   // complex exponential function
    return Vec4d(cexp(x.get_low()), cexp(x.get_high()));
}

#endif // VECTORF256_H == 1

#endif // VECTORMATH_LIB_H
