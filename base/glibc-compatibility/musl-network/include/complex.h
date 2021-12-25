#ifndef _COMPLEX_H
#define _COMPLEX_H

#ifdef __cplusplus
extern "C" {
#endif

#define complex _Complex
#ifdef __GNUC__
#define _Complex_I (__extension__ (0.0f+1.0fi))
#else
#define _Complex_I (0.0f+1.0fi)
#endif
#define I _Complex_I

double complex cacos(double complex);
float complex cacosf(float complex);
long double complex cacosl(long double complex);

double complex casin(double complex);
float complex casinf(float complex);
long double complex casinl(long double complex);

double complex catan(double complex);
float complex catanf(float complex);
long double complex catanl(long double complex);

double complex ccos(double complex);
float complex ccosf(float complex);
long double complex ccosl(long double complex);

double complex csin(double complex);
float complex csinf(float complex);
long double complex csinl(long double complex);

double complex ctan(double complex);
float complex ctanf(float complex);
long double complex ctanl(long double complex);

double complex cacosh(double complex);
float complex cacoshf(float complex);
long double complex cacoshl(long double complex);

double complex casinh(double complex);
float complex casinhf(float complex);
long double complex casinhl(long double complex);

double complex catanh(double complex);
float complex catanhf(float complex);
long double complex catanhl(long double complex);

double complex ccosh(double complex);
float complex ccoshf(float complex);
long double complex ccoshl(long double complex);

double complex csinh(double complex);
float complex csinhf(float complex);
long double complex csinhl(long double complex);

double complex ctanh(double complex);
float complex ctanhf(float complex);
long double complex ctanhl(long double complex);

double complex cexp(double complex);
float complex cexpf(float complex);
long double complex cexpl(long double complex);

double complex clog(double complex);
float complex clogf(float complex);
long double complex clogl(long double complex);

double cabs(double complex);
float cabsf(float complex);
long double cabsl(long double complex);

double complex cpow(double complex, double complex);
float complex cpowf(float complex, float complex);
long double complex cpowl(long double complex, long double complex);

double complex csqrt(double complex);
float complex csqrtf(float complex);
long double complex csqrtl(long double complex);

double carg(double complex);
float cargf(float complex);
long double cargl(long double complex);

double cimag(double complex);
float cimagf(float complex);
long double cimagl(long double complex);

double complex conj(double complex);
float complex conjf(float complex);
long double complex conjl(long double complex);

double complex cproj(double complex);
float complex cprojf(float complex);
long double complex cprojl(long double complex);

double creal(double complex);
float crealf(float complex);
long double creall(long double complex);

#ifndef __cplusplus
#define __CIMAG(x, t) \
	(+(union { _Complex t __z; t __xy[2]; }){(_Complex t)(x)}.__xy[1])

#define creal(x) ((double)(x))
#define crealf(x) ((float)(x))
#define creall(x) ((long double)(x))

#define cimag(x) __CIMAG(x, double)
#define cimagf(x) __CIMAG(x, float)
#define cimagl(x) __CIMAG(x, long double)
#endif

#if __STDC_VERSION__ >= 201112L
#if defined(_Imaginary_I)
#define __CMPLX(x, y, t) ((t)(x) + _Imaginary_I*(t)(y))
#elif defined(__clang__)
#define __CMPLX(x, y, t) (+(_Complex t){ (t)(x), (t)(y) })
#else
#define __CMPLX(x, y, t) (__builtin_complex((t)(x), (t)(y)))
#endif
#define CMPLX(x, y) __CMPLX(x, y, double)
#define CMPLXF(x, y) __CMPLX(x, y, float)
#define CMPLXL(x, y) __CMPLX(x, y, long double)
#endif

#ifdef __cplusplus
}
#endif
#endif
