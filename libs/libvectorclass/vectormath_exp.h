/****************************  vectormath_exp.h   ******************************
* Author:        Agner Fog
* Date created:  2014-04-18
* Last modified: 2014-10-22
* Version:       1.16
* Project:       vector classes
* Description:
* Header file containing inline vector functions of logarithms, exponential 
* and power functions:
* exp         exponential function
* exmp1       exponential function minus 1
* log         natural logarithm
* log1p       natural logarithm of 1+x
* cbrt        cube root
* pow         raise vector elements to power
* pow_ratio   raise vector elements to rational power
*
* Theory, methods and inspiration based partially on these sources:
* > Moshier, Stephen Lloyd Baluk: Methods and programs for mathematical functions.
*   Ellis Horwood, 1989.
* > VDT library developed on CERN by Danilo Piparo, Thomas Hauth and
*   Vincenzo Innocente, 2012, https://svnweb.cern.ch/trac/vdt
* > Cephes math library by Stephen L. Moshier 1992,
*   http://www.netlib.org/cephes/
*
* For detailed instructions, see vectormath_common.h and VectorClass.pdf
*
* (c) Copyright 2014 GNU General Public License http://www.gnu.org/licenses
******************************************************************************/

#ifndef VECTORMATH_EXP_H
#define VECTORMATH_EXP_H  1 

#include "vectormath_common.h"  


/******************************************************************************
*                 Exponential functions
******************************************************************************/

// Helper functions, used internally:

// This function calculates pow(2,n) where n must be an integer. Does not check for overflow or underflow
static inline Vec2d vm_pow2n (Vec2d const & n) {
    const double pow2_52 = 4503599627370496.0;   // 2^52
    const double bias = 1023.0;                  // bias in exponent
    Vec2d a = n + (bias + pow2_52);              // put n + bias in least significant bits
    Vec2q b = reinterpret_i(a);                  // bit-cast to integer
    Vec2q c = b << 52;                           // shift left 52 places to get into exponent field
    Vec2d d = reinterpret_d(c);                  // bit-cast back to double
    return d;
}

static inline Vec4f vm_pow2n (Vec4f const & n) {
    const float pow2_23 =  8388608.0;            // 2^23
    const float bias = 127.0;                    // bias in exponent
    Vec4f a = n + (bias + pow2_23);              // put n + bias in least significant bits
    Vec4i b = reinterpret_i(a);                  // bit-cast to integer
    Vec4i c = b << 23;                           // shift left 23 places to get into exponent field
    Vec4f d = reinterpret_f(c);                  // bit-cast back to float
    return d;
}

#if MAX_VECTOR_SIZE >= 256

static inline Vec4d vm_pow2n (Vec4d const & n) {
    const double pow2_52 = 4503599627370496.0;   // 2^52
    const double bias = 1023.0;                  // bias in exponent
    Vec4d a = n + (bias + pow2_52);              // put n + bias in least significant bits
    Vec4q b = reinterpret_i(a);                  // bit-cast to integer
    Vec4q c = b << 52;                           // shift left 52 places to get value into exponent field
    Vec4d d = reinterpret_d(c);                  // bit-cast back to double
    return d;
}

static inline Vec8f vm_pow2n (Vec8f const & n) {
    const float pow2_23 =  8388608.0;            // 2^23
    const float bias = 127.0;                    // bias in exponent
    Vec8f a = n + (bias + pow2_23);              // put n + bias in least significant bits
    Vec8i b = reinterpret_i(a);                  // bit-cast to integer
    Vec8i c = b << 23;                           // shift left 23 places to get into exponent field
    Vec8f d = reinterpret_f(c);                  // bit-cast back to float
    return d;
}

#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512

static inline Vec8d vm_pow2n (Vec8d const & n) {
    const double pow2_52 = 4503599627370496.0;   // 2^52
    const double bias = 1023.0;                  // bias in exponent
    Vec8d a = n + (bias + pow2_52);              // put n + bias in least significant bits
    Vec8q b = Vec8q(reinterpret_i(a));           // bit-cast to integer
    Vec8q c = b << 52;                           // shift left 52 places to get value into exponent field
    Vec8d d = Vec8d(reinterpret_d(c));           // bit-cast back to double
    return d;
}

static inline Vec16f vm_pow2n (Vec16f const & n) {
    const float pow2_23 =  8388608.0;            // 2^23
    const float bias = 127.0;                    // bias in exponent
    Vec16f a = n + (bias + pow2_23);             // put n + bias in least significant bits
    Vec16i b = Vec16i(reinterpret_i(a));         // bit-cast to integer
    Vec16i c = b << 23;                          // shift left 23 places to get into exponent field
    Vec16f d = Vec16f(reinterpret_f(c));         // bit-cast back to float
    return d;
}

#endif // MAX_VECTOR_SIZE >= 512


// Template for exp function, double precision
// The limit of abs(x) is defined by max_x below
// This function does not produce denormals
// Template parameters:
// VTYPE: double vector type
// BTYPE: boolean vector type
// M1: 0 for exp, 1 for expm1
// BA: 0 for exp, 1 for 0.5*exp, 2 for pow(2,x), 10 for pow(10,x)

#if 1  // choose method

// Taylor expansion
template<class VTYPE, class BTYPE, int M1, int BA> 
static inline VTYPE exp_d(VTYPE const & initial_x) {    

    // Taylor coefficients, 1/n!
    // Not using minimax approximation because we prioritize precision close to x = 0
    const double p2  = 1./2.;
    const double p3  = 1./6.;
    const double p4  = 1./24.;
    const double p5  = 1./120.; 
    const double p6  = 1./720.; 
    const double p7  = 1./5040.; 
    const double p8  = 1./40320.; 
    const double p9  = 1./362880.; 
    const double p10 = 1./3628800.; 
    const double p11 = 1./39916800.; 
    const double p12 = 1./479001600.; 
    const double p13 = 1./6227020800.; 

    // maximum abs(x), value depends on BA, defined below
    // The lower limit of x is slightly more restrictive than the upper limit.
    // We are specifying the lower limit, except for BA = 1 because it is not used for negative x
    double max_x;

    // data vectors
    VTYPE x, r, z, n2;
    BTYPE inrange;                               // boolean vector

    if (BA <= 1) { // exp(x)
        max_x = BA == 0 ? 708.39 : 709.7; // lower limit for 0.5*exp(x) is -707.6, but we are using 0.5*exp(x) only for positive x in hyperbolic functions
        const double ln2d_hi = 0.693145751953125;
        const double ln2d_lo = 1.42860682030941723212E-6;
        x  = initial_x;
        r  = round(initial_x*VM_LOG2E);
        // subtraction in two steps for higher precision
        x = nmul_add(r, ln2d_hi, x);             //  x -= r * ln2d_hi;
        x = nmul_add(r, ln2d_lo, x);             //  x -= r * ln2d_lo;
    }
    else if (BA == 2) { // pow(2,x)
        max_x = 1022.0;
        r  = round(initial_x);
        x  = initial_x - r;
        x *= VM_LN2;
    }
    else if (BA == 10) { // pow(10,x)
        max_x = 307.65;
        const double log10_2_hi = 0.30102999554947019;     // log10(2) in two parts
        const double log10_2_lo = 1.1451100899212592E-10;
        x  = initial_x;
        r  = round(initial_x*(VM_LOG2E*VM_LN10));
        // subtraction in two steps for higher precision
        x  = nmul_add(r, log10_2_hi, x);         //  x -= r * log10_2_hi;
        x  = nmul_add(r, log10_2_lo, x);         //  x -= r * log10_2_lo;
        x *= VM_LN10;
    }
    else  {  // undefined value of BA
        return 0.;
    }

    z = polynomial_13m(x, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13);

    if (BA == 1) r--;  // 0.5 * exp(x)

    // multiply by power of 2 
    n2 = vm_pow2n(r);

    if (M1 == 0) {
        // exp
        z = (z + 1.0) * n2;
    }
    else {
        // expm1
        z = mul_add(z, n2, n2 - 1.0);            // z = z * n2 + (n2 - 1.0);
    }

    // check for overflow
    inrange  = abs(initial_x) < max_x;
    // check for INF and NAN
    inrange &= is_finite(initial_x);

    if (horizontal_and(inrange)) {
        // fast normal path
        return z;
    }
    else {
        // overflow, underflow and NAN
        r = select(sign_bit(initial_x), 0.-M1, infinite_vec<VTYPE>()); // value in case of +/- overflow or INF
        z = select(inrange, z, r);                                     // +/- underflow
        z = select(is_nan(initial_x), initial_x, z);                   // NAN goes through
        return z;
    }
}

#else

// Pade expansion uses less code and fewer registers, but is slower
template<class VTYPE, class BTYPE, int M1, int BA> 
static inline VTYPE exp_d(VTYPE const & initial_x) {

    // define constants
    const double ln2p1   = 0.693145751953125;
    const double ln2p2   = 1.42860682030941723212E-6;
    const double log2e   = VM_LOG2E;
    const double max_exp = 708.39;
    // coefficients of pade polynomials
    const double P0exp = 9.99999999999999999910E-1;
    const double P1exp = 3.02994407707441961300E-2;
    const double P2exp = 1.26177193074810590878E-4;
    const double Q0exp = 2.00000000000000000009E0;
    const double Q1exp = 2.27265548208155028766E-1;
    const double Q2exp = 2.52448340349684104192E-3;
    const double Q3exp = 3.00198505138664455042E-6;

    VTYPE x, r, xx, px, qx, y, n2;               // data vectors
    BTYPE inrange;                               // boolean vector

    x = initial_x;
    r = round(initial_x*log2e);

    // subtraction in one step would gives loss of precision
    x -= r * ln2p1;
    x -= r * ln2p2;

    xx = x * x;

    // px = x * P(x^2).
    px = polynomial_2(xx, P0exp, P1exp, P2exp) * x;

    // Evaluate Q(x^2).
    qx = polynomial_3(xx, Q0exp, Q1exp, Q2exp, Q3exp);

    // e^x = 1 + 2*P(x^2)/( Q(x^2) - P(x^2) )
    y = (2.0 * px) / (qx - px);

    // Get 2^n in double.
    // n  = round_to_int64_limited(r);
    // n2 = exp2(n);
    n2 = vm_pow2n(r);  // this is faster

    if (M1 == 0) {
        // exp
        y = (y + 1.0) * n2;
    }
    else {
        // expm1
        y = y * n2 + (n2 - 1.0);
    }

    // overflow
    inrange  = abs(initial_x) < max_exp;
    // check for INF and NAN
    inrange &= is_finite(initial_x);

    if (horizontal_and(inrange)) {
        // fast normal path
        return y;
    }
    else {
        // overflow, underflow and NAN
        r = select(sign_bit(initial_x), 0.-M1, infinite_vec<VTYPE>()); // value in case of overflow or INF
        y = select(inrange, y, r);                                     // +/- overflow
        y = select(is_nan(initial_x), initial_x, y);                   // NAN goes through
        return y;
    }
}
#endif

// instances of exp_d template
static inline Vec2d exp(Vec2d const & x) {
    return exp_d<Vec2d, Vec2db, 0, 0>(x);
}

static inline Vec2d expm1(Vec2d const & x) {
    return exp_d<Vec2d, Vec2db, 1, 0>(x);
}

static inline Vec2d exp2(Vec2d const & x) {
    return exp_d<Vec2d, Vec2db, 0, 2>(x);
}

static inline Vec2d exp10(Vec2d const & x) {
    return exp_d<Vec2d, Vec2db, 0, 10>(x);
}

#if MAX_VECTOR_SIZE >= 256

static inline Vec4d exp(Vec4d const & x) {
    return exp_d<Vec4d, Vec4db, 0, 0>(x);
}

static inline Vec4d expm1(Vec4d const & x) {
    return exp_d<Vec4d, Vec4db, 1, 0>(x);
}

static inline Vec4d exp2(Vec4d const & x) {
    return exp_d<Vec4d, Vec4db, 0, 2>(x);
}

static inline Vec4d exp10(Vec4d const & x) {
    return exp_d<Vec4d, Vec4db, 0, 10>(x);
}

#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512

static inline Vec8d exp(Vec8d const & x) {
    return exp_d<Vec8d, Vec8db, 0, 0>(x);
}

static inline Vec8d expm1(Vec8d const & x) {
    return exp_d<Vec8d, Vec8db, 1, 0>(x);
}

static inline Vec8d exp2(Vec8d const & x) {
    return exp_d<Vec8d, Vec8db, 0, 2>(x);
}

static inline Vec8d exp10(Vec8d const & x) {
    return exp_d<Vec8d, Vec8db, 0, 10>(x);
}

#endif // MAX_VECTOR_SIZE >= 512

// Template for exp function, single precision
// The limit of abs(x) is defined by max_x below
// This function does not produce denormals
// Template parameters:
// VTYPE: float vector type
// BTYPE: boolean vector type
// M1: 0 for exp, 1 for expm1
// BA: 0 for exp, 1 for 0.5*exp, 2 for pow(2,x), 10 for pow(10,x)

template<class VTYPE, class BTYPE, int M1, int BA> 
static inline VTYPE exp_f(VTYPE const & initial_x) {

    // Taylor coefficients
    const float P0expf   =  1.f/2.f;
    const float P1expf   =  1.f/6.f;
    const float P2expf   =  1.f/24.f;
    const float P3expf   =  1.f/120.f; 
    const float P4expf   =  1.f/720.f; 
    const float P5expf   =  1.f/5040.f; 

    VTYPE x, r, x2, z, n2;                       // data vectors        
    BTYPE inrange;                               // boolean vector

    // maximum abs(x), value depends on BA, defined below
    // The lower limit of x is slightly more restrictive than the upper limit.
    // We are specifying the lower limit, except for BA = 1 because it is not used for negative x
    float max_x;

    if (BA <= 1) { // exp(x)
        const float ln2f_hi  =  0.693359375f;
        const float ln2f_lo  = -2.12194440e-4f;
        max_x = (BA == 0) ? 87.3f : 89.0f;

        x = initial_x;
        r = round(initial_x*float(VM_LOG2E));
        x = nmul_add(r, VTYPE(ln2f_hi), x);      //  x -= r * ln2f_hi;
        x = nmul_add(r, VTYPE(ln2f_lo), x);      //  x -= r * ln2f_lo;
    }
    else if (BA == 2) {                          // pow(2,x)
        max_x = 126.f;
        r = round(initial_x);
        x = initial_x - r;
        x = x * (float)VM_LN2;
    }
    else if (BA == 10) {                         // pow(10,x)
        max_x = 37.9f;
        const float log10_2_hi = 0.301025391f;   // log10(2) in two parts
        const float log10_2_lo = 4.60503907E-6f;
        x = initial_x;
        r = round(initial_x*float(VM_LOG2E*VM_LN10));
        x = nmul_add(r, VTYPE(log10_2_hi), x);   //  x -= r * log10_2_hi;
        x = nmul_add(r, VTYPE(log10_2_lo), x);   //  x -= r * log10_2_lo;
        x = x * (float)VM_LN10;
    }
    else  {  // undefined value of BA
        return 0.;
    }

    x2 = x * x;
    z = polynomial_5(x,P0expf,P1expf,P2expf,P3expf,P4expf,P5expf);    
    z = mul_add(z, x2, x);                       // z *= x2;  z += x;

    if (BA == 1) r--;                            // 0.5 * exp(x)

    // multiply by power of 2 
    n2 = vm_pow2n(r);

    if (M1 == 0) {
        // exp
        z = (z + 1.0f) * n2;
    }
    else {
        // expm1
        z = mul_add(z, n2, n2 - 1.0f);           //  z = z * n2 + (n2 - 1.0f);
    }

    // check for overflow
    inrange  = abs(initial_x) < max_x;
    // check for INF and NAN
    inrange &= is_finite(initial_x);

    if (horizontal_and(inrange)) {
        // fast normal path
        return z;
    }
    else {
        // overflow, underflow and NAN
        r = select(sign_bit(initial_x), 0.f-M1, infinite_vec<VTYPE>()); // value in case of +/- overflow or INF
        z = select(inrange, z, r);                                      // +/- underflow
        z = select(is_nan(initial_x), initial_x, z);                    // NAN goes through
        return z;
    }
}

// instances of exp_f template
static inline Vec4f exp(Vec4f const & x) {
    return exp_f<Vec4f, Vec4fb, 0, 0>(x);
}

static inline Vec4f expm1(Vec4f const & x) {
    return exp_f<Vec4f, Vec4fb, 1, 0>(x);
}

static inline Vec4f exp2(Vec4f const & x) {
    return exp_f<Vec4f, Vec4fb, 0, 2>(x);
}

static inline Vec4f exp10(Vec4f const & x) {
    return exp_f<Vec4f, Vec4fb, 0, 10>(x);
}

#if MAX_VECTOR_SIZE >= 256

static inline Vec8f exp(Vec8f const & x) {
    return exp_f<Vec8f, Vec8fb, 0, 0>(x);
}

static inline Vec8f expm1(Vec8f const & x) {
    return exp_f<Vec8f, Vec8fb, 1, 0>(x);
}

static inline Vec8f exp2(Vec8f const & x) {
    return exp_f<Vec8f, Vec8fb, 0, 2>(x);
}

static inline Vec8f exp10(Vec8f const & x) {
    return exp_f<Vec8f, Vec8fb, 0, 10>(x);
}

#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512

static inline Vec16f exp(Vec16f const & x) {
    return exp_f<Vec16f, Vec16fb, 0, 0>(x);
}

static inline Vec16f expm1(Vec16f const & x) {
    return exp_f<Vec16f, Vec16fb, 1, 0>(x);
}

static inline Vec16f exp2(Vec16f const & x) {
    return exp_f<Vec16f, Vec16fb, 0, 2>(x);
}

static inline Vec16f exp10(Vec16f const & x) {
    return exp_f<Vec16f, Vec16fb, 0, 10>(x);
}

#endif // MAX_VECTOR_SIZE >= 512


/******************************************************************************
*                 Logarithm functions
******************************************************************************/

// Helper functions: fraction_2(x) = fraction(x)*0.5

// Modified fraction function:
// Extract the fraction part of a floating point number, and divide by 2
// The fraction function is defined in vectorf128.h etc.
// fraction_2(x) = fraction(x)*0.5
// This version gives half the fraction without extra delay
// Does not work for x = 0
static inline Vec4f fraction_2(Vec4f const & a) {
    Vec4ui t1 = _mm_castps_si128(a);   // reinterpret as 32-bit integer
    Vec4ui t2 = Vec4ui((t1 & 0x007FFFFF) | 0x3F000000); // set exponent to 0 + bias
    return _mm_castsi128_ps(t2);
}

static inline Vec2d fraction_2(Vec2d const & a) {
    Vec2uq t1 = _mm_castpd_si128(a);   // reinterpret as 64-bit integer
    Vec2uq t2 = Vec2uq((t1 & 0x000FFFFFFFFFFFFFll) | 0x3FE0000000000000ll); // set exponent to 0 + bias
    return _mm_castsi128_pd(t2);
}

#if MAX_VECTOR_SIZE >= 256

static inline Vec8f fraction_2(Vec8f const & a) {
#if defined (VECTORI256_H) && VECTORI256_H > 2  // 256 bit integer vectors are available, AVX2
    Vec8ui t1 = _mm256_castps_si256(a);   // reinterpret as 32-bit integer
    Vec8ui t2 = (t1 & 0x007FFFFF) | 0x3F000000; // set exponent to 0 + bias
    return _mm256_castsi256_ps(t2);
#else
    return Vec8f(fraction_2(a.get_low()), fraction_2(a.get_high()));
#endif
}

static inline Vec4d fraction_2(Vec4d const & a) {
#if VECTORI256_H > 1  // AVX2
    Vec4uq t1 = _mm256_castpd_si256(a);   // reinterpret as 64-bit integer
    Vec4uq t2 = Vec4uq((t1 & 0x000FFFFFFFFFFFFFll) | 0x3FE0000000000000ll); // set exponent to 0 + bias
    return _mm256_castsi256_pd(t2);
#else
    return Vec4d(fraction_2(a.get_low()), fraction_2(a.get_high()));
#endif
}

#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512

static inline Vec16f fraction_2(Vec16f const & a) {
#if INSTRSET >= 9                    // 512 bit integer vectors are available, AVX512
    return _mm512_getmant_ps(a, _MM_MANT_NORM_p5_1, _MM_MANT_SIGN_zero);
    //return Vec16f(_mm512_getmant_ps(a, _MM_MANT_NORM_1_2, _MM_MANT_SIGN_zero)) * 0.5f;
#else
    return Vec16f(fraction_2(a.get_low()), fraction_2(a.get_high()));
#endif
}

static inline Vec8d fraction_2(Vec8d const & a) {
#if INSTRSET >= 9                    // 512 bit integer vectors are available, AVX512
    return _mm512_getmant_pd(a, _MM_MANT_NORM_p5_1, _MM_MANT_SIGN_zero);
    //return Vec8d(_mm512_getmant_pd(a, _MM_MANT_NORM_1_2, _MM_MANT_SIGN_zero)) * 0.5;
#else
    return Vec8d(fraction_2(a.get_low()), fraction_2(a.get_high()));
#endif
}

#endif // MAX_VECTOR_SIZE >= 512


// Helper functions: exponent_f(x) = exponent(x) as floating point number

union vm_ufi {
    float f;
    uint32_t i;
};

union vm_udi {
    double d;
    uint64_t i;
};

// extract exponent of a positive number x as a floating point number
static inline Vec4f exponent_f(Vec4f const & x) {
    const float pow2_23 =  8388608.0f;           // 2^23
    const float bias = 127.f;                    // bias in exponent
    const vm_ufi upow2_23 = {pow2_23};
    Vec4ui a = reinterpret_i(x);                 // bit-cast x to integer
    Vec4ui b = a >> 23;                          // shift down exponent to low bits
    Vec4ui c = b | Vec4ui(upow2_23.i);           // insert new exponent
    Vec4f  d = reinterpret_f(c);                 // bit-cast back to double
    Vec4f  e = d - (pow2_23 + bias);             // subtract magic number and bias
    return e;
}

static inline Vec2d exponent_f(Vec2d const & x) {
    const double pow2_52 = 4503599627370496.0;   // 2^52
    const double bias = 1023.0;                  // bias in exponent
    const vm_udi upow2_52 = {pow2_52};

    Vec2uq a = reinterpret_i(x);                 // bit-cast x to integer
    Vec2uq b = a >> 52;                          // shift down exponent to low bits
    Vec2uq c = b | Vec2uq(upow2_52.i);           // insert new exponent
    Vec2d  d = reinterpret_d(c);                 // bit-cast back to double
    Vec2d  e = d - (pow2_52 + bias);             // subtract magic number and bias
    return e;
}

#if MAX_VECTOR_SIZE >= 256

static inline Vec8f exponent_f(Vec8f const & x) {
    const float pow2_23 =  8388608.0f;           // 2^23
    const float bias = 127.f;                    // bias in exponent
    const vm_ufi upow2_23 = {pow2_23};
    Vec8ui a = reinterpret_i(x);                 // bit-cast x to integer
    Vec8ui b = a >> 23;                          // shift down exponent to low bits
    Vec8ui c = b | Vec8ui(upow2_23.i);           // insert new exponent
    Vec8f  d = reinterpret_f(c);                 // bit-cast back to double
    Vec8f  e = d - (pow2_23 + bias);             // subtract magic number and bias
    return e;
} 

// extract exponent of a positive number x as a floating point number
static inline Vec4d exponent_f(Vec4d const & x) {
    const double pow2_52 = 4503599627370496.0;   // 2^52
    const double bias = 1023.0;                  // bias in exponent
    const vm_udi upow2_52 = {pow2_52};

    Vec4uq a = reinterpret_i(x);                 // bit-cast x to integer
    Vec4uq b = a >> 52;                          // shift down exponent to low bits
    Vec4uq c = b | Vec4uq(upow2_52.i);           // insert new exponent
    Vec4d  d = reinterpret_d(c);                 // bit-cast back to double
    Vec4d  e = d - (pow2_52 + bias);             // subtract magic number and bias
    return e;
}

#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512

static inline Vec16f exponent_f(Vec16f const & x) {
#if INSTRSET >= 9                                // AVX512
    return _mm512_getexp_ps(x);
#else
    return Vec16f(exponent_f(x.get_low()), exponent_f(x.get_high()));
#endif
} 

// extract exponent of a positive number x as a floating point number
static inline Vec8d exponent_f(Vec8d const & x) {
#if INSTRSET >= 9                                // AVX512
    return _mm512_getexp_pd(x);
#else
    return Vec8d(exponent_f(x.get_low()), exponent_f(x.get_high()));
#endif
}

#endif // MAX_VECTOR_SIZE >= 512


// log function, double precision
// template parameters:
// VTYPE: f.p. vector type
// BTYPE: boolean vector type
// M1: 0 for log, 1 for log1p
template<class VTYPE, class BTYPE, int M1> 
static inline VTYPE log_d(VTYPE const & initial_x) {

    // define constants
    const double ln2_hi =  0.693359375;
    const double ln2_lo = -2.121944400546905827679E-4;
    const double P0log  =  7.70838733755885391666E0;
    const double P1log  =  1.79368678507819816313E1;
    const double P2log  =  1.44989225341610930846E1;
    const double P3log  =  4.70579119878881725854E0;
    const double P4log  =  4.97494994976747001425E-1;
    const double P5log  =  1.01875663804580931796E-4;
    const double Q0log  =  2.31251620126765340583E1;
    const double Q1log  =  7.11544750618563894466E1;
    const double Q2log  =  8.29875266912776603211E1;
    const double Q3log  =  4.52279145837532221105E1;
    const double Q4log  =  1.12873587189167450590E1;

    VTYPE x1, x, x2, px, qx, res, fe;            // data vectors
    BTYPE blend, overflow, underflow;            // boolean vectors

    if (M1 == 0) {
        x1 = initial_x;                          // log(x)
    }
    else {
        x1 = initial_x + 1.0;                    // log(x+1)
    }
    // separate mantissa from exponent 
    // VTYPE x  = fraction(x1) * 0.5;
    x  = fraction_2(x1);
    fe = exponent_f(x1);

    blend = x > VM_SQRT2*0.5;
    x  = if_add(!blend, x, x);                   // conditional add
    fe = if_add(blend, fe, 1.);                  // conditional add

    if (M1 == 0) {
        // log(x). Expand around 1.0
        x -= 1.0;
    }
    else {
        // log(x+1). Avoid loss of precision when adding 1 and later subtracting 1 if exponent = 0
        x = select(fe==0., initial_x, x - 1.0);
    }

    // rational form 
    px  = polynomial_5 (x, P0log, P1log, P2log, P3log, P4log, P5log);
    x2  = x * x;
    px *= x * x2;
    qx  = polynomial_5n(x, Q0log, Q1log, Q2log, Q3log, Q4log);
    res = px / qx ;

    // add exponent
    res  = mul_add(fe, ln2_lo, res);             // res += fe * ln2_lo;
    res += nmul_add(x2, 0.5, x);                 // res += x  - 0.5 * x2;
    res  = mul_add(fe, ln2_hi, res);             // res += fe * ln2_hi;

    overflow  = !is_finite(x1);
    underflow = x1 < VM_SMALLEST_NORMAL;         // denormals not supported by this functions

    if (!horizontal_or(overflow | underflow)) {
        // normal path
        return res;
    }
    else {
        // overflow and underflow
        res = select(underflow, nan_vec<VTYPE>(NAN_LOG), res);                   // x1  < 0 gives NAN
        res = select(x1 == 0. || is_subnormal(x1), -infinite_vec<VTYPE>(), res); // x1 == 0 gives -INF
        res = select(overflow,  x1, res);                                        // INF or NAN goes through
        res = select(is_inf(x1)&sign_bit(x1), nan_vec<VTYPE>(NAN_LOG), res);     // -INF gives NAN
        return res;
    }
}


static inline Vec2d log(Vec2d const & x) {
    return log_d<Vec2d, Vec2db, 0>(x);
}

static inline Vec2d log1p(Vec2d const & x) {
    return log_d<Vec2d, Vec2db, 1>(x);
}

static inline Vec2d log2(Vec2d const & x) {
    return VM_LOG2E * log_d<Vec2d, Vec2db, 0>(x);
}

static inline Vec2d log10(Vec2d const & x) {
    return VM_LOG10E * log_d<Vec2d, Vec2db, 0>(x);
}

#if MAX_VECTOR_SIZE >= 256

static inline Vec4d log(Vec4d const & x) {
    return log_d<Vec4d, Vec4db, 0>(x);
}

static inline Vec4d log1p(Vec4d const & x) {
    return log_d<Vec4d, Vec4db, 1>(x);
}

static inline Vec4d log2(Vec4d const & x) {
    return VM_LOG2E * log_d<Vec4d, Vec4db, 0>(x);
}

static inline Vec4d log10(Vec4d const & x) {
    return VM_LOG10E * log_d<Vec4d, Vec4db, 0>(x);
}

#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512

static inline Vec8d log(Vec8d const & x) {
    return log_d<Vec8d, Vec8db, 0>(x);
}

static inline Vec8d log1p(Vec8d const & x) {
    return log_d<Vec8d, Vec8db, 1>(x);
}

static inline Vec8d log2(Vec8d const & x) {
    return VM_LOG2E * log_d<Vec8d, Vec8db, 0>(x);
}

static inline Vec8d log10(Vec8d const & x) {
    return VM_LOG10E * log_d<Vec8d, Vec8db, 0>(x);
}

#endif // MAX_VECTOR_SIZE >= 512



// log function, single precision
// template parameters:
// VTYPE: f.p. vector type
// ITYPE: integer vector type with same element size
// BTYPE: boolean vector type
// BTYPEI: boolean vector type for ITYPE
// M1: 0 for log, 1 for log1p
template<class VTYPE, class ITYPE, class BTYPE, class BTYPEI, int M1> 
static inline VTYPE log_f(VTYPE const & initial_x) {

    // define constants
    const float ln2f_hi =  0.693359375f;
    const float ln2f_lo = -2.12194440E-4f;
    const float P0logf  =  3.3333331174E-1f;
    const float P1logf  = -2.4999993993E-1f;
    const float P2logf  =  2.0000714765E-1f;
    const float P3logf  = -1.6668057665E-1f;
    const float P4logf  =  1.4249322787E-1f;
    const float P5logf  = -1.2420140846E-1f;
    const float P6logf  =  1.1676998740E-1f;
    const float P7logf  = -1.1514610310E-1f;
    const float P8logf  =  7.0376836292E-2f;

    VTYPE x1, x, res, x2, fe;                    // data vectors
    ITYPE e;                                     // integer vector
    BTYPE blend, overflow, underflow;            // boolean vectors

    if (M1 == 0) {
        x1 = initial_x;                          // log(x)
    }
    else {
        x1 = initial_x + 1.0f;                   // log(x+1)
    }

    // separate mantissa from exponent 
    x = fraction_2(x1);
    e = exponent(x1);

    blend = x > float(VM_SQRT2*0.5);
    x  = if_add(!blend, x, x);                   // conditional add
    e  = if_add(BTYPEI(blend),  e, ITYPE(1));    // conditional add
    fe = to_float(e);

    if (M1 == 0) {
        // log(x). Expand around 1.0
        x -= 1.0f;
    }
    else {
        // log(x+1). Avoid loss of precision when adding 1 and later subtracting 1 if exponent = 0
        x = select(BTYPE(e==0), initial_x, x - 1.0f);
    }

    // Taylor expansion
    res = polynomial_8(x, P0logf, P1logf, P2logf, P3logf, P4logf, P5logf, P6logf, P7logf, P8logf);
    x2  = x*x;
    res *= x2*x;

    // add exponent
    res  = mul_add(fe, ln2f_lo, res);            // res += ln2f_lo  * fe;
    res += nmul_add(x2, 0.5f, x);                // res += x - 0.5f * x2;
    res  = mul_add(fe, ln2f_hi, res);            // res += ln2f_hi  * fe;

    overflow  = !is_finite(x1);
    underflow = x1 < VM_SMALLEST_NORMALF;        // denormals not supported by this functions

    if (!horizontal_or(overflow | underflow)) {
        // normal path
        return res;
    }
    else {
        // overflow and underflow
        res = select(underflow, nan_vec<VTYPE>(NAN_LOG), res);                    // x1 < 0 gives NAN
        res = select(x1 == 0.f || is_subnormal(x1), -infinite_vec<VTYPE>(), res); // x1 == 0 or denormal gives -INF
        res = select(overflow,  x1, res);                                         // INF or NAN goes through
        res = select(is_inf(x1)&sign_bit(x1), nan_vec<VTYPE>(NAN_LOG), res);      // -INF gives NAN
        return res;
    }
}

static inline Vec4f log(Vec4f const & x) {
    return log_f<Vec4f, Vec4i, Vec4fb, Vec4ib, 0>(x);
}

static inline Vec4f log1p(Vec4f const & x) {
    return log_f<Vec4f, Vec4i, Vec4fb, Vec4ib, 1>(x);
}

static inline Vec4f log2(Vec4f const & x) {
    return float(VM_LOG2E) * log_f<Vec4f, Vec4i, Vec4fb, Vec4ib, 0>(x);
}

static inline Vec4f log10(Vec4f const & x) {
    return float(VM_LOG10E) * log_f<Vec4f, Vec4i, Vec4fb, Vec4ib, 0>(x);
}

#if MAX_VECTOR_SIZE >= 256

static inline Vec8f log(Vec8f const & x) {
    return log_f<Vec8f, Vec8i, Vec8fb, Vec8ib, 0>(x);
}

static inline Vec8f log1p(Vec8f const & x) {
    return log_f<Vec8f, Vec8i, Vec8fb, Vec8ib, 1>(x);
}

static inline Vec8f log2(Vec8f const & x) {
    return float(VM_LOG2E) * log_f<Vec8f, Vec8i, Vec8fb, Vec8ib, 0>(x);
}

static inline Vec8f log10(Vec8f const & x) {
    return float(VM_LOG10E) * log_f<Vec8f, Vec8i, Vec8fb, Vec8ib, 0>(x);
}

#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512

static inline Vec16f log(Vec16f const & x) {
    return log_f<Vec16f, Vec16i, Vec16fb, Vec16ib, 0>(x);
}

static inline Vec16f log1p(Vec16f const & x) {
    return log_f<Vec16f, Vec16i, Vec16fb, Vec16ib, 1>(x);
}

static inline Vec16f log2(Vec16f const & x) {
    return float(VM_LOG2E) * log_f<Vec16f, Vec16i, Vec16fb, Vec16ib, 0>(x);
}

static inline Vec16f log10(Vec16f const & x) {
    return float(VM_LOG10E) * log_f<Vec16f, Vec16i, Vec16fb, Vec16ib, 0>(x);
}

#endif // MAX_VECTOR_SIZE >= 512


/******************************************************************************
*           Cube root and reciprocal cube root
******************************************************************************/

// cube root template, double precision
// template parameters:
// VTYPE:  f.p. vector type
// ITYPE:  uint32_t integer vector type with same total number of bits
// ITYPE2: uint64_t integer vector type with same total number of bits
// BTYPE:  boolean vector type
// CR:     -1 for reciprocal cube root, 1 for cube root, 2 for cube root squared
template<class VTYPE, class ITYPE, class ITYPE2, class BTYPE, int CR> 
static inline VTYPE cbrt_d(VTYPE const & x) {
    const int iter = 7;     // iteration count of x^(-1/3) loop
    int i;
    VTYPE  xa, xa3, a, a2;
    ITYPE  m1, m2;
    BTYPE  underflow;
    ITYPE2 q1(0x5540000000000000ULL);            // exponent bias
    ITYPE2 q2(0x0005555500000000ULL);            // exponent multiplier for 1/3
    ITYPE2 q3(0x0010000000000000ULL);            // denormal limit
    const double one_third  = 1./3.;
    const double four_third = 4./3.;

    xa  = abs(x);
    xa3 = one_third*xa;

    // multiply exponent by -1/3
    m1 = reinterpret_i(xa);
    m2 = ITYPE(q1) - (m1 >> 20) * ITYPE(q2);
    a  = reinterpret_d(m2);
    underflow = BTYPE(ITYPE2(m1) < q3);          // true if denormal or zero

    // Newton Raphson iteration
    for (i = 0; i < iter-1; i++) {
        a2 = a * a;
        a = nmul_add(xa3, a2*a2, four_third*a);  // a = four_third*a - xa3*a2*a2;
    }
    // last iteration with better precision
    a2 = a * a;    
    a = mul_add(one_third, nmul_add(xa, a2*a2, a), a); // a = a + one_third*(a - xa*a2*a2);

    if (CR == -1) {  // reciprocal cube root
        // (note: gives wrong sign when input is INF)
        // generate INF if underflow
        a = select(underflow, infinite_vec<VTYPE>(), a);
        // get sign
        a = sign_combine(a, x);
    }
    else if (CR == 1) {     // cube root
        a = a * a * x;
        // generate 0 if underflow
        a = select(underflow, 0., a);
    }
    else if (CR == 2) {     // cube root squared
        // (note: gives wrong sign when input is INF)
        a = a * xa;
        // generate 0 if underflow
        a = select(underflow, 0., a);
    }
    return a;
}

// template instances for cbrt and reciprocal_cbrt

// cube root
static inline Vec2d cbrt(Vec2d const & x) {
    return cbrt_d<Vec2d, Vec4ui, Vec2uq, Vec2db, 1> (x);
}

// reciprocal cube root
static inline Vec2d reciprocal_cbrt(Vec2d const & x) {
    return cbrt_d<Vec2d, Vec4ui, Vec2uq, Vec2db, -1> (x);
}

// square cube root
static inline Vec2d square_cbrt(Vec2d const & x) {
    return cbrt_d<Vec2d, Vec4ui, Vec2uq, Vec2db, 2> (x);
}

#if MAX_VECTOR_SIZE >= 256

static inline Vec4d cbrt(Vec4d const & x) {
    return cbrt_d<Vec4d, Vec8ui, Vec4uq, Vec4db, 1> (x);
}

static inline Vec4d reciprocal_cbrt(Vec4d const & x) {
    return cbrt_d<Vec4d, Vec8ui, Vec4uq, Vec4db, -1> (x);
}

static inline Vec4d square_cbrt(Vec4d const & x) {
    return cbrt_d<Vec4d, Vec8ui, Vec4uq, Vec4db, 2> (x);
}

#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512

static inline Vec8d cbrt(Vec8d const & x) {
    return cbrt_d<Vec8d, Vec16ui, Vec8uq, Vec8db, 1> (x);
}

static inline Vec8d reciprocal_cbrt(Vec8d const & x) {
    return cbrt_d<Vec8d, Vec16ui, Vec8uq, Vec8db, -1> (x);
}

static inline Vec8d square_cbrt(Vec8d const & x) {
    return cbrt_d<Vec8d, Vec16ui, Vec8uq, Vec8db, 2> (x);
}

#endif // MAX_VECTOR_SIZE >= 512


// cube root template, single precision
// template parameters:
// VTYPE:  f.p. vector type
// ITYPE:  uint32_t integer vector type
// BTYPE:  boolean vector type
// CR:     -1 for reciprocal cube root, 1 for cube root, 2 for cube root squared
template<class VTYPE, class ITYPE, class BTYPE, int CR> 
static inline VTYPE cbrt_f(VTYPE const & x) {

    const int iter = 6;                          // iteration count of x^(-1/3) loop
    int i;
    VTYPE  xa, xa3, a, a2;
    ITYPE  m1, m2;
    BTYPE  underflow;
    ITYPE  q1(0x54800000U);                      // exponent bias
    ITYPE  q2(0x002AAAAAU);                      // exponent multiplier for 1/3
    ITYPE  q3(0x00800000U);                      // denormal limit
    const  float one_third  = float(1./3.);
    const  float four_third = float(4./3.);

    xa  = abs(x);
    xa3 = one_third*xa;

    // multiply exponent by -1/3
    m1 = reinterpret_i(xa);
    m2 = q1 - (m1 >> 23) * q2;
    a  = reinterpret_f(m2);

    underflow = BTYPE(m1 < q3);                  // true if denormal or zero

    // Newton Raphson iteration
    for (i = 0; i < iter-1; i++) {
        a2 = a*a;        
        a = nmul_add(xa3, a2*a2, four_third*a);  // a = four_third*a - xa3*a2*a2;
    }
    // last iteration with better precision
    a2 = a*a;    
    a = mul_add(one_third, nmul_add(xa, a2*a2, a), a); //a = a + one_third*(a - xa*a2*a2);

    if (CR == -1) {                              // reciprocal cube root
        // generate INF if underflow
        a = select(underflow, infinite_vec<VTYPE>(), a);
        // get sign
        a = sign_combine(a, x);
    }
    else if (CR == 1) {                          // cube root
        a = a * a * x;
        // generate 0 if underflow
        a = select(underflow, 0., a);
    }
    else if (CR == 2) {                          // cube root squared
        a = a * xa;
        // generate 0 if underflow
        a = select(underflow, 0., a);
    }
    return a;
}

// template instances for cbrt and reciprocal_cbrt

// cube root
static inline Vec4f cbrt(Vec4f const & x) {
    return cbrt_f<Vec4f, Vec4ui, Vec4fb, 1> (x);
}

// reciprocal cube root
static inline Vec4f reciprocal_cbrt(Vec4f const & x) {
    return cbrt_f<Vec4f, Vec4ui, Vec4fb, -1> (x);
}

// square cube root
static inline Vec4f square_cbrt(Vec4f const & x) {
    return cbrt_f<Vec4f, Vec4ui, Vec4fb, 2> (x);
}

#if MAX_VECTOR_SIZE >= 256

static inline Vec8f cbrt(Vec8f const & x) {
    return cbrt_f<Vec8f, Vec8ui, Vec8fb, 1> (x);
}

static inline Vec8f reciprocal_cbrt(Vec8f const & x) {
    return cbrt_f<Vec8f, Vec8ui, Vec8fb, -1> (x);
}

static inline Vec8f square_cbrt(Vec8f const & x) {
    return cbrt_f<Vec8f, Vec8ui, Vec8fb, 2> (x);
}

#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512

static inline Vec16f cbrt(Vec16f const & x) {
    return cbrt_f<Vec16f, Vec16ui, Vec16fb, 1> (x);
}

static inline Vec16f reciprocal_cbrt(Vec16f const & x) {
    return cbrt_f<Vec16f, Vec16ui, Vec16fb, -1> (x);
}

static inline Vec16f square_cbrt(Vec16f const & x) {
    return cbrt_f<Vec16f, Vec16ui, Vec16fb, 2> (x);
}

#endif // MAX_VECTOR_SIZE >= 512


// ****************************************************************************
//                pow template, double precision
// ****************************************************************************
// Calculate x to the power of y.

// Precision is important here because rounding errors get multiplied by y.
// The logarithm is calculated with extra precision, and the exponent is 
// calculated separately.
// The logarithm is calculated by Pad\E9 approximation with 6'th degree 
// polynomials. A 7'th degree would be preferred for best precision by high y.
// The alternative method: log(x) = z + z^3*R(z)/S(z), where z = 2(x-1)/(x+1)
// did not give better precision.

// Template parameters:
// VTYPE:  data vector type
// ITYPE:  signed integer vector type
// BTYPE:  boolean vector type
template <class VTYPE, class ITYPE, class BTYPE>
static inline VTYPE pow_template_d(VTYPE const & x0, VTYPE const & y) {

    // define constants
    const double ln2d_hi = 0.693145751953125;           // log(2) in extra precision, high bits
    const double ln2d_lo = 1.42860682030941723212E-6;   // low bits of log(2)
    const double log2e   = VM_LOG2E;                    // 1/log(2)
    const double pow2_52 = 4503599627370496.0;          // 2^52

    // coefficients for Pad\E9 polynomials
    const double P0logl =  2.0039553499201281259648E1;
    const double P1logl =  5.7112963590585538103336E1;
    const double P2logl =  6.0949667980987787057556E1;
    const double P3logl =  2.9911919328553073277375E1;
    const double P4logl =  6.5787325942061044846969E0;
    const double P5logl =  4.9854102823193375972212E-1;
    const double P6logl =  4.5270000862445199635215E-5;
    const double Q0logl =  6.0118660497603843919306E1;
    const double Q1logl =  2.1642788614495947685003E2;
    const double Q2logl =  3.0909872225312059774938E2;
    const double Q3logl =  2.2176239823732856465394E2;
    const double Q4logl =  8.3047565967967209469434E1;
    const double Q5logl =  1.5062909083469192043167E1;

    // Taylor coefficients for exp function, 1/n!
    const double p2  = 1./2.;
    const double p3  = 1./6.;
    const double p4  = 1./24.;
    const double p5  = 1./120.; 
    const double p6  = 1./720.; 
    const double p7  = 1./5040.; 
    const double p8  = 1./40320.; 
    const double p9  = 1./362880.; 
    const double p10 = 1./3628800.; 
    const double p11 = 1./39916800.; 
    const double p12 = 1./479001600.; 
    const double p13 = 1./6227020800.; 

    // data vectors
    VTYPE x, x1, x2;
    VTYPE px, qx, ef, yr, v, z, z1;
    VTYPE lg, lg1, lg2;
    VTYPE lgerr, x2err;
    VTYPE e1, e2, e3, ee;
    // integer vectors
    ITYPE ei, ej, yodd;
    // boolean vectors
    BTYPE blend, xzero, xnegative;
    BTYPE overflow, underflow, xfinite, yfinite, efinite;

    // remove sign
    x1 = abs(x0);

    // Separate mantissa from exponent 
    // This gives the mantissa * 0.5
    x  = fraction_2(x1);

    // reduce range of x = +/- sqrt(2)/2
    blend = x > VM_SQRT2*0.5;
    x  = if_add(!blend, x, x);                   // conditional add

    // Pade approximation
    // Higher precision than in log function. Still higher precision wanted
    x -= 1.0;
    x2 = x*x;
    px = polynomial_6  (x, P0logl, P1logl, P2logl, P3logl, P4logl, P5logl, P6logl);
    px *= x * x2;
    qx = polynomial_6n (x, Q0logl, Q1logl, Q2logl, Q3logl, Q4logl, Q5logl);
    lg1 = px / qx;
 
    // extract exponent
    ef = exponent_f(x1);
    ef = if_add(blend, ef, 1.);                  // conditional add

    // multiply exponent by y
    // nearest integer e1 goes into exponent of result, remainder yr is added to log
    e1 = round(ef * y);
    yr = mul_sub_x(ef, y, e1);                   // calculate remainder yr. precision very important here

    // add initial terms to Pade expansion
    lg = nmul_add(0.5, x2, x) + lg1;             // lg = (x - 0.5 * x2) + lg1;
    // calculate rounding errors in lg
    // rounding error in multiplication 0.5*x*x
    x2err = mul_sub_x(0.5*x, x, 0.5*x2);
    // rounding error in additions and subtractions
    lgerr = mul_add(0.5, x2, lg - x) - lg1;      // lgerr = ((lg - x) + 0.5 * x2) - lg1;

    // extract something for the exponent
    e2 = round(lg * y * VM_LOG2E);
    // subtract this from lg, with extra precision
    v = mul_sub_x(lg, y, e2 * ln2d_hi);
    v = nmul_add(e2, ln2d_lo, v);                // v -= e2 * ln2d_lo;

    // add remainder from ef * y
    v = mul_add(yr, VM_LN2, v);                  // v += yr * VM_LN2;

    // correct for previous rounding errors
    v = nmul_add(lgerr + x2err, y, v);           // v -= (lgerr + x2err) * y;

    // exp function

    // extract something for the exponent if possible
    x = v;
    e3 = round(x*log2e);
    // high precision multiplication not needed here because abs(e3) <= 1
    x = nmul_add(e3, VM_LN2, x);                 // x -= e3 * VM_LN2;

    z = polynomial_13m(x, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13);
    z = z + 1.0;

    // contributions to exponent
    ee = e1 + e2 + e3;
    ei = round_to_int64_limited(ee);
    // biased exponent of result:
    ej = ei + (ITYPE(reinterpret_i(z)) >> 52);
    // check exponent for overflow and underflow
    overflow  = BTYPE(ej >= 0x07FF) | (ee >  3000.);
    underflow = BTYPE(ej <= 0x0000) | (ee < -3000.);

    // add exponent by integer addition
    z = reinterpret_d(ITYPE(reinterpret_i(z)) + (ei << 52));

    // check for special cases
    xfinite   = is_finite(x0);
    yfinite   = is_finite(y);
    efinite   = is_finite(ee);
    xzero     = is_zero_or_subnormal(x0);
    xnegative = x0  < 0.;

    // check for overflow and underflow
    if (horizontal_or(overflow | underflow)) {
        // handle errors
        z = select(underflow, VTYPE(0.), z);
        z = select(overflow, infinite_vec<VTYPE>(), z);
    }

    // check for x == 0
    z = select(xzero, select(y < 0., infinite_vec<VTYPE>(), select(y == 0., VTYPE(1.), VTYPE(0.))), z);

    // check for x < 0. y must be integer
    if (horizontal_or(xnegative)) {
        // test if y odd
        yodd = ITYPE(reinterpret_i(abs(y) + pow2_52)) << 63;     // convert y to integer and shift bit 0 to position of sign bit
        z1 = z | (x0 & VTYPE(reinterpret_d(yodd)));              // apply sign if y odd
        z1 = select(y == round(y), z1, nan_vec<VTYPE>(NAN_POW)); // NAN if y not integer
        z = select(xnegative, z1, z);
    }

    // check for range errors
    if (horizontal_and(xfinite & yfinite & efinite)) {
        // fast return if no special cases
        return z;
    }
    // handle special error cases
    z = select(yfinite & efinite, z, select(x1 == 1., VTYPE(1.), select((x1 > 1.) ^ sign_bit(y), infinite_vec<VTYPE>(), 0.)));
    yodd = ITYPE(reinterpret_i(abs(y) + pow2_52)) << 63; // same as above
    z = select(xfinite, z, select(y == 0., VTYPE(1.), select(y < 0., VTYPE(0.), infinite_vec<VTYPE>() | ( VTYPE(reinterpret_d(yodd)) & x0))));
    z = select(is_nan(x0), select(is_nan(y), x0 | y, x0), select(is_nan(y), y, z));
    return z;
}; 


//This template is in vectorf128.h to prevent implicit conversion of float y to int when float version is not defined:
//template <typename TT> static Vec2d pow(Vec2d const & a, TT n);

// instantiations of pow_template_d:
template <>
inline Vec2d pow<Vec2d const &>(Vec2d const & x, Vec2d const & y) {
    return pow_template_d<Vec2d, Vec2q, Vec2db>(x, y);
}

template <>
inline Vec2d pow<double>(Vec2d const & x, double y) {
    return pow_template_d<Vec2d, Vec2q, Vec2db>(x, y);
}
template <>
inline Vec2d pow<float>(Vec2d const & x, float y) {
    return pow_template_d<Vec2d, Vec2q, Vec2db>(x, (double)y);
}

#if MAX_VECTOR_SIZE >= 256

template <>
inline Vec4d pow<Vec4d const &>(Vec4d const & x, Vec4d const & y) {
    return pow_template_d<Vec4d, Vec4q, Vec4db>(x, y);
}

template <>
inline Vec4d pow<double>(Vec4d const & x, double y) {
    return pow_template_d<Vec4d, Vec4q, Vec4db>(x, y);
}

template <>
inline Vec4d pow<float>(Vec4d const & x, float y) {
    return pow_template_d<Vec4d, Vec4q, Vec4db>(x, (double)y);
}

#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512

template <>
inline Vec8d pow<Vec8d const &>(Vec8d const & x, Vec8d const & y) {
    return pow_template_d<Vec8d, Vec8q, Vec8db>(x, y);
}

template <>
inline Vec8d pow<double>(Vec8d const & x, double y) {
    return pow_template_d<Vec8d, Vec8q, Vec8db>(x, y);
}

template <>
inline Vec8d pow<float>(Vec8d const & x, float y) {
    return pow_template_d<Vec8d, Vec8q, Vec8db>(x, (double)y);
}

#endif // MAX_VECTOR_SIZE >= 512



// ****************************************************************************
//                pow template, single precision
// ****************************************************************************

// Template parameters:
// VTYPE:  data vector type
// ITYPE:  signed integer vector type
// BTYPE:  boolean vector type
// Calculate x to the power of y
template <class VTYPE, class ITYPE, class BTYPE>
static inline VTYPE pow_template_f(VTYPE const & x0, VTYPE const & y) {

    // define constants
    const float ln2f_hi  =  0.693359375f;
    const float ln2f_lo  = -2.12194440e-4f;
    //const float max_expf =  87.3f;
    const float log2e    =  float(VM_LOG2E);     // 1/log(2)
    const float pow2_23  =  8388608.0f;          // 2^23

    const float P0logf  =  3.3333331174E-1f;
    const float P1logf  = -2.4999993993E-1f;
    const float P2logf  =  2.0000714765E-1f;
    const float P3logf  = -1.6668057665E-1f;
    const float P4logf  =  1.4249322787E-1f;
    const float P5logf  = -1.2420140846E-1f;
    const float P6logf  =  1.1676998740E-1f;
    const float P7logf  = -1.1514610310E-1f;
    const float P8logf  =  7.0376836292E-2f;

    // Taylor coefficients for exp function, 1/n!
    const float p2expf   =  1.f/2.f;
    const float p3expf   =  1.f/6.f;
    const float p4expf   =  1.f/24.f;
    const float p5expf   =  1.f/120.f; 
    const float p6expf   =  1.f/720.f; 
    const float p7expf   =  1.f/5040.f; 

    // data vectors
    VTYPE x, x1, x2;
    VTYPE ef, yr, v, z, z1;
    VTYPE lg, lg1;
    VTYPE lgerr, x2err;
    VTYPE e1, e2, e3, ee;
    // integer vectors
    ITYPE ei, ej, yodd;
    // boolean vectors
    BTYPE blend, xzero, xnegative;
    BTYPE overflow, underflow, xfinite, yfinite, efinite;

    // remove sign
    x1 = abs(x0);

    // Separate mantissa from exponent 
    // This gives the mantissa * 0.5
    x  = fraction_2(x1);

    // reduce range of x = +/- sqrt(2)/2
    blend = x > float(VM_SQRT2 * 0.5);
    x  = if_add(!blend, x, x);                   // conditional add

    // Taylor expansion, high precision
    x   -= 1.0f;
    x2   = x * x;
    lg1  = polynomial_8(x, P0logf, P1logf, P2logf, P3logf, P4logf, P5logf, P6logf, P7logf, P8logf);
    lg1 *= x2 * x; 
 
    // extract exponent
    ef = exponent_f(x1);
    ef = if_add(blend, ef, 1.0f);                // conditional add

    // multiply exponent by y
    // nearest integer e1 goes into exponent of result, remainder yr is added to log
    e1 = round(ef * y);
    yr = mul_sub_x(ef, y, e1);                   // calculate remainder yr. precision very important here

    // add initial terms to expansion
    lg = nmul_add(0.5f, x2, x) + lg1;            // lg = (x - 0.5f * x2) + lg1;

    // calculate rounding errors in lg
    // rounding error in multiplication 0.5*x*x
    x2err = mul_sub_x(0.5f*x, x, 0.5f * x2);
    // rounding error in additions and subtractions
    lgerr = mul_add(0.5f, x2, lg - x) - lg1;     // lgerr = ((lg - x) + 0.5f * x2) - lg1;

    // extract something for the exponent
    e2 = round(lg * y * float(VM_LOG2E));
    // subtract this from lg, with extra precision
    v = mul_sub_x(lg, y, e2 * ln2f_hi);
    v = nmul_add(e2, ln2f_lo, v);                // v -= e2 * ln2f_lo;

    // correct for previous rounding errors
    v -= mul_sub(lgerr + x2err, y, yr * float(VM_LN2)); // v -= (lgerr + x2err) * y - yr * float(VM_LN2) ;

    // exp function

    // extract something for the exponent if possible
    x = v;
    e3 = round(x*log2e);
    // high precision multiplication not needed here because abs(e3) <= 1
    x = nmul_add(e3, float(VM_LN2), x);          // x -= e3 * float(VM_LN2);

    // Taylor polynomial
    x2  = x  * x;
    z = polynomial_5(x, p2expf, p3expf, p4expf, p5expf, p6expf, p7expf)*x2 + x + 1.0f;

    // contributions to exponent
    ee = e1 + e2 + e3;
    ei = round_to_int(ee);
    // biased exponent of result:
    ej = ei + (ITYPE(reinterpret_i(z)) >> 23);
    // check exponent for overflow and underflow
    overflow  = BTYPE(ej >= 0x0FF) | (ee >  300.f);
    underflow = BTYPE(ej <= 0x000) | (ee < -300.f);

    // add exponent by integer addition
    z = reinterpret_f(ITYPE(reinterpret_i(z)) + (ei << 23)); // the extra 0x10000 is shifted out here

    // check for special cases
    xfinite   = is_finite(x0);
    yfinite   = is_finite(y);
    efinite   = is_finite(ee);
    xzero     = is_zero_or_subnormal(x0);
    xnegative = x0  < 0.f;

    // check for overflow and underflow
    if (horizontal_or(overflow | underflow)) {
        // handle errors
        z = select(underflow, VTYPE(0.f), z);
        z = select(overflow, infinite_vec<VTYPE>(), z);
    }

    // check for x == 0
    z = select(xzero, select(y < 0.f, infinite_vec<VTYPE>(), select(y == 0.f, VTYPE(1.), VTYPE(0.f))), z);

    // check for x < 0. y must be integer
    if (horizontal_or(xnegative)) {
        // test if y odd
        yodd = ITYPE(reinterpret_i(abs(y) + pow2_23)) << 31;     // convert y to integer and shift bit 0 to position of sign bit
        z1 = z | (x0 & VTYPE(reinterpret_f(yodd)));              // apply sign if y odd
        z1 = select(y == round(y), z1, nan_vec<VTYPE>(NAN_POW)); // NAN if y not integer
        z = select(xnegative, z1, z);
    }

    // check for range errors
    if (horizontal_and(xfinite & yfinite & efinite)) {
        // fast return if no special cases
        return z;
    }
    // handle special error cases
    z = select(yfinite & efinite, z, select(x1 == 1.f, VTYPE(1.f), select((x1 > 1.f) ^ sign_bit(y), infinite_vec<VTYPE>(), 0.f)));
    yodd = ITYPE(reinterpret_i(abs(y) + pow2_23)) << 31; // same as above
    z = select(xfinite, z, select(y == 0.f, VTYPE(1.f), select(y < 0.f, VTYPE(0.f), infinite_vec<VTYPE>() | (VTYPE(reinterpret_f(yodd)) & x0))));
    z = select(is_nan(x0), select(is_nan(y), x0 | y, x0), select(is_nan(y), y, z));
    return z;
}

//This template is in vectorf128.h to prevent implicit conversion of float y to int when float version is not defined:
//template <typename TT> static Vec4f pow(Vec4f const & a, TT n);

template <>
inline Vec4f pow<Vec4f const &>(Vec4f const & x, Vec4f const & y) {
    return pow_template_f<Vec4f, Vec4i, Vec4fb>(x, y);
}

template <>
inline Vec4f pow<float>(Vec4f const & x, float y) {
    return pow_template_f<Vec4f, Vec4i, Vec4fb>(x, y);
}

template <>
inline Vec4f pow<double>(Vec4f const & x, double y) {
    return pow_template_f<Vec4f, Vec4i, Vec4fb>(x, (float)y);
}

#if MAX_VECTOR_SIZE >= 256

template <>
inline Vec8f pow<Vec8f const &>(Vec8f const & x, Vec8f const & y) {
    return pow_template_f<Vec8f, Vec8i,  Vec8fb>(x, y);
}

template <>
inline Vec8f pow<float>(Vec8f const & x, float y) {
    return pow_template_f<Vec8f, Vec8i,  Vec8fb>(x, y);
}
template <>
inline Vec8f pow<double>(Vec8f const & x, double y) {
    return pow_template_f<Vec8f, Vec8i,  Vec8fb>(x, (float)y);
}

#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512

inline Vec16f pow(Vec16f const & x, Vec16f const & y) {
    return pow_template_f<Vec16f, Vec16i,  Vec16fb>(x, y);
}

inline Vec16f pow(Vec16f const & x, float y) {
    return pow_template_f<Vec16f, Vec16i,  Vec16fb>(x, y);
}

inline Vec16f pow(Vec16f const & x, double y) {
    return pow_template_f<Vec16f, Vec16i,  Vec16fb>(x, (float)y);
}

#endif // MAX_VECTOR_SIZE >= 512


// *************************************************************
//             power function with rational exponent
// *************************************************************
// Power function with rational exponent: x^(a/b)
// Template must be defined as class to allow partial template specialization
template <int a, int b>
class Power_rational {
public:
    // overloaded member function for each vector type
    static Vec4f pow(Vec4f const & x) {
        Vec4f y = x;
        // negative x allowed when b odd or a even
        // (if a is even then either b is odd or a/b can be reduced, 
        // but we can check a even anyway at no cost to be sure)
        if (a == 0) return 1.f;
        if ((b | ~a) & 1) y = abs(y);
        y = ::pow(y, float(double(a)/double(b)));
        if (a & b & 1) y = sign_combine(y, x);          // apply sign if a and b both odd
        if ((a ^ b) >= 0) y = select(x == 0.f, 0.f, y); // zero allowed for positive a and b
        return y;
    }
    static Vec2d pow(Vec2d const & x) {
        Vec2d y = x;
        if (a == 0) return 1.;
        if ((b | ~a) & 1) y = abs(y);
        y = ::pow(y, double((long double)a/(long double)b));
        if (a & b & 1) y = sign_combine(y, x);
        if ((a ^ b) >= 0) y = select(x == 0., 0., y);
        return y;
    }
#if MAX_VECTOR_SIZE >= 256
    static Vec8f pow(Vec8f const & x) {
        Vec8f y = x;
        if (a == 0) return 1.f;
        if ((b | ~a) & 1) y = abs(y);
        y = ::pow(y, float(double(a)/double(b)));
        if (a & b & 1) y = sign_combine(y, x);
        if ((a ^ b) >= 0) y = select(x == 0.f, 0.f, y);
        return y;
    }
    static Vec4d pow(Vec4d const & x) {
        Vec4d y = x;
        if (a == 0) return 1.;
        if ((b | ~a) & 1) y = abs(y);
        y = ::pow(y, double((long double)a/(long double)b));
        if (a & b & 1) y = sign_combine(y, x);
        if ((a ^ b) >= 0) y = select(x == 0., 0., y);
        return y;
    }
#endif // MAX_VECTOR_SIZE >= 256
#if MAX_VECTOR_SIZE >= 512
    static Vec16f pow(Vec16f const & x) {
        Vec16f y = x;
        if (a == 0) return 1.f;
        if ((b | ~a) & 1) y = abs(y);
        y = ::pow(y, float(double(a)/double(b)));
        if (a & b & 1) y = sign_combine(y, x);
        if ((a ^ b) >= 0) y = select(x == 0.f, 0.f, y);
        return y;
    }
    static Vec8d pow(Vec8d const & x) {
        Vec8d y = x;
        if (a == 0) return 1.;
        if ((b | ~a) & 1) y = abs(y);
        y = ::pow(y, double((long double)a/(long double)b));
        if (a & b & 1) y = sign_combine(y, x);
        if ((a ^ b) >= 0) y = select(x == 0., 0., y);
        return y;
    }
#endif // MAX_VECTOR_SIZE >= 512
};

// partial specialization for b = 0
template<int a>
class Power_rational<a,0> {
public:
    template<class VTYPE>
    static VTYPE pow(VTYPE const & x) {return nan_vec<VTYPE>(NAN_LOG);}
};

// partial specialization for b = 1
template<int a>
class Power_rational<a,1> {
public:
    template<class VTYPE>
    static VTYPE pow(VTYPE const & x) {return pow_n<a>(x);}
};

// partial specialization for b = 2
template<int a>
class Power_rational<a,2> {
public:
    template<class VTYPE>
    static VTYPE pow(VTYPE const & x) {
        VTYPE y = pow_n<(a > 0 ? a/2 : (a-1)/2)>(x);
        if (a & 1) y *= sqrt(x);
        return y;
    }
};

// full specialization for a = 1, b = 2
template<>
class Power_rational<1,2> {
public:
    template<class VTYPE>
    static VTYPE pow(VTYPE const & x) {        
        return sqrt(x);
    }
};

// full specialization for a = -1, b = 2
template<>
class Power_rational<-1,2> {
public:
    template<class VTYPE>
    static VTYPE pow(VTYPE const & x) {        
        // (this is faster than iteration method on modern CPUs)
        return VTYPE(1.f) / sqrt(x);
    }
};

// partial specialization for b = 3
template<int a>
class Power_rational<a,3> {
public:
    template<class VTYPE>
    static VTYPE pow(VTYPE const & x) {
        VTYPE t;
        switch (a % 3) {
        case -2:
            t = reciprocal_cbrt(x);
            t *= t;
            if (a == -2) return t;
            t = t / pow_n<(-a-2)/3>(x);
            break;
        case -1:
            t = reciprocal_cbrt(x);
            if (a == -1) return t;
            t = t / pow_n<(-a-1)/3>(x);
            break;
        case  0:
            t = pow_n<a/3>(x);
            break;
        case  1:
            t = cbrt(x);
            if (a == 1) return t;
            t = t * pow_n<a/3>(x);
            break;
        case  2:
            t = square_cbrt(x);
            if (a == 2) return t;
            t = t * pow_n<a/3>(x);
            break;
        }
        return t;
    }
};

// partial specialization for b = 4
template<int a>
class Power_rational<a,4> {
public:
    template<class VTYPE>
    static VTYPE pow(VTYPE const & x) {
        VTYPE t, s1, s2;
        s1 = sqrt(x);
        if (a & 1) s2 = sqrt(s1);
        switch (a % 4) {
        case -3:
            t = s2 / pow_n<1+(-a)/4>(x);
            break;
        case -2:
            t = s1 / pow_n<1+(-a)/4>(x);
            break;
        case -1:
            if (a != -1) s2 *= pow_n<(-a)/4>(x);
            t = VTYPE(1.f) / s2;
            break;
        case  0: default:
            t = pow_n<a/4>(x);
            break;
        case  1:
            t = s2;
            if (a != 1) t *= pow_n<a/4>(x);
            break;
        case  2:
            t = s1;
            if (a != 2) t *= pow_n<a/4>(x);
            break;
        case  3:
            t = s1 * s2;
            if (a != 3) t *= pow_n<a/4>(x);
            break;
        }
        return t;
    }
};

// partial specialization for b = 6
template<int a>
class Power_rational<a,6> {
public:
    template<class VTYPE>
    static VTYPE pow(VTYPE const & x) {
        VTYPE t, s1, s2, s3;
        switch (a % 6) {
        case -5:
            t = reciprocal_cbrt(x);
            t = t * t * sqrt(t);
            if (a != -5) t /= pow_n<(-a)/6>(x);
            break;
        case -4:
            t = reciprocal_cbrt(x);
            t *= t;
            if (a != -4) t /= pow_n<(-a)/6>(x);
            break;
        case -3:
            t = pow_n<a/6>(x);
            t /= sqrt(x);
            break;
        case -2:
            t = reciprocal_cbrt(x);
            if (a != -2) t /= pow_n<(-a)/6>(x);
            break;
        case -1:
            t = sqrt(reciprocal_cbrt(x));
            if (a != -1) t /= pow_n<(-a)/6>(x);
            break;
        case  0: default:
            t = pow_n<a/6>(x);
            break;
        case  1:
            t = sqrt(cbrt(x));
            if (a != 1) t *= pow_n<a/6>(x);
            break;
        case  2:
            t = cbrt(x);
            if (a != 2) t *= pow_n<a/6>(x);
            break;
        case  3:
            t = sqrt(x);
            if (a != 3) t *= pow_n<a/6>(x);
            break;
        case  4:
            t = square_cbrt(x);
            if (a != 4) t *= pow_n<a/6>(x);
            break;
        case  5:
            t = cbrt(x);
            t = t * t * sqrt(t);
            if (a != 5) t *= pow_n<a/6>(x);
            break;
        }
        return t;
    }
};

// partial specialization for b = 8
template<int a>
class Power_rational<a,8> {
public:
    template<class VTYPE>
    static VTYPE pow(VTYPE const & x) {
        VTYPE t, s1, s2, s3;
        s1 = sqrt(x);               // x^(1/2)
        if (a & 3) s2 = sqrt(s1);   // x^(1/4)
        if (a & 1) s3 = sqrt(s2);   // x^(1/8)
        switch (a % 8) {
        case -7:
            t = s3 / pow_n<1+(-a)/8>(x);
            break;
        case -6:
            t = s2 / pow_n<1+(-a)/8>(x);
            break;
        case -5:
            t = s3 * (s2 / pow_n<1+(-a)/8>(x));
            break;
        case -4:
            t = s1 / pow_n<1+(-a)/8>(x);
            break;
        case -3:
            t = s3 * (s1 / pow_n<1+(-a)/8>(x));
            break;
        case -2:
            if (a != -2) s2 *= pow_n<(-a)/8>(x);
            t = VTYPE(1.f) / s2;
            break;
        case -1:
            if (a != -1) s3 *= pow_n<(-a)/8>(x);
            t = VTYPE(1.f) / s3;
            break;
        case  0: default:
            t = pow_n<a/8>(x);
            break;
        case  1:
            t = s3;
            if (a != 1) t *= pow_n<a/8>(x);
            break;
        case  2:
            t = s2;
            if (a != 2) t *= pow_n<a/8>(x);
            break;
        case  3:
            t = s2 * s3;
            if (a != 3) t *= pow_n<a/8>(x);
            break;
        case  4:
            t = s1;
            if (a != 4) t *= pow_n<a/8>(x);
            break;
        case  5:
            t = s1 * s3;
            if (a != 5) t *= pow_n<a/8>(x);
            break;
        case  6:
            t = s1 * s2;
            if (a != 6) t *= pow_n<a/8>(x);
            break;
        case  7:
            t = s2 * s3;
            if (a != 7) s1 *= pow_n<a/8>(x);
            t *= s1;
            break;

        }
        return t;
    }
};

// macro to call template class member function pow
#define pow_ratio(x, a, b) (Power_rational<(b)<0 ? -(a):(a), (b)<0 ? -(b):(b)> ().pow(x))


/******************************************************************************
*                 Detect NAN codes
*
* These functions return the code hidden in a NAN. The sign bit is ignored
******************************************************************************/

Vec4i nan_code(Vec4f const & x) {
    Vec4i  a = reinterpret_i(x);
    Vec4ib b = (a & 0x7F800000) == 0x7F800000;   // check if NAN/INF
    return a & 0x007FFFFF & Vec4i(b);            // isolate NAN code bits
}

// This function returns the code hidden in a NAN. The sign bit is ignored
Vec2q nan_code(Vec2d const & x) {
    Vec2q  a = reinterpret_i(x);
    Vec2q const m = 0x7FF0000000000000;
    Vec2q const n = 0x000FFFFFFFFFFFFF;
    Vec2qb b = (a & m) == m;                     // check if NAN/INF
    return a & n & Vec2q(b);                     // isolate NAN code bits
}

#if MAX_VECTOR_SIZE >= 256

// This function returns the code hidden in a NAN. The sign bit is ignored
Vec8i nan_code(Vec8f const & x) {
    Vec8i  a = reinterpret_i(x);
    Vec8ib b = (a & 0x7F800000) == 0x7F800000;   // check if NAN/INF
    return a & 0x007FFFFF & Vec8i(b);            // isolate NAN code bits
}

// This function returns the code hidden in a NAN. The sign bit is ignored
Vec4q nan_code(Vec4d const & x) {
    Vec4q  a = reinterpret_i(x);
    Vec4q const m = 0x7FF0000000000000;
    Vec4q const n = 0x000FFFFFFFFFFFFF;
    Vec4qb b = (a & m) == m;                     // check if NAN/INF
    return a & n & Vec4q(b);                     // isolate NAN code bits
}

#endif // MAX_VECTOR_SIZE >= 256 
#if MAX_VECTOR_SIZE >= 512

// This function returns the code hidden in a NAN. The sign bit is ignored
Vec16i nan_code(Vec16f const & x) {
    Vec16i  a = Vec16i(reinterpret_i(x));
    Vec16ib b = (a & 0x7F800000) == 0x7F800000;  // check if NAN/INF
    return a & 0x007FFFFF & Vec16i(b);           // isolate NAN code bits
}

// This function returns the code hidden in a NAN. The sign bit is ignored
Vec8q nan_code(Vec8d const & x) {
    Vec8q  a = Vec8q(reinterpret_i(x));
    Vec8q const m = 0x7FF0000000000000;
    Vec8q const n = 0x000FFFFFFFFFFFFF;
    Vec8qb b = (a & m) == m;                     // check if NAN/INF
    return a & n & Vec8q(b);                     // isolate NAN code bits
}

#endif // MAX_VECTOR_SIZE >= 512

#endif  // VECTORMATH_EXP_H
