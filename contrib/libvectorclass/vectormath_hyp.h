/****************************  vectormath_hyp.h   ******************************
* Author:        Agner Fog
* Date created:  2014-07-09
* Last modified: 2014-10-16
* Version:       1.16
* Project:       vector classes
* Description:
* Header file containing inline vector functions of hyperbolic and inverse 
* hyperbolic functions:
* sinh        hyperbolic sine
* cosh        hyperbolic cosine
* tanh        hyperbolic tangent
* asinh       inverse hyperbolic sine
* acosh       inverse hyperbolic cosine
* atanh       inverse hyperbolic tangent
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

#ifndef VECTORMATH_HYP_H
#define VECTORMATH_HYP_H  1 

#include "vectormath_exp.h"  


/******************************************************************************
*                 Hyperbolic functions
******************************************************************************/

// Template for sinh function, double precision
// This function does not produce denormals
// Template parameters:
// VTYPE: double vector type
// BTYPE: boolean vector type 
template<class VTYPE, class BTYPE> 
static inline VTYPE sinh_d(VTYPE const & x0) {    
// The limit of abs(x) is 709.7, as defined by max_x in vectormath_exp.h for 0.5*exp(x).

    // Coefficients
    const double p0 = -3.51754964808151394800E5;
    const double p1 = -1.15614435765005216044E4;
    const double p2 = -1.63725857525983828727E2;
    const double p3 = -7.89474443963537015605E-1; 

    const double q0 = -2.11052978884890840399E6;
    const double q1 =  3.61578279834431989373E4;
    const double q2 = -2.77711081420602794433E2;
    const double q3 =  1.0; 

    // data vectors
    VTYPE x, x2, y1, y2;
    BTYPE x_small;                               // boolean vector

    x = abs(x0);
    x_small = x <= 1.0;                          // use Pade approximation if abs(x) <= 1

    if (horizontal_or(x_small)) {
        // At least one element needs small method
        x2 = x*x;
        y1 = polynomial_3(x2, p0, p1, p2, p3) / polynomial_3(x2, q0, q1, q2, q3);
        y1 = mul_add(y1, x*x2, x);               // y1 = x + x2*(x*y1);
    }
    if (!horizontal_and(x_small)) {
        // At least one element needs big method
        y2 =  exp_d<VTYPE, BTYPE, 0, 1>(x);      //   0.5 * exp(x)
        y2 -= 0.25 / y2;                         // - 0.5 * exp(-x)
    }
    y1 = select(x_small, y1, y2);                // choose method
    y1 = sign_combine(y1, x0);                   // get original sign

    return y1;
}

// instances of sinh_d template
static inline Vec2d sinh(Vec2d const & x) {
    return sinh_d<Vec2d, Vec2db>(x);
}

#if MAX_VECTOR_SIZE >= 256
static inline Vec4d sinh(Vec4d const & x) {
    return sinh_d<Vec4d, Vec4db>(x);
}
#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512
static inline Vec8d sinh(Vec8d const & x) {
    return sinh_d<Vec8d, Vec8db>(x);
}
#endif // MAX_VECTOR_SIZE >= 512


// Template for sinh function, single precision
// This function does not produce denormals
// Template parameters:
// VTYPE: double vector type
// BTYPE: boolean vector type 
template<class VTYPE, class BTYPE> 
static inline VTYPE sinh_f(VTYPE const & x0) {    
// The limit of abs(x) is 89.0, as defined by max_x in vectormath_exp.h for 0.5*exp(x).

    // Coefficients
    const float r0 = 1.66667160211E-1f;
    const float r1 = 8.33028376239E-3f;
    const float r2 = 2.03721912945E-4f;

    // data vectors
    VTYPE x, x2, y1, y2;
    BTYPE x_small;                               // boolean vector

    x = abs(x0);
    x_small = x <= 1.0f;                         // use polynomial approximation if abs(x) <= 1

    if (horizontal_or(x_small)) {
        // At least one element needs small method
        x2 = x*x;
        y1 = polynomial_2(x2, r0, r1, r2);
        y1 = mul_add(y1, x2*x, x);               // y1 = x + x2*(x*y1);
    }
    if (!horizontal_and(x_small)) {
        // At least one element needs big method
        y2 =  exp_f<VTYPE, BTYPE, 0, 1>(x);      //   0.5 * exp(x)
        y2 -= 0.25f / y2;                        // - 0.5 * exp(-x)
    }
    y1 = select(x_small, y1, y2);                // choose method
    y1 = sign_combine(y1, x0);                   // get original sign

    return y1;
}

// instances of sinh_f template
static inline Vec4f sinh(Vec4f const & x) {
    return sinh_f<Vec4f, Vec4fb>(x);
}

#if MAX_VECTOR_SIZE >= 256
static inline Vec8f sinh(Vec8f const & x) {
    return sinh_f<Vec8f, Vec8fb>(x);
}
#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512
static inline Vec16f sinh(Vec16f const & x) {
    return sinh_f<Vec16f, Vec16fb>(x);
}
#endif // MAX_VECTOR_SIZE >= 512


// Template for cosh function, double precision
// This function does not produce denormals
// Template parameters:
// VTYPE: double vector type
// BTYPE: boolean vector type 
template<class VTYPE, class BTYPE> 
static inline VTYPE cosh_d(VTYPE const & x0) {    
// The limit of abs(x) is 709.7, as defined by max_x in vectormath_exp.h for 0.5*exp(x).

    // data vectors
    VTYPE x, y;

    x  = abs(x0);
    y  = exp_d<VTYPE, BTYPE, 0, 1>(x);           //   0.5 * exp(x)
    y += 0.25 / y;                               // + 0.5 * exp(-x)
    return y;
}

// instances of sinh_d template
static inline Vec2d cosh(Vec2d const & x) {
    return cosh_d<Vec2d, Vec2db>(x);
}

#if MAX_VECTOR_SIZE >= 256
static inline Vec4d cosh(Vec4d const & x) {
    return cosh_d<Vec4d, Vec4db>(x);
}
#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512
static inline Vec8d cosh(Vec8d const & x) {
    return cosh_d<Vec8d, Vec8db>(x);
}
#endif // MAX_VECTOR_SIZE >= 512


// Template for cosh function, single precision
// This function does not produce denormals
// Template parameters:
// VTYPE: double vector type
// BTYPE: boolean vector type 
template<class VTYPE, class BTYPE> 
static inline VTYPE cosh_f(VTYPE const & x0) {    
// The limit of abs(x) is 89.0, as defined by max_x in vectormath_exp.h for 0.5*exp(x).

    // data vectors
    VTYPE x, y;

    x  = abs(x0);
    y  = exp_f<VTYPE, BTYPE, 0, 1>(x);           //   0.5 * exp(x)
    y += 0.25f / y;                              // + 0.5 * exp(-x)
    return y;
}

// instances of sinh_d template
static inline Vec4f cosh(Vec4f const & x) {
    return cosh_f<Vec4f, Vec4fb>(x);
}

#if MAX_VECTOR_SIZE >= 256
static inline Vec8f cosh(Vec8f const & x) {
    return cosh_f<Vec8f, Vec8fb>(x);
}
#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512
static inline Vec16f cosh(Vec16f const & x) {
    return cosh_f<Vec16f, Vec16fb>(x);
}
#endif // MAX_VECTOR_SIZE >= 512


// Template for tanh function, double precision
// This function does not produce denormals
// Template parameters:
// VTYPE: double vector type
// BTYPE: boolean vector type 
template<class VTYPE, class BTYPE> 
static inline VTYPE tanh_d(VTYPE const & x0) {    

    // Coefficients
    const double p0 = -1.61468768441708447952E3;
    const double p1 = -9.92877231001918586564E1;
    const double p2 = -9.64399179425052238628E-1;
    
    const double q0 =  4.84406305325125486048E3;
    const double q1 =  2.23548839060100448583E3;
    const double q2 =  1.12811678491632931402E2;
    const double q3 =  1.0; 

    // data vectors
    VTYPE x, x2, y1, y2;
    BTYPE x_small, x_big;                        // boolean vectors

    x = abs(x0);
    x_small = x <= 0.625;                        // use Pade approximation if abs(x) <= 5/8

    if (horizontal_or(x_small)) {
        // At least one element needs small method
        x2 = x*x;
        y1 = polynomial_2(x2, p0, p1, p2) / polynomial_3(x2, q0, q1, q2, q3);
        y1 = mul_add(y1, x2*x, x);               // y1 = x + x2*(x*y1);
    }
    if (!horizontal_and(x_small)) {
        // At least one element needs big method
        y2 = exp(x+x);                           // exp(2*x)
        y2 = 1.0 - 2.0 / (y2 + 1.0);             // tanh(x)
    }
    x_big = x > 350.;
    y1 = select(x_small, y1, y2);                // choose method
    y1 = select(x_big,  1.0, y1);                // avoid overflow
    y1 = sign_combine(y1, x0);                   // get original sign

    return y1;
}

// instances of tanh_d template
static inline Vec2d tanh(Vec2d const & x) {
    return tanh_d<Vec2d, Vec2db>(x);
}

#if MAX_VECTOR_SIZE >= 256
static inline Vec4d tanh(Vec4d const & x) {
    return tanh_d<Vec4d, Vec4db>(x);
}
#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512
static inline Vec8d tanh(Vec8d const & x) {
    return tanh_d<Vec8d, Vec8db>(x);
}
#endif // MAX_VECTOR_SIZE >= 512


// Template for tanh function, single precision
// This function does not produce denormals
// Template parameters:
// VTYPE: double vector type
// BTYPE: boolean vector type 
template<class VTYPE, class BTYPE> 
static inline VTYPE tanh_f(VTYPE const & x0) {    
// The limit of abs(x) is 89.0, as defined by max_x in vectormath_exp.h for 0.5*exp(x).

    // Coefficients
    const float r0 = -3.33332819422E-1f;
    const float r1 =  1.33314422036E-1f;
    const float r2 = -5.37397155531E-2f;
    const float r3 =  2.06390887954E-2f;
    const float r4 = -5.70498872745E-3f;

    // data vectors
    VTYPE x, x2, y1, y2;
    BTYPE x_small, x_big;                        // boolean vectors

    x = abs(x0);
    x_small = x <= 0.625f;                       // use polynomial approximation if abs(x) <= 5/8

    if (horizontal_or(x_small)) {
        // At least one element needs small method
        x2 = x*x;
        y1 = polynomial_4(x2, r0, r1, r2, r3, r4);
        y1 = mul_add(y1, x2*x, x);               // y1 = x + (x2*x)*y1;
    }
    if (!horizontal_and(x_small)) {
        // At least one element needs big method
        y2 = exp(x+x);                           // exp(2*x)
        y2 = 1.0f - 2.0f / (y2 + 1.0f);          // tanh(x)
    }
    x_big = x > 44.4f;
    y1 = select(x_small, y1, y2);                // choose method
    y1 = select(x_big,  1.0f, y1);               // avoid overflow
    y1 = sign_combine(y1, x0);                   // get original sign

    return y1;
}

// instances of tanh_f template
static inline Vec4f tanh(Vec4f const & x) {
    return tanh_f<Vec4f, Vec4fb>(x);
}

#if MAX_VECTOR_SIZE >= 256
static inline Vec8f tanh(Vec8f const & x) {
    return tanh_f<Vec8f, Vec8fb>(x);
}
#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512
static inline Vec16f tanh(Vec16f const & x) {
    return tanh_f<Vec16f, Vec16fb>(x);
}
#endif // MAX_VECTOR_SIZE >= 512



/******************************************************************************
*                 Inverse hyperbolic functions
******************************************************************************/

// Template for asinh function, double precision
// This function does not produce denormals
// Template parameters:
// VTYPE: double vector type
// BTYPE: boolean vector type 
template<class VTYPE, class BTYPE> 
static inline VTYPE asinh_d(VTYPE const & x0) {    

    // Coefficients
    const double p0 = -5.56682227230859640450E0;
    const double p1 = -9.09030533308377316566E0;
    const double p2 = -4.37390226194356683570E0;
    const double p3 = -5.91750212056387121207E-1; 
    const double p4 = -4.33231683752342103572E-3;

    const double q0 =  3.34009336338516356383E1;
    const double q1 =  6.95722521337257608734E1;
    const double q2 =  4.86042483805291788324E1;
    const double q3 =  1.28757002067426453537E1;
    const double q4 =  1.0;

    // data vectors
    VTYPE x, x2, y1, y2;
    BTYPE x_small, x_huge;                       // boolean vectors

    x2 = x0 * x0;
    x  = abs(x0);
    x_small = x <= 0.533;                        // use Pade approximation if abs(x) <= 0.5
                                                 // both methods give the highest error close to 0.5. this limit is adjusted for minimum error
    x_huge  = x > 1.E20;                         // simple approximation, avoid overflow

    if (horizontal_or(x_small)) {
        // At least one element needs small method
        y1 = polynomial_4(x2, p0, p1, p2, p3, p4) / polynomial_4(x2, q0, q1, q2, q3, q4);
        y1 = mul_add(y1, x2*x, x);               // y1 = x + (x2*x)*y1;
    }
    if (!horizontal_and(x_small)) {
        // At least one element needs big method
        y2 = log(x + sqrt(x2 + 1.0));
        if (horizontal_or(x_huge)) {
            // At least one element needs huge method to avoid overflow
            y2 = select(x_huge, log(x) + VM_LN2, y2);
        }
    }
    y1 = select(x_small, y1, y2);                // choose method
    y1 = sign_combine(y1, x0);                   // get original sign

    return y1;
}

// instances of asinh_d template
static inline Vec2d asinh(Vec2d const & x) {
    return asinh_d<Vec2d, Vec2db>(x);
}

#if MAX_VECTOR_SIZE >= 256
static inline Vec4d asinh(Vec4d const & x) {
    return asinh_d<Vec4d, Vec4db>(x);
}
#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512
static inline Vec8d asinh(Vec8d const & x) {
    return asinh_d<Vec8d, Vec8db>(x);
}
#endif // MAX_VECTOR_SIZE >= 512


// Template for asinh function, single precision
// This function does not produce denormals
// Template parameters:
// VTYPE: double vector type
// BTYPE: boolean vector type 
template<class VTYPE, class BTYPE> 
static inline VTYPE asinh_f(VTYPE const & x0) {    

    // Coefficients
    const float r0 = -1.6666288134E-1f;
    const float r1 =  7.4847586088E-2f;
    const float r2 = -4.2699340972E-2f;
    const float r3 =  2.0122003309E-2f;

    // data vectors
    VTYPE x, x2, y1, y2;
    BTYPE x_small, x_huge;                       // boolean vectors

    x2 = x0 * x0;
    x  = abs(x0);
    x_small = x <= 0.51f;                        // use polynomial approximation if abs(x) <= 0.5
    x_huge  = x > 1.E10f;                        // simple approximation, avoid overflow

    if (horizontal_or(x_small)) {
        // At least one element needs small method
        y1 = polynomial_3(x2, r0, r1, r2, r3);
        y1 = mul_add(y1, x2*x, x);               // y1 = x + (x2*x)*y1;
    }
    if (!horizontal_and(x_small)) {
        // At least one element needs big method
        y2 = log(x + sqrt(x2 + 1.0f));
        if (horizontal_or(x_huge)) {
            // At least one element needs huge method to avoid overflow
            y2 = select(x_huge, log(x) + (float)VM_LN2, y2);
        }
    }
    y1 = select(x_small, y1, y2);                // choose method
    y1 = sign_combine(y1, x0);                   // get original sign

    return y1;
}

// instances of asinh_f template
static inline Vec4f asinh(Vec4f const & x) {
    return asinh_f<Vec4f, Vec4fb>(x);
}

#if MAX_VECTOR_SIZE >= 256
static inline Vec8f asinh(Vec8f const & x) {
    return asinh_f<Vec8f, Vec8fb>(x);
}
#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512
static inline Vec16f asinh(Vec16f const & x) {
    return asinh_f<Vec16f, Vec16fb>(x);
}
#endif // MAX_VECTOR_SIZE >= 512


// Template for acosh function, double precision
// This function does not produce denormals
// Template parameters:
// VTYPE: double vector type
// BTYPE: boolean vector type 
template<class VTYPE, class BTYPE> 
static inline VTYPE acosh_d(VTYPE const & x0) {    

    // Coefficients
    const double p0 = 1.10855947270161294369E5;
    const double p1 = 1.08102874834699867335E5;
    const double p2 = 3.43989375926195455866E4;
    const double p3 = 3.94726656571334401102E3; 
    const double p4 = 1.18801130533544501356E2;

    const double q0 = 7.83869920495893927727E4;
    const double q1 = 8.29725251988426222434E4;
    const double q2 = 2.97683430363289370382E4;
    const double q3 = 4.15352677227719831579E3;
    const double q4 = 1.86145380837903397292E2;
    const double q5 = 1.0;

    // data vectors
    VTYPE x1, y1, y2;
    BTYPE x_small, x_huge, undef;                // boolean vectors

    x1      = x0 - 1.0;
    undef   = x0 < 1.0;                          // result is NAN
    x_small = x1 < 0.49;                         // use Pade approximation if abs(x-1) < 0.5
    x_huge  = x1 > 1.E20;                        // simple approximation, avoid overflow

    if (horizontal_or(x_small)) {
        // At least one element needs small method
        y1 = sqrt(x1) * (polynomial_4(x1, p0, p1, p2, p3, p4) / polynomial_5(x1, q0, q1, q2, q3, q4, q5));
        // x < 1 generates NAN
        y1 = select(undef, nan_vec<VTYPE>(NAN_HYP), y1);
    }
    if (!horizontal_and(x_small)) {
        // At least one element needs big method
        y2 = log(x0 + sqrt(mul_sub(x0,x0,1.0)));
        if (horizontal_or(x_huge)) {
            // At least one element needs huge method to avoid overflow
            y2 = select(x_huge, log(x0) + VM_LN2, y2);
        }
    }
    y1 = select(x_small, y1, y2);                // choose method
    return y1;
}

// instances of acosh_d template
static inline Vec2d acosh(Vec2d const & x) {
    return acosh_d<Vec2d, Vec2db>(x);
}

#if MAX_VECTOR_SIZE >= 256
static inline Vec4d acosh(Vec4d const & x) {
    return acosh_d<Vec4d, Vec4db>(x);
}
#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512
static inline Vec8d acosh(Vec8d const & x) {
    return acosh_d<Vec8d, Vec8db>(x);
}
#endif // MAX_VECTOR_SIZE >= 512


// Template for acosh function, single precision
// This function does not produce denormals
// Template parameters:
// VTYPE: double vector type
// BTYPE: boolean vector type 
template<class VTYPE, class BTYPE> 
static inline VTYPE acosh_f(VTYPE const & x0) {    

    // Coefficients
    const float r0 =  1.4142135263E0f;
    const float r1 = -1.1784741703E-1f;
    const float r2 =  2.6454905019E-2f;
    const float r3 = -7.5272886713E-3f;
    const float r4 =  1.7596881071E-3f;

    // data vectors
    VTYPE x1, y1, y2;
    BTYPE x_small, x_huge, undef;                // boolean vectors

    x1      = x0 - 1.0f;
    undef   = x0 < 1.0f;                         // result is NAN
    x_small = x1 < 0.49f;                        // use Pade approximation if abs(x-1) < 0.5
    x_huge  = x1 > 1.E10f;                       // simple approximation, avoid overflow

    if (horizontal_or(x_small)) {
        // At least one element needs small method
        y1 = sqrt(x1) * polynomial_4(x1, r0, r1, r2, r3, r4);
        // x < 1 generates NAN
        y1 = select(undef, nan_vec<VTYPE>(NAN_HYP), y1);
    }
    if (!horizontal_and(x_small)) {
        // At least one element needs big method
        y2 = log(x0 + sqrt(mul_sub(x0,x0,1.0)));
        if (horizontal_or(x_huge)) {
            // At least one element needs huge method to avoid overflow
            y2 = select(x_huge, log(x0) + (float)VM_LN2, y2);
        }
    }
    y1 = select(x_small, y1, y2);                // choose method
    return y1;
}

// instances of acosh_f template
static inline Vec4f acosh(Vec4f const & x) {
    return acosh_f<Vec4f, Vec4fb>(x);
}

#if MAX_VECTOR_SIZE >= 256
static inline Vec8f acosh(Vec8f const & x) {
    return acosh_f<Vec8f, Vec8fb>(x);
}
#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512
static inline Vec16f acosh(Vec16f const & x) {
    return acosh_f<Vec16f, Vec16fb>(x);
}
#endif // MAX_VECTOR_SIZE >= 512


// Template for atanh function, double precision
// This function does not produce denormals
// Template parameters:
// VTYPE: double vector type
// BTYPE: boolean vector type 
template<class VTYPE, class BTYPE> 
static inline VTYPE atanh_d(VTYPE const & x0) {    

    // Coefficients
    const double p0 = -3.09092539379866942570E1;
    const double p1 =  6.54566728676544377376E1;
    const double p2 = -4.61252884198732692637E1;
    const double p3 =  1.20426861384072379242E1;
    const double p4 = -8.54074331929669305196E-1;

    const double q0 = -9.27277618139601130017E1;
    const double q1 =  2.52006675691344555838E2;
    const double q2 = -2.49839401325893582852E2;
    const double q3 =  1.08938092147140262656E2;
    const double q4 = -1.95638849376911654834E1;
    const double q5 =  1.0;

    // data vectors
    VTYPE x, x2, y1, y2, y3;
    BTYPE x_small;                               // boolean vector

    x  = abs(x0);
    x_small = x < 0.5;                           // use Pade approximation if abs(x) < 0.5

    if (horizontal_or(x_small)) {
        // At least one element needs small method
        x2 = x * x;
        y1 = polynomial_4(x2, p0, p1, p2, p3, p4) / polynomial_5(x2, q0, q1, q2, q3, q4, q5);
        y1 = mul_add(y1, x2*x, x);
    }
    if (!horizontal_and(x_small)) {
        // At least one element needs big method
        y2 = log((1.0+x)/(1.0-x)) * 0.5;
        // check if out of range
        y3 = select(x == 1.0, infinite_vec<VTYPE>(), nan_vec<VTYPE>(NAN_HYP));
        y2 = select(x >= 1.0, y3, y2);
    }
    y1 = select(x_small, y1, y2);                // choose method
    y1 = sign_combine(y1, x0);                   // get original sign

    return y1;
}

// instances of atanh_d template
static inline Vec2d atanh(Vec2d const & x) {
    return atanh_d<Vec2d, Vec2db>(x);
}

#if MAX_VECTOR_SIZE >= 256
static inline Vec4d atanh(Vec4d const & x) {
    return atanh_d<Vec4d, Vec4db>(x);
}
#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512
static inline Vec8d atanh(Vec8d const & x) {
    return atanh_d<Vec8d, Vec8db>(x);
}
#endif // MAX_VECTOR_SIZE >= 512


// Template for atanh function, single precision
// This function does not produce denormals
// Template parameters:
// VTYPE: double vector type
// BTYPE: boolean vector type 
template<class VTYPE, class BTYPE> 
static inline VTYPE atanh_f(VTYPE const & x0) {    

    // Coefficients
    const float r0 = 3.33337300303E-1f;
    const float r1 = 1.99782164500E-1f;
    const float r2 = 1.46691431730E-1f;
    const float r3 = 8.24370301058E-2f;
    const float r4 = 1.81740078349E-1f;

    // data vectors
    VTYPE x, x2, y1, y2, y3;
    BTYPE x_small;                               // boolean vector

    x  = abs(x0);
    x_small = x < 0.5f;                          // use polynomial approximation if abs(x) < 0.5

    if (horizontal_or(x_small)) {
        // At least one element needs small method
        x2 = x * x;
        y1 = polynomial_4(x2, r0, r1, r2, r3, r4);
        y1 = mul_add(y1, x2*x, x);
    }
    if (!horizontal_and(x_small)) {
        // At least one element needs big method
        y2 = log((1.0f+x)/(1.0f-x)) * 0.5f;
        // check if out of range
        y3 = select(x == 1.0f, infinite_vec<VTYPE>(), nan_vec<VTYPE>(NAN_HYP));
        y2 = select(x >= 1.0f, y3, y2);
    }
    y1 = select(x_small, y1, y2);                // choose method
    y1 = sign_combine(y1, x0);                   // get original sign

    return y1;
}

// instances of atanh_f template
static inline Vec4f atanh(Vec4f const & x) {
    return atanh_f<Vec4f, Vec4fb>(x);
}

#if MAX_VECTOR_SIZE >= 256
static inline Vec8f atanh(Vec8f const & x) {
    return atanh_f<Vec8f, Vec8fb>(x);
}
#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512
static inline Vec16f atanh(Vec16f const & x) {
    return atanh_f<Vec16f, Vec16fb>(x);
}
#endif // MAX_VECTOR_SIZE >= 512

#endif
