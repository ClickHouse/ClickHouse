/****************************  vectorf512.h   *******************************
* Author:        Agner Fog
* Date created:  2014-07-23
* Last modified: 2014-10-22
* Version:       1.16
* Project:       vector classes
* Description:
* Header file defining floating point vector classes as interface to intrinsic 
* functions in x86 microprocessors with AVX512 and later instruction sets.
*
* Instructions:
* Use Gnu, Intel or Microsoft C++ compiler. Compile for the desired 
* instruction set, which must be at least AVX512F. 
*
* The following vector classes are defined here:
* Vec16f    Vector of  16  single precision floating point numbers
* Vec16fb   Vector of  16  Booleans for use with Vec16f
* Vec8d     Vector of   8  double precision floating point numbers
* Vec8db    Vector of   8  Booleans for use with Vec8d
*
* Each vector object is represented internally in the CPU as a 512-bit register.
* This header file defines operators and functions for these vectors.
*
* For detailed instructions, see VectorClass.pdf
*
* (c) Copyright 2014 GNU General Public License http://www.gnu.org/licenses
*****************************************************************************/

// check combination of header files
#if defined (VECTORF512_H)
#if    VECTORF512_H != 2
#error Two different versions of vectorf512.h included
#endif
#else
#define VECTORF512_H  2

#include "vectori512.h"

// Define missing intrinsic functions
#if defined (GCC_VERSION) && GCC_VERSION < 41102 && !defined(__INTEL_COMPILER) && !defined(__clang__)

static inline __m512 _mm512_castpd_ps(__m512d x) {
    union {
        __m512d a;
        __m512  b;
    } u;
    u.a = x;
    return u.b;
}

static inline __m512d _mm512_castps_pd(__m512 x) {
    union {
        __m512  a;
        __m512d b;
    } u;
    u.a = x;
    return u.b;
}


static inline __m512i _mm512_castps_si512(__m512 x) {
    union {
        __m512  a;
        __m512i b;
    } u;
    u.a = x;
    return u.b;
}

static inline __m512 _mm512_castsi512_ps(__m512i x) {
    union {
        __m512i a;
        __m512  b;
    } u;
    u.a = x;
    return u.b;
}

static inline __m512i _mm512_castpd_si512(__m512d x) {
    union {
        __m512d a;
        __m512i b;
    } u;
    u.a = x;
    return u.b;
}

static inline __m512d _mm512_castsi512_pd(__m512i x) {
    union {
        __m512i a;
        __m512d b;
    } u;
    u.a = x;
    return u.b;
}

static inline __m512 _mm512_castps256_ps512(__m256 x) {
    union {
        __m256 a;
        __m512 b;
    } u;
    u.a = x;
    return u.b;
}

static inline __m256 _mm512_castps512_ps256(__m512 x) {
    union {
        __m512 a;
        __m256 b;
    } u;
    u.a = x;
    return u.b;
}

static inline __m512d _mm512_castpd256_pd512(__m256d x) {
    union {
        __m256d a;
        __m512d b;
    } u;
    u.a = x;
    return u.b;
}

static inline __m256d _mm512_castpd512_pd256(__m512d x) {
    union {
        __m512d a;
        __m256d b;
    } u;
    u.a = x;
    return u.b;
}

#endif 


/*****************************************************************************
*
*          Vec16fb: Vector of 16 Booleans for use with Vec16f
*
*****************************************************************************/
class Vec16fb : public Vec16b {
public:
    // Default constructor:
    Vec16fb () {
    }
    Vec16fb (Vec16b x) {
        m16 = x;
    }
    // Constructor to build from all elements:
    Vec16fb(bool x0, bool x1, bool x2, bool x3, bool x4, bool x5, bool x6, bool x7,
        bool x8, bool x9, bool x10, bool x11, bool x12, bool x13, bool x14, bool x15) :
        Vec16b(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15) {
    }
    // Constructor to convert from type __mmask16 used in intrinsics:
    Vec16fb (__mmask16 x) {
        m16 = x;
    }
    // Constructor to broadcast single value:
    Vec16fb(bool b) : Vec16b(b) {}
private: // Prevent constructing from int, etc.
    Vec16fb(int b);
public:
    // Constructor to make from two halves
    Vec16fb (Vec8fb const & x0, Vec8fb const & x1) {
        m16 = Vec16b(Vec8ib(x0), Vec8ib(x1));
    }
    // Assignment operator to convert from type __mmask16 used in intrinsics:
    Vec16fb & operator = (__mmask16 x) {
        m16 = x;
        return *this;
    }
    // Assignment operator to broadcast scalar value:
    Vec16fb & operator = (bool b) {
        m16 = Vec16b(b);
        return *this;
    }
private: // Prevent assigning int because of ambiguity
    Vec16fb & operator = (int x);
public:
};

// Define operators for Vec16fb

// vector operator & : bitwise and
static inline Vec16fb operator & (Vec16fb a, Vec16fb b) {
    return Vec16b(a) & Vec16b(b);
}
static inline Vec16fb operator && (Vec16fb a, Vec16fb b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec16fb operator | (Vec16fb a, Vec16fb b) {
    return Vec16b(a) | Vec16b(b);
}
static inline Vec16fb operator || (Vec16fb a, Vec16fb b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec16fb operator ^ (Vec16fb a, Vec16fb b) {
    return Vec16b(a) ^ Vec16b(b);
}

// vector operator ~ : bitwise not
static inline Vec16fb operator ~ (Vec16fb a) {
    return ~Vec16b(a);
}

// vector operator ! : element not
static inline Vec16fb operator ! (Vec16fb a) {
    return ~a;
}

// vector operator &= : bitwise and
static inline Vec16fb & operator &= (Vec16fb & a, Vec16fb b) {
    a = a & b;
    return a;
}

// vector operator |= : bitwise or
static inline Vec16fb & operator |= (Vec16fb & a, Vec16fb b) {
    a = a | b;
    return a;
}

// vector operator ^= : bitwise xor
static inline Vec16fb & operator ^= (Vec16fb & a, Vec16fb b) {
    a = a ^ b;
    return a;
}


/*****************************************************************************
*
*          Vec8db: Vector of 8 Booleans for use with Vec8d
*
*****************************************************************************/

class Vec8db : public Vec8b {
public:
    // Default constructor:
    Vec8db () {
    }
    Vec8db (Vec16b x) {
        m16 = x;
    }
    // Constructor to build from all elements:
    Vec8db(bool x0, bool x1, bool x2, bool x3, bool x4, bool x5, bool x6, bool x7) :
        Vec8b(x0, x1, x2, x3, x4, x5, x6, x7) {
    }
    // Constructor to convert from type __mmask8 used in intrinsics:
    Vec8db (__mmask8 x) {
        m16 = x;
    }
    // Constructor to build from two halves
    Vec8db (Vec4db const & x0, Vec4db const & x1) {
        m16 = Vec8qb(Vec4qb(x0), Vec4qb(x1));
    }
    // Assignment operator to convert from type __mmask8 used in intrinsics:
    Vec8db & operator = (__mmask8 x) {
        m16 = x;
        return *this;
    }
    // Constructor to broadcast single value:
    Vec8db(bool b) : Vec8b(b) {}
    // Assignment operator to broadcast scalar:
    Vec8db & operator = (bool b) {
        m16 = Vec8b(b);
        return *this;
    }
private: // Prevent constructing from int, etc.
    Vec8db(int b);
    Vec8db & operator = (int x);
public:
    static int size () {
        return 8;
    }
};

// Define operators for Vec8db

// vector operator & : bitwise and
static inline Vec8db operator & (Vec8db a, Vec8db b) {
    return Vec16b(a) & Vec16b(b);
}
static inline Vec8db operator && (Vec8db a, Vec8db b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec8db operator | (Vec8db a, Vec8db b) {
    return Vec16b(a) | Vec16b(b);
}
static inline Vec8db operator || (Vec8db a, Vec8db b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec8db operator ^ (Vec8db a, Vec8db b) {
    return Vec16b(a) ^ Vec16b(b);
}

// vector operator ~ : bitwise not
static inline Vec8db operator ~ (Vec8db a) {
    return ~Vec16b(a);
}

// vector operator ! : element not
static inline Vec8db operator ! (Vec8db a) {
    return ~a;
}

// vector operator &= : bitwise and
static inline Vec8db & operator &= (Vec8db & a, Vec8db b) {
    a = a & b;
    return a;
}

// vector operator |= : bitwise or
static inline Vec8db & operator |= (Vec8db & a, Vec8db b) {
    a = a | b;
    return a;
}

// vector operator ^= : bitwise xor
static inline Vec8db & operator ^= (Vec8db & a, Vec8db b) {
    a = a ^ b;
    return a;
}


/*****************************************************************************
*
*          Vec16f: Vector of 16 single precision floating point values
*
*****************************************************************************/

class Vec16f {
protected:
    __m512 zmm; // Float vector
public:
    // Default constructor:
    Vec16f() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec16f(float f) {
        zmm = _mm512_set1_ps(f);
    }
    // Constructor to build from all elements:
    Vec16f(float f0, float f1, float f2, float f3, float f4, float f5, float f6, float f7,
    float f8, float f9, float f10, float f11, float f12, float f13, float f14, float f15) {
        zmm = _mm512_setr_ps(f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15); 
    }
    // Constructor to build from two Vec8f:
    Vec16f(Vec8f const & a0, Vec8f const & a1) {
        zmm = _mm512_castpd_ps(_mm512_insertf64x4(_mm512_castps_pd(_mm512_castps256_ps512(a0)), _mm256_castps_pd(a1), 1));
    }
    // Constructor to convert from type __m512 used in intrinsics:
    Vec16f(__m512 const & x) {
        zmm = x;
    }
    // Assignment operator to convert from type __m512 used in intrinsics:
    Vec16f & operator = (__m512 const & x) {
        zmm = x;
        return *this;
    }
    // Type cast operator to convert to __m512 used in intrinsics
    operator __m512() const {
        return zmm;
    }
    // Member function to load from array (unaligned)
    Vec16f & load(float const * p) {
        zmm = _mm512_loadu_ps(p);
        return *this;
    }
    // Member function to load from array, aligned by 64
    // You may use load_a instead of load if you are certain that p points to an address
    // divisible by 64.
    Vec16f & load_a(float const * p) {
        zmm = _mm512_load_ps(p);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(float * p) const {
        _mm512_storeu_ps(p, zmm);
    }
    // Member function to store into array, aligned by 64
    // You may use store_a instead of store if you are certain that p points to an address
    // divisible by 64.
    void store_a(float * p) const {
        _mm512_store_ps(p, zmm);
    }
    // Partial load. Load n elements and set the rest to 0
    Vec16f & load_partial(int n, float const * p) {
        zmm = _mm512_maskz_loadu_ps(__mmask16((1 << n) - 1), p);
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, float * p) const {
        _mm512_mask_storeu_ps(p, __mmask16((1 << n) - 1), zmm);
    }
    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec16f & cutoff(int n) {
        zmm = _mm512_maskz_mov_ps(__mmask16((1 << n) - 1), zmm);
        return *this;
    }
    // Member function to change a single element in vector
    Vec16f const & insert(uint32_t index, float value) {
        //zmm = _mm512_mask_set1_ps(zmm, __mmask16(1 << index), value);  // this intrinsic function does not exist (yet?)
        zmm = _mm512_castsi512_ps(_mm512_mask_set1_epi32(_mm512_castps_si512(zmm), __mmask16(1 << index), *(int32_t*)&value));  // ignore warning
        return *this;
    }
    // Member function extract a single element from vector
    float extract(uint32_t index) const {
        float a[16];
        store(a);
        return a[index & 15];
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    float operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4f:
    Vec8f get_low() const {
        return _mm512_castps512_ps256(zmm);
    }
    Vec8f get_high() const {
        return _mm256_castpd_ps(_mm512_extractf64x4_pd(_mm512_castps_pd(zmm),1));
    }
    static int size () {
        return 16;
    }
};


/*****************************************************************************
*
*          Operators for Vec16f
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec16f operator + (Vec16f const & a, Vec16f const & b) {
    return _mm512_add_ps(a, b);
}

// vector operator + : add vector and scalar
static inline Vec16f operator + (Vec16f const & a, float b) {
    return a + Vec16f(b);
}
static inline Vec16f operator + (float a, Vec16f const & b) {
    return Vec16f(a) + b;
}

// vector operator += : add
static inline Vec16f & operator += (Vec16f & a, Vec16f const & b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec16f operator ++ (Vec16f & a, int) {
    Vec16f a0 = a;
    a = a + 1.0f;
    return a0;
}

// prefix operator ++
static inline Vec16f & operator ++ (Vec16f & a) {
    a = a + 1.0f;
    return a;
}

// vector operator - : subtract element by element
static inline Vec16f operator - (Vec16f const & a, Vec16f const & b) {
    return _mm512_sub_ps(a, b);
}

// vector operator - : subtract vector and scalar
static inline Vec16f operator - (Vec16f const & a, float b) {
    return a - Vec16f(b);
}
static inline Vec16f operator - (float a, Vec16f const & b) {
    return Vec16f(a) - b;
}

// vector operator - : unary minus
// Change sign bit, even for 0, INF and NAN
static inline Vec16f operator - (Vec16f const & a) {
    return _mm512_castsi512_ps(Vec16i(_mm512_castps_si512(a)) ^ 0x80000000);
}

// vector operator -= : subtract
static inline Vec16f & operator -= (Vec16f & a, Vec16f const & b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec16f operator -- (Vec16f & a, int) {
    Vec16f a0 = a;
    a = a - 1.0f;
    return a0;
}

// prefix operator --
static inline Vec16f & operator -- (Vec16f & a) {
    a = a - 1.0f;
    return a;
}

// vector operator * : multiply element by element
static inline Vec16f operator * (Vec16f const & a, Vec16f const & b) {
    return _mm512_mul_ps(a, b);
}

// vector operator * : multiply vector and scalar
static inline Vec16f operator * (Vec16f const & a, float b) {
    return a * Vec16f(b);
}
static inline Vec16f operator * (float a, Vec16f const & b) {
    return Vec16f(a) * b;
}

// vector operator *= : multiply
static inline Vec16f & operator *= (Vec16f & a, Vec16f const & b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer
static inline Vec16f operator / (Vec16f const & a, Vec16f const & b) {
    return _mm512_div_ps(a, b);
}

// vector operator / : divide vector and scalar
static inline Vec16f operator / (Vec16f const & a, float b) {
    return a / Vec16f(b);
}
static inline Vec16f operator / (float a, Vec16f const & b) {
    return Vec16f(a) / b;
}

// vector operator /= : divide
static inline Vec16f & operator /= (Vec16f & a, Vec16f const & b) {
    a = a / b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec16fb operator == (Vec16f const & a, Vec16f const & b) {
//    return _mm512_cmpeq_ps_mask(a, b);
    return _mm512_cmp_ps_mask(a, b, 0);
}

// vector operator != : returns true for elements for which a != b
static inline Vec16fb operator != (Vec16f const & a, Vec16f const & b) {
//    return _mm512_cmpneq_ps_mask(a, b);
    return _mm512_cmp_ps_mask(a, b, 4);
}

// vector operator < : returns true for elements for which a < b
static inline Vec16fb operator < (Vec16f const & a, Vec16f const & b) {
//    return _mm512_cmplt_ps_mask(a, b);
    return _mm512_cmp_ps_mask(a, b, 1);

}

// vector operator <= : returns true for elements for which a <= b
static inline Vec16fb operator <= (Vec16f const & a, Vec16f const & b) {
//    return _mm512_cmple_ps_mask(a, b);
    return _mm512_cmp_ps_mask(a, b, 2);
}

// vector operator > : returns true for elements for which a > b
static inline Vec16fb operator > (Vec16f const & a, Vec16f const & b) {
    return b < a;
}

// vector operator >= : returns true for elements for which a >= b
static inline Vec16fb operator >= (Vec16f const & a, Vec16f const & b) {
    return b <= a;
}

// Bitwise logical operators

// vector operator & : bitwise and
static inline Vec16f operator & (Vec16f const & a, Vec16f const & b) {
    return _mm512_castsi512_ps(Vec16i(_mm512_castps_si512(a)) & Vec16i(_mm512_castps_si512(b)));
}

// vector operator &= : bitwise and
static inline Vec16f & operator &= (Vec16f & a, Vec16f const & b) {
    a = a & b;
    return a;
}

// vector operator & : bitwise and of Vec16f and Vec16fb
static inline Vec16f operator & (Vec16f const & a, Vec16fb const & b) {
    return _mm512_maskz_mov_ps(b, a);
}
static inline Vec16f operator & (Vec16fb const & a, Vec16f const & b) {
    return b & a;
}

// vector operator | : bitwise or
static inline Vec16f operator | (Vec16f const & a, Vec16f const & b) {
    return _mm512_castsi512_ps(Vec16i(_mm512_castps_si512(a)) | Vec16i(_mm512_castps_si512(b)));
}

// vector operator |= : bitwise or
static inline Vec16f & operator |= (Vec16f & a, Vec16f const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec16f operator ^ (Vec16f const & a, Vec16f const & b) {
    return _mm512_castsi512_ps(Vec16i(_mm512_castps_si512(a)) ^ Vec16i(_mm512_castps_si512(b)));
}

// vector operator ^= : bitwise xor
static inline Vec16f & operator ^= (Vec16f & a, Vec16f const & b) {
    a = a ^ b;
    return a;
}

// vector operator ! : logical not. Returns Boolean vector
static inline Vec16fb operator ! (Vec16f const & a) {
    return a == Vec16f(0.0f);
}


/*****************************************************************************
*
*          Functions for Vec16f
*
*****************************************************************************/

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 8; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or 0xFFFFFFFF (true). No other values are allowed.
static inline Vec16f select (Vec16fb const & s, Vec16f const & a, Vec16f const & b) {
    return _mm512_mask_mov_ps(b, s, a);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec16f if_add (Vec16fb const & f, Vec16f const & a, Vec16f const & b) {
    return _mm512_mask_add_ps(a, f, a, b);
}

// Conditional multiply: For all vector elements i: result[i] = f[i] ? (a[i] * b[i]) : a[i]
static inline Vec16f if_mul (Vec16fb const & f, Vec16f const & a, Vec16f const & b) {
    return _mm512_mask_mul_ps(a, f, a, b);
}

// Horizontal add: Calculates the sum of all vector elements.
static inline float horizontal_add (Vec16f const & a) {
#if defined(__INTEL_COMPILER)
    return _mm512_reduce_add_ps(a);
#else
    return horizontal_add(a.get_low() + a.get_high());
#endif
}

// function max: a > b ? a : b
static inline Vec16f max(Vec16f const & a, Vec16f const & b) {
    return _mm512_max_ps(a,b);
}

// function min: a < b ? a : b
static inline Vec16f min(Vec16f const & a, Vec16f const & b) {
    return _mm512_min_ps(a,b);
}

// function abs: absolute value
// Removes sign bit, even for -0.0f, -INF and -NAN
static inline Vec16f abs(Vec16f const & a) {
    union {
        int32_t i;
        float   f;
    } u = {0x7FFFFFFF};
    return a & Vec16f(u.f);
}

// function sqrt: square root
static inline Vec16f sqrt(Vec16f const & a) {
    return _mm512_sqrt_ps(a);
}

// function square: a * a
static inline Vec16f square(Vec16f const & a) {
    return a * a;
}

// pow(Vec16f, int):
template <typename TT> static Vec16f pow(Vec16f const & a, TT n);

// Raise floating point numbers to integer power n
template <>
inline Vec16f pow<int>(Vec16f const & x0, int n) {
    return pow_template_i<Vec16f>(x0, n);
}

// allow conversion from unsigned int
template <>
inline Vec16f pow<uint32_t>(Vec16f const & x0, uint32_t n) {
    return pow_template_i<Vec16f>(x0, (int)n);
}


// Raise floating point numbers to integer power n, where n is a compile-time constant
template <int n>
static inline Vec16f pow_n(Vec16f const & a) {
    if (n < 0)    return Vec16f(1.0f) / pow_n<-n>(a);
    if (n == 0)   return Vec16f(1.0f);
    if (n >= 256) return pow(a, n);
    Vec16f x = a;                      // a^(2^i)
    Vec16f y;                          // accumulator
    const int lowest = n - (n & (n-1));// lowest set bit in n
    if (n & 1) y = x;
    if (n < 2) return y;
    x = x*x;                           // x^2
    if (n & 2) {
        if (lowest == 2) y = x; else y *= x;
    }
    if (n < 4) return y;
    x = x*x;                           // x^4
    if (n & 4) {
        if (lowest == 4) y = x; else y *= x;
    }
    if (n < 8) return y;
    x = x*x;                           // x^8
    if (n & 8) {
        if (lowest == 8) y = x; else y *= x;
    }
    if (n < 16) return y;
    x = x*x;                           // x^16
    if (n & 16) {
        if (lowest == 16) y = x; else y *= x;
    }
    if (n < 32) return y;
    x = x*x;                           // x^32
    if (n & 32) {
        if (lowest == 32) y = x; else y *= x;
    }
    if (n < 64) return y;
    x = x*x;                           // x^64
    if (n & 64) {
        if (lowest == 64) y = x; else y *= x;
    }
    if (n < 128) return y;
    x = x*x;                           // x^128
    if (n & 128) {
        if (lowest == 128) y = x; else y *= x;
    }
    return y;
}

template <int n>
static inline Vec16f pow(Vec16f const & a, Const_int_t<n>) {
    return pow_n<n>(a);
}


// function round: round to nearest integer (even). (result as float vector)
static inline Vec16f round(Vec16f const & a) {
    return _mm512_roundscale_ps(a, 0);
}

// function truncate: round towards zero. (result as float vector)
static inline Vec16f truncate(Vec16f const & a) {
    return _mm512_roundscale_ps(a, 3);
}

// function floor: round towards minus infinity. (result as float vector)
static inline Vec16f floor(Vec16f const & a) {
    return _mm512_roundscale_ps(a, 1);
}

// function ceil: round towards plus infinity. (result as float vector)
static inline Vec16f ceil(Vec16f const & a) {
    return _mm512_roundscale_ps(a, 2);
}

// function round_to_int: round to nearest integer (even). (result as integer vector)
static inline Vec16i round_to_int(Vec16f const & a) {
    // Note: assume MXCSR control register is set to rounding
    return _mm512_cvt_roundps_epi32(a, _MM_FROUND_NO_EXC);
}

// function truncate_to_int: round towards zero. (result as integer vector)
static inline Vec16i truncate_to_int(Vec16f const & a) {
    return _mm512_cvtt_roundps_epi32(a, _MM_FROUND_NO_EXC);
}

// function to_float: convert integer vector to float vector
static inline Vec16f to_float(Vec16i const & a) {
    return _mm512_cvtepi32_ps(a);
}


// Approximate math functions

// approximate reciprocal (Faster than 1.f / a.
// relative accuracy better than 2^-11 without AVX512, 2^-14 with AVX512)
static inline Vec16f approx_recipr(Vec16f const & a) {
    return _mm512_rcp14_ps(a);
}

// approximate reciprocal squareroot (Faster than 1.f / sqrt(a).
// Relative accuracy better than 2^-11 without AVX512, 2^-14 with AVX512)
static inline Vec16f approx_rsqrt(Vec16f const & a) {
    return _mm512_rsqrt14_ps(a);
}


// Fused multiply and add functions

// Multiply and add
static inline Vec16f mul_add(Vec16f const & a, Vec16f const & b, Vec16f const & c) {
    return _mm512_fmadd_ps(a, b, c);
}

// Multiply and subtract
static inline Vec16f mul_sub(Vec16f const & a, Vec16f const & b, Vec16f const & c) {
    return _mm512_fmsub_ps(a, b, c);
}

// Multiply and inverse subtract
static inline Vec16f nmul_add(Vec16f const & a, Vec16f const & b, Vec16f const & c) {
    return _mm512_fnmadd_ps(a, b, c);
}

// Multiply and subtract with extra precision on the intermediate calculations, 
static inline Vec16f mul_sub_x(Vec16f const & a, Vec16f const & b, Vec16f const & c) {
    return _mm512_fmsub_ps(a, b, c);
}


// Math functions using fast bit manipulation

// Extract the exponent as an integer
// exponent(a) = floor(log2(abs(a)));
// exponent(1.0f) = 0, exponent(0.0f) = -127, exponent(INF) = +128, exponent(NAN) = +128
static inline Vec16i exponent(Vec16f const & a) {
    // return round_to_int(Vec16i(_mm512_getexp_ps(a)));
    Vec16ui t1 = _mm512_castps_si512(a);// reinterpret as 32-bit integers
    Vec16ui t2 = t1 << 1;               // shift out sign bit
    Vec16ui t3 = t2 >> 24;              // shift down logical to position 0
    Vec16i  t4 = Vec16i(t3) - 0x7F;     // subtract bias from exponent
    return t4;
}

// Extract the fraction part of a floating point number
// a = 2^exponent(a) * fraction(a), except for a = 0
// fraction(1.0f) = 1.0f, fraction(5.0f) = 1.25f 
static inline Vec16f fraction(Vec16f const & a) {
#if 1
    return _mm512_getmant_ps(a, _MM_MANT_NORM_1_2, _MM_MANT_SIGN_zero);
#else
    Vec8ui t1 = _mm512_castps_si512(a);   // reinterpret as 32-bit integer
    Vec8ui t2 = (t1 & 0x007FFFFF) | 0x3F800000; // set exponent to 0 + bias
    return _mm512_castsi512_ps(t2);
#endif
}

// Fast calculation of pow(2,n) with n integer
// n  =    0 gives 1.0f
// n >=  128 gives +INF
// n <= -127 gives 0.0f
// This function will never produce denormals, and never raise exceptions
static inline Vec16f exp2(Vec16i const & n) {
    Vec16i t1 = max(n,  -0x7F);         // limit to allowed range
    Vec16i t2 = min(t1,  0x80);
    Vec16i t3 = t2 + 0x7F;              // add bias
    Vec16i t4 = t3 << 23;               // put exponent into position 23
    return _mm512_castsi512_ps(t4);     // reinterpret as float
}
//static Vec16f exp2(Vec16f const & x); // defined in vectormath_exp.h



// Categorization functions

// Function sign_bit: gives true for elements that have the sign bit set
// even for -0.0f, -INF and -NAN
// Note that sign_bit(Vec16f(-0.0f)) gives true, while Vec16f(-0.0f) < Vec16f(0.0f) gives false
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec16fb sign_bit(Vec16f const & a) {
    Vec16i t1 = _mm512_castps_si512(a);    // reinterpret as 32-bit integer
    return Vec16fb(t1 < 0);
}

// Function sign_combine: changes the sign of a when b has the sign bit set
// same as select(sign_bit(b), -a, a)
static inline Vec16f sign_combine(Vec16f const & a, Vec16f const & b) {
    union {
        uint32_t i;
        float    f;
    } signmask = {0x80000000};
    return a ^ (b & Vec16f(signmask.f));
}

// Function is_finite: gives true for elements that are normal, denormal or zero, 
// false for INF and NAN
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec16fb is_finite(Vec16f const & a) {
    Vec16i  t1 = _mm512_castps_si512(a);    // reinterpret as 32-bit integer
    Vec16i  t2 = t1 << 1;                   // shift out sign bit
    Vec16ib t3 = Vec16i(t2 & 0xFF000000) != 0xFF000000; // exponent field is not all 1s
    return Vec16fb(t3);
}

// Function is_inf: gives true for elements that are +INF or -INF
// false for finite numbers and NAN
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec16fb is_inf(Vec16f const & a) {
    Vec16i t1 = _mm512_castps_si512(a); // reinterpret as 32-bit integer
    Vec16i t2 = t1 << 1;                // shift out sign bit
    return Vec16fb(t2 == 0xFF000000);   // exponent is all 1s, fraction is 0
}

// Function is_nan: gives true for elements that are +NAN or -NAN
// false for finite numbers and +/-INF
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec16fb is_nan(Vec16f const & a) {
    Vec16i t1 = _mm512_castps_si512(a); // reinterpret as 32-bit integer
    Vec16i t2 = t1 << 1;                // shift out sign bit
    Vec16i t3 = 0xFF000000;             // exponent mask
    Vec16i t4 = t2 & t3;                // exponent
    Vec16i t5 = _mm512_andnot_si512(t3,t2);// fraction
    return Vec16fb(t4 == t3 && t5 != 0);// exponent = all 1s and fraction != 0
}

// Function is_subnormal: gives true for elements that are denormal (subnormal)
// false for finite numbers, zero, NAN and INF
static inline Vec16fb is_subnormal(Vec16f const & a) {
    Vec16i t1 = _mm512_castps_si512(a);    // reinterpret as 32-bit integer
    Vec16i t2 = t1 << 1;                   // shift out sign bit
    Vec16i t3 = 0xFF000000;                // exponent mask
    Vec16i t4 = t2 & t3;                   // exponent
    Vec16i t5 = _mm512_andnot_si512(t3,t2);// fraction
    return Vec16fb(t4 == 0 && t5 != 0);     // exponent = 0 and fraction != 0
}

// Function is_zero_or_subnormal: gives true for elements that are zero or subnormal (denormal)
// false for finite numbers, NAN and INF
static inline Vec16fb is_zero_or_subnormal(Vec16f const & a) {
    Vec16i t = _mm512_castps_si512(a);            // reinterpret as 32-bit integer
           t &= 0x7F800000;                       // isolate exponent
    return Vec16fb(t == 0);                       // exponent = 0
}

// Function infinite4f: returns a vector where all elements are +INF
static inline Vec16f infinite16f() {
    union {
        int32_t i;
        float   f;
    } inf = {0x7F800000};
    return Vec16f(inf.f);
}

// Function nan4f: returns a vector where all elements are +NAN (quiet)
static inline Vec16f nan16f(int n = 0x10) {
    union {
        int32_t i;
        float   f;
    } nanf = {0x7FC00000 + n};
    return Vec16f(nanf.f);
}

// change signs on vectors Vec16f
// Each index i0 - i7 is 1 for changing sign on the corresponding element, 0 for no change
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7, int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
static inline Vec16f change_sign(Vec16f const & a) {
    const __mmask16 m = __mmask16((i0&1) | (i1&1)<<1 | (i2&1)<< 2 | (i3&1)<<3 | (i4&1)<<4 | (i5&1)<<5 | (i6&1)<<6 | (i7&1)<<7
        | (i8&1)<<8 | (i9&1)<<9 | (i10&1)<<10 | (i11&1)<<11 | (i12&1)<<12 | (i13&1)<<13 | (i14&1)<<14 | (i15&1)<<15);
    if ((uint16_t)m == 0) return a;
    __m512 s = _mm512_castsi512_ps(_mm512_maskz_set1_epi32(m, 0x80000000));
    return a ^ s;
}



/*****************************************************************************
*
*          Vec8d: Vector of 8 double precision floating point values
*
*****************************************************************************/

class Vec8d {
protected:
    __m512d zmm; // double vector
public:
    // Default constructor:
    Vec8d() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec8d(double d) {
        zmm = _mm512_set1_pd(d);
    }
    // Constructor to build from all elements:
    Vec8d(double d0, double d1, double d2, double d3, double d4, double d5, double d6, double d7) {
        zmm = _mm512_setr_pd(d0, d1, d2, d3, d4, d5, d6, d7); 
    }
    // Constructor to build from two Vec4d:
    Vec8d(Vec4d const & a0, Vec4d const & a1) {
        zmm = _mm512_insertf64x4(_mm512_castpd256_pd512(a0), a1, 1);
    }
    // Constructor to convert from type __m512d used in intrinsics:
    Vec8d(__m512d const & x) {
        zmm = x;
    }
    // Assignment operator to convert from type __m512d used in intrinsics:
    Vec8d & operator = (__m512d const & x) {
        zmm = x;
        return *this;
    }
    // Type cast operator to convert to __m512d used in intrinsics
    operator __m512d() const {
        return zmm;
    }
    // Member function to load from array (unaligned)
    Vec8d & load(double const * p) {
        zmm = _mm512_loadu_pd(p);
        return *this;
    }
    // Member function to load from array, aligned by 64
    // You may use load_a instead of load if you are certain that p points to an address
    // divisible by 64
    Vec8d & load_a(double const * p) {
        zmm = _mm512_load_pd(p);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(double * p) const {
        _mm512_storeu_pd(p, zmm);
    }
    // Member function to store into array, aligned by 64
    // You may use store_a instead of store if you are certain that p points to an address
    // divisible by 64
    void store_a(double * p) const {
        _mm512_store_pd(p, zmm);
    }
    // Partial load. Load n elements and set the rest to 0
    Vec8d & load_partial(int n, double const * p) {
        zmm = _mm512_maskz_loadu_pd(__mmask8((1<<n)-1), p);
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, double * p) const {
        _mm512_mask_storeu_pd(p, __mmask8((1<<n)-1), zmm);
    }
    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec8d & cutoff(int n) {
        zmm = _mm512_maskz_mov_pd(__mmask8((1<<n)-1), zmm);
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec8d const & insert(uint32_t index, double value) {
        //zmm = _mm512_mask_set1_pd(zmm, __mmask8(1 << index), value);  // this intrinsic function does not exist (yet?)
        zmm = _mm512_castsi512_pd(_mm512_mask_set1_epi64(_mm512_castpd_si512(zmm), __mmask8(1 << index), *(int64_t*)&value)); // ignore warning
        return *this;
    }
    // Member function extract a single element from vector
    double extract(uint32_t index) const {
        double a[8];
        store(a);
        return a[index & 7];        
    }

    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    double operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4d:
    Vec4d get_low() const {
        return _mm512_castpd512_pd256(zmm);
    }
    Vec4d get_high() const {
        return _mm512_extractf64x4_pd(zmm,1);
    }
    static int size () {
        return 8;
    }
};



/*****************************************************************************
*
*          Operators for Vec8d
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec8d operator + (Vec8d const & a, Vec8d const & b) {
    return _mm512_add_pd(a, b);
}

// vector operator + : add vector and scalar
static inline Vec8d operator + (Vec8d const & a, double b) {
    return a + Vec8d(b);
}
static inline Vec8d operator + (double a, Vec8d const & b) {
    return Vec8d(a) + b;
}

// vector operator += : add
static inline Vec8d & operator += (Vec8d & a, Vec8d const & b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec8d operator ++ (Vec8d & a, int) {
    Vec8d a0 = a;
    a = a + 1.0;
    return a0;
}

// prefix operator ++
static inline Vec8d & operator ++ (Vec8d & a) {
    a = a + 1.0;
    return a;
}

// vector operator - : subtract element by element
static inline Vec8d operator - (Vec8d const & a, Vec8d const & b) {
    return _mm512_sub_pd(a, b);
}

// vector operator - : subtract vector and scalar
static inline Vec8d operator - (Vec8d const & a, double b) {
    return a - Vec8d(b);
}
static inline Vec8d operator - (double a, Vec8d const & b) {
    return Vec8d(a) - b;
}

// vector operator - : unary minus
// Change sign bit, even for 0, INF and NAN
static inline Vec8d operator - (Vec8d const & a) {
    return _mm512_castsi512_pd(Vec8q(_mm512_castpd_si512(a)) ^ Vec8q(0x8000000000000000));
}

// vector operator -= : subtract
static inline Vec8d & operator -= (Vec8d & a, Vec8d const & b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec8d operator -- (Vec8d & a, int) {
    Vec8d a0 = a;
    a = a - 1.0;
    return a0;
}

// prefix operator --
static inline Vec8d & operator -- (Vec8d & a) {
    a = a - 1.0;
    return a;
}

// vector operator * : multiply element by element
static inline Vec8d operator * (Vec8d const & a, Vec8d const & b) {
    return _mm512_mul_pd(a, b);
}

// vector operator * : multiply vector and scalar
static inline Vec8d operator * (Vec8d const & a, double b) {
    return a * Vec8d(b);
}
static inline Vec8d operator * (double a, Vec8d const & b) {
    return Vec8d(a) * b;
}

// vector operator *= : multiply
static inline Vec8d & operator *= (Vec8d & a, Vec8d const & b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer
static inline Vec8d operator / (Vec8d const & a, Vec8d const & b) {
    return _mm512_div_pd(a, b);
}

// vector operator / : divide vector and scalar
static inline Vec8d operator / (Vec8d const & a, double b) {
    return a / Vec8d(b);
}
static inline Vec8d operator / (double a, Vec8d const & b) {
    return Vec8d(a) / b;
}

// vector operator /= : divide
static inline Vec8d & operator /= (Vec8d & a, Vec8d const & b) {
    a = a / b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec8db operator == (Vec8d const & a, Vec8d const & b) {
    return _mm512_cmp_pd_mask(a, b, 0);
}

// vector operator != : returns true for elements for which a != b
static inline Vec8db operator != (Vec8d const & a, Vec8d const & b) {
    return _mm512_cmp_pd_mask(a, b, 4);
}

// vector operator < : returns true for elements for which a < b
static inline Vec8db operator < (Vec8d const & a, Vec8d const & b) {
    return _mm512_cmp_pd_mask(a, b, 1);
}

// vector operator <= : returns true for elements for which a <= b
static inline Vec8db operator <= (Vec8d const & a, Vec8d const & b) {
    return _mm512_cmp_pd_mask(a, b, 2);
}

// vector operator > : returns true for elements for which a > b
static inline Vec8db operator > (Vec8d const & a, Vec8d const & b) {
    return b < a;
}

// vector operator >= : returns true for elements for which a >= b
static inline Vec8db operator >= (Vec8d const & a, Vec8d const & b) {
    return b <= a;
}

// Bitwise logical operators

// vector operator & : bitwise and
static inline Vec8d operator & (Vec8d const & a, Vec8d const & b) {
    return _mm512_castsi512_pd(Vec8q(_mm512_castpd_si512(a)) & Vec8q(_mm512_castpd_si512(b)));
}

// vector operator &= : bitwise and
static inline Vec8d & operator &= (Vec8d & a, Vec8d const & b) {
    a = a & b;
    return a;
}

// vector operator & : bitwise and of Vec8d and Vec8db
static inline Vec8d operator & (Vec8d const & a, Vec8db const & b) {
    return _mm512_maskz_mov_pd(b, a);
}

static inline Vec8d operator & (Vec8db const & a, Vec8d const & b) {
    return b & a;
}

// vector operator | : bitwise or
static inline Vec8d operator | (Vec8d const & a, Vec8d const & b) {
    return _mm512_castsi512_pd(Vec8q(_mm512_castpd_si512(a)) | Vec8q(_mm512_castpd_si512(b)));
}

// vector operator |= : bitwise or
static inline Vec8d & operator |= (Vec8d & a, Vec8d const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec8d operator ^ (Vec8d const & a, Vec8d const & b) {
    return _mm512_castsi512_pd(Vec8q(_mm512_castpd_si512(a)) ^ Vec8q(_mm512_castpd_si512(b)));
}

// vector operator ^= : bitwise xor
static inline Vec8d & operator ^= (Vec8d & a, Vec8d const & b) {
    a = a ^ b;
    return a;
}

// vector operator ! : logical not. Returns Boolean vector
static inline Vec8db operator ! (Vec8d const & a) {
    return a == Vec8d(0.0);
}


/*****************************************************************************
*
*          Functions for Vec8d
*
*****************************************************************************/

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 2; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec8d select (Vec8db const & s, Vec8d const & a, Vec8d const & b) {
    return _mm512_mask_mov_pd (b, s, a);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec8d if_add (Vec8db const & f, Vec8d const & a, Vec8d const & b) {
    return _mm512_mask_add_pd(a, f, a, b);
}

// Conditional multiply: For all vector elements i: result[i] = f[i] ? (a[i] * b[i]) : a[i]
static inline Vec8d if_mul (Vec8db const & f, Vec8d const & a, Vec8d const & b) {
    return _mm512_mask_mul_pd(a, f, a, b);
}


// General arithmetic functions, etc.

// Horizontal add: Calculates the sum of all vector elements.
static inline double horizontal_add (Vec8d const & a) {
#if defined(__INTEL_COMPILER)
    return _mm512_reduce_add_pd(a);
#else
    return horizontal_add(a.get_low() + a.get_high());
#endif
}

// function max: a > b ? a : b
static inline Vec8d max(Vec8d const & a, Vec8d const & b) {
    return _mm512_max_pd(a,b);
}

// function min: a < b ? a : b
static inline Vec8d min(Vec8d const & a, Vec8d const & b) {
    return _mm512_min_pd(a,b);
}

// function abs: absolute value
// Removes sign bit, even for -0.0f, -INF and -NAN
static inline Vec8d abs(Vec8d const & a) {
    return _mm512_castsi512_pd(Vec8q(_mm512_castpd_si512(a)) & Vec8q(0x7FFFFFFFFFFFFFFF));
}

// function sqrt: square root
static inline Vec8d sqrt(Vec8d const & a) {
    return _mm512_sqrt_pd(a);
}

// function square: a * a
static inline Vec8d square(Vec8d const & a) {
    return a * a;
}

// pow(Vec8d, int):
template <typename TT> static Vec8d pow(Vec8d const & a, TT n);

// Raise floating point numbers to integer power n
template <>
inline Vec8d pow<int>(Vec8d const & x0, int n) {
    return pow_template_i<Vec8d>(x0, n);
}

// allow conversion from unsigned int
template <>
inline Vec8d pow<uint32_t>(Vec8d const & x0, uint32_t n) {
    return pow_template_i<Vec8d>(x0, (int)n);
}


// Raise floating point numbers to integer power n, where n is a compile-time constant
template <int n>
static inline Vec8d pow_n(Vec8d const & a) {
    if (n < 0)    return Vec8d(1.0) / pow_n<-n>(a);
    if (n == 0)   return Vec8d(1.0);
    if (n >= 256) return pow(a, n);
    Vec8d x = a;                       // a^(2^i)
    Vec8d y;                           // accumulator
    const int lowest = n - (n & (n-1));// lowest set bit in n
    if (n & 1) y = x;
    if (n < 2) return y;
    x = x*x;                           // x^2
    if (n & 2) {
        if (lowest == 2) y = x; else y *= x;
    }
    if (n < 4) return y;
    x = x*x;                           // x^4
    if (n & 4) {
        if (lowest == 4) y = x; else y *= x;
    }
    if (n < 8) return y;
    x = x*x;                           // x^8
    if (n & 8) {
        if (lowest == 8) y = x; else y *= x;
    }
    if (n < 16) return y;
    x = x*x;                           // x^16
    if (n & 16) {
        if (lowest == 16) y = x; else y *= x;
    }
    if (n < 32) return y;
    x = x*x;                           // x^32
    if (n & 32) {
        if (lowest == 32) y = x; else y *= x;
    }
    if (n < 64) return y;
    x = x*x;                           // x^64
    if (n & 64) {
        if (lowest == 64) y = x; else y *= x;
    }
    if (n < 128) return y;
    x = x*x;                           // x^128
    if (n & 128) {
        if (lowest == 128) y = x; else y *= x;
    }
    return y;
}

template <int n>
static inline Vec8d pow(Vec8d const & a, Const_int_t<n>) {
    return pow_n<n>(a);
}


// function round: round to nearest integer (even). (result as double vector)
static inline Vec8d round(Vec8d const & a) {
    return _mm512_roundscale_pd(a, 0);
}

// function truncate: round towards zero. (result as double vector)
static inline Vec8d truncate(Vec8d const & a) {
    return _mm512_roundscale_pd(a, 3);
}

// function floor: round towards minus infinity. (result as double vector)
static inline Vec8d floor(Vec8d const & a) {
    return _mm512_roundscale_pd(a, 1);
}

// function ceil: round towards plus infinity. (result as double vector)
static inline Vec8d ceil(Vec8d const & a) {
    return _mm512_roundscale_pd(a, 2);
}

// function round_to_int: round to nearest integer (even). (result as integer vector)
static inline Vec8i round_to_int(Vec8d const & a) {
    // Note: assume MXCSR control register is set to rounding
    return _mm512_cvtpd_epi32(a);
}

// function truncate_to_int: round towards zero. (result as integer vector)
static inline Vec8i truncate_to_int(Vec8d const & a) {
    return _mm512_cvttpd_epi32(a);
}

// function truncate_to_int64: round towards zero. (inefficient)
static inline Vec8q truncate_to_int64(Vec8d const & a) {
    // in 64-bit mode, use __int64 _mm_cvttsd_si64(__m128d a) ?
    double aa[8];
    a.store(aa);
    return Vec8q(int64_t(aa[0]), int64_t(aa[1]), int64_t(aa[2]), int64_t(aa[3]), int64_t(aa[4]), int64_t(aa[5]), int64_t(aa[6]), int64_t(aa[7]));
}

// function truncate_to_int64_limited: round towards zero.
// result as 64-bit integer vector, but with limited range
static inline Vec8q truncate_to_int64_limited(Vec8d const & a) {
    // Note: assume MXCSR control register is set to rounding
    Vec4q   b = _mm512_cvttpd_epi32(a);                    // round to 32-bit integers
    __m512i c = permute8q<0,-256,1,-256,2,-256,3,-256>(Vec8q(b,b));      // get bits 64-127 to position 128-191, etc.
    __m512i s = _mm512_srai_epi32(c, 31);                  // sign extension bits
    return      _mm512_unpacklo_epi32(c, s);               // interleave with sign extensions
} 

// function round_to_int64: round to nearest or even. (inefficient)
static inline Vec8q round_to_int64(Vec8d const & a) {
    return truncate_to_int64(round(a));
}

// function round_to_int64_limited: round to nearest integer (even)
// result as 64-bit integer vector, but with limited range
static inline Vec8q round_to_int64_limited(Vec8d const & a) {
    // Note: assume MXCSR control register is set to rounding
    Vec4q   b = _mm512_cvtpd_epi32(a);                     // round to 32-bit integers
    __m512i c = permute8q<0,-256,1,-256,2,-256,3,-256>(Vec8q(b,b));      // get bits 64-127 to position 128-191, etc.
    __m512i s = _mm512_srai_epi32(c, 31);                  // sign extension bits
    return      _mm512_unpacklo_epi32(c, s);               // interleave with sign extensions
}

// function to_double: convert integer vector elements to double vector (inefficient)
static inline Vec8d to_double(Vec8q const & a) {
    int64_t aa[8];
    a.store(aa);
    return Vec8d(double(aa[0]), double(aa[1]), double(aa[2]), double(aa[3]), double(aa[4]), double(aa[5]), double(aa[6]), double(aa[7]));
}

// function to_double_limited: convert integer vector elements to double vector
// limited to abs(x) < 2^31
static inline Vec8d to_double_limited(Vec8q const & x) {
    Vec16i compressed = permute16i<0,2,4,6,8,10,12,14,-256,-256,-256,-256,-256,-256,-256,-256>(Vec16i(x));
    return _mm512_cvtepi32_pd(compressed.get_low());
}

// function to_double: convert integer vector to double vector
static inline Vec8d to_double(Vec8i const & a) {
    return _mm512_cvtepi32_pd(a);
}

// function compress: convert two Vec8d to one Vec16f
static inline Vec16f compress (Vec8d const & low, Vec8d const & high) {
    __m256 t1 = _mm512_cvtpd_ps(low);
    __m256 t2 = _mm512_cvtpd_ps(high);
    return Vec16f(t1, t2);
}

// Function extend_low : convert Vec16f vector elements 0 - 3 to Vec8d
static inline Vec8d extend_low(Vec16f const & a) {
    return _mm512_cvtps_pd(_mm512_castps512_ps256(a));
}

// Function extend_high : convert Vec16f vector elements 4 - 7 to Vec8d
static inline Vec8d extend_high (Vec16f const & a) {
    return _mm512_cvtps_pd(a.get_high());
}


// Fused multiply and add functions

// Multiply and add
static inline Vec8d mul_add(Vec8d const & a, Vec8d const & b, Vec8d const & c) {
    return _mm512_fmadd_pd(a, b, c);
}

// Multiply and subtract
static inline Vec8d mul_sub(Vec8d const & a, Vec8d const & b, Vec8d const & c) {
    return _mm512_fmsub_pd(a, b, c);
}

// Multiply and inverse subtract
static inline Vec8d nmul_add(Vec8d const & a, Vec8d const & b, Vec8d const & c) {
    return _mm512_fnmadd_pd(a, b, c);
}

// Multiply and subtract with extra precision on the intermediate calculations, 
static inline Vec8d mul_sub_x(Vec8d const & a, Vec8d const & b, Vec8d const & c) {
    return _mm512_fmsub_pd(a, b, c);
}


// Math functions using fast bit manipulation

// Extract the exponent as an integer
// exponent(a) = floor(log2(abs(a)));
// exponent(1.0) = 0, exponent(0.0) = -1023, exponent(INF) = +1024, exponent(NAN) = +1024
static inline Vec8q exponent(Vec8d const & a) {
    Vec8uq t1 = _mm512_castpd_si512(a);// reinterpret as 64-bit integer
    Vec8uq t2 = t1 << 1;               // shift out sign bit
    Vec8uq t3 = t2 >> 53;              // shift down logical to position 0
    Vec8q  t4 = Vec8q(t3) - 0x3FF;     // subtract bias from exponent
    return t4;
}

// Extract the fraction part of a floating point number
// a = 2^exponent(a) * fraction(a), except for a = 0
// fraction(1.0) = 1.0, fraction(5.0) = 1.25 
static inline Vec8d fraction(Vec8d const & a) {
    return _mm512_getmant_pd(a, _MM_MANT_NORM_1_2, _MM_MANT_SIGN_zero);
}

// Fast calculation of pow(2,n) with n integer
// n  =     0 gives 1.0
// n >=  1024 gives +INF
// n <= -1023 gives 0.0
// This function will never produce denormals, and never raise exceptions
static inline Vec8d exp2(Vec8q const & n) {
    Vec8q t1 = max(n,  -0x3FF);        // limit to allowed range
    Vec8q t2 = min(t1,  0x400);
    Vec8q t3 = t2 + 0x3FF;             // add bias
    Vec8q t4 = t3 << 52;               // put exponent into position 52
    return _mm512_castsi512_pd(t4);    // reinterpret as double
}
//static Vec8d exp2(Vec8d const & x); // defined in vectormath_exp.h


// Categorization functions

// Function sign_bit: gives true for elements that have the sign bit set
// even for -0.0, -INF and -NAN
// Note that sign_bit(Vec8d(-0.0)) gives true, while Vec8d(-0.0) < Vec8d(0.0) gives false
static inline Vec8db sign_bit(Vec8d const & a) {
    Vec8q t1 = _mm512_castpd_si512(a);    // reinterpret as 64-bit integer
    return Vec8db(t1 < 0);
}

// Function sign_combine: changes the sign of a when b has the sign bit set
// same as select(sign_bit(b), -a, a)
static inline Vec8d sign_combine(Vec8d const & a, Vec8d const & b) {
    union {
        uint64_t i;
        double f;
    } u = {0x8000000000000000};  // mask for sign bit
    return a ^ (b & Vec8d(u.f));
}

// Function is_finite: gives true for elements that are normal, denormal or zero, 
// false for INF and NAN
static inline Vec8db is_finite(Vec8d const & a) {
    Vec8q  t1 = _mm512_castpd_si512(a); // reinterpret as 64-bit integer
    Vec8q  t2 = t1 << 1;                // shift out sign bit
    Vec8q  t3 = 0xFFE0000000000000;     // exponent mask
    Vec8qb t4 = Vec8q(t2 & t3) != t3;   // exponent field is not all 1s
    return Vec8db(t4);
}

// Function is_inf: gives true for elements that are +INF or -INF
// false for finite numbers and NAN
static inline Vec8db is_inf(Vec8d const & a) {
    Vec8q t1 = _mm512_castpd_si512(a);           // reinterpret as 64-bit integer
    Vec8q t2 = t1 << 1;                          // shift out sign bit
    return Vec8db(t2 == 0xFFE0000000000000);     // exponent is all 1s, fraction is 0
}

// Function is_nan: gives true for elements that are +NAN or -NAN
// false for finite numbers and +/-INF
static inline Vec8db is_nan(Vec8d const & a) {
    Vec8q t1 = _mm512_castpd_si512(a); // reinterpret as 64-bit integer
    Vec8q t2 = t1 << 1;                // shift out sign bit
    Vec8q t3 = 0xFFE0000000000000;     // exponent mask
    Vec8q t4 = t2 & t3;                // exponent
    Vec8q t5 = _mm512_andnot_si512(t3,t2);// fraction
    return Vec8db(t4 == t3 && t5 != 0);// exponent = all 1s and fraction != 0
}

// Function is_subnormal: gives true for elements that are denormal (subnormal)
// false for finite numbers, zero, NAN and INF
static inline Vec8db is_subnormal(Vec8d const & a) {
    Vec8q t1 = _mm512_castpd_si512(a); // reinterpret as 64-bit integer
    Vec8q t2 = t1 << 1;                // shift out sign bit
    Vec8q t3 = 0xFFE0000000000000;     // exponent mask
    Vec8q t4 = t2 & t3;                // exponent
    Vec8q t5 = _mm512_andnot_si512(t3,t2);// fraction
    return Vec8db(t4 == 0 && t5 != 0); // exponent = 0 and fraction != 0
}

// Function is_zero_or_subnormal: gives true for elements that are zero or subnormal (denormal)
// false for finite numbers, NAN and INF
static inline Vec8db is_zero_or_subnormal(Vec8d const & a) {
    Vec8q t = _mm512_castpd_si512(a);            // reinterpret as 32-bit integer
          t &= 0x7FF0000000000000ll;             // isolate exponent
    return Vec8db(t == 0);                       // exponent = 0
}

// Function infinite2d: returns a vector where all elements are +INF
static inline Vec8d infinite8d() {
    union {
        uint64_t i;
        double f;
    } u = {0x7FF0000000000000};
    return Vec8d(u.f);
}

// Function nan8d: returns a vector where all elements are +NAN (quiet NAN)
static inline Vec8d nan8d(int n = 0x10) {
    union {
        uint64_t i;
        double f;
    } u = {0x7FF8000000000000 + uint64_t(n)};
    return Vec8d(u.f);
}

// change signs on vectors Vec8d
// Each index i0 - i3 is 1 for changing sign on the corresponding element, 0 for no change
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8d change_sign(Vec8d const & a) {
    const __mmask8 m = __mmask8((i0&1) | (i1&1)<<1 | (i2&1)<< 2 | (i3&1)<<3 | (i4&1)<<4 | (i5&1)<<5 | (i6&1)<<6 | (i7&1)<<7);
    if ((uint8_t)m == 0) return a;
    __m512d s = _mm512_castsi512_pd(_mm512_maskz_set1_epi64(m, 0x8000000000000000));
    return a ^ s;
}


/*****************************************************************************
*
*          Functions for reinterpretation between vector types
*
*****************************************************************************/

// AVX512 requires gcc version 4.9 or higher. Apparently the problem with mangling intrinsic vector types no longer exists in gcc 4.x

static inline __m512i reinterpret_i (__m512i const & x) {
    return x;
}

static inline __m512i reinterpret_i (__m512  const & x) {
    return _mm512_castps_si512(x);
}

static inline __m512i reinterpret_i (__m512d const & x) {
    return _mm512_castpd_si512(x);
}

static inline __m512  reinterpret_f (__m512i const & x) {
    return _mm512_castsi512_ps(x);
}

static inline __m512  reinterpret_f (__m512  const & x) {
    return x;
}

static inline __m512  reinterpret_f (__m512d const & x) {
    return _mm512_castpd_ps(x);
}

static inline __m512d reinterpret_d (__m512i const & x) {
    return _mm512_castsi512_pd(x);
}

static inline __m512d reinterpret_d (__m512  const & x) {
    return _mm512_castps_pd(x);
}

static inline __m512d reinterpret_d (__m512d const & x) {
    return x;
}

/*****************************************************************************
*
*          Vector permute functions
*
******************************************************************************
*
* These permute functions can reorder the elements of a vector and optionally
* set some elements to zero. 
*
* The indexes are inserted as template parameters in <>. These indexes must be
* constants. Each template parameter is an index to the element you want to select.
* An index of -1 will generate zero. An index of -256 means don't care.
*
* Example:
* Vec8d a(10,11,12,13,14,15,16,17);      // a is (10,11,12,13,14,15,16,17)
* Vec8d b;
* b = permute8d<0,2,7,7,-1,-1,1,1>(a);   // b is (10,12,17,17, 0, 0,11,11)
*
* A lot of the code here is metaprogramming aiming to find the instructions
* that best fit the template parameters and instruction set. The metacode
* will be reduced out to leave only a few vector instructions in release
* mode with optimization on.
*****************************************************************************/

// Permute vector of 8 64-bit integers.
// Index -1 gives 0, index -256 means don't care.
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8d permute8d(Vec8d const & a) {

    // Combine indexes into a single bitfield, with 4 bits for each
    const int m1 = (i0&7) | (i1&7)<<4 | (i2&7)<< 8 | (i3&7)<<12 | (i4&7)<<16 | (i5&7)<<20 | (i6&7)<<24 | (i7&7)<<28;

    // Mask to zero out negative indexes
    const int mz = (i0<0?0:0xF) | (i1<0?0:0xF0) | (i2<0?0:0xF00) | (i3<0?0:0xF000) | (i4<0?0:0xF0000) | (i5<0?0:0xF00000) | (i6<0?0:0xF000000) | (i7<0?0:0xF0000000);
    const int m2 = m1 & mz;

    // zeroing needed
    const bool dozero = ((i0|i1|i2|i3|i4|i5|i6|i7) & 0x80) != 0;

    // special case: all zero
    if (mz == 0) return  _mm512_setzero_pd();

    // mask for elements not zeroed
    const __mmask8  z = __mmask8((i0>=0)<<0 | (i1>=0)<<1 | (i2>=0)<<2 | (i3>=0)<<3 | (i4>=0)<<4 | (i5>=0)<<5 | (i6>=0)<<6 | (i7>=0)<<7);
    // same with 2 bits for each element
    const __mmask16 zz = __mmask16((i0>=0?3:0) | (i1>=0?0xC:0) | (i2>=0?0x30:0) | (i3>=0?0xC0:0) | (i4>=0?0x300:0) | (i5>=0?0xC00:0) | (i6>=0?0x3000:0) | (i7>=0?0xC000:0));

    if (((m1 ^ 0x76543210) & mz) == 0) {
        // no shuffling
        if (dozero) {
            // zero some elements
            return _mm512_maskz_mov_pd(z, a);
        }
        return a;                                 // do nothing
    }

    if (((m1 ^ 0x66442200) & 0x66666666 & mz) == 0) {
        // no exchange of data between the four 128-bit lanes
        const int pat = ((m2 | m2 >> 8 | m2 >> 16 | m2 >> 24) & 0x11) * 0x01010101;
        const int pmask = ((pat & 1) * 10 + 4) | ((((pat >> 4) & 1) * 10 + 4) << 4);
        if (((m1 ^ pat) & mz & 0x11111111) == 0) {
            // same permute pattern in all lanes
            if (dozero) {  // permute within lanes and zero
                return _mm512_castsi512_pd(_mm512_maskz_shuffle_epi32(zz, _mm512_castpd_si512(a), (_MM_PERM_ENUM)pmask));
            }
            else {  // permute within lanes
                return _mm512_castsi512_pd(_mm512_shuffle_epi32(_mm512_castpd_si512(a), (_MM_PERM_ENUM)pmask));
            }
        }
        // different permute patterns in each lane. It's faster to do a full permute than four masked permutes within lanes
    }
    if ((((m1 ^ 0x10101010) & 0x11111111 & mz) == 0) 
    &&  ((m1 ^ (m1 >> 4)) & 0x06060606 & mz & (mz >> 4)) == 0) {
        // permute lanes only. no permutation within each lane
        const int m3 = m2 | (m2 >> 4);
        const int s = ((m3 >> 1) & 3) | (((m3 >> 9) & 3) << 2) | (((m3 >> 17) & 3) << 4) | (((m3 >> 25) & 3) << 6);
        if (dozero) {
            // permute lanes and zero some 64-bit elements
            return  _mm512_maskz_shuffle_f64x2(z, a, a, (_MM_PERM_ENUM)s);
        }
        else {
            // permute lanes
            return _mm512_shuffle_f64x2(a, a, (_MM_PERM_ENUM)s);
        }
    }
    // full permute needed
    const __m512i pmask = constant16i<i0&7, 0, i1&7, 0, i2&7, 0, i3&7, 0, i4&7, 0, i5&7, 0, i6&7, 0, i7&7, 0>();
    if (dozero) {
        // full permute and zeroing
        return _mm512_maskz_permutexvar_pd(z, pmask, a);
    }
    else {    
        return _mm512_permutexvar_pd(pmask, a);
    }
}



// Permute vector of 16 32-bit integers.
// Index -1 gives 0, index -256 means don't care.
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7, int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
static inline Vec16f permute16f(Vec16f const & a) {

    // Combine indexes into a single bitfield, with 4 bits for each
    const uint64_t m1 = (i0&15) | (i1&15)<<4 | (i2&15)<< 8 | (i3&15)<<12 | (i4&15)<<16 | (i5&15)<<20 | (i6&15)<<24 | (i7&15LL)<<28   // 15LL avoids sign extension of (int32_t | int64_t)
        | (i8&15LL)<<32 | (i9&15LL)<<36 | (i10&15LL)<<40 | (i11&15LL)<<44 | (i12&15LL)<<48 | (i13&15LL)<<52 | (i14&15LL)<<56 | (i15&15LL)<<60;

    // Mask to zero out negative indexes
    const uint64_t mz = (i0<0?0:0xF) | (i1<0?0:0xF0) | (i2<0?0:0xF00) | (i3<0?0:0xF000) | (i4<0?0:0xF0000) | (i5<0?0:0xF00000) | (i6<0?0:0xF000000) | (i7<0?0:0xF0000000ULL) | (i8<0?0:0xF00000000) 
        | (i9<0?0:0xF000000000) | (i10<0?0:0xF0000000000) | (i11<0?0:0xF00000000000) | (i12<0?0:0xF000000000000) | (i13<0?0:0xF0000000000000) | (i14<0?0:0xF00000000000000) | (i15<0?0:0xF000000000000000);

    const uint64_t m2 = m1 & mz;

    // zeroing needed
    const bool dozero = ((i0|i1|i2|i3|i4|i5|i6|i7|i8|i9|i10|i11|i12|i13|i14|i15) & 0x80) != 0;

    // special case: all zero
    if (mz == 0) return  _mm512_setzero_ps();

    // mask for elements not zeroed
    const __mmask16 z = __mmask16((i0>=0)<<0 | (i1>=0)<<1 | (i2>=0)<<2 | (i3>=0)<<3 | (i4>=0)<<4 | (i5>=0)<<5 | (i6>=0)<<6 | (i7>=0)<<7
        | (i8>=0)<<8 | (i9>=0)<<9 | (i10>=0)<<10 | (i11>=0)<<11 | (i12>=0)<<12 | (i13>=0)<<13 | (i14>=0)<<14 | (i15>=0)<<15);

    if (((m1 ^ 0xFEDCBA9876543210) & mz) == 0) {
        // no shuffling
        if (dozero) {
            // zero some elements
            return _mm512_maskz_mov_ps(z, a);
        }
        return a;                                 // do nothing
    }

    if (((m1 ^ 0xCCCC888844440000) & 0xCCCCCCCCCCCCCCCC & mz) == 0) {
        // no exchange of data between the four 128-bit lanes
        const uint64_t pat = ((m2 | (m2 >> 16) | (m2 >> 32) | (m2 >> 48)) & 0x3333) * 0x0001000100010001;
        const int pmask = (pat & 3) | (((pat >> 4) & 3) << 2) | (((pat >> 8) & 3) << 4) | (((pat >> 12) & 3) << 6);
        if (((m1 ^ pat) & 0x3333333333333333 & mz) == 0) {
            // same permute pattern in all lanes
            if (dozero) {  // permute within lanes and zero
                return _mm512_castsi512_ps(_mm512_maskz_shuffle_epi32(z, _mm512_castps_si512(a), (_MM_PERM_ENUM)pmask));
            }
            else {  // permute within lanes
                return _mm512_castsi512_ps(_mm512_shuffle_epi32(_mm512_castps_si512(a), (_MM_PERM_ENUM)pmask));
            }
        }
        // different permute patterns in each lane. It's faster to do a full permute than four masked permutes within lanes
    }
    const uint64_t lane = (m2 | m2 >> 4 | m2 >> 8 | m2 >> 12) & 0x000C000C000C000C;
    if ((((m1 ^ 0x3210321032103210) & 0x3333333333333333 & mz) == 0) 
    &&  ((m1 ^ (lane * 0x1111)) & 0xCCCCCCCCCCCCCCCC & mz) == 0) {
        // permute lanes only. no permutation within each lane
        const uint64_t s = ((lane >> 2) & 3) | (((lane >> 18) & 3) << 2) | (((lane >> 34) & 3) << 4) | (((lane >> 50) & 3) << 6);
        if (dozero) {
            // permute lanes and zero some 64-bit elements
            return  _mm512_maskz_shuffle_f32x4(z, a, a, (_MM_PERM_ENUM)s);
        }
        else {
            // permute lanes
            return _mm512_shuffle_f32x4(a, a, (_MM_PERM_ENUM)s);
        }
    }
    // full permute needed
    const __m512i pmask = constant16i<i0&15, i1&15, i2&15, i3&15, i4&15, i5&15, i6&15, i7&15, i8&15, i9&15, i10&15, i11&15, i12&15, i13&15, i14&15, i15&15>();
    if (dozero) {
        // full permute and zeroing
        return _mm512_maskz_permutexvar_ps(z, pmask, a);
    }
    else {    
        return _mm512_permutexvar_ps(pmask, a);
    }
}


/*****************************************************************************
*
*          Vector blend functions
*
******************************************************************************
*
* These blend functions can mix elements from two different vectors and
* optionally set some elements to zero. 
*
* The indexes are inserted as template parameters in <>. These indexes must be
* constants. Each template parameter is an index to the element you want to 
* select, where higher indexes indicate an element from the second source
* vector. For example, if each vector has 8 elements, then indexes 0 - 7
* will select an element from the first vector and indexes 8 - 15 will select 
* an element from the second vector. A negative index will generate zero.
*
* Example:
* Vec8d a(100,101,102,103,104,105,106,107); // a is (100, 101, 102, 103, 104, 105, 106, 107)
* Vec8d b(200,201,202,203,204,205,206,207); // b is (200, 201, 202, 203, 204, 205, 206, 207)
* Vec8d c;
* c = blend8d<1,0,9,8,7,-1,15,15> (a,b);    // c is (101, 100, 201, 200, 107,   0, 207, 207)
*
* A lot of the code here is metaprogramming aiming to find the instructions
* that best fit the template parameters and instruction set. The metacode
* will be reduced out to leave only a few vector instructions in release
* mode with optimization on.
*****************************************************************************/


template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7> 
static inline Vec8d blend8d(Vec8d const & a, Vec8d const & b) {  

    // Combine indexes into a single bitfield, with 4 bits for each
    const int m1 = (i0&0xF) | (i1&0xF)<<4 | (i2&0xF)<< 8 | (i3&0xF)<<12 | (i4&0xF)<<16 | (i5&0xF)<<20 | (i6&0xF)<<24 | (i7&0xF)<<28;

    // Mask to zero out negative indexes
    const int mz = (i0<0?0:0xF) | (i1<0?0:0xF0) | (i2<0?0:0xF00) | (i3<0?0:0xF000) | (i4<0?0:0xF0000) | (i5<0?0:0xF00000) | (i6<0?0:0xF000000) | (i7<0?0:0xF0000000);
    const int m2 = m1 & mz;

    // zeroing needed
    const bool dozero = ((i0|i1|i2|i3|i4|i5|i6|i7) & 0x80) != 0;

    // mask for elements not zeroed
    const __mmask8 z = __mmask8((i0>=0)<<0 | (i1>=0)<<1 | (i2>=0)<<2 | (i3>=0)<<3 | (i4>=0)<<4 | (i5>=0)<<5 | (i6>=0)<<6 | (i7>=0)<<7);

    // special case: all zero
    if (mz == 0) return  _mm512_setzero_pd();

    // special case: all from a
    if ((m1 & 0x88888888 & mz) == 0) {
        return permute8d <i0, i1, i2, i3, i4, i5, i6, i7> (a);
    }

    // special case: all from b
    if ((~m1 & 0x88888888 & mz) == 0) {
        return permute8d <i0^8, i1^8, i2^8, i3^8, i4^8, i5^8, i6^8, i7^8> (b);
    }

    // special case: blend without permute
    if (((m1 ^ 0x76543210) & 0x77777777 & mz) == 0) {
        __mmask8 blendmask = __mmask8((i0&8)>>3 | (i1&8)>>2 | (i2&8)>>1 | (i3&8)>>0 | (i4&8)<<1 | (i5&8)<<2 | (i6&8)<<3 | (i7&8)<<4 );
        __m512d t = _mm512_mask_blend_pd(blendmask, a, b);
        if (dozero) {
            t = _mm512_maskz_mov_pd(z, t);
        }
        return t;
    }
    // special case: all data stay within their lane
    if (((m1 ^ 0x66442200) & 0x66666666 & mz) == 0) {

        // mask for elements from a and b
        const uint32_t mb = ((i0&8)?0xF:0) | ((i1&8)?0xF0:0) | ((i2&8)?0xF00:0) | ((i3&8)?0xF000:0) | ((i4&8)?0xF0000:0) | ((i5&8)?0xF00000:0) | ((i6&8)?0xF000000:0) | ((i7&8)?0xF0000000:0);
        const uint32_t mbz = mb & mz;     // mask for nonzero elements from b
        const uint32_t maz = ~mb & mz;    // mask for nonzero elements from a
        const uint32_t m1a = m1 & maz;
        const uint32_t m1b = m1 & mbz;
        const uint32_t pata = ((m1a | m1a >> 8 | m1a >> 16 | m1a >> 24) & 0xFF) * 0x01010101;  // permute pattern for elements from a
        const uint32_t patb = ((m1b | m1b >> 8 | m1b >> 16 | m1b >> 24) & 0xFF) * 0x01010101;  // permute pattern for elements from b

        if (((m1 ^ pata) & 0x11111111 & maz) == 0 && ((m1 ^ patb) & 0x11111111 & mbz) == 0) {
            // Same permute pattern in all lanes:
            // todo!!: make special case for PSHUFD

            // This code generates two instructions instead of one, but we are avoiding the slow lane-crossing instruction,
            // and we are saving 64 bytes of data cache.
            // 1. Permute a, zero elements not from a (using _mm512_maskz_shuffle_epi32)
            __m512d ta = permute8d< (maz&0xF)?i0&7:-1, (maz&0xF0)?i1&7:-1, (maz&0xF00)?i2&7:-1, (maz&0xF000)?i3&7:-1, 
                (maz&0xF0000)?i4&7:-1, (maz&0xF00000)?i5&7:-1, (maz&0xF000000)?i6&7:-1, (maz&0xF0000000)?i7&7:-1> (a);
            // write mask for elements from b
            const __mmask16 sb = ((mbz&0xF)?3:0) | ((mbz&0xF0)?0xC:0) | ((mbz&0xF00)?0x30:0) | ((mbz&0xF000)?0xC0:0) | ((mbz&0xF0000)?0x300:0) | ((mbz&0xF00000)?0xC00:0) | ((mbz&0xF000000)?0x3000:0) | ((mbz&0xF0000000)?0xC000:0);
            // permute index for elements from b
            const int pi = ((patb & 1) * 10 + 4) | ((((patb >> 4) & 1) * 10 + 4) << 4);
            // 2. Permute elements from b and combine with elements from a through write mask
            return _mm512_castsi512_pd(_mm512_mask_shuffle_epi32(_mm512_castpd_si512(ta), sb, _mm512_castpd_si512(b), (_MM_PERM_ENUM)pi));
        }
        // not same permute pattern in all lanes. use full permute
    }
    // general case: full permute
    const __m512i pmask = constant16i<i0&0xF, 0, i1&0xF, 0, i2&0xF, 0, i3&0xF, 0, i4&0xF, 0, i5&0xF, 0, i6&0xF, 0, i7&0xF, 0>();
    if (dozero) {
        return _mm512_maskz_permutex2var_pd(z, a, pmask, b);
    }
    else {
        return _mm512_permutex2var_pd(a, pmask, b);
    }
}


template <int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15 > 
static inline Vec16f blend16f(Vec16f const & a, Vec16f const & b) {  

    // Combine indexes into a single bitfield, with 4 bits for each indicating shuffle, but not source
    const uint64_t m1 = (i0&0xF) | (i1&0xF)<<4 | (i2&0xF)<<8 | (i3&0xF)<<12 | (i4&0xF)<<16 | (i5&0xF)<<20 | (i6&0xF)<<24 | (i7&0xFLL)<<28
        | (i8&0xFLL)<<32 | (i9&0xFLL)<<36 | (i10&0xFLL)<<40 | (i11&0xFLL)<<44 | (i12&0xFLL)<<48 | (i13&0xFLL)<<52 | (i14&0xFLL)<<56 | (i15&0xFLL)<<60;

    // Mask to zero out negative indexes
    const uint64_t mz = (i0<0?0:0xF) | (i1<0?0:0xF0) | (i2<0?0:0xF00) | (i3<0?0:0xF000) | (i4<0?0:0xF0000) | (i5<0?0:0xF00000) | (i6<0?0:0xF000000) | (i7<0?0:0xF0000000ULL)
        | (i8<0?0:0xF00000000) | (i9<0?0:0xF000000000) | (i10<0?0:0xF0000000000) | (i11<0?0:0xF00000000000) | (i12<0?0:0xF000000000000) | (i13<0?0:0xF0000000000000) | (i14<0?0:0xF00000000000000) | (i15<0?0:0xF000000000000000);
    const uint64_t m2 = m1 & mz;

    // collect bit 4 of each index = select source
    const uint64_t ms = ((i0&16)?0xF:0) | ((i1&16)?0xF0:0) | ((i2&16)?0xF00:0) | ((i3&16)?0xF000:0) | ((i4&16)?0xF0000:0) | ((i5&16)?0xF00000:0) | ((i6&16)?0xF000000:0) | ((i7&16)?0xF0000000ULL:0)
        | ((i8&16)?0xF00000000:0) | ((i9&16)?0xF000000000:0) | ((i10&16)?0xF0000000000:0) | ((i11&16)?0xF00000000000:0) | ((i12&16)?0xF000000000000:0) | ((i13&16)?0xF0000000000000:0) | ((i14&16)?0xF00000000000000:0) | ((i15&16)?0xF000000000000000:0);

    // zeroing needed
    const bool dozero = ((i0|i1|i2|i3|i4|i5|i6|i7|i8|i9|i10|i11|i12|i13|i14|i15) & 0x80) != 0;

    // mask for elements not zeroed
    const __mmask16 z = __mmask16((i0>=0)<<0 | (i1>=0)<<1 | (i2>=0)<<2 | (i3>=0)<<3 | (i4>=0)<<4 | (i5>=0)<<5 | (i6>=0)<<6 | (i7>=0)<<7 
        | (i8>=0)<<8 | (i9>=0)<<9 | (i10>=0)<<10 | (i11>=0)<<11 | (i12>=0)<<12 | (i13>=0)<<13 | (i14>=0)<<14 | (i15>=0)<<15);

    // special case: all zero
    if (mz == 0) return  _mm512_setzero_ps();

    // special case: all from a
    if ((ms & mz) == 0) {
        return permute16f<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15> (a);
    }

    // special case: all from b
    if ((~ms & mz) == 0) {
        return permute16f<i0^16,i1^16,i2^16,i3^16,i4^16,i5^16,i6^16,i7^16,i8^16,i9^16,i10^16,i11^16,i12^16,i13^16,i14^16,i15^16 > (b);
    }

    // special case: blend without permute
    if (((m1 ^ 0xFEDCBA9876543210) & mz) == 0) {
        __mmask16 blendmask = __mmask16((i0&16)>>4 | (i1&16)>>3 | (i2&16)>>2 | (i3&16)>>1 | (i4&16) | (i5&16)<<1 | (i6&16)<<2 | (i7&16)<<3
            | (i8&16)<<4 | (i9&16)<<5 | (i10&16)<<6 | (i11&16)<<7 | (i12&16)<<8 | (i13&16)<<9 | (i14&16)<<10 | (i15&16)<<11);
        __m512 t = _mm512_mask_blend_ps(blendmask, a, b);
        if (dozero) {
            t = _mm512_maskz_mov_ps(z, t);
        }
        return t;
    }

    // special case: all data stay within their lane
    if (((m1 ^ 0xCCCC888844440000) & 0xCCCCCCCCCCCCCCCC & mz) == 0) {

        // mask for elements from a and b
        const uint64_t mb  = ms;
        const uint64_t mbz = mb & mz;     // mask for nonzero elements from b
        const uint64_t maz = ~mb & mz;    // mask for nonzero elements from a
        const uint64_t m1a = m1 & maz;
        const uint64_t m1b = m1 & mbz;
        const uint64_t pata = ((m1a | m1a >> 16 | m1a >> 32 | m1a >> 48) & 0xFFFF) * 0x0001000100010001;  // permute pattern for elements from a
        const uint64_t patb = ((m1b | m1b >> 16 | m1b >> 32 | m1b >> 48) & 0xFFFF) * 0x0001000100010001;  // permute pattern for elements from b

        if (((m1 ^ pata) & 0x3333333333333333 & maz) == 0 && ((m1 ^ patb) & 0x3333333333333333 & mbz) == 0) {
            // Same permute pattern in all lanes:
            // todo!!: special case for SHUFPS

            // This code generates two instructions instead of one, but we are avoiding the slow lane-crossing instruction,
            // and we are saving 64 bytes of data cache.
            // 1. Permute a, zero elements not from a (using _mm512_maskz_shuffle_epi32)
            __m512 ta = permute16f< (maz&0xF)?i0&15:-1, (maz&0xF0)?i1&15:-1, (maz&0xF00)?i2&15:-1, (maz&0xF000)?i3&15:-1, 
                (maz&0xF0000)?i4&15:-1, (maz&0xF00000)?i5&15:-1, (maz&0xF000000)?i6&15:-1, (maz&0xF0000000)?i7&15:-1,
                (maz&0xF00000000)?i8&15:-1, (maz&0xF000000000)?i9&15:-1, (maz&0xF0000000000)?i10&15:-1, (maz&0xF00000000000)?i11&15:-1, 
                (maz&0xF000000000000)?i12&15:-1, (maz&0xF0000000000000)?i13&15:-1, (maz&0xF00000000000000)?i14&15:-1, (maz&0xF000000000000000)?i15&15:-1> (a);
            // write mask for elements from b
            const __mmask16 sb = ((mbz&0xF)?1:0) | ((mbz&0xF0)?0x2:0) | ((mbz&0xF00)?0x4:0) | ((mbz&0xF000)?0x8:0) | ((mbz&0xF0000)?0x10:0) | ((mbz&0xF00000)?0x20:0) | ((mbz&0xF000000)?0x40:0) | ((mbz&0xF0000000)?0x80:0) 
                | ((mbz&0xF00000000)?0x100:0) | ((mbz&0xF000000000)?0x200:0) | ((mbz&0xF0000000000)?0x400:0) | ((mbz&0xF00000000000)?0x800:0) | ((mbz&0xF000000000000)?0x1000:0) | ((mbz&0xF0000000000000)?0x2000:0) | ((mbz&0xF00000000000000)?0x4000:0) | ((mbz&0xF000000000000000)?0x8000:0);
            // permute index for elements from b
            const int pi = (patb & 3) | (((patb >> 4) & 3) << 2) | (((patb >> 8) & 3) << 4) | (((patb >> 12) & 3) << 6);
            // 2. Permute elements from b and combine with elements from a through write mask
            return _mm512_castsi512_ps(_mm512_mask_shuffle_epi32(_mm512_castps_si512(ta), sb, _mm512_castps_si512(b), (_MM_PERM_ENUM)pi));
        }
        // not same permute pattern in all lanes. use full permute
    }

    // general case: full permute
    const __m512i pmask = constant16i<i0&0x1F, i1&0x1F, i2&0x1F, i3&0x1F, i4&0x1F, i5&0x1F, i6&0x1F, i7&0x1F, 
        i8&0x1F, i9&0x1F, i10&0x1F, i11&0x1F, i12&0x1F, i13&0x1F, i14&0x1F, i15&0x1F>();
    if (dozero) {
        return _mm512_maskz_permutex2var_ps(z, a, pmask, b);        
    }
    else {
        return _mm512_permutex2var_ps(a, pmask, b);
    }
}


/*****************************************************************************
*
*          Vector lookup functions
*
******************************************************************************
*
* These functions use vector elements as indexes into a table.
* The table is given as one or more vectors or as an array.
*
* This can be used for several purposes:
*  - table lookup
*  - permute or blend with variable indexes
*  - blend from more than two sources
*  - gather non-contiguous data
*
* An index out of range may produce any value - the actual value produced is
* implementation dependent and may be different for different instruction
* sets. An index out of range does not produce an error message or exception.
*
* Example:
* Vec8d a(2,0,0,6,4,3,5,0);                 // index a is (  2,   0,   0,   6,   4,   3,   5,   0)
* Vec8d b(100,101,102,103,104,105,106,107); // table b is (100, 101, 102, 103, 104, 105, 106, 107)
* Vec8d c;
* c = lookup8 (a,b);                        // c is       (102, 100, 100, 106, 104, 103, 105, 100)
*
*****************************************************************************/

static inline Vec16f lookup16(Vec16i const & index, Vec16f const & table) {
    return _mm512_permutexvar_ps(index, table);
}

template <int n>
static inline Vec16f lookup(Vec16i const & index, float const * table) {
    if (n <= 0) return 0;
    if (n <= 16) {
        Vec16f table1 = Vec16f().load((float*)table);
        return lookup16(index, table1);
    }
    if (n <= 32) {
        Vec16f table1 = Vec16f().load((float*)table);
        Vec16f table2 = Vec16f().load((float*)table + 16);
        return _mm512_permutex2var_ps(table1, index, table2);
    }
    // n > 32. Limit index
    Vec16ui index1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec16ui(index) & (n-1);
    }
    else {
        // n is not a power of 2, limit to n-1
        index1 = min(Vec16ui(index), uint32_t(n-1));
    }
    return _mm512_i32gather_ps(index1, (const float*)table, 4);
}


static inline Vec8d lookup8(Vec8q const & index, Vec8d const & table) {
    return _mm512_permutexvar_pd(index, table);
}

template <int n>
static inline Vec8d lookup(Vec8q const & index, double const * table) {
    if (n <= 0) return 0;
    if (n <= 8) {
        Vec8d table1 = Vec8d().load((double*)table);
        return lookup8(index, table1);
    }
    if (n <= 16) {
        Vec8d table1 = Vec8d().load((double*)table);
        Vec8d table2 = Vec8d().load((double*)table + 8);
        return _mm512_permutex2var_pd(table1, index, table2);
    }
    // n > 16. Limit index
    Vec8uq index1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec8uq(index) & (n-1);
    }
    else {
        // n is not a power of 2, limit to n-1
        index1 = min(Vec8uq(index), uint32_t(n-1));
    }
    return _mm512_i64gather_pd(index1, (const double*)table, 8);
}


/*****************************************************************************
*
*          Gather functions with fixed indexes
*
*****************************************************************************/
// Load elements from array a with indices i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7, 
int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
static inline Vec16f gather16f(void const * a) {
    Static_error_check<(i0|i1|i2|i3|i4|i5|i6|i7|i8|i9|i10|i11|i12|i13|i14|i15)>=0> Negative_array_index;  // Error message if index is negative
    // find smallest and biggest index, using only compile-time constant expressions
    const int i01min   = i0  < i1  ? i0  : i1;
    const int i23min   = i2  < i3  ? i2  : i3;
    const int i45min   = i4  < i5  ? i4  : i5;
    const int i67min   = i6  < i7  ? i6  : i7;
    const int i89min   = i8  < i9  ? i8  : i9;
    const int i1011min = i10 < i11 ? i10 : i11;
    const int i1213min = i12 < i13 ? i12 : i13;
    const int i1415min = i14 < i15 ? i14 : i15;
    const int i0_3min   = i01min   < i23min    ? i01min   : i23min;
    const int i4_7min   = i45min   < i67min    ? i45min   : i67min;
    const int i8_11min  = i89min   < i1011min  ? i89min   : i1011min;
    const int i12_15min = i1213min < i1415min  ? i1213min : i1415min;
    const int i0_7min   = i0_3min  < i4_7min   ? i0_3min  : i4_7min;
    const int i8_15min  = i8_11min < i12_15min ? i8_11min : i12_15min;
    const int imin      = i0_7min  < i8_15min  ? i0_7min  : i8_15min;
    const int i01max   = i0  > i1  ? i0  : i1;
    const int i23max   = i2  > i3  ? i2  : i3;
    const int i45max   = i4  > i5  ? i4  : i5;
    const int i67max   = i6  > i7  ? i6  : i7;
    const int i89max   = i8  > i9  ? i8  : i9;
    const int i1011max = i10 > i11 ? i10 : i11;
    const int i1213max = i12 > i13 ? i12 : i13;
    const int i1415max = i14 > i15 ? i14 : i15;
    const int i0_3max   = i01max   > i23max    ? i01max   : i23max;
    const int i4_7max   = i45max   > i67max    ? i45max   : i67max;
    const int i8_11max  = i89max   > i1011max  ? i89max   : i1011max;
    const int i12_15max = i1213max > i1415max  ? i1213max : i1415max;
    const int i0_7max   = i0_3max  > i4_7max   ? i0_3max  : i4_7max;
    const int i8_15max  = i8_11max > i12_15max ? i8_11max : i12_15max;
    const int imax      = i0_7max  > i8_15max  ? i0_7max  : i8_15max;
    if (imax - imin <= 15) {
        // load one contiguous block and permute
        if (imax > 15) {
            // make sure we don't read past the end of the array
            Vec16f b = Vec16f().load((float const *)a + imax-15);
            return permute16f<i0-imax+15, i1-imax+15, i2-imax+15, i3-imax+15, i4-imax+15, i5-imax+15, i6-imax+15, i7-imax+15,
                i8-imax+15, i9-imax+15, i10-imax+15, i11-imax+15, i12-imax+15, i13-imax+15, i14-imax+15, i15-imax+15> (b);
        }
        else {
            Vec16f b = Vec16f().load((float const *)a + imin);
            return permute16f<i0-imin, i1-imin, i2-imin, i3-imin, i4-imin, i5-imin, i6-imin, i7-imin,
                i8-imin, i9-imin, i10-imin, i11-imin, i12-imin, i13-imin, i14-imin, i15-imin> (b);
        }
    }
    if ((i0<imin+16  || i0>imax-16)  && (i1<imin+16  || i1>imax-16)  && (i2<imin+16  || i2>imax-16)  && (i3<imin+16  || i3>imax-16)
    &&  (i4<imin+16  || i4>imax-16)  && (i5<imin+16  || i5>imax-16)  && (i6<imin+16  || i6>imax-16)  && (i7<imin+16  || i7>imax-16)    
    &&  (i8<imin+16  || i8>imax-16)  && (i9<imin+16  || i9>imax-16)  && (i10<imin+16 || i10>imax-16) && (i11<imin+16 || i11>imax-16)
    &&  (i12<imin+16 || i12>imax-16) && (i13<imin+16 || i13>imax-16) && (i14<imin+16 || i14>imax-16) && (i15<imin+16 || i15>imax-16) ) {
        // load two contiguous blocks and blend
        Vec16f b = Vec16f().load((float const *)a + imin);
        Vec16f c = Vec16f().load((float const *)a + imax-15);
        const int j0  = i0 <imin+16 ? i0 -imin : 31-imax+i0;
        const int j1  = i1 <imin+16 ? i1 -imin : 31-imax+i1;
        const int j2  = i2 <imin+16 ? i2 -imin : 31-imax+i2;
        const int j3  = i3 <imin+16 ? i3 -imin : 31-imax+i3;
        const int j4  = i4 <imin+16 ? i4 -imin : 31-imax+i4;
        const int j5  = i5 <imin+16 ? i5 -imin : 31-imax+i5;
        const int j6  = i6 <imin+16 ? i6 -imin : 31-imax+i6;
        const int j7  = i7 <imin+16 ? i7 -imin : 31-imax+i7;
        const int j8  = i8 <imin+16 ? i8 -imin : 31-imax+i8;
        const int j9  = i9 <imin+16 ? i9 -imin : 31-imax+i9;
        const int j10 = i10<imin+16 ? i10-imin : 31-imax+i10;
        const int j11 = i11<imin+16 ? i11-imin : 31-imax+i11;
        const int j12 = i12<imin+16 ? i12-imin : 31-imax+i12;
        const int j13 = i13<imin+16 ? i13-imin : 31-imax+i13;
        const int j14 = i14<imin+16 ? i14-imin : 31-imax+i14;
        const int j15 = i15<imin+16 ? i15-imin : 31-imax+i15;
        return blend16f<j0,j1,j2,j3,j4,j5,j6,j7,j8,j9,j10,j11,j12,j13,j14,j15>(b, c);
    }
    // use gather instruction
    return _mm512_i32gather_ps(Vec16i(i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15), (const float *)a, 4);
}


template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8d gather8d(void const * a) {
    Static_error_check<(i0|i1|i2|i3|i4|i5|i6|i7)>=0> Negative_array_index;  // Error message if index is negative

    const int i01min = i0 < i1 ? i0 : i1;
    const int i23min = i2 < i3 ? i2 : i3;
    const int i45min = i4 < i5 ? i4 : i5;
    const int i67min = i6 < i7 ? i6 : i7;
    const int i0123min = i01min < i23min ? i01min : i23min;
    const int i4567min = i45min < i67min ? i45min : i67min;
    const int imin = i0123min < i4567min ? i0123min : i4567min;
    const int i01max = i0 > i1 ? i0 : i1;
    const int i23max = i2 > i3 ? i2 : i3;
    const int i45max = i4 > i5 ? i4 : i5;
    const int i67max = i6 > i7 ? i6 : i7;
    const int i0123max = i01max > i23max ? i01max : i23max;
    const int i4567max = i45max > i67max ? i45max : i67max;
    const int imax = i0123max > i4567max ? i0123max : i4567max;
    if (imax - imin <= 7) {
        // load one contiguous block and permute
        if (imax > 7) {
            // make sure we don't read past the end of the array
            Vec8d b = Vec8d().load((double const *)a + imax-7);
            return permute8d<i0-imax+7, i1-imax+7, i2-imax+7, i3-imax+7, i4-imax+7, i5-imax+7, i6-imax+7, i7-imax+7> (b);
        }
        else {
            Vec8d b = Vec8d().load((double const *)a + imin);
            return permute8d<i0-imin, i1-imin, i2-imin, i3-imin, i4-imin, i5-imin, i6-imin, i7-imin> (b);
        }
    }
    if ((i0<imin+8 || i0>imax-8) && (i1<imin+8 || i1>imax-8) && (i2<imin+8 || i2>imax-8) && (i3<imin+8 || i3>imax-8)
    &&  (i4<imin+8 || i4>imax-8) && (i5<imin+8 || i5>imax-8) && (i6<imin+8 || i6>imax-8) && (i7<imin+8 || i7>imax-8)) {
        // load two contiguous blocks and blend
        Vec8d b = Vec8d().load((double const *)a + imin);
        Vec8d c = Vec8d().load((double const *)a + imax-7);
        const int j0 = i0<imin+8 ? i0-imin : 15-imax+i0;
        const int j1 = i1<imin+8 ? i1-imin : 15-imax+i1;
        const int j2 = i2<imin+8 ? i2-imin : 15-imax+i2;
        const int j3 = i3<imin+8 ? i3-imin : 15-imax+i3;
        const int j4 = i4<imin+8 ? i4-imin : 15-imax+i4;
        const int j5 = i5<imin+8 ? i5-imin : 15-imax+i5;
        const int j6 = i6<imin+8 ? i6-imin : 15-imax+i6;
        const int j7 = i7<imin+8 ? i7-imin : 15-imax+i7;
        return blend8d<j0, j1, j2, j3, j4, j5, j6, j7>(b, c);
    }
    // use gather instruction
    return _mm512_i64gather_pd(Vec8q(i0,i1,i2,i3,i4,i5,i6,i7), (const double *)a, 8);
}


/*****************************************************************************
*
*          Horizontal scan functions
*
*****************************************************************************/

// Get index to the first element that is true. Return -1 if all are false
static inline int horizontal_find_first(Vec16fb const & x) {
    return horizontal_find_first(Vec16ib(x));
}

static inline int horizontal_find_first(Vec8db const & x) {
    return horizontal_find_first(Vec8qb(x));
}

// Count the number of elements that are true
static inline uint32_t horizontal_count(Vec16fb const & x) {
    return horizontal_count(Vec16ib(x));
}

static inline uint32_t horizontal_count(Vec8db const & x) {
    return horizontal_count(Vec8qb(x));
}

/*****************************************************************************
*
*          Boolean <-> bitfield conversion functions
*
*****************************************************************************/

// to_bits: convert boolean vector to integer bitfield
static inline uint16_t to_bits(Vec16fb x) {
    return to_bits(Vec16ib(x));
}

// to_Vec16fb: convert integer bitfield to boolean vector
static inline Vec16fb to_Vec16fb(uint16_t x) {
    return Vec16fb(to_Vec16ib(x));
}

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec8db x) {
    return to_bits(Vec8qb(x));
}

// to_Vec8db: convert integer bitfield to boolean vector
static inline Vec8db to_Vec8db(uint8_t x) {
    return Vec8db(to_Vec8qb(x));
}

#endif // VECTORF512_H
