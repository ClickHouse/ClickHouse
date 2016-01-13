/****************************  vectorf256.h   *******************************
* Author:        Agner Fog
* Date created:  2012-05-30
* Last modified: 2014-10-22
* Version:       1.16
* Project:       vector classes
* Description:
* Header file defining 256-bit floating point vector classes as interface
* to intrinsic functions in x86 microprocessors with AVX instruction set.
*
* Instructions:
* Use Gnu, Intel or Microsoft C++ compiler. Compile for the desired 
* instruction set, which must be at least AVX.
*
* The following vector classes are defined here:
* Vec8f     Vector of 8 single precision floating point numbers
* Vec8fb    Vector of 8 Booleans for use with Vec8f
* Vec4d     Vector of 4 double precision floating point numbers
* Vec4db    Vector of 4 Booleans for use with Vec4d
*
* Each vector object is represented internally in the CPU as a 256-bit register.
* This header file defines operators and functions for these vectors.
*
* For example:
* Vec4d a(1., 2., 3., 4.), b(5., 6., 7., 8.), c;
* c = a + b;     // now c contains (6., 8., 10., 12.)
*
* For detailed instructions, see VectorClass.pdf
*
* (c) Copyright 2012 - 2014 GNU General Public License http://www.gnu.org/licenses
*****************************************************************************/

// check combination of header files
#if defined (VECTORF256_H)
#if    VECTORF256_H != 2
#error Two different versions of vectorf256.h included
#endif
#else
#define VECTORF256_H  2

#if INSTRSET < 7   // AVX required
#error Please compile for the AVX instruction set or higher
#endif

#include "vectorf128.h"  // Define 128-bit vectors



/*****************************************************************************
*
*          select functions
*
*****************************************************************************/
// Select between two __m256 sources, element by element. Used in various functions 
// and operators. Corresponds to this pseudocode:
// for (int i = 0; i < 8; i++) result[i] = s[i] ? a[i] : b[i];
// Each element in s must be either 0 (false) or 0xFFFFFFFF (true).
static inline __m256 selectf (__m256 const & s, __m256 const & a, __m256 const & b) {
    return _mm256_blendv_ps (b, a, s);
}

// Same, with two __m256d sources.
// and operators. Corresponds to this pseudocode:
// for (int i = 0; i < 4; i++) result[i] = s[i] ? a[i] : b[i];
// Each element in s must be either 0 (false) or 0xFFFFFFFFFFFFFFFF (true). No other 
// values are allowed.
static inline __m256d selectd (__m256d const & s, __m256d const & a, __m256d const & b) {
    return _mm256_blendv_pd (b, a, s);
}



/*****************************************************************************
*
*          Generate compile-time constant vector
*
*****************************************************************************/
// Generate a constant vector of 8 integers stored in memory,
// load as __m256
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline __m256 constant8f() {
    static const union {
        int     i[8];
        __m256  ymm;
    } u = {{i0,i1,i2,i3,i4,i5,i6,i7}};
    return u.ymm;
}


/*****************************************************************************
*
*         Join two 128-bit vectors
*
*****************************************************************************/
#define set_m128r(lo,hi) _mm256_insertf128_ps(_mm256_castps128_ps256(lo),(hi),1)
    // _mm256_set_m128(hi,lo); // not defined in all versions of immintrin.h


/*****************************************************************************
*
*          Vec8fb: Vector of 8 Booleans for use with Vec8f
*
*****************************************************************************/

class Vec8fb {
protected:
    __m256 ymm; // Float vector
public:
    // Default constructor:
    Vec8fb() {
    }
    // Constructor to build from all elements:
    Vec8fb(bool b0, bool b1, bool b2, bool b3, bool b4, bool b5, bool b6, bool b7) {
#if INSTRSET >= 8  // AVX2
        ymm = _mm256_castsi256_ps(_mm256_setr_epi32(-(int)b0, -(int)b1, -(int)b2, -(int)b3, -(int)b4, -(int)b5, -(int)b6, -(int)b7)); 
#else
        __m128 blo = _mm_castsi128_ps(_mm_setr_epi32(-(int)b0, -(int)b1, -(int)b2, -(int)b3));
        __m128 bhi = _mm_castsi128_ps(_mm_setr_epi32(-(int)b4, -(int)b5, -(int)b6, -(int)b7));
        ymm = set_m128r(blo,bhi);
#endif
    }
    // Constructor to build from two Vec4fb:
    Vec8fb(Vec4fb const & a0, Vec4fb const & a1) {
        ymm = set_m128r(a0, a1);
    }
    // Constructor to convert from type __m256 used in intrinsics:
    Vec8fb(__m256 const & x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256 used in intrinsics:
    Vec8fb & operator = (__m256 const & x) {
        ymm = x;
        return *this;
    }
    // Constructor to broadcast the same value into all elements:
    Vec8fb(bool b) {
#if INSTRSET >= 8  // AVX2
        ymm = _mm256_castsi256_ps(_mm256_set1_epi32(-(int)b));
#else
        __m128 b1 = _mm_castsi128_ps(_mm_set1_epi32(-(int)b));
        //ymm = _mm256_set_m128(b1,b1);
        ymm = set_m128r(b1,b1);
#endif
    }
    // Assignment operator to broadcast scalar value:
    Vec8fb & operator = (bool b) {
        *this = Vec8fb(b);
        return *this;
    }
private: // Prevent constructing from int, etc.
    Vec8fb(int b);
    Vec8fb & operator = (int x);
public:
    // Type cast operator to convert to __m256 used in intrinsics
    operator __m256() const {
        return ymm;
    }
#if defined (VECTORI256_H)
#if VECTORI256_H >= 2  // AVX2 version
    // Constructor to convert from type Vec8ib used as Boolean for integer vectors
    Vec8fb(Vec8ib const & x) {
        ymm = _mm256_castsi256_ps(x);
    }
    // Assignment operator to convert from type Vec8ib used as Boolean for integer vectors
    Vec8fb & operator = (Vec8ib const & x) {
        ymm = _mm256_castsi256_ps(x);
        return *this;
    }
#ifndef FIX_CLANG_VECTOR_ALIAS_AMBIGUITY
    // Type cast operator to convert to type Vec8ib used as Boolean for integer vectors
    operator Vec8ib() const {
        return _mm256_castps_si256(ymm);
    }
#endif
#else
    // Constructor to convert from type Vec8ib used as Boolean for integer vectors
    Vec8fb(Vec8ib const & x) {
        ymm = set_m128r(_mm_castsi128_ps(x.get_low()), _mm_castsi128_ps(x.get_high()));
    }
    // Assignment operator to convert from type Vec8ib used as Boolean for integer vectors
    Vec8fb & operator = (Vec8ib const & x) {
        ymm = set_m128r(_mm_castsi128_ps(x.get_low()), _mm_castsi128_ps(x.get_high()));
        return *this;
    }
    // Type cast operator to convert to type Vec8ib used as Boolean for integer vectors
    operator Vec8ib() const {
        return Vec8i(_mm_castps_si128(get_low()), _mm_castps_si128(get_high()));
    }
#endif
#endif // VECTORI256_H
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec8fb const & insert(uint32_t index, bool value) {
        static const int32_t maskl[16] = {0,0,0,0,0,0,0,0,-1,0,0,0,0,0,0,0};
        __m256 mask  = _mm256_loadu_ps((float const*)(maskl+8-(index & 7))); // mask with FFFFFFFF at index position
        if (value) {
            ymm = _mm256_or_ps(ymm,mask);
        }
        else {
            ymm = _mm256_andnot_ps(mask,ymm);
        }
        return *this;
    }
    // Member function extract a single element from vector
    bool extract(uint32_t index) const {
        union {
            float   f[8];
            int32_t i[8];
        } u;
        _mm256_storeu_ps(u.f, ymm);
        return u.i[index & 7] != 0;
    }
    // Extract a single element. Operator [] can only read an element, not write.
    bool operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4fb:
    Vec4fb get_low() const {
        return _mm256_castps256_ps128(ymm);
    }
    Vec4fb get_high() const {
        return _mm256_extractf128_ps(ymm,1);
    }
    static int size () {
        return 8;
    }
};


/*****************************************************************************
*
*          Operators for Vec8fb
*
*****************************************************************************/

// vector operator & : bitwise and
static inline Vec8fb operator & (Vec8fb const & a, Vec8fb const & b) {
    return _mm256_and_ps(a, b);
}
static inline Vec8fb operator && (Vec8fb const & a, Vec8fb const & b) {
    return a & b;
}

// vector operator &= : bitwise and
static inline Vec8fb & operator &= (Vec8fb & a, Vec8fb const & b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec8fb operator | (Vec8fb const & a, Vec8fb const & b) {
    return _mm256_or_ps(a, b);
}
static inline Vec8fb operator || (Vec8fb const & a, Vec8fb const & b) {
    return a | b;
}

// vector operator |= : bitwise or
static inline Vec8fb & operator |= (Vec8fb & a, Vec8fb const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec8fb operator ^ (Vec8fb const & a, Vec8fb const & b) {
    return _mm256_xor_ps(a, b);
}

// vector operator ^= : bitwise xor
static inline Vec8fb & operator ^= (Vec8fb & a, Vec8fb const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec8fb operator ~ (Vec8fb const & a) {
    return _mm256_xor_ps(a, constant8f<-1,-1,-1,-1,-1,-1,-1,-1>());
}

// vector operator ! : logical not
// (operator ! is less efficient than operator ~. Use only where not
// all bits in an element are the same)
static inline Vec8fb operator ! (Vec8fb const & a) {
return Vec8fb( !Vec8ib(a));
}

// Functions for Vec8fb

// andnot: a & ~ b
static inline Vec8fb andnot(Vec8fb const & a, Vec8fb const & b) {
    return _mm256_andnot_ps(b, a);
}



/*****************************************************************************
*
*          Horizontal Boolean functions
*
*****************************************************************************/

// horizontal_and. Returns true if all bits are 1
static inline bool horizontal_and (Vec8fb const & a) {
    return _mm256_testc_ps(a,constant8f<-1,-1,-1,-1,-1,-1,-1,-1>()) != 0;
}

// horizontal_or. Returns true if at least one bit is 1
static inline bool horizontal_or (Vec8fb const & a) {
    return ! _mm256_testz_ps(a,a);
}


/*****************************************************************************
*
*          Vec4db: Vector of 4 Booleans for use with Vec4d
*
*****************************************************************************/

class Vec4db {
protected:
    __m256d ymm; // double vector
public:
    // Default constructor:
    Vec4db() {
    }
    // Constructor to build from all elements:
    Vec4db(bool b0, bool b1, bool b2, bool b3) {
#if INSTRSET >= 8  // AVX2
        ymm = _mm256_castsi256_pd(_mm256_setr_epi64x(-(int64_t)b0, -(int64_t)b1, -(int64_t)b2, -(int64_t)b3)); 
#else
        __m128 blo = _mm_castsi128_ps(_mm_setr_epi32(-(int)b0, -(int)b0, -(int)b1, -(int)b1));
        __m128 bhi = _mm_castsi128_ps(_mm_setr_epi32(-(int)b2, -(int)b2, -(int)b3, -(int)b3));
        ymm = _mm256_castps_pd(set_m128r(bhi,blo));
#endif
    }
    // Constructor to build from two Vec2db:
    Vec4db(Vec2db const & a0, Vec2db const & a1) {
        ymm = _mm256_castps_pd(set_m128r(_mm_castpd_ps(a0),_mm_castpd_ps(a1)));
        //ymm = _mm256_set_m128d(a1, a0);
    }
    // Constructor to convert from type __m256d used in intrinsics:
    Vec4db(__m256d const & x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256d used in intrinsics:
    Vec4db & operator = (__m256d const & x) {
        ymm = x;
        return *this;
    }
    // Constructor to broadcast the same value into all elements:
    Vec4db(bool b) {
#if INSTRSET >= 8  // AVX2
        ymm = _mm256_castsi256_pd(_mm256_set1_epi64x(-(int64_t)b));
#else
        __m128 b1 = _mm_castsi128_ps(_mm_set1_epi32(-(int)b));
        ymm = _mm256_castps_pd(set_m128r(b1,b1));
#endif
    }
    // Assignment operator to broadcast scalar value:
    Vec4db & operator = (bool b) {
        ymm = _mm256_castsi256_pd(_mm256_set1_epi32(-int32_t(b)));
        return *this;
    }
private: // Prevent constructing from int, etc.
    Vec4db(int b);
    Vec4db & operator = (int x);
public:
    // Type cast operator to convert to __m256d used in intrinsics
    operator __m256d() const {
        return ymm;
    }
#ifdef VECTORI256_H  
#if VECTORI256_H == 2  // 256 bit integer vectors are available, AVX2
    // Constructor to convert from type Vec4qb used as Boolean for integer vectors
    Vec4db(Vec4qb const & x) {
        ymm = _mm256_castsi256_pd(x);
    }
    // Assignment operator to convert from type Vec4qb used as Boolean for integer vectors
    Vec4db & operator = (Vec4qb const & x) {
        ymm = _mm256_castsi256_pd(x);
        return *this;
    }
#ifndef FIX_CLANG_VECTOR_ALIAS_AMBIGUITY
    // Type cast operator to convert to type Vec4qb used as Boolean for integer vectors
    operator Vec4qb() const {
        return _mm256_castpd_si256(ymm);
    }
#endif
#else   // 256 bit integer vectors emulated without AVX2
    // Constructor to convert from type Vec4qb used as Boolean for integer vectors
    Vec4db(Vec4qb const & x) {
        *this = Vec4db(_mm_castsi128_pd(x.get_low()), _mm_castsi128_pd(x.get_high()));
    }
    // Assignment operator to convert from type Vec4qb used as Boolean for integer vectors
    Vec4db & operator = (Vec4qb const & x) {
        *this = Vec4db(_mm_castsi128_pd(x.get_low()), _mm_castsi128_pd(x.get_high()));
        return *this;
    }
    // Type cast operator to convert to type Vec4qb used as Boolean for integer vectors
    operator Vec4qb() const {
        return Vec4q(_mm_castpd_si128(get_low()), _mm_castpd_si128(get_high()));
    }
#endif
#endif // VECTORI256_H
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec4db const & insert(uint32_t index, bool value) {
        static const int32_t maskl[16] = {0,0,0,0,0,0,0,0,-1,-1,0,0,0,0,0,0};
        __m256d mask = _mm256_loadu_pd((double const*)(maskl+8-(index&3)*2)); // mask with FFFFFFFFFFFFFFFF at index position
        if (value) {
            ymm = _mm256_or_pd(ymm,mask);
        }
        else {
            ymm = _mm256_andnot_pd(mask,ymm);
        }
        return *this;
    }
    // Member function extract a single element from vector
    bool extract(uint32_t index) const {
        union {
            double  f[8];
            int32_t i[16];
        } u;
        _mm256_storeu_pd(u.f, ymm);
        return u.i[(index & 3) * 2 + 1] != 0;
    }
    // Extract a single element. Operator [] can only read an element, not write.
    bool operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4fb:
    Vec2db get_low() const {
        return _mm256_castpd256_pd128(ymm);
    }
    Vec2db get_high() const {
        return _mm256_extractf128_pd(ymm,1);
    }
    static int size () {
        return 4;
    }
};


/*****************************************************************************
*
*          Operators for Vec4db
*
*****************************************************************************/

// vector operator & : bitwise and
static inline Vec4db operator & (Vec4db const & a, Vec4db const & b) {
    return _mm256_and_pd(a, b);
}
static inline Vec4db operator && (Vec4db const & a, Vec4db const & b) {
    return a & b;
}

// vector operator &= : bitwise and
static inline Vec4db & operator &= (Vec4db & a, Vec4db const & b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec4db operator | (Vec4db const & a, Vec4db const & b) {
    return _mm256_or_pd(a, b);
}
static inline Vec4db operator || (Vec4db const & a, Vec4db const & b) {
    return a | b;
}

// vector operator |= : bitwise or
static inline Vec4db & operator |= (Vec4db & a, Vec4db const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec4db operator ^ (Vec4db const & a, Vec4db const & b) {
    return _mm256_xor_pd(a, b);
}

// vector operator ^= : bitwise xor
static inline Vec4db & operator ^= (Vec4db & a, Vec4db const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec4db operator ~ (Vec4db const & a) {
    return _mm256_xor_pd(a, _mm256_castps_pd (constant8f<-1,-1,-1,-1,-1,-1,-1,-1>()));
}

// vector operator ! : logical not
// (operator ! is less efficient than operator ~. Use only where not
// all bits in an element are the same)
static inline Vec4db operator ! (Vec4db const & a) {
return Vec4db( ! Vec4qb(a));
}

// Functions for Vec8fb

// andnot: a & ~ b
static inline Vec4db andnot(Vec4db const & a, Vec4db const & b) {
    return _mm256_andnot_pd(b, a);
}


/*****************************************************************************
*
*          Horizontal Boolean functions
*
*****************************************************************************/

// horizontal_and. Returns true if all bits are 1
static inline bool horizontal_and (Vec4db const & a) {
#if defined (VECTORI256_H) && VECTORI256_H > 1  // 256 bit integer vectors are available, AVX2
    return horizontal_and(Vec256b(_mm256_castpd_si256(a)));
#else  // split into 128 bit vectors
    return horizontal_and(a.get_low() & a.get_high());
#endif
}

// horizontal_or. Returns true if at least one bit is 1
static inline bool horizontal_or (Vec4db const & a) {
#if defined (VECTORI256_H) && VECTORI256_H > 1  // 256 bit integer vectors are available, AVX2
    return horizontal_or(Vec256b(_mm256_castpd_si256(a)));
#else  // split into 128 bit vectors
    return horizontal_or(a.get_low() | a.get_high());
#endif
}


 /*****************************************************************************
*
*          Vec8f: Vector of 8 single precision floating point values
*
*****************************************************************************/

class Vec8f {
protected:
    __m256 ymm; // Float vector
public:
    // Default constructor:
    Vec8f() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec8f(float f) {
        ymm = _mm256_set1_ps(f);
    }
    // Constructor to build from all elements:
    Vec8f(float f0, float f1, float f2, float f3, float f4, float f5, float f6, float f7) {
        ymm = _mm256_setr_ps(f0, f1, f2, f3, f4, f5, f6, f7); 
    }
    // Constructor to build from two Vec4f:
    Vec8f(Vec4f const & a0, Vec4f const & a1) {
        ymm = set_m128r(a0, a1);
        //ymm = _mm256_set_m128(a1, a0);
    }
    // Constructor to convert from type __m256 used in intrinsics:
    Vec8f(__m256 const & x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256 used in intrinsics:
    Vec8f & operator = (__m256 const & x) {
        ymm = x;
        return *this;
    }
    // Type cast operator to convert to __m256 used in intrinsics
    operator __m256() const {
        return ymm;
    }
    // Member function to load from array (unaligned)
    Vec8f & load(float const * p) {
        ymm = _mm256_loadu_ps(p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    // You may use load_a instead of load if you are certain that p points to an address
    // divisible by 32.
    Vec8f & load_a(float const * p) {
        ymm = _mm256_load_ps(p);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(float * p) const {
        _mm256_storeu_ps(p, ymm);
    }
    // Member function to store into array, aligned by 32
    // You may use store_a instead of store if you are certain that p points to an address
    // divisible by 32.
    void store_a(float * p) const {
        _mm256_store_ps(p, ymm);
    }
    // Partial load. Load n elements and set the rest to 0
    Vec8f & load_partial(int n, float const * p) {
        if (n > 0 && n <= 4) {
            *this = Vec8f(Vec4f().load_partial(n, p), _mm_setzero_ps());
            // ymm = _mm256_castps128_ps256(Vec4f().load_partial<n>(p)); (this doesn't work on MS compiler due to sloppy definition of the cast)
        }
        else if (n > 4 && n <= 8) {
            *this = Vec8f(Vec4f().load(p), Vec4f().load_partial(n - 4, p + 4));
        }
        else {
            ymm = _mm256_setzero_ps();
        }
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, float * p) const {
        if (n <= 4) {
            get_low().store_partial(n, p);
        }
        else if (n <= 8) {
            get_low().store(p);
            get_high().store_partial(n - 4, p + 4);
        }
    }
    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec8f & cutoff(int n) {
        if (uint32_t(n) >= 8) return *this;
        static const union {        
            int32_t i[16];
            float   f[16];
        } mask = {{-1,-1,-1,-1,-1,-1,-1,-1,0,0,0,0,0,0,0,0}};
        *this = Vec8fb(*this) & Vec8fb(Vec8f().load(mask.f + 8 - n));
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec8f const & insert(uint32_t index, float value) {
        __m256 v0 = _mm256_broadcast_ss(&value);
        switch (index) {
        case 0:
            ymm = _mm256_blend_ps (ymm, v0, 1);  break;
        case 1:
            ymm = _mm256_blend_ps (ymm, v0, 2);  break;
        case 2:
            ymm = _mm256_blend_ps (ymm, v0, 4);  break;
        case 3:
            ymm = _mm256_blend_ps (ymm, v0, 8);  break;
        case 4:
            ymm = _mm256_blend_ps (ymm, v0, 0x10);  break;
        case 5:
            ymm = _mm256_blend_ps (ymm, v0, 0x20);  break;
        case 6:
            ymm = _mm256_blend_ps (ymm, v0, 0x40);  break;
        default:
            ymm = _mm256_blend_ps (ymm, v0, 0x80);  break;
        }
        return *this;
    }
    // Member function extract a single element from vector
    float extract(uint32_t index) const {
        float x[8];
        store(x);
        return x[index & 7];
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    float operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4f:
    Vec4f get_low() const {
        return _mm256_castps256_ps128(ymm);
    }
    Vec4f get_high() const {
        return _mm256_extractf128_ps(ymm,1);
    }
    static int size () {
        return 8;
    }
};


/*****************************************************************************
*
*          Operators for Vec8f
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec8f operator + (Vec8f const & a, Vec8f const & b) {
    return _mm256_add_ps(a, b);
}

// vector operator + : add vector and scalar
static inline Vec8f operator + (Vec8f const & a, float b) {
    return a + Vec8f(b);
}
static inline Vec8f operator + (float a, Vec8f const & b) {
    return Vec8f(a) + b;
}

// vector operator += : add
static inline Vec8f & operator += (Vec8f & a, Vec8f const & b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec8f operator ++ (Vec8f & a, int) {
    Vec8f a0 = a;
    a = a + 1.0f;
    return a0;
}

// prefix operator ++
static inline Vec8f & operator ++ (Vec8f & a) {
    a = a + 1.0f;
    return a;
}

// vector operator - : subtract element by element
static inline Vec8f operator - (Vec8f const & a, Vec8f const & b) {
    return _mm256_sub_ps(a, b);
}

// vector operator - : subtract vector and scalar
static inline Vec8f operator - (Vec8f const & a, float b) {
    return a - Vec8f(b);
}
static inline Vec8f operator - (float a, Vec8f const & b) {
    return Vec8f(a) - b;
}

// vector operator - : unary minus
// Change sign bit, even for 0, INF and NAN
static inline Vec8f operator - (Vec8f const & a) {
    return _mm256_xor_ps(a, constant8f<(int)0x80000000,(int)0x80000000,(int)0x80000000,(int)0x80000000,(int)0x80000000,(int)0x80000000,(int)0x80000000,(int)0x80000000> ());
}

// vector operator -= : subtract
static inline Vec8f & operator -= (Vec8f & a, Vec8f const & b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec8f operator -- (Vec8f & a, int) {
    Vec8f a0 = a;
    a = a - 1.0f;
    return a0;
}

// prefix operator --
static inline Vec8f & operator -- (Vec8f & a) {
    a = a - 1.0f;
    return a;
}

// vector operator * : multiply element by element
static inline Vec8f operator * (Vec8f const & a, Vec8f const & b) {
    return _mm256_mul_ps(a, b);
}

// vector operator * : multiply vector and scalar
static inline Vec8f operator * (Vec8f const & a, float b) {
    return a * Vec8f(b);
}
static inline Vec8f operator * (float a, Vec8f const & b) {
    return Vec8f(a) * b;
}

// vector operator *= : multiply
static inline Vec8f & operator *= (Vec8f & a, Vec8f const & b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer
static inline Vec8f operator / (Vec8f const & a, Vec8f const & b) {
    return _mm256_div_ps(a, b);
}

// vector operator / : divide vector and scalar
static inline Vec8f operator / (Vec8f const & a, float b) {
    return a / Vec8f(b);
}
static inline Vec8f operator / (float a, Vec8f const & b) {
    return Vec8f(a) / b;
}

// vector operator /= : divide
static inline Vec8f & operator /= (Vec8f & a, Vec8f const & b) {
    a = a / b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec8fb operator == (Vec8f const & a, Vec8f const & b) {
    return _mm256_cmp_ps(a, b, 0);
}

// vector operator != : returns true for elements for which a != b
static inline Vec8fb operator != (Vec8f const & a, Vec8f const & b) {
    return _mm256_cmp_ps(a, b, 4);
}

// vector operator < : returns true for elements for which a < b
static inline Vec8fb operator < (Vec8f const & a, Vec8f const & b) {
    return _mm256_cmp_ps(a, b, 1);
}

// vector operator <= : returns true for elements for which a <= b
static inline Vec8fb operator <= (Vec8f const & a, Vec8f const & b) {
    return _mm256_cmp_ps(a, b, 2);
}

// vector operator > : returns true for elements for which a > b
static inline Vec8fb operator > (Vec8f const & a, Vec8f const & b) {
    return b < a;
}

// vector operator >= : returns true for elements for which a >= b
static inline Vec8fb operator >= (Vec8f const & a, Vec8f const & b) {
    return b <= a;
}

// Bitwise logical operators

// vector operator & : bitwise and
static inline Vec8f operator & (Vec8f const & a, Vec8f const & b) {
    return _mm256_and_ps(a, b);
}

// vector operator &= : bitwise and
static inline Vec8f & operator &= (Vec8f & a, Vec8f const & b) {
    a = a & b;
    return a;
}

// vector operator & : bitwise and of Vec8f and Vec8fb
static inline Vec8f operator & (Vec8f const & a, Vec8fb const & b) {
    return _mm256_and_ps(a, b);
}
static inline Vec8f operator & (Vec8fb const & a, Vec8f const & b) {
    return _mm256_and_ps(a, b);
}

// vector operator | : bitwise or
static inline Vec8f operator | (Vec8f const & a, Vec8f const & b) {
    return _mm256_or_ps(a, b);
}

// vector operator |= : bitwise or
static inline Vec8f & operator |= (Vec8f & a, Vec8f const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec8f operator ^ (Vec8f const & a, Vec8f const & b) {
    return _mm256_xor_ps(a, b);
}

// vector operator ^= : bitwise xor
static inline Vec8f & operator ^= (Vec8f & a, Vec8f const & b) {
    a = a ^ b;
    return a;
}

// vector operator ! : logical not. Returns Boolean vector
static inline Vec8fb operator ! (Vec8f const & a) {
    return a == Vec8f(0.0f);
}


/*****************************************************************************
*
*          Functions for Vec8f
*
*****************************************************************************/

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 8; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or 0xFFFFFFFF (true). No other values are allowed.
static inline Vec8f select (Vec8fb const & s, Vec8f const & a, Vec8f const & b) {
    return _mm256_blendv_ps (b, a, s);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec8f if_add (Vec8fb const & f, Vec8f const & a, Vec8f const & b) {
    return a + (Vec8f(f) & b);
}

// Conditional multiply: For all vector elements i: result[i] = f[i] ? (a[i] * b[i]) : a[i]
static inline Vec8f if_mul (Vec8fb const & f, Vec8f const & a, Vec8f const & b) {
    return a * select(f, b, 1.f);
}


// General arithmetic functions, etc.

// Horizontal add: Calculates the sum of all vector elements.
static inline float horizontal_add (Vec8f const & a) {
    __m256 t1 = _mm256_hadd_ps(a,a);
    __m256 t2 = _mm256_hadd_ps(t1,t1);
    __m128 t3 = _mm256_extractf128_ps(t2,1);
    __m128 t4 = _mm_add_ss(_mm256_castps256_ps128(t2),t3);
    return _mm_cvtss_f32(t4);        
}

// function max: a > b ? a : b
static inline Vec8f max(Vec8f const & a, Vec8f const & b) {
    return _mm256_max_ps(a,b);
}

// function min: a < b ? a : b
static inline Vec8f min(Vec8f const & a, Vec8f const & b) {
    return _mm256_min_ps(a,b);
}

// function abs: absolute value
// Removes sign bit, even for -0.0f, -INF and -NAN
static inline Vec8f abs(Vec8f const & a) {
    __m256 mask = constant8f<0x7FFFFFFF,0x7FFFFFFF,0x7FFFFFFF,0x7FFFFFFF,0x7FFFFFFF,0x7FFFFFFF,0x7FFFFFFF,0x7FFFFFFF> ();
    return _mm256_and_ps(a,mask);
}

// function sqrt: square root
static inline Vec8f sqrt(Vec8f const & a) {
    return _mm256_sqrt_ps(a);
}

// function square: a * a
static inline Vec8f square(Vec8f const & a) {
    return a * a;
}

// pow(Vec8f, int):
template <typename TT> static Vec8f pow(Vec8f const & a, TT n);

// Raise floating point numbers to integer power n
template <>
inline Vec8f pow<int>(Vec8f const & x0, int n) {
    return pow_template_i<Vec8f>(x0, n);
}

// allow conversion from unsigned int
template <>
inline Vec8f pow<uint32_t>(Vec8f const & x0, uint32_t n) {
    return pow_template_i<Vec8f>(x0, (int)n);
}


// Raise floating point numbers to integer power n, where n is a compile-time constant
template <int n>
static inline Vec8f pow_n(Vec8f const & a) {
    if (n < 0)    return Vec8f(1.0f) / pow_n<-n>(a);
    if (n == 0)   return Vec8f(1.0f);
    if (n >= 256) return pow(a, n);
    Vec8f x = a;                       // a^(2^i)
    Vec8f y;                           // accumulator
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
static inline Vec8f pow(Vec8f const & a, Const_int_t<n>) {
    return pow_n<n>(a);
}


// function round: round to nearest integer (even). (result as float vector)
static inline Vec8f round(Vec8f const & a) {
    return _mm256_round_ps(a, 0);
}

// function truncate: round towards zero. (result as float vector)
static inline Vec8f truncate(Vec8f const & a) {
    return _mm256_round_ps(a, 3);
}

// function floor: round towards minus infinity. (result as float vector)
static inline Vec8f floor(Vec8f const & a) {
    return _mm256_round_ps(a, 1);
}

// function ceil: round towards plus infinity. (result as float vector)
static inline Vec8f ceil(Vec8f const & a) {
    return _mm256_round_ps(a, 2);
}

#ifdef VECTORI256_H  // 256 bit integer vectors are available
#if VECTORI256_H > 1  // AVX2
// function round_to_int: round to nearest integer (even). (result as integer vector)
static inline Vec8i round_to_int(Vec8f const & a) {
    // Note: assume MXCSR control register is set to rounding
    return _mm256_cvtps_epi32(a);
}

// function truncate_to_int: round towards zero. (result as integer vector)
static inline Vec8i truncate_to_int(Vec8f const & a) {
    return _mm256_cvttps_epi32(a);
}

// function to_float: convert integer vector to float vector
static inline Vec8f to_float(Vec8i const & a) {
    return _mm256_cvtepi32_ps(a);
}
#else // no AVX2

// function round_to_int: round to nearest integer (even). (result as integer vector)
static inline Vec8i round_to_int(Vec8f const & a) {
    // Note: assume MXCSR control register is set to rounding
    return Vec8i(_mm_cvtps_epi32(a.get_low()), _mm_cvtps_epi32(a.get_high()));
}

// function truncate_to_int: round towards zero. (result as integer vector)
static inline Vec8i truncate_to_int(Vec8f const & a) {
    return Vec8i(_mm_cvttps_epi32(a.get_low()), _mm_cvttps_epi32(a.get_high()));
}

// function to_float: convert integer vector to float vector
static inline Vec8f to_float(Vec8i const & a) {
    return Vec8f(_mm_cvtepi32_ps(a.get_low()), _mm_cvtepi32_ps(a.get_high()));
}
#endif
#endif // VECTORI256_H


// Fused multiply and add functions

// Multiply and add
static inline Vec8f mul_add(Vec8f const & a, Vec8f const & b, Vec8f const & c) {
#ifdef __FMA__
    return _mm256_fmadd_ps(a, b, c);
#elif defined (__FMA4__)
    return _mm256_macc_ps(a, b, c);
#else
    return a * b + c;
#endif
    
}

// Multiply and subtract
static inline Vec8f mul_sub(Vec8f const & a, Vec8f const & b, Vec8f const & c) {
#ifdef __FMA__
    return _mm256_fmsub_ps(a, b, c);
#elif defined (__FMA4__)
    return _mm256_msub_ps(a, b, c);
#else
    return a * b - c;
#endif    
}

// Multiply and inverse subtract
static inline Vec8f nmul_add(Vec8f const & a, Vec8f const & b, Vec8f const & c) {
#ifdef __FMA__
    return _mm256_fnmadd_ps(a, b, c);
#elif defined (__FMA4__)
    return _mm256_nmacc_ps(a, b, c);
#else
    return c - a * b;
#endif
}


// Multiply and subtract with extra precision on the intermediate calculations, 
// even if FMA instructions not supported, using Veltkamp-Dekker split
static inline Vec8f mul_sub_x(Vec8f const & a, Vec8f const & b, Vec8f const & c) {
#ifdef __FMA__
    return _mm256_fmsub_ps(a, b, c);
#elif defined (__FMA4__)
    return _mm256_msub_ps(a, b, c);
#else
    // calculate a * b - c with extra precision
    const int b12 = -(1 << 12);                  // mask to remove lower 12 bits
    Vec8f upper_mask = constant8f<b12,b12,b12,b12,b12,b12,b12,b12>();
    Vec8f a_high = a & upper_mask;               // split into high and low parts
    Vec8f b_high = b & upper_mask;
    Vec8f a_low  = a - a_high;
    Vec8f b_low  = b - b_high;
    Vec8f r1 = a_high * b_high;                  // this product is exact
    Vec8f r2 = r1 - c;                           // subtract c from high product
    Vec8f r3 = r2 + (a_high * b_low + b_high * a_low) + a_low * b_low; // add rest of product
    return r3; // + ((r2 - r1) + c);
#endif
}


// Approximate math functions

// approximate reciprocal (Faster than 1.f / a. relative accuracy better than 2^-11)
static inline Vec8f approx_recipr(Vec8f const & a) {
    return _mm256_rcp_ps(a);
}

// approximate reciprocal squareroot (Faster than 1.f / sqrt(a). Relative accuracy better than 2^-11)
static inline Vec8f approx_rsqrt(Vec8f const & a) {
    return _mm256_rsqrt_ps(a);
}


// Math functions using fast bit manipulation

#ifdef VECTORI256_H  // 256 bit integer vectors are available, AVX2
// Extract the exponent as an integer
// exponent(a) = floor(log2(abs(a)));
// exponent(1.0f) = 0, exponent(0.0f) = -127, exponent(INF) = +128, exponent(NAN) = +128
static inline Vec8i exponent(Vec8f const & a) {
#if  VECTORI256_H > 1  // AVX2
    Vec8ui t1 = _mm256_castps_si256(a);// reinterpret as 32-bit integer
    Vec8ui t2 = t1 << 1;               // shift out sign bit
    Vec8ui t3 = t2 >> 24;              // shift down logical to position 0
    Vec8i  t4 = Vec8i(t3) - 0x7F;      // subtract bias from exponent
    return t4;
#else  // no AVX2
    return Vec8i(exponent(a.get_low()), exponent(a.get_high()));
#endif
}
#endif

// Extract the fraction part of a floating point number
// a = 2^exponent(a) * fraction(a), except for a = 0
// fraction(1.0f) = 1.0f, fraction(5.0f) = 1.25f 
static inline Vec8f fraction(Vec8f const & a) {
#if defined (VECTORI256_H) && VECTORI256_H > 2  // 256 bit integer vectors are available, AVX2
    Vec8ui t1 = _mm256_castps_si256(a);   // reinterpret as 32-bit integer
    Vec8ui t2 = (t1 & 0x007FFFFF) | 0x3F800000; // set exponent to 0 + bias
    return _mm256_castsi256_ps(t2);
#else
    return Vec8f(fraction(a.get_low()), fraction(a.get_high()));
#endif
}

#ifdef VECTORI256_H  // 256 bit integer vectors are available, AVX2
// Fast calculation of pow(2,n) with n integer
// n  =    0 gives 1.0f
// n >=  128 gives +INF
// n <= -127 gives 0.0f
// This function will never produce denormals, and never raise exceptions
static inline Vec8f exp2(Vec8i const & n) {
#if  VECTORI256_H > 1  // AVX2
    Vec8i t1 = max(n,  -0x7F);         // limit to allowed range
    Vec8i t2 = min(t1,  0x80);
    Vec8i t3 = t2 + 0x7F;              // add bias
    Vec8i t4 = t3 << 23;               // put exponent into position 23
    return _mm256_castsi256_ps(t4);    // reinterpret as float
#else
    return Vec8f(exp2(n.get_low()), exp2(n.get_high()));
#endif
}
//static inline Vec8f exp2(Vec8f const & x); // defined in vectormath_exp.h

#endif // VECTORI256_H


// Categorization functions

// Function sign_bit: gives true for elements that have the sign bit set
// even for -0.0f, -INF and -NAN
// Note that sign_bit(Vec8f(-0.0f)) gives true, while Vec8f(-0.0f) < Vec8f(0.0f) gives false
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec8fb sign_bit(Vec8f const & a) {
#if defined (VECTORI256_H) && VECTORI256_H > 1  // 256 bit integer vectors are available, AVX2
    Vec8i t1 = _mm256_castps_si256(a);    // reinterpret as 32-bit integer
    Vec8i t2 = t1 >> 31;                  // extend sign bit
    return _mm256_castsi256_ps(t2);       // reinterpret as 32-bit Boolean
#else
    return Vec8fb(sign_bit(a.get_low()), sign_bit(a.get_high()));
#endif
}

// Function sign_combine: changes the sign of a when b has the sign bit set
// same as select(sign_bit(b), -a, a)
static inline Vec8f sign_combine(Vec8f const & a, Vec8f const & b) {
    Vec8f signmask = constant8f<(int)0x80000000,(int)0x80000000,(int)0x80000000,(int)0x80000000,(int)0x80000000,(int)0x80000000,(int)0x80000000,(int)0x80000000>();  // -0.0
    return a ^ (b & signmask);
}

// Function is_finite: gives true for elements that are normal, denormal or zero, 
// false for INF and NAN
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec8fb is_finite(Vec8f const & a) {
#if defined (VECTORI256_H) && VECTORI256_H > 1  // 256 bit integer vectors are available, AVX2
    Vec8i t1 = _mm256_castps_si256(a);    // reinterpret as 32-bit integer
    Vec8i t2 = t1 << 1;                // shift out sign bit
    Vec8ib t3 = Vec8i(t2 & 0xFF000000) != 0xFF000000; // exponent field is not all 1s
    return t3;
#else
    return Vec8fb(is_finite(a.get_low()), is_finite(a.get_high()));
#endif
}

// Function is_inf: gives true for elements that are +INF or -INF
// false for finite numbers and NAN
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec8fb is_inf(Vec8f const & a) {
#if defined (VECTORI256_H) && VECTORI256_H > 1  // 256 bit integer vectors are available, AVX2
    Vec8i t1 = _mm256_castps_si256(a); // reinterpret as 32-bit integer
    Vec8i t2 = t1 << 1;                // shift out sign bit
    return t2 == 0xFF000000;           // exponent is all 1s, fraction is 0
#else
    return Vec8fb(is_inf(a.get_low()), is_inf(a.get_high()));
#endif
}

// Function is_nan: gives true for elements that are +NAN or -NAN
// false for finite numbers and +/-INF
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec8fb is_nan(Vec8f const & a) {
#if defined (VECTORI256_H) && VECTORI256_H > 1  // 256 bit integer vectors are available, AVX2
    Vec8i t1 = _mm256_castps_si256(a); // reinterpret as 32-bit integer
    Vec8i t2 = t1 << 1;                // shift out sign bit
    Vec8i t3 = 0xFF000000;             // exponent mask
    Vec8i t4 = t2 & t3;                // exponent
    Vec8i t5 = _mm256_andnot_si256(t3,t2);// fraction
    return Vec8ib(t4 == t3 && t5 != 0);// exponent = all 1s and fraction != 0
#else
    return Vec8fb(is_nan(a.get_low()), is_nan(a.get_high()));
#endif
}

// Function is_subnormal: gives true for elements that are denormal (subnormal)
// false for finite numbers, zero, NAN and INF
static inline Vec8fb is_subnormal(Vec8f const & a) {
#if defined (VECTORI256_H) && VECTORI256_H > 1  // 256 bit integer vectors are available, AVX2
    Vec8i t1 = _mm256_castps_si256(a);    // reinterpret as 32-bit integer
    Vec8i t2 = t1 << 1;                   // shift out sign bit
    Vec8i t3 = 0xFF000000;                // exponent mask
    Vec8i t4 = t2 & t3;                   // exponent
    Vec8i t5 = _mm256_andnot_si256(t3,t2);// fraction
    return Vec8ib(t4 == 0 && t5 != 0);    // exponent = 0 and fraction != 0
#else
    return Vec8fb(is_subnormal(a.get_low()), is_subnormal(a.get_high()));
#endif
}

// Function is_zero_or_subnormal: gives true for elements that are zero or subnormal (denormal)
// false for finite numbers, NAN and INF
static inline Vec8fb is_zero_or_subnormal(Vec8f const & a) {
#if defined (VECTORI256_H) && VECTORI256_H > 1   // 256 bit integer vectors are available, AVX2
    Vec8i t = _mm256_castps_si256(a);            // reinterpret as 32-bit integer
          t &= 0x7F800000;                       // isolate exponent
    return t == 0;                               // exponent = 0
#else
    return Vec8fb(is_zero_or_subnormal(a.get_low()), is_zero_or_subnormal(a.get_high()));
#endif
}

// Function infinite4f: returns a vector where all elements are +INF
static inline Vec8f infinite8f() {
    return constant8f<0x7F800000,0x7F800000,0x7F800000,0x7F800000,0x7F800000,0x7F800000,0x7F800000,0x7F800000>();
}

// Function nan4f: returns a vector where all elements are +NAN (quiet)
static inline Vec8f nan8f(int n = 0x10) {
    return _mm256_castsi256_ps(_mm256_set1_epi32(0x7FC00000 + n));
}

// change signs on vectors Vec8f
// Each index i0 - i7 is 1 for changing sign on the corresponding element, 0 for no change
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8f change_sign(Vec8f const & a) {
    if ((i0 | i1 | i2 | i3 | i4 | i5 | i6 | i7) == 0) return a;
    __m256 mask = constant8f<i0 ? 0x80000000 : 0, i1 ? 0x80000000 : 0, i2 ? 0x80000000 : 0, i3 ? 0x80000000 : 0,
        i4 ? 0x80000000 : 0, i5 ? 0x80000000 : 0, i6 ? 0x80000000 : 0, i7 ? 0x80000000 : 0> ();
    return _mm256_xor_ps(a, mask);
}


/*****************************************************************************
*
*          Vec4d: Vector of 4 double precision floating point values
*
*****************************************************************************/

class Vec4d {
protected:
    __m256d ymm; // double vector
public:
    // Default constructor:
    Vec4d() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec4d(double d) {
        ymm = _mm256_set1_pd(d);
    }
    // Constructor to build from all elements:
    Vec4d(double d0, double d1, double d2, double d3) {
        ymm = _mm256_setr_pd(d0, d1, d2, d3); 
    }
    // Constructor to build from two Vec2d:
    Vec4d(Vec2d const & a0, Vec2d const & a1) {
        ymm = _mm256_castps_pd(set_m128r(_mm_castpd_ps(a0), _mm_castpd_ps(a1)));
        //ymm = _mm256_set_m128d(a1, a0);
    }
    // Constructor to convert from type __m256d used in intrinsics:
    Vec4d(__m256d const & x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256d used in intrinsics:
    Vec4d & operator = (__m256d const & x) {
        ymm = x;
        return *this;
    }
    // Type cast operator to convert to __m256d used in intrinsics
    operator __m256d() const {
        return ymm;
    }
    // Member function to load from array (unaligned)
    Vec4d & load(double const * p) {
        ymm = _mm256_loadu_pd(p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    // You may use load_a instead of load if you are certain that p points to an address
    // divisible by 32
    Vec4d & load_a(double const * p) {
        ymm = _mm256_load_pd(p);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(double * p) const {
        _mm256_storeu_pd(p, ymm);
    }
    // Member function to store into array, aligned by 32
    // You may use store_a instead of store if you are certain that p points to an address
    // divisible by 32
    void store_a(double * p) const {
        _mm256_store_pd(p, ymm);
    }
    // Partial load. Load n elements and set the rest to 0
    Vec4d & load_partial(int n, double const * p) {
        if (n > 0 && n <= 2) {
            *this = Vec4d(Vec2d().load_partial(n, p), _mm_setzero_pd());
        }
        else if (n > 2 && n <= 4) {
            *this = Vec4d(Vec2d().load(p), Vec2d().load_partial(n - 2, p + 2));
        }
        else {
            ymm = _mm256_setzero_pd();
        }
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, double * p) const {
        if (n <= 2) {
            get_low().store_partial(n, p);
        }
        else if (n <= 4) {
            get_low().store(p);
            get_high().store_partial(n - 2, p + 2);
        }
    }
    // cut off vector to n elements. The last 4-n elements are set to zero
    Vec4d & cutoff(int n) {
        ymm = _mm256_castps_pd(Vec8f(_mm256_castpd_ps(ymm)).cutoff(n*2));
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec4d const & insert(uint32_t index, double value) {
        __m256d v0 = _mm256_broadcast_sd(&value);
        switch (index) {
        case 0:
            ymm = _mm256_blend_pd (ymm, v0, 1);  break;
        case 1:
            ymm = _mm256_blend_pd (ymm, v0, 2);  break;
        case 2:
            ymm = _mm256_blend_pd (ymm, v0, 4);  break;
        default:
            ymm = _mm256_blend_pd (ymm, v0, 8);  break;
        }
        return *this;
    }
    // Member function extract a single element from vector
    double extract(uint32_t index) const {
        double x[4];
        store(x);
        return x[index & 3];
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    double operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec2d:
    Vec2d get_low() const {
        return _mm256_castpd256_pd128(ymm);
    }
    Vec2d get_high() const {
        return _mm256_extractf128_pd(ymm,1);
    }
    static int size () {
        return 4;
    }
};



/*****************************************************************************
*
*          Operators for Vec4d
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec4d operator + (Vec4d const & a, Vec4d const & b) {
    return _mm256_add_pd(a, b);
}

// vector operator + : add vector and scalar
static inline Vec4d operator + (Vec4d const & a, double b) {
    return a + Vec4d(b);
}
static inline Vec4d operator + (double a, Vec4d const & b) {
    return Vec4d(a) + b;
}

// vector operator += : add
static inline Vec4d & operator += (Vec4d & a, Vec4d const & b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec4d operator ++ (Vec4d & a, int) {
    Vec4d a0 = a;
    a = a + 1.0;
    return a0;
}

// prefix operator ++
static inline Vec4d & operator ++ (Vec4d & a) {
    a = a + 1.0;
    return a;
}

// vector operator - : subtract element by element
static inline Vec4d operator - (Vec4d const & a, Vec4d const & b) {
    return _mm256_sub_pd(a, b);
}

// vector operator - : subtract vector and scalar
static inline Vec4d operator - (Vec4d const & a, double b) {
    return a - Vec4d(b);
}
static inline Vec4d operator - (double a, Vec4d const & b) {
    return Vec4d(a) - b;
}

// vector operator - : unary minus
// Change sign bit, even for 0, INF and NAN
static inline Vec4d operator - (Vec4d const & a) {
    return _mm256_xor_pd(a, _mm256_castps_pd(constant8f<0,(int)0x80000000,0,(int)0x80000000,0,(int)0x80000000,0,(int)0x80000000> ()));
}

// vector operator -= : subtract
static inline Vec4d & operator -= (Vec4d & a, Vec4d const & b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec4d operator -- (Vec4d & a, int) {
    Vec4d a0 = a;
    a = a - 1.0;
    return a0;
}

// prefix operator --
static inline Vec4d & operator -- (Vec4d & a) {
    a = a - 1.0;
    return a;
}

// vector operator * : multiply element by element
static inline Vec4d operator * (Vec4d const & a, Vec4d const & b) {
    return _mm256_mul_pd(a, b);
}

// vector operator * : multiply vector and scalar
static inline Vec4d operator * (Vec4d const & a, double b) {
    return a * Vec4d(b);
}
static inline Vec4d operator * (double a, Vec4d const & b) {
    return Vec4d(a) * b;
}

// vector operator *= : multiply
static inline Vec4d & operator *= (Vec4d & a, Vec4d const & b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer
static inline Vec4d operator / (Vec4d const & a, Vec4d const & b) {
    return _mm256_div_pd(a, b);
}

// vector operator / : divide vector and scalar
static inline Vec4d operator / (Vec4d const & a, double b) {
    return a / Vec4d(b);
}
static inline Vec4d operator / (double a, Vec4d const & b) {
    return Vec4d(a) / b;
}

// vector operator /= : divide
static inline Vec4d & operator /= (Vec4d & a, Vec4d const & b) {
    a = a / b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec4db operator == (Vec4d const & a, Vec4d const & b) {
    return _mm256_cmp_pd(a, b, 0);
}

// vector operator != : returns true for elements for which a != b
static inline Vec4db operator != (Vec4d const & a, Vec4d const & b) {
    return _mm256_cmp_pd(a, b, 4);
}

// vector operator < : returns true for elements for which a < b
static inline Vec4db operator < (Vec4d const & a, Vec4d const & b) {
    return _mm256_cmp_pd(a, b, 1);
}

// vector operator <= : returns true for elements for which a <= b
static inline Vec4db operator <= (Vec4d const & a, Vec4d const & b) {
    return _mm256_cmp_pd(a, b, 2);
}

// vector operator > : returns true for elements for which a > b
static inline Vec4db operator > (Vec4d const & a, Vec4d const & b) {
    return b < a;
}

// vector operator >= : returns true for elements for which a >= b
static inline Vec4db operator >= (Vec4d const & a, Vec4d const & b) {
    return b <= a;
}

// Bitwise logical operators

// vector operator & : bitwise and
static inline Vec4d operator & (Vec4d const & a, Vec4d const & b) {
    return _mm256_and_pd(a, b);
}

// vector operator &= : bitwise and
static inline Vec4d & operator &= (Vec4d & a, Vec4d const & b) {
    a = a & b;
    return a;
}

// vector operator & : bitwise and of Vec4d and Vec4db
static inline Vec4d operator & (Vec4d const & a, Vec4db const & b) {
    return _mm256_and_pd(a, b);
}
static inline Vec4d operator & (Vec4db const & a, Vec4d const & b) {
    return _mm256_and_pd(a, b);
}

// vector operator | : bitwise or
static inline Vec4d operator | (Vec4d const & a, Vec4d const & b) {
    return _mm256_or_pd(a, b);
}

// vector operator |= : bitwise or
static inline Vec4d & operator |= (Vec4d & a, Vec4d const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec4d operator ^ (Vec4d const & a, Vec4d const & b) {
    return _mm256_xor_pd(a, b);
}

// vector operator ^= : bitwise xor
static inline Vec4d & operator ^= (Vec4d & a, Vec4d const & b) {
    a = a ^ b;
    return a;
}

// vector operator ! : logical not. Returns Boolean vector
static inline Vec4db operator ! (Vec4d const & a) {
    return a == Vec4d(0.0);
}


/*****************************************************************************
*
*          Functions for Vec4d
*
*****************************************************************************/

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 2; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or 0xFFFFFFFFFFFFFFFF (true). 
// No other values are allowed.
static inline Vec4d select (Vec4db const & s, Vec4d const & a, Vec4d const & b) {
    return _mm256_blendv_pd(b, a, s);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec4d if_add (Vec4db const & f, Vec4d const & a, Vec4d const & b) {
    return a + (Vec4d(f) & b);
}

// Conditional multiply: For all vector elements i: result[i] = f[i] ? (a[i] * b[i]) : a[i]
static inline Vec4d if_mul (Vec4db const & f, Vec4d const & a, Vec4d const & b) {
    return a * select(f, b, 1.);
}


// General arithmetic functions, etc.

// Horizontal add: Calculates the sum of all vector elements.
static inline double horizontal_add (Vec4d const & a) {
    __m256d t1 = _mm256_hadd_pd(a,a);
    __m128d t2 = _mm256_extractf128_pd(t1,1);
    __m128d t3 = _mm_add_sd(_mm256_castpd256_pd128(t1),t2);
    return _mm_cvtsd_f64(t3);        
}

// function max: a > b ? a : b
static inline Vec4d max(Vec4d const & a, Vec4d const & b) {
    return _mm256_max_pd(a,b);
}

// function min: a < b ? a : b
static inline Vec4d min(Vec4d const & a, Vec4d const & b) {
    return _mm256_min_pd(a,b);
}

// function abs: absolute value
// Removes sign bit, even for -0.0f, -INF and -NAN
static inline Vec4d abs(Vec4d const & a) {
    __m256d mask = _mm256_castps_pd(constant8f<-1,0x7FFFFFFF,-1,0x7FFFFFFF,-1,0x7FFFFFFF,-1,0x7FFFFFFF> ());
    return _mm256_and_pd(a,mask);
}

// function sqrt: square root
static inline Vec4d sqrt(Vec4d const & a) {
    return _mm256_sqrt_pd(a);
}

// function square: a * a
static inline Vec4d square(Vec4d const & a) {
    return a * a;
}

// pow(Vec4d, int):
template <typename TT> static Vec4d pow(Vec4d const & a, TT n);

// Raise floating point numbers to integer power n
template <>
inline Vec4d pow<int>(Vec4d const & x0, int n) {
    return pow_template_i<Vec4d>(x0, n);
}

// allow conversion from unsigned int
template <>
inline Vec4d pow<uint32_t>(Vec4d const & x0, uint32_t n) {
    return pow_template_i<Vec4d>(x0, (int)n);
}


// Raise floating point numbers to integer power n, where n is a compile-time constant
template <int n>
static inline Vec4d pow_n(Vec4d const & a) {
    if (n < 0)    return Vec4d(1.0) / pow_n<-n>(a);
    if (n == 0)   return Vec4d(1.0);
    if (n >= 256) return pow(a, n);
    Vec4d x = a;                       // a^(2^i)
    Vec4d y;                           // accumulator
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
static inline Vec4d pow(Vec4d const & a, Const_int_t<n>) {
    return pow_n<n>(a);
}


// function round: round to nearest integer (even). (result as double vector)
static inline Vec4d round(Vec4d const & a) {
    return _mm256_round_pd(a, 0);
}

// function truncate: round towards zero. (result as double vector)
static inline Vec4d truncate(Vec4d const & a) {
    return _mm256_round_pd(a, 3);
}

// function floor: round towards minus infinity. (result as double vector)
static inline Vec4d floor(Vec4d const & a) {
    return _mm256_round_pd(a, 1);
}

// function ceil: round towards plus infinity. (result as double vector)
static inline Vec4d ceil(Vec4d const & a) {
    return _mm256_round_pd(a, 2);
}

// function round_to_int: round to nearest integer (even). (result as integer vector)
static inline Vec4i round_to_int(Vec4d const & a) {
    // Note: assume MXCSR control register is set to rounding
    return _mm256_cvtpd_epi32(a);
}

// function truncate_to_int: round towards zero. (result as integer vector)
static inline Vec4i truncate_to_int(Vec4d const & a) {
    return _mm256_cvttpd_epi32(a);
}

#ifdef VECTORI256_H  // 256 bit integer vectors are available

// function truncate_to_int64: round towards zero. (inefficient)
static inline Vec4q truncate_to_int64(Vec4d const & a) {
    double aa[4];
    a.store(aa);
    return Vec4q(int64_t(aa[0]), int64_t(aa[1]), int64_t(aa[2]), int64_t(aa[3]));
}

// function truncate_to_int64_limited: round towards zero.
// result as 64-bit integer vector, but with limited range
static inline Vec4q truncate_to_int64_limited(Vec4d const & a) {
#if VECTORI256_H > 1
    // Note: assume MXCSR control register is set to rounding
    Vec2q   b = _mm256_cvttpd_epi32(a);                    // round to 32-bit integers
    __m256i c = permute4q<0,-256,1,-256>(Vec4q(b,b));      // get bits 64-127 to position 128-191
    __m256i s = _mm256_srai_epi32(c, 31);                  // sign extension bits
    return      _mm256_unpacklo_epi32(c, s);               // interleave with sign extensions
#else
    return Vec4q(truncate_to_int64_limited(a.get_low()), truncate_to_int64_limited(a.get_high()));
#endif
} 

// function round_to_int64: round to nearest or even. (inefficient)
static inline Vec4q round_to_int64(Vec4d const & a) {
    return truncate_to_int64(round(a));
}

// function round_to_int64_limited: round to nearest integer (even)
// result as 64-bit integer vector, but with limited range
static inline Vec4q round_to_int64_limited(Vec4d const & a) {
#if VECTORI256_H > 1
    // Note: assume MXCSR control register is set to rounding
    Vec2q   b = _mm256_cvtpd_epi32(a);                     // round to 32-bit integers
    __m256i c = permute4q<0,-256,1,-256>(Vec4q(b,b));      // get bits 64-127 to position 128-191
    __m256i s = _mm256_srai_epi32(c, 31);                  // sign extension bits
    return      _mm256_unpacklo_epi32(c, s);               // interleave with sign extensions
#else
    return Vec4q(round_to_int64_limited(a.get_low()), round_to_int64_limited(a.get_high()));
#endif
}

// function to_double: convert integer vector elements to double vector (inefficient)
static inline Vec4d to_double(Vec4q const & a) {
    int64_t aa[4];
    a.store(aa);
    return Vec4d(double(aa[0]), double(aa[1]), double(aa[2]), double(aa[3]));
}

// function to_double_limited: convert integer vector elements to double vector
// limited to abs(x) < 2^31
static inline Vec4d to_double_limited(Vec4q const & x) {
    Vec8i compressed = permute8i<0,2,4,6,-256,-256,-256,-256>(Vec8i(x));
    return _mm256_cvtepi32_pd(compressed.get_low());  // AVX
}

#endif // VECTORI256_H

// function to_double: convert integer vector to double vector
static inline Vec4d to_double(Vec4i const & a) {
    return _mm256_cvtepi32_pd(a);
}

// function compress: convert two Vec4d to one Vec8f
static inline Vec8f compress (Vec4d const & low, Vec4d const & high) {
    __m128 t1 = _mm256_cvtpd_ps(low);
    __m128 t2 = _mm256_cvtpd_ps(high);
    return Vec8f(t1, t2);
}

// Function extend_low : convert Vec8f vector elements 0 - 3 to Vec4d
static inline Vec4d extend_low(Vec8f const & a) {
    return _mm256_cvtps_pd(_mm256_castps256_ps128(a));
}

// Function extend_high : convert Vec8f vector elements 4 - 7 to Vec4d
static inline Vec4d extend_high (Vec8f const & a) {
    return _mm256_cvtps_pd(_mm256_extractf128_ps(a,1));
}

// Fused multiply and add functions

// Multiply and add
static inline Vec4d mul_add(Vec4d const & a, Vec4d const & b, Vec4d const & c) {
#ifdef __FMA__
    return _mm256_fmadd_pd(a, b, c);
#elif defined (__FMA4__)
    return _mm256_macc_pd(a, b, c);
#else
    return a * b + c;
#endif
    
}


// Multiply and subtract
static inline Vec4d mul_sub(Vec4d const & a, Vec4d const & b, Vec4d const & c) {
#ifdef __FMA__
    return _mm256_fmsub_pd(a, b, c);
#elif defined (__FMA4__)
    return _mm256_msub_pd(a, b, c);
#else
    return a * b - c;
#endif
   
}

// Multiply and inverse subtract
static inline Vec4d nmul_add(Vec4d const & a, Vec4d const & b, Vec4d const & c) {
#ifdef __FMA__
    return _mm256_fnmadd_pd(a, b, c);
#elif defined (__FMA4__)
    return _mm256_nmacc_pd(a, b, c);
#else
    return c - a * b;
#endif
}

// Multiply and subtract with extra precision on the intermediate calculations, 
// even if FMA instructions not supported, using Veltkamp-Dekker split
static inline Vec4d mul_sub_x(Vec4d const & a, Vec4d const & b, Vec4d const & c) {
#ifdef __FMA__
    return _mm256_fmsub_pd(a, b, c);
#elif defined (__FMA4__)
    return _mm256_msub_pd(a, b, c);
#else
    // calculate a * b - c with extra precision
    // mask to remove lower 27 bits
    Vec4d upper_mask = _mm256_castps_pd(constant8f<(int)0xF8000000,-1,(int)0xF8000000,-1,(int)0xF8000000,-1,(int)0xF8000000,-1>());
    Vec4d a_high = a & upper_mask;               // split into high and low parts
    Vec4d b_high = b & upper_mask;
    Vec4d a_low  = a - a_high;
    Vec4d b_low  = b - b_high;
    Vec4d r1 = a_high * b_high;                  // this product is exact
    Vec4d r2 = r1 - c;                           // subtract c from high product
    Vec4d r3 = r2 + (a_high * b_low + b_high * a_low) + a_low * b_low; // add rest of product
    return r3; // + ((r2 - r1) + c);
#endif
}


// Math functions using fast bit manipulation

#ifdef VECTORI256_H  // 256 bit integer vectors are available
// Extract the exponent as an integer
// exponent(a) = floor(log2(abs(a)));
// exponent(1.0) = 0, exponent(0.0) = -1023, exponent(INF) = +1024, exponent(NAN) = +1024
static inline Vec4q exponent(Vec4d const & a) {
#if VECTORI256_H > 1  // AVX2
    Vec4uq t1 = _mm256_castpd_si256(a);// reinterpret as 64-bit integer
    Vec4uq t2 = t1 << 1;               // shift out sign bit
    Vec4uq t3 = t2 >> 53;              // shift down logical to position 0
    Vec4q  t4 = Vec4q(t3) - 0x3FF;     // subtract bias from exponent
    return t4;
#else
    return Vec4q(exponent(a.get_low()), exponent(a.get_high()));
#endif
}

// Extract the fraction part of a floating point number
// a = 2^exponent(a) * fraction(a), except for a = 0
// fraction(1.0) = 1.0, fraction(5.0) = 1.25 
static inline Vec4d fraction(Vec4d const & a) {
#if VECTORI256_H > 1  // AVX2
    Vec4uq t1 = _mm256_castpd_si256(a);   // reinterpret as 64-bit integer
    Vec4uq t2 = Vec4uq((t1 & 0x000FFFFFFFFFFFFF) | 0x3FF0000000000000); // set exponent to 0 + bias
    return _mm256_castsi256_pd(t2);
#else
    return Vec4d(fraction(a.get_low()), fraction(a.get_high()));
#endif
}

// Fast calculation of pow(2,n) with n integer
// n  =     0 gives 1.0
// n >=  1024 gives +INF
// n <= -1023 gives 0.0
// This function will never produce denormals, and never raise exceptions
static inline Vec4d exp2(Vec4q const & n) {
#if VECTORI256_H > 1  // AVX2
    Vec4q t1 = max(n,  -0x3FF);        // limit to allowed range
    Vec4q t2 = min(t1,  0x400);
    Vec4q t3 = t2 + 0x3FF;             // add bias
    Vec4q t4 = t3 << 52;               // put exponent into position 52
    return _mm256_castsi256_pd(t4);       // reinterpret as double
#else
    return Vec4d(exp2(n.get_low()), exp2(n.get_high()));
#endif
}
//static inline Vec4d exp2(Vec4d const & x); // defined in vectormath_exp.h
#endif


// Categorization functions

// Function sign_bit: gives true for elements that have the sign bit set
// even for -0.0, -INF and -NAN
// Note that sign_bit(Vec4d(-0.0)) gives true, while Vec4d(-0.0) < Vec4d(0.0) gives false
static inline Vec4db sign_bit(Vec4d const & a) {
#if defined (VECTORI256_H) && VECTORI256_H > 1  // 256 bit integer vectors are available, AVX2
    Vec4q t1 = _mm256_castpd_si256(a);    // reinterpret as 64-bit integer
    Vec4q t2 = t1 >> 63;               // extend sign bit
    return _mm256_castsi256_pd(t2);       // reinterpret as 64-bit Boolean
#else
    return Vec4db(sign_bit(a.get_low()),sign_bit(a.get_high()));
#endif
}

// Function sign_combine: changes the sign of a when b has the sign bit set
// same as select(sign_bit(b), -a, a)
static inline Vec4d sign_combine(Vec4d const & a, Vec4d const & b) {
    Vec4d signmask = _mm256_castps_pd(constant8f<0,(int)0x80000000,0,(int)0x80000000,0,(int)0x80000000,0,(int)0x80000000>());  // -0.0
    return a ^ (b & signmask);
}

// Function is_finite: gives true for elements that are normal, denormal or zero, 
// false for INF and NAN
static inline Vec4db is_finite(Vec4d const & a) {
#if defined (VECTORI256_H) && VECTORI256_H > 1  // 256 bit integer vectors are available, AVX2
    Vec4q t1 = _mm256_castpd_si256(a); // reinterpret as 64-bit integer
    Vec4q t2 = t1 << 1;                // shift out sign bit
    Vec4q t3 = 0xFFE0000000000000;     // exponent mask
    Vec4qb t4 = Vec4q(t2 & t3) != t3;  // exponent field is not all 1s
    return t4;
#else
    return Vec4db(is_finite(a.get_low()),is_finite(a.get_high()));
#endif
}

// Function is_inf: gives true for elements that are +INF or -INF
// false for finite numbers and NAN
static inline Vec4db is_inf(Vec4d const & a) {
#if defined (VECTORI256_H) && VECTORI256_H > 1  // 256 bit integer vectors are available, AVX2
    Vec4q t1 = _mm256_castpd_si256(a); // reinterpret as 64-bit integer
    Vec4q t2 = t1 << 1;                // shift out sign bit
    return t2 == 0xFFE0000000000000;   // exponent is all 1s, fraction is 0
#else
    return Vec4db(is_inf(a.get_low()),is_inf(a.get_high()));
#endif
}

// Function is_nan: gives true for elements that are +NAN or -NAN
// false for finite numbers and +/-INF
static inline Vec4db is_nan(Vec4d const & a) {
#if defined (VECTORI256_H) && VECTORI256_H > 1  // 256 bit integer vectors are available, AVX2
    Vec4q t1 = _mm256_castpd_si256(a); // reinterpret as 64-bit integer
    Vec4q t2 = t1 << 1;                // shift out sign bit
    Vec4q t3 = 0xFFE0000000000000;     // exponent mask
    Vec4q t4 = t2 & t3;                // exponent
    Vec4q t5 = _mm256_andnot_si256(t3,t2);// fraction
    return Vec4qb(t4 == t3 && t5 != 0);// exponent = all 1s and fraction != 0
#else
    return Vec4db(is_nan(a.get_low()),is_nan(a.get_high()));
#endif
}

// Function is_subnormal: gives true for elements that are denormal (subnormal)
// false for finite numbers, zero, NAN and INF
static inline Vec4db is_subnormal(Vec4d const & a) {
#if defined (VECTORI256_H) && VECTORI256_H > 1  // 256 bit integer vectors are available, AVX2
    Vec4q t1 = _mm256_castpd_si256(a); // reinterpret as 64-bit integer
    Vec4q t2 = t1 << 1;                // shift out sign bit
    Vec4q t3 = 0xFFE0000000000000;     // exponent mask
    Vec4q t4 = t2 & t3;                // exponent
    Vec4q t5 = _mm256_andnot_si256(t3,t2);// fraction
    return Vec4qb(t4 == 0 && t5 != 0); // exponent = 0 and fraction != 0
#else
    return Vec4db(is_subnormal(a.get_low()),is_subnormal(a.get_high()));
#endif
}

// Function is_zero_or_subnormal: gives true for elements that are zero or subnormal (denormal)
// false for finite numbers, NAN and INF
static inline Vec4db is_zero_or_subnormal(Vec4d const & a) {
#if defined (VECTORI256_H) && VECTORI256_H > 1  // 256 bit integer vectors are available, AVX2
    Vec4q t = _mm256_castpd_si256(a);     // reinterpret as 32-bit integer
          t &= 0x7FF0000000000000ll;   // isolate exponent
    return t == 0;                     // exponent = 0
#else
    return Vec4db(is_zero_or_subnormal(a.get_low()),is_zero_or_subnormal(a.get_high()));
#endif
}

// Function infinite2d: returns a vector where all elements are +INF
static inline Vec4d infinite4d() {
    return _mm256_castps_pd(constant8f<0,0x7FF00000,0,0x7FF00000,0,0x7FF00000,0,0x7FF00000>());
}

// Function nan4d: returns a vector where all elements are +NAN (quiet)
static inline Vec4d nan4d(int n = 0x10) {
#if defined (VECTORI256_H) && VECTORI256_H > 1  // 256 bit integer vectors are available, AVX2
    return _mm256_castsi256_pd(Vec4q(0x7FF8000000000000 + n));
#else
    return Vec4d(nan2d(n),nan2d(n));
#endif
}

// change signs on vectors Vec4d
// Each index i0 - i3 is 1 for changing sign on the corresponding element, 0 for no change
template <int i0, int i1, int i2, int i3>
static inline Vec4d change_sign(Vec4d const & a) {
    if ((i0 | i1 | i2 | i3) == 0) return a;
    __m256 mask = constant8f<0, i0 ? 0x80000000 : 0, 0, i1 ? 0x80000000 : 0, 0, i2 ? 0x80000000 : 0, 0, i3 ? 0x80000000 : 0> ();
    return _mm256_xor_pd(a, _mm256_castps_pd(mask));
}


/*****************************************************************************
*
*          Functions for reinterpretation between vector types
*
*****************************************************************************/

#if defined (VECTORI256_H) && VECTORI256_H >= 2
// AVX2 vectors defined


// ABI version 4 or later needed on Gcc for correct mangling of 256-bit intrinsic vectors.
// It is recommended to compile with -fabi-version=0 to get the latest abi version
#if !defined (GCC_VERSION) || (defined (__GXX_ABI_VERSION) && __GXX_ABI_VERSION >= 1004)  
static inline __m256i reinterpret_i (__m256i const & x) {
    return x;
}

static inline __m256i reinterpret_i (__m256  const & x) {
    return _mm256_castps_si256(x);
}

static inline __m256i reinterpret_i (__m256d const & x) {
    return _mm256_castpd_si256(x);
}

static inline __m256  reinterpret_f (__m256i const & x) {
    return _mm256_castsi256_ps(x);
}

static inline __m256  reinterpret_f (__m256  const & x) {
    return x;
}

static inline __m256  reinterpret_f (__m256d const & x) {
    return _mm256_castpd_ps(x);
}

static inline __m256d reinterpret_d (__m256i const & x) {
    return _mm256_castsi256_pd(x);
}

static inline __m256d reinterpret_d (__m256  const & x) {
    return _mm256_castps_pd(x);
}

static inline __m256d reinterpret_d (__m256d const & x) {
    return x;
}

#else  // __GXX_ABI_VERSION < 1004

static inline __m256i reinterpret_i (Vec32c const & x) {
    return x;
}

static inline __m256i reinterpret_i (Vec16s const & x) {
    return x;
}

static inline __m256i reinterpret_i (Vec8i const & x) {
    return x;
}

static inline __m256i reinterpret_i (Vec4q const & x) {
    return x;
}

static inline __m256i reinterpret_i (Vec8f  const & x) {
    return _mm256_castps_si256(x);
}

static inline __m256i reinterpret_i (Vec4d const & x) {
    return _mm256_castpd_si256(x);
}

static inline __m256  reinterpret_f (Vec32c const & x) {
    return _mm256_castsi256_ps(x);
}

static inline __m256  reinterpret_f (Vec16s const & x) {
    return _mm256_castsi256_ps(x);
}

static inline __m256  reinterpret_f (Vec8i const & x) {
    return _mm256_castsi256_ps(x);
}

static inline __m256  reinterpret_f (Vec4q const & x) {
    return _mm256_castsi256_ps(x);
}

static inline __m256  reinterpret_f (Vec8f  const & x) {
    return x;
}

static inline __m256  reinterpret_f (Vec4d const & x) {
    return _mm256_castpd_ps(x);
}

static inline __m256d reinterpret_d (Vec32c const & x) {
    return _mm256_castsi256_pd(x);
}

static inline __m256d reinterpret_d (Vec16s const & x) {
    return _mm256_castsi256_pd(x);
}

static inline __m256d reinterpret_d (Vec8i const & x) {
    return _mm256_castsi256_pd(x);
}

static inline __m256d reinterpret_d (Vec4q const & x) {
    return _mm256_castsi256_pd(x);
}

static inline __m256d reinterpret_d (Vec8f  const & x) {
    return _mm256_castps_pd(x);
}

static inline __m256d reinterpret_d (Vec4d const & x) {
    return x;
}

#endif  // __GXX_ABI_VERSION

#else
// AVX2 emulated in vectori256e.h, AVX supported

// ABI version 4 or later needed on Gcc for correct mangling of 256-bit intrinsic vectors.
// It is recommended to compile with -fabi-version=0 to get the latest abi version
#if !defined (GCC_VERSION) || (defined (__GXX_ABI_VERSION) && __GXX_ABI_VERSION >= 1004)  

static inline Vec256ie reinterpret_i (__m256  const & x) {
    Vec8f xx(x);
    return Vec256ie(reinterpret_i(xx.get_low()), reinterpret_i(xx.get_high()));
}

static inline Vec256ie reinterpret_i (__m256d const & x) {
    Vec4d xx(x);
    return Vec256ie(reinterpret_i(xx.get_low()), reinterpret_i(xx.get_high()));
}

static inline __m256  reinterpret_f (__m256  const & x) {
    return x;
}

static inline __m256  reinterpret_f (__m256d const & x) {
    return _mm256_castpd_ps(x);
}

static inline __m256d reinterpret_d (__m256  const & x) {
    return _mm256_castps_pd(x);
}

static inline __m256d reinterpret_d (__m256d const & x) {
    return x;
}

#else  // __GXX_ABI_VERSION < 1004

static inline Vec256ie reinterpret_i (Vec8f const & x) {
    Vec8f xx(x);
    return Vec256ie(reinterpret_i(xx.get_low()), reinterpret_i(xx.get_high()));
}

static inline Vec256ie reinterpret_i (Vec4d const & x) {
    Vec4d xx(x);
    return Vec256ie(reinterpret_i(xx.get_low()), reinterpret_i(xx.get_high()));
}

static inline __m256  reinterpret_f (Vec8f const & x) {
    return x;
}

static inline __m256  reinterpret_f (Vec4d const & x) {
    return _mm256_castpd_ps(x);
}

static inline __m256d reinterpret_d (Vec8f const & x) {
    return _mm256_castps_pd(x);
}

static inline __m256d reinterpret_d (Vec4d const & x) {
    return x;
}

#endif  // __GXX_ABI_VERSION

static inline Vec256ie reinterpret_i (Vec256ie const & x) {
    return x;
}

static inline __m256  reinterpret_f (Vec256ie const & x) {
    return Vec8f(Vec4f(reinterpret_f(x.get_low())), Vec4f(reinterpret_f(x.get_high())));
}

static inline __m256d reinterpret_d (Vec256ie const & x) {
    return Vec4d(Vec2d(reinterpret_d(x.get_low())), Vec2d(reinterpret_d(x.get_high())));
}

#endif  // VECTORI256_H


/*****************************************************************************
*
*          Vector permute and blend functions
*
******************************************************************************
*
* The permute function can reorder the elements of a vector and optionally
* set some elements to zero. 
*
* The indexes are inserted as template parameters in <>. These indexes must be
* constants. Each template parameter is an index to the element you want to 
* select. An index of -1 will generate zero. An index of -256 means don't care.
*
* Example:
* Vec4d a(10., 11., 12., 13.);    // a is (10, 11, 12, 13)
* Vec4d b;
* b = permute4d<1,0,-1,3>(a);     // b is (11, 10,  0, 13)
*
*
* The blend function can mix elements from two different vectors and
* optionally set some elements to zero. 
*
* The indexes are inserted as template parameters in <>. These indexes must be
* constants. Each template parameter is an index to the element you want to 
* select, where indexes 0 - 3 indicate an element from the first source
* vector and indexes 4 - 7 indicate an element from the second source vector.
* A negative index will generate zero.
*
*
* Example:
* Vec4d a(10., 11., 12., 13.);    // a is (10, 11, 12, 13)
* Vec4d b(20., 21., 22., 23.);    // a is (20, 21, 22, 23)
* Vec4d c;
* c = blend4d<4,3,7,-1> (a,b);    // c is (20, 13, 23,  0)
*
* A lot of the code here is metaprogramming aiming to find the instructions
* that best fit the template parameters and instruction set. The metacode
* will be reduced out to leave only a few vector instructions in release
* mode with optimization on.
*****************************************************************************/

// permute vector Vec4d
template <int i0, int i1, int i2, int i3>
static inline Vec4d permute4d(Vec4d const & a) {

    const int ior = i0 | i1 | i2 | i3;  // OR indexes

    // is zeroing needed
    const bool do_zero    = ior < 0 && (ior & 0x80); // at least one index is negative, and not -0x100

    // is shuffling needed
    const bool do_shuffle = (i0>0) || (i1!=1 && i1>=0) || (i2!=2 && i2>=0) || (i3!=3 && i3>=0);

    if (!do_shuffle) {       // no shuffling needed
        if (do_zero) {       // zeroing
            if ((i0 & i1 & i2 & i3) < 0) {
                return _mm256_setzero_pd(); // zero everything
            }
            // zero some elements
            __m256d const mask = _mm256_castps_pd (
                constant8f< -int(i0>=0), -int(i0>=0), -int(i1>=0), -int(i1>=0), -int(i2>=0), -int(i2>=0), -int(i3>=0), -int(i3>=0) > ());
            return _mm256_and_pd(a, mask);     // zero with AND mask
        }
        else {
            return a;  // do nothing
        }
    }
#if INSTRSET >= 8  // AVX2: use VPERMPD
    __m256d x = _mm256_permute4x64_pd(a, (i0&3) | (i1&3)<<2 | (i2&3)<<4 | (i3&3)<<6);
    if (do_zero) {       // zeroing
        // zero some elements
        __m256d const mask2 = _mm256_castps_pd (
            constant8f< -int(i0>=0), -int(i0>=0), -int(i1>=0), -int(i1>=0), -int(i2>=0), -int(i2>=0), -int(i3>=0), -int(i3>=0) > ());
        x = _mm256_and_pd(x, mask2);     // zero with AND mask
    }
    return x;
#else   // AVX

    // Needed contents of low/high part of each source register in VSHUFPD
    // 0: a.low, 1: a.high, 3: zero
    const int s1 = (i0 < 0 ? 3 : (i0 & 2) >> 1) | (i2 < 0 ? 0x30 : (i2 & 2) << 3);
    const int s2 = (i1 < 0 ? 3 : (i1 & 2) >> 1) | (i3 < 0 ? 0x30 : (i3 & 2) << 3);
    // permute mask
    const int sm = (i0 < 0 ? 0 : (i0 & 1)) | (i1 < 0 ? 1 : (i1 & 1)) << 1 | (i2 < 0 ? 0 : (i2 & 1)) << 2 | (i3 < 0 ? 1 : (i3 & 1)) << 3;

    if (s1 == 0x01 || s1 == 0x11 || s2 == 0x01 || s2 == 0x11) {
        // too expensive to use 256 bit permute, split into two 128 bit permutes
        Vec2d alo = a.get_low();
        Vec2d ahi = a.get_high();
        Vec2d rlo = blend2d<i0, i1> (alo, ahi);
        Vec2d rhi = blend2d<i2, i3> (alo, ahi);
        return Vec4d(rlo, rhi);
    }

    // make operands for VSHUFPD
    __m256d r1, r2;

    switch (s1) {
    case 0x00:  // LL
        r1 = _mm256_insertf128_pd(a,_mm256_castpd256_pd128(a),1);  break;
    case 0x03:  // LZ
        r1 = _mm256_insertf128_pd(do_zero ? _mm256_setzero_pd() : __m256d(a), _mm256_castpd256_pd128(a), 1);
        break;
    case 0x10:  // LH
        r1 = a;  break;
    case 0x13:  // ZH
        r1 = do_zero ? _mm256_and_pd(a, _mm256_castps_pd(constant8f<0,0,0,0,-1,-1,-1,-1>())) : __m256d(a);  break;
    case 0x30:  // LZ
        if (do_zero) {
            __m128d t  = _mm256_castpd256_pd128(a);
            t  = _mm_and_pd(t,t);
            r1 = _mm256_castpd128_pd256(t);  
        }
        else r1 = a;
        break;
    case 0x31:  // HZ
        r1 = _mm256_castpd128_pd256(_mm256_extractf128_pd(a,1));  break;
    case 0x33:  // ZZ
        r1 = do_zero ? _mm256_setzero_pd() : __m256d(a);  break;
    }

    if (s2 == s1) {
        if (sm == 0x0A) return r1;
        r2 = r1;
    }
    else {
        switch (s2) {
        case 0x00:  // LL
            r2 = _mm256_insertf128_pd(a,_mm256_castpd256_pd128(a),1);  break;
        case 0x03:  // ZL
            r2 = _mm256_insertf128_pd(do_zero ? _mm256_setzero_pd() : __m256d(a), _mm256_castpd256_pd128(a), 1);
            break;
        case 0x10:  // LH
            r2 = a;  break;
        case 0x13:  // ZH
            r2 = do_zero ? _mm256_and_pd(a,_mm256_castps_pd(constant8f<0,0,0,0,-1,-1,-1,-1>())) : __m256d(a);  break;
        case 0x30:  // LZ
            if (do_zero) {
                __m128d t  = _mm256_castpd256_pd128(a);
                t  = _mm_and_pd(t,t);
                r2 = _mm256_castpd128_pd256(t);  
            }
            else r2 = a;
            break;
        case 0x31:  // HZ
            r2 = _mm256_castpd128_pd256(_mm256_extractf128_pd(a,1));  break;
        case 0x33:  // ZZ
            r2 = do_zero ? _mm256_setzero_pd() : __m256d(a);  break;
        }
    }
    return  _mm256_shuffle_pd(r1, r2, sm);

#endif  // INSTRSET >= 8
}


// blend vectors Vec4d
template <int i0, int i1, int i2, int i3>
static inline Vec4d blend4d(Vec4d const & a, Vec4d const & b) {

    // Combine all the indexes into a single bitfield, with 8 bits for each
    const int m1 = (i0 & 7) | (i1 & 7) << 8 | (i2 & 7) << 16 | (i3 & 7) << 24; 

    // Mask to zero out negative indexes
    const uint32_t mz = (i0 < 0 ? 0 : 0xFF) | (i1 < 0 ? 0 : 0xFF) << 8 | (i2 < 0 ? 0 : 0xFF) << 16 | (i3 < 0 ? 0 : 0xFF) << 24;

    if (mz == 0) return _mm256_setzero_pd();  // all zero
    
    __m256d t1;
    if ((((m1 & 0xFEFEFEFE) ^ 0x06020400) & mz) == 0) {
        // fits VSHUFPD(a,b)
        t1 = _mm256_shuffle_pd(a, b, (i0 & 1) | (i1 & 1) << 1 | (i2 & 1) << 2 | (i3 & 1) << 3);
        if (mz == 0xFFFFFFFF) return t1;
        return permute4d<i0 < 0 ? -1 : 0, i1 < 0 ? -1 : 1, i2 < 0 ? -1 : 2, i3 < 0 ? -1 : 3> (t1);
    }
    if ((((m1 & 0xFEFEFEFE) ^0x02060004) & mz) == 0) {
        // fits VSHUFPD(b,a)
        t1 = _mm256_shuffle_pd(b, a, (i0 & 1) | (i1 & 1) << 1 | (i2 & 1) << 2 | (i3 & 1) << 3);
        if (mz == 0xFFFFFFFF) return t1;
        return permute4d<i0 < 0 ? -1 : 0, i1 < 0 ? -1 : 1, i2 < 0 ? -1 : 2, i3 < 0 ? -1 : 3> (t1);
    }
    if ((((m1 & 0x03030303) ^ 0x03020100) & mz) == 0) {
        // blend and zero, no permute
        if ((m1 & 0x04040404 & mz) == 0) {
            t1 = a;
        }
        else if (((m1 ^ 0x04040404) & 0x04040404 & mz) == 0) {
            t1 = b;
        }
        else {
            t1 = _mm256_blend_pd(a, b, (i0&4)>>2 | (i1&4)>>1 | (i2&4) | (i3&4) << 1);
        }
        if (mz == 0xFFFFFFFF) return t1;
        return permute4d<i0 < 0 ? -1 : 0, i1 < 0 ? -1 : 1, i2 < 0 ? -1 : 2, i3 < 0 ? -1 : 3> (t1);
    }
    if ((m1 & 0x04040404 & mz) == 0) {
        // all from a
        return permute4d<i0, i1, i2, i3> (a);
    }
    if (((m1 ^ 0x04040404) & 0x04040404 & mz) == 0) {
        // all from b
        return permute4d<i0 ^ 4, i1 ^ 4, i2 ^ 4, i3 ^ 4> (b);
    }
    // check if we can do 128-bit blend/permute
    if (((m1 ^ 0x01000100) & 0x01010101 & mz) == 0) {
        const uint32_t j0 = uint32_t((i0 >= 0 ? i0 : i1 >= 0 ? i1 : -1) >> 1);
        const uint32_t j1 = uint32_t((i2 >= 0 ? i2 : i3 >= 0 ? i3 : -1) >> 1);
        if (((m1 ^ ((j0 & 3) * 0x00000202 | (j1 & 3) * 0x02020000)) & 0x06060606 & mz) == 0) {
            t1 = _mm256_permute2f128_pd(a, b, (j0 & 0x0F) | (j1 & 0x0F) << 4);
            const bool partialzero = (((i0 | i1) ^ j0) & 0x80) != 0 || (((i2 | i3) ^ j1) & 0x80) != 0;
            if (partialzero) {
                // zero some elements
                __m256d mask = _mm256_castps_pd (constant8f < 
                    i0 < 0 ? 0 : -1, i0 < 0 ? 0 : -1, i1 < 0 ? 0 : -1, i1 < 0 ? 0 : -1, 
                    i2 < 0 ? 0 : -1, i2 < 0 ? 0 : -1, i3 < 0 ? 0 : -1, i3 < 0 ? 0 : -1 > ());
                return _mm256_and_pd(t1, mask);
            }
            else return t1;
        }
    }
    // general case. combine two permutes
    Vec4d a1 = permute4d <
        (uint32_t)i0 < 4 ? i0 : -0x100,
        (uint32_t)i1 < 4 ? i1 : -0x100,
        (uint32_t)i2 < 4 ? i2 : -0x100,
        (uint32_t)i3 < 4 ? i3 : -0x100 > (a);
    Vec4d b1 = permute4d <
        (uint32_t)(i0^4) < 4 ? (i0^4) : -0x100,
        (uint32_t)(i1^4) < 4 ? (i1^4) : -0x100,
        (uint32_t)(i2^4) < 4 ? (i2^4) : -0x100,
        (uint32_t)(i3^4) < 4 ? (i3^4) : -0x100 > (b);   
    t1 = _mm256_blend_pd(a1, b1, (i0&4)>>2 | (i1&4)>>1 | (i2&4) | (i3&4) << 1);
    if (mz == 0xFFFFFFFF) return t1;
    return permute4d<i0 < 0 ? -1 : 0, i1 < 0 ? -1 : 1, i2 < 0 ? -1 : 2, i3 < 0 ? -1 : 3> (t1);
}

/*****************************************************************************
*
*          Vector Vec8f permute and blend functions
*
*****************************************************************************/

// permute vector Vec8f
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8f permute8f(Vec8f const & a) {

    __m256 t1, mask;

    const int ior = i0 | i1 | i2 | i3 | i4 | i5 | i6 | i7;  // OR indexes

    // is zeroing needed
    const bool do_zero    = ior < 0 && (ior & 0x80); // at least one index is negative, and not -0x100

    // is shuffling needed
    const bool do_shuffle = (i0>0) || (i1!=1 && i1>=0) || (i2!=2 && i2>=0) || (i3!=3 && i3>=0) ||
        (i4!=4 && i4>=0) || (i5!=5 && i5>=0) || (i6!=6 && i6>=0) || (i7!=7 && i7>=0);

    if (!do_shuffle) {       // no shuffling needed
        if (do_zero) {       // zeroing
            if ((i0 & i1 & i2 & i3 & i4 & i5 & i6 & i7) < 0) {
                return _mm256_setzero_ps(); // zero everything
            }
            // zero some elements
            mask = constant8f< -int(i0>=0), -int(i1>=0), -int(i2>=0), -int(i3>=0), -int(i4>=0), -int(i5>=0), -int(i6>=0), -int(i7>=0) > ();
            return _mm256_and_ps(a, mask);     // zero with AND mask
        }
        else {
            return a;  // do nothing
        }
    }

#if INSTRSET >= 8  // AVX2: use VPERMPS
    if (do_shuffle) {    // shuffling
        mask = constant8f< i0 & 7, i1 & 7, i2 & 7, i3 & 7, i4 & 7, i5 & 7, i6 & 7, i7 & 7 > ();
#if defined (_MSC_VER) && _MSC_VER < 1700 && ! defined(__INTEL_COMPILER)
        // bug in MS VS 11 beta: operands in wrong order. fixed in 11.0
        t1 = _mm256_permutevar8x32_ps(mask, _mm256_castps_si256(a));      //  problem in immintrin.h
#elif defined (GCC_VERSION) && GCC_VERSION <= 40700 && !defined(__INTEL_COMPILER) && !defined(__clang__)
        // Gcc 4.7.0 has wrong parameter type and operands in wrong order. fixed in version 4.7.1
        t1 = _mm256_permutevar8x32_ps(mask, a);
#else   // no bug version
        t1 = _mm256_permutevar8x32_ps(a, _mm256_castps_si256(mask));
#endif
    }
    else {
        t1 = a;          // no shuffling
    }
    if (do_zero) {       // zeroing
        if ((i0 & i1 & i2 & i3 & i4 & i5 & i6 & i7) < 0) {
            return _mm256_setzero_ps(); // zero everything
        }
        // zero some elements
        mask = constant8f< -int(i0>=0), -int(i1>=0), -int(i2>=0), -int(i3>=0), -int(i4>=0), -int(i5>=0), -int(i6>=0), -int(i7>=0) > ();
        t1 = _mm256_and_ps(t1, mask);     // zero with AND mask
    }
    return t1;
#else   // AVX

    // Combine all the indexes into a single bitfield, with 4 bits for each
    const int m1 = (i0&7) | (i1&7)<<4 | (i2&7)<<8 | (i3&7)<<12 | (i4&7)<<16 | (i5&7)<<20 | (i6&7)<<24 | (i7&7)<<28;

    // Mask to zero out negative indexes
    const int m2 = (i0<0?0:0xF) | (i1<0?0:0xF)<<4 | (i2<0?0:0xF)<<8 | (i3<0?0:0xF)<<12 | (i4<0?0:0xF)<<16 | (i5<0?0:0xF)<<20 | (i6<0?0:0xF)<<24 | (i7<0?0:0xF)<<28;

    // Check if it is possible to use VSHUFPS. Index n must match index n+4 on bit 0-1, and even index n must match odd index n+1 on bit 2
    const bool sps = ((m1 ^ (m1 >> 16)) & 0x3333 & m2 & (m2 >> 16)) == 0  &&  ((m1 ^ (m1 >> 4)) & 0x04040404 & m2 & m2 >> 4) == 0;

    if (sps) {   // can use VSHUFPS

        // Index of each pair (i[n],i[n+1])
        const int j0 = i0 >= 0 ? i0 : i1;
        const int j1 = i2 >= 0 ? i2 : i3;
        const int j2 = i4 >= 0 ? i4 : i5;
        const int j3 = i6 >= 0 ? i6 : i7;

        // Index of each pair (i[n],i[n+4])
        const int k0 = i0 >= 0 ? i0 : i4;
        const int k1 = i1 >= 0 ? i1 : i5;
        const int k2 = i2 >= 0 ? i2 : i6;
        const int k3 = i3 >= 0 ? i3 : i7;

        // Needed contents of low/high part of each source register in VSHUFPS
        // 0: a.low, 1: a.high, 3: zero or don't care
        const int s1 = (j0 < 0 ? 3 : (j0 & 4) >> 2) | (j2 < 0 ? 0x30 : (j2 & 4) << 2);
        const int s2 = (j1 < 0 ? 3 : (j1 & 4) >> 2) | (j3 < 0 ? 0x30 : (j3 & 4) << 2);

        // calculate cost of using VSHUFPS
        const int cost1 = (s1 == 0x01 || s1 == 0x11) ? 2 : (s1 == 0x00 || s1 == 0x03 || s1 == 0x31) ? 1 : 0;
        const int cost2 = (s2 == s1) ? 0 : (s2 == 0x01 || s2 == 0x11) ? 2 : (s2 == 0x00 || (s2 == 0x03 && (s1 & 0xF0) != 0x00) || (s2 == 0x31 && (s1 & 0x0F) != 0x01)) ? 1 : 0;

        if (cost1 + cost2 <= 3) {

            // permute mask
            const int sm = (k0 < 0 ? 0 : (k0 & 3)) | (k1 < 0 ? 1 : (k1 & 3)) << 2 | (k2 < 0 ? 2 : (k2 & 3)) << 4 | (k3 < 0 ? 3 : (k3 & 3)) << 6;

            // make operands for VSHUFPS
            __m256 r1, r2;

            switch (s1) {
            case 0x00:  // LL
            case 0x03:  // ZL
                r1 = _mm256_insertf128_ps(a,_mm256_castps256_ps128(a),1);  break;
            case 0x01:  // HL
                r1 = _mm256_castps128_ps256(_mm256_extractf128_ps(a,1));
                r1 = _mm256_insertf128_ps(r1,_mm256_castps256_ps128(a),1);  break;
            case 0x10:  // LH
            case 0x13:  // ZH
            case 0x30:  // LZ
            case 0x33:  // ZZ
                r1 = a;  break;
            case 0x11:  // HH
                r1 = _mm256_castps128_ps256(_mm256_extractf128_ps(a,1));
                r1 = _mm256_insertf128_ps(r1,_mm256_castps256_ps128(r1),1);  break;
            case 0x31:  // HZ
                r1 = _mm256_castps128_ps256(_mm256_extractf128_ps(a,1));  break;
            }

            if (s2 == s1) {
                if (sm == 0xE4) return r1;
                r2 = r1;
            }
            else {
                switch (s2) {
                case 0x00:  // LL
                    r2 = _mm256_insertf128_ps(a,_mm256_castps256_ps128(a),1);  break;
                case 0x03:  // ZL
                    if ((s1 & 0xF0) == 0x00) r2 = r1;
                    else {
                        r2 = _mm256_insertf128_ps(a,_mm256_castps256_ps128(a),1);
                    }
                    break;
                case 0x01:  // HL
                    r2 = _mm256_castps128_ps256(_mm256_extractf128_ps(a,1));
                    r2 = _mm256_insertf128_ps(r1,_mm256_castps256_ps128(a),1);  break;
                case 0x10:  // LH
                case 0x13:  // ZH
                case 0x30:  // LZ
                case 0x33:  // ZZ
                    r2 = a;  break;
                case 0x11:  // HH
                    r2 = _mm256_castps128_ps256(_mm256_extractf128_ps(a,1));
                    r2 = _mm256_insertf128_ps(r2,_mm256_castps256_ps128(r2),1);  break;
                case 0x31:  // HZ
                    if ((s1 & 0x0F) == 0x01) r2 = r1;
                    else {
                        r2 = _mm256_castps128_ps256(_mm256_extractf128_ps(a,1));
                    }
                    break;
                }
            }

            // now the permute instruction
            t1 = _mm256_shuffle_ps(r1, r2, sm);

            if (do_zero) {
                // zero some elements
                mask = constant8f< -int(i0>=0), -int(i1>=0), -int(i2>=0), -int(i3>=0), -int(i4>=0), -int(i5>=0), -int(i6>=0), -int(i7>=0) > ();
                t1 = _mm256_and_ps(t1, mask);     // zero with AND mask
            }
            return t1;
        }
    }
    // not using VSHUFPS. Split into low and high part
    Vec4f alo = a.get_low();
    Vec4f ahi = a.get_high();
    Vec4f rlo = blend4f<i0, i1, i2, i3> (alo, ahi);
    Vec4f rhi = blend4f<i4, i5, i6, i7> (alo, ahi);
    return Vec8f(rlo, rhi);
#endif
}


// blend vectors Vec8f
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8f blend8f(Vec8f const & a, Vec8f const & b) {

    const int ior = i0 | i1 | i2 | i3 | i4 | i5 | i6 | i7;  // OR indexes

    // is zeroing needed
    const bool do_zero  = ior < 0 && (ior & 0x80); // at least one index is negative, and not -0x100

    // Combine all the indexes into a single bitfield, with 4 bits for each
    const int m1 = (i0&0xF) | (i1&0xF)<<4 | (i2&0xF)<<8 | (i3&0xF)<<12 | (i4&0xF)<<16 | (i5&0xF)<<20 | (i6&0xF)<<24 | (i7&0xF)<<28;

    // Mask to zero out negative indexes
    const int mz = (i0<0?0:0xF) | (i1<0?0:0xF)<<4 | (i2<0?0:0xF)<<8 | (i3<0?0:0xF)<<12 | (i4<0?0:0xF)<<16 | (i5<0?0:0xF)<<20 | (i6<0?0:0xF)<<24 | (i7<0?0:0xF)<<28;

    __m256 t1, mask;

    if (mz == 0) return _mm256_setzero_ps();  // all zero

    if ((m1 & 0x88888888 & mz) == 0) {
        // all from a
        return permute8f<i0, i1, i2, i3, i4, i5, i6, i7> (a);
    }

    if (((m1 ^ 0x88888888) & 0x88888888 & mz) == 0) {
        // all from b
        return permute8f<i0&~8, i1&~8, i2&~8, i3&~8, i4&~8, i5&~8, i6&~8, i7&~8> (b);
    }

    if ((((m1 & 0x77777777) ^ 0x76543210) & mz) == 0) {
        // blend and zero, no permute
        mask = constant8f<(i0&8)?0:-1, (i1&8)?0:-1, (i2&8)?0:-1, (i3&8)?0:-1, (i4&8)?0:-1, (i5&8)?0:-1, (i6&8)?0:-1, (i7&8)?0:-1> ();
        t1   = select(mask, a, b);
        if (!do_zero) return t1;
        // zero some elements
        mask = constant8f< (i0<0&&(i0&8)) ? 0 : -1, (i1<0&&(i1&8)) ? 0 : -1, (i2<0&&(i2&8)) ? 0 : -1, (i3<0&&(i3&8)) ? 0 : -1, 
            (i4<0&&(i4&8)) ? 0 : -1, (i5<0&&(i5&8)) ? 0 : -1, (i6<0&&(i6&8)) ? 0 : -1, (i7<0&&(i7&8)) ? 0 : -1 > ();
        return _mm256_and_ps(t1, mask);
    }

    // check if we can do 128-bit blend/permute
    if (((m1 ^ 0x32103210) & 0x33333333 & mz) == 0) {
        const uint32_t j0 = (i0 >= 0 ? i0 : i1 >= 0 ? i1 : i2 >= 0 ? i2 : i3 >= 0 ? i3 : -1) >> 2;
        const uint32_t j1 = (i4 >= 0 ? i4 : i5 >= 0 ? i5 : i6 >= 0 ? i6 : i7 >= 0 ? i7 : -1) >> 2;
        if (((m1 ^ ((j0 & 3) * 0x00004444 | (j1 & 3) * 0x44440000)) & 0xCCCCCCCC & mz) == 0) {
            t1 = _mm256_permute2f128_ps(a, b, (j0 & 0x0F) | (j1 & 0x0F) << 4);
            const bool partialzero = (((i0 | i1 | i2 | i3) ^ j0) & 0x80) != 0 || (((i4 | i5 | i6 | i7) ^ j1) & 0x80) != 0;
            if (partialzero) {
                // zero some elements
                mask = constant8f< i0 < 0 ? 0 : -1, i1 < 0 ? 0 : -1, i2 < 0 ? 0 : -1, i3 < 0 ? 0 : -1, 
                    i4 < 0 ? 0 : -1, i5 < 0 ? 0 : -1, i6 < 0 ? 0 : -1, i7 < 0 ? 0 : -1 > ();
                return _mm256_and_ps(t1, mask);
            }
            else return t1;
        }
    }
    // Not checking special cases for vunpckhps, vunpcklps: they are too rare

    // Check if it is possible to use VSHUFPS. 
    // Index n must match index n+4 on bit 0-1, and even index n must match odd index n+1 on bit 2-3
    const bool sps = ((m1 ^ (m1 >> 16)) & 0x3333 & mz & (mz >> 16)) == 0  &&  ((m1 ^ (m1 >> 4)) & 0x0C0C0C0C & mz & mz >> 4) == 0;

    if (sps) {   // can use VSHUFPS

        // Index of each pair (i[n],i[n+1])
        const int j0 = i0 >= 0 ? i0 : i1;
        const int j1 = i2 >= 0 ? i2 : i3;
        const int j2 = i4 >= 0 ? i4 : i5;
        const int j3 = i6 >= 0 ? i6 : i7;

        // Index of each pair (i[n],i[n+4])
        const int k0 = i0 >= 0 ? i0 : i4;
        const int k1 = i1 >= 0 ? i1 : i5;
        const int k2 = i2 >= 0 ? i2 : i6;
        const int k3 = i3 >= 0 ? i3 : i7;

        // Needed contents of low/high part of each source register in VSHUFPS
        // 0: a.low, 1: a.high, 2: b.low, 3: b.high, 4: zero or don't care
        const int s1 = (j0 < 0 ? 4 : (j0 & 0xC) >> 2) | (j2 < 0 ? 0x30 : (j2 & 0xC) << 2);
        const int s2 = (j1 < 0 ? 3 : (j1 & 0xC) >> 2) | (j3 < 0 ? 0x30 : (j3 & 0xC) << 2);

        // permute mask
        const int sm = (k0 < 0 ? 0 : (k0 & 3)) | (k1 < 0 ? 1 : (k1 & 3)) << 2 | (k2 < 0 ? 2 : (k2 & 3)) << 4 | (k3 < 0 ? 3 : (k3 & 3)) << 6;

        __m256 r1, r2;
        __m128 ahi = _mm256_extractf128_ps(a,1);    // 1
        __m128 bhi = _mm256_extractf128_ps(b,1);    // 3

        switch (s1) {
        case 0x00:  case 0x04:
            r1 = _mm256_insertf128_ps(a,_mm256_castps256_ps128(a),1);  break;
        case 0x01:  case 0x41:
            r1 = _mm256_insertf128_ps(_mm256_castps128_ps256(ahi),_mm256_castps256_ps128(a),1);  break;
        case 0x02:
            r1 = _mm256_insertf128_ps(b,_mm256_castps256_ps128(a),1);  break;
        case 0x03:
            r1 = _mm256_insertf128_ps(_mm256_castps128_ps256(bhi),_mm256_castps256_ps128(a),1);  break;
        case 0x10:  case 0x14:  case 0x40:  case 0x44:
            r1 = a;  break;
        case 0x11:
            r1 = _mm256_insertf128_ps(_mm256_castps128_ps256(ahi),ahi,1);  break;
        case 0x12:
            r1 = _mm256_insertf128_ps(b,ahi,1);  break;
        case 0x13:
            r1 = _mm256_insertf128_ps(_mm256_castps128_ps256(bhi),ahi,1);  break;
        case 0x20:
            r1 = _mm256_insertf128_ps(a,_mm256_castps256_ps128(b),1);  break;
        case 0x21:
            r1 = _mm256_insertf128_ps(_mm256_castps128_ps256(ahi),_mm256_castps256_ps128(b),1);  break;
        case 0x22:  case 0x24:  case 0x42:
            r1 = _mm256_insertf128_ps(b,_mm256_castps256_ps128(b),1);  break;
        case 0x23:  case 0x43:
            r1 = _mm256_insertf128_ps(_mm256_castps128_ps256(bhi),_mm256_castps256_ps128(b),1);  break;
        case 0x30:
            r1 = _mm256_insertf128_ps(a,bhi,1);  break;
        case 0x31:
            r1 = _mm256_insertf128_ps(_mm256_castps128_ps256(ahi),bhi,1);  break;
        case 0x32:  case 0x34:
            r1 = b;  break;
        case 0x33:
            r1 = _mm256_insertf128_ps(_mm256_castps128_ps256(bhi),bhi,1);  break;
        }
        if (s2 == s1 || ((s2 & 0x04) && ((s1 ^ s2) & 0xF0) == 0) || ((s2 & 0x40) && ((s1 ^ s2) & 0x0F) == 0)) {
            // can use r2 = r1
            if (sm == 0xE4) return r1;  // no shuffling needed
            r2 = r1;
        }
        else {
            switch (s2) {
            case 0x00:  case 0x04:
                r2 = _mm256_insertf128_ps(a,_mm256_castps256_ps128(a),1);  break;
            case 0x01:  case 0x41:
                r2 = _mm256_insertf128_ps(_mm256_castps128_ps256(ahi),_mm256_castps256_ps128(a),1);  break;
            case 0x02:
                r2 = _mm256_insertf128_ps(b,_mm256_castps256_ps128(a),1);  break;
            case 0x03:
                r2 = _mm256_insertf128_ps(_mm256_castps128_ps256(bhi),_mm256_castps256_ps128(a),1);  break;
            case 0x10:  case 0x14:  case 0x40:  case 0x44:
                r2 = a;  break;
            case 0x11:
                r2 = _mm256_insertf128_ps(_mm256_castps128_ps256(ahi),ahi,1);  break;
            case 0x12:
                r2 = _mm256_insertf128_ps(b,ahi,1);  break;
            case 0x13:
                r2 = _mm256_insertf128_ps(_mm256_castps128_ps256(bhi),ahi,1);  break;
            case 0x20:
                r2 = _mm256_insertf128_ps(a,_mm256_castps256_ps128(b),1);  break;
            case 0x21:
                r2 = _mm256_insertf128_ps(_mm256_castps128_ps256(ahi),_mm256_castps256_ps128(b),1);  break;
            case 0x22:  case 0x24:  case 0x42:
                r2 = _mm256_insertf128_ps(b,_mm256_castps256_ps128(b),1);  break;
            case 0x23:  case 0x43:
                r2 = _mm256_insertf128_ps(_mm256_castps128_ps256(bhi),_mm256_castps256_ps128(b),1);  break;
            case 0x30:
                r2 = _mm256_insertf128_ps(a,bhi,1);  break;
            case 0x31:
                r2 = _mm256_insertf128_ps(_mm256_castps128_ps256(ahi),bhi,1);  break;
            case 0x32:  case 0x34:
                r2 = b;  break;
            case 0x33:
                r2 = _mm256_insertf128_ps(_mm256_castps128_ps256(bhi),bhi,1);  break;
            }
        }

        // now the shuffle instruction
        t1 = _mm256_shuffle_ps(r1, r2, sm);

        if (do_zero) {
            // zero some elements
            mask = constant8f< -int(i0>=0), -int(i1>=0), -int(i2>=0), -int(i3>=0), -int(i4>=0), -int(i5>=0), -int(i6>=0), -int(i7>=0) > ();
            t1 = _mm256_and_ps(t1, mask);     // zero with AND mask
        }
        return t1;
    }

    // Check if we can use 64-bit blend. Even numbered indexes must be even and odd numbered
    // indexes must be equal to the preceding index + 1, except for negative indexes.
    if (((m1 ^ 0x10101010) & 0x11111111 & mz) == 0 && ((m1 ^ m1 >> 4) & 0x0E0E0E0E & mz & mz >> 4) == 0) {

        const bool partialzero = int((i0 ^ i1) | (i2 ^ i3) | (i4 ^ i5) | (i6 ^ i7)) < 0; // part of a 64-bit block is zeroed
        const int blank1 = partialzero ? -0x100 : -1;  // ignore or zero
        const int n0 = i0 > 0 ? i0/2 : i1 > 0 ? i1/2 : blank1;  // indexes for 64 bit blend
        const int n1 = i2 > 0 ? i2/2 : i3 > 0 ? i3/2 : blank1;
        const int n2 = i4 > 0 ? i4/2 : i5 > 0 ? i5/2 : blank1;
        const int n3 = i6 > 0 ? i6/2 : i7 > 0 ? i7/2 : blank1;
        t1 = _mm256_castpd_ps (blend4d<n0,n1,n2,n3> (_mm256_castps_pd(a), _mm256_castps_pd(b)));
        if (blank1 == -1 || !do_zero) {    
            return  t1;
        }
        // need more zeroing
        mask = constant8f< -int(i0>=0), -int(i1>=0), -int(i2>=0), -int(i3>=0), -int(i4>=0), -int(i5>=0), -int(i6>=0), -int(i7>=0) > ();
        return _mm256_and_ps(t1, mask);     // zero with AND mask
    }

    // general case: permute and blend and possible zero
    const int blank2 = do_zero ? -1 : -0x100;  // ignore or zero

    Vec8f ta = permute8f <
        (uint32_t)i0 < 8 ? i0 : blank2,
        (uint32_t)i1 < 8 ? i1 : blank2,
        (uint32_t)i2 < 8 ? i2 : blank2,
        (uint32_t)i3 < 8 ? i3 : blank2,
        (uint32_t)i4 < 8 ? i4 : blank2,
        (uint32_t)i5 < 8 ? i5 : blank2,
        (uint32_t)i6 < 8 ? i6 : blank2,
        (uint32_t)i7 < 8 ? i7 : blank2 > (a);
    Vec8f tb = permute8f <
        (uint32_t)(i0^8) < 8 ? (i0^8) : blank2,
        (uint32_t)(i1^8) < 8 ? (i1^8) : blank2,
        (uint32_t)(i2^8) < 8 ? (i2^8) : blank2,
        (uint32_t)(i3^8) < 8 ? (i3^8) : blank2,
        (uint32_t)(i4^8) < 8 ? (i4^8) : blank2,
        (uint32_t)(i5^8) < 8 ? (i5^8) : blank2,
        (uint32_t)(i6^8) < 8 ? (i6^8) : blank2,
        (uint32_t)(i7^8) < 8 ? (i7^8) : blank2 > (b);

    if (blank2 == -1) {    
        return  _mm256_or_ps(ta, tb); 
    }
    // no zeroing, need to blend
    const int maskb = ((i0 >> 3) & 1) | ((i1 >> 2) & 2) | ((i2 >> 1) & 4) | (i3 & 8) | 
        ((i4 << 1) & 0x10) | ((i5 << 2) & 0x20) | ((i6 << 3) & 0x40) | ((i7 << 4) & 0x80);
    return _mm256_blend_ps(ta, tb, maskb);  // blend
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
* Vec4i a(2,0,0,3);               // index  a is (  2,   0,   0,   3)
* Vec4f b(1.0f,1.1f,1.2f,1.3f);   // table  b is (1.0, 1.1, 1.2, 1.3)
* Vec4f c;
* c = lookup4 (a,b);              // result c is (1.2, 1.0, 1.0, 1.3)
*
*****************************************************************************/

#ifdef VECTORI256_H  // Vec8i and Vec4q must be defined

static inline Vec8f lookup8(Vec8i const & index, Vec8f const & table) {
#if INSTRSET >= 8 && VECTORI256_H > 1 // AVX2
#if defined (_MSC_VER) && _MSC_VER < 1700 && ! defined(__INTEL_COMPILER)        
    // bug in MS VS 11 beta: operands in wrong order. fixed in 11.0
    return _mm256_permutevar8x32_ps(_mm256_castsi256_ps(index), _mm256_castps_si256(table)); 
#elif defined (GCC_VERSION) && GCC_VERSION <= 40700 && !defined(__INTEL_COMPILER) && !defined(__clang__)
        // Gcc 4.7.0 has wrong parameter type and operands in wrong order. fixed in version 4.7.1
    return _mm256_permutevar8x32_ps(_mm256_castsi256_ps(index), table);
#else
    // no bug version
    return _mm256_permutevar8x32_ps(table, index);
#endif

#else // AVX
    // swap low and high part of table
    __m256  t1 = _mm256_castps128_ps256(_mm256_extractf128_ps(table, 1));
    __m256  t2 = _mm256_insertf128_ps(t1, _mm256_castps256_ps128(table), 1);
    // join index parts
    __m256i index2 = _mm256_insertf128_si256(_mm256_castsi128_si256(index.get_low()), index.get_high(), 1);
    // permute within each 128-bit part
    __m256  r0 = _mm256_permutevar_ps(table, index2);
    __m256  r1 = _mm256_permutevar_ps(t2,    index2);
    // high index bit for blend
    __m128i k1 = _mm_slli_epi32(index.get_high() ^ 4, 29);
    __m128i k0 = _mm_slli_epi32(index.get_low(),      29);
    __m256  kk = _mm256_insertf128_ps(_mm256_castps128_ps256(_mm_castsi128_ps(k0)), _mm_castsi128_ps(k1), 1);
    // blend the two permutes
    return _mm256_blendv_ps(r0, r1, kk);
#endif
}

template <int n>
static inline Vec8f lookup(Vec8i const & index, float const * table) {
    if (n <= 0) return 0;
    if (n <= 4) {
        Vec4f table1 = Vec4f().load(table);        
        return Vec8f(       
            lookup4 (index.get_low(),  table1),
            lookup4 (index.get_high(), table1));
    }
#if INSTRSET < 8  // not AVX2
    if (n <= 8) {
        return lookup8(index, Vec8f().load(table));
    }
#endif
    // Limit index
    Vec8ui index1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec8ui(index) & (n-1);
    }
    else {
        // n is not a power of 2, limit to n-1
        index1 = min(Vec8ui(index), n-1);
    }
#if INSTRSET >= 8 && VECTORI256_H > 1 // AVX2
    return _mm256_i32gather_ps(table, index1, 4);
#else // AVX
    return Vec8f(table[index1[0]],table[index1[1]],table[index1[2]],table[index1[3]],
    table[index1[4]],table[index1[5]],table[index1[6]],table[index1[7]]);
#endif
}

static inline Vec4d lookup4(Vec4q const & index, Vec4d const & table) {
#if INSTRSET >= 8 && VECTORI256_H > 1 // AVX2
    // We can't use VPERMPD because it has constant indexes.
    // Convert the index to fit VPERMPS
    Vec8i index1 = permute8i<0,0,2,2,4,4,6,6> (Vec8i(index+index));
    Vec8i index2 = index1 + Vec8i(constant8i<0,1,0,1,0,1,0,1>());
#if defined (_MSC_VER) && _MSC_VER < 1700 && ! defined(__INTEL_COMPILER)        
    // bug in MS VS 11 beta: operands in wrong order. fixed in 11.0
    return _mm256_castps_pd(_mm256_permutevar8x32_ps(_mm256_castsi256_ps(index2), _mm256_castpd_si256(table))); 
#elif defined (GCC_VERSION) && GCC_VERSION <= 40700 && !defined(__INTEL_COMPILER) && !defined(__clang__)
        // Gcc 4.7.0 has wrong parameter type and operands in wrong order
    return _mm256_castps_pd(_mm256_permutevar8x32_ps(_mm256_castsi256_ps(index2), _mm256_castpd_ps(table)));
#else
    // no bug version
    return _mm256_castps_pd(_mm256_permutevar8x32_ps(_mm256_castpd_ps(table), index2));
#endif

#else // AVX
    // swap low and high part of table
    __m256d t1 = _mm256_castpd128_pd256(_mm256_extractf128_pd(table, 1));
    __m256d t2 = _mm256_insertf128_pd(t1, _mm256_castpd256_pd128(table), 1);
    // index << 1
    __m128i index2lo = index.get_low()  + index.get_low();
    __m128i index2hi = index.get_high() + index.get_high();
    // join index parts
    __m256i index3 = _mm256_insertf128_si256(_mm256_castsi128_si256(index2lo), index2hi, 1);
    // permute within each 128-bit part
    __m256d r0 = _mm256_permutevar_pd(table, index3);
    __m256d r1 = _mm256_permutevar_pd(t2,    index3);
    // high index bit for blend
    __m128i k1 = _mm_slli_epi64(index.get_high() ^ 2, 62);
    __m128i k0 = _mm_slli_epi64(index.get_low(),      62);
    __m256d kk = _mm256_insertf128_pd(_mm256_castpd128_pd256(_mm_castsi128_pd(k0)), _mm_castsi128_pd(k1), 1);
    // blend the two permutes
    return _mm256_blendv_pd(r0, r1, kk);
#endif
}

template <int n>
static inline Vec4d lookup(Vec4q const & index, double const * table) {
    if (n <= 0) return 0;
    if (n <= 2) {
        Vec2d table1 = Vec2d().load(table);        
        return Vec4d(       
            lookup2 (index.get_low(),  table1),
            lookup2 (index.get_high(), table1));
    }
#if INSTRSET < 8  // not AVX2
    if (n <= 4) {
        return lookup4(index, Vec4d().load(table));
    }
#endif
    // Limit index
    Vec8ui index1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec8ui(index) & constant8i<n-1, 0, n-1, 0, n-1, 0, n-1, 0>();
    }
    else {
        // n is not a power of 2, limit to n-1
        index1 = min(Vec8ui(index), constant8i<n-1, 0, n-1, 0, n-1, 0, n-1, 0>() );
    }
#if INSTRSET >= 8 && VECTORI256_H > 1 // AVX2
    return _mm256_i64gather_pd(table, index1, 8);
#else // AVX
    Vec4q index2 = Vec4q(index1);
    return Vec4d(table[index2[0]],table[index2[1]],table[index2[2]],table[index2[3]]);
#endif
}
#endif  // VECTORI256_H

/*****************************************************************************
*
*          Gather functions with fixed indexes
*
*****************************************************************************/
// Load elements from array a with indices i0, i1, i2, i3, ..
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8f gather8f(void const * a) {
    return reinterpret_f(gather8i<i0, i1, i2, i3, i4, i5, i6, i7>(a));
}

// Load elements from array a with indices i0, i1, i2, i3
template <int i0, int i1, int i2, int i3>
static inline Vec4d gather4d(void const * a) {
    return reinterpret_d(gather4q<i0, i1, i2, i3>(a));
}



/*****************************************************************************
*
*          Horizontal scan functions
*
*****************************************************************************/

// Get index to the first element that is true. Return -1 if all are false
static inline int horizontal_find_first(Vec8fb const & x) {
    return horizontal_find_first(Vec8ib(x));
}

static inline int horizontal_find_first(Vec4db const & x) {
    return horizontal_find_first(Vec4qb(x));
}

// Count the number of elements that are true
static inline uint32_t horizontal_count(Vec8fb const & x) {
    return horizontal_count(Vec8ib(x));
}

static inline uint32_t horizontal_count(Vec4db const & x) {
    return horizontal_count(Vec4qb(x));
}

/*****************************************************************************
*
*          Boolean <-> bitfield conversion functions
*
*****************************************************************************/

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec8fb const & x) {
    return to_bits(Vec8ib(x));
}

// to_Vec8fb: convert integer bitfield to boolean vector
static inline Vec8fb to_Vec8fb(uint8_t x) {
    return Vec8fb(to_Vec8ib(x));
}

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec4db const & x) {
    return to_bits(Vec4qb(x));
}

// to_Vec4db: convert integer bitfield to boolean vector
static inline Vec4db to_Vec4db(uint8_t x) {
    return Vec4db(to_Vec4qb(x));
}

#endif // VECTORF256_H
