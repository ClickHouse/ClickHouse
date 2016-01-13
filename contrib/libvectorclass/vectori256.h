/****************************  vectori256.h   *******************************
* Author:        Agner Fog
* Date created:  2012-05-30
* Last modified: 2014-10-16
* Version:       1.16
* Project:       vector classes
* Description:
* Header file defining integer vector classes as interface to intrinsic 
* functions in x86 microprocessors with AVX2 and later instruction sets.
*
* Instructions:
* Use Gnu, Intel or Microsoft C++ compiler. Compile for the desired 
* instruction set, which must be at least AVX2. 
*
* The following vector classes are defined here:
* Vec256b   Vector of 256  1-bit unsigned  integers or Booleans
* Vec32c    Vector of  32  8-bit signed    integers
* Vec32uc   Vector of  32  8-bit unsigned  integers
* Vec32cb   Vector of  32  Booleans for use with Vec32c and Vec32uc
* Vec16s    Vector of  16  16-bit signed   integers
* Vec16us   Vector of  16  16-bit unsigned integers
* Vec16sb   Vector of  16  Booleans for use with Vec16s and Vec16us
* Vec8i     Vector of   8  32-bit signed   integers
* Vec8ui    Vector of   8  32-bit unsigned integers
* Vec8ib    Vector of   8  Booleans for use with Vec8i and Vec8ui
* Vec4q     Vector of   4  64-bit signed   integers
* Vec4uq    Vector of   4  64-bit unsigned integers
* Vec4qb    Vector of   4  Booleans for use with Vec4q and Vec4uq
*
* Each vector object is represented internally in the CPU as a 256-bit register.
* This header file defines operators and functions for these vectors.
*
* For example:
* Vec8i a(1,2,3,4,5,6,7,8), b(9,10,11,12,13,14,15,16), c;
* c = a + b;     // now c contains (10,12,14,16,18,20,22,24)
*
* For detailed instructions, see VectorClass.pdf
*
* (c) Copyright 2012 - 2013 GNU General Public License http://www.gnu.org/licenses
*****************************************************************************/

// check combination of header files
#if defined (VECTORI256_H)
#if    VECTORI256_H != 2
#error Two different versions of vectori256.h included
#endif
#else
#define VECTORI256_H  2

#ifdef VECTORF256_H
#error Please put header file vectori256.h before vectorf256.h
#endif


#if INSTRSET < 8   // AVX2 required
#error Wrong instruction set for vectori256.h, AVX2 required or use vectori256e.h
#endif

#include "vectori128.h"


/*****************************************************************************
*
*         Join two 128-bit vectors
*
*****************************************************************************/
#define set_m128ir(lo,hi) _mm256_inserti128_si256(_mm256_castsi128_si256(lo),(hi),1)


/*****************************************************************************
*
*          Vector of 256 1-bit unsigned integers or Booleans
*
*****************************************************************************/
class Vec256b {
protected:
    __m256i ymm; // Integer vector
public:
    // Default constructor:
    Vec256b() {
    };
    // Constructor to broadcast the same value into all elements
    // Removed because of undesired implicit conversions
    //Vec256b(int i) {
    //    ymm = _mm256_set1_epi32(-(i & 1));}

    // Constructor to build from two Vec128b:
    Vec256b(Vec128b const & a0, Vec128b const & a1) {
        ymm = set_m128ir(a0, a1);
    }
    // Constructor to convert from type __m256i used in intrinsics:
    Vec256b(__m256i const & x) {
        ymm = x;
    };
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec256b & operator = (__m256i const & x) {
        ymm = x;
        return *this;
    };
    // Type cast operator to convert to __m256i used in intrinsics
    operator __m256i() const {
        return ymm;
    }
    // Member function to load from array (unaligned)
    Vec256b & load(void const * p) {
        ymm = _mm256_loadu_si256((__m256i const*)p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    // You may use load_a instead of load if you are certain that p points to an address
    // divisible by 32, but there is hardly any speed advantage of load_a on modern processors
    Vec256b & load_a(void const * p) {
        ymm = _mm256_load_si256((__m256i const*)p);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(void * p) const {
        _mm256_storeu_si256((__m256i*)p, ymm);
    }
    // Member function to store into array, aligned by 32
    // You may use store_a instead of store if you are certain that p points to an address
    // divisible by 32, but there is hardly any speed advantage of load_a on modern processors
    void store_a(void * p) const {
        _mm256_store_si256((__m256i*)p, ymm);
    }
    // Member function to change a single bit
    // Note: This function is inefficient. Use load function if changing more than one bit
    Vec256b const & set_bit(uint32_t index, int value) {
        static uint64_t m[8] = {0,0,0,0,1,0,0,0};
        int wi = (index >> 6) & 3;               // qword index
        int bi = index & 0x3F;                   // bit index within qword w

        __m256i mask = Vec256b().load(m+4-wi);   // 1 in qword number wi
        mask = _mm256_sll_epi64(mask,_mm_cvtsi32_si128(bi)); // mask with bit number b set
        if (value & 1) {
            ymm = _mm256_or_si256(mask,ymm);
        }
        else {
            ymm = _mm256_andnot_si256(mask,ymm);
        }
        return *this;
    }
    // Member function to get a single bit
    // Note: This function is inefficient. Use store function if reading more than one bit
    int get_bit(uint32_t index) const {
        union {
            __m256i x;
            uint8_t i[32];
        } u;
        u.x = ymm; 
        int wi = (index >> 3) & 0x1F;            // byte index
        int bi = index & 7;                      // bit index within byte w
        return (u.i[wi] >> bi) & 1;
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    bool operator [] (uint32_t index) const {
        return get_bit(index) != 0;
    }
    // Member functions to split into two Vec128b:
    Vec128b get_low() const {
        return _mm256_castsi256_si128(ymm);
    }
    Vec128b get_high() const {
        return _mm256_extractf128_si256(ymm,1);
    }
    static int size() {
        return 256;
    }
};


// Define operators for this class

// vector operator & : bitwise and
static inline Vec256b operator & (Vec256b const & a, Vec256b const & b) {
    return _mm256_and_si256(a, b);
}
static inline Vec256b operator && (Vec256b const & a, Vec256b const & b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec256b operator | (Vec256b const & a, Vec256b const & b) {
    return _mm256_or_si256(a, b);
}
static inline Vec256b operator || (Vec256b const & a, Vec256b const & b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec256b operator ^ (Vec256b const & a, Vec256b const & b) {
    return _mm256_xor_si256(a, b);
}

// vector operator ~ : bitwise not
static inline Vec256b operator ~ (Vec256b const & a) {
    return _mm256_xor_si256(a, _mm256_set1_epi32(-1));
}

// vector operator &= : bitwise and
static inline Vec256b & operator &= (Vec256b & a, Vec256b const & b) {
    a = a & b;
    return a;
}

// vector operator |= : bitwise or
static inline Vec256b & operator |= (Vec256b & a, Vec256b const & b) {
    a = a | b;
    return a;
}

// vector operator ^= : bitwise xor
static inline Vec256b & operator ^= (Vec256b & a, Vec256b const & b) {
    a = a ^ b;
    return a;
}

// Define functions for this class

// function andnot: a & ~ b
static inline Vec256b andnot (Vec256b const & a, Vec256b const & b) {
    return _mm256_andnot_si256(b, a);
}


/*****************************************************************************
*
*          Generate compile-time constant vector
*
*****************************************************************************/
// Generate a constant vector of 8 integers stored in memory.
// Can be converted to any integer vector type
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline __m256i constant8i() {
    static const union {
        int32_t i[8];
        __m256i ymm;
    } u = {{i0,i1,i2,i3,i4,i5,i6,i7}};
    return u.ymm;
}


/*****************************************************************************
*
*          selectb function
*
*****************************************************************************/
// Select between two sources, byte by byte. Used in various functions and operators
// Corresponds to this pseudocode:
// for (int i = 0; i < 32; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or 0xFF (true). No other values are allowed.
// Only bit 7 in each byte of s is checked, 
static inline __m256i selectb (__m256i const & s, __m256i const & a, __m256i const & b) {
    return _mm256_blendv_epi8 (b, a, s);
}



/*****************************************************************************
*
*          Horizontal Boolean functions
*
*****************************************************************************/

// horizontal_and. Returns true if all bits are 1
static inline bool horizontal_and (Vec256b const & a) {
    return _mm256_testc_si256(a,constant8i<-1,-1,-1,-1,-1,-1,-1,-1>()) != 0;
}

// horizontal_or. Returns true if at least one bit is 1
static inline bool horizontal_or (Vec256b const & a) {
    return ! _mm256_testz_si256(a,a);
}



/*****************************************************************************
*
*          Vector of 32 8-bit signed integers
*
*****************************************************************************/

class Vec32c : public Vec256b {
public:
    // Default constructor:
    Vec32c(){
    };
    // Constructor to broadcast the same value into all elements:
    Vec32c(int i) {
        ymm = _mm256_set1_epi8((char)i);
    };
    // Constructor to build from all elements:
    Vec32c(int8_t i0, int8_t i1, int8_t i2, int8_t i3, int8_t i4, int8_t i5, int8_t i6, int8_t i7,
        int8_t i8, int8_t i9, int8_t i10, int8_t i11, int8_t i12, int8_t i13, int8_t i14, int8_t i15,        
        int8_t i16, int8_t i17, int8_t i18, int8_t i19, int8_t i20, int8_t i21, int8_t i22, int8_t i23,
        int8_t i24, int8_t i25, int8_t i26, int8_t i27, int8_t i28, int8_t i29, int8_t i30, int8_t i31) {
        ymm = _mm256_setr_epi8(i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15,
            i16, i17, i18, i19, i20, i21, i22, i23, i24, i25, i26, i27, i28, i29, i30, i31);
    };
    // Constructor to build from two Vec16c:
    Vec32c(Vec16c const & a0, Vec16c const & a1) {
        ymm = set_m128ir(a0, a1);
    }
    // Constructor to convert from type __m256i used in intrinsics:
    Vec32c(__m256i const & x) {
        ymm = x;
    };
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec32c & operator = (__m256i const & x) {
        ymm = x;
        return *this;
    };
    // Type cast operator to convert to __m256i used in intrinsics
    operator __m256i() const {
        return ymm;
    }
    // Member function to load from array (unaligned)
    Vec32c & load(void const * p) {
        ymm = _mm256_loadu_si256((__m256i const*)p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec32c & load_a(void const * p) {
        ymm = _mm256_load_si256((__m256i const*)p);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec32c & load_partial(int n, void const * p) {
        if (n <= 0) {
            *this = 0;
        }
        else if (n <= 16) {
            *this = Vec32c(Vec16c().load_partial(n, p), 0);
        }
        else if (n < 32) {
            *this = Vec32c(Vec16c().load(p), Vec16c().load_partial(n-16, (char*)p+16));
        }
        else {
            load(p);
        }
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
        if (n <= 0) {
            return;
        }
        else if (n <= 16) {
            get_low().store_partial(n, p);
        }
        else if (n < 32) {
            get_low().store(p);
            get_high().store_partial(n-16, (char*)p+16);
        }
        else {
            store(p);
        }
    }
    // cut off vector to n elements. The last 32-n elements are set to zero
    Vec32c & cutoff(int n) {
        if (uint32_t(n) >= 32) return *this;
        static const union {
            int32_t i[16];
            char    c[64];
        } mask = {{-1,-1,-1,-1,-1,-1,-1,-1,0,0,0,0,0,0,0,0}};
        *this &= Vec32c().load(mask.c+32-n);
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec32c const & insert(uint32_t index, int8_t value) {
        static const int8_t maskl[64] = {0,0,0,0, 0,0,0,0, 0,0,0,0 ,0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,
            -1,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0 ,0,0,0,0, 0,0,0,0, 0,0,0,0};
        __m256i broad = _mm256_set1_epi8(value);  // broadcast value into all elements
        __m256i mask  = _mm256_loadu_si256((__m256i const*)(maskl+32-(index & 0x1F))); // mask with FF at index position
        ymm = selectb(mask,broad,ymm);
        return *this;
    }
    // Member function extract a single element from vector
    int8_t extract(uint32_t index) const {
        int8_t x[32];
        store(x);
        return x[index & 0x1F];
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int8_t operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec16c:
    Vec16c get_low() const {
        return _mm256_castsi256_si128(ymm);
    }
    Vec16c get_high() const {
#if defined (_MSC_VER) && _MSC_VER <= 1700 && ! defined(__INTEL_COMPILER)
        return _mm256_extractf128_si256(ymm,1);    // workaround bug in MS compiler VS 11
#else
        return _mm256_extracti128_si256(ymm,1);
#endif
    }
    static int size() {
        return 32;
    }
};


/*****************************************************************************
*
*          Vec32cb: Vector of 32 Booleans for use with Vec32c and Vec32uc
*
*****************************************************************************/

class Vec32cb : public Vec32c {
public:
    // Default constructor:
    Vec32cb(){
    }
    // Constructor to build from all elements:
    Vec32cb(bool x0, bool x1, bool x2, bool x3, bool x4, bool x5, bool x6, bool x7,
        bool x8, bool x9, bool x10, bool x11, bool x12, bool x13, bool x14, bool x15,
        bool x16, bool x17, bool x18, bool x19, bool x20, bool x21, bool x22, bool x23,
        bool x24, bool x25, bool x26, bool x27, bool x28, bool x29, bool x30, bool x31) :
        Vec32c(-int8_t(x0), -int8_t(x1), -int8_t(x2), -int8_t(x3), -int8_t(x4), -int8_t(x5), -int8_t(x6), -int8_t(x7), 
            -int8_t(x8), -int8_t(x9), -int8_t(x10), -int8_t(x11), -int8_t(x12), -int8_t(x13), -int8_t(x14), -int8_t(x15),
            -int8_t(x16), -int8_t(x17), -int8_t(x18), -int8_t(x19), -int8_t(x20), -int8_t(x21), -int8_t(x22), -int8_t(x23),
            -int8_t(x24), -int8_t(x25), -int8_t(x26), -int8_t(x27), -int8_t(x28), -int8_t(x29), -int8_t(x30), -int8_t(x31))
        {}
    // Constructor to convert from type __m256i used in intrinsics:
    Vec32cb(__m256i const & x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec32cb & operator = (__m256i const & x) {
        ymm = x;
        return *this;
    }
    // Constructor to broadcast scalar value:
    Vec32cb(bool b) : Vec32c(-int8_t(b)) {
    }
    // Assignment operator to broadcast scalar value:
    Vec32cb & operator = (bool b) {
        *this = Vec32cb(b);
        return *this;
    }
private: // Prevent constructing from int, etc.
    Vec32cb(int b);
    Vec32cb & operator = (int x);
public:
    // Member functions to split into two Vec16c:
    Vec16cb get_low() const {
        return Vec16cb(Vec32c::get_low());
    }
    Vec16cb get_high() const {
        return Vec16cb(Vec32c::get_high());
    }
    Vec32cb & insert (int index, bool a) {
        Vec32c::insert(index, -(int)a);
        return *this;
    }    
    // Member function extract a single element from vector
    bool extract(uint32_t index) const {
        return Vec32c::extract(index) != 0;
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    bool operator [] (uint32_t index) const {
        return extract(index);
    }
};


/*****************************************************************************
*
*          Define operators for Vec32cb
*
*****************************************************************************/

// vector operator & : bitwise and
static inline Vec32cb operator & (Vec32cb const & a, Vec32cb const & b) {
    return Vec32cb(Vec256b(a) & Vec256b(b));
}
static inline Vec32cb operator && (Vec32cb const & a, Vec32cb const & b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec32cb & operator &= (Vec32cb & a, Vec32cb const & b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec32cb operator | (Vec32cb const & a, Vec32cb const & b) {
    return Vec32cb(Vec256b(a) | Vec256b(b));
}
static inline Vec32cb operator || (Vec32cb const & a, Vec32cb const & b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec32cb & operator |= (Vec32cb & a, Vec32cb const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec32cb operator ^ (Vec32cb const & a, Vec32cb const & b) {
    return Vec32cb(Vec256b(a) ^ Vec256b(b));
}
// vector operator ^= : bitwise xor
static inline Vec32cb & operator ^= (Vec32cb & a, Vec32cb const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec32cb operator ~ (Vec32cb const & a) {
    return Vec32cb( ~ Vec256b(a));
}

// vector operator ! : element not
static inline Vec32cb operator ! (Vec32cb const & a) {
    return ~ a;
}

// vector function andnot
static inline Vec32cb andnot (Vec32cb const & a, Vec32cb const & b) {
    return Vec32cb(andnot(Vec256b(a), Vec256b(b)));
}


/*****************************************************************************
*
*          Operators for Vec32c
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec32c operator + (Vec32c const & a, Vec32c const & b) {
    return _mm256_add_epi8(a, b);
}

// vector operator += : add
static inline Vec32c & operator += (Vec32c & a, Vec32c const & b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec32c operator ++ (Vec32c & a, int) {
    Vec32c a0 = a;
    a = a + 1;
    return a0;
}

// prefix operator ++
static inline Vec32c & operator ++ (Vec32c & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec32c operator - (Vec32c const & a, Vec32c const & b) {
    return _mm256_sub_epi8(a, b);
}

// vector operator - : unary minus
static inline Vec32c operator - (Vec32c const & a) {
    return _mm256_sub_epi8(_mm256_setzero_si256(), a);
}

// vector operator -= : add
static inline Vec32c & operator -= (Vec32c & a, Vec32c const & b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec32c operator -- (Vec32c & a, int) {
    Vec32c a0 = a;
    a = a - 1;
    return a0;
}

// prefix operator --
static inline Vec32c & operator -- (Vec32c & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec32c operator * (Vec32c const & a, Vec32c const & b) {
    // There is no 8-bit multiply in SSE2. Split into two 16-bit multiplies
    __m256i aodd    = _mm256_srli_epi16(a,8);                 // odd numbered elements of a
    __m256i bodd    = _mm256_srli_epi16(b,8);                 // odd numbered elements of b
    __m256i muleven = _mm256_mullo_epi16(a,b);                // product of even numbered elements
    __m256i mulodd  = _mm256_mullo_epi16(aodd,bodd);          // product of odd  numbered elements
            mulodd  = _mm256_slli_epi16(mulodd,8);            // put odd numbered elements back in place
    __m256i mask    = _mm256_set1_epi32(0x00FF00FF);          // mask for even positions
    __m256i product = selectb(mask,muleven,mulodd);           // interleave even and odd
    return product;
}

// vector operator *= : multiply
static inline Vec32c & operator *= (Vec32c & a, Vec32c const & b) {
    a = a * b;
    return a;
}

// vector operator << : shift left all elements
static inline Vec32c operator << (Vec32c const & a, int b) {
    uint32_t mask = (uint32_t)0xFF >> (uint32_t)b;                // mask to remove bits that are shifted out
    __m256i am    = _mm256_and_si256(a,_mm256_set1_epi8((char)mask));// remove bits that will overflow
    __m256i res   = _mm256_sll_epi16(am,_mm_cvtsi32_si128(b));   // 16-bit shifts
    return res;
}

// vector operator <<= : shift left
static inline Vec32c & operator <<= (Vec32c & a, int b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic all elements
static inline Vec32c operator >> (Vec32c const & a, int b) {
    __m256i aeven = _mm256_slli_epi16(a,8);                            // even numbered elements of a. get sign bit in position
            aeven = _mm256_sra_epi16(aeven,_mm_cvtsi32_si128(b+8));    // shift arithmetic, back to position
    __m256i aodd  = _mm256_sra_epi16(a,_mm_cvtsi32_si128(b));          // shift odd numbered elements arithmetic
    __m256i mask  = _mm256_set1_epi32(0x00FF00FF);                     // mask for even positions
    __m256i res   = selectb(mask,aeven,aodd);                          // interleave even and odd
    return res;
}

// vector operator >>= : shift right artihmetic
static inline Vec32c & operator >>= (Vec32c & a, int b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec32cb operator == (Vec32c const & a, Vec32c const & b) {
    return _mm256_cmpeq_epi8(a,b);
}

// vector operator != : returns true for elements for which a != b
static inline Vec32cb operator != (Vec32c const & a, Vec32c const & b) {
#ifdef __XOP2__  // Possible future 256-bit XOP extension ?
    return _mm256_comneq_epi8(a,b);
#else  // AVX2 instruction set
    return Vec32cb(Vec32c(~(a == b)));
#endif
}

// vector operator > : returns true for elements for which a > b (signed)
static inline Vec32cb operator > (Vec32c const & a, Vec32c const & b) {
    return _mm256_cmpgt_epi8(a,b);
}

// vector operator < : returns true for elements for which a < b (signed)
static inline Vec32cb operator < (Vec32c const & a, Vec32c const & b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec32cb operator >= (Vec32c const & a, Vec32c const & b) {
#ifdef __XOP2__  // // Possible future 256-bit XOP extension ?
    return _mm256_comge_epi8(a,b);
#else  // SSE2 instruction set
    return Vec32cb(Vec32c(~(b > a)));
#endif
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec32cb operator <= (Vec32c const & a, Vec32c const & b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec32c operator & (Vec32c const & a, Vec32c const & b) {
    return Vec32c(Vec256b(a) & Vec256b(b));
}
static inline Vec32c operator && (Vec32c const & a, Vec32c const & b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec32c & operator &= (Vec32c & a, Vec32c const & b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec32c operator | (Vec32c const & a, Vec32c const & b) {
    return Vec32c(Vec256b(a) | Vec256b(b));
}
static inline Vec32c operator || (Vec32c const & a, Vec32c const & b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec32c & operator |= (Vec32c & a, Vec32c const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec32c operator ^ (Vec32c const & a, Vec32c const & b) {
    return Vec32c(Vec256b(a) ^ Vec256b(b));
}
// vector operator ^= : bitwise xor
static inline Vec32c & operator ^= (Vec32c & a, Vec32c const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec32c operator ~ (Vec32c const & a) {
    return Vec32c( ~ Vec256b(a));
}

// vector operator ! : logical not, returns true for elements == 0
static inline Vec32cb operator ! (Vec32c const & a) {
    return _mm256_cmpeq_epi8(a,_mm256_setzero_si256());
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or -1 (true). No other values are allowed.
static inline Vec32c select (Vec32cb const & s, Vec32c const & a, Vec32c const & b) {
    return selectb(s,a,b);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec32c if_add (Vec32cb const & f, Vec32c const & a, Vec32c const & b) {
    return a + (Vec32c(f) & b);
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
static inline uint32_t horizontal_add (Vec32c const & a) {
    __m256i sum1 = _mm256_sad_epu8(a,_mm256_setzero_si256());
    __m256i sum2 = _mm256_shuffle_epi32(sum1,2);
    __m256i sum3 = _mm256_add_epi16(sum1,sum2);
#if defined (_MSC_VER) && _MSC_VER <= 1700 && ! defined(__INTEL_COMPILER)
    __m128i sum4 = _mm256_extractf128_si256(sum3,1);                // bug in MS VS 11
#else
    __m128i sum4 = _mm256_extracti128_si256(sum3,1);
#endif
    __m128i sum5 = _mm_add_epi16(_mm256_castsi256_si128(sum3),sum4);
    int8_t  sum6 = (int8_t)_mm_cvtsi128_si32(sum5);                  // truncate to 8 bits
    return  sum6;                                                    // sign extend to 32 bits
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Each element is sign-extended before addition to avoid overflow
static inline int32_t horizontal_add_x (Vec32c const & a) {
    __m256i aeven = _mm256_slli_epi16(a,8);                          // even numbered elements of a. get sign bit in position
            aeven = _mm256_srai_epi16(aeven,8);                      // sign extend even numbered elements
    __m256i aodd  = _mm256_srai_epi16(a,8);                          // sign extend odd  numbered elements
    __m256i sum1  = _mm256_add_epi16(aeven,aodd);                    // add even and odd elements
    __m256i sum2  = _mm256_hadd_epi16(sum1,sum1);                    // horizontally add 2x8 elements in 3 steps
    __m256i sum3  = _mm256_hadd_epi16(sum2,sum2);
    __m256i sum4  = _mm256_hadd_epi16(sum3,sum3);
#if defined (_MSC_VER) && _MSC_VER <= 1700 && ! defined(__INTEL_COMPILER)
    __m128i sum5  = _mm256_extractf128_si256(sum4,1);                // bug in MS VS 11
#else
    __m128i sum5  = _mm256_extracti128_si256(sum4,1);                // get high sum
#endif
    __m128i sum6  = _mm_add_epi16(_mm256_castsi256_si128(sum4),sum5);// add high and low sum
    int16_t sum7  = (int16_t)_mm_cvtsi128_si32(sum6);                // 16 bit sum
    return  sum7;                                                    // sign extend to 32 bits
}

// function add_saturated: add element by element, signed with saturation
static inline Vec32c add_saturated(Vec32c const & a, Vec32c const & b) {
    return _mm256_adds_epi8(a, b);
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec32c sub_saturated(Vec32c const & a, Vec32c const & b) {
    return _mm256_subs_epi8(a, b);
}

// function max: a > b ? a : b
static inline Vec32c max(Vec32c const & a, Vec32c const & b) {
    return _mm256_max_epi8(a,b);
}

// function min: a < b ? a : b
static inline Vec32c min(Vec32c const & a, Vec32c const & b) {
    return _mm256_min_epi8(a,b);
}

// function abs: a >= 0 ? a : -a
static inline Vec32c abs(Vec32c const & a) {
    return _mm256_sign_epi8(a,a);
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec32c abs_saturated(Vec32c const & a) {
    __m256i absa   = abs(a);                                         // abs(a)
    __m256i overfl = _mm256_cmpgt_epi8(_mm256_setzero_si256(),absa); // 0 > a
    return           _mm256_add_epi8(absa,overfl);                   // subtract 1 if 0x80
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec32c rotate_left(Vec32c const & a, int b) {
#ifdef __XOP2__      // Possible future 256-bit XOP extension ?
    return _mm256_rot_epi8(a,_mm256_set1_epi8(b));
#else  // SSE2 instruction set
    __m128i bb        = _mm_cvtsi32_si128(b & 7);             // b modulo 8
    __m128i mbb       = _mm_cvtsi32_si128((8-b) & 7);         // 8-b modulo 8
    __m256i maskeven  = _mm256_set1_epi32(0x00FF00FF);        // mask for even numbered bytes
    __m256i even      = _mm256_and_si256(a,maskeven);         // even numbered bytes of a
    __m256i odd       = _mm256_andnot_si256(maskeven,a);      // odd numbered bytes of a
    __m256i evenleft  = _mm256_sll_epi16(even,bb);            // even bytes of a << b
    __m256i oddleft   = _mm256_sll_epi16(odd,bb);             // odd  bytes of a << b
    __m256i evenright = _mm256_srl_epi16(even,mbb);           // even bytes of a >> 8-b
    __m256i oddright  = _mm256_srl_epi16(odd,mbb);            // odd  bytes of a >> 8-b
    __m256i evenrot   = _mm256_or_si256(evenleft,evenright);  // even bytes of a rotated
    __m256i oddrot    = _mm256_or_si256(oddleft,oddright);    // odd  bytes of a rotated
    __m256i allrot    = selectb(maskeven,evenrot,oddrot);     // all  bytes rotated
    return  allrot;
#endif
}



/*****************************************************************************
*
*          Vector of 16 8-bit unsigned integers
*
*****************************************************************************/

class Vec32uc : public Vec32c {
public:
    // Default constructor:
    Vec32uc(){
    };
    // Constructor to broadcast the same value into all elements:
    Vec32uc(uint32_t i) {
        ymm = _mm256_set1_epi8((char)i);
    };
    // Constructor to build from all elements:
    Vec32uc(uint8_t i0, uint8_t i1, uint8_t i2, uint8_t i3, uint8_t i4, uint8_t i5, uint8_t i6, uint8_t i7,
        uint8_t i8, uint8_t i9, uint8_t i10, uint8_t i11, uint8_t i12, uint8_t i13, uint8_t i14, uint8_t i15,        
        uint8_t i16, uint8_t i17, uint8_t i18, uint8_t i19, uint8_t i20, uint8_t i21, uint8_t i22, uint8_t i23,
        uint8_t i24, uint8_t i25, uint8_t i26, uint8_t i27, uint8_t i28, uint8_t i29, uint8_t i30, uint8_t i31) {
        ymm = _mm256_setr_epi8(i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15,
            i16, i17, i18, i19, i20, i21, i22, i23, i24, i25, i26, i27, i28, i29, i30, i31);
    };
    // Constructor to build from two Vec16uc:
    Vec32uc(Vec16uc const & a0, Vec16uc const & a1) {
        ymm = set_m128ir(a0, a1);
    }
    // Constructor to convert from type __m256i used in intrinsics:
    Vec32uc(__m256i const & x) {
        ymm = x;
    };
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec32uc & operator = (__m256i const & x) {
        ymm = x;
        return *this;
    };
    // Member function to load from array (unaligned)
    Vec32uc & load(void const * p) {
        ymm = _mm256_loadu_si256((__m256i const*)p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec32uc & load_a(void const * p) {
        ymm = _mm256_load_si256((__m256i const*)p);
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec32uc const & insert(uint32_t index, uint8_t value) {
        Vec32c::insert(index, value);
        return *this;
    }
    // Member function extract a single element from vector
    uint8_t extract(uint32_t index) const {
        return Vec32c::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint8_t operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec16uc:
    Vec16uc get_low() const {
        return _mm256_castsi256_si128(ymm);
    }
    Vec16uc get_high() const {
        return _mm256_extractf128_si256(ymm,1);
    }
};

// Define operators for this class

// vector operator + : add
static inline Vec32uc operator + (Vec32uc const & a, Vec32uc const & b) {
    return Vec32uc (Vec32c(a) + Vec32c(b));
}

// vector operator - : subtract
static inline Vec32uc operator - (Vec32uc const & a, Vec32uc const & b) {
    return Vec32uc (Vec32c(a) - Vec32c(b));
}

// vector operator * : multiply
static inline Vec32uc operator * (Vec32uc const & a, Vec32uc const & b) {
    return Vec32uc (Vec32c(a) * Vec32c(b));
}

// vector operator << : shift left all elements
static inline Vec32uc operator << (Vec32uc const & a, uint32_t b) {
    uint32_t mask = (uint32_t)0xFF >> (uint32_t)b;                // mask to remove bits that are shifted out
    __m256i am    = _mm256_and_si256(a,_mm256_set1_epi8((char)mask));// remove bits that will overflow
    __m256i res   = _mm256_sll_epi16(am,_mm_cvtsi32_si128(b));    // 16-bit shifts
    return res;
}

// vector operator << : shift left all elements
static inline Vec32uc operator << (Vec32uc const & a, int32_t b) {
    return a << (uint32_t)b;
}

// vector operator >> : shift right logical all elements
static inline Vec32uc operator >> (Vec32uc const & a, uint32_t b) {
    uint32_t mask = (uint32_t)0xFF << (uint32_t)b;                // mask to remove bits that are shifted out
    __m256i am    = _mm256_and_si256(a,_mm256_set1_epi8((char)mask));// remove bits that will overflow
    __m256i res   = _mm256_srl_epi16(am,_mm_cvtsi32_si128(b));    // 16-bit shifts
    return res;
}

// vector operator >> : shift right logical all elements
static inline Vec32uc operator >> (Vec32uc const & a, int32_t b) {
    return a >> (uint32_t)b;
}

// vector operator >>= : shift right artihmetic
static inline Vec32uc & operator >>= (Vec32uc & a, uint32_t b) {
    a = a >> b;
    return a;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec32cb operator >= (Vec32uc const & a, Vec32uc const & b) {
#ifdef __XOP2__  // Possible future 256-bit XOP extension ?
    return _mm256_comge_epu8(a,b);
#else 
    return _mm256_cmpeq_epi8(_mm256_max_epu8(a,b), a); // a == max(a,b)
#endif
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec32cb operator <= (Vec32uc const & a, Vec32uc const & b) {
    return b >= a;
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec32cb operator > (Vec32uc const & a, Vec32uc const & b) {
#ifdef __XOP2__  // Possible future 256-bit XOP extension ?
    return _mm256_comgt_epu8(a,b);
#else  // SSE2 instruction set
    return Vec32cb(Vec32c(~(b >= a)));
#endif
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec32cb operator < (Vec32uc const & a, Vec32uc const & b) {
    return b > a;
}

// vector operator & : bitwise and
static inline Vec32uc operator & (Vec32uc const & a, Vec32uc const & b) {
    return Vec32uc(Vec256b(a) & Vec256b(b));
}
static inline Vec32uc operator && (Vec32uc const & a, Vec32uc const & b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec32uc operator | (Vec32uc const & a, Vec32uc const & b) {
    return Vec32uc(Vec256b(a) | Vec256b(b));
}
static inline Vec32uc operator || (Vec32uc const & a, Vec32uc const & b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec32uc operator ^ (Vec32uc const & a, Vec32uc const & b) {
    return Vec32uc(Vec256b(a) ^ Vec256b(b));
}

// vector operator ~ : bitwise not
static inline Vec32uc operator ~ (Vec32uc const & a) {
    return Vec32uc( ~ Vec256b(a));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 32; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or -1 (true). No other values are allowed.
// (s is signed)
static inline Vec32uc select (Vec32cb const & s, Vec32uc const & a, Vec32uc const & b) {
    return selectb(s,a,b);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec32uc if_add (Vec32cb const & f, Vec32uc const & a, Vec32uc const & b) {
    return a + (Vec32uc(f) & b);
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
// (Note: horizontal_add_x(Vec32uc) is slightly faster)
static inline uint32_t horizontal_add (Vec32uc const & a) {
    __m256i  sum1 = _mm256_sad_epu8(a,_mm256_setzero_si256());
    __m256i  sum2 = _mm256_shuffle_epi32(sum1,2);
    __m256i  sum3 = _mm256_add_epi16(sum1,sum2);
#if defined (_MSC_VER) && _MSC_VER <= 1700 && ! defined(__INTEL_COMPILER)
    __m128i  sum4 = _mm256_extractf128_si256(sum3,1); // bug in MS compiler VS 11
#else
    __m128i  sum4 = _mm256_extracti128_si256(sum3,1);
#endif
    __m128i  sum5 = _mm_add_epi16(_mm256_castsi256_si128(sum3),sum4);
    uint8_t  sum6 = (uint8_t)_mm_cvtsi128_si32(sum5); // truncate to 8 bits
    return   sum6;                                    // zero extend to 32 bits
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Each element is zero-extended before addition to avoid overflow
static inline uint32_t horizontal_add_x (Vec32uc const & a) {
    __m256i sum1 = _mm256_sad_epu8(a,_mm256_setzero_si256());
    __m256i sum2 = _mm256_shuffle_epi32(sum1,2);
    __m256i sum3 = _mm256_add_epi16(sum1,sum2);
#if defined (_MSC_VER) && _MSC_VER <= 1700 && ! defined(__INTEL_COMPILER)
    __m128i sum4 = _mm256_extractf128_si256(sum3,1); // bug in MS compiler VS 11
#else
    __m128i sum4 = _mm256_extracti128_si256(sum3,1);
#endif
    __m128i sum5 = _mm_add_epi16(_mm256_castsi256_si128(sum3),sum4);
    return         _mm_cvtsi128_si32(sum5);
}

// function add_saturated: add element by element, unsigned with saturation
static inline Vec32uc add_saturated(Vec32uc const & a, Vec32uc const & b) {
    return _mm256_adds_epu8(a, b);
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec32uc sub_saturated(Vec32uc const & a, Vec32uc const & b) {
    return _mm256_subs_epu8(a, b);
}

// function max: a > b ? a : b
static inline Vec32uc max(Vec32uc const & a, Vec32uc const & b) {
    return _mm256_max_epu8(a,b);
}

// function min: a < b ? a : b
static inline Vec32uc min(Vec32uc const & a, Vec32uc const & b) {
    return _mm256_min_epu8(a,b);
}


    
/*****************************************************************************
*
*          Vector of 16 16-bit signed integers
*
*****************************************************************************/

class Vec16s : public Vec256b {
public:
    // Default constructor:
    Vec16s() {
    };
    // Constructor to broadcast the same value into all elements:
    Vec16s(int i) {
        ymm = _mm256_set1_epi16((int16_t)i);
    };
    // Constructor to build from all elements:
    Vec16s(int16_t i0, int16_t i1, int16_t i2,  int16_t i3,  int16_t i4,  int16_t i5,  int16_t i6,  int16_t i7,
           int16_t i8, int16_t i9, int16_t i10, int16_t i11, int16_t i12, int16_t i13, int16_t i14, int16_t i15) {
        ymm = _mm256_setr_epi16(i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15 );
    };
    // Constructor to build from two Vec8s:
    Vec16s(Vec8s const & a0, Vec8s const & a1) {
        ymm = set_m128ir(a0, a1);
    }
    // Constructor to convert from type __m256i used in intrinsics:
    Vec16s(__m256i const & x) {
        ymm = x;
    };
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec16s & operator = (__m256i const & x) {
        ymm = x;
        return *this;
    };
    // Type cast operator to convert to __m256i used in intrinsics
    operator __m256i() const {
        return ymm;
    };
    // Member function to load from array (unaligned)
    Vec16s & load(void const * p) {
        ymm = _mm256_loadu_si256((__m256i const*)p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec16s & load_a(void const * p) {
        ymm = _mm256_load_si256((__m256i const*)p);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec16s & load_partial(int n, void const * p) {
        if (n <= 0) {
            *this = 0;
        }
        else if (n <= 8) {
            *this = Vec16s(Vec8s().load_partial(n, p), 0);
        }
        else if (n < 16) {
            *this = Vec16s(Vec8s().load(p), Vec8s().load_partial(n-8, (int16_t*)p+8));
        }
        else {
            load(p);
        }
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
        if (n <= 0) {
            return;
        }
        else if (n <= 8) {
            get_low().store_partial(n, p);
        }
        else if (n < 16) {
            get_low().store(p);
            get_high().store_partial(n-8, (int16_t*)p+8);
        }
        else {
            store(p);
        }
    }
    // cut off vector to n elements. The last 16-n elements are set to zero
    Vec16s & cutoff(int n) {
        *this = Vec32c(*this).cutoff(n * 2);
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec16s const & insert(uint32_t index, int16_t value) {
        static const int16_t m[32] = {0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, -1,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0};
        __m256i mask  = Vec256b().load(m + 16 - (index & 0x0F));
        __m256i broad = _mm256_set1_epi16(value);
        ymm = selectb(mask, broad, ymm);
        return *this;
    };
    // Member function extract a single element from vector
    int16_t extract(uint32_t index) const {
        int16_t x[16];
        store(x);
        return x[index & 0x0F];
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int16_t operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec8s:
    Vec8s get_low() const {
        return _mm256_castsi256_si128(ymm);
    }
    Vec8s get_high() const {
        return _mm256_extractf128_si256(ymm,1);
    }
    static int size() {
        return 16;
    }
};


/*****************************************************************************
*
*          Vec16sb: Vector of 16 Booleans for use with Vec16s and Vec16us
*
*****************************************************************************/
class Vec16sb : public Vec16s {
public:
    // Default constructor:
    Vec16sb() {
    }
    // Constructor to build from all elements:
    Vec16sb(bool x0, bool x1, bool x2, bool x3, bool x4, bool x5, bool x6, bool x7,
        bool x8, bool x9, bool x10, bool x11, bool x12, bool x13, bool x14, bool x15) :
        Vec16s(-int16_t(x0), -int16_t(x1), -int16_t(x2), -int16_t(x3), -int16_t(x4), -int16_t(x5), -int16_t(x6), -int16_t(x7), 
            -int16_t(x8), -int16_t(x9), -int16_t(x10), -int16_t(x11), -int16_t(x12), -int16_t(x13), -int16_t(x14), -int16_t(x15))
        {}
    // Constructor to convert from type __m256i used in intrinsics:
    Vec16sb(__m256i const & x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec16sb & operator = (__m256i const & x) {
        ymm = x;
        return *this;
    }
    // Constructor to broadcast scalar value:
    Vec16sb(bool b) : Vec16s(-int16_t(b)) {
    }
    // Assignment operator to broadcast scalar value:
    Vec16sb & operator = (bool b) {
        *this = Vec16sb(b);
        return *this;
    }
private: // Prevent constructing from int, etc.
    Vec16sb(int b);
    Vec16sb & operator = (int x);
public:
    Vec8sb get_low() const {
        return Vec8sb(Vec16s::get_low());
    }
    Vec8sb get_high() const {
        return Vec8sb(Vec16s::get_high());
    }
    Vec16sb & insert (int index, bool a) {
        Vec16s::insert(index, -(int)a);
        return *this;
    }    
    // Member function extract a single element from vector
    bool extract(uint32_t index) const {
        return Vec16s::extract(index) != 0;
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    bool operator [] (uint32_t index) const {
        return extract(index);
    }
};


/*****************************************************************************
*
*          Define operators for Vec16sb
*
*****************************************************************************/

// vector operator & : bitwise and
static inline Vec16sb operator & (Vec16sb const & a, Vec16sb const & b) {
    return Vec16sb(Vec256b(a) & Vec256b(b));
}
static inline Vec16sb operator && (Vec16sb const & a, Vec16sb const & b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec16sb & operator &= (Vec16sb & a, Vec16sb const & b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec16sb operator | (Vec16sb const & a, Vec16sb const & b) {
    return Vec16sb(Vec256b(a) | Vec256b(b));
}
static inline Vec16sb operator || (Vec16sb const & a, Vec16sb const & b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec16sb & operator |= (Vec16sb & a, Vec16sb const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec16sb operator ^ (Vec16sb const & a, Vec16sb const & b) {
    return Vec16sb(Vec256b(a) ^ Vec256b(b));
}
// vector operator ^= : bitwise xor
static inline Vec16sb & operator ^= (Vec16sb & a, Vec16sb const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec16sb operator ~ (Vec16sb const & a) {
    return Vec16sb( ~ Vec256b(a));
}

// vector operator ! : element not
static inline Vec16sb operator ! (Vec16sb const & a) {
    return ~ a;
}

// vector function andnot
static inline Vec16sb andnot (Vec16sb const & a, Vec16sb const & b) {
    return Vec16sb(andnot(Vec256b(a), Vec256b(b)));
}


/*****************************************************************************
*
*          Operators for Vec16s
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec16s operator + (Vec16s const & a, Vec16s const & b) {
    return _mm256_add_epi16(a, b);
}

// vector operator += : add
static inline Vec16s & operator += (Vec16s & a, Vec16s const & b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec16s operator ++ (Vec16s & a, int) {
    Vec16s a0 = a;
    a = a + 1;
    return a0;
}

// prefix operator ++
static inline Vec16s & operator ++ (Vec16s & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec16s operator - (Vec16s const & a, Vec16s const & b) {
    return _mm256_sub_epi16(a, b);
}

// vector operator - : unary minus
static inline Vec16s operator - (Vec16s const & a) {
    return _mm256_sub_epi16(_mm256_setzero_si256(), a);
}

// vector operator -= : subtract
static inline Vec16s & operator -= (Vec16s & a, Vec16s const & b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec16s operator -- (Vec16s & a, int) {
    Vec16s a0 = a;
    a = a - 1;
    return a0;
}

// prefix operator --
static inline Vec16s & operator -- (Vec16s & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec16s operator * (Vec16s const & a, Vec16s const & b) {
    return _mm256_mullo_epi16(a, b);
}

// vector operator *= : multiply
static inline Vec16s & operator *= (Vec16s & a, Vec16s const & b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer
// See bottom of file


// vector operator << : shift left
static inline Vec16s operator << (Vec16s const & a, int b) {
    return _mm256_sll_epi16(a,_mm_cvtsi32_si128(b));
}

// vector operator <<= : shift left
static inline Vec16s & operator <<= (Vec16s & a, int b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec16s operator >> (Vec16s const & a, int b) {
    return _mm256_sra_epi16(a,_mm_cvtsi32_si128(b));
}

// vector operator >>= : shift right arithmetic
static inline Vec16s & operator >>= (Vec16s & a, int b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec16sb operator == (Vec16s const & a, Vec16s const & b) {
    return _mm256_cmpeq_epi16(a, b);
}

// vector operator != : returns true for elements for which a != b
static inline Vec16sb operator != (Vec16s const & a, Vec16s const & b) {
#ifdef __XOP2__  // Possible future 256-bit XOP extension ?
    return _mm256_comneq_epi16(a,b);
#else  // SSE2 instruction set
    return Vec16sb(Vec16s(~(a == b)));
#endif
}

// vector operator > : returns true for elements for which a > b
static inline Vec16sb operator > (Vec16s const & a, Vec16s const & b) {
    return _mm256_cmpgt_epi16(a, b);
}

// vector operator < : returns true for elements for which a < b
static inline Vec16sb operator < (Vec16s const & a, Vec16s const & b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec16sb operator >= (Vec16s const & a, Vec16s const & b) {
#ifdef __XOP2__  // Possible future 256-bit XOP extension ?
    return _mm256_comge_epi16(a,b);
#else  // SSE2 instruction set
    return Vec16sb(Vec16s(~(b > a)));
#endif
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec16sb operator <= (Vec16s const & a, Vec16s const & b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec16s operator & (Vec16s const & a, Vec16s const & b) {
    return Vec16s(Vec256b(a) & Vec256b(b));
}
static inline Vec16s operator && (Vec16s const & a, Vec16s const & b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec16s & operator &= (Vec16s & a, Vec16s const & b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec16s operator | (Vec16s const & a, Vec16s const & b) {
    return Vec16s(Vec256b(a) | Vec256b(b));
}
static inline Vec16s operator || (Vec16s const & a, Vec16s const & b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec16s & operator |= (Vec16s & a, Vec16s const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec16s operator ^ (Vec16s const & a, Vec16s const & b) {
    return Vec16s(Vec256b(a) ^ Vec256b(b));
}
// vector operator ^= : bitwise xor
static inline Vec16s & operator ^= (Vec16s & a, Vec16s const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec16s operator ~ (Vec16s const & a) {
    return Vec16s( ~ Vec256b(a));
}

// vector operator ! : logical not, returns true for elements == 0
static inline Vec16sb operator ! (Vec16s const & a) {
    return _mm256_cmpeq_epi16(a,_mm256_setzero_si256());
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or -1 (true). No other values are allowed.
// (s is signed)
static inline Vec16s select (Vec16sb const & s, Vec16s const & a, Vec16s const & b) {
    return selectb(s,a,b);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec16s if_add (Vec16sb const & f, Vec16s const & a, Vec16s const & b) {
    return a + (Vec16s(f) & b);
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
static inline int32_t horizontal_add (Vec16s const & a) {
// #ifdef __XOP2__  // Possible future 256-bit XOP extension ?
    __m256i sum1  = _mm256_hadd_epi16(a,a);                           // horizontally add 2x8 elements in 3 steps
    __m256i sum2  = _mm256_hadd_epi16(sum1,sum1);
    __m256i sum3  = _mm256_hadd_epi16(sum2,sum2); 
#if defined (_MSC_VER) && _MSC_VER <= 1700 && ! defined(__INTEL_COMPILER)
    __m128i sum4  = _mm256_extractf128_si256(sum3,1);                 // bug in MS compiler VS 11
#else
    __m128i sum4  = _mm256_extracti128_si256(sum3,1);                 // get high part
#endif
    __m128i sum5  = _mm_add_epi16(_mm256_castsi256_si128(sum3),sum4); // add low and high parts
    int16_t sum6  = (int16_t)_mm_cvtsi128_si32(sum5);                 // truncate to 16 bits
    return  sum6;                                                     // sign extend to 32 bits
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Elements are sign extended before adding to avoid overflow
static inline int32_t horizontal_add_x (Vec16s const & a) {
    __m256i aeven = _mm256_slli_epi32(a,16);                  // even numbered elements of a. get sign bit in position
            aeven = _mm256_srai_epi32(aeven,16);              // sign extend even numbered elements
    __m256i aodd  = _mm256_srai_epi32(a,16);                  // sign extend odd  numbered elements
    __m256i sum1  = _mm256_add_epi32(aeven,aodd);             // add even and odd elements
    __m256i sum2  = _mm256_hadd_epi32(sum1,sum1);             // horizontally add 2x4 elements in 2 steps
    __m256i sum3  = _mm256_hadd_epi32(sum2,sum2);
#if defined (_MSC_VER) && _MSC_VER <= 1700 && ! defined(__INTEL_COMPILER)
    __m128i sum4  = _mm256_extractf128_si256(sum3,1);         // bug in MS compiler VS 11
#else
    __m128i sum4  = _mm256_extracti128_si256(sum3,1);
#endif
    __m128i sum5  = _mm_add_epi32(_mm256_castsi256_si128(sum3),sum4);
    return          _mm_cvtsi128_si32(sum5); 
}

// function add_saturated: add element by element, signed with saturation
static inline Vec16s add_saturated(Vec16s const & a, Vec16s const & b) {
    return _mm256_adds_epi16(a, b);
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec16s sub_saturated(Vec16s const & a, Vec16s const & b) {
    return _mm256_subs_epi16(a, b);
}

// function max: a > b ? a : b
static inline Vec16s max(Vec16s const & a, Vec16s const & b) {
    return _mm256_max_epi16(a,b);
}

// function min: a < b ? a : b
static inline Vec16s min(Vec16s const & a, Vec16s const & b) {
    return _mm256_min_epi16(a,b);
}

// function abs: a >= 0 ? a : -a
static inline Vec16s abs(Vec16s const & a) {
    return _mm256_sign_epi16(a,a);
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec16s abs_saturated(Vec16s const & a) {
    __m256i absa   = abs(a);                                  // abs(a)
    __m256i overfl = _mm256_srai_epi16(absa,15);              // sign
    return           _mm256_add_epi16(absa,overfl);           // subtract 1 if 0x8000
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec16s rotate_left(Vec16s const & a, int b) {
#ifdef __XOP2__  // Possible future 256-bit XOP extension ?
    return _mm256_rot_epi16(a,_mm256_set1_epi16(b));
#else  // SSE2 instruction set
    __m256i left  = _mm256_sll_epi16(a,_mm_cvtsi32_si128(b & 0x0F));      // a << b 
    __m256i right = _mm256_srl_epi16(a,_mm_cvtsi32_si128((16-b) & 0x0F)); // a >> (16 - b)
    __m256i rot   = _mm256_or_si256(left,right);                          // or
    return  rot;
#endif
}


/*****************************************************************************
*
*          Vector of 16 16-bit unsigned integers
*
*****************************************************************************/

class Vec16us : public Vec16s {
public:
    // Default constructor:
    Vec16us(){
    };
    // Constructor to broadcast the same value into all elements:
    Vec16us(uint32_t i) {
        ymm = _mm256_set1_epi16((int16_t)i);
    };
    // Constructor to build from all elements:
    Vec16us(uint16_t i0, uint16_t i1, uint16_t i2,  uint16_t i3,  uint16_t i4,  uint16_t i5,  uint16_t i6,  uint16_t i7,
            uint16_t i8, uint16_t i9, uint16_t i10, uint16_t i11, uint16_t i12, uint16_t i13, uint16_t i14, uint16_t i15) {
        ymm = _mm256_setr_epi16(i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15 );
    };
    // Constructor to build from two Vec8us:
    Vec16us(Vec8us const & a0, Vec8us const & a1) {
        ymm = set_m128ir(a0, a1);
    }
    // Constructor to convert from type __m256i used in intrinsics:
    Vec16us(__m256i const & x) {
        ymm = x;
    };
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec16us & operator = (__m256i const & x) {
        ymm = x;
        return *this;
    };
    // Member function to load from array (unaligned)
    Vec16us & load(void const * p) {
        ymm = _mm256_loadu_si256((__m256i const*)p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec16us & load_a(void const * p) {
        ymm = _mm256_load_si256((__m256i const*)p);
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec16us const & insert(uint32_t index, uint16_t value) {
        Vec16s::insert(index, value);
        return *this;
    };
    // Member function extract a single element from vector
    uint16_t extract(uint32_t index) const {
        return Vec16s::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint16_t operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec8us:
    Vec8us get_low() const {
        return _mm256_castsi256_si128(ymm);
    }
    Vec8us get_high() const {
        return _mm256_extractf128_si256(ymm,1);
    }
};

// Define operators for this class

// vector operator + : add
static inline Vec16us operator + (Vec16us const & a, Vec16us const & b) {
    return Vec16us (Vec16s(a) + Vec16s(b));
}

// vector operator - : subtract
static inline Vec16us operator - (Vec16us const & a, Vec16us const & b) {
    return Vec16us (Vec16s(a) - Vec16s(b));
}

// vector operator * : multiply
static inline Vec16us operator * (Vec16us const & a, Vec16us const & b) {
    return Vec16us (Vec16s(a) * Vec16s(b));
}

// vector operator / : divide
// See bottom of file

// vector operator >> : shift right logical all elements
static inline Vec16us operator >> (Vec16us const & a, uint32_t b) {
    return _mm256_srl_epi16(a,_mm_cvtsi32_si128(b)); 
}

// vector operator >> : shift right logical all elements
static inline Vec16us operator >> (Vec16us const & a, int32_t b) {
    return a >> (uint32_t)b;
}

// vector operator >>= : shift right artihmetic
static inline Vec16us & operator >>= (Vec16us & a, uint32_t b) {
    a = a >> b;
    return a;
}

// vector operator << : shift left all elements
static inline Vec16us operator << (Vec16us const & a, uint32_t b) {
    return _mm256_sll_epi16(a,_mm_cvtsi32_si128(b)); 
}

// vector operator << : shift left all elements
static inline Vec16us operator << (Vec16us const & a, int32_t b) {
    return a << (uint32_t)b;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec16sb operator >= (Vec16us const & a, Vec16us const & b) {
#ifdef __XOP2__  // Possible future 256-bit XOP extension ?
    return _mm256_comge_epu16(a,b);
#else
    __m256i max_ab = _mm256_max_epu16(a,b);                   // max(a,b), unsigned
    return _mm256_cmpeq_epi16(a,max_ab);                      // a == max(a,b)
#endif
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec16sb operator <= (Vec16us const & a, Vec16us const & b) {
    return b >= a;
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec16sb operator > (Vec16us const & a, Vec16us const & b) {
#ifdef __XOP2__  // Possible future 256-bit XOP extension ?
    return _mm256_comgt_epu16(a,b);
#else  // SSE2 instruction set
    return Vec16sb(Vec16s(~(b >= a)));
#endif
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec16sb operator < (Vec16us const & a, Vec16us const & b) {
    return b > a;
}

// vector operator & : bitwise and
static inline Vec16us operator & (Vec16us const & a, Vec16us const & b) {
    return Vec16us(Vec256b(a) & Vec256b(b));
}
static inline Vec16us operator && (Vec16us const & a, Vec16us const & b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec16us operator | (Vec16us const & a, Vec16us const & b) {
    return Vec16us(Vec256b(a) | Vec256b(b));
}
static inline Vec16us operator || (Vec16us const & a, Vec16us const & b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec16us operator ^ (Vec16us const & a, Vec16us const & b) {
    return Vec16us(Vec256b(a) ^ Vec256b(b));
}

// vector operator ~ : bitwise not
static inline Vec16us operator ~ (Vec16us const & a) {
    return Vec16us( ~ Vec256b(a));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 8; i++) result[i] = s[i] ? a[i] : b[i];
// Each word in s must be either 0 (false) or -1 (true). No other values are allowed.
// (s is signed)
static inline Vec16us select (Vec16sb const & s, Vec16us const & a, Vec16us const & b) {
    return selectb(s,a,b);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec16us if_add (Vec16sb const & f, Vec16us const & a, Vec16us const & b) {
    return a + (Vec16us(f) & b);
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
static inline uint32_t horizontal_add (Vec16us const & a) {
//#ifdef __XOP2__  // Possible future 256-bit XOP extension ?
    __m256i sum1  = _mm256_hadd_epi16(a,a);                           // horizontally add 2x8 elements in 3 steps
    __m256i sum2  = _mm256_hadd_epi16(sum1,sum1);
    __m256i sum3  = _mm256_hadd_epi16(sum2,sum2);
#if defined (_MSC_VER) && _MSC_VER <= 1700 && ! defined(__INTEL_COMPILER)
    __m128i sum4  = _mm256_extractf128_si256(sum3,1);                 // bug in MS compiler VS 11
#else
    __m128i sum4  = _mm256_extracti128_si256(sum3,1);                 // get high part
#endif
    __m128i sum5  = _mm_add_epi32(_mm256_castsi256_si128(sum3),sum4); // add low and high parts
    return          _mm_cvtsi128_si32(sum5);  
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Each element is zero-extended before addition to avoid overflow
static inline uint32_t horizontal_add_x (Vec16us const & a) {
    __m256i mask  = _mm256_set1_epi32(0x0000FFFF);                    // mask for even positions
    __m256i aeven = _mm256_and_si256(a,mask);                         // even numbered elements of a
    __m256i aodd  = _mm256_srli_epi32(a,16);                          // zero extend odd numbered elements
    __m256i sum1  = _mm256_add_epi32(aeven,aodd);                     // add even and odd elements
    __m256i sum2  = _mm256_hadd_epi32(sum1,sum1);                     // horizontally add 2x4 elements in 2 steps
    __m256i sum3  = _mm256_hadd_epi32(sum2,sum2);
#if defined (_MSC_VER) && _MSC_VER <= 1700 && ! defined(__INTEL_COMPILER)
    __m128i sum4  = _mm256_extractf128_si256(sum3,1);                 // bug in MS compiler VS 11
#else
    __m128i sum4  = _mm256_extracti128_si256(sum3,1);                 // get high part
#endif
    __m128i sum5  = _mm_add_epi32(_mm256_castsi256_si128(sum3),sum4); // add low and high parts
    return          _mm_cvtsi128_si32(sum5);  
}

// function add_saturated: add element by element, unsigned with saturation
static inline Vec16us add_saturated(Vec16us const & a, Vec16us const & b) {
    return _mm256_adds_epu16(a, b);
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec16us sub_saturated(Vec16us const & a, Vec16us const & b) {
    return _mm256_subs_epu16(a, b);
}

// function max: a > b ? a : b
static inline Vec16us max(Vec16us const & a, Vec16us const & b) {
    return _mm256_max_epu16(a,b);
}

// function min: a < b ? a : b
static inline Vec16us min(Vec16us const & a, Vec16us const & b) {
    return _mm256_min_epu16(a,b);
}


/*****************************************************************************
*
*          Vector of 8 32-bit signed integers
*
*****************************************************************************/

class Vec8i : public Vec256b {
public:
    // Default constructor:
    Vec8i() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec8i(int i) {
        ymm = _mm256_set1_epi32(i);
    }
    // Constructor to build from all elements:
    Vec8i(int32_t i0, int32_t i1, int32_t i2, int32_t i3, int32_t i4, int32_t i5, int32_t i6, int32_t i7) {
        ymm = _mm256_setr_epi32(i0, i1, i2, i3, i4, i5, i6, i7);
    }
    // Constructor to build from two Vec4i:
    Vec8i(Vec4i const & a0, Vec4i const & a1) {
        ymm = set_m128ir(a0, a1);
    }
    // Constructor to convert from type __m256i used in intrinsics:
    Vec8i(__m256i const & x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec8i & operator = (__m256i const & x) {
        ymm = x;
        return *this;
    }
    // Type cast operator to convert to __m256i used in intrinsics
    operator __m256i() const {
        return ymm;
    }
    // Member function to load from array (unaligned)
    Vec8i & load(void const * p) {
        ymm = _mm256_loadu_si256((__m256i const*)p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec8i & load_a(void const * p) {
        ymm = _mm256_load_si256((__m256i const*)p);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec8i & load_partial(int n, void const * p) {
        if (n <= 0) {
            *this = 0;
        }
        else if (n <= 4) {
            *this = Vec8i(Vec4i().load_partial(n, p), 0);
        }
        else if (n < 8) {
            *this = Vec8i(Vec4i().load(p), Vec4i().load_partial(n-4, (int32_t*)p+4));
        }
        else {
            load(p);
        }
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
        if (n <= 0) {
            return;
        }
        else if (n <= 4) {
            get_low().store_partial(n, p);
        }
        else if (n < 8) {
            get_low().store(p);
            get_high().store_partial(n-4, (int32_t*)p+4);
        }
        else {
            store(p);
        }
    }
    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec8i & cutoff(int n) {
        *this = Vec32c(*this).cutoff(n * 4);
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec8i const & insert(uint32_t index, int32_t value) {
        static const int32_t maskl[16] = {0,0,0,0,0,0,0,0, -1,0,0,0,0,0,0,0};
        __m256i broad = _mm256_set1_epi32(value);  // broadcast value into all elements
        __m256i mask  = Vec256b().load(maskl + 8 - (index & 7)); // mask with FFFFFFFF at index position
        ymm = selectb (mask, broad, ymm);
        return *this;
    }
    // Member function extract a single element from vector
    int32_t extract(uint32_t index) const {
        int32_t x[8];
        store(x);
        return x[index & 7];
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int32_t operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4i:
    Vec4i get_low() const {
        return _mm256_castsi256_si128(ymm);
    }
    Vec4i get_high() const {
        return _mm256_extractf128_si256(ymm,1);
    }
    static int size() {
        return 8;
    }
};


/*****************************************************************************
*
*          Vec8ib: Vector of 8 Booleans for use with Vec8i and Vec8ui
*
*****************************************************************************/

class Vec8ib : public Vec8i {
public:
    // Default constructor:
    Vec8ib() {
    }
    // Constructor to build from all elements:
    Vec8ib(bool x0, bool x1, bool x2, bool x3, bool x4, bool x5, bool x6, bool x7) :
        Vec8i(-int32_t(x0), -int32_t(x1), -int32_t(x2), -int32_t(x3), -int32_t(x4), -int32_t(x5), -int32_t(x6), -int32_t(x7))
        {}
    // Constructor to convert from type __m256i used in intrinsics:
    Vec8ib(__m256i const & x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec8ib & operator = (__m256i const & x) {
        ymm = x;
        return *this;
    }
    // Constructor to broadcast scalar value:
    Vec8ib(bool b) : Vec8i(-int32_t(b)) {
    }
    // Assignment operator to broadcast scalar value:
    Vec8ib & operator = (bool b) {
        *this = Vec8ib(b);
        return *this;
    }
private: // Prevent constructing from int, etc.
    Vec8ib(int b);
    Vec8ib & operator = (int x);
public:
    Vec4ib get_low() const {
        return Vec4ib(Vec8i::get_low());
    }
    Vec4ib get_high() const {
        return Vec4ib(Vec8i::get_high());
    }
    Vec8ib & insert (int index, bool a) {
        Vec8i::insert(index, -(int)a);
        return *this;
    }
    // Member function extract a single element from vector
    bool extract(uint32_t index) const {
        return Vec8i::extract(index) != 0;
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    bool operator [] (uint32_t index) const {
        return extract(index);
    }
};


/*****************************************************************************
*
*          Define operators for Vec8ib
*
*****************************************************************************/

// vector operator & : bitwise and
static inline Vec8ib operator & (Vec8ib const & a, Vec8ib const & b) {
    return Vec8ib(Vec256b(a) & Vec256b(b));
}
static inline Vec8ib operator && (Vec8ib const & a, Vec8ib const & b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec8ib & operator &= (Vec8ib & a, Vec8ib const & b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec8ib operator | (Vec8ib const & a, Vec8ib const & b) {
    return Vec8ib(Vec256b(a) | Vec256b(b));
}
static inline Vec8ib operator || (Vec8ib const & a, Vec8ib const & b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec8ib & operator |= (Vec8ib & a, Vec8ib const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec8ib operator ^ (Vec8ib const & a, Vec8ib const & b) {
    return Vec8ib(Vec256b(a) ^ Vec256b(b));
}
// vector operator ^= : bitwise xor
static inline Vec8ib & operator ^= (Vec8ib & a, Vec8ib const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec8ib operator ~ (Vec8ib const & a) {
    return Vec8ib( ~ Vec256b(a));
}

// vector operator ! : element not
static inline Vec8ib operator ! (Vec8ib const & a) {
    return ~ a;
}

// vector function andnot
static inline Vec8ib andnot (Vec8ib const & a, Vec8ib const & b) {
    return Vec8ib(andnot(Vec256b(a), Vec256b(b)));
}


/*****************************************************************************
*
*          Operators for Vec8i
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec8i operator + (Vec8i const & a, Vec8i const & b) {
    return _mm256_add_epi32(a, b);
}

// vector operator += : add
static inline Vec8i & operator += (Vec8i & a, Vec8i const & b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec8i operator ++ (Vec8i & a, int) {
    Vec8i a0 = a;
    a = a + 1;
    return a0;
}

// prefix operator ++
static inline Vec8i & operator ++ (Vec8i & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec8i operator - (Vec8i const & a, Vec8i const & b) {
    return _mm256_sub_epi32(a, b);
}

// vector operator - : unary minus
static inline Vec8i operator - (Vec8i const & a) {
    return _mm256_sub_epi32(_mm256_setzero_si256(), a);
}

// vector operator -= : subtract
static inline Vec8i & operator -= (Vec8i & a, Vec8i const & b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec8i operator -- (Vec8i & a, int) {
    Vec8i a0 = a;
    a = a - 1;
    return a0;
}

// prefix operator --
static inline Vec8i & operator -- (Vec8i & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec8i operator * (Vec8i const & a, Vec8i const & b) {
    return _mm256_mullo_epi32(a, b);
}

// vector operator *= : multiply
static inline Vec8i & operator *= (Vec8i & a, Vec8i const & b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer
// See bottom of file


// vector operator << : shift left
static inline Vec8i operator << (Vec8i const & a, int32_t b) {
    return _mm256_sll_epi32(a, _mm_cvtsi32_si128(b));
}

// vector operator <<= : shift left
static inline Vec8i & operator <<= (Vec8i & a, int32_t b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec8i operator >> (Vec8i const & a, int32_t b) {
    return _mm256_sra_epi32(a, _mm_cvtsi32_si128(b));
}

// vector operator >>= : shift right arithmetic
static inline Vec8i & operator >>= (Vec8i & a, int32_t b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec8ib operator == (Vec8i const & a, Vec8i const & b) {
    return _mm256_cmpeq_epi32(a, b);
}

// vector operator != : returns true for elements for which a != b
static inline Vec8ib operator != (Vec8i const & a, Vec8i const & b) {
#ifdef __XOP2__  // Possible future 256-bit XOP extension ?
    return _mm256_comneq_epi32(a,b);
#else  // SSE2 instruction set
    return Vec8ib(Vec8i(~(a == b)));
#endif
}
  
// vector operator > : returns true for elements for which a > b
static inline Vec8ib operator > (Vec8i const & a, Vec8i const & b) {
    return _mm256_cmpgt_epi32(a, b);
}

// vector operator < : returns true for elements for which a < b
static inline Vec8ib operator < (Vec8i const & a, Vec8i const & b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec8ib operator >= (Vec8i const & a, Vec8i const & b) {
#ifdef __XOP2__  // Possible future 256-bit XOP extension ?
    return _mm256_comge_epi32(a,b);
#else  // SSE2 instruction set
    return Vec8ib(Vec8i(~(b > a)));
#endif
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec8ib operator <= (Vec8i const & a, Vec8i const & b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec8i operator & (Vec8i const & a, Vec8i const & b) {
    return Vec8i(Vec256b(a) & Vec256b(b));
}
static inline Vec8i operator && (Vec8i const & a, Vec8i const & b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec8i & operator &= (Vec8i & a, Vec8i const & b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec8i operator | (Vec8i const & a, Vec8i const & b) {
    return Vec8i(Vec256b(a) | Vec256b(b));
}
static inline Vec8i operator || (Vec8i const & a, Vec8i const & b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec8i & operator |= (Vec8i & a, Vec8i const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec8i operator ^ (Vec8i const & a, Vec8i const & b) {
    return Vec8i(Vec256b(a) ^ Vec256b(b));
}
// vector operator ^= : bitwise xor
static inline Vec8i & operator ^= (Vec8i & a, Vec8i const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec8i operator ~ (Vec8i const & a) {
    return Vec8i( ~ Vec256b(a));
}

// vector operator ! : returns true for elements == 0
static inline Vec8ib operator ! (Vec8i const & a) {
    return _mm256_cmpeq_epi32(a, _mm256_setzero_si256());
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 8; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or -1 (true). No other values are allowed.
// (s is signed)
static inline Vec8i select (Vec8ib const & s, Vec8i const & a, Vec8i const & b) {
    return selectb(s,a,b);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec8i if_add (Vec8ib const & f, Vec8i const & a, Vec8i const & b) {
    return a + (Vec8i(f) & b);
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
static inline int32_t horizontal_add (Vec8i const & a) {
//#ifdef __XOP2__  // Possible future 256-bit XOP extension ?
    __m256i sum1  = _mm256_hadd_epi32(a,a);                           // horizontally add 2x4 elements in 2 steps
    __m256i sum2  = _mm256_hadd_epi32(sum1,sum1);
#if defined (_MSC_VER) && _MSC_VER <= 1700 && ! defined(__INTEL_COMPILER)
    __m128i sum3  = _mm256_extractf128_si256(sum2,1);                 // bug in MS VS 11
#else
    __m128i sum3  = _mm256_extracti128_si256(sum2,1);                 // get high part
#endif
    __m128i sum4  = _mm_add_epi32(_mm256_castsi256_si128(sum2),sum3); // add low and high parts
    return          _mm_cvtsi128_si32(sum4);
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Elements are sign extended before adding to avoid overflow
// static inline int64_t horizontal_add_x (Vec8i const & a); // defined below

// function add_saturated: add element by element, signed with saturation
static inline Vec8i add_saturated(Vec8i const & a, Vec8i const & b) {
    __m256i sum    = _mm256_add_epi32(a, b);                  // a + b
    __m256i axb    = _mm256_xor_si256(a, b);                  // check if a and b have different sign
    __m256i axs    = _mm256_xor_si256(a, sum);                // check if a and sum have different sign
    __m256i overf1 = _mm256_andnot_si256(axb,axs);            // check if sum has wrong sign
    __m256i overf2 = _mm256_srai_epi32(overf1,31);            // -1 if overflow
    __m256i asign  = _mm256_srli_epi32(a,31);                 // 1  if a < 0
    __m256i sat1   = _mm256_srli_epi32(overf2,1);             // 7FFFFFFF if overflow
    __m256i sat2   = _mm256_add_epi32(sat1,asign);            // 7FFFFFFF if positive overflow 80000000 if negative overflow
    return  selectb(overf2,sat2,sum);                         // sum if not overflow, else sat2
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec8i sub_saturated(Vec8i const & a, Vec8i const & b) {
    __m256i diff   = _mm256_sub_epi32(a, b);                  // a + b
    __m256i axb    = _mm256_xor_si256(a, b);                  // check if a and b have different sign
    __m256i axs    = _mm256_xor_si256(a, diff);               // check if a and sum have different sign
    __m256i overf1 = _mm256_and_si256(axb,axs);               // check if sum has wrong sign
    __m256i overf2 = _mm256_srai_epi32(overf1,31);            // -1 if overflow
    __m256i asign  = _mm256_srli_epi32(a,31);                 // 1  if a < 0
    __m256i sat1   = _mm256_srli_epi32(overf2,1);             // 7FFFFFFF if overflow
    __m256i sat2   = _mm256_add_epi32(sat1,asign);            // 7FFFFFFF if positive overflow 80000000 if negative overflow
    return  selectb(overf2,sat2,diff);                        // diff if not overflow, else sat2
}

// function max: a > b ? a : b
static inline Vec8i max(Vec8i const & a, Vec8i const & b) {
    return _mm256_max_epi32(a,b);
}

// function min: a < b ? a : b
static inline Vec8i min(Vec8i const & a, Vec8i const & b) {
    return _mm256_min_epi32(a,b);
}

// function abs: a >= 0 ? a : -a
static inline Vec8i abs(Vec8i const & a) {
    return _mm256_sign_epi32(a,a);
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec8i abs_saturated(Vec8i const & a) {
    __m256i absa   = abs(a);                                  // abs(a)
    __m256i overfl = _mm256_srai_epi32(absa,31);              // sign
    return           _mm256_add_epi32(absa,overfl);           // subtract 1 if 0x80000000
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec8i rotate_left(Vec8i const & a, int b) {
#ifdef __XOP2__  // Possible future 256-bit XOP extension ?
    return _mm256_rot_epi32(a,_mm_set1_epi32(b));
#else  // SSE2 instruction set
    __m256i left  = _mm256_sll_epi32(a,_mm_cvtsi32_si128(b & 0x1F));      // a << b 
    __m256i right = _mm256_srl_epi32(a,_mm_cvtsi32_si128((32-b) & 0x1F)); // a >> (32 - b)
    __m256i rot   = _mm256_or_si256(left,right);                          // or
    return  rot;
#endif
}


/*****************************************************************************
*
*          Vector of 8 32-bit unsigned integers
*
*****************************************************************************/

class Vec8ui : public Vec8i {
public:
    // Default constructor:
    Vec8ui() {
    };
    // Constructor to broadcast the same value into all elements:
    Vec8ui(uint32_t i) {
        ymm = _mm256_set1_epi32(i);
    };
    // Constructor to build from all elements:
    Vec8ui(uint32_t i0, uint32_t i1, uint32_t i2, uint32_t i3, uint32_t i4, uint32_t i5, uint32_t i6, uint32_t i7) {
        ymm = _mm256_setr_epi32(i0, i1, i2, i3, i4, i5, i6, i7);
    };
    // Constructor to build from two Vec4ui:
    Vec8ui(Vec4ui const & a0, Vec4ui const & a1) {
        ymm = set_m128ir(a0, a1);
    }
    // Constructor to convert from type __m256i used in intrinsics:
    Vec8ui(__m256i const & x) {
        ymm = x;
    };
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec8ui & operator = (__m256i const & x) {
        ymm = x;
        return *this;
    };
    // Member function to load from array (unaligned)
    Vec8ui & load(void const * p) {
        ymm = _mm256_loadu_si256((__m256i const*)p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec8ui & load_a(void const * p) {
        ymm = _mm256_load_si256((__m256i const*)p);
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec8ui const & insert(uint32_t index, uint32_t value) {
        Vec8i::insert(index, value);
        return *this;
    }
    // Member function extract a single element from vector
    uint32_t extract(uint32_t index) const {
        return Vec8i::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint32_t operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4ui:
    Vec4ui get_low() const {
        return _mm256_castsi256_si128(ymm);
    }
    Vec4ui get_high() const {
        return _mm256_extractf128_si256(ymm,1);
    }
};

// Define operators for this class

// vector operator + : add
static inline Vec8ui operator + (Vec8ui const & a, Vec8ui const & b) {
    return Vec8ui (Vec8i(a) + Vec8i(b));
}

// vector operator - : subtract
static inline Vec8ui operator - (Vec8ui const & a, Vec8ui const & b) {
    return Vec8ui (Vec8i(a) - Vec8i(b));
}

// vector operator * : multiply
static inline Vec8ui operator * (Vec8ui const & a, Vec8ui const & b) {
    return Vec8ui (Vec8i(a) * Vec8i(b));
}

// vector operator / : divide
// See bottom of file

// vector operator >> : shift right logical all elements
static inline Vec8ui operator >> (Vec8ui const & a, uint32_t b) {
    return _mm256_srl_epi32(a,_mm_cvtsi32_si128(b)); 
}

// vector operator >> : shift right logical all elements
static inline Vec8ui operator >> (Vec8ui const & a, int32_t b) {
    return a >> (uint32_t)b;
}

// vector operator >>= : shift right logical
static inline Vec8ui & operator >>= (Vec8ui & a, uint32_t b) {
    a = a >> b;
    return a;
} 

// vector operator << : shift left all elements
static inline Vec8ui operator << (Vec8ui const & a, uint32_t b) {
    return Vec8ui ((Vec8i)a << (int32_t)b);
}

// vector operator << : shift left all elements
static inline Vec8ui operator << (Vec8ui const & a, int32_t b) {
    return Vec8ui ((Vec8i)a << (int32_t)b);
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec8ib operator > (Vec8ui const & a, Vec8ui const & b) {
#ifdef __XOP2__  // Possible future 256-bit XOP extension ?
    return _mm256_comgt_epu32(a,b);
#else  // AVX2 instruction set
    __m256i signbit = _mm256_set1_epi32(0x80000000);
    __m256i a1      = _mm256_xor_si256(a,signbit);
    __m256i b1      = _mm256_xor_si256(b,signbit);
    return _mm256_cmpgt_epi32(a1,b1);                         // signed compare
#endif
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec8ib operator < (Vec8ui const & a, Vec8ui const & b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec8ib operator >= (Vec8ui const & a, Vec8ui const & b) {
#ifdef __XOP2__  // Possible future 256-bit XOP extension ?
    return _mm256_comge_epu32(a,b);
#else
    __m256i max_ab = _mm256_max_epu32(a,b);                   // max(a,b), unsigned
    return _mm256_cmpeq_epi32(a,max_ab);                      // a == max(a,b)
#endif
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec8ib operator <= (Vec8ui const & a, Vec8ui const & b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec8ui operator & (Vec8ui const & a, Vec8ui const & b) {
    return Vec8ui(Vec256b(a) & Vec256b(b));
}
static inline Vec8ui operator && (Vec8ui const & a, Vec8ui const & b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec8ui operator | (Vec8ui const & a, Vec8ui const & b) {
    return Vec8ui(Vec256b(a) | Vec256b(b));
}
static inline Vec8ui operator || (Vec8ui const & a, Vec8ui const & b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec8ui operator ^ (Vec8ui const & a, Vec8ui const & b) {
    return Vec8ui(Vec256b(a) ^ Vec256b(b));
}

// vector operator ~ : bitwise not
static inline Vec8ui operator ~ (Vec8ui const & a) {
    return Vec8ui( ~ Vec256b(a));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
// Each word in s must be either 0 (false) or -1 (true). No other values are allowed.
// (s is signed)
static inline Vec8ui select (Vec8ib const & s, Vec8ui const & a, Vec8ui const & b) {
    return selectb(s,a,b);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec8ui if_add (Vec8ib const & f, Vec8ui const & a, Vec8ui const & b) {
    return a + (Vec8ui(f) & b);
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
static inline uint32_t horizontal_add (Vec8ui const & a) {
    return horizontal_add((Vec8i)a);
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Elements are zero extended before adding to avoid overflow
// static inline uint64_t horizontal_add_x (Vec8ui const & a); // defined later

// function add_saturated: add element by element, unsigned with saturation
static inline Vec8ui add_saturated(Vec8ui const & a, Vec8ui const & b) {
    Vec8ui sum      = a + b;
    Vec8ui aorb     = Vec8ui(a | b);
    Vec8ui overflow = Vec8ui(sum < aorb);                  // overflow if a + b < (a | b)
    return Vec8ui (sum | overflow);                        // return 0xFFFFFFFF if overflow
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec8ui sub_saturated(Vec8ui const & a, Vec8ui const & b) {
    Vec8ui diff      = a - b;
    Vec8ui underflow = Vec8ui(diff > a);                   // underflow if a - b > a
    return _mm256_andnot_si256(underflow,diff);            // return 0 if underflow
}

// function max: a > b ? a : b
static inline Vec8ui max(Vec8ui const & a, Vec8ui const & b) {
    return _mm256_max_epu32(a,b);
}

// function min: a < b ? a : b
static inline Vec8ui min(Vec8ui const & a, Vec8ui const & b) {
    return _mm256_min_epu32(a,b);
}


/*****************************************************************************
*
*          Vector of 4 64-bit signed integers
*
*****************************************************************************/

class Vec4q : public Vec256b {
public:
    // Default constructor:
    Vec4q() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec4q(int64_t i) {
#if defined (_MSC_VER) && ! defined (__x86_64__) && ! defined(__INTEL_COMPILER)
        // MS compiler cannot use _mm256_set1_epi64x in 32 bit mode, and  
        // cannot put 64-bit values into xmm register without using
        // mmx registers, and it makes no emms
        union {
            int64_t q[4];
            int32_t r[8];
        } u;
        u.q[0] = u.q[1] = u.q[2] = u.q[3] = i;
        ymm = _mm256_setr_epi32(u.r[0], u.r[1], u.r[2], u.r[3], u.r[4], u.r[5], u.r[6], u.r[7]);
#else
        ymm = _mm256_set1_epi64x(i);
#endif
    }
    // Constructor to build from all elements:
    Vec4q(int64_t i0, int64_t i1, int64_t i2, int64_t i3) {
#if defined (_MSC_VER) && ! defined (__x86_64__) && ! defined(__INTEL_COMPILER)
        // MS compiler cannot put 64-bit values into xmm register without using
        // mmx registers, and it makes no emms
        union {
            int64_t q[4];
            int32_t r[8];
        } u;
        u.q[0] = i0;  u.q[1] = i1;  u.q[2] = i2;  u.q[3] = i3;
        ymm = _mm256_setr_epi32(u.r[0], u.r[1], u.r[2], u.r[3], u.r[4], u.r[5], u.r[6], u.r[7]);
#else
        ymm = _mm256_setr_epi64x(i0, i1, i2, i3);
#endif
    }
    // Constructor to build from two Vec2q:
    Vec4q(Vec2q const & a0, Vec2q const & a1) {
        ymm = set_m128ir(a0, a1);
    }
    // Constructor to convert from type __m256i used in intrinsics:
    Vec4q(__m256i const & x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec4q & operator = (__m256i const & x) {
        ymm = x;
        return *this;
    }
    // Type cast operator to convert to __m256i used in intrinsics
    operator __m256i() const {
        return ymm;
    }
    // Member function to load from array (unaligned)
    Vec4q & load(void const * p) {
        ymm = _mm256_loadu_si256((__m256i const*)p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec4q & load_a(void const * p) {
        ymm = _mm256_load_si256((__m256i const*)p);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec4q & load_partial(int n, void const * p) {
        if (n <= 0) {
            *this = 0;
        }
        else if (n <= 2) {
            *this = Vec4q(Vec2q().load_partial(n, p), 0);
        }
        else if (n < 4) {
            *this = Vec4q(Vec2q().load(p), Vec2q().load_partial(n-2, (int64_t*)p+2));
        }
        else {
            load(p);
        }
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
        if (n <= 0) {
            return;
        }
        else if (n <= 2) {
            get_low().store_partial(n, p);
        }
        else if (n < 4) {
            get_low().store(p);
            get_high().store_partial(n-2, (int64_t*)p+2);
        }
        else {
            store(p);
        }
    }
    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec4q & cutoff(int n) {
        *this = Vec32c(*this).cutoff(n * 8);
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec4q const & insert(uint32_t index, int64_t value) {
        Vec4q x(value);
        switch (index) {
        case 0:        
            ymm = _mm256_blend_epi32(ymm,x,0x03);  break;
        case 1:
            ymm = _mm256_blend_epi32(ymm,x,0x0C);  break;
        case 2:
            ymm = _mm256_blend_epi32(ymm,x,0x30);  break;
        case 3:
            ymm = _mm256_blend_epi32(ymm,x,0xC0);  break;
        }
        return *this;
    }
    // Member function extract a single element from vector
    int64_t extract(uint32_t index) const {
        int64_t x[4];
        store(x);
        return x[index & 3];
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int64_t operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec2q:
    Vec2q get_low() const {
        return _mm256_castsi256_si128(ymm);
    }
    Vec2q get_high() const {
        return _mm256_extractf128_si256(ymm,1);
    }
    static int size() {
        return 4;
    }
};

/*****************************************************************************
*
*          Vec4qb: Vector of 4 Booleans for use with Vec4q and Vec4uq
*
*****************************************************************************/

class Vec4qb : public Vec4q {
public:
    // Default constructor:
    Vec4qb() {
    }
    // Constructor to build from all elements:
    Vec4qb(bool x0, bool x1, bool x2, bool x3) :
        Vec4q(-int64_t(x0), -int64_t(x1), -int64_t(x2), -int64_t(x3)) {
    }
    // Constructor to convert from type __m256i used in intrinsics:
    Vec4qb(__m256i const & x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec4qb & operator = (__m256i const & x) {
        ymm = x;
        return *this;
    }
    // Constructor to broadcast scalar value:
    Vec4qb(bool b) : Vec4q(-int64_t(b)) {
    }
    // Assignment operator to broadcast scalar value:
    Vec4qb & operator = (bool b) {
        *this = Vec4qb(b);
        return *this;
    }
private: // Prevent constructing from int, etc.
    Vec4qb(int b);
    Vec4qb & operator = (int x);
public:
    // Member functions to split into two Vec2qb:
    Vec2qb get_low() const {
        return Vec2qb(Vec4q::get_low());
    }
    Vec2qb get_high() const {
        return Vec2qb(Vec4q::get_high());
    }
    Vec4qb & insert (int index, bool a) {
        Vec4q::insert(index, -(int64_t)a);
        return *this;
    };    
    // Member function extract a single element from vector
    bool extract(uint32_t index) const {
        return Vec4q::extract(index) != 0;
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    bool operator [] (uint32_t index) const {
        return extract(index);
    }
};


/*****************************************************************************
*
*          Define operators for Vec4qb
*
*****************************************************************************/

// vector operator & : bitwise and
static inline Vec4qb operator & (Vec4qb const & a, Vec4qb const & b) {
    return Vec4qb(Vec256b(a) & Vec256b(b));
}
static inline Vec4qb operator && (Vec4qb const & a, Vec4qb const & b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec4qb & operator &= (Vec4qb & a, Vec4qb const & b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec4qb operator | (Vec4qb const & a, Vec4qb const & b) {
    return Vec4qb(Vec256b(a) | Vec256b(b));
}
static inline Vec4qb operator || (Vec4qb const & a, Vec4qb const & b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec4qb & operator |= (Vec4qb & a, Vec4qb const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec4qb operator ^ (Vec4qb const & a, Vec4qb const & b) {
    return Vec4qb(Vec256b(a) ^ Vec256b(b));
}
// vector operator ^= : bitwise xor
static inline Vec4qb & operator ^= (Vec4qb & a, Vec4qb const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec4qb operator ~ (Vec4qb const & a) {
    return Vec4qb( ~ Vec256b(a));
}

// vector operator ! : element not
static inline Vec4qb operator ! (Vec4qb const & a) {
    return ~ a;
}

// vector function andnot
static inline Vec4qb andnot (Vec4qb const & a, Vec4qb const & b) {
    return Vec4qb(andnot(Vec256b(a), Vec256b(b)));
}




/*****************************************************************************
*
*          Operators for Vec4q
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec4q operator + (Vec4q const & a, Vec4q const & b) {
    return _mm256_add_epi64(a, b);
}

// vector operator += : add
static inline Vec4q & operator += (Vec4q & a, Vec4q const & b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec4q operator ++ (Vec4q & a, int) {
    Vec4q a0 = a;
    a = a + 1;
    return a0;
}

// prefix operator ++
static inline Vec4q & operator ++ (Vec4q & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec4q operator - (Vec4q const & a, Vec4q const & b) {
    return _mm256_sub_epi64(a, b);
}

// vector operator - : unary minus
static inline Vec4q operator - (Vec4q const & a) {
    return _mm256_sub_epi64(_mm256_setzero_si256(), a);
}

// vector operator -= : subtract
static inline Vec4q & operator -= (Vec4q & a, Vec4q const & b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec4q operator -- (Vec4q & a, int) {
    Vec4q a0 = a;
    a = a - 1;
    return a0;
}

// prefix operator --
static inline Vec4q & operator -- (Vec4q & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec4q operator * (Vec4q const & a, Vec4q const & b) {
    // instruction does not exist. Split into 32-bit multiplies
    __m256i bswap   = _mm256_shuffle_epi32(b,0xB1);           // swap H<->L
    __m256i prodlh  = _mm256_mullo_epi32(a,bswap);            // 32 bit L*H products
    __m256i zero    = _mm256_setzero_si256();                 // 0
    __m256i prodlh2 = _mm256_hadd_epi32(prodlh,zero);         // a0Lb0H+a0Hb0L,a1Lb1H+a1Hb1L,0,0
    __m256i prodlh3 = _mm256_shuffle_epi32(prodlh2,0x73);     // 0, a0Lb0H+a0Hb0L, 0, a1Lb1H+a1Hb1L
    __m256i prodll  = _mm256_mul_epu32(a,b);                  // a0Lb0L,a1Lb1L, 64 bit unsigned products
    __m256i prod    = _mm256_add_epi64(prodll,prodlh3);       // a0Lb0L+(a0Lb0H+a0Hb0L)<<32, a1Lb1L+(a1Lb1H+a1Hb1L)<<32
    return  prod;
}

// vector operator *= : multiply
static inline Vec4q & operator *= (Vec4q & a, Vec4q const & b) {
    a = a * b;
    return a;
}

// vector operator << : shift left
static inline Vec4q operator << (Vec4q const & a, int32_t b) {
    return _mm256_sll_epi64(a, _mm_cvtsi32_si128(b));
}

// vector operator <<= : shift left
static inline Vec4q & operator <<= (Vec4q & a, int32_t b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec4q operator >> (Vec4q const & a, int32_t b) {
    // instruction does not exist. Split into 32-bit shifts
    if (b <= 32) {
        __m128i bb   = _mm_cvtsi32_si128(b);                   // b
        __m256i sra  = _mm256_sra_epi32(a,bb);                 // a >> b signed dwords
        __m256i srl  = _mm256_srl_epi64(a,bb);                 // a >> b unsigned qwords
        __m256i mask = constant8i<0,-1,0,-1,0,-1,0,-1>();      // mask for signed high part
        return  selectb(mask, sra, srl);
    }
    else {  // b > 32
        __m128i bm32 = _mm_cvtsi32_si128(b-32);                // b - 32
        __m256i sign = _mm256_srai_epi32(a,31);                // sign of a
        __m256i sra2 = _mm256_sra_epi32(a,bm32);               // a >> (b-32) signed dwords
        __m256i sra3 = _mm256_srli_epi64(sra2,32);             // a >> (b-32) >> 32 (second shift unsigned qword)
        __m256i mask = constant8i<0,-1,0,-1,0,-1,0,-1>();      // mask for high part containing only sign
        return  selectb(mask, sign ,sra3);
    }
}

// vector operator >>= : shift right arithmetic
static inline Vec4q & operator >>= (Vec4q & a, int32_t b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec4qb operator == (Vec4q const & a, Vec4q const & b) {
    return _mm256_cmpeq_epi64(a, b);
}

// vector operator != : returns true for elements for which a != b
static inline Vec4qb operator != (Vec4q const & a, Vec4q const & b) {
#ifdef __XOP2__  // Possible future 256-bit XOP extension ?
    return _mm256_comneq_epi64(a,b);
#else 
    return Vec4qb(Vec4q(~(a == b)));
#endif
}
  
// vector operator < : returns true for elements for which a < b
static inline Vec4qb operator < (Vec4q const & a, Vec4q const & b) {
    return _mm256_cmpgt_epi64(b, a);
}

// vector operator > : returns true for elements for which a > b
static inline Vec4qb operator > (Vec4q const & a, Vec4q const & b) {
    return b < a;
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec4qb operator >= (Vec4q const & a, Vec4q const & b) {
#ifdef __XOP2__  // Possible future 256-bit XOP extension ?
    return _mm256_comge_epi64(a,b);
#else  // SSE2 instruction set
    return Vec4qb(Vec4q(~(a < b)));
#endif
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec4qb operator <= (Vec4q const & a, Vec4q const & b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec4q operator & (Vec4q const & a, Vec4q const & b) {
    return Vec4q(Vec256b(a) & Vec256b(b));
}
static inline Vec4q operator && (Vec4q const & a, Vec4q const & b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec4q & operator &= (Vec4q & a, Vec4q const & b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec4q operator | (Vec4q const & a, Vec4q const & b) {
    return Vec4q(Vec256b(a) | Vec256b(b));
}
static inline Vec4q operator || (Vec4q const & a, Vec4q const & b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec4q & operator |= (Vec4q & a, Vec4q const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec4q operator ^ (Vec4q const & a, Vec4q const & b) {
    return Vec4q(Vec256b(a) ^ Vec256b(b));
}
// vector operator ^= : bitwise xor
static inline Vec4q & operator ^= (Vec4q & a, Vec4q const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec4q operator ~ (Vec4q const & a) {
    return Vec4q( ~ Vec256b(a));
}

// vector operator ! : logical not, returns true for elements == 0
static inline Vec4qb operator ! (Vec4q const & a) {
    return a == Vec4q(_mm256_setzero_si256());
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 4; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or -1 (true). No other values are allowed.
// (s is signed)
static inline Vec4q select (Vec4qb const & s, Vec4q const & a, Vec4q const & b) {
    return selectb(s,a,b);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec4q if_add (Vec4qb const & f, Vec4q const & a, Vec4q const & b) {
    return a + (Vec4q(f) & b);
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
static inline int64_t horizontal_add (Vec4q const & a) {
    __m256i sum1  = _mm256_shuffle_epi32(a,0x0E);                     // high element
    __m256i sum2  = _mm256_add_epi64(a,sum1);                         // sum
#if defined (_MSC_VER) && _MSC_VER <= 1700 && ! defined(__INTEL_COMPILER)
    __m128i sum3  = _mm256_extractf128_si256(sum2, 1);                // bug in MS compiler VS 11
#else
    __m128i sum3  = _mm256_extracti128_si256(sum2, 1);                // get high part
#endif
    __m128i sum4  = _mm_add_epi64(_mm256_castsi256_si128(sum2),sum3); // add low and high parts
#if defined(__x86_64__)
    return          _mm_cvtsi128_si64(sum4);                          // 64 bit mode
#else
    union {
        __m128i x;  // silly definition of _mm256_storel_epi64 requires __m256i
        uint64_t i;
    } u;
    _mm_storel_epi64(&u.x,sum4);
    return u.i;
#endif
}

// function max: a > b ? a : b
static inline Vec4q max(Vec4q const & a, Vec4q const & b) {
    return select(a > b, a, b);
}

// function min: a < b ? a : b
static inline Vec4q min(Vec4q const & a, Vec4q const & b) {
    return select(a < b, a, b);
}

// function abs: a >= 0 ? a : -a
static inline Vec4q abs(Vec4q const & a) {
    __m256i sign  = _mm256_cmpgt_epi64(_mm256_setzero_si256(), a);// 0 > a
    __m256i inv   = _mm256_xor_si256(a, sign);                    // invert bits if negative
    return          _mm256_sub_epi64(inv, sign);                  // add 1
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec4q abs_saturated(Vec4q const & a) {
    __m256i absa   = abs(a);                                        // abs(a)
    __m256i overfl = _mm256_cmpgt_epi64(_mm256_setzero_si256(), absa); // 0 > a
    return           _mm256_add_epi64(absa, overfl);                // subtract 1 if 0x8000000000000000
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec4q rotate_left(Vec4q const & a, int b) {
#ifdef __XOP2__  // Possible future 256-bit XOP extension ?
    return _mm256_rot_epi64(a,Vec4q(b));
#else  // SSE2 instruction set
    __m256i left  = _mm256_sll_epi64(a,_mm_cvtsi32_si128(b & 0x3F));      // a << b 
    __m256i right = _mm256_srl_epi64(a,_mm_cvtsi32_si128((64-b) & 0x3F)); // a >> (64 - b)
    __m256i rot   = _mm256_or_si256(left, right);                         // or
    return  rot;
#endif
}


/*****************************************************************************
*
*          Vector of 4 64-bit unsigned integers
*
*****************************************************************************/

class Vec4uq : public Vec4q {
public:
    // Default constructor:
    Vec4uq() {
    };
    // Constructor to broadcast the same value into all elements:
    Vec4uq(uint64_t i) {
        ymm = Vec4q(i);
    };
    // Constructor to build from all elements:
    Vec4uq(uint64_t i0, uint64_t i1, uint64_t i2, uint64_t i3) {
        ymm = Vec4q(i0, i1, i2, i3);
    };
    // Constructor to build from two Vec2uq:
    Vec4uq(Vec2uq const & a0, Vec2uq const & a1) {
        ymm = set_m128ir(a0, a1);
    }
    // Constructor to convert from type __m256i used in intrinsics:
    Vec4uq(__m256i const & x) {
        ymm = x;
    };
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec4uq & operator = (__m256i const & x) {
        ymm = x;
        return *this;
    };
    // Member function to load from array (unaligned)
    Vec4uq & load(void const * p) {
        ymm = _mm256_loadu_si256((__m256i const*)p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec4uq & load_a(void const * p) {
        ymm = _mm256_load_si256((__m256i const*)p);
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec4uq const & insert(uint32_t index, uint64_t value) {
        Vec4q::insert(index, value);
        return *this;
    }
    // Member function extract a single element from vector
    uint64_t extract(uint32_t index) const {
        return Vec4q::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint64_t operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec2uq:
    Vec2uq get_low() const {
        return _mm256_castsi256_si128(ymm);
    }
    Vec2uq get_high() const {
        return _mm256_extractf128_si256(ymm,1);
    }
};

// Define operators for this class

// vector operator + : add
static inline Vec4uq operator + (Vec4uq const & a, Vec4uq const & b) {
    return Vec4uq (Vec4q(a) + Vec4q(b));
}

// vector operator - : subtract
static inline Vec4uq operator - (Vec4uq const & a, Vec4uq const & b) {
    return Vec4uq (Vec4q(a) - Vec4q(b));
}

// vector operator * : multiply element by element
static inline Vec4uq operator * (Vec4uq const & a, Vec4uq const & b) {
    return Vec4uq (Vec4q(a) * Vec4q(b));
}

// vector operator >> : shift right logical all elements
static inline Vec4uq operator >> (Vec4uq const & a, uint32_t b) {
    return _mm256_srl_epi64(a,_mm_cvtsi32_si128(b)); 
}

// vector operator >> : shift right logical all elements
static inline Vec4uq operator >> (Vec4uq const & a, int32_t b) {
    return a >> (uint32_t)b;
}

// vector operator >>= : shift right artihmetic
static inline Vec4uq & operator >>= (Vec4uq & a, uint32_t b) {
    a = a >> b;
    return a;
} 

// vector operator << : shift left all elements
static inline Vec4uq operator << (Vec4uq const & a, uint32_t b) {
    return Vec4uq ((Vec4q)a << (int32_t)b);
}

// vector operator << : shift left all elements
static inline Vec4uq operator << (Vec4uq const & a, int32_t b) {
    return Vec4uq ((Vec4q)a << b);
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec4qb operator > (Vec4uq const & a, Vec4uq const & b) {
#ifdef __XOP2__  // Possible future 256-bit XOP extension ?
    return _mm256_comgt_epu64(a,b);
#else  // SSE2 instruction set
    __m256i sign32  = _mm256_set1_epi32(0x80000000);          // sign bit of each dword
    __m256i aflip   = _mm256_xor_si256(a,sign32);             // a with sign bits flipped
    __m256i bflip   = _mm256_xor_si256(b,sign32);             // b with sign bits flipped
    __m256i equal   = _mm256_cmpeq_epi32(a,b);                // a == b, dwords
    __m256i bigger  = _mm256_cmpgt_epi32(aflip,bflip);        // a > b, dwords
    __m256i biggerl = _mm256_shuffle_epi32(bigger,0xA0);      // a > b, low dwords copied to high dwords
    __m256i eqbig   = _mm256_and_si256(equal,biggerl);        // high part equal and low part bigger
    __m256i hibig   = _mm256_or_si256(bigger,eqbig);          // high part bigger or high part equal and low part bigger
    __m256i big     = _mm256_shuffle_epi32(hibig,0xF5);       // result copied to low part
    return  big;
#endif
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec4qb operator < (Vec4uq const & a, Vec4uq const & b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec4qb operator >= (Vec4uq const & a, Vec4uq const & b) {
#ifdef __XOP2__  // Possible future 256-bit XOP extension ?
    return _mm256_comge_epu64(a,b);
#else  // SSE2 instruction set
    return  Vec4qb(Vec4q(~(b > a)));
#endif
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec4qb operator <= (Vec4uq const & a, Vec4uq const & b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec4uq operator & (Vec4uq const & a, Vec4uq const & b) {
    return Vec4uq(Vec256b(a) & Vec256b(b));
}
static inline Vec4uq operator && (Vec4uq const & a, Vec4uq const & b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec4uq operator | (Vec4uq const & a, Vec4uq const & b) {
    return Vec4uq(Vec256b(a) | Vec256b(b));
}
static inline Vec4uq operator || (Vec4uq const & a, Vec4uq const & b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec4uq operator ^ (Vec4uq const & a, Vec4uq const & b) {
    return Vec4uq(Vec256b(a) ^ Vec256b(b));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 4; i++) result[i] = s[i] ? a[i] : b[i];
// Each word in s must be either 0 (false) or -1 (true). No other values are allowed.
// (s is signed)
static inline Vec4uq select (Vec4qb const & s, Vec4uq const & a, Vec4uq const & b) {
    return selectb(s,a,b);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec4uq if_add (Vec4qb const & f, Vec4uq const & a, Vec4uq const & b) {
    return a + (Vec4uq(f) & b);
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
static inline uint64_t horizontal_add (Vec4uq const & a) {
    return horizontal_add((Vec4q)a);
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Elements are sing/zero extended before adding to avoid overflow
static inline int64_t horizontal_add_x (Vec8i const & a) {
    __m256i signs = _mm256_srai_epi32(a,31);                          // sign of all elements
    Vec4q   a01   = _mm256_unpacklo_epi32(a,signs);                   // sign-extended a0, a1, a4, a5
    Vec4q   a23   = _mm256_unpackhi_epi32(a,signs);                   // sign-extended a2, a3, a6, a7
    return  horizontal_add(a01 + a23);
}

static inline uint64_t horizontal_add_x (Vec8ui const & a) {
    __m256i zero  = _mm256_setzero_si256();                           // 0
    __m256i a01   = _mm256_unpacklo_epi32(a,zero);                    // zero-extended a0, a1
    __m256i a23   = _mm256_unpackhi_epi32(a,zero);                    // zero-extended a2, a3
    return horizontal_add(Vec4q(a01) + Vec4q(a23));
}

// function max: a > b ? a : b
static inline Vec4uq max(Vec4uq const & a, Vec4uq const & b) {
    return Vec4uq(select(a > b, a, b));
}

// function min: a < b ? a : b
static inline Vec4uq min(Vec4uq const & a, Vec4uq const & b) {
    return Vec4uq(select(a > b, b, a));
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
* Vec8i a(10,11,12,13,14,15,16,17);      // a is (10,11,12,13,14,15,16,17)
* Vec8i b;
* b = permute8i<0,2,7,7,-1,-1,1,1>(a);   // b is (10,12,17,17, 0, 0,11,11)
*
* A lot of the code here is metaprogramming aiming to find the instructions
* that best fit the template parameters and instruction set. The metacode
* will be reduced out to leave only a few vector instructions in release
* mode with optimization on.
*****************************************************************************/

// Permute vector of 4 64-bit integers.
// Index -1 gives 0, index -256 means don't care.
template <int i0, int i1, int i2, int i3 >
static inline Vec4q permute4q(Vec4q const & a) {

    // Combine indexes into a single bitfield, with 8 bits for each
    const int m1 = (i0 & 3) | (i1 & 3) << 8 | (i2 & 3) << 16 | (i3 & 3) << 24;

    // Mask to zero out negative indexes
    const int mz = (i0<0 ? 0 : 0xFF) | (i1<0 ? 0 : 0xFF) << 8 | (i2<0 ? 0 : 0xFF) << 16 | (i3<0 ? 0 : 0xFF) << 24;

    // zeroing needed
    const bool dozero = ((i0|i1|i2|i3) & 0x80) != 0;

    if (((m1 ^ 0x03020100) & mz) == 0) {
        // no shuffling
        if (dozero) {
            // zero some elements
            const __m256i maskz = constant8i <
                i0 < 0 ? 0 : -1, i0 < 0 ? 0 : -1, i1 < 0 ? 0 : -1, i1 < 0 ? 0 : -1, 
                i2 < 0 ? 0 : -1, i2 < 0 ? 0 : -1, i3 < 0 ? 0 : -1, i3 < 0 ? 0 : -1 > ();                    
            return _mm256_and_si256(a, maskz);
        }
        return a;                                 // do nothing
    }

    if (((m1 ^ 0x02020000) & 0x02020202 & mz) == 0) {
        // no exchange of data between low and high half

        if (((m1 ^ (m1 >> 16)) & 0x0101 & mz & (mz >> 16)) == 0 && !dozero) {
            // same pattern in low and high half. use VPSHUFD
            const int sd = (((i0>=0)?(i0&1):(i2&1)) * 10 + 4) | (((i1>=0)?(i1&1):(i3&1)) * 10 + 4) << 4;
            return _mm256_shuffle_epi32(a, sd);
        }

        // use VPSHUFB
        const __m256i mm = constant8i <
            i0 < 0 ? -1 : (i0 & 1) * 0x08080808 + 0x03020100,
            i0 < 0 ? -1 : (i0 & 1) * 0x08080808 + 0x07060504,
            i1 < 0 ? -1 : (i1 & 1) * 0x08080808 + 0x03020100,
            i1 < 0 ? -1 : (i1 & 1) * 0x08080808 + 0x07060504,
            i2 < 0 ? -1 : (i2 & 1) * 0x08080808 + 0x03020100,
            i2 < 0 ? -1 : (i2 & 1) * 0x08080808 + 0x07060504,
            i3 < 0 ? -1 : (i3 & 1) * 0x08080808 + 0x03020100,
            i3 < 0 ? -1 : (i3 & 1) * 0x08080808 + 0x07060504 > ();
        return _mm256_shuffle_epi8(a, mm);
    }

    // general case. Use VPERMQ
    const int ms = (i0 & 3) | (i1 & 3) << 2 | (i2 & 3) << 4 | (i3 & 3) << 6;        
    __m256i t1 = _mm256_permute4x64_epi64(a, ms);

    if (dozero) {
        // zero some elements
        const __m256i maskz = constant8i <
            i0 < 0 ? 0 : -1, i0 < 0 ? 0 : -1, i1 < 0 ? 0 : -1, i1 < 0 ? 0 : -1, 
            i2 < 0 ? 0 : -1, i2 < 0 ? 0 : -1, i3 < 0 ? 0 : -1, i3 < 0 ? 0 : -1 > ();                    
        return _mm256_and_si256(t1, maskz);
    }
    return t1;
}

template <int i0, int i1, int i2, int i3>
static inline Vec4uq permute4uq(Vec4uq const & a) {
    return Vec4uq (permute4q<i0,i1,i2,i3> (a));
}

// Permute vector of 8 32-bit integers.
// Index -1 gives 0, index -256 means don't care.
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7 >
static inline Vec8i permute8i(Vec8i const & a) {

    // Combine indexes into a single bitfield, with 4 bits for each
    const int m1 = (i0&7) | (i1&7)<<4 | (i2&7)<<8 | (i3&7)<<12
        | (i4&7)<<16 | (i5&7)<<20 | (i6&7)<<24 | (i7&7)<<28;

    // Mask to zero out negative indexes
    const int mz = (i0<0?0:0xF) | (i1<0?0:0xF)<<4 | (i2<0?0:0xF)<<8 | (i3<0?0:0xF)<<12
        | (i4<0?0:0xF)<<16 | (i5<0?0:0xF)<<20 | (i6<0?0:0xF)<<24 | (i7<0?0:0xF)<<28;

    // zeroing needed
    const bool dozero = ((i0|i1|i2|i3|i4|i5|i6|i7) & 0x80) != 0;

    __m256i t1, mask;

    if (((m1 ^ 0x76543210) & mz) == 0) {
        // no shuffling
        if (dozero) {
            // zero some elements
            mask = constant8i <
                i0 < 0 ? 0 : -1, i1 < 0 ? 0 : -1, i2 < 0 ? 0 : -1, i3 < 0 ? 0 : -1, 
                i4 < 0 ? 0 : -1, i5 < 0 ? 0 : -1, i6 < 0 ? 0 : -1, i7 < 0 ? 0 : -1 > ();                    
            return _mm256_and_si256(a, mask);
        }
        return a;                                 // do nothing
    }

    // Check if we can use 64-bit permute. Even numbered indexes must be even and odd numbered
    // indexes must be equal to the preceding index + 1, except for negative indexes.
    if (((m1 ^ 0x10101010) & 0x11111111 & mz) == 0 && ((m1 ^ m1 >> 4) & 0x0E0E0E0E & mz & mz >> 4) == 0) {

        const bool partialzero = int((i0^i1)|(i2^i3)|(i4^i5)|(i6^i7)) < 0; // part of a 64-bit block is zeroed
        const int blank1 = partialzero ? -0x100 : -1;  // ignore or zero
        const int n0 = i0 > 0 ? i0 /2 : i1 > 0 ? i1 /2 : blank1;  // indexes for 64 bit blend
        const int n1 = i2 > 0 ? i2 /2 : i3 > 0 ? i3 /2 : blank1;
        const int n2 = i4 > 0 ? i4 /2 : i5 > 0 ? i5 /2 : blank1;
        const int n3 = i6 > 0 ? i6 /2 : i7 > 0 ? i7 /2 : blank1;
        // do 64-bit permute
        t1 = permute4q<n0,n1,n2,n3> (Vec4q(a));
        if (blank1 == -1 || !dozero) {    
            return  t1;
        }
        // need more zeroing
        mask = constant8i <
            i0 < 0 ? 0 : -1, i1 < 0 ? 0 : -1, i2 < 0 ? 0 : -1, i3 < 0 ? 0 : -1, 
            i4 < 0 ? 0 : -1, i5 < 0 ? 0 : -1, i6 < 0 ? 0 : -1, i7 < 0 ? 0 : -1 > ();                    
        return _mm256_and_si256(t1, mask);
    }

    if (((m1 ^ 0x44440000) & 0x44444444 & mz) == 0) {
        // no exchange of data between low and high half

        if (((m1 ^ (m1 >> 16)) & 0x3333 & mz & (mz >> 16)) == 0 && !dozero) {
            // same pattern in low and high half. use VPSHUFD
            const int sd = ((i0>=0)?(i0&3):(i4&3)) | ((i1>=0)?(i1&3):(i5&3)) << 2 |
                ((i2>=0)?(i2&3):(i6&3)) << 4 | ((i3>=0)?(i3&3):(i7&3)) << 6;
            return _mm256_shuffle_epi32(a, sd);
        }

        // use VPSHUFB
        mask = constant8i <
            i0 < 0 ? -1 : (i0 & 3) * 0x04040404 + 0x03020100,
            i1 < 0 ? -1 : (i1 & 3) * 0x04040404 + 0x03020100,
            i2 < 0 ? -1 : (i2 & 3) * 0x04040404 + 0x03020100,
            i3 < 0 ? -1 : (i3 & 3) * 0x04040404 + 0x03020100,
            i4 < 0 ? -1 : (i4 & 3) * 0x04040404 + 0x03020100,
            i5 < 0 ? -1 : (i5 & 3) * 0x04040404 + 0x03020100,
            i6 < 0 ? -1 : (i6 & 3) * 0x04040404 + 0x03020100,
            i7 < 0 ? -1 : (i7 & 3) * 0x04040404 + 0x03020100 > ();
        return _mm256_shuffle_epi8(a, mask);
    }

    // general case. Use VPERMD
    mask = constant8i <
        i0 < 0 ? -1 : (i0 & 7), i1 < 0 ? -1 : (i1 & 7),
        i2 < 0 ? -1 : (i2 & 7), i3 < 0 ? -1 : (i3 & 7),
        i4 < 0 ? -1 : (i4 & 7), i5 < 0 ? -1 : (i5 & 7),
        i6 < 0 ? -1 : (i6 & 7), i7 < 0 ? -1 : (i7 & 7) > ();
#if defined (_MSC_VER) && _MSC_VER < 1700 && ! defined(__INTEL_COMPILER)
    // bug in MS VS 11 beta: operands in wrong order. fixed in v. 11.0
    t1 = _mm256_permutevar8x32_epi32(mask, a);   // ms
#elif defined (GCC_VERSION) && GCC_VERSION <= 40700 && !defined(__INTEL_COMPILER) && !defined(__clang__)
    // Gcc 4.7.0 also has operands in wrong order. fixed in version 4.7.1
    t1 = _mm256_permutevar8x32_epi32(mask, a);   // GCC
#else
    t1 = _mm256_permutevar8x32_epi32(a, mask);   // no-bug version
#endif

    if (dozero) {
        // zero some elements
        mask = constant8i <
            i0 < 0 ? 0 : -1, i1 < 0 ? 0 : -1, i2 < 0 ? 0 : -1, i3 < 0 ? 0 : -1, 
            i4 < 0 ? 0 : -1, i5 < 0 ? 0 : -1, i6 < 0 ? 0 : -1, i7 < 0 ? 0 : -1 > ();                    
        return _mm256_and_si256(t1, mask);
    }
    return t1;
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7 >
static inline Vec8ui permute8ui(Vec8ui const & a) {
    return Vec8ui (permute8i<i0,i1,i2,i3,i4,i5,i6,i7> (a));
}

// Permute vector of 16 16-bit integers.
// Index -1 gives 0, index -256 means don't care.
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7,
    int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15 >
static inline Vec16s permute16s(Vec16s const & a) {

    // Combine indexes 0 - 7 into a single bitfield, with 4 bits for each
    const int mlo = (i0&0xF) | (i1&0xF)<<4 | (i2&0xF)<<8 | (i3&0xF)<<12 
        | (i4&0xF)<<16 | (i5&0xF)<<20 | (i6&0xF)<<24 | (i7&0xF)<<28; 

    // Combine indexes 8 - 15 into a single bitfield, with 4 bits for each
    const int mhi = (i8&0xF) | (i9&0xF)<<4 | (i10&0xF)<<8 | (i11&0xF)<<12 
        | (i12&0xF)<<16 | (i13&0xF)<<20 | (i14&0xF)<<24 | (i15&0xF)<<28;

    // Mask to zero out negative indexes 0 - 7
    const int zlo = (i0<0?0:0xF) | (i1<0?0:0xF)<<4 | (i2<0?0:0xF)<<8 | (i3<0?0:0xF)<<12
        | (i4<0?0:0xF)<<16 | (i5<0?0:0xF)<<20 | (i6<0?0:0xF)<<24 | (i7<0?0:0xF)<<28;

    // Mask to zero out negative indexes 8 - 15
    const int zhi = (i8<0?0:0xF) | (i9<0?0:0xF)<<4 | (i10<0?0:0xF)<<8 | (i11<0?0:0xF)<<12
        | (i12<0?0:0xF)<<16 | (i13<0?0:0xF)<<20 | (i14<0?0:0xF)<<24 | (i15<0?0:0xF)<<28;

    // zeroing needed
    const bool dozero = ((i0|i1|i2|i3|i4|i5|i6|i7|i8|i9|i10|i11|i12|i13|i14|i15) & 0x80) != 0;

    __m256i t1, mask;

    // special case: all zero
    if (zlo == 0 && zhi == 0) {
        return _mm256_setzero_si256();
    }

    // special case: rotate 128 bits
    if (i0>=0 && i0 < 16 && i1 ==((i0+1)&7) && i2 ==((i0+2)&7) && i3 ==((i0+3)&7) && i4 ==((i0+4)&7) && i5 ==((i0+5)&7) && i6 ==((i0+6)&7) && i7 ==((i0+7)&7) 
        && i8 ==i0 +8 && i9 ==i1 +8 && i10==i2 +8 && i11==i3 +8 && i12==i4 +8 && i13==i5 +8 && i14==i6 +8 && i15==i7 +8 ) {
        return _mm256_alignr_epi8(a, a, (i0 & 7) * 2);
    }

    // special case: rotate 256 bits
    if (i0>=0 && i0 < 16     && i1 ==((i0+1 )&15) && i2 ==((i0+2 )&15) && i3 ==((i0+3 )&15) && i4 ==((i0+4 )&15) && i5 ==((i0+5 )&15) && i6 ==((i0+6 )&15) && i7 ==((i0+7 )&15) 
        && i8 ==((i0+8 )&15) && i9 ==((i0+9 )&15) && i10==((i0+10)&15) && i11==((i0+11)&15) && i12==((i0+12)&15) && i13==((i0+13)&15) && i14==((i0+14)&15) && i15==((i0+15)&15)) {
        t1 = _mm256_permute4x64_epi64(a, 0x4E);
        return _mm256_alignr_epi8(a, t1, (i0 & 7) * 2);
    }

    // special case: no exchange of data between 64-bit sections, and same pattern in low and high 128 bits:
    // can use VPSHUFLW or VPSHUFHW
    if (((mlo ^ 0x44440000) & 0xCCCCCCCC & zlo) == 0 && ((mhi ^ 0xCCCC8888) & 0xCCCCCCCC & zhi) == 0
        && ((mlo ^ mhi) & 0x33333333 & zlo & zhi) == 0) {

        const int slo = (i0 >= 0 ? (i0&3) : i8 >= 0 ? (i8&3) : 0) | (i1 >= 0 ? (i1&3) : i9 >= 0 ? (i9&3) : 1) << 2 
            | (i2 >= 0 ? (i2&3) : i10 >= 0 ? (i10&3) : 2) << 4 | (i3 >= 0 ? (i3&3) : i11 >= 0 ? (i11&3) : 3) << 6;

        const int shi = (i4 >= 0 ? (i4&3) : i12 >= 0 ? (i12&3) : 0) | (i5 >= 0 ? (i5&3) : i13 >= 0 ? (i13&3) : 1) << 2 
            | (i6 >= 0 ? (i6&3) : i14 >= 0 ? (i14&3) : 2) << 4 | (i7 >= 0 ? (i7&3) : i15 >= 0 ? (i15&3) : 3) << 6;

        if (shi == 0xE4 && slo == 0xE4) {             // no permute
            if (dozero) {
                // zero some elements
                const __m256i maskz = constant8i<
                    (i0 <0?0:0xFFFF) | (i1 <0?0:0xFFFF0000),
                    (i2 <0?0:0xFFFF) | (i3 <0?0:0xFFFF0000),
                    (i4 <0?0:0xFFFF) | (i5 <0?0:0xFFFF0000),
                    (i6 <0?0:0xFFFF) | (i7 <0?0:0xFFFF0000),
                    (i8 <0?0:0xFFFF) | (i9 <0?0:0xFFFF0000),
                    (i10<0?0:0xFFFF) | (i11<0?0:0xFFFF0000),
                    (i12<0?0:0xFFFF) | (i13<0?0:0xFFFF0000),
                    (i14<0?0:0xFFFF) | (i15<0?0:0xFFFF0000) > ();                    
                return _mm256_and_si256(a, maskz);
            }
            return a;                                 // do nothing
        }
        if (shi == 0xE4 && !dozero) {
            return _mm256_shufflelo_epi16(a, slo);    // low permute only
        }
        if (slo == 0xE4 && !dozero) {
            return _mm256_shufflehi_epi16(a, shi);    // high permute only
        }
    }
    
    // Check if we can use 32-bit permute. Even numbered indexes must be even and odd numbered
    // indexes must be equal to the preceding index + 1, except for negative indexes.
    if (((mlo ^ 0x10101010) & 0x11111111 & zlo) == 0 && ((mlo ^ mlo >> 4) & 0x0E0E0E0E & zlo & zlo >> 4) == 0 &&
        ((mhi ^ 0x10101010) & 0x11111111 & zhi) == 0 && ((mhi ^ mhi >> 4) & 0x0E0E0E0E & zhi & zhi >> 4) == 0 ) {

        const bool partialzero = int((i0^i1)|(i2^i3)|(i4^i5)|(i6^i7)|(i8^i9)|(i10^i11)|(i12^i13)|(i14^i15)) < 0; // part of a 32-bit block is zeroed
        const int blank1 = partialzero ? -0x100 : -1;  // ignore or zero
        const int n0 = i0 > 0 ? i0 /2 : i1 > 0 ? i1 /2 : blank1;  // indexes for 64 bit blend
        const int n1 = i2 > 0 ? i2 /2 : i3 > 0 ? i3 /2 : blank1;
        const int n2 = i4 > 0 ? i4 /2 : i5 > 0 ? i5 /2 : blank1;
        const int n3 = i6 > 0 ? i6 /2 : i7 > 0 ? i7 /2 : blank1;
        const int n4 = i8 > 0 ? i8 /2 : i9 > 0 ? i9 /2 : blank1;
        const int n5 = i10> 0 ? i10/2 : i11> 0 ? i11/2 : blank1;
        const int n6 = i12> 0 ? i12/2 : i13> 0 ? i13/2 : blank1;
        const int n7 = i14> 0 ? i14/2 : i15> 0 ? i15/2 : blank1;
        // do 32-bit permute
        t1 = permute8i<n0,n1,n2,n3,n4,n5,n6,n7> (Vec8i(a));
        if (blank1 == -1 || !dozero) {    
            return  t1;
        }
        // need more zeroing
        mask = constant8i<
            (i0 <0?0:0xFFFF) | (i1 <0?0:0xFFFF0000),
            (i2 <0?0:0xFFFF) | (i3 <0?0:0xFFFF0000),
            (i4 <0?0:0xFFFF) | (i5 <0?0:0xFFFF0000),
            (i6 <0?0:0xFFFF) | (i7 <0?0:0xFFFF0000),
            (i8 <0?0:0xFFFF) | (i9 <0?0:0xFFFF0000),
            (i10<0?0:0xFFFF) | (i11<0?0:0xFFFF0000),
            (i12<0?0:0xFFFF) | (i13<0?0:0xFFFF0000),
            (i14<0?0:0xFFFF) | (i15<0?0:0xFFFF0000) > ();                    
        return _mm256_and_si256(t1, mask);
    }

    // special case: all elements from same half
    if ((mlo & 0x88888888 & zlo) == 0 && ((mhi ^ 0x88888888) & 0x88888888 & zhi) == 0) {
        mask = constant8i<
            (i0  < 0 ? 0xFFFF : (i0  & 7) * 0x202 + 0x100) | (i1  < 0 ? 0xFFFF : (i1  & 7) * 0x202 + 0x100) << 16,
            (i2  < 0 ? 0xFFFF : (i2  & 7) * 0x202 + 0x100) | (i3  < 0 ? 0xFFFF : (i3  & 7) * 0x202 + 0x100) << 16,
            (i4  < 0 ? 0xFFFF : (i4  & 7) * 0x202 + 0x100) | (i5  < 0 ? 0xFFFF : (i5  & 7) * 0x202 + 0x100) << 16,
            (i6  < 0 ? 0xFFFF : (i6  & 7) * 0x202 + 0x100) | (i7  < 0 ? 0xFFFF : (i7  & 7) * 0x202 + 0x100) << 16,
            (i8  < 0 ? 0xFFFF : (i8  & 7) * 0x202 + 0x100) | (i9  < 0 ? 0xFFFF : (i9  & 7) * 0x202 + 0x100) << 16,
            (i10 < 0 ? 0xFFFF : (i10 & 7) * 0x202 + 0x100) | (i11 < 0 ? 0xFFFF : (i11 & 7) * 0x202 + 0x100) << 16,
            (i12 < 0 ? 0xFFFF : (i12 & 7) * 0x202 + 0x100) | (i13 < 0 ? 0xFFFF : (i13 & 7) * 0x202 + 0x100) << 16,
            (i14 < 0 ? 0xFFFF : (i14 & 7) * 0x202 + 0x100) | (i15 < 0 ? 0xFFFF : (i15 & 7) * 0x202 + 0x100) << 16 > ();
        return _mm256_shuffle_epi8(a, mask);
    }

    // special case: all elements from low half
    if ((mlo & 0x88888888 & zlo) == 0 && (mhi & 0x88888888 & zhi) == 0) {
        mask = constant8i<
            (i0  < 0 ? 0xFFFF : (i0  & 7) * 0x202 + 0x100) | (i1  < 0 ? 0xFFFF : (i1  & 7) * 0x202 + 0x100) << 16,
            (i2  < 0 ? 0xFFFF : (i2  & 7) * 0x202 + 0x100) | (i3  < 0 ? 0xFFFF : (i3  & 7) * 0x202 + 0x100) << 16,
            (i4  < 0 ? 0xFFFF : (i4  & 7) * 0x202 + 0x100) | (i5  < 0 ? 0xFFFF : (i5  & 7) * 0x202 + 0x100) << 16,
            (i6  < 0 ? 0xFFFF : (i6  & 7) * 0x202 + 0x100) | (i7  < 0 ? 0xFFFF : (i7  & 7) * 0x202 + 0x100) << 16,
            (i8  < 0 ? 0xFFFF : (i8  & 7) * 0x202 + 0x100) | (i9  < 0 ? 0xFFFF : (i9  & 7) * 0x202 + 0x100) << 16,
            (i10 < 0 ? 0xFFFF : (i10 & 7) * 0x202 + 0x100) | (i11 < 0 ? 0xFFFF : (i11 & 7) * 0x202 + 0x100) << 16,
            (i12 < 0 ? 0xFFFF : (i12 & 7) * 0x202 + 0x100) | (i13 < 0 ? 0xFFFF : (i13 & 7) * 0x202 + 0x100) << 16,
            (i14 < 0 ? 0xFFFF : (i14 & 7) * 0x202 + 0x100) | (i15 < 0 ? 0xFFFF : (i15 & 7) * 0x202 + 0x100) << 16 > ();
        t1 = _mm256_inserti128_si256(a, _mm256_castsi256_si128(a), 1);  // low, low
        return _mm256_shuffle_epi8(t1, mask);
    }

    // special case: all elements from high half
    if (((mlo ^ 0x88888888) & 0x88888888 & zlo) == 0 && ((mhi ^ 0x88888888) & 0x88888888 & zhi) == 0) {
        mask = constant8i<
            (i0  < 0 ? 0xFFFF : (i0  & 7) * 0x202 + 0x100) | (i1  < 0 ? 0xFFFF : (i1  & 7) * 0x202 + 0x100) << 16,
            (i2  < 0 ? 0xFFFF : (i2  & 7) * 0x202 + 0x100) | (i3  < 0 ? 0xFFFF : (i3  & 7) * 0x202 + 0x100) << 16,
            (i4  < 0 ? 0xFFFF : (i4  & 7) * 0x202 + 0x100) | (i5  < 0 ? 0xFFFF : (i5  & 7) * 0x202 + 0x100) << 16,
            (i6  < 0 ? 0xFFFF : (i6  & 7) * 0x202 + 0x100) | (i7  < 0 ? 0xFFFF : (i7  & 7) * 0x202 + 0x100) << 16,
            (i8  < 0 ? 0xFFFF : (i8  & 7) * 0x202 + 0x100) | (i9  < 0 ? 0xFFFF : (i9  & 7) * 0x202 + 0x100) << 16,
            (i10 < 0 ? 0xFFFF : (i10 & 7) * 0x202 + 0x100) | (i11 < 0 ? 0xFFFF : (i11 & 7) * 0x202 + 0x100) << 16,
            (i12 < 0 ? 0xFFFF : (i12 & 7) * 0x202 + 0x100) | (i13 < 0 ? 0xFFFF : (i13 & 7) * 0x202 + 0x100) << 16,
            (i14 < 0 ? 0xFFFF : (i14 & 7) * 0x202 + 0x100) | (i15 < 0 ? 0xFFFF : (i15 & 7) * 0x202 + 0x100) << 16 > ();
        t1 = _mm256_permute4x64_epi64(a, 0xEE);  // high, high
        return _mm256_shuffle_epi8(t1, mask);
    }

    // special case: all elements from opposite half
    if (((mlo ^ 0x88888888) & 0x88888888 & zlo) == 0 && (mhi & 0x88888888 & zhi) == 0) {
        mask = constant8i<
            (i0  < 0 ? 0xFFFF : (i0  & 7) * 0x202 + 0x100) | (i1  < 0 ? 0xFFFF : (i1  & 7) * 0x202 + 0x100) << 16,
            (i2  < 0 ? 0xFFFF : (i2  & 7) * 0x202 + 0x100) | (i3  < 0 ? 0xFFFF : (i3  & 7) * 0x202 + 0x100) << 16,
            (i4  < 0 ? 0xFFFF : (i4  & 7) * 0x202 + 0x100) | (i5  < 0 ? 0xFFFF : (i5  & 7) * 0x202 + 0x100) << 16,
            (i6  < 0 ? 0xFFFF : (i6  & 7) * 0x202 + 0x100) | (i7  < 0 ? 0xFFFF : (i7  & 7) * 0x202 + 0x100) << 16,
            (i8  < 0 ? 0xFFFF : (i8  & 7) * 0x202 + 0x100) | (i9  < 0 ? 0xFFFF : (i9  & 7) * 0x202 + 0x100) << 16,
            (i10 < 0 ? 0xFFFF : (i10 & 7) * 0x202 + 0x100) | (i11 < 0 ? 0xFFFF : (i11 & 7) * 0x202 + 0x100) << 16,
            (i12 < 0 ? 0xFFFF : (i12 & 7) * 0x202 + 0x100) | (i13 < 0 ? 0xFFFF : (i13 & 7) * 0x202 + 0x100) << 16,
            (i14 < 0 ? 0xFFFF : (i14 & 7) * 0x202 + 0x100) | (i15 < 0 ? 0xFFFF : (i15 & 7) * 0x202 + 0x100) << 16 > ();
        t1 = _mm256_permute4x64_epi64(a, 0x4E);  // high, low
        return _mm256_shuffle_epi8(t1, mask);
    }

    // general case: elements from both halves
    const __m256i mmsame = constant8i<
            ((i0 ^8) < 8 ? 0xFFFF : (i0  & 7) * 0x202 + 0x100) | ((i1 ^8) < 8 ? 0xFFFF : (i1  & 7) * 0x202 + 0x100) << 16,
            ((i2 ^8) < 8 ? 0xFFFF : (i2  & 7) * 0x202 + 0x100) | ((i3 ^8) < 8 ? 0xFFFF : (i3  & 7) * 0x202 + 0x100) << 16,
            ((i4 ^8) < 8 ? 0xFFFF : (i4  & 7) * 0x202 + 0x100) | ((i5 ^8) < 8 ? 0xFFFF : (i5  & 7) * 0x202 + 0x100) << 16,
            ((i6 ^8) < 8 ? 0xFFFF : (i6  & 7) * 0x202 + 0x100) | ((i7 ^8) < 8 ? 0xFFFF : (i7  & 7) * 0x202 + 0x100) << 16,
            (i8  < 8 ? 0xFFFF : (i8  & 7) * 0x202 + 0x100) | (i9  < 8 ? 0xFFFF : (i9  & 7) * 0x202 + 0x100) << 16,
            (i10 < 8 ? 0xFFFF : (i10 & 7) * 0x202 + 0x100) | (i11 < 8 ? 0xFFFF : (i11 & 7) * 0x202 + 0x100) << 16,
            (i12 < 8 ? 0xFFFF : (i12 & 7) * 0x202 + 0x100) | (i13 < 8 ? 0xFFFF : (i13 & 7) * 0x202 + 0x100) << 16,
            (i14 < 8 ? 0xFFFF : (i14 & 7) * 0x202 + 0x100) | (i15 < 8 ? 0xFFFF : (i15 & 7) * 0x202 + 0x100) << 16 > ();

    const __m256i mmopposite = constant8i<
            (i0  < 8 ? 0xFFFF : (i0  & 7) * 0x202 + 0x100) | (i1  < 8 ? 0xFFFF : (i1  & 7) * 0x202 + 0x100) << 16,
            (i2  < 8 ? 0xFFFF : (i2  & 7) * 0x202 + 0x100) | (i3  < 8 ? 0xFFFF : (i3  & 7) * 0x202 + 0x100) << 16,
            (i4  < 8 ? 0xFFFF : (i4  & 7) * 0x202 + 0x100) | (i5  < 8 ? 0xFFFF : (i5  & 7) * 0x202 + 0x100) << 16,
            (i6  < 8 ? 0xFFFF : (i6  & 7) * 0x202 + 0x100) | (i7  < 8 ? 0xFFFF : (i7  & 7) * 0x202 + 0x100) << 16,
            ((i8 ^8) < 8 ? 0xFFFF : (i8  & 7) * 0x202 + 0x100) | ((i9 ^8) < 8 ? 0xFFFF : (i9  & 7) * 0x202 + 0x100) << 16,
            ((i10^8) < 8 ? 0xFFFF : (i10 & 7) * 0x202 + 0x100) | ((i11^8) < 8 ? 0xFFFF : (i11 & 7) * 0x202 + 0x100) << 16,
            ((i12^8) < 8 ? 0xFFFF : (i12 & 7) * 0x202 + 0x100) | ((i13^8) < 8 ? 0xFFFF : (i13 & 7) * 0x202 + 0x100) << 16,
            ((i14^8) < 8 ? 0xFFFF : (i14 & 7) * 0x202 + 0x100) | ((i15^8) < 8 ? 0xFFFF : (i15 & 7) * 0x202 + 0x100) << 16 > ();

    __m256i topp = _mm256_permute4x64_epi64(a, 0x4E);  // high, low
    __m256i r1   = _mm256_shuffle_epi8(topp, mmopposite);
    __m256i r2   = _mm256_shuffle_epi8(a, mmsame);
    return         _mm256_or_si256(r1, r2);
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7,
    int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15 >
static inline Vec16us permute16us(Vec16us const & a) {
    return Vec16us (permute16s<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15> (a));
}

template <int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15,
          int i16, int i17, int i18, int i19, int i20, int i21, int i22, int i23,
          int i24, int i25, int i26, int i27, int i28, int i29, int i30, int i31 >
static inline Vec32c permute32c(Vec32c const & a) {

    // collect bit 4 of each index
    const int m1 = 
        (i0 &16)>>4  | (i1 &16)>>3  | (i2 &16)>>2  | (i3 &16)>>1  | (i4 &16)     | (i5 &16)<<1  | (i6 &16)<<2  | (i7 &16)<<3  | 
        (i8 &16)<<4  | (i9 &16)<<5  | (i10&16)<<6  | (i11&16)<<7  | (i12&16)<<8  | (i13&16)<<9  | (i14&16)<<10 | (i15&16)<<11 | 
        (i16&16)<<12 | (i17&16)<<13 | (i18&16)<<14 | (i19&16)<<15 | (i20&16)<<16 | (i21&16)<<17 | (i22&16)<<18 | (i23&16)<<19 | 
        (i24&16)<<20 | (i25&16)<<21 | (i26&16)<<22 | (i27&16)<<23 | (i28&16)<<24 | (i29&16)<<25 | (i30&16)<<26 | (i31&16)<<27 ;

    // check which elements to set to zero
    const int mz = ~ (
        (i0 <0)     | (i1 <0)<<1  | (i2 <0)<<2  | (i3 <0)<<3  | (i4 <0)<<4  | (i5 <0)<<5  | (i6 <0)<<6  | (i7 <0)<<7  | 
        (i8 <0)<<8  | (i9 <0)<<9  | (i10<0)<<10 | (i11<0)<<11 | (i12<0)<<12 | (i13<0)<<13 | (i14<0)<<14 | (i15<0)<<15 | 
        (i16<0)<<16 | (i17<0)<<17 | (i18<0)<<18 | (i19<0)<<19 | (i20<0)<<20 | (i21<0)<<21 | (i22<0)<<22 | (i23<0)<<23 | 
        (i24<0)<<24 | (i25<0)<<25 | (i26<0)<<26 | (i27<0)<<27 | (i28<0)<<28 | (i29<0)<<29 | (i30<0)<<30 | (i31<0)<<31 );

    // Combine indexes 0-7, 8-15, 16-23, 24-31 into a bitfields, with 8 bits for each
    const uint64_t g0 = (i0 &0x1F)|(i1 &0x1F)<<8|(i2 &0x1F)<<16|(i3 &0x1F)<<24|(i4 &0x1FLL)<<32|(i5 &0x1FLL)<<40|(i6 &0x1FLL)<<48|(i7 &0x1FLL)<<56;
    const uint64_t g1 = (i8 &0x1F)|(i9 &0x1F)<<8|(i10&0x1F)<<16|(i11&0x1F)<<24|(i12&0x1FLL)<<32|(i13&0x1FLL)<<40|(i14&0x1FLL)<<48|(i15&0x1FLL)<<56; 
    const uint64_t g2 = (i16&0x1F)|(i17&0x1F)<<8|(i18&0x1F)<<16|(i19&0x1F)<<24|(i20&0x1FLL)<<32|(i21&0x1FLL)<<40|(i22&0x1FLL)<<48|(i23&0x1FLL)<<56; 
    const uint64_t g3 = (i24&0x1F)|(i25&0x1F)<<8|(i26&0x1F)<<16|(i27&0x1F)<<24|(i28&0x1FLL)<<32|(i29&0x1FLL)<<40|(i30&0x1FLL)<<48|(i31&0x1FLL)<<56; 
    
    // Masks to zero out negative indexes
    const uint64_t z0 = (i0 <0?0:0xFF)|(i1 <0?0:0xFF)<<8|(i2 <0?0:0xFF)<<16|(i3 <0?0:0xFF)<<24|(i4 <0?0:0xFFLL)<<32|(i5 <0?0:0xFFLL)<<40|(i6 <0?0:0xFFLL)<<48|(i7 <0?0:0xFFLL)<<56;
    const uint64_t z1 = (i8 <0?0:0xFF)|(i9 <0?0:0xFF)<<8|(i10<0?0:0xFF)<<16|(i11<0?0:0xFF)<<24|(i12<0?0:0xFFLL)<<32|(i13<0?0:0xFFLL)<<40|(i14<0?0:0xFFLL)<<48|(i15<0?0:0xFFLL)<<56;
    const uint64_t z2 = (i16<0?0:0xFF)|(i17<0?0:0xFF)<<8|(i18<0?0:0xFF)<<16|(i19<0?0:0xFF)<<24|(i20<0?0:0xFFLL)<<32|(i21<0?0:0xFFLL)<<40|(i22<0?0:0xFFLL)<<48|(i23<0?0:0xFFLL)<<56;
    const uint64_t z3 = (i24<0?0:0xFF)|(i25<0?0:0xFF)<<8|(i26<0?0:0xFF)<<16|(i27<0?0:0xFF)<<24|(i28<0?0:0xFFLL)<<32|(i29<0?0:0xFFLL)<<40|(i30<0?0:0xFFLL)<<48|(i31<0?0:0xFFLL)<<56;

    // zeroing needed
    const bool dozero = ((i0|i1|i2|i3|i4|i5|i6|i7|i8|i9|i10|i11|i12|i13|i14|i15|i16|i17|i18|i19|i20|i21|i22|i23|i24|i25|i26|i27|i28|i29|i30|i31) & 0x80) != 0;

    __m256i t1, mask;

    // special case: all zero
    if (mz == 0) return  _mm256_setzero_si256();

    // special case: no permute
    if ((i0 <0||i0 == 0) && (i1 <0||i1 == 1) && (i2 <0||i2 == 2) && (i3 <0||i3 == 3) && (i4 <0||i4 == 4) && (i5 <0||i5 == 5) && (i6 <0||i6 == 6) && (i7 <0||i7 == 7) &&
        (i8 <0||i8 == 8) && (i9 <0||i9 == 9) && (i10<0||i10==10) && (i11<0||i11==11) && (i12<0||i12==12) && (i13<0||i13==13) && (i14<0||i14==14) && (i15<0||i15==15) &&
        (i16<0||i16==16) && (i17<0||i17==17) && (i18<0||i18==18) && (i19<0||i19==19) && (i20<0||i20==20) && (i21<0||i21==21) && (i22<0||i22==22) && (i23<0||i23==23) &&
        (i24<0||i24==24) && (i25<0||i25==25) && (i26<0||i26==26) && (i27<0||i27==27) && (i28<0||i28==28) && (i29<0||i29==29) && (i30<0||i30==30) && (i31<0||i31==31)) {
        if (dozero) {
            // zero some elements
            mask = constant8i <
                (i0 <0?0:0xFF) | (i1 <0?0:0xFF00) | (i2 <0?0:0xFF0000) | (i3 <0?0:0xFF000000),
                (i4 <0?0:0xFF) | (i5 <0?0:0xFF00) | (i6 <0?0:0xFF0000) | (i7 <0?0:0xFF000000),
                (i8 <0?0:0xFF) | (i9 <0?0:0xFF00) | (i10<0?0:0xFF0000) | (i11<0?0:0xFF000000),
                (i12<0?0:0xFF) | (i13<0?0:0xFF00) | (i14<0?0:0xFF0000) | (i15<0?0:0xFF000000),
                (i16<0?0:0xFF) | (i17<0?0:0xFF00) | (i18<0?0:0xFF0000) | (i19<0?0:0xFF000000),
                (i20<0?0:0xFF) | (i21<0?0:0xFF00) | (i22<0?0:0xFF0000) | (i23<0?0:0xFF000000),
                (i24<0?0:0xFF) | (i25<0?0:0xFF00) | (i26<0?0:0xFF0000) | (i27<0?0:0xFF000000),
                (i28<0?0:0xFF) | (i29<0?0:0xFF00) | (i30<0?0:0xFF0000) | (i31<0?0:0xFF000000) > ();
            return _mm256_and_si256(a, mask);
        }
        return a; // do nothing
    }

    // special case: rotate 128 bits
    if (i0>=0 && i0 < 32     && i1 ==((i0+1 )&15) && i2 ==((i0+2 )&15) && i3 ==((i0+3 )&15) && i4 ==((i0+4 )&15) && i5 ==((i0+5 )&15) && i6 ==((i0+6 )&15) && i7 ==((i0+7 )&15) 
        && i8 ==((i0+8 )&15) && i9 ==((i0+9 )&15) && i10==((i0+10)&15) && i11==((i0+11)&15) && i12==((i0+12)&15) && i13==((i0+13)&15) && i14==((i0+14)&15) && i15==((i0+15)&15)
        && i16==i0 +16 && i17==i1 +16 && i18==i2 +16 && i19==i3 +16 && i20==i4 +16 && i21==i5 +16 && i22==i6 +16 && i23==i7 +16 
        && i24==i8 +16 && i25==i9 +16 && i26==i10+16 && i27==i11+16 && i28==i12+16 && i29==i13+16 && i30==i14+16 && i31==i15+16 ) {
        return _mm256_alignr_epi8(a, a, i0 & 15);
    }

    // special case: rotate 256 bits
    if (i0>=0 && i0 < 32     && i1 ==((i0+1 )&31) && i2 ==((i0+2 )&31) && i3 ==((i0+3 )&31) && i4 ==((i0+4 )&31) && i5 ==((i0+5 )&31) && i6 ==((i0+6 )&31) && i7 ==((i0+7 )&31) 
        && i8 ==((i0+8 )&31) && i9 ==((i0+9 )&31) && i10==((i0+10)&31) && i11==((i0+11)&31) && i12==((i0+12)&31) && i13==((i0+13)&31) && i14==((i0+14)&31) && i15==((i0+15)&31)
        && i16==((i0+16)&31) && i17==((i0+17)&31) && i18==((i0+18)&31) && i19==((i0+19)&31) && i20==((i0+20)&31) && i21==((i0+21)&31) && i22==((i0+22)&31) && i23==((i0+23)&31)
        && i24==((i0+24)&31) && i25==((i0+25)&31) && i26==((i0+26)&31) && i27==((i0+27)&31) && i28==((i0+28)&31) && i29==((i0+29)&31) && i30==((i0+30)&31) && i31==((i0+31)&31)) {
        __m256i t1 = _mm256_permute4x64_epi64(a, 0x4E);
        return _mm256_alignr_epi8(a, t1, i0 & 15);
    }

    // Check if we can use 16-bit permute. Even numbered indexes must be even and odd numbered
    // indexes must be equal to the preceding index + 1, except for negative indexes.
    if (((g0 ^ 0x0100010001000100) & 0x0101010101010101 & z0) == 0 && ((g0 ^ g0 >> 8) & 0x00FE00FE00FE00FE & z0 & z0 >> 8) == 0 &&
        ((g1 ^ 0x0100010001000100) & 0x0101010101010101 & z1) == 0 && ((g1 ^ g1 >> 8) & 0x00FE00FE00FE00FE & z1 & z1 >> 8) == 0 &&
        ((g2 ^ 0x0100010001000100) & 0x0101010101010101 & z2) == 0 && ((g2 ^ g2 >> 8) & 0x00FE00FE00FE00FE & z2 & z2 >> 8) == 0 &&
        ((g3 ^ 0x0100010001000100) & 0x0101010101010101 & z3) == 0 && ((g3 ^ g3 >> 8) & 0x00FE00FE00FE00FE & z3 & z3 >> 8) == 0 ) {
    
        const bool partialzero = int((i0^i1)|(i2^i3)|(i4^i5)|(i6^i7)|(i8^i9)|(i10^i11)|(i12^i13)|(i14^i15)
            |(i16^i17)|(i18^i19)|(i20^i21)|(i22^i23)|(i24^i25)|(i26^i27)|(i28^i29)|(i30^i31)) < 0; // part of a 16-bit block is zeroed
        const int blank1 = partialzero ? -0x100 : -1;  // ignore or zero
        const int n0 = i0 > 0 ? i0 /2 : i1 > 0 ? i1 /2 : blank1;  // indexes for 64 bit blend
        const int n1 = i2 > 0 ? i2 /2 : i3 > 0 ? i3 /2 : blank1;
        const int n2 = i4 > 0 ? i4 /2 : i5 > 0 ? i5 /2 : blank1;
        const int n3 = i6 > 0 ? i6 /2 : i7 > 0 ? i7 /2 : blank1;
        const int n4 = i8 > 0 ? i8 /2 : i9 > 0 ? i9 /2 : blank1;
        const int n5 = i10> 0 ? i10/2 : i11> 0 ? i11/2 : blank1;
        const int n6 = i12> 0 ? i12/2 : i13> 0 ? i13/2 : blank1;
        const int n7 = i14> 0 ? i14/2 : i15> 0 ? i15/2 : blank1;
        const int n8 = i16> 0 ? i16/2 : i17> 0 ? i17/2 : blank1;
        const int n9 = i18> 0 ? i18/2 : i19> 0 ? i19/2 : blank1;
        const int n10= i20> 0 ? i20/2 : i21> 0 ? i21/2 : blank1;
        const int n11= i22> 0 ? i22/2 : i23> 0 ? i23/2 : blank1;
        const int n12= i24> 0 ? i24/2 : i25> 0 ? i25/2 : blank1;
        const int n13= i26> 0 ? i26/2 : i27> 0 ? i27/2 : blank1;
        const int n14= i28> 0 ? i28/2 : i29> 0 ? i29/2 : blank1;
        const int n15= i30> 0 ? i30/2 : i31> 0 ? i31/2 : blank1;
        // do 16-bit permute
        t1 = permute16s<n0,n1,n2,n3,n4,n5,n6,n7,n8,n9,n10,n11,n12,n13,n14,n15> (Vec16s(a));
        if (blank1 == -1 || !dozero) {    
            return  t1;
        }
        // need more zeroing
        mask = constant8i <
            (i0 <0?0:0xFF) | (i1 <0?0:0xFF00) | (i2 <0?0:0xFF0000) | (i3 <0?0:0xFF000000),
            (i4 <0?0:0xFF) | (i5 <0?0:0xFF00) | (i6 <0?0:0xFF0000) | (i7 <0?0:0xFF000000),
            (i8 <0?0:0xFF) | (i9 <0?0:0xFF00) | (i10<0?0:0xFF0000) | (i11<0?0:0xFF000000),
            (i12<0?0:0xFF) | (i13<0?0:0xFF00) | (i14<0?0:0xFF0000) | (i15<0?0:0xFF000000),
            (i16<0?0:0xFF) | (i17<0?0:0xFF00) | (i18<0?0:0xFF0000) | (i19<0?0:0xFF000000),
            (i20<0?0:0xFF) | (i21<0?0:0xFF00) | (i22<0?0:0xFF0000) | (i23<0?0:0xFF000000),
            (i24<0?0:0xFF) | (i25<0?0:0xFF00) | (i26<0?0:0xFF0000) | (i27<0?0:0xFF000000),
            (i28<0?0:0xFF) | (i29<0?0:0xFF00) | (i30<0?0:0xFF0000) | (i31<0?0:0xFF000000) > ();
        return _mm256_and_si256(a, mask);
    } 

    // special case: all elements from same half
    if (((m1 ^ 0xFFFF0000) & mz) == 0) {
        mask = constant8i <
            (i0  & 0xFF) | (i1  & 0xFF) << 8 | (i2  & 0xFF) << 16 | (i3  & 0xFF) << 24,
            (i4  & 0xFF) | (i5  & 0xFF) << 8 | (i6  & 0xFF) << 16 | (i7  & 0xFF) << 24,
            (i8  & 0xFF) | (i9  & 0xFF) << 8 | (i10 & 0xFF) << 16 | (i11 & 0xFF) << 24,
            (i12 & 0xFF) | (i13 & 0xFF) << 8 | (i14 & 0xFF) << 16 | (i15 & 0xFF) << 24,
            (i16 & 0xEF) | (i17 & 0xEF) << 8 | (i18 & 0xEF) << 16 | (i19 & 0xEF) << 24,
            (i20 & 0xEF) | (i21 & 0xEF) << 8 | (i22 & 0xEF) << 16 | (i23 & 0xEF) << 24,
            (i24 & 0xEF) | (i25 & 0xEF) << 8 | (i26 & 0xEF) << 16 | (i27 & 0xEF) << 24,
            (i28 & 0xEF) | (i29 & 0xEF) << 8 | (i30 & 0xEF) << 16 | (i31 & 0xEF) << 24 > ();
        return _mm256_shuffle_epi8(a, mask);
    }

    // special case: all elements from low half
    if ((m1 & mz) == 0) {
        mask = constant8i <
            (i0  & 0xFF) | (i1  & 0xFF) << 8 | (i2  & 0xFF) << 16 | (i3  & 0xFF) << 24,
            (i4  & 0xFF) | (i5  & 0xFF) << 8 | (i6  & 0xFF) << 16 | (i7  & 0xFF) << 24,
            (i8  & 0xFF) | (i9  & 0xFF) << 8 | (i10 & 0xFF) << 16 | (i11 & 0xFF) << 24,
            (i12 & 0xFF) | (i13 & 0xFF) << 8 | (i14 & 0xFF) << 16 | (i15 & 0xFF) << 24,
            (i16 & 0xFF) | (i17 & 0xFF) << 8 | (i18 & 0xFF) << 16 | (i19 & 0xFF) << 24,
            (i20 & 0xFF) | (i21 & 0xFF) << 8 | (i22 & 0xFF) << 16 | (i23 & 0xFF) << 24,
            (i24 & 0xFF) | (i25 & 0xFF) << 8 | (i26 & 0xFF) << 16 | (i27 & 0xFF) << 24,
            (i28 & 0xFF) | (i29 & 0xFF) << 8 | (i30 & 0xFF) << 16 | (i31 & 0xFF) << 24 > ();
        t1 = _mm256_inserti128_si256(a, _mm256_castsi256_si128(a), 1);  // low, low
        return _mm256_shuffle_epi8(t1, mask);
    }

    // special case: all elements from high half
    if (((m1 ^ 0xFFFFFFFF) & mz) == 0) {
        mask = constant8i <
            (i0  & 0xEF) | (i1  & 0xEF) << 8 | (i2  & 0xEF) << 16 | (i3  & 0xEF) << 24,
            (i4  & 0xEF) | (i5  & 0xEF) << 8 | (i6  & 0xEF) << 16 | (i7  & 0xEF) << 24,
            (i8  & 0xEF) | (i9  & 0xEF) << 8 | (i10 & 0xEF) << 16 | (i11 & 0xEF) << 24,
            (i12 & 0xEF) | (i13 & 0xEF) << 8 | (i14 & 0xEF) << 16 | (i15 & 0xEF) << 24,
            (i16 & 0xEF) | (i17 & 0xEF) << 8 | (i18 & 0xEF) << 16 | (i19 & 0xEF) << 24,
            (i20 & 0xEF) | (i21 & 0xEF) << 8 | (i22 & 0xEF) << 16 | (i23 & 0xEF) << 24,
            (i24 & 0xEF) | (i25 & 0xEF) << 8 | (i26 & 0xEF) << 16 | (i27 & 0xEF) << 24,
            (i28 & 0xEF) | (i29 & 0xEF) << 8 | (i30 & 0xEF) << 16 | (i31 & 0xEF) << 24 > ();
        t1 = _mm256_permute4x64_epi64(a, 0xEE);  // high, high
        return _mm256_shuffle_epi8(t1, mask);
    }

    // special case: all elements from opposite half
    if (((m1 ^ 0x0000FFFF) & mz) == 0) {
        mask = constant8i<
            (i0  & 0xEF) | (i1  & 0xEF) << 8 | (i2  & 0xEF) << 16 | (i3  & 0xEF) << 24,
            (i4  & 0xEF) | (i5  & 0xEF) << 8 | (i6  & 0xEF) << 16 | (i7  & 0xEF) << 24,
            (i8  & 0xEF) | (i9  & 0xEF) << 8 | (i10 & 0xEF) << 16 | (i11 & 0xEF) << 24,
            (i12 & 0xEF) | (i13 & 0xEF) << 8 | (i14 & 0xEF) << 16 | (i15 & 0xEF) << 24,
            (i16 & 0xFF) | (i17 & 0xFF) << 8 | (i18 & 0xFF) << 16 | (i19 & 0xFF) << 24,
            (i20 & 0xFF) | (i21 & 0xFF) << 8 | (i22 & 0xFF) << 16 | (i23 & 0xFF) << 24,
            (i24 & 0xFF) | (i25 & 0xFF) << 8 | (i26 & 0xFF) << 16 | (i27 & 0xFF) << 24,
            (i28 & 0xFF) | (i29 & 0xFF) << 8 | (i30 & 0xFF) << 16 | (i31 & 0xFF) << 24 > ();

        t1 = _mm256_permute4x64_epi64(a, 0x4E);  // high, low
        return _mm256_shuffle_epi8(t1, mask);
    }

    // general case: elements from both halves
    const __m256i mmsame = constant8i <
        ((i0 &0xF0)?0xFF:(i0 &15)) | ((i1 &0xF0)?0xFF:(i1 &15)) << 8 | ((i2 &0xF0)?0xFF:(i2 &15)) << 16 | ((i3 &0xF0)?0xFF:(i3 &15)) << 24, 
        ((i4 &0xF0)?0xFF:(i4 &15)) | ((i5 &0xF0)?0xFF:(i5 &15)) << 8 | ((i6 &0xF0)?0xFF:(i6 &15)) << 16 | ((i7 &0xF0)?0xFF:(i7 &15)) << 24, 
        ((i8 &0xF0)?0xFF:(i8 &15)) | ((i9 &0xF0)?0xFF:(i9 &15)) << 8 | ((i10&0xF0)?0xFF:(i10&15)) << 16 | ((i11&0xF0)?0xFF:(i11&15)) << 24, 
        ((i12&0xF0)?0xFF:(i12&15)) | ((i13&0xF0)?0xFF:(i13&15)) << 8 | ((i14&0xF0)?0xFF:(i14&15)) << 16 | ((i15&0xF0)?0xFF:(i15&15)) << 24,
        ((i16&0xF0)!=0x10?0xFF:(i16&15)) | ((i17&0xF0)!=0x10?0xFF:(i17&15)) << 8 | ((i18&0xF0)!=0x10?0xFF:(i18&15)) << 16 | ((i19&0xF0)!=0x10?0xFF:(i19&15)) << 24, 
        ((i20&0xF0)!=0x10?0xFF:(i20&15)) | ((i21&0xF0)!=0x10?0xFF:(i21&15)) << 8 | ((i22&0xF0)!=0x10?0xFF:(i22&15)) << 16 | ((i23&0xF0)!=0x10?0xFF:(i23&15)) << 24, 
        ((i24&0xF0)!=0x10?0xFF:(i24&15)) | ((i25&0xF0)!=0x10?0xFF:(i25&15)) << 8 | ((i26&0xF0)!=0x10?0xFF:(i26&15)) << 16 | ((i27&0xF0)!=0x10?0xFF:(i27&15)) << 24, 
        ((i28&0xF0)!=0x10?0xFF:(i28&15)) | ((i29&0xF0)!=0x10?0xFF:(i29&15)) << 8 | ((i30&0xF0)!=0x10?0xFF:(i30&15)) << 16 | ((i31&0xF0)!=0x10?0xFF:(i31&15)) << 24 > ();

    const __m256i mmopposite = constant8i <
        ((i0 &0xF0)!=0x10?0xFF:(i0 &15)) | ((i1 &0xF0)!=0x10?0xFF:(i1 &15)) << 8 | ((i2 &0xF0)!=0x10?0xFF:(i2 &15)) << 16 | ((i3 &0xF0)!=0x10?0xFF:(i3 &15)) << 24, 
        ((i4 &0xF0)!=0x10?0xFF:(i4 &15)) | ((i5 &0xF0)!=0x10?0xFF:(i5 &15)) << 8 | ((i6 &0xF0)!=0x10?0xFF:(i6 &15)) << 16 | ((i7 &0xF0)!=0x10?0xFF:(i7 &15)) << 24, 
        ((i8 &0xF0)!=0x10?0xFF:(i8 &15)) | ((i9 &0xF0)!=0x10?0xFF:(i9 &15)) << 8 | ((i10&0xF0)!=0x10?0xFF:(i10&15)) << 16 | ((i11&0xF0)!=0x10?0xFF:(i11&15)) << 24, 
        ((i12&0xF0)!=0x10?0xFF:(i12&15)) | ((i13&0xF0)!=0x10?0xFF:(i13&15)) << 8 | ((i14&0xF0)!=0x10?0xFF:(i14&15)) << 16 | ((i15&0xF0)!=0x10?0xFF:(i15&15)) << 24,
        ((i16&0xF0)?0xFF:(i16&15)) | ((i17&0xF0)?0xFF:(i17&15)) << 8 | ((i18&0xF0)?0xFF:(i18&15)) << 16 | ((i19&0xF0)?0xFF:(i19&15)) << 24, 
        ((i20&0xF0)?0xFF:(i20&15)) | ((i21&0xF0)?0xFF:(i21&15)) << 8 | ((i22&0xF0)?0xFF:(i22&15)) << 16 | ((i23&0xF0)?0xFF:(i23&15)) << 24, 
        ((i24&0xF0)?0xFF:(i24&15)) | ((i25&0xF0)?0xFF:(i25&15)) << 8 | ((i26&0xF0)?0xFF:(i26&15)) << 16 | ((i27&0xF0)?0xFF:(i27&15)) << 24, 
        ((i28&0xF0)?0xFF:(i28&15)) | ((i29&0xF0)?0xFF:(i29&15)) << 8 | ((i30&0xF0)?0xFF:(i30&15)) << 16 | ((i31&0xF0)?0xFF:(i31&15)) << 24 > ();

    __m256i topp = _mm256_permute4x64_epi64(a, 0x4E);  // high, low
    __m256i r1   = _mm256_shuffle_epi8(topp, mmopposite);
    __m256i r2   = _mm256_shuffle_epi8(a, mmsame);
    return         _mm256_or_si256(r1, r2);
}

template <
    int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
    int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15,
    int i16, int i17, int i18, int i19, int i20, int i21, int i22, int i23,
    int i24, int i25, int i26, int i27, int i28, int i29, int i30, int i31 >
    static inline Vec32uc permute32uc(Vec32uc const & a) {
        return Vec32uc (permute32c<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15,    
            i16,i17,i18,i19,i20,i21,i22,i23,i24,i25,i26,i27,i28,i29,i30,i31> (a));
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
* Vec8i a(100,101,102,103,104,105,106,107); // a is (100, 101, 102, 103, 104, 105, 106, 107)
* Vec8i b(200,201,202,203,204,205,206,207); // b is (200, 201, 202, 203, 204, 205, 206, 207)
* Vec8i c;
* c = blend8i<1,0,9,8,7,-1,15,15> (a,b);    // c is (101, 100, 201, 200, 107,   0, 207, 207)
*
* A lot of the code here is metaprogramming aiming to find the instructions
* that best fit the template parameters and instruction set. The metacode
* will be reduced out to leave only a few vector instructions in release
* mode with optimization on.
*****************************************************************************/

template <int i0,  int i1,  int i2,  int i3> 
static inline Vec4q blend4q(Vec4q const & a, Vec4q const & b) {  

    // Combine indexes into a single bitfield, with 8 bits for each
    const int m1 = (i0 & 7) | (i1 & 7) << 8 | (i2 & 7) << 16 | (i3 & 7) << 24;

    // Mask to zero out negative indexes
    const int mz = (i0<0 ? 0 : 0xFF) | (i1<0 ? 0 : 0xFF) << 8 | (i2<0 ? 0 : 0xFF) << 16 | (i3<0 ? 0 : 0xFF) << 24;

    // zeroing needed. An index of -0x100 means don't care
    const bool dozero = ((i0|i1|i2|i3) & 0x80) != 0;

    __m256i t1, mask;

    // special case: 128 bit blend/permute
    if (((m1 ^ 0x01000100) & 0x01010101 & mz) == 0 && (((m1 + 0x00010001) ^ (m1 >> 8)) & 0x00FF00FF & mz & mz >> 8) == 0) {
        const int j0 = i0 >= 0 ? i0 / 2 : i1 >= 0 ? i1 / 2 : 4;  // index for low 128 bits
        const int j1 = i2 >= 0 ? i2 / 2 : i3 >= 0 ? i3 / 2 : 4;  // index for high 128 bits
        const bool partialzero = int((i0 ^ i1) | (i2 ^ i3)) < 0; // part of a 128-bit block is zeroed
        __m256i t1;

        switch (j0 | j1 << 4) {
        case 0x00:
            t1 = _mm256_inserti128_si256(a, _mm256_castsi256_si128(a), 1);  break;
        case 0x02:
            t1 = _mm256_inserti128_si256(b, _mm256_castsi256_si128(a), 1);  break;
        case 0x04:
            if (dozero && !partialzero) return _mm256_inserti128_si256(_mm256_setzero_si256(), _mm256_castsi256_si128(a), 1);
            t1 = _mm256_inserti128_si256(a, _mm256_castsi256_si128(a), 1);  break;
        case 0x12:
            t1 = _mm256_inserti128_si256(a, _mm256_castsi256_si128(b), 0);  break;
        case 0x14:
            if (dozero && !partialzero) return _mm256_inserti128_si256(a,_mm_setzero_si128(), 0);
            t1 = a;  break;
        case 0x01: case 0x10: case 0x11: // all from a
            return permute4q <i0, i1, i2, i3> (a);
        case 0x20:
            t1 = _mm256_inserti128_si256(a, _mm256_castsi256_si128(b), 1);  break;
        case 0x22:
            t1 = _mm256_inserti128_si256(b, _mm256_castsi256_si128(b), 1);  break;
        case 0x24:
            if (dozero && !partialzero) return _mm256_inserti128_si256(_mm256_setzero_si256(), _mm256_castsi256_si128(b), 1);
            t1 = _mm256_inserti128_si256(b, _mm256_castsi256_si128(b), 1);  break;
        case 0x30:
            t1 = _mm256_inserti128_si256(b, _mm256_castsi256_si128(a), 0);  break;
        case 0x34:
            if (dozero && !partialzero) return _mm256_inserti128_si256(b,_mm_setzero_si128(), 0);
            t1 = b;  break;
        case 0x23: case 0x32: case 0x33:  // all from b
            return permute4q <i0^4, i1^4, i2^4, i3^4> (b);
        case 0x40:
            if (dozero && !partialzero) return _mm256_castsi128_si256(_mm_and_si128(_mm256_castsi256_si128(a),_mm256_castsi256_si128(a)));
            t1 = a;  break;
        case 0x42:
            if (dozero && !partialzero) return _mm256_castsi128_si256(_mm_and_si128(_mm256_castsi256_si128(b),_mm256_castsi256_si128(b)));
            t1 = b;  break;
        case 0x44:
            return _mm256_setzero_si256();
        default:
            t1 = _mm256_permute2x128_si256(a, b, (j0&0x0F) | (j1&0x0F) << 4);
        }
        if (dozero) {
            // zero some elements
            const __m256i maskz = constant8i <
                i0 < 0 ? 0 : -1, i0 < 0 ? 0 : -1, i1 < 0 ? 0 : -1, i1 < 0 ? 0 : -1, 
                i2 < 0 ? 0 : -1, i2 < 0 ? 0 : -1, i3 < 0 ? 0 : -1, i3 < 0 ? 0 : -1 > ();
            return _mm256_and_si256(t1, maskz);
        }
        return t1;
    }

    // special case: all from a
    if ((m1 & 0x04040404 & mz) == 0) {
        return permute4q <i0, i1, i2, i3> (a);
    }

    // special case: all from b
    if ((~m1 & 0x04040404 & mz) == 0) {
        return permute4q <i0^4, i1^4, i2^4, i3^4> (b);
    }

    // special case: blend without permute
    if (((m1 ^ 0x03020100) & 0xFBFBFBFB & mz) == 0) {

        mask = constant8i <
            (i0 & 4) ? -1 : 0, (i0 & 4) ? -1 : 0, (i1 & 4) ? -1 : 0, (i1 & 4) ? -1 : 0, 
            (i2 & 4) ? -1 : 0, (i2 & 4) ? -1 : 0, (i3 & 4) ? -1 : 0, (i3 & 4) ? -1 : 0 > ();

        t1 = _mm256_blendv_epi8(a, b, mask);  // blend

        if (dozero) {
            // zero some elements
            const __m256i maskz = constant8i <
                i0 < 0 ? 0 : -1, i0 < 0 ? 0 : -1, i1 < 0 ? 0 : -1, i1 < 0 ? 0 : -1, 
                i2 < 0 ? 0 : -1, i2 < 0 ? 0 : -1, i3 < 0 ? 0 : -1, i3 < 0 ? 0 : -1 > ();
            return _mm256_and_si256(t1, maskz);
        }
        return t1;
    } 

    // special case: shift left
    if (i0 > 0 && i0 < 4 && mz == -1 && (m1 ^ ((i0 & 3) * 0x01010101 + 0x03020100)) == 0) {
        t1 = _mm256_permute2x128_si256(a, b, 0x21);
        if (i0 < 2) return _mm256_alignr_epi8(t1, a, (i0 & 1) * 8);
        else        return _mm256_alignr_epi8(b, t1, (i0 & 1) * 8);
    }
    // special case: shift right
    if (i0 > 4 && i0 < 8 && mz == -1 && (m1 ^ 0x04040404 ^ ((i0 & 3) * 0x01010101 + 0x03020100)) == 0) {
        t1 = _mm256_permute2x128_si256(b, a, 0x21);
        if (i0 < 6) return _mm256_alignr_epi8(t1, b, (i0 & 1) * 8);
        else        return _mm256_alignr_epi8(a, t1, (i0 & 1) * 8);
    }

    // general case: permute and blend and possibly zero
    const int blank = dozero ? -1 : -0x100;  // ignore or zero

    // permute and blend
    __m256i ta = permute4q <
        (i0 & 4) ? blank : i0, (i1 & 4) ? blank : i1, (i2 & 4) ? blank : i2, (i3 & 4) ? blank : i3 > (a);

    __m256i tb = permute4q <
        ((i0^4) & 4) ? blank : i0^4, ((i1^4) & 4) ? blank : i1^4, ((i2^4) & 4) ? blank : i2^4, ((i3^4) & 4) ? blank : i3^4 > (b);

    if (blank == -1) {
        // we have zeroed, need only to OR
        return _mm256_or_si256(ta, tb);
    }
    // no zeroing, need to blend
    mask = constant8i <
        (i0 & 4) ? -1 : 0, (i0 & 4) ? -1 : 0, (i1 & 4) ? -1 : 0, (i1 & 4) ? -1 : 0, 
        (i2 & 4) ? -1 : 0, (i2 & 4) ? -1 : 0, (i3 & 4) ? -1 : 0, (i3 & 4) ? -1 : 0 > ();

    return _mm256_blendv_epi8(ta, tb, mask);  // blend
}

template <int i0, int i1, int i2, int i3> 
static inline Vec4uq blend4uq(Vec4uq const & a, Vec4uq const & b) {
    return Vec4uq( blend4q<i0,i1,i2,i3> (a,b));
}


template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7> 
static inline Vec8i blend8i(Vec8i const & a, Vec8i const & b) {  

    const int ior = i0 | i1 | i2 | i3 | i4 | i5 | i6 | i7;  // OR indexes

    // is zeroing needed
    const bool do_zero  = ior < 0 && (ior & 0x80); // at least one index is negative, and not -0x100

    // Combine all the indexes into a single bitfield, with 4 bits for each
    const int m1 = (i0&0xF) | (i1&0xF)<<4 | (i2&0xF)<<8 | (i3&0xF)<<12 | (i4&0xF)<<16 | (i5&0xF)<<20 | (i6&0xF)<<24 | (i7&0xF)<<28;

    // Mask to zero out negative indexes
    const int mz = (i0<0?0:0xF) | (i1<0?0:0xF)<<4 | (i2<0?0:0xF)<<8 | (i3<0?0:0xF)<<12 | (i4<0?0:0xF)<<16 | (i5<0?0:0xF)<<20 | (i6<0?0:0xF)<<24 | (i7<0?0:0xF)<<28;

    __m256i t1, mask;

    if (mz == 0) return _mm256_setzero_si256();  // all zero

    // special case: 64 bit blend/permute
    if (((m1 ^ 0x10101010) & 0x11111111 & mz) == 0 && ((m1 ^ (m1 >> 4)) & 0x0E0E0E0E & mz & mz >> 4) == 0) {
        // check if part of a 64-bit block is zeroed
        const bool partialzero = int((i0^i1) | (i2^i3) | (i4^i5) | (i6^i7)) < 0; 
        const int blank1 = partialzero ? -0x100 : -1;  // ignore if zeroing later anyway
        // indexes for 64 bit blend
        const int j0 = i0 >= 0 ? i0 / 2 : i1 >= 0 ? i1 / 2 : blank1;
        const int j1 = i2 >= 0 ? i2 / 2 : i3 >= 0 ? i3 / 2 : blank1;
        const int j2 = i4 >= 0 ? i4 / 2 : i5 >= 0 ? i5 / 2 : blank1;
        const int j3 = i6 >= 0 ? i6 / 2 : i7 >= 0 ? i7 / 2 : blank1;
        // 64-bit blend and permute
        t1 = blend4q<j0,j1,j2,j3>(Vec4q(a), Vec4q(b));
        if (partialzero && do_zero) {
            // zero some elements
            mask = constant8i< i0 < 0 ? 0 : -1, i1 < 0 ? 0 : -1, i2 < 0 ? 0 : -1, i3 < 0 ? 0 : -1, 
                i4 < 0 ? 0 : -1, i5 < 0 ? 0 : -1, i6 < 0 ? 0 : -1, i7 < 0 ? 0 : -1 > ();
            return _mm256_and_si256(t1, mask);
        }
        return t1;
    }

    if ((m1 & 0x88888888 & mz) == 0) {
        // all from a
        return permute8i<i0, i1, i2, i3, i4, i5, i6, i7> (a);
    }

    if (((m1 ^ 0x88888888) & 0x88888888 & mz) == 0) {
        // all from b
        return permute8i<i0&~8, i1&~8, i2&~8, i3&~8, i4&~8, i5&~8, i6&~8, i7&~8> (b);
    }

    if ((((m1 & 0x77777777) ^ 0x76543210) & mz) == 0) {
        // blend and zero, no permute
        mask = constant8i<(i0&8)?0:-1, (i1&8)?0:-1, (i2&8)?0:-1, (i3&8)?0:-1, (i4&8)?0:-1, (i5&8)?0:-1, (i6&8)?0:-1, (i7&8)?0:-1> ();
        t1   = select(mask, a, b);
        if (!do_zero) return t1;
        // zero some elements
        mask = constant8i< (i0<0&&(i0&8)) ? 0 : -1, (i1<0&&(i1&8)) ? 0 : -1, (i2<0&&(i2&8)) ? 0 : -1, (i3<0&&(i3&8)) ? 0 : -1, 
            (i4<0&&(i4&8)) ? 0 : -1, (i5<0&&(i5&8)) ? 0 : -1, (i6<0&&(i6&8)) ? 0 : -1, (i7<0&&(i7&8)) ? 0 : -1 > ();
        return _mm256_and_si256(t1, mask);
    }

    // special case: shift left
    if (i0 > 0 && i0 < 8 && mz == -1 && (m1 ^ ((i0 & 7) * 0x11111111u + 0x76543210u)) == 0) {
        t1 = _mm256_permute2x128_si256(a, b, 0x21);
        if (i0 < 4) return _mm256_alignr_epi8(t1, a, (i0 & 3) * 4);
        else        return _mm256_alignr_epi8(b, t1, (i0 & 3) * 4);
    }
    // special case: shift right
    if (i0 > 8 && i0 < 16 && mz == -1 && (m1 ^ 0x88888888 ^ ((i0 & 7) * 0x11111111u + 0x76543210u)) == 0) {
        t1 = _mm256_permute2x128_si256(b, a, 0x21);
        if (i0 < 12) return _mm256_alignr_epi8(t1, b, (i0 & 3) * 4);
        else         return _mm256_alignr_epi8(a, t1, (i0 & 3) * 4);
    }

    // general case: permute and blend and possible zero
    const int blank = do_zero ? -1 : -0x100;  // ignore or zero

    Vec8i ta = permute8i <
        (uint32_t)i0 < 8 ? i0 : blank,
        (uint32_t)i1 < 8 ? i1 : blank,
        (uint32_t)i2 < 8 ? i2 : blank,
        (uint32_t)i3 < 8 ? i3 : blank,
        (uint32_t)i4 < 8 ? i4 : blank,
        (uint32_t)i5 < 8 ? i5 : blank,
        (uint32_t)i6 < 8 ? i6 : blank,
        (uint32_t)i7 < 8 ? i7 : blank > (a);
    Vec8i tb = permute8i <
        (uint32_t)(i0^8) < 8 ? (i0^8) : blank,
        (uint32_t)(i1^8) < 8 ? (i1^8) : blank,
        (uint32_t)(i2^8) < 8 ? (i2^8) : blank,
        (uint32_t)(i3^8) < 8 ? (i3^8) : blank,
        (uint32_t)(i4^8) < 8 ? (i4^8) : blank,
        (uint32_t)(i5^8) < 8 ? (i5^8) : blank,
        (uint32_t)(i6^8) < 8 ? (i6^8) : blank,
        (uint32_t)(i7^8) < 8 ? (i7^8) : blank > (b);
    if (blank == -1) {    
        return  _mm256_or_si256(ta, tb); 
    }
    // no zeroing, need to blend
    const int maskb = ((i0 >> 3) & 1) | ((i1 >> 2) & 2) | ((i2 >> 1) & 4) | (i3 & 8) | 
        ((i4 << 1) & 0x10) | ((i5 << 2) & 0x20) | ((i6 << 3) & 0x40) | ((i7 << 4) & 0x80);
    return _mm256_blend_epi32(ta, tb, maskb);  // blend
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7> 
static inline Vec8ui blend8ui(Vec8ui const & a, Vec8ui const & b) {
    return Vec8ui( blend8i<i0,i1,i2,i3,i4,i5,i6,i7> (a,b));
}


template <int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15 > 
static inline Vec16s blend16s(Vec16s const & a, Vec16s const & b) {  
    //  #ifdef __XOP2__  // Possible future 256-bit XOP extension ?

    // collect bit 4 of each index
    const int m1 = 
        (i0 &16)>>4  | (i1 &16)>>3  | (i2 &16)>>2  | (i3 &16)>>1  | (i4 &16)     | (i5 &16)<<1  | (i6 &16)<<2  | (i7 &16)<<3  | 
        (i8 &16)<<4  | (i9 &16)<<5  | (i10&16)<<6  | (i11&16)<<7  | (i12&16)<<8  | (i13&16)<<9  | (i14&16)<<10 | (i15&16)<<11 ;

    // check which elements to set to zero
    const int mz = 0x0000FFFF ^ (
        (i0 <0)     | (i1 <0)<<1  | (i2 <0)<<2  | (i3 <0)<<3  | (i4 <0)<<4  | (i5 <0)<<5  | (i6 <0)<<6  | (i7 <0)<<7  | 
        (i8 <0)<<8  | (i9 <0)<<9  | (i10<0)<<10 | (i11<0)<<11 | (i12<0)<<12 | (i13<0)<<13 | (i14<0)<<14 | (i15<0)<<15 );

    __m256i t1, mask;

    // special case: all zero
    if (mz == 0) return  _mm256_setzero_si256();

    // special case: all from a
    if ((m1 & mz) == 0) {
        return permute16s<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15> (a);
    }

    // special case: all from b
    if (((m1 ^ 0xFFFF) & mz) == 0) {
        return permute16s<i0^16,i1^16,i2^16,i3^16,i4^16,i5^16,i6^16,i7^16,i8^16,i9^16,i10^16,i11^16,i12^16,i13^16,i14^16,i15^16 > (b);
    }

    // special case: blend without permute
    if ((i0 <0||(i0 &15)== 0) && (i1 <0||(i1 &15)== 1) && (i2 <0||(i2 &15)== 2) && (i3 <0||(i3 &15)== 3) && 
        (i4 <0||(i4 &15)== 4) && (i5 <0||(i5 &15)== 5) && (i6 <0||(i6 &15)== 6) && (i7 <0||(i7 &15)== 7) && 
        (i8 <0||(i8 &15)== 8) && (i9 <0||(i9 &15)== 9) && (i10<0||(i10&15)==10) && (i11<0||(i11&15)==11) && 
        (i12<0||(i12&15)==12) && (i13<0||(i13&15)==13) && (i14<0||(i14&15)==14) && (i15<0||(i15&15)==15)) {

        mask = constant8i <
            ((i0 & 16) ? 0xFFFF : 0) | ((i1 & 16) ? 0xFFFF0000 : 0),
            ((i2 & 16) ? 0xFFFF : 0) | ((i3 & 16) ? 0xFFFF0000 : 0),
            ((i4 & 16) ? 0xFFFF : 0) | ((i5 & 16) ? 0xFFFF0000 : 0),
            ((i6 & 16) ? 0xFFFF : 0) | ((i7 & 16) ? 0xFFFF0000 : 0),
            ((i8 & 16) ? 0xFFFF : 0) | ((i9 & 16) ? 0xFFFF0000 : 0),
            ((i10& 16) ? 0xFFFF : 0) | ((i11& 16) ? 0xFFFF0000 : 0),
            ((i12& 16) ? 0xFFFF : 0) | ((i13& 16) ? 0xFFFF0000 : 0),
            ((i14& 16) ? 0xFFFF : 0) | ((i15& 16) ? 0xFFFF0000 : 0) > ();

        t1 = _mm256_blendv_epi8(a, b, mask);  // blend

        if (mz != 0xFFFF) {
            // zero some elements
            mask = constant8i <
                (i0  < 0 ? 0 : 0xFFFF) | (i1  < 0 ? 0 : 0xFFFF0000),
                (i2  < 0 ? 0 : 0xFFFF) | (i3  < 0 ? 0 : 0xFFFF0000),
                (i4  < 0 ? 0 : 0xFFFF) | (i5  < 0 ? 0 : 0xFFFF0000),
                (i6  < 0 ? 0 : 0xFFFF) | (i7  < 0 ? 0 : 0xFFFF0000),
                (i8  < 0 ? 0 : 0xFFFF) | (i9  < 0 ? 0 : 0xFFFF0000),
                (i10 < 0 ? 0 : 0xFFFF) | (i11 < 0 ? 0 : 0xFFFF0000),
                (i12 < 0 ? 0 : 0xFFFF) | (i13 < 0 ? 0 : 0xFFFF0000),
                (i14 < 0 ? 0 : 0xFFFF) | (i15 < 0 ? 0 : 0xFFFF0000) > ();
            return _mm256_and_si256(t1, mask);
        }
        return t1;
    }

    // special case: shift left
    const int slb = i0 > 0 ? i0 : i15 - 15;
    if (slb > 0 && slb < 16 
        && (i0==slb+ 0||i0<0) && (i1==slb+ 1||i1<0) && (i2 ==slb+ 2||i2 <0) && (i3 ==slb+ 3||i3 <0) && (i4 ==slb+ 4||i4 <0) && (i5 ==slb+ 5||i5 <0) && (i6 ==slb+ 6||i6 <0) && (i7 ==slb+ 7||i7 <0)
        && (i8==slb+ 8||i8<0) && (i9==slb+ 9||i9<0) && (i10==slb+10||i10<0) && (i11==slb+11||i11<0) && (i12==slb+12||i12<0) && (i13==slb+13||i13<0) && (i14==slb+14||i14<0) && (i15==slb+15||i15<0)) {
        t1 = _mm256_permute2x128_si256(a, b, 0x21);
        if (slb < 8) t1 = _mm256_alignr_epi8(t1, a, (slb & 7) * 2);
        else         t1 = _mm256_alignr_epi8(b, t1, (slb & 7) * 2);
        if (mz != 0xFFFF) {
            // zero some elements
            mask = constant8i <
                (i0  < 0 ? 0 : 0xFFFF) | (i1  < 0 ? 0 : 0xFFFF0000),
                (i2  < 0 ? 0 : 0xFFFF) | (i3  < 0 ? 0 : 0xFFFF0000),
                (i4  < 0 ? 0 : 0xFFFF) | (i5  < 0 ? 0 : 0xFFFF0000),
                (i6  < 0 ? 0 : 0xFFFF) | (i7  < 0 ? 0 : 0xFFFF0000),
                (i8  < 0 ? 0 : 0xFFFF) | (i9  < 0 ? 0 : 0xFFFF0000),
                (i10 < 0 ? 0 : 0xFFFF) | (i11 < 0 ? 0 : 0xFFFF0000),
                (i12 < 0 ? 0 : 0xFFFF) | (i13 < 0 ? 0 : 0xFFFF0000),
                (i14 < 0 ? 0 : 0xFFFF) | (i15 < 0 ? 0 : 0xFFFF0000) > ();
            return _mm256_and_si256(t1, mask);
        }
        return t1;
    }
    // special case: shift right
    const int srb = i0 > 0 ? (i0^16) : (i15^16) - 15;
    if (srb > 0 && srb < 16
        && ((i0 ^16)==srb+ 0||i0 <0) && ((i1 ^16)==srb+ 1||i1 <0) && ((i2 ^16)==srb+ 2||i2 <0) && ((i3 ^16)==srb+ 3||i3 <0) && ((i4 ^16)==srb+ 4||i4 <0) && ((i5 ^16)==srb+ 5||i5 <0) && ((i6 ^16)==srb+ 6||i6 <0) && ((i7 ^16)==srb+ 7||i7 <0)
        && ((i8 ^16)==srb+ 8||i8 <0) && ((i9 ^16)==srb+ 9||i9 <0) && ((i10^16)==srb+10||i10<0) && ((i11^16)==srb+11||i11<0) && ((i12^16)==srb+12||i12<0) && ((i13^16)==srb+13||i13<0) && ((i14^16)==srb+14||i14<0) && ((i15^16)==srb+15||i15<0)) {
        t1 = _mm256_permute2x128_si256(b, a, 0x21);
        if (srb < 8) t1 = _mm256_alignr_epi8(t1, b, (srb & 7) * 2);
        else         t1 = _mm256_alignr_epi8(a, t1, (srb & 7) * 2);
        if (mz != 0xFFFF) {
            // zero some elements
            mask = constant8i <
                (i0  < 0 ? 0 : 0xFFFF) | (i1  < 0 ? 0 : 0xFFFF0000),
                (i2  < 0 ? 0 : 0xFFFF) | (i3  < 0 ? 0 : 0xFFFF0000),
                (i4  < 0 ? 0 : 0xFFFF) | (i5  < 0 ? 0 : 0xFFFF0000),
                (i6  < 0 ? 0 : 0xFFFF) | (i7  < 0 ? 0 : 0xFFFF0000),
                (i8  < 0 ? 0 : 0xFFFF) | (i9  < 0 ? 0 : 0xFFFF0000),
                (i10 < 0 ? 0 : 0xFFFF) | (i11 < 0 ? 0 : 0xFFFF0000),
                (i12 < 0 ? 0 : 0xFFFF) | (i13 < 0 ? 0 : 0xFFFF0000),
                (i14 < 0 ? 0 : 0xFFFF) | (i15 < 0 ? 0 : 0xFFFF0000) > ();
            return _mm256_and_si256(t1, mask);
        }
        return t1;
    }
    
    // general case: permute and blend and possibly zero
    const int blank = (mz == 0xFFFF) ? -0x100 : -1;  // ignore or zero

    // permute and blend
    __m256i ta = permute16s <
        (i0 &16)?blank:i0 , (i1 &16)?blank:i1 , (i2 &16)?blank:i2 , (i3 &16)?blank:i3 ,
        (i4 &16)?blank:i4 , (i5 &16)?blank:i5 , (i6 &16)?blank:i6 , (i7 &16)?blank:i7 ,
        (i8 &16)?blank:i8 , (i9 &16)?blank:i9 , (i10&16)?blank:i10, (i11&16)?blank:i11,
        (i12&16)?blank:i12, (i13&16)?blank:i13, (i14&16)?blank:i14, (i15&16)?blank:i15 > (a);

    __m256i tb = permute16s <
        ((i0 ^16)&16)?blank:i0 ^16, ((i1 ^16)&16)?blank:i1 ^16, ((i2 ^16)&16)?blank:i2 ^16, ((i3 ^16)&16)?blank:i3 ^16, 
        ((i4 ^16)&16)?blank:i4 ^16, ((i5 ^16)&16)?blank:i5 ^16, ((i6 ^16)&16)?blank:i6 ^16, ((i7 ^16)&16)?blank:i7 ^16, 
        ((i8 ^16)&16)?blank:i8 ^16, ((i9 ^16)&16)?blank:i9 ^16, ((i10^16)&16)?blank:i10^16, ((i11^16)&16)?blank:i11^16,
        ((i12^16)&16)?blank:i12^16, ((i13^16)&16)?blank:i13^16, ((i14^16)&16)?blank:i14^16, ((i15^16)&16)?blank:i15^16 > (b);

    if (blank == -1) {
        // we have zeroed, need only to OR
        return _mm256_or_si256(ta, tb);
    }
    // no zeroing, need to blend
    mask = constant8i <
        ((i0 & 16) ? 0xFFFF : 0) | ((i1 & 16) ? 0xFFFF0000 : 0),
        ((i2 & 16) ? 0xFFFF : 0) | ((i3 & 16) ? 0xFFFF0000 : 0),
        ((i4 & 16) ? 0xFFFF : 0) | ((i5 & 16) ? 0xFFFF0000 : 0),
        ((i6 & 16) ? 0xFFFF : 0) | ((i7 & 16) ? 0xFFFF0000 : 0),
        ((i8 & 16) ? 0xFFFF : 0) | ((i9 & 16) ? 0xFFFF0000 : 0),
        ((i10& 16) ? 0xFFFF : 0) | ((i11& 16) ? 0xFFFF0000 : 0),
        ((i12& 16) ? 0xFFFF : 0) | ((i13& 16) ? 0xFFFF0000 : 0),
        ((i14& 16) ? 0xFFFF : 0) | ((i15& 16) ? 0xFFFF0000 : 0) > ();

    return _mm256_blendv_epi8(ta, tb, mask);  // blend
}

template <int i0, int i1, int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15 > 
static inline Vec16us blend16us(Vec16us const & a, Vec16us const & b) {
    return Vec16us( blend16s<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15> (a,b));
}

template <int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15,
          int i16, int i17, int i18, int i19, int i20, int i21, int i22, int i23,
          int i24, int i25, int i26, int i27, int i28, int i29, int i30, int i31 > 
static inline Vec32c blend32c(Vec32c const & a, Vec32c const & b) {  
    //  #ifdef __XOP2__  // Possible future 256-bit XOP extension ?

    // collect bit 5 of each index
    const int m1 = 
        (i0 &32)>>5  | (i1 &32)>>4  | (i2 &32)>>3  | (i3 &32)>>2  | (i4 &32)>>1  | (i5 &32)     | (i6 &32)<<1  | (i7 &32)<<2  | 
        (i8 &32)<<3  | (i9 &32)<<4  | (i10&32)<<5  | (i11&32)<<6  | (i12&32)<<7  | (i13&32)<<8  | (i14&32)<<9  | (i15&32)<<10 | 
        (i16&32)<<11 | (i17&32)<<12 | (i18&32)<<13 | (i19&32)<<14 | (i20&32)<<15 | (i21&32)<<16 | (i22&32)<<17 | (i23&32)<<18 | 
        (i24&32)<<19 | (i25&32)<<20 | (i26&32)<<21 | (i27&32)<<22 | (i28&32)<<23 | (i29&32)<<24 | (i30&32)<<25 | (i31&32)<<26 ;

    // check which elements to set to zero
    const int mz = ~ (
        (i0 <0)     | (i1 <0)<<1  | (i2 <0)<<2  | (i3 <0)<<3  | (i4 <0)<<4  | (i5 <0)<<5  | (i6 <0)<<6  | (i7 <0)<<7  | 
        (i8 <0)<<8  | (i9 <0)<<9  | (i10<0)<<10 | (i11<0)<<11 | (i12<0)<<12 | (i13<0)<<13 | (i14<0)<<14 | (i15<0)<<15 | 
        (i16<0)<<16 | (i17<0)<<17 | (i18<0)<<18 | (i19<0)<<19 | (i20<0)<<20 | (i21<0)<<21 | (i22<0)<<22 | (i23<0)<<23 | 
        (i24<0)<<24 | (i25<0)<<25 | (i26<0)<<26 | (i27<0)<<27 | (i28<0)<<28 | (i29<0)<<29 | (i30<0)<<30 | (i31<0)<<31 );

    __m256i t1, mask;

    // special case: all zero
    if (mz == 0) return  _mm256_setzero_si256();

    // special case: all from a
    if ((m1 & mz) == 0) {
        return permute32c<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15,
            i16,i17,i18,i19,i20,i21,i22,i23,i24,i25,i26,i27,i28,i29,i30,i31> (a);
    }

    // special case: all from b
    if ((~m1 & mz) == 0) {
        return permute32c<i0^32,i1^32,i2^32,i3^32,i4^32,i5^32,i6^32,i7^32,i8^32,i9^32,i10^32,i11^32,i12^32,i13^32,i14^32,i15^32,
            i16^32,i17^32,i18^32,i19^32,i20^32,i21^32,i22^32,i23^32,i24^32,i25^32,i26^32,i27^32,i28^32,i29^32,i30^32,i31^32> (b);
    }

    // special case: blend without permute
    if ((i0 <0||(i0 &31)== 0) && (i1 <0||(i1 &31)== 1) && (i2 <0||(i2 &31)== 2) && (i3 <0||(i3 &31)== 3) && 
        (i4 <0||(i4 &31)== 4) && (i5 <0||(i5 &31)== 5) && (i6 <0||(i6 &31)== 6) && (i7 <0||(i7 &31)== 7) && 
        (i8 <0||(i8 &31)== 8) && (i9 <0||(i9 &31)== 9) && (i10<0||(i10&31)==10) && (i11<0||(i11&31)==11) && 
        (i12<0||(i12&31)==12) && (i13<0||(i13&31)==13) && (i14<0||(i14&31)==14) && (i15<0||(i15&31)==15) &&
        (i16<0||(i16&31)==16) && (i17<0||(i17&31)==17) && (i18<0||(i18&31)==18) && (i19<0||(i19&31)==19) && 
        (i20<0||(i20&31)==20) && (i21<0||(i21&31)==21) && (i22<0||(i22&31)==22) && (i23<0||(i23&31)==23) && 
        (i24<0||(i24&31)==24) && (i25<0||(i25&31)==25) && (i26<0||(i26&31)==26) && (i27<0||(i27&31)==27) && 
        (i28<0||(i28&31)==28) && (i29<0||(i29&31)==29) && (i30<0||(i30&31)==30) && (i31<0||(i31&31)==31) ) {

        mask = constant8i <
            ((i0 <<2)&0x80) | ((i1 <<10)&0x8000) | ((i2 <<18)&0x800000) | (uint32_t(i3 <<26)&0x80000000) ,
            ((i4 <<2)&0x80) | ((i5 <<10)&0x8000) | ((i6 <<18)&0x800000) | (uint32_t(i7 <<26)&0x80000000) ,
            ((i8 <<2)&0x80) | ((i9 <<10)&0x8000) | ((i10<<18)&0x800000) | (uint32_t(i11<<26)&0x80000000) ,
            ((i12<<2)&0x80) | ((i13<<10)&0x8000) | ((i14<<18)&0x800000) | (uint32_t(i15<<26)&0x80000000) ,
            ((i16<<2)&0x80) | ((i17<<10)&0x8000) | ((i18<<18)&0x800000) | (uint32_t(i19<<26)&0x80000000) ,
            ((i20<<2)&0x80) | ((i21<<10)&0x8000) | ((i22<<18)&0x800000) | (uint32_t(i23<<26)&0x80000000) ,
            ((i24<<2)&0x80) | ((i25<<10)&0x8000) | ((i26<<18)&0x800000) | (uint32_t(i27<<26)&0x80000000) ,
            ((i28<<2)&0x80) | ((i29<<10)&0x8000) | ((i30<<18)&0x800000) | (uint32_t(i31<<26)&0x80000000) > ();

        t1 = _mm256_blendv_epi8(a, b, mask);  // blend

        if (mz != -1) {
            // zero some elements
            const __m256i maskz = constant8i <
                (i0 <0?0:0xFF) | (i1 <0?0:0xFF00) | (i2 <0?0:0xFF0000) | (i3 <0?0:0xFF000000),
                (i4 <0?0:0xFF) | (i5 <0?0:0xFF00) | (i6 <0?0:0xFF0000) | (i7 <0?0:0xFF000000),
                (i8 <0?0:0xFF) | (i9 <0?0:0xFF00) | (i10<0?0:0xFF0000) | (i11<0?0:0xFF000000),
                (i12<0?0:0xFF) | (i13<0?0:0xFF00) | (i14<0?0:0xFF0000) | (i15<0?0:0xFF000000),
                (i16<0?0:0xFF) | (i17<0?0:0xFF00) | (i18<0?0:0xFF0000) | (i19<0?0:0xFF000000),
                (i20<0?0:0xFF) | (i21<0?0:0xFF00) | (i22<0?0:0xFF0000) | (i23<0?0:0xFF000000),
                (i24<0?0:0xFF) | (i25<0?0:0xFF00) | (i26<0?0:0xFF0000) | (i27<0?0:0xFF000000),
                (i28<0?0:0xFF) | (i29<0?0:0xFF00) | (i30<0?0:0xFF0000) | (i31<0?0:0xFF000000) > ();
            return _mm256_and_si256(t1, maskz);
        }
        return t1;
    }

    // special case: shift left
    const int slb = i0 > 0 ? i0 : i31 - 31;
    if (slb > 0 && slb < 32 
        && (i0 ==slb+ 0||i0 <0) && (i1 ==slb+ 1||i1 <0) && (i2 ==slb+ 2||i2 <0) && (i3 ==slb+ 3||i3 <0)
        && (i4 ==slb+ 4||i4 <0) && (i5 ==slb+ 5||i5 <0) && (i6 ==slb+ 6||i6 <0) && (i7 ==slb+ 7||i7 <0)
        && (i8 ==slb+ 8||i8 <0) && (i9 ==slb+ 9||i9 <0) && (i10==slb+10||i10<0) && (i11==slb+11||i11<0)
        && (i12==slb+12||i12<0) && (i13==slb+13||i13<0) && (i14==slb+14||i14<0) && (i15==slb+15||i15<0)
        && (i16==slb+16||i16<0) && (i17==slb+17||i17<0) && (i18==slb+18||i18<0) && (i19==slb+19||i19<0)
        && (i20==slb+20||i20<0) && (i21==slb+21||i21<0) && (i22==slb+22||i22<0) && (i23==slb+23||i23<0)
        && (i24==slb+24||i24<0) && (i25==slb+25||i25<0) && (i26==slb+26||i26<0) && (i27==slb+27||i27<0)
        && (i28==slb+28||i28<0) && (i29==slb+29||i29<0) && (i30==slb+30||i30<0) && (i31==slb+31||i31<0)) {
        t1 = _mm256_permute2x128_si256(a, b, 0x21);
        if (slb < 16) t1 = _mm256_alignr_epi8(t1, a, slb & 15);
        else          t1 = _mm256_alignr_epi8(b, t1, slb & 15);
        if (mz != -1) {
            // zero some elements
            const __m256i maskz = constant8i <
                (i0 <0?0:0xFF) | (i1 <0?0:0xFF00) | (i2 <0?0:0xFF0000) | (i3 <0?0:0xFF000000),
                (i4 <0?0:0xFF) | (i5 <0?0:0xFF00) | (i6 <0?0:0xFF0000) | (i7 <0?0:0xFF000000),
                (i8 <0?0:0xFF) | (i9 <0?0:0xFF00) | (i10<0?0:0xFF0000) | (i11<0?0:0xFF000000),
                (i12<0?0:0xFF) | (i13<0?0:0xFF00) | (i14<0?0:0xFF0000) | (i15<0?0:0xFF000000),
                (i16<0?0:0xFF) | (i17<0?0:0xFF00) | (i18<0?0:0xFF0000) | (i19<0?0:0xFF000000),
                (i20<0?0:0xFF) | (i21<0?0:0xFF00) | (i22<0?0:0xFF0000) | (i23<0?0:0xFF000000),
                (i24<0?0:0xFF) | (i25<0?0:0xFF00) | (i26<0?0:0xFF0000) | (i27<0?0:0xFF000000),
                (i28<0?0:0xFF) | (i29<0?0:0xFF00) | (i30<0?0:0xFF0000) | (i31<0?0:0xFF000000) > ();
            return _mm256_and_si256(t1, maskz);
        }
        return t1;
    }
    // special case: shift right
    const int srb = i0 > 0 ? (i0^32) : (i31^32) - 31;
    if (srb > 0 && srb < 32
        && ((i0 ^32)==srb+ 0||i0 <0) && ((i1 ^32)==srb+ 1||i1 <0) && ((i2 ^32)==srb+ 2||i2 <0) && ((i3 ^32)==srb+ 3||i3 <0)
        && ((i4 ^32)==srb+ 4||i4 <0) && ((i5 ^32)==srb+ 5||i5 <0) && ((i6 ^32)==srb+ 6||i6 <0) && ((i7 ^32)==srb+ 7||i7 <0)
        && ((i8 ^32)==srb+ 8||i8 <0) && ((i9 ^32)==srb+ 9||i9 <0) && ((i10^32)==srb+10||i10<0) && ((i11^32)==srb+11||i11<0)
        && ((i12^32)==srb+12||i12<0) && ((i13^32)==srb+13||i13<0) && ((i14^32)==srb+14||i14<0) && ((i15^32)==srb+15||i15<0)
        && ((i16^32)==srb+16||i16<0) && ((i17^32)==srb+17||i17<0) && ((i18^32)==srb+18||i18<0) && ((i19^32)==srb+19||i19<0)
        && ((i20^32)==srb+20||i20<0) && ((i21^32)==srb+21||i21<0) && ((i22^32)==srb+22||i22<0) && ((i23^32)==srb+23||i23<0)
        && ((i24^32)==srb+24||i24<0) && ((i25^32)==srb+25||i25<0) && ((i26^32)==srb+26||i26<0) && ((i27^32)==srb+27||i27<0)
        && ((i28^32)==srb+28||i28<0) && ((i29^32)==srb+29||i29<0) && ((i30^32)==srb+30||i30<0) && ((i31^32)==srb+31||i31<0)) {
        t1 = _mm256_permute2x128_si256(b, a, 0x21);
        if (srb < 16) t1 = _mm256_alignr_epi8(t1, b, srb & 15);
        else          t1 = _mm256_alignr_epi8(a, t1, srb & 15);
        if (mz != -1) {
            // zero some elements
            const __m256i maskz = constant8i <
                (i0 <0?0:0xFF) | (i1 <0?0:0xFF00) | (i2 <0?0:0xFF0000) | (i3 <0?0:0xFF000000),
                (i4 <0?0:0xFF) | (i5 <0?0:0xFF00) | (i6 <0?0:0xFF0000) | (i7 <0?0:0xFF000000),
                (i8 <0?0:0xFF) | (i9 <0?0:0xFF00) | (i10<0?0:0xFF0000) | (i11<0?0:0xFF000000),
                (i12<0?0:0xFF) | (i13<0?0:0xFF00) | (i14<0?0:0xFF0000) | (i15<0?0:0xFF000000),
                (i16<0?0:0xFF) | (i17<0?0:0xFF00) | (i18<0?0:0xFF0000) | (i19<0?0:0xFF000000),
                (i20<0?0:0xFF) | (i21<0?0:0xFF00) | (i22<0?0:0xFF0000) | (i23<0?0:0xFF000000),
                (i24<0?0:0xFF) | (i25<0?0:0xFF00) | (i26<0?0:0xFF0000) | (i27<0?0:0xFF000000),
                (i28<0?0:0xFF) | (i29<0?0:0xFF00) | (i30<0?0:0xFF0000) | (i31<0?0:0xFF000000) > ();
            return _mm256_and_si256(t1, maskz);
        }
        return t1;
    }

    // general case: permute and blend and possible zero
    const int blank = (mz == -1) ? -0x100 : -1;  // ignore or zero

    // permute and blend
    __m256i ta = permute32c <
        (i0 &32)?blank:i0 , (i1 &32)?blank:i1 , (i2 &32)?blank:i2 , (i3 &32)?blank:i3 , 
        (i4 &32)?blank:i4 , (i5 &32)?blank:i5 , (i6 &32)?blank:i6 , (i7 &32)?blank:i7 , 
        (i8 &32)?blank:i8 , (i9 &32)?blank:i9 , (i10&32)?blank:i10, (i11&32)?blank:i11,
        (i12&32)?blank:i12, (i13&32)?blank:i13, (i14&32)?blank:i14, (i15&32)?blank:i15, 
        (i16&32)?blank:i16, (i17&32)?blank:i17, (i18&32)?blank:i18, (i19&32)?blank:i19, 
        (i20&32)?blank:i20, (i21&32)?blank:i21, (i22&32)?blank:i22, (i23&32)?blank:i23, 
        (i24&32)?blank:i24, (i25&32)?blank:i25, (i26&32)?blank:i26, (i27&32)?blank:i27, 
        (i28&32)?blank:i28, (i29&32)?blank:i29, (i30&32)?blank:i30, (i31&32)?blank:i31 > (a);

    __m256i tb = permute32c <
        ((i0 ^32)&32)?blank:i0 ^32, ((i1 ^32)&32)?blank:i1 ^32, ((i2 ^32)&32)?blank:i2 ^32, ((i3 ^32)&32)?blank:i3 ^32, 
        ((i4 ^32)&32)?blank:i4 ^32, ((i5 ^32)&32)?blank:i5 ^32, ((i6 ^32)&32)?blank:i6 ^32, ((i7 ^32)&32)?blank:i7 ^32, 
        ((i8 ^32)&32)?blank:i8 ^32, ((i9 ^32)&32)?blank:i9 ^32, ((i10^32)&32)?blank:i10^32, ((i11^32)&32)?blank:i11^32,
        ((i12^32)&32)?blank:i12^32, ((i13^32)&32)?blank:i13^32, ((i14^32)&32)?blank:i14^32, ((i15^32)&32)?blank:i15^32,
        ((i16^32)&32)?blank:i16^32, ((i17^32)&32)?blank:i17^32, ((i18^32)&32)?blank:i18^32, ((i19^32)&32)?blank:i19^32,
        ((i20^32)&32)?blank:i20^32, ((i21^32)&32)?blank:i21^32, ((i22^32)&32)?blank:i22^32, ((i23^32)&32)?blank:i23^32,
        ((i24^32)&32)?blank:i24^32, ((i25^32)&32)?blank:i25^32, ((i26^32)&32)?blank:i26^32, ((i27^32)&32)?blank:i27^32,
        ((i28^32)&32)?blank:i28^32, ((i29^32)&32)?blank:i29^32, ((i30^32)&32)?blank:i30^32, ((i31^32)&32)?blank:i31^32 > (b);

    if (blank == -1) {
        // we have zeroed, need only to OR
        return _mm256_or_si256(ta, tb);
    }
    // no zeroing, need to blend
    mask = constant8i <
        ((i0 <<2)&0x80) | ((i1 <<10)&0x8000) | ((i2 <<18)&0x800000) | (uint32_t(i3 <<26)&0x80000000) ,
        ((i4 <<2)&0x80) | ((i5 <<10)&0x8000) | ((i6 <<18)&0x800000) | (uint32_t(i7 <<26)&0x80000000) ,
        ((i8 <<2)&0x80) | ((i9 <<10)&0x8000) | ((i10<<18)&0x800000) | (uint32_t(i11<<26)&0x80000000) ,
        ((i12<<2)&0x80) | ((i13<<10)&0x8000) | ((i14<<18)&0x800000) | (uint32_t(i15<<26)&0x80000000) ,
        ((i16<<2)&0x80) | ((i17<<10)&0x8000) | ((i18<<18)&0x800000) | (uint32_t(i19<<26)&0x80000000) ,
        ((i20<<2)&0x80) | ((i21<<10)&0x8000) | ((i22<<18)&0x800000) | (uint32_t(i23<<26)&0x80000000) ,
        ((i24<<2)&0x80) | ((i25<<10)&0x8000) | ((i26<<18)&0x800000) | (uint32_t(i27<<26)&0x80000000) ,
        ((i28<<2)&0x80) | ((i29<<10)&0x8000) | ((i30<<18)&0x800000) | (uint32_t(i31<<26)&0x80000000) > ();

    return _mm256_blendv_epi8(ta, tb, mask);  // blend
}

template <
    int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
    int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15,
    int i16, int i17, int i18, int i19, int i20, int i21, int i22, int i23,
    int i24, int i25, int i26, int i27, int i28, int i29, int i30, int i31 >
    static inline Vec32uc blend32uc(Vec32uc const & a, Vec32uc const & b) {
        return Vec32uc (blend32c<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15,    
            i16,i17,i18,i19,i20,i21,i22,i23,i24,i25,i26,i27,i28,i29,i30,i31> (a, b));
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
* Vec8i a(2,0,0,6,4,3,5,0);                 // index a is (  2,   0,   0,   6,   4,   3,   5,   0)
* Vec8i b(100,101,102,103,104,105,106,107); // table b is (100, 101, 102, 103, 104, 105, 106, 107)
* Vec8i c;
* c = lookup8 (a,b);                        // c is       (102, 100, 100, 106, 104, 103, 105, 100)
*
*****************************************************************************/

static inline Vec32c lookup32(Vec32c const & index, Vec32c const & table) {
#ifdef __XOP__  // AMD XOP instruction set. Use VPPERM
    Vec16c t0 = _mm_perm_epi8(table.get_low(), table.get_high(), index.get_low());
    Vec16c t1 = _mm_perm_epi8(table.get_low(), table.get_high(), index.get_high());
    return Vec32c(t0, t1);
#else
    Vec32c f0 = constant8i<0,0,0,0,0x10101010,0x10101010,0x10101010,0x10101010>();
    Vec32c f1 = constant8i<0x10101010,0x10101010,0x10101010,0x10101010,0,0,0,0>();
    Vec32c tablef = _mm256_permute4x64_epi64(table, 0x4E);   // low and high parts swapped
    Vec32c r0 = _mm256_shuffle_epi8(table,  (index ^ f0) + 0x70);
    Vec32c r1 = _mm256_shuffle_epi8(tablef, (index ^ f1) + 0x70);
    return r0 | r1;
#endif
}

template <int n>
static inline Vec32c lookup(Vec32uc const & index, void const * table) {
    if (n <=  0) return 0;
    if (n <= 16) {
        Vec16c tt = Vec16c().load(table);
        Vec16c r0 = lookup16(index.get_low(),  tt);
        Vec16c r1 = lookup16(index.get_high(), tt);
        return Vec32c(r0, r1);
    }
    if (n <= 32) return lookup32(index, Vec32c().load(table));
    // n > 32. Limit index
    Vec32uc index1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec32uc(index) & uint8_t(n-1);
    }
    else {
        // n is not a power of 2, limit to n-1
        index1 = min(Vec32uc(index), uint8_t(n-1));
    }
    Vec8ui mask0 = Vec8ui(0x000000FF);  // mask 8 bits
    Vec32c t0 = _mm256_i32gather_epi32((const int *)table, __m256i(mask0 & Vec8ui(index1)),      1); // positions 0, 4, 8,  ...
    Vec32c t1 = _mm256_i32gather_epi32((const int *)table, __m256i(mask0 & _mm256_srli_epi32(index1, 8)), 1); // positions 1, 5, 9,  ...
    Vec32c t2 = _mm256_i32gather_epi32((const int *)table, __m256i(mask0 & _mm256_srli_epi32(index1,16)), 1); // positions 2, 6, 10, ...
    Vec32c t3 = _mm256_i32gather_epi32((const int *)table,         _mm256_srli_epi32(index1,24), 1); // positions 3, 7, 11, ...
    t0 = t0 & mask0;
    t1 = _mm256_slli_epi32(t1 & mask0,  8);
    t2 = _mm256_slli_epi32(t2 & mask0, 16);
    t3 = _mm256_slli_epi32(t3,         24);
    return (t0 | t3) | (t1 | t2);
}

template <int n>
static inline Vec32c lookup(Vec32c const & index, void const * table) {
    return lookup<n>(Vec32uc(index), table);
}


static inline Vec16s lookup16(Vec16s const & index, Vec16s const & table) {
    return Vec16s(lookup32(Vec32c(index * 0x202 + 0x100), Vec32c(table)));
}

template <int n>
static inline Vec16s lookup(Vec16s const & index, void const * table) {
    if (n <=  0) return 0;
    if (n <=  8) {
        Vec8s table1 = Vec8s().load(table);        
        return Vec16s(       
            lookup8 (index.get_low(),  table1),
            lookup8 (index.get_high(), table1));
    }
    if (n <= 16) return lookup16(index, Vec16s().load(table));
    // n > 16. Limit index
    Vec16us index1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec16us(index) & (n-1);
    }
    else {
        // n is not a power of 2, limit to n-1
        index1 = min(Vec16us(index), n-1);
    }
    Vec16s t1 = _mm256_i32gather_epi32((const int *)table, __m256i(Vec8ui(index1) & 0x0000FFFF), 2);  // even positions
    Vec16s t2 = _mm256_i32gather_epi32((const int *)table, _mm256_srli_epi32(index1, 16) , 2);        // odd  positions
    return blend16s<0,16,2,18,4,20,6,22,8,24,10,26,12,28,14,30>(t1, t2);
}

static inline Vec8i lookup8(Vec8i const & index, Vec8i const & table) {
    return _mm256_permutevar8x32_epi32(table, index);
}

template <int n>
static inline Vec8i lookup(Vec8i const & index, void const * table) {
    if (n <= 0) return 0;
    if (n <= 8) {
        Vec8i table1 = Vec8i().load(table);
        return lookup8(index, table1);
    }
    if (n <= 16) {
        Vec8i table1 = Vec8i().load(table);
        Vec8i table2 = Vec8i().load((int32_t*)table + 8);
        Vec8i y1 = lookup8(index, table1);
        Vec8i y2 = lookup8(index, table2);
        Vec8ib s = index > 7;
        return select(s, y2, y1);
    }
    // n > 16. Limit index
    Vec8ui index1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec8ui(index) & (n-1);
    }
    else {
        // n is not a power of 2, limit to n-1
        index1 = min(Vec8ui(index), n-1);
    }
    return _mm256_i32gather_epi32((const int *)table, index1, 4);
}

static inline Vec4q lookup4(Vec4q const & index, Vec4q const & table) {
    return Vec4q(lookup8(Vec8i(index * 0x200000002ll + 0x100000000ll), Vec8i(table)));
}

template <int n>
static inline Vec4q lookup(Vec4q const & index, int64_t const * table) {
    if (n <= 0) return 0;
    // n > 0. Limit index
    Vec4uq index1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec4uq(index) & (n-1);
    }
    else {
        // n is not a power of 2, limit to n-1.
        // There is no 64-bit min instruction, but we can use the 32-bit unsigned min,
        // since n is a 32-bit integer
        index1 = Vec4uq(min(Vec8ui(index), constant8i<n-1, 0, n-1, 0, n-1, 0, n-1, 0>()));
    }
// old compilers can't agree how to define a 64 bit integer. Intel and MS use __int64, gcc use long long
#if defined (__clang__) && CLANG_VERSION < 30400
// clang 3.3 uses const int * in accordance with official Intel doc., which is wrong. will be fixed
    return _mm256_i64gather_epi64((const int *)table, index1, 8);
#elif defined (_MSC_VER) && _MSC_VER < 1700 && ! defined(__INTEL_COMPILER)
// Old MS and Intel use non-standard type __int64
    return _mm256_i64gather_epi64((const int64_t *)table, index1, 8);
#else
// Gnu, Clang 3.4, MS 11.0
    return _mm256_i64gather_epi64((const long long *)table, index1, 8);
#endif
}


/*****************************************************************************
*
*          Other permutations with variable indexes
*
*****************************************************************************/

// Function shift_bytes_up: shift whole vector left by b bytes.
// You may use a permute function instead if b is a compile-time constant
static inline Vec32c shift_bytes_up(Vec32c const & a, int b) {
    if (b < 16) {    
        return Vec32c(shift_bytes_up(a.get_low(),b), shift_bytes_up(a.get_high(),b) | shift_bytes_down(a.get_low(),16-b));
    }
    else {
        return Vec32c(Vec16c(0), shift_bytes_up(a.get_high(),b-16));
    }
}

// Function shift_bytes_down: shift whole vector right by b bytes
// You may use a permute function instead if b is a compile-time constant
static inline Vec32c shift_bytes_down(Vec32c const & a, int b) {
    if (b < 16) {    
        return Vec32c(shift_bytes_down(a.get_low(),b) | shift_bytes_up(a.get_high(),16-b), shift_bytes_down(a.get_high(),b));
    }
    else {
        return Vec32c(shift_bytes_down(a.get_high(),b-16), Vec16c(0));
    }
}

/*****************************************************************************
*
*          Gather functions with fixed indexes
*
*****************************************************************************/
// Load elements from array a with indices i0, i1, i2, i3, i4, i5, i6, i7
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8i gather8i(void const * a) {
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
            Vec8i b = Vec8i().load((int32_t const *)a + imax-7);
            return permute8i<i0-imax+7, i1-imax+7, i2-imax+7, i3-imax+7, i4-imax+7, i5-imax+7, i6-imax+7, i7-imax+7>(b);
        }
        else {
            Vec8i b = Vec8i().load((int32_t const *)a + imin);
            return permute8i<i0-imin, i1-imin, i2-imin, i3-imin, i4-imin, i5-imin, i6-imin, i7-imin>(b);
        }
    }
    if ((i0<imin+8 || i0>imax-8) && (i1<imin+8 || i1>imax-8) && (i2<imin+8 || i2>imax-8) && (i3<imin+8 || i3>imax-8)
    &&  (i4<imin+8 || i4>imax-8) && (i5<imin+8 || i5>imax-8) && (i6<imin+8 || i6>imax-8) && (i7<imin+8 || i7>imax-8)) {
        // load two contiguous blocks and blend
        Vec8i b = Vec8i().load((int32_t const *)a + imin);
        Vec8i c = Vec8i().load((int32_t const *)a + imax-7);
        const int j0 = i0<imin+8 ? i0-imin : 15-imax+i0;
        const int j1 = i1<imin+8 ? i1-imin : 15-imax+i1;
        const int j2 = i2<imin+8 ? i2-imin : 15-imax+i2;
        const int j3 = i3<imin+8 ? i3-imin : 15-imax+i3;
        const int j4 = i4<imin+8 ? i4-imin : 15-imax+i4;
        const int j5 = i5<imin+8 ? i5-imin : 15-imax+i5;
        const int j6 = i6<imin+8 ? i6-imin : 15-imax+i6;
        const int j7 = i7<imin+8 ? i7-imin : 15-imax+i7;
        return blend8i<j0, j1, j2, j3, j4, j5, j6, j7>(b, c);
    }
    // use AVX2 gather
    return _mm256_i32gather_epi32((const int *)a, Vec8i(i0,i1,i2,i3,i4,i5,i6,i7), 4);
}

template <int i0, int i1, int i2, int i3>
static inline Vec4q gather4q(void const * a) {
    Static_error_check<(i0|i1|i2|i3)>=0> Negative_array_index;  // Error message if index is negative
    const int i01min = i0 < i1 ? i0 : i1;
    const int i23min = i2 < i3 ? i2 : i3;
    const int imin   = i01min < i23min ? i01min : i23min;
    const int i01max = i0 > i1 ? i0 : i1;
    const int i23max = i2 > i3 ? i2 : i3;
    const int imax   = i01max > i23max ? i01max : i23max;
    if (imax - imin <= 3) {
        // load one contiguous block and permute
        if (imax > 3) {
            // make sure we don't read past the end of the array
            Vec4q b = Vec4q().load((int64_t const *)a + imax-3);
            return permute4q<i0-imax+3, i1-imax+3, i2-imax+3, i3-imax+3>(b);
        }
        else {
            Vec4q b = Vec4q().load((int64_t const *)a + imin);
            return permute4q<i0-imin, i1-imin, i2-imin, i3-imin>(b);
        }
    }
    if ((i0<imin+4 || i0>imax-4) && (i1<imin+4 || i1>imax-4) && (i2<imin+4 || i2>imax-4) && (i3<imin+4 || i3>imax-4)) {
        // load two contiguous blocks and blend
        Vec4q b = Vec4q().load((int64_t const *)a + imin);
        Vec4q c = Vec4q().load((int64_t const *)a + imax-3);
        const int j0 = i0<imin+4 ? i0-imin : 7-imax+i0;
        const int j1 = i1<imin+4 ? i1-imin : 7-imax+i1;
        const int j2 = i2<imin+4 ? i2-imin : 7-imax+i2;
        const int j3 = i3<imin+4 ? i3-imin : 7-imax+i3;
        return blend4q<j0, j1, j2, j3>(b, c);
    }
    // use AVX2 gather
    // old compilers can't agree how to define a 64 bit integer. Intel and MS use __int64, gcc use long long
#if defined (__clang__) && CLANG_VERSION < 30400
    // clang 3.3 uses const int * in accordance with official Intel doc., which is wrong. will be fixed
    return _mm256_i32gather_epi64((const int *)a, Vec4i(i0,i1,i2,i3), 8);
#elif defined (_MSC_VER) && _MSC_VER < 1700 && ! defined(__INTEL_COMPILER)
    // Old MS and Intel use non-standard type __int64
    return _mm256_i32gather_epi64((const int64_t *)a, Vec4i(i0,i1,i2,i3), 8);
#else
    // Gnu, Clang 3.4, MS 11.0
    return _mm256_i32gather_epi64((const long long *)a, Vec4i(i0,i1,i2,i3), 8);
#endif
}


/*****************************************************************************
*
*          Functions for conversion between integer sizes
*
*****************************************************************************/

// Extend 8-bit integers to 16-bit integers, signed and unsigned

// Function extend_low : extends the low 16 elements to 16 bits with sign extension
static inline Vec16s extend_low (Vec32c const & a) {
    __m256i a2   = permute4q<0,-256,1,-256>(Vec4q(a));           // get bits 64-127 to position 128-191
    __m256i sign = _mm256_cmpgt_epi8(_mm256_setzero_si256(),a2); // 0 > a2
    return         _mm256_unpacklo_epi8(a2, sign);               // interleave with sign extensions
}

// Function extend_high : extends the high 16 elements to 16 bits with sign extension
static inline Vec16s extend_high (Vec32c const & a) {
    __m256i a2   = permute4q<-256,2,-256,3>(Vec4q(a));           // get bits 128-191 to position 64-127
    __m256i sign = _mm256_cmpgt_epi8(_mm256_setzero_si256(),a2); // 0 > a2
    return         _mm256_unpackhi_epi8(a2, sign);               // interleave with sign extensions
}

// Function extend_low : extends the low 16 elements to 16 bits with zero extension
static inline Vec16us extend_low (Vec32uc const & a) {
    __m256i a2 = permute4q<0,-256,1,-256>(Vec4q(a));             // get bits 64-127 to position 128-191
    return    _mm256_unpacklo_epi8(a2, _mm256_setzero_si256());  // interleave with zero extensions
}

// Function extend_high : extends the high 19 elements to 16 bits with zero extension
static inline Vec16us extend_high (Vec32uc const & a) {
    __m256i a2 = permute4q<-256,2,-256,3>(Vec4q(a));             // get bits 128-191 to position 64-127
    return  _mm256_unpackhi_epi8(a2, _mm256_setzero_si256());    // interleave with zero extensions
}

// Extend 16-bit integers to 32-bit integers, signed and unsigned

// Function extend_low : extends the low 8 elements to 32 bits with sign extension
static inline Vec8i extend_low (Vec16s const & a) {
    __m256i a2   = permute4q<0,-256,1,-256>(Vec4q(a));           // get bits 64-127 to position 128-191
    __m256i sign = _mm256_srai_epi16(a2, 15);                    // sign bit
    return         _mm256_unpacklo_epi16(a2 ,sign);              // interleave with sign extensions
}

// Function extend_high : extends the high 8 elements to 32 bits with sign extension
static inline Vec8i extend_high (Vec16s const & a) {
    __m256i a2 = permute4q<-256,2,-256,3>(Vec4q(a));             // get bits 128-191 to position 64-127
    __m256i sign = _mm256_srai_epi16(a2, 15);                    // sign bit
    return         _mm256_unpackhi_epi16(a2, sign);              // interleave with sign extensions
}

// Function extend_low : extends the low 8 elements to 32 bits with zero extension
static inline Vec8ui extend_low (Vec16us const & a) {
    __m256i a2 = permute4q<0,-256,1,-256>(Vec4q(a));             // get bits 64-127 to position 128-191
    return    _mm256_unpacklo_epi16(a2, _mm256_setzero_si256()); // interleave with zero extensions
}

// Function extend_high : extends the high 8 elements to 32 bits with zero extension
static inline Vec8ui extend_high (Vec16us const & a) {
    __m256i a2 = permute4q<-256,2,-256,3>(Vec4q(a));             // get bits 128-191 to position 64-127
    return  _mm256_unpackhi_epi16(a2, _mm256_setzero_si256());   // interleave with zero extensions
}

// Extend 32-bit integers to 64-bit integers, signed and unsigned

// Function extend_low : extends the low 4 elements to 64 bits with sign extension
static inline Vec4q extend_low (Vec8i const & a) {
    __m256i a2 = permute4q<0,-256,1,-256>(Vec4q(a));             // get bits 64-127 to position 128-191
    __m256i sign = _mm256_srai_epi32(a2, 31);                    // sign bit
    return         _mm256_unpacklo_epi32(a2, sign);              // interleave with sign extensions
}

// Function extend_high : extends the high 4 elements to 64 bits with sign extension
static inline Vec4q extend_high (Vec8i const & a) {
    __m256i a2 = permute4q<-256,2,-256,3>(Vec4q(a));             // get bits 128-191 to position 64-127
    __m256i sign = _mm256_srai_epi32(a2, 31);                    // sign bit
    return         _mm256_unpackhi_epi32(a2, sign);              // interleave with sign extensions
}

// Function extend_low : extends the low 4 elements to 64 bits with zero extension
static inline Vec4uq extend_low (Vec8ui const & a) {
    __m256i a2 = permute4q<0,-256,1,-256>(Vec4q(a));             // get bits 64-127 to position 128-191
    return  _mm256_unpacklo_epi32(a2, _mm256_setzero_si256());   // interleave with zero extensions
}

// Function extend_high : extends the high 4 elements to 64 bits with zero extension
static inline Vec4uq extend_high (Vec8ui const & a) {
    __m256i a2 = permute4q<-256,2,-256,3>(Vec4q(a));             // get bits 128-191 to position 64-127
    return  _mm256_unpackhi_epi32(a2, _mm256_setzero_si256());   // interleave with zero extensions
}

// Compress 16-bit integers to 8-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Overflow wraps around
static inline Vec32c compress (Vec16s const & low, Vec16s const & high) {
    __m256i mask  = _mm256_set1_epi32(0x00FF00FF);            // mask for low bytes
    __m256i lowm  = _mm256_and_si256(low, mask);              // bytes of low
    __m256i highm = _mm256_and_si256(high, mask);             // bytes of high
    __m256i pk    = _mm256_packus_epi16(lowm, highm);         // unsigned pack
    return          _mm256_permute4x64_epi64(pk, 0xD8);       // put in right place
}

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Signed, with saturation
static inline Vec32c compress_saturated (Vec16s const & low, Vec16s const & high) {
    __m256i pk    = _mm256_packs_epi16(low,high);             // packed with signed saturation
    return          _mm256_permute4x64_epi64(pk, 0xD8);       // put in right place
}

// Function compress : packs two vectors of 16-bit integers to one vector of 8-bit integers
// Unsigned, overflow wraps around
static inline Vec32uc compress (Vec16us const & low, Vec16us const & high) {
    return  Vec32uc (compress((Vec16s)low, (Vec16s)high));
}

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Unsigned, with saturation
static inline Vec32uc compress_saturated (Vec16us const & low, Vec16us const & high) {
    __m256i maxval  = _mm256_set1_epi32(0x00FF00FF);          // maximum value
    __m256i minval  = _mm256_setzero_si256();                 // minimum value = 0
    __m256i low1    = _mm256_min_epu16(low,maxval);           // upper limit
    __m256i high1   = _mm256_min_epu16(high,maxval);          // upper limit
    __m256i low2    = _mm256_max_epu16(low1,minval);          // lower limit
    __m256i high2   = _mm256_max_epu16(high1,minval);         // lower limit
    __m256i pk      = _mm256_packus_epi16(low2,high2);        // this instruction saturates from signed 32 bit to unsigned 16 bit
    return            _mm256_permute4x64_epi64(pk, 0xD8);     // put in right place
}

// Compress 32-bit integers to 16-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Overflow wraps around
static inline Vec16s compress (Vec8i const & low, Vec8i const & high) {
    __m256i mask  = _mm256_set1_epi32(0x0000FFFF);            // mask for low words
    __m256i lowm  = _mm256_and_si256(low,mask);               // bytes of low
    __m256i highm = _mm256_and_si256(high,mask);              // bytes of high
    __m256i pk    = _mm256_packus_epi32(lowm,highm);          // unsigned pack
    return          _mm256_permute4x64_epi64(pk, 0xD8);       // put in right place
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Signed with saturation
static inline Vec16s compress_saturated (Vec8i const & low, Vec8i const & high) {
    __m256i pk    =  _mm256_packs_epi32(low,high);            // pack with signed saturation
    return           _mm256_permute4x64_epi64(pk, 0xD8);      // put in right place
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Overflow wraps around
static inline Vec16us compress (Vec8ui const & low, Vec8ui const & high) {
    return Vec16us (compress((Vec8i)low, (Vec8i)high));
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Unsigned, with saturation
static inline Vec16us compress_saturated (Vec8ui const & low, Vec8ui const & high) {
    __m256i maxval  = _mm256_set1_epi32(0x0000FFFF);          // maximum value
    __m256i minval  = _mm256_setzero_si256();                 // minimum value = 0
    __m256i low1    = _mm256_min_epu32(low,maxval);           // upper limit
    __m256i high1   = _mm256_min_epu32(high,maxval);          // upper limit
    __m256i low2    = _mm256_max_epu32(low1,minval);          // lower limit
    __m256i high2   = _mm256_max_epu32(high1,minval);         // lower limit
    __m256i pk      = _mm256_packus_epi32(low2,high2);        // this instruction saturates from signed 32 bit to unsigned 16 bit
    return            _mm256_permute4x64_epi64(pk, 0xD8);     // put in right place
}

// Compress 64-bit integers to 32-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Overflow wraps around
static inline Vec8i compress (Vec4q const & low, Vec4q const & high) {
    __m256i low2  = _mm256_shuffle_epi32(low,0xD8);           // low dwords of low  to pos. 0 and 32
    __m256i high2 = _mm256_shuffle_epi32(high,0xD8);          // low dwords of high to pos. 0 and 32
    __m256i pk    = _mm256_unpacklo_epi64(low2,high2);        // interleave
    return          _mm256_permute4x64_epi64(pk, 0xD8);       // put in right place
}

// Function compress : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Signed, with saturation
static inline Vec8i compress_saturated (Vec4q const & a, Vec4q const & b) {
    Vec4q maxval = constant8i<0x7FFFFFFF,0,0x7FFFFFFF,0,0x7FFFFFFF,0,0x7FFFFFFF,0>();
    Vec4q minval = constant8i<(int)0x80000000,-1,(int)0x80000000,-1,(int)0x80000000,-1,(int)0x80000000,-1>();
    Vec4q a1  = min(a,maxval);
    Vec4q b1  = min(b,maxval);
    Vec4q a2  = max(a1,minval);
    Vec4q b2  = max(b1,minval);
    return compress(a2,b2);
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Overflow wraps around
static inline Vec8ui compress (Vec4uq const & low, Vec4uq const & high) {
    return Vec8ui (compress((Vec4q)low, (Vec4q)high));
}

// Function compress : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Unsigned, with saturation
static inline Vec8ui compress_saturated (Vec4uq const & low, Vec4uq const & high) {
    __m256i zero     = _mm256_setzero_si256();                // 0
    __m256i lowzero  = _mm256_cmpeq_epi32(low,zero);          // for each dword is zero
    __m256i highzero = _mm256_cmpeq_epi32(high,zero);         // for each dword is zero
    __m256i mone     = _mm256_set1_epi32(-1);                 // FFFFFFFF
    __m256i lownz    = _mm256_xor_si256(lowzero,mone);        // for each dword is nonzero
    __m256i highnz   = _mm256_xor_si256(highzero,mone);       // for each dword is nonzero
    __m256i lownz2   = _mm256_srli_epi64(lownz,32);           // shift down to low dword
    __m256i highnz2  = _mm256_srli_epi64(highnz,32);          // shift down to low dword
    __m256i lowsatur = _mm256_or_si256(low,lownz2);           // low, saturated
    __m256i hisatur  = _mm256_or_si256(high,highnz2);         // high, saturated
    return  Vec8ui (compress(Vec4q(lowsatur), Vec4q(hisatur)));
}


/*****************************************************************************
*
*          Integer division operators
*
*          Please see the file vectori128.h for explanation.
*
*****************************************************************************/

// vector operator / : divide each element by divisor

// vector of 8 32-bit signed integers
static inline Vec8i operator / (Vec8i const & a, Divisor_i const & d) {
    __m256i m   = _mm256_broadcastq_epi64(d.getm());       // broadcast multiplier
    __m256i sgn = _mm256_broadcastq_epi64(d.getsign());    // broadcast sign of d
    __m256i t1  = _mm256_mul_epi32(a,m);                   // 32x32->64 bit signed multiplication of even elements of a
    __m256i t2  = _mm256_srli_epi64(t1,32);                // high dword of even numbered results
    __m256i t3  = _mm256_srli_epi64(a,32);                 // get odd elements of a into position for multiplication
    __m256i t4  = _mm256_mul_epi32(t3,m);                  // 32x32->64 bit signed multiplication of odd elements
    __m256i t5  = constant8i<0,-1,0,-1,0,-1,0,-1> ();      // mask for odd elements
    __m256i t7  = _mm256_blendv_epi8(t2,t4,t5);            // blend two results
    __m256i t8  = _mm256_add_epi32(t7,a);                  // add
    __m256i t9  = _mm256_sra_epi32(t8,d.gets1());          // shift right artihmetic
    __m256i t10 = _mm256_srai_epi32(a,31);                 // sign of a
    __m256i t11 = _mm256_sub_epi32(t10,sgn);               // sign of a - sign of d
    __m256i t12 = _mm256_sub_epi32(t9,t11);                // + 1 if a < 0, -1 if d < 0
    return        _mm256_xor_si256(t12,sgn);               // change sign if divisor negative
}

// vector of 8 32-bit unsigned integers
static inline Vec8ui operator / (Vec8ui const & a, Divisor_ui const & d) {
    __m256i m   = _mm256_broadcastq_epi64(d.getm());       // broadcast multiplier
    __m256i t1  = _mm256_mul_epu32(a,m);                   // 32x32->64 bit unsigned multiplication of even elements of a
    __m256i t2  = _mm256_srli_epi64(t1,32);                // high dword of even numbered results
    __m256i t3  = _mm256_srli_epi64(a,32);                 // get odd elements of a into position for multiplication
    __m256i t4  = _mm256_mul_epu32(t3,m);                  // 32x32->64 bit unsigned multiplication of odd elements
    __m256i t5  = constant8i<0,-1,0,-1,0,-1,0,-1> ();      // mask for odd elements
    __m256i t7  = _mm256_blendv_epi8(t2,t4,t5);            // blend two results
    __m256i t8  = _mm256_sub_epi32(a,t7);                  // subtract
    __m256i t9  = _mm256_srl_epi32(t8,d.gets1());          // shift right logical
    __m256i t10 = _mm256_add_epi32(t7,t9);                 // add
    return        _mm256_srl_epi32(t10,d.gets2());         // shift right logical 
}

// vector of 16 16-bit signed integers
static inline Vec16s operator / (Vec16s const & a, Divisor_s const & d) {
    __m256i m   = _mm256_broadcastq_epi64(d.getm());       // broadcast multiplier
    __m256i sgn = _mm256_broadcastq_epi64(d.getsign());    // broadcast sign of d
    __m256i t1  = _mm256_mulhi_epi16(a, m);                // multiply high signed words
    __m256i t2  = _mm256_add_epi16(t1,a);                  // + a
    __m256i t3  = _mm256_sra_epi16(t2,d.gets1());          // shift right artihmetic
    __m256i t4  = _mm256_srai_epi16(a,15);                 // sign of a
    __m256i t5  = _mm256_sub_epi16(t4,sgn);                // sign of a - sign of d
    __m256i t6  = _mm256_sub_epi16(t3,t5);                 // + 1 if a < 0, -1 if d < 0
    return        _mm256_xor_si256(t6,sgn);                // change sign if divisor negative
}

// vector of 16 16-bit unsigned integers
static inline Vec16us operator / (Vec16us const & a, Divisor_us const & d) {
    __m256i m   = _mm256_broadcastq_epi64(d.getm());       // broadcast multiplier
    __m256i t1  = _mm256_mulhi_epu16(a, m);                // multiply high signed words
    __m256i t2  = _mm256_sub_epi16(a,t1);                  // subtract
    __m256i t3  = _mm256_srl_epi16(t2,d.gets1());          // shift right logical
    __m256i t4  = _mm256_add_epi16(t1,t3);                 // add
    return        _mm256_srl_epi16(t4,d.gets2());          // shift right logical 
}

// vector of 32 8-bit signed integers
static inline Vec32c operator / (Vec32c const & a, Divisor_s const & d) {
    // expand into two Vec16s
    Vec16s low  = extend_low(a) / d;
    Vec16s high = extend_high(a) / d;
    return compress(low,high);
}

// vector of 32 8-bit unsigned integers
static inline Vec32uc operator / (Vec32uc const & a, Divisor_us const & d) {
    // expand into two Vec16s
    Vec16us low  = extend_low(a) / d;
    Vec16us high = extend_high(a) / d;
    return compress(low,high);
}

// vector operator /= : divide
static inline Vec8i & operator /= (Vec8i & a, Divisor_i const & d) {
    a = a / d;
    return a;
}

// vector operator /= : divide
static inline Vec8ui & operator /= (Vec8ui & a, Divisor_ui const & d) {
    a = a / d;
    return a;
}

// vector operator /= : divide
static inline Vec16s & operator /= (Vec16s & a, Divisor_s const & d) {
    a = a / d;
    return a;
}


// vector operator /= : divide
static inline Vec16us & operator /= (Vec16us & a, Divisor_us const & d) {
    a = a / d;
    return a;

}

// vector operator /= : divide
static inline Vec32c & operator /= (Vec32c & a, Divisor_s const & d) {
    a = a / d;
    return a;
}

// vector operator /= : divide
static inline Vec32uc & operator /= (Vec32uc & a, Divisor_us const & d) {
    a = a / d;
    return a;
}


/*****************************************************************************
*
*          Integer division 2: divisor is a compile-time constant
*
*****************************************************************************/

// Divide Vec8i by compile-time constant
template <int32_t d>
static inline Vec8i divide_by_i(Vec8i const & x) {
    Static_error_check<(d!=0)> Dividing_by_zero;                     // Error message if dividing by zero
    if (d ==  1) return  x;
    if (d == -1) return -x;
    if (uint32_t(d) == 0x80000000u) return Vec8i(x == Vec8i(0x80000000)) & 1; // prevent overflow when changing sign
    const uint32_t d1 = d > 0 ? uint32_t(d) : -uint32_t(d);          // compile-time abs(d). (force GCC compiler to treat d as 32 bits, not 64 bits)
    if ((d1 & (d1-1)) == 0) {
        // d1 is a power of 2. use shift
        const int k = bit_scan_reverse_const(d1);
        __m256i sign;
        if (k > 1) sign = _mm256_srai_epi32(x, k-1); else sign = x;  // k copies of sign bit
        __m256i bias    = _mm256_srli_epi32(sign, 32-k);             // bias = x >= 0 ? 0 : k-1
        __m256i xpbias  = _mm256_add_epi32 (x, bias);                // x + bias
        __m256i q       = _mm256_srai_epi32(xpbias, k);              // (x + bias) >> k
        if (d > 0)      return q;                                    // d > 0: return  q
        return _mm256_sub_epi32(_mm256_setzero_si256(), q);          // d < 0: return -q
    }
    // general case
    const int32_t sh = bit_scan_reverse_const(uint32_t(d1)-1);       // ceil(log2(d1)) - 1. (d1 < 2 handled by power of 2 case)
    const int32_t mult = int(1 + (uint64_t(1) << (32+sh)) / uint32_t(d1) - (int64_t(1) << 32));   // multiplier
    const Divisor_i div(mult, sh, d < 0 ? -1 : 0);
    return x / div;
}

// define Vec8i a / const_int(d)
template <int32_t d>
static inline Vec8i operator / (Vec8i const & a, Const_int_t<d>) {
    return divide_by_i<d>(a);
}

// define Vec8i a / const_uint(d)
template <uint32_t d>
static inline Vec8i operator / (Vec8i const & a, Const_uint_t<d>) {
    Static_error_check< (d<0x80000000u) > Error_overflow_dividing_signed_by_unsigned; // Error: dividing signed by overflowing unsigned
    return divide_by_i<int32_t(d)>(a);                               // signed divide
}

// vector operator /= : divide
template <int32_t d>
static inline Vec8i & operator /= (Vec8i & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec8i & operator /= (Vec8i & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}


// Divide Vec8ui by compile-time constant
template <uint32_t d>
static inline Vec8ui divide_by_ui(Vec8ui const & x) {
    Static_error_check<(d!=0)> Dividing_by_zero;                     // Error message if dividing by zero
    if (d == 1) return x;                                            // divide by 1
    const int b = bit_scan_reverse_const(d);                         // floor(log2(d))
    if ((uint32_t(d) & (uint32_t(d)-1)) == 0) {
        // d is a power of 2. use shift
        return  _mm256_srli_epi32(x, b);                             // x >> b
    }
    // general case (d > 2)
    uint32_t mult = uint32_t((uint64_t(1) << (b+32)) / d);           // multiplier = 2^(32+b) / d
    const uint64_t rem = (uint64_t(1) << (b+32)) - uint64_t(d)*mult; // remainder 2^(32+b) % d
    const bool round_down = (2*rem < d);                             // check if fraction is less than 0.5
    if (!round_down) {
        mult = mult + 1;                                             // round up mult
    }
    // do 32*32->64 bit unsigned multiplication and get high part of result
    const __m256i multv = _mm256_set_epi32(0,mult,0,mult,0,mult,0,mult);// zero-extend mult and broadcast
    __m256i t1 = _mm256_mul_epu32(x,multv);                          // 32x32->64 bit unsigned multiplication of x[0] and x[2]
    if (round_down) {
        t1      = _mm256_add_epi64(t1,multv);                        // compensate for rounding error. (x+1)*m replaced by x*m+m to avoid overflow
    }
    __m256i t2 = _mm256_srli_epi64(t1,32);                           // high dword of result 0 and 2
    __m256i t3 = _mm256_srli_epi64(x,32);                            // get x[1] and x[3] into position for multiplication
    __m256i t4 = _mm256_mul_epu32(t3,multv);                         // 32x32->64 bit unsigned multiplication of x[1] and x[3]
    if (round_down) {
        t4      = _mm256_add_epi64(t4,multv);                        // compensate for rounding error. (x+1)*m replaced by x*m+m to avoid overflow
    }
    __m256i t5 = _mm256_set_epi32(-1,0,-1,0,-1,0,-1,0);              // mask of dword 1 and 3
    __m256i t7 = _mm256_blendv_epi8(t2,t4,t5);                       // blend two results
    Vec8ui  q  = _mm256_srli_epi32(t7, b);                           // shift right by b
    return q;                                                        // no overflow possible
}

// define Vec8ui a / const_uint(d)
template <uint32_t d>
static inline Vec8ui operator / (Vec8ui const & a, Const_uint_t<d>) {
    return divide_by_ui<d>(a);
}

// define Vec8ui a / const_int(d)
template <int32_t d>
static inline Vec8ui operator / (Vec8ui const & a, Const_int_t<d>) {
    Static_error_check< (d>=0) > Error_dividing_unsigned_by_negative;// Error: dividing unsigned by negative is ambiguous
    return divide_by_ui<d>(a);                                       // unsigned divide
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec8ui & operator /= (Vec8ui & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <int32_t d>
static inline Vec8ui & operator /= (Vec8ui & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}


// Divide Vec16s by compile-time constant 
template <int d>
static inline Vec16s divide_by_i(Vec16s const & x) {
    const int16_t d0 = int16_t(d);                                   // truncate d to 16 bits
    Static_error_check<(d0 != 0)> Dividing_by_zero;                  // Error message if dividing by zero
    if (d0 ==  1) return  x;                                         // divide by  1
    if (d0 == -1) return -x;                                         // divide by -1
    if (uint16_t(d0) == 0x8000u) return Vec16s(x == Vec16s(0x8000)) & 1;// prevent overflow when changing sign
    const uint16_t d1 = d0 > 0 ? d0 : -d0;                           // compile-time abs(d0)
    if ((d1 & (d1-1)) == 0) {
        // d is a power of 2. use shift
        const int k = bit_scan_reverse_const(uint32_t(d1));
        __m256i sign;
        if (k > 1) sign = _mm256_srai_epi16(x, k-1); else sign = x;  // k copies of sign bit
        __m256i bias    = _mm256_srli_epi16(sign, 16-k);             // bias = x >= 0 ? 0 : k-1
        __m256i xpbias  = _mm256_add_epi16 (x, bias);                // x + bias
        __m256i q       = _mm256_srai_epi16(xpbias, k);              // (x + bias) >> k
        if (d0 > 0)  return q;                                       // d0 > 0: return  q
        return _mm256_sub_epi16(_mm256_setzero_si256(), q);          // d0 < 0: return -q
    }
    // general case
    const int L = bit_scan_reverse_const(uint16_t(d1-1)) + 1;        // ceil(log2(d)). (d < 2 handled above)
    const int16_t mult = int16_t(1 + (1u << (15+L)) / uint32_t(d1) - 0x10000);// multiplier
    const int shift1 = L - 1;
    const Divisor_s div(mult, shift1, d0 > 0 ? 0 : -1);
    return x / div;
}

// define Vec16s a / const_int(d)
template <int d>
static inline Vec16s operator / (Vec16s const & a, Const_int_t<d>) {
    return divide_by_i<d>(a);
}

// define Vec16s a / const_uint(d)
template <uint32_t d>
static inline Vec16s operator / (Vec16s const & a, Const_uint_t<d>) {
    Static_error_check< (d<0x8000u) > Error_overflow_dividing_signed_by_unsigned; // Error: dividing signed by overflowing unsigned
    return divide_by_i<int(d)>(a);                                   // signed divide
}

// vector operator /= : divide
template <int32_t d>
static inline Vec16s & operator /= (Vec16s & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec16s & operator /= (Vec16s & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}


// Divide Vec16us by compile-time constant
template <uint32_t d>
static inline Vec16us divide_by_ui(Vec16us const & x) {
    const uint16_t d0 = uint16_t(d);                                 // truncate d to 16 bits
    Static_error_check<(d0 != 0)> Dividing_by_zero;                  // Error message if dividing by zero
    if (d0 == 1) return x;                                           // divide by 1
    const int b = bit_scan_reverse_const(d0);                        // floor(log2(d))
    if ((d0 & (d0-1)) == 0) {
        // d is a power of 2. use shift
        return  _mm256_srli_epi16(x, b);                             // x >> b
    }
    // general case (d > 2)
    uint16_t mult = uint16_t((uint32_t(1) << (b+16)) / d0);          // multiplier = 2^(32+b) / d
    const uint32_t rem = (uint32_t(1) << (b+16)) - uint32_t(d0)*mult;// remainder 2^(32+b) % d
    const bool round_down = (2*rem < d0);                            // check if fraction is less than 0.5
    Vec16us x1 = x;
    if (round_down) {
        x1 = x1 + 1;                                                 // round down mult and compensate by adding 1 to x
    }
    else {
        mult = mult + 1;                                             // round up mult. no compensation needed
    }
    const __m256i multv = _mm256_set1_epi16(mult);                   // broadcast mult
    __m256i xm = _mm256_mulhi_epu16(x1, multv);                      // high part of 16x16->32 bit unsigned multiplication
    Vec16us q    = _mm256_srli_epi16(xm, b);                         // shift right by b
    if (round_down) {
        Vec16sb overfl = (x1 == Vec16us(_mm256_setzero_si256()));     // check for overflow of x+1
        return select(overfl, Vec16us(mult >> b), q);                // deal with overflow (rarely needed)
    }
    else {
        return q;                                                    // no overflow possible
    }
}

// define Vec16us a / const_uint(d)
template <uint32_t d>
static inline Vec16us operator / (Vec16us const & a, Const_uint_t<d>) {
    return divide_by_ui<d>(a);
}

// define Vec16us a / const_int(d)
template <int d>
static inline Vec16us operator / (Vec16us const & a, Const_int_t<d>) {
    Static_error_check< (d>=0) > Error_dividing_unsigned_by_negative;// Error: dividing unsigned by negative is ambiguous
    return divide_by_ui<d>(a);                                       // unsigned divide
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec16us & operator /= (Vec16us & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <int32_t d>
static inline Vec16us & operator /= (Vec16us & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}


// define Vec32c a / const_int(d)
template <int d>
static inline Vec32c operator / (Vec32c const & a, Const_int_t<d>) {
    // expand into two Vec16s
    Vec16s low  = extend_low(a)  / Const_int_t<d>();
    Vec16s high = extend_high(a) / Const_int_t<d>();
    return compress(low,high);
}

// define Vec32c a / const_uint(d)
template <uint32_t d>
static inline Vec32c operator / (Vec32c const & a, Const_uint_t<d>) {
    Static_error_check< (uint8_t(d)<0x80u) > Error_overflow_dividing_signed_by_unsigned; // Error: dividing signed by overflowing unsigned
    return a / Const_int_t<d>();                                     // signed divide
}

// vector operator /= : divide
template <int32_t d>
static inline Vec32c & operator /= (Vec32c & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}
// vector operator /= : divide
template <uint32_t d>
static inline Vec32c & operator /= (Vec32c & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// define Vec32uc a / const_uint(d)
template <uint32_t d>
static inline Vec32uc operator / (Vec32uc const & a, Const_uint_t<d>) {
    // expand into two Vec16us
    Vec16us low  = extend_low(a)  / Const_uint_t<d>();
    Vec16us high = extend_high(a) / Const_uint_t<d>();
    return compress(low,high);
}

// define Vec32uc a / const_int(d)
template <int d>
static inline Vec32uc operator / (Vec32uc const & a, Const_int_t<d>) {
    Static_error_check< (int8_t(d)>=0) > Error_dividing_unsigned_by_negative;// Error: dividing unsigned by negative is ambiguous
    return a / Const_uint_t<d>();                                    // unsigned divide
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec32uc & operator /= (Vec32uc & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <int32_t d>
static inline Vec32uc & operator /= (Vec32uc & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}

/*****************************************************************************
*
*          Horizontal scan functions
*
*****************************************************************************/

// Get index to the first element that is true. Return -1 if all are false
static inline int horizontal_find_first(Vec32cb const & x) {
    uint32_t a = _mm256_movemask_epi8(x);
    if (a == 0) return -1;
    int32_t b = bit_scan_forward(a);
    return b;
}

static inline int horizontal_find_first(Vec16sb const & x) {
    return horizontal_find_first(Vec32cb(x)) >> 1;
}

static inline int horizontal_find_first(Vec8ib const & x) {
    return horizontal_find_first(Vec32cb(x)) >> 2;
}

static inline int horizontal_find_first(Vec4qb const & x) {
    return horizontal_find_first(Vec32cb(x)) >> 3;
}

// Count the number of elements that are true
static inline uint32_t horizontal_count(Vec32cb const & x) {
    uint32_t a = _mm256_movemask_epi8(x);
    return vml_popcnt(a);
}

static inline uint32_t horizontal_count(Vec16sb const & x) {
    return horizontal_count(Vec32cb(x)) >> 1;
}

static inline uint32_t horizontal_count(Vec8ib const & x) {
    return horizontal_count(Vec32cb(x)) >> 2;
}

static inline uint32_t horizontal_count(Vec4qb const & x) {
    return horizontal_count(Vec32cb(x)) >> 3;
}

/*****************************************************************************
*
*          Boolean <-> bitfield conversion functions
*
*****************************************************************************/

// to_bits: convert boolean vector to integer bitfield
static inline uint32_t to_bits(Vec32cb const & x) {
    return (uint32_t)_mm256_movemask_epi8(x);
}

// to_Vec16c: convert integer bitfield to boolean vector
static inline Vec32cb to_Vec32cb(uint32_t x) {
    return Vec32cb(Vec32c(to_Vec16cb(uint16_t(x)), to_Vec16cb(uint16_t(x>>16))));
}

// to_bits: convert boolean vector to integer bitfield
static inline uint16_t to_bits(Vec16sb const & x) {
    __m128i a = _mm_packs_epi16(x.get_low(), x.get_high());  // 16-bit words to bytes
    return (uint16_t)_mm_movemask_epi8(a);
}

// to_Vec16sb: convert integer bitfield to boolean vector
static inline Vec16sb to_Vec16sb(uint16_t x) {
    return Vec16sb(Vec16s(to_Vec8sb(uint8_t(x)), to_Vec8sb(uint8_t(x>>8))));
}

#if INSTRSET < 9 || MAX_VECTOR_SIZE < 512
// These functions are defined in Vectori512.h if AVX512 instruction set is used

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec8ib const & x) {
    __m128i a = _mm_packs_epi32(x.get_low(), x.get_high());  // 32-bit dwords to 16-bit words
    __m128i b = _mm_packs_epi16(a, a);  // 16-bit words to bytes
    return (uint8_t)_mm_movemask_epi8(b);
}

// to_Vec8ib: convert integer bitfield to boolean vector
static inline Vec8ib to_Vec8ib(uint8_t x) {
    return Vec8ib(Vec8i(to_Vec4ib(x), to_Vec4ib(x>>4)));
}

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec4qb const & x) {
    uint32_t a = _mm256_movemask_epi8(x);
    return ((a & 1) | ((a >> 7) & 2)) | (((a >> 14) & 4) | ((a >> 21) & 8));
}

// to_Vec4qb: convert integer bitfield to boolean vector
static inline Vec4qb to_Vec4qb(uint8_t x) {
    return  Vec4qb(Vec4q(-(x&1), -((x>>1)&1), -((x>>2)&1), -((x>>3)&1)));
}

#else  // function prototypes here only

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec8ib x);

// to_Vec8ib: convert integer bitfield to boolean vector
static inline Vec8ib to_Vec8ib(uint8_t x);

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec4qb x);

// to_Vec4qb: convert integer bitfield to boolean vector
static inline Vec4qb to_Vec4qb(uint8_t x);

#endif  // INSTRSET < 9 || MAX_VECTOR_SIZE < 512


#endif // VECTORI256_H
