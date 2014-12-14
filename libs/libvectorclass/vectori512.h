/****************************  vectori512.h   *******************************
* Author:        Agner Fog
* Date created:  2014-07-23
* Last modified: 2014-10-16
* Version:       1.16
* Project:       vector classes
* Description:
* Header file defining integer vector classes as interface to intrinsic 
* functions in x86 microprocessors with AVX512 and later instruction sets.
*
* Instructions:
* Use Gnu, Intel or Microsoft C++ compiler. Compile for the desired 
* instruction set, which must be at least AVX512. 
*
* The following vector classes are defined here:
* Vec16i    Vector of  16  32-bit signed   integers
* Vec16ui   Vector of  16  32-bit unsigned integers
* Vec16ib   Vector of  16  Booleans for use with Vec16i and Vec16ui
* Vec8q     Vector of   8  64-bit signed   integers
* Vec8uq    Vector of   8  64-bit unsigned integers
* Vec8qb    Vector of   8  Booleans for use with Vec8q and Vec8uq
*
* Each vector object is represented internally in the CPU as a 512-bit register.
* This header file defines operators and functions for these vectors.
*
* For detailed instructions, see VectorClass.pdf
*
* (c) Copyright 2014 GNU General Public License http://www.gnu.org/licenses
*****************************************************************************/

// check combination of header files
#if defined (VECTORI512_H)
#if    VECTORI512_H != 2
#error Two different versions of vectori512.h included
#endif
#else
#define VECTORI512_H  2

#ifdef VECTORF512_H
#error Please put header file vectori512.h before vectorf512.h
#endif


#if INSTRSET < 9   // AVX512 required
#error Wrong instruction set for vectori512.h, AVX512 required or use vectori512e.h
#endif

#include "vectori256.h"


// Bug fix for missing intrinsics:
// _mm512_cmpgt_epu32_mask, _mm512_cmpgt_epu64_mask
// all typecast intrinsics
// Fix expected in GCC version 4.9.2 but not seen yet https://gcc.gnu.org/bugzilla/show_bug.cgi?id=61878

// questionable
// _mm512_mask_mov_epi32 check select(). Doc at https://software.intel.com/en-us/node/513888 is wrong. Bug report filed


#if defined (GCC_VERSION) && GCC_VERSION < 41102 && !defined(__INTEL_COMPILER) && !defined(__clang__)

static inline  __m512i _mm512_castsi256_si512(__m256i x) {
    union {
        __m512i a;
        __m256i b;
    } u;
    u.b = x;
    return u.a;
}

static inline  __m256i _mm512_castsi512_si256(__m512i x) {
    union {
        __m512i a;
        __m256i b;
    } u;
    u.a = x;
    return u.b;
}

static inline  __m512i _mm512_castsi128_si512(__m128i x) {
    union {
        __m128i a;
        __m512i b;
    } u;
    u.a = x;
    return u.b;
}

static inline  __m128i _mm512_castsi512_si128(__m512i x) {
    union {
        __m512i a;
        __m128i b;
    } u;
    u.a = x;
    return u.b;
}

#endif


/*****************************************************************************
*
*          Generate compile-time constant vector
*
*****************************************************************************/
// Generate a constant vector of 8 integers stored in memory.
// Can be converted to any integer vector type
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7, 
int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
static inline __m512i constant16i() {
    static const union {
        int32_t i[16];
        __m512i zmm;
    } u = {{i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15}};
    return u.zmm;
}


/*****************************************************************************
*
*          Boolean vector base classes for AVX512
*
*****************************************************************************/

class Vec16b {
protected:
    __mmask16  m16; // Boolean vector
public:
    // Default constructor:
    Vec16b () {
    }
    // Constructor to convert from type __mmask16 used in intrinsics:
    Vec16b (__mmask16 x) {
        m16 = x;
    }
    // Constructor to build from all elements:
    Vec16b(bool b0, bool b1, bool b2, bool b3, bool b4, bool b5, bool b6, bool b7, 
    bool b8, bool b9, bool b10, bool b11, bool b12, bool b13, bool b14, bool b15) {
        m16 = uint16_t(b0 | b1<<1 | b2<<2 | b3<<3 | b4<<4 | b5<<5 | b6<<6 | b7<<7 |
              b8<<8 | b9<<9 | b10<<10 | b11<<11 | b12<<12 | b13<<13 | b14<<14 | b15<<15);
    }
    // Constructor to broadcast single value:
    Vec16b(bool b) {
        m16 = __mmask16(-int16_t(b));
    }
private: // Prevent constructing from int, etc.
    Vec16b(int b);
public:
    // Constructor to make from two halves
    Vec16b (Vec8ib const & x0, Vec8ib const & x1) {
        // = Vec16i(x0,x1) != 0;  (not defined yet)
        __m512i z = _mm512_inserti64x4(_mm512_castsi256_si512(x0), x1, 1);
        m16 = _mm512_cmpneq_epi32_mask(z, _mm512_setzero_epi32());
    }        
    // Assignment operator to convert from type __mmask16 used in intrinsics:
    Vec16b & operator = (__mmask16 x) {
        m16 = x;
        return *this;
    }
    // Assignment operator to broadcast scalar value:
    Vec16b & operator = (bool b) {
        m16 = Vec16b(b);
        return *this;
    }
private: // Prevent assigning int because of ambiguity
    Vec16b & operator = (int x);
public:
    // Type cast operator to convert to __mmask16 used in intrinsics
    operator __mmask16() const {
        return m16;
    }
    // split into two halves
    Vec8ib get_low() const {
        return to_Vec8ib((uint8_t)m16);
    }
    Vec8ib get_high() const {
        return to_Vec8ib((uint16_t)m16 >> 8);
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec16b const & insert(uint32_t index, bool value) {
        m16 = __mmask16(((uint16_t)m16 & ~(1 << index)) | (int)value << index);
        return *this;
    }
    // Member function extract a single element from vector
    bool extract(uint32_t index) const {
        return ((uint32_t)m16 >> index) & 1;
    }
    // Extract a single element. Operator [] can only read an element, not write.
    bool operator [] (uint32_t index) const {
        return extract(index);
    }
    static int size () {
        return 16;
    }
};

// Define operators for this class

// vector operator & : bitwise and
static inline Vec16b operator & (Vec16b a, Vec16b b) {
    return _mm512_kand(a, b);
}
static inline Vec16b operator && (Vec16b a, Vec16b b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec16b operator | (Vec16b a, Vec16b b) {
    return _mm512_kor(a, b);
}
static inline Vec16b operator || (Vec16b a, Vec16b b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec16b operator ^ (Vec16b a, Vec16b b) {
    return _mm512_kxor(a, b);
}

// vector operator ~ : bitwise not
static inline Vec16b operator ~ (Vec16b a) {
    return _mm512_knot(a);
}

// vector operator ! : element not
static inline Vec16b operator ! (Vec16b a) {
    return ~a;
}

// vector operator &= : bitwise and
static inline Vec16b & operator &= (Vec16b & a, Vec16b b) {
    a = a & b;
    return a;
}

// vector operator |= : bitwise or
static inline Vec16b & operator |= (Vec16b & a, Vec16b b) {
    a = a | b;
    return a;
}

// vector operator ^= : bitwise xor
static inline Vec16b & operator ^= (Vec16b & a, Vec16b b) {
    a = a ^ b;
    return a;
}


/*****************************************************************************
*
*          Functions for boolean vectors
*
*****************************************************************************/

// function andnot: a & ~ b
static inline Vec16b andnot (Vec16b a, Vec16b b) {
    return _mm512_kandn(b, a);
}

// horizontal_and. Returns true if all bits are 1
static inline bool horizontal_and (Vec16b const & a) {
    return (uint16_t)(__mmask16)a == 0xFFFF;
}

// horizontal_or. Returns true if at least one bit is 1
static inline bool horizontal_or (Vec16b const & a) {
    return (uint16_t)(__mmask16)a != 0;
}


/*****************************************************************************
*
*          Vec16ib: Vector of 16 Booleans for use with Vec16i and Vec16ui
*
*****************************************************************************/

class Vec16ib : public Vec16b {
public:
    // Default constructor:
    Vec16ib () {
    }
    Vec16ib (Vec16b x) {
        m16 = x;
    }
    // Constructor to build from all elements:
    Vec16ib(bool x0, bool x1, bool x2, bool x3, bool x4, bool x5, bool x6, bool x7,
        bool x8, bool x9, bool x10, bool x11, bool x12, bool x13, bool x14, bool x15) :
        Vec16b(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15) {
    }
    // Constructor to convert from type __mmask16 used in intrinsics:
    Vec16ib (__mmask16 x) {
        m16 = x;
    }
    // Constructor to broadcast single value:
    Vec16ib(bool b) : Vec16b(b) {}
private: // Prevent constructing from int, etc.
    Vec16ib(int b);
public:
    // Constructor to make from two halves
    Vec16ib (Vec8ib const & x0, Vec8ib const & x1) {
        m16 = Vec16b(x0, x1);
    }
    // Assignment operator to convert from type __mmask16 used in intrinsics:
    Vec16ib & operator = (__mmask16 x) {
        m16 = x;
        return *this;
    }
    // Assignment operator to broadcast scalar value:
    Vec16ib & operator = (bool b) {
        m16 = Vec16b(b);
        return *this;
    }
private: // Prevent assigning int because of ambiguity
    Vec16ib & operator = (int x);
public:
};

// Define operators for Vec16ib

// vector operator & : bitwise and
static inline Vec16ib operator & (Vec16ib a, Vec16ib b) {
    return Vec16b(a) & Vec16b(b);
}
static inline Vec16ib operator && (Vec16ib a, Vec16ib b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec16ib operator | (Vec16ib a, Vec16ib b) {
    return Vec16b(a) | Vec16b(b);
}
static inline Vec16ib operator || (Vec16ib a, Vec16ib b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec16ib operator ^ (Vec16ib a, Vec16ib b) {
    return Vec16b(a) ^ Vec16b(b);
}

// vector operator ~ : bitwise not
static inline Vec16ib operator ~ (Vec16ib a) {
    return ~Vec16b(a);
}

// vector operator ! : element not
static inline Vec16ib operator ! (Vec16ib a) {
    return ~a;
}

// vector operator &= : bitwise and
static inline Vec16ib & operator &= (Vec16ib & a, Vec16ib b) {
    a = a & b;
    return a;
}

// vector operator |= : bitwise or
static inline Vec16ib & operator |= (Vec16ib & a, Vec16ib b) {
    a = a | b;
    return a;
}

// vector operator ^= : bitwise xor
static inline Vec16ib & operator ^= (Vec16ib & a, Vec16ib b) {
    a = a ^ b;
    return a;
}

// vector function andnot
static inline Vec16ib andnot (Vec16ib a, Vec16ib b) {
    return Vec16ib(andnot(Vec16b(a), Vec16b(b)));
}


/*****************************************************************************
*
*          Vec8b: Base class vector of 8 Booleans
*
*****************************************************************************/

class Vec8b : public Vec16b {
public:
    // Default constructor:
    Vec8b () {
    }
    // Constructor to convert from type __mmask8 used in intrinsics:
    Vec8b (__mmask8 x) {
        m16 = x;
    }
    // Constructor to build from all elements:
    Vec8b(bool b0, bool b1, bool b2, bool b3, bool b4, bool b5, bool b6, bool b7) {
        m16 = uint16_t(b0 | b1<<1 | b2<<2 | b3<<3 | b4<<4 | b5<<5 | b6<<6 | b7<<7);
    }
    Vec8b (Vec16b const & x) {
        m16 = __mmask8(x);
    }
    // Constructor to broadcast single value:
    Vec8b(bool b) {
        m16 = __mmask8(-int8_t(b));
    }
    // Assignment operator to convert from type __mmask8 used in intrinsics:
    Vec8b & operator = (__mmask8 x) {
        m16 = x;
        return *this;
    }
private: // Prevent constructing from int etc. because of ambiguity
    Vec8b(int b);
    Vec8b & operator = (int x);
public:
    // split into two halves
    Vec4qb get_low() const {
        return Vec4qb(Vec4q(_mm512_castsi512_si256(_mm512_maskz_set1_epi64(__mmask16(m16), -1LL))));
    }
    Vec4qb get_high() const {
        return Vec8b(__mmask8(m16 >> 4)).get_low();
    }
    static int size () {
        return 8;
    }
};


/*****************************************************************************
*
*          Vec8qb: Vector of 8 Booleans for use with Vec8q and Vec8qu
*
*****************************************************************************/

class Vec8qb : public Vec8b {
public:
    // Default constructor:
    Vec8qb () {
    }
    Vec8qb (Vec16b x) {
        m16 = x;
    }
    // Constructor to build from all elements:
    Vec8qb(bool x0, bool x1, bool x2, bool x3, bool x4, bool x5, bool x6, bool x7) :
        Vec8b(x0, x1, x2, x3, x4, x5, x6, x7) {
    }
    // Constructor to convert from type __mmask8 used in intrinsics:
    Vec8qb (__mmask8 x) {
        m16 = x;
    }
    // Assignment operator to convert from type __mmask8 used in intrinsics:
    Vec8qb & operator = (__mmask8 x) {
        m16 = x;
        return *this;
    }
    // Constructor to broadcast single value:
    Vec8qb(bool b) : Vec8b(b) {}
    // Assignment operator to broadcast scalar:
    Vec8qb & operator = (bool b) {
        m16 = Vec8b(b);
        return *this;
    }
private: // Prevent constructing from int, etc.
    Vec8qb(int b);
    Vec8qb & operator = (int x);
public:
    // Constructor to make from two halves
    Vec8qb (Vec4qb const & x0, Vec4qb const & x1) {
        // = Vec8q(x0,x1) != 0;  (not defined yet)
        __m512i z = _mm512_inserti64x4(_mm512_castsi256_si512(x0), x1, 1);
        m16 = _mm512_cmpneq_epi64_mask(z, _mm512_setzero_si512());
    }        
};

// Define operators for Vec8qb

// vector operator & : bitwise and
static inline Vec8qb operator & (Vec8qb a, Vec8qb b) {
    return Vec16b(a) & Vec16b(b);
}
static inline Vec8qb operator && (Vec8qb a, Vec8qb b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec8qb operator | (Vec8qb a, Vec8qb b) {
    return Vec16b(a) | Vec16b(b);
}
static inline Vec8qb operator || (Vec8qb a, Vec8qb b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec8qb operator ^ (Vec8qb a, Vec8qb b) {
    return Vec16b(a) ^ Vec16b(b);
}

// vector operator ~ : bitwise not
static inline Vec8qb operator ~ (Vec8qb a) {
    return ~Vec16b(a);
}

// vector operator ! : element not
static inline Vec8qb operator ! (Vec8qb a) {
    return ~a;
}

// vector operator &= : bitwise and
static inline Vec8qb & operator &= (Vec8qb & a, Vec8qb b) {
    a = a & b;
    return a;
}

// vector operator |= : bitwise or
static inline Vec8qb & operator |= (Vec8qb & a, Vec8qb b) {
    a = a | b;
    return a;
}

// vector operator ^= : bitwise xor
static inline Vec8qb & operator ^= (Vec8qb & a, Vec8qb b) {
    a = a ^ b;
    return a;
}

// to_bits: convert to integer bitfield
static inline uint32_t to_bits(Vec8qb a) {
    return (uint8_t)(__mmask16)a;
}

// vector function andnot
static inline Vec8qb andnot (Vec8qb a, Vec8qb b) {
    return Vec8qb(andnot(Vec16b(a), Vec16b(b)));
}


/*****************************************************************************
*
*          Vector of 512 1-bit unsigned integers (base class for Vec16i)
*
*****************************************************************************/
class Vec512b {
protected:
    __m512i zmm; // Integer vector
public:
    // Default constructor:
    Vec512b() {
    }
    // Constructor to build from two Vec256b:
    Vec512b(Vec256b const & a0, Vec256b const & a1) {
        zmm = _mm512_inserti64x4(_mm512_castsi256_si512(a0), a1, 1);
    }
    // Constructor to convert from type __m512i used in intrinsics:
    Vec512b(__m512i const & x) {
        zmm = x;
    }
    // Assignment operator to convert from type __m512i used in intrinsics:
    Vec512b & operator = (__m512i const & x) {
        zmm = x;
        return *this;
    }
    // Type cast operator to convert to __m512i used in intrinsics
    operator __m512i() const {
        return zmm;
    }
    // Member function to load from array (unaligned)
    Vec512b & load(void const * p) {
        zmm = _mm512_loadu_si512(p);
        return *this;
    }
    // Member function to load from array, aligned by 64
    // You may use load_a instead of load if you are certain that p points to an address
    // divisible by 64, but there is hardly any speed advantage of load_a on modern processors
    Vec512b & load_a(void const * p) {
        zmm = _mm512_load_si512(p);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(void * p) const {
        _mm512_storeu_si512(p, zmm);
    }
    // Member function to store into array, aligned by 64
    // You may use store_a instead of store if you are certain that p points to an address
    // divisible by 64, but there is hardly any speed advantage of store_a on modern processors
    void store_a(void * p) const {
        _mm512_store_si512(p, zmm);
    }
    // Member function to change a single bit, mainly for test purposes
    // Note: This function is inefficient. Use load function if changing more than one bit
    Vec512b const & set_bit(uint32_t index, int value) {
        static uint64_t m[16] = {0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0};
        int wi = (index >> 6) & 7;               // qword index
        int bi = index & 0x3F;                   // bit index within qword w

        __m512i mask = Vec512b().load(m+8-wi);   // 1 in qword number wi
        mask = _mm512_sll_epi64(mask,_mm_cvtsi32_si128(bi)); // mask with bit number b set
        if (value & 1) {
            zmm = _mm512_or_si512(mask,zmm);
        }
        else {
            zmm = _mm512_andnot_si512(mask,zmm);
        }
        return *this;
    }
    // Member function to get a single bit, mainly for test purposes
    // Note: This function is inefficient. Use store function if reading more than one bit
    int get_bit(uint32_t index) const {
        union {
            __m512i z;
            uint8_t i[64];
        } u;
        u.z = zmm; 
        int wi = (index >> 3) & 0x3F;            // byte index
        int bi = index & 7;                      // bit index within byte w
        return (u.i[wi] >> bi) & 1;
    }
    // Member functions to split into two Vec256b:
    Vec256b get_low() const {
        return _mm512_castsi512_si256(zmm);
    }
    Vec256b get_high() const {
        return _mm512_extracti64x4_epi64(zmm,1);
    }
    static int size () {
        return 512;
    }
};


// Define operators for this class

// vector operator & : bitwise and
static inline Vec512b operator & (Vec512b const & a, Vec512b const & b) {
    return _mm512_and_epi32(a, b);
}
static inline Vec512b operator && (Vec512b const & a, Vec512b const & b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec512b operator | (Vec512b const & a, Vec512b const & b) {
    return _mm512_or_epi32(a, b);
}
static inline Vec512b operator || (Vec512b const & a, Vec512b const & b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec512b operator ^ (Vec512b const & a, Vec512b const & b) {
    return _mm512_xor_epi32(a, b);
}

// vector operator ~ : bitwise not
static inline Vec512b operator ~ (Vec512b const & a) {
    return _mm512_xor_epi32(a, _mm512_set1_epi32(-1));
}

// vector operator &= : bitwise and
static inline Vec512b & operator &= (Vec512b & a, Vec512b const & b) {
    a = a & b;
    return a;
}

// vector operator |= : bitwise or
static inline Vec512b & operator |= (Vec512b & a, Vec512b const & b) {
    a = a | b;
    return a;
}

// vector operator ^= : bitwise xor
static inline Vec512b & operator ^= (Vec512b & a, Vec512b const & b) {
    a = a ^ b;
    return a;
}

// Define functions for this class

// function andnot: a & ~ b
static inline Vec512b andnot (Vec512b const & a, Vec512b const & b) {
    return _mm512_andnot_epi32(b, a);
}


/*****************************************************************************
*
*          Vector of 16 32-bit signed integers
*
*****************************************************************************/

class Vec16i: public Vec512b {
public:
    // Default constructor:
    Vec16i() {
    };
    // Constructor to broadcast the same value into all elements:
    Vec16i(int i) {
        zmm = _mm512_set1_epi32(i);
    };
    // Constructor to build from all elements:
    Vec16i(int32_t i0, int32_t i1, int32_t i2, int32_t i3, int32_t i4, int32_t i5, int32_t i6, int32_t i7,
        int32_t i8, int32_t i9, int32_t i10, int32_t i11, int32_t i12, int32_t i13, int32_t i14, int32_t i15) {
        zmm = _mm512_setr_epi32(i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15);
    };
    // Constructor to build from two Vec8i:
    Vec16i(Vec8i const & a0, Vec8i const & a1) {
        zmm = _mm512_inserti64x4(_mm512_castsi256_si512(a0), a1, 1);
    }
    // Constructor to convert from type __m512i used in intrinsics:
    Vec16i(__m512i const & x) {
        zmm = x;
    };
    // Assignment operator to convert from type __m512i used in intrinsics:
    Vec16i & operator = (__m512i const & x) {
        zmm = x;
        return *this;
    };
    // Type cast operator to convert to __m512i used in intrinsics
    operator __m512i() const {
        return zmm;
    };
    // Member function to load from array (unaligned)
    Vec16i & load(void const * p) {
        zmm = _mm512_loadu_si512(p);
        return *this;
    }
    // Member function to load from array, aligned by 64
    Vec16i & load_a(void const * p) {
        zmm = _mm512_load_si512(p);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec16i & load_partial(int n, void const * p) {
        zmm = _mm512_maskz_loadu_epi32(__mmask16((1 << n) - 1), p);
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
        _mm512_mask_storeu_epi32(p, __mmask16((1 << n) - 1), zmm);
    }
    // cut off vector to n elements. The last 16-n elements are set to zero
    Vec16i & cutoff(int n) {
        zmm = _mm512_maskz_mov_epi32(__mmask16((1 << n) - 1), zmm);
        return *this;
    }
    // Member function to change a single element in vector
    Vec16i const & insert(uint32_t index, int32_t value) {
        zmm = _mm512_mask_set1_epi32(zmm, __mmask16(1 << index), value);
        return *this;
    };
    // Member function extract a single element from vector
    int32_t extract(uint32_t index) const {
        int32_t a[16];
        store(a);
        return a[index & 15];
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int32_t operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec8i:
    Vec8i get_low() const {
        return _mm512_castsi512_si256(zmm);
    }
    Vec8i get_high() const {
        return _mm512_extracti64x4_epi64(zmm,1);
    }
    static int size () {
        return 16;
    }
};


// Define operators for Vec16i

// vector operator + : add element by element
static inline Vec16i operator + (Vec16i const & a, Vec16i const & b) {
    return _mm512_add_epi32(a, b);
}

// vector operator += : add
static inline Vec16i & operator += (Vec16i & a, Vec16i const & b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec16i operator ++ (Vec16i & a, int) {
    Vec16i a0 = a;
    a = a + 1;
    return a0;
}

// prefix operator ++
static inline Vec16i & operator ++ (Vec16i & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec16i operator - (Vec16i const & a, Vec16i const & b) {
    return _mm512_sub_epi32(a, b);
}

// vector operator - : unary minus
static inline Vec16i operator - (Vec16i const & a) {
    return _mm512_sub_epi32(_mm512_setzero_epi32(), a);
}

// vector operator -= : subtract
static inline Vec16i & operator -= (Vec16i & a, Vec16i const & b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec16i operator -- (Vec16i & a, int) {
    Vec16i a0 = a;
    a = a - 1;
    return a0;
}

// prefix operator --
static inline Vec16i & operator -- (Vec16i & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec16i operator * (Vec16i const & a, Vec16i const & b) {
    return _mm512_mullo_epi32(a, b);
}

// vector operator *= : multiply
static inline Vec16i & operator *= (Vec16i & a, Vec16i const & b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer
// See bottom of file


// vector operator << : shift left
static inline Vec16i operator << (Vec16i const & a, int32_t b) {
    return _mm512_sll_epi32(a, _mm_cvtsi32_si128(b));
}

// vector operator <<= : shift left
static inline Vec16i & operator <<= (Vec16i & a, int32_t b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec16i operator >> (Vec16i const & a, int32_t b) {
    return _mm512_sra_epi32(a, _mm_cvtsi32_si128(b));
}

// vector operator >>= : shift right arithmetic
static inline Vec16i & operator >>= (Vec16i & a, int32_t b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec16ib operator == (Vec16i const & a, Vec16i const & b) {
    return _mm512_cmpeq_epi32_mask(a, b);
}

// vector operator != : returns true for elements for which a != b
static inline Vec16ib operator != (Vec16i const & a, Vec16i const & b) {
    return _mm512_cmpneq_epi32_mask(a, b);
}
  
// vector operator > : returns true for elements for which a > b
static inline Vec16ib operator > (Vec16i const & a, Vec16i const & b) {
    return  _mm512_cmpgt_epi32_mask(a, b);
}

// vector operator < : returns true for elements for which a < b
static inline Vec16ib operator < (Vec16i const & a, Vec16i const & b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec16ib operator >= (Vec16i const & a, Vec16i const & b) {
    return _mm512_cmpge_epi32_mask(a, b);
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec16ib operator <= (Vec16i const & a, Vec16i const & b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec16i operator & (Vec16i const & a, Vec16i const & b) {
    return _mm512_and_epi32(a, b);
}

// vector operator &= : bitwise and
static inline Vec16i & operator &= (Vec16i & a, Vec16i const & b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec16i operator | (Vec16i const & a, Vec16i const & b) {
    return _mm512_or_epi32(a, b);
}

// vector operator |= : bitwise or
static inline Vec16i & operator |= (Vec16i & a, Vec16i const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec16i operator ^ (Vec16i const & a, Vec16i const & b) {
    return _mm512_xor_epi32(a, b);
}

// vector operator ^= : bitwise xor
static inline Vec16i & operator ^= (Vec16i & a, Vec16i const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec16i operator ~ (Vec16i const & a) {
    return a ^ Vec16i(-1);
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec16i select (Vec16ib const & s, Vec16i const & a, Vec16i const & b) {
    return _mm512_mask_mov_epi32(b, s, a);  // conditional move may be optimized better by the compiler than blend
    // return _mm512_mask_blend_epi32(s, b, a);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec16i if_add (Vec16ib const & f, Vec16i const & a, Vec16i const & b) {
    return _mm512_mask_add_epi32(a, f, a, b);
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
static inline int32_t horizontal_add (Vec16i const & a) {
#if defined(__INTEL_COMPILER)
    return _mm512_reduce_add_epi32(a);
#else
    return horizontal_add(a.get_low() + a.get_high());
#endif
}

// function add_saturated: add element by element, signed with saturation
// (is it faster to up-convert to 64 bit integers, and then downconvert the sum with saturation?)
static inline Vec16i add_saturated(Vec16i const & a, Vec16i const & b) {
    __m512i sum    = _mm512_add_epi32(a, b);                  // a + b
    __m512i axb    = _mm512_xor_epi32(a, b);                  // check if a and b have different sign
    __m512i axs    = _mm512_xor_epi32(a, sum);                // check if a and sum have different sign
    __m512i ovf1   = _mm512_andnot_epi32(axb,axs);            // check if sum has wrong sign
    __m512i ovf2   = _mm512_srai_epi32(ovf1,31);              // -1 if overflow
    __mmask16 ovf3 = _mm512_cmpneq_epi32_mask(ovf2, _mm512_setzero_epi32()); // same, as mask
    __m512i asign  = _mm512_srli_epi32(a,31);                 // 1  if a < 0
    __m512i sat1   = _mm512_srli_epi32(ovf2,1);               // 7FFFFFFF if overflow
    __m512i sat2   = _mm512_add_epi32(sat1,asign);            // 7FFFFFFF if positive overflow 80000000 if negative overflow
    return _mm512_mask_blend_epi32(ovf3, sum, sat2);          // sum if not overflow, else sat2
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec16i sub_saturated(Vec16i const & a, Vec16i const & b) {
    __m512i diff   = _mm512_sub_epi32(a, b);                  // a + b
    __m512i axb    = _mm512_xor_si512(a, b);                  // check if a and b have different sign
    __m512i axs    = _mm512_xor_si512(a, diff);               // check if a and sum have different sign
    __m512i ovf1   = _mm512_and_si512(axb,axs);               // check if sum has wrong sign
    __m512i ovf2   = _mm512_srai_epi32(ovf1,31);              // -1 if overflow
    __mmask16 ovf3 = _mm512_cmpneq_epi32_mask(ovf2, _mm512_setzero_epi32()); // same, as mask
    __m512i asign  = _mm512_srli_epi32(a,31);                 // 1  if a < 0
    __m512i sat1   = _mm512_srli_epi32(ovf2,1);               // 7FFFFFFF if overflow
    __m512i sat2   = _mm512_add_epi32(sat1,asign);            // 7FFFFFFF if positive overflow 80000000 if negative overflow
    return _mm512_mask_blend_epi32(ovf3, diff, sat2);         // sum if not overflow, else sat2
}

// function max: a > b ? a : b
static inline Vec16i max(Vec16i const & a, Vec16i const & b) {
    return _mm512_max_epi32(a,b);
}

// function min: a < b ? a : b
static inline Vec16i min(Vec16i const & a, Vec16i const & b) {
    return _mm512_min_epi32(a,b);
}

// function abs: a >= 0 ? a : -a
static inline Vec16i abs(Vec16i const & a) {
    return _mm512_abs_epi32(a);
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec16i abs_saturated(Vec16i const & a) {
    return _mm512_min_epu32(abs(a), Vec16i(0x7FFFFFFF));
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec16i rotate_left(Vec16i const & a, int b) {
    return _mm512_rolv_epi32(a, Vec16i(b));
}


/*****************************************************************************
*
*          Vector of 16 32-bit unsigned integers
*
*****************************************************************************/


class Vec16ui : public Vec16i {
public:
    // Default constructor:
    Vec16ui() {
    };
    // Constructor to broadcast the same value into all elements:
    Vec16ui(uint32_t i) {
        zmm = _mm512_set1_epi32(i);
    };
    // Constructor to build from all elements:
    Vec16ui(uint32_t i0, uint32_t i1, uint32_t i2, uint32_t i3, uint32_t i4, uint32_t i5, uint32_t i6, uint32_t i7, 
        uint32_t i8, uint32_t i9, uint32_t i10, uint32_t i11, uint32_t i12, uint32_t i13, uint32_t i14, uint32_t i15) {
        zmm = _mm512_setr_epi32(i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15);
    };
    // Constructor to build from two Vec8ui:
    Vec16ui(Vec8ui const & a0, Vec8ui const & a1) {
        zmm = Vec16i(Vec8i(a0), Vec8i(a1));
    }
    // Constructor to convert from type __m512i used in intrinsics:
    Vec16ui(__m512i const & x) {
        zmm = x;
    };
    // Assignment operator to convert from type __m512i used in intrinsics:
    Vec16ui & operator = (__m512i const & x) {
        zmm = x;
        return *this;
    };
    // Member function to load from array (unaligned)
    Vec16ui & load(void const * p) {
        Vec16i::load(p);
        return *this;
    }
    // Member function to load from array, aligned by 64
    Vec16ui & load_a(void const * p) {
        Vec16i::load_a(p);
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec16ui const & insert(uint32_t index, uint32_t value) {
        Vec16i::insert(index, value);
        return *this;
    }
    // Member function extract a single element from vector
    uint32_t extract(uint32_t index) const {
        return Vec16i::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint32_t operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4ui:
    Vec8ui get_low() const {
        return Vec8ui(Vec16i::get_low());
    }
    Vec8ui get_high() const {
        return Vec8ui(Vec16i::get_high());
    }
};

// Define operators for this class

// vector operator + : add
static inline Vec16ui operator + (Vec16ui const & a, Vec16ui const & b) {
    return Vec16ui (Vec16i(a) + Vec16i(b));
}

// vector operator - : subtract
static inline Vec16ui operator - (Vec16ui const & a, Vec16ui const & b) {
    return Vec16ui (Vec16i(a) - Vec16i(b));
}

// vector operator * : multiply
static inline Vec16ui operator * (Vec16ui const & a, Vec16ui const & b) {
    return Vec16ui (Vec16i(a) * Vec16i(b));
}

// vector operator / : divide
// See bottom of file

// vector operator >> : shift right logical all elements
static inline Vec16ui operator >> (Vec16ui const & a, uint32_t b) {
    return _mm512_srl_epi32(a, _mm_cvtsi32_si128(b)); 
}

// vector operator >> : shift right logical all elements
static inline Vec16ui operator >> (Vec16ui const & a, int32_t b) {
    return a >> (uint32_t)b;
}

// vector operator >>= : shift right logical
static inline Vec16ui & operator >>= (Vec16ui & a, uint32_t b) {
    a = a >> b;
    return a;
} 

// vector operator >>= : shift right logical
static inline Vec16ui & operator >>= (Vec16ui & a, int32_t b) {
    a = a >> uint32_t(b);
    return a;
}

// vector operator << : shift left all elements
static inline Vec16ui operator << (Vec16ui const & a, uint32_t b) {
    return Vec16ui ((Vec16i)a << (int32_t)b);
}

// vector operator << : shift left all elements
static inline Vec16ui operator << (Vec16ui const & a, int32_t b) {
    return Vec16ui ((Vec16i)a << (int32_t)b);
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec16ib operator < (Vec16ui const & a, Vec16ui const & b) {
    return _mm512_cmplt_epu32_mask(a, b);
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec16ib operator > (Vec16ui const & a, Vec16ui const & b) {
    return b < a;
}


// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec16ib operator >= (Vec16ui const & a, Vec16ui const & b) {
    return  _mm512_cmpge_epu32_mask(a, b);
}            

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec16ib operator <= (Vec16ui const & a, Vec16ui const & b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec16ui operator & (Vec16ui const & a, Vec16ui const & b) {
    return Vec16ui(Vec16i(a) & Vec16i(b));
}

// vector operator | : bitwise or
static inline Vec16ui operator | (Vec16ui const & a, Vec16ui const & b) {
    return Vec16ui(Vec16i(a) | Vec16i(b));
}

// vector operator ^ : bitwise xor
static inline Vec16ui operator ^ (Vec16ui const & a, Vec16ui const & b) {
    return Vec16ui(Vec16i(a) ^ Vec16i(b));
}

// vector operator ~ : bitwise not
static inline Vec16ui operator ~ (Vec16ui const & a) {
    return Vec16ui( ~ Vec16i(a));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec16ui select (Vec16ib const & s, Vec16ui const & a, Vec16ui const & b) {
    return Vec16ui(select(s, Vec16i(a), Vec16i(b)));
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec16ui if_add (Vec16ib const & f, Vec16ui const & a, Vec16ui const & b) {
    return Vec16ui(if_add(f, Vec16i(a), Vec16i(b)));
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
static inline uint32_t horizontal_add (Vec16ui const & a) {
    return horizontal_add((Vec16i)a);
}

// horizontal_add_x: Horizontal add extended: Calculates the sum of all vector elements. Defined later in this file

// function add_saturated: add element by element, unsigned with saturation
static inline Vec16ui add_saturated(Vec16ui const & a, Vec16ui const & b) {
    Vec16ui sum      = a + b;
    Vec16ib overflow = sum < (a | b);                  // overflow if (a + b) < (a | b)
    return _mm512_mask_set1_epi32(sum, overflow, -1);  // 0xFFFFFFFF if overflow
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec16ui sub_saturated(Vec16ui const & a, Vec16ui const & b) {
    Vec16ui diff      = a - b;
    return _mm512_maskz_mov_epi32(diff <= a, diff);   // underflow if diff > a gives zero
}

// function max: a > b ? a : b
static inline Vec16ui max(Vec16ui const & a, Vec16ui const & b) {
    return _mm512_max_epu32(a,b);
}

// function min: a < b ? a : b
static inline Vec16ui min(Vec16ui const & a, Vec16ui const & b) {
    return _mm512_min_epu32(a,b);
}


/*****************************************************************************
*
*          Vector of 8 64-bit signed integers
*
*****************************************************************************/

class Vec8q : public Vec512b {
public:
    // Default constructor:
    Vec8q() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec8q(int64_t i) {
        zmm = _mm512_set1_epi64(i);
    }
    // Constructor to build from all elements:
    Vec8q(int64_t i0, int64_t i1, int64_t i2, int64_t i3, int64_t i4, int64_t i5, int64_t i6, int64_t i7) {
        zmm = _mm512_setr_epi64(i0, i1, i2, i3, i4, i5, i6, i7);
    }
    // Constructor to build from two Vec4q:
    Vec8q(Vec4q const & a0, Vec4q const & a1) {
        zmm = _mm512_inserti64x4(_mm512_castsi256_si512(a0), a1, 1);
    }
    // Constructor to convert from type __m512i used in intrinsics:
    Vec8q(__m512i const & x) {
        zmm = x;
    }
    // Assignment operator to convert from type __m512i used in intrinsics:
    Vec8q & operator = (__m512i const & x) {
        zmm = x;
        return *this;
    }
    // Type cast operator to convert to __m512i used in intrinsics
    operator __m512i() const {
        return zmm;
    }
    // Member function to load from array (unaligned)
    Vec8q & load(void const * p) {
        zmm = _mm512_loadu_si512(p);
        return *this;
    }
    // Member function to load from array, aligned by 64
    Vec8q & load_a(void const * p) {
        zmm = _mm512_load_si512(p);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec8q & load_partial(int n, void const * p) {
        zmm = _mm512_maskz_loadu_epi64(__mmask8((1 << n) - 1), p);
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
        _mm512_mask_storeu_epi64(p, __mmask8((1 << n) - 1), zmm);
    }
    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec8q & cutoff(int n) {
        zmm = _mm512_maskz_mov_epi64(__mmask8((1 << n) - 1), zmm);
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec8q const & insert(uint32_t index, int64_t value) {
        zmm = _mm512_mask_set1_epi64(zmm, __mmask8(1 << index), value);
        // zmm = _mm512_mask_blend_epi64(__mmask8(1 << index), zmm, _mm512_set1_epi64(value));
        return *this;
    }
    // Member function extract a single element from vector
    int64_t extract(uint32_t index) const {
        int64_t a[8];
        store (a);
        return a[index & 7];
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int64_t operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec2q:
    Vec4q get_low() const {
        return _mm512_castsi512_si256(zmm);
    }
    Vec4q get_high() const {
        return _mm512_extracti64x4_epi64(zmm,1);
    }
    static int size () {
        return 8;
    }
};


// Define operators for Vec8q

// vector operator + : add element by element
static inline Vec8q operator + (Vec8q const & a, Vec8q const & b) {
    return _mm512_add_epi64(a, b);
}

// vector operator += : add
static inline Vec8q & operator += (Vec8q & a, Vec8q const & b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec8q operator ++ (Vec8q & a, int) {
    Vec8q a0 = a;
    a = a + 1;
    return a0;
}

// prefix operator ++
static inline Vec8q & operator ++ (Vec8q & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec8q operator - (Vec8q const & a, Vec8q const & b) {
    return _mm512_sub_epi64(a, b);
}

// vector operator - : unary minus
static inline Vec8q operator - (Vec8q const & a) {
    return _mm512_sub_epi64(_mm512_setzero_epi32(), a);
}

// vector operator -= : subtract
static inline Vec8q & operator -= (Vec8q & a, Vec8q const & b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec8q operator -- (Vec8q & a, int) {
    Vec8q a0 = a;
    a = a - 1;
    return a0;
}

// prefix operator --
static inline Vec8q & operator -- (Vec8q & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec8q operator * (Vec8q const & a, Vec8q const & b) {
#if defined (GCC_VERSION) && GCC_VERSION < 41100 && !defined(__INTEL_COMPILER) && !defined(__clang__)
    return Vec8q(a.get_low() * b.get_low(), a.get_high() * b.get_high());  // _mm512_mullox_epi64 missing in gcc 4.10.
#else
    return _mm512_mullox_epi64(a, b);
#endif
}

// vector operator *= : multiply
static inline Vec8q & operator *= (Vec8q & a, Vec8q const & b) {
    a = a * b;
    return a;
}

// vector operator << : shift left
static inline Vec8q operator << (Vec8q const & a, int32_t b) {
    return _mm512_sll_epi64(a, _mm_cvtsi32_si128(b));
}

// vector operator <<= : shift left
static inline Vec8q & operator <<= (Vec8q & a, int32_t b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec8q operator >> (Vec8q const & a, int32_t b) {
    return _mm512_sra_epi64(a, _mm_cvtsi32_si128(b));
}

// vector operator >>= : shift right arithmetic
static inline Vec8q & operator >>= (Vec8q & a, int32_t b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec8qb operator == (Vec8q const & a, Vec8q const & b) {
    return _mm512_cmpeq_epi64_mask(a, b);
}

// vector operator != : returns true for elements for which a != b
static inline Vec8qb operator != (Vec8q const & a, Vec8q const & b) {
    return _mm512_cmpneq_epi64_mask(a, b);
}
  
// vector operator < : returns true for elements for which a < b
static inline Vec8qb operator < (Vec8q const & a, Vec8q const & b) {
    return _mm512_cmplt_epi64_mask(a, b);
}

// vector operator > : returns true for elements for which a > b
static inline Vec8qb operator > (Vec8q const & a, Vec8q const & b) {
    return b < a;
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec8qb operator >= (Vec8q const & a, Vec8q const & b) {
    return _mm512_cmpge_epi64_mask(a, b);
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec8qb operator <= (Vec8q const & a, Vec8q const & b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec8q operator & (Vec8q const & a, Vec8q const & b) {
    return _mm512_and_epi32(a, b);
}

// vector operator &= : bitwise and
static inline Vec8q & operator &= (Vec8q & a, Vec8q const & b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec8q operator | (Vec8q const & a, Vec8q const & b) {
    return _mm512_or_epi32(a, b);
}

// vector operator |= : bitwise or
static inline Vec8q & operator |= (Vec8q & a, Vec8q const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec8q operator ^ (Vec8q const & a, Vec8q const & b) {
    return _mm512_xor_epi32(a, b);
}
// vector operator ^= : bitwise xor
static inline Vec8q & operator ^= (Vec8q & a, Vec8q const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec8q operator ~ (Vec8q const & a) {
    return Vec8q(~ Vec16i(a));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 4; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec8q select (Vec8qb const & s, Vec8q const & a, Vec8q const & b) {
    return _mm512_mask_mov_epi64(b, s, a);
    //return _mm512_mask_blend_epi64(s, b, a);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec8q if_add (Vec8qb const & f, Vec8q const & a, Vec8q const & b) {
    return _mm512_mask_add_epi64(a, f, a, b);
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
static inline int64_t horizontal_add (Vec8q const & a) {
#if defined(__INTEL_COMPILER)
    return _mm512_reduce_add_epi64(a);
#else
    return horizontal_add(a.get_low()+a.get_high());
#endif
}

// Horizontal add extended: Calculates the sum of all vector elements
// Elements are sign extended before adding to avoid overflow
static inline int64_t horizontal_add_x (Vec16i const & x) {
    Vec8q a = _mm512_cvtepi32_epi64(x.get_low());
    Vec8q b = _mm512_cvtepi32_epi64(x.get_high());
    return horizontal_add(a+b);
}

// Horizontal add extended: Calculates the sum of all vector elements
// Elements are zero extended before adding to avoid overflow
static inline uint64_t horizontal_add_x (Vec16ui const & x) {
    Vec8q a = _mm512_cvtepu32_epi64(x.get_low());
    Vec8q b = _mm512_cvtepu32_epi64(x.get_high());
    return horizontal_add(a+b);
}

// function max: a > b ? a : b
static inline Vec8q max(Vec8q const & a, Vec8q const & b) {
    return _mm512_max_epi64(a, b);
}

// function min: a < b ? a : b
static inline Vec8q min(Vec8q const & a, Vec8q const & b) {
    return _mm512_min_epi64(a, b);
}

// function abs: a >= 0 ? a : -a
static inline Vec8q abs(Vec8q const & a) {
    return _mm512_abs_epi64(a);
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec8q abs_saturated(Vec8q const & a) {
    return _mm512_min_epu64(abs(a), Vec8q(0x7FFFFFFFFFFFFFFF));
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec8q rotate_left(Vec8q const & a, int b) {
    return _mm512_rolv_epi64(a, Vec8q(b));
}


/*****************************************************************************
*
*          Vector of 8 64-bit unsigned integers
*
*****************************************************************************/

class Vec8uq : public Vec8q {
public:
    // Default constructor:
    Vec8uq() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec8uq(uint64_t i) {
        zmm = Vec8q(i);
    }
    // Constructor to convert from Vec8q:
    Vec8uq(Vec8q const & x) {
        zmm = x;
    }
    // Constructor to convert from type __m512i used in intrinsics:
    Vec8uq(__m512i const & x) {
        zmm = x;
    }
    // Constructor to build from all elements:
    Vec8uq(uint64_t i0, uint64_t i1, uint64_t i2, uint64_t i3, uint64_t i4, uint64_t i5, uint64_t i6, uint64_t i7) {
        zmm = Vec8q(i0, i1, i2, i3, i4, i5, i6, i7);
    }
    // Constructor to build from two Vec4uq:
    Vec8uq(Vec4uq const & a0, Vec4uq const & a1) {
        zmm = Vec8q(Vec4q(a0), Vec4q(a1));
    }
    // Assignment operator to convert from Vec8q:
    Vec8uq  & operator = (Vec8q const & x) {
        zmm = x;
        return *this;
    }
    // Assignment operator to convert from type __m512i used in intrinsics:
    Vec8uq & operator = (__m512i const & x) {
        zmm = x;
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec8uq & load(void const * p) {
        Vec8q::load(p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec8uq & load_a(void const * p) {
        Vec8q::load_a(p);
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec8uq const & insert(uint32_t index, uint64_t value) {
        Vec8q::insert(index, value);
        return *this;
    }
    // Member function extract a single element from vector
    uint64_t extract(uint32_t index) const {
        return Vec8q::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint64_t operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec2uq:
    Vec4uq get_low() const {
        return Vec4uq(Vec8q::get_low());
    }
    Vec4uq get_high() const {
        return Vec4uq(Vec8q::get_high());
    }
};

// Define operators for this class

// vector operator + : add
static inline Vec8uq operator + (Vec8uq const & a, Vec8uq const & b) {
    return Vec8uq (Vec8q(a) + Vec8q(b));
}

// vector operator - : subtract
static inline Vec8uq operator - (Vec8uq const & a, Vec8uq const & b) {
    return Vec8uq (Vec8q(a) - Vec8q(b));
}

// vector operator * : multiply element by element
static inline Vec8uq operator * (Vec8uq const & a, Vec8uq const & b) {
    return Vec8uq (Vec8q(a) * Vec8q(b));
}

// vector operator >> : shift right logical all elements
static inline Vec8uq operator >> (Vec8uq const & a, uint32_t b) {
    return _mm512_srl_epi64(a,_mm_cvtsi32_si128(b)); 
}

// vector operator >> : shift right logical all elements
static inline Vec8uq operator >> (Vec8uq const & a, int32_t b) {
    return a >> (uint32_t)b;
}

// vector operator >>= : shift right artihmetic
static inline Vec8uq & operator >>= (Vec8uq & a, uint32_t b) {
    a = a >> b;
    return a;
}

// vector operator >>= : shift right logical
static inline Vec8uq & operator >>= (Vec8uq & a, int32_t b) {
    a = a >> uint32_t(b);
    return a;
}

// vector operator << : shift left all elements
static inline Vec8uq operator << (Vec8uq const & a, uint32_t b) {
    return Vec8uq ((Vec8q)a << (int32_t)b);
}

// vector operator << : shift left all elements
static inline Vec8uq operator << (Vec8uq const & a, int32_t b) {
    return Vec8uq ((Vec8q)a << b);
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec8qb operator < (Vec8uq const & a, Vec8uq const & b) {
    return _mm512_cmplt_epu64_mask(a, b);
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec8qb operator > (Vec8uq const & a, Vec8uq const & b) {
    return b < a;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec8qb operator >= (Vec8uq const & a, Vec8uq const & b) {
    return _mm512_cmpge_epu64_mask(a, b);
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec8qb operator <= (Vec8uq const & a, Vec8uq const & b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec8uq operator & (Vec8uq const & a, Vec8uq const & b) {
    return Vec8uq(Vec8q(a) & Vec8q(b));
}

// vector operator | : bitwise or
static inline Vec8uq operator | (Vec8uq const & a, Vec8uq const & b) {
    return Vec8uq(Vec8q(a) | Vec8q(b));
}

// vector operator ^ : bitwise xor
static inline Vec8uq operator ^ (Vec8uq const & a, Vec8uq const & b) {
    return Vec8uq(Vec8q(a) ^ Vec8q(b));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 4; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec8uq select (Vec8qb const & s, Vec8uq const & a, Vec8uq const & b) {
    return Vec8uq(select(s, Vec8q(a), Vec8q(b)));
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec8uq if_add (Vec8qb const & f, Vec8uq const & a, Vec8uq const & b) {
    return _mm512_mask_add_epi64(a, f, a, b);
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
static inline uint64_t horizontal_add (Vec8uq const & a) {
    return horizontal_add(Vec8q(a));
}

// function max: a > b ? a : b
static inline Vec8uq max(Vec8uq const & a, Vec8uq const & b) {
    return _mm512_max_epu64(a, b);
}

// function min: a < b ? a : b
static inline Vec8uq min(Vec8uq const & a, Vec8uq const & b) {
    return _mm512_min_epu64(a, b);
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
* Vec8q a(10,11,12,13,14,15,16,17);      // a is (10,11,12,13,14,15,16,17)
* Vec8q b;
* b = permute8q<0,2,7,7,-1,-1,1,1>(a);   // b is (10,12,17,17, 0, 0,11,11)
*
* A lot of the code here is metaprogramming aiming to find the instructions
* that best fit the template parameters and instruction set. The metacode
* will be reduced out to leave only a few vector instructions in release
* mode with optimization on.
*****************************************************************************/

// Permute vector of 8 64-bit integers.
// Index -1 gives 0, index -256 means don't care.
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8q permute8q(Vec8q const & a) {

    // Combine indexes into a single bitfield, with 4 bits for each
    const int m1 = (i0&7) | (i1&7)<<4 | (i2&7)<< 8 | (i3&7)<<12 | (i4&7)<<16 | (i5&7)<<20 | (i6&7)<<24 | (i7&7)<<28;

    // Mask to zero out negative indexes
    const int mz = (i0<0?0:0xF) | (i1<0?0:0xF0) | (i2<0?0:0xF00) | (i3<0?0:0xF000) | (i4<0?0:0xF0000) | (i5<0?0:0xF00000) | (i6<0?0:0xF000000) | (i7<0?0:0xF0000000);
    const int m2 = m1 & mz;

    // zeroing needed
    const bool dozero = ((i0|i1|i2|i3|i4|i5|i6|i7) & 0x80) != 0;

    // special case: all zero
    if (mz == 0) return  _mm512_setzero_epi32();

    // mask for elements not zeroed
    const __mmask8  z = __mmask8((i0>=0)<<0 | (i1>=0)<<1 | (i2>=0)<<2 | (i3>=0)<<3 | (i4>=0)<<4 | (i5>=0)<<5 | (i6>=0)<<6 | (i7>=0)<<7);
    // same with 2 bits for each element
    const __mmask16 zz = __mmask16((i0>=0?3:0) | (i1>=0?0xC:0) | (i2>=0?0x30:0) | (i3>=0?0xC0:0) | (i4>=0?0x300:0) | (i5>=0?0xC00:0) | (i6>=0?0x3000:0) | (i7>=0?0xC000:0));

    if (((m1 ^ 0x76543210) & mz) == 0) {
        // no shuffling
        if (dozero) {
            // zero some elements
            return _mm512_maskz_mov_epi64(z, a);
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
                return _mm512_maskz_shuffle_epi32(zz, a, (_MM_PERM_ENUM)pmask);
            }
            else {  // permute within lanes
                return _mm512_shuffle_epi32(a, (_MM_PERM_ENUM)pmask);
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
            return  _mm512_maskz_shuffle_i64x2(z, a, a, (_MM_PERM_ENUM)s);
        }
        else {
            // permute lanes
            return _mm512_shuffle_i64x2(a, a, (_MM_PERM_ENUM)s);
        }
    }
    // full permute needed
    const __m512i pmask = constant16i<i0&7, 0, i1&7, 0, i2&7, 0, i3&7, 0, i4&7, 0, i5&7, 0, i6&7, 0, i7&7, 0>();
    if (dozero) {
        // full permute and zeroing
        // Documentation is inconsistent. which order of the operands is correct?
        return _mm512_maskz_permutexvar_epi64(z, pmask, a);
    }
    else {    
        return _mm512_permutexvar_epi64(pmask, a);
    }
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8uq permute8uq(Vec8uq const & a) {
    return Vec8uq (permute8q<i0,i1,i2,i3,i4,i5,i6,i7> (a));
}


// Permute vector of 16 32-bit integers.
// Index -1 gives 0, index -256 means don't care.
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7, int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
static inline Vec16i permute16i(Vec16i const & a) {

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
    if (mz == 0) return  _mm512_setzero_epi32();

    // mask for elements not zeroed
    const __mmask16 z = __mmask16((i0>=0)<<0 | (i1>=0)<<1 | (i2>=0)<<2 | (i3>=0)<<3 | (i4>=0)<<4 | (i5>=0)<<5 | (i6>=0)<<6 | (i7>=0)<<7
        | (i8>=0)<<8 | (i9>=0)<<9 | (i10>=0)<<10 | (i11>=0)<<11 | (i12>=0)<<12 | (i13>=0)<<13 | (i14>=0)<<14 | (i15>=0)<<15);

    if (((m1 ^ 0xFEDCBA9876543210) & mz) == 0) {
        // no shuffling
        if (dozero) {
            // zero some elements
            return _mm512_maskz_mov_epi32(z, a);
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
                return _mm512_maskz_shuffle_epi32(z, a, (_MM_PERM_ENUM)pmask);
            }
            else {  // permute within lanes
                return _mm512_shuffle_epi32(a, (_MM_PERM_ENUM)pmask);
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
            return  _mm512_maskz_shuffle_i32x4(z, a, a, (_MM_PERM_ENUM)s);
        }
        else {
            // permute lanes
            return _mm512_shuffle_i32x4(a, a, (_MM_PERM_ENUM)s);
        }
    }
    // full permute needed
    const __m512i pmask = constant16i<i0&15, i1&15, i2&15, i3&15, i4&15, i5&15, i6&15, i7&15, i8&15, i9&15, i10&15, i11&15, i12&15, i13&15, i14&15, i15&15>();
    if (dozero) {
        // full permute and zeroing
        return _mm512_maskz_permutexvar_epi32(z, pmask, a);
    }
    else {    
        return _mm512_permutexvar_epi32(pmask, a);
    }
}


template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7, int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
static inline Vec16ui permute16ui(Vec16ui const & a) {
    return Vec16ui (permute16i<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15> (a));
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
* Vec8q a(100,101,102,103,104,105,106,107); // a is (100, 101, 102, 103, 104, 105, 106, 107)
* Vec8q b(200,201,202,203,204,205,206,207); // b is (200, 201, 202, 203, 204, 205, 206, 207)
* Vec8q c;
* c = blend8q<1,0,9,8,7,-1,15,15> (a,b);    // c is (101, 100, 201, 200, 107,   0, 207, 207)
*
* A lot of the code here is metaprogramming aiming to find the instructions
* that best fit the template parameters and instruction set. The metacode
* will be reduced out to leave only a few vector instructions in release
* mode with optimization on.
*****************************************************************************/


template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7> 
static inline Vec8q blend8q(Vec8q const & a, Vec8q const & b) {  

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
    if (mz == 0) return  _mm512_setzero_epi32();

    // special case: all from a
    if ((m1 & 0x88888888 & mz) == 0) {
        return permute8q <i0, i1, i2, i3, i4, i5, i6, i7> (a);
    }

    // special case: all from b
    if ((~m1 & 0x88888888 & mz) == 0) {
        return permute8q <i0^8, i1^8, i2^8, i3^8, i4^8, i5^8, i6^8, i7^8> (b);
    }

    // special case: blend without permute
    if (((m1 ^ 0x76543210) & 0x77777777 & mz) == 0) {
        __mmask8 blendmask = __mmask8((i0&8)>>3 | (i1&8)>>2 | (i2&8)>>1 | (i3&8)>>0 | (i4&8)<<1 | (i5&8)<<2 | (i6&8)<<3 | (i7&8)<<4 );
        __m512i t = _mm512_mask_blend_epi64(blendmask, a, b);
        if (dozero) {
            t = _mm512_maskz_mov_epi64(z, t);
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
            // This code generates two instructions instead of one, but we are avoiding the slow lane-crossing instruction,
            // and we are saving 64 bytes of data cache.
            // 1. Permute a, zero elements not from a (using _mm512_maskz_shuffle_epi32)
            __m512i ta = permute8q< (maz&0xF)?i0&7:-1, (maz&0xF0)?i1&7:-1, (maz&0xF00)?i2&7:-1, (maz&0xF000)?i3&7:-1, 
                (maz&0xF0000)?i4&7:-1, (maz&0xF00000)?i5&7:-1, (maz&0xF000000)?i6&7:-1, (maz&0xF0000000)?i7&7:-1> (a);
            // write mask for elements from b
            const __mmask16 sb = ((mbz&0xF)?3:0) | ((mbz&0xF0)?0xC:0) | ((mbz&0xF00)?0x30:0) | ((mbz&0xF000)?0xC0:0) | ((mbz&0xF0000)?0x300:0) | ((mbz&0xF00000)?0xC00:0) | ((mbz&0xF000000)?0x3000:0) | ((mbz&0xF0000000)?0xC000:0);
            // permute index for elements from b
            const int pi = ((patb & 1) * 10 + 4) | ((((patb >> 4) & 1) * 10 + 4) << 4);
            // 2. Permute elements from b and combine with elements from a through write mask
            return _mm512_mask_shuffle_epi32(ta, sb, b, (_MM_PERM_ENUM)pi);
        }
        // not same permute pattern in all lanes. use full permute
    }
    // general case: full permute
    const __m512i pmask = constant16i<i0&0xF, 0, i1&0xF, 0, i2&0xF, 0, i3&0xF, 0, i4&0xF, 0, i5&0xF, 0, i6&0xF, 0, i7&0xF, 0>();
    if (dozero) {
        return _mm512_maskz_permutex2var_epi64(z, a, pmask, b);
    }
    else {
        return _mm512_permutex2var_epi64(a, pmask, b);
    }
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7> 
static inline Vec8uq blend8uq(Vec8uq const & a, Vec8uq const & b) {
    return Vec8uq( blend8q<i0,i1,i2,i3,i4,i5,i6,i7> (a,b));
}


template <int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15 > 
static inline Vec16i blend16i(Vec16i const & a, Vec16i const & b) {  

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
    if (mz == 0) return  _mm512_setzero_epi32();

    // special case: all from a
    if ((ms & mz) == 0) {
        return permute16i<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15> (a);
    }

    // special case: all from b
    if ((~ms & mz) == 0) {
        return permute16i<i0^16,i1^16,i2^16,i3^16,i4^16,i5^16,i6^16,i7^16,i8^16,i9^16,i10^16,i11^16,i12^16,i13^16,i14^16,i15^16 > (b);
    }

    // special case: blend without permute
    if (((m1 ^ 0xFEDCBA9876543210) & mz) == 0) {
        __mmask16 blendmask = __mmask16((i0&16)>>4 | (i1&16)>>3 | (i2&16)>>2 | (i3&16)>>1 | (i4&16) | (i5&16)<<1 | (i6&16)<<2 | (i7&16)<<3
            | (i8&16)<<4 | (i9&16)<<5 | (i10&16)<<6 | (i11&16)<<7 | (i12&16)<<8 | (i13&16)<<9 | (i14&16)<<10 | (i15&16)<<11);
        __m512i t = _mm512_mask_blend_epi32(blendmask, a, b);
        if (dozero) {
            t = _mm512_maskz_mov_epi32(z, t);
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
            // This code generates two instructions instead of one, but we are avoiding the slow lane-crossing instruction,
            // and we are saving 64 bytes of data cache.
            // 1. Permute a, zero elements not from a (using _mm512_maskz_shuffle_epi32)
            __m512i ta = permute16i< (maz&0xF)?i0&15:-1, (maz&0xF0)?i1&15:-1, (maz&0xF00)?i2&15:-1, (maz&0xF000)?i3&15:-1, 
                (maz&0xF0000)?i4&15:-1, (maz&0xF00000)?i5&15:-1, (maz&0xF000000)?i6&15:-1, (maz&0xF0000000)?i7&15:-1,
                (maz&0xF00000000)?i8&15:-1, (maz&0xF000000000)?i9&15:-1, (maz&0xF0000000000)?i10&15:-1, (maz&0xF00000000000)?i11&15:-1, 
                (maz&0xF000000000000)?i12&15:-1, (maz&0xF0000000000000)?i13&15:-1, (maz&0xF00000000000000)?i14&15:-1, (maz&0xF000000000000000)?i15&15:-1> (a);
            // write mask for elements from b
            const __mmask16 sb = ((mbz&0xF)?1:0) | ((mbz&0xF0)?0x2:0) | ((mbz&0xF00)?0x4:0) | ((mbz&0xF000)?0x8:0) | ((mbz&0xF0000)?0x10:0) | ((mbz&0xF00000)?0x20:0) | ((mbz&0xF000000)?0x40:0) | ((mbz&0xF0000000)?0x80:0) 
                | ((mbz&0xF00000000)?0x100:0) | ((mbz&0xF000000000)?0x200:0) | ((mbz&0xF0000000000)?0x400:0) | ((mbz&0xF00000000000)?0x800:0) | ((mbz&0xF000000000000)?0x1000:0) | ((mbz&0xF0000000000000)?0x2000:0) | ((mbz&0xF00000000000000)?0x4000:0) | ((mbz&0xF000000000000000)?0x8000:0);
            // permute index for elements from b
            const int pi = (patb & 3) | (((patb >> 4) & 3) << 2) | (((patb >> 8) & 3) << 4) | (((patb >> 12) & 3) << 6);
            // 2. Permute elements from b and combine with elements from a through write mask
            return _mm512_mask_shuffle_epi32(ta, sb, b, (_MM_PERM_ENUM)pi);
        }
        // not same permute pattern in all lanes. use full permute
    }

    // general case: full permute
    const __m512i pmask = constant16i<i0&0x1F, i1&0x1F, i2&0x1F, i3&0x1F, i4&0x1F, i5&0x1F, i6&0x1F, i7&0x1F, 
        i8&0x1F, i9&0x1F, i10&0x1F, i11&0x1F, i12&0x1F, i13&0x1F, i14&0x1F, i15&0x1F>();
    if (dozero) {
        return _mm512_maskz_permutex2var_epi32(z, a, pmask, b);        
    }
    else {
        return _mm512_permutex2var_epi32(a, pmask, b);
    }
}

template <int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15 > 
static inline Vec16ui blend16ui(Vec16ui const & a, Vec16ui const & b) {
    return Vec16ui( blend16i<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15> (Vec16i(a),Vec16i(b)));
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
* Vec8q a(2,0,0,6,4,3,5,0);                 // index a is (  2,   0,   0,   6,   4,   3,   5,   0)
* Vec8q b(100,101,102,103,104,105,106,107); // table b is (100, 101, 102, 103, 104, 105, 106, 107)
* Vec8q c;
* c = lookup8 (a,b);                        // c is       (102, 100, 100, 106, 104, 103, 105, 100)
*
*****************************************************************************/

static inline Vec16i lookup16(Vec16i const & index, Vec16i const & table) {
    return _mm512_permutexvar_epi32(index, table);
}

template <int n>
static inline Vec16i lookup(Vec16i const & index, void const * table) {
    if (n <= 0) return 0;
    if (n <= 16) {
        Vec16i table1 = Vec16i().load(table);
        return lookup16(index, table1);
    }
    if (n <= 32) {
        Vec16i table1 = Vec16i().load(table);
        Vec16i table2 = Vec16i().load((int8_t*)table + 64);
        return _mm512_permutex2var_epi32(table1, index, table2);
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
    return _mm512_i32gather_epi32(index1, (const int*)table, 4);
    // return  _mm512_i32gather_epi32(index1, table, _MM_UPCONV_EPI32_NONE, 4, 0);
}


static inline Vec8q lookup8(Vec8q const & index, Vec8q const & table) {
    return _mm512_permutexvar_epi64(index, table);
}

template <int n>
static inline Vec8q lookup(Vec8q const & index, void const * table) {
    if (n <= 0) return 0;
    if (n <= 8) {
        Vec8q table1 = Vec8q().load(table);
        return lookup8(index, table1);
    }
    if (n <= 16) {
        Vec8q table1 = Vec8q().load(table);
        Vec8q table2 = Vec8q().load((int8_t*)table + 64);
        return _mm512_permutex2var_epi64(table1, index, table2);
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
    return _mm512_i64gather_epi64(index1, (const long long*)table, 8);
}


/*****************************************************************************
*
*          Gather functions with fixed indexes
*
*****************************************************************************/
// Load elements from array a with indices i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7, 
int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
static inline Vec16i gather16i(void const * a) {
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
            Vec16i b = Vec16i().load((int32_t const *)a + imax-15);
            return permute16i<i0-imax+15, i1-imax+15, i2-imax+15, i3-imax+15, i4-imax+15, i5-imax+15, i6-imax+15, i7-imax+15,
                i8-imax+15, i9-imax+15, i10-imax+15, i11-imax+15, i12-imax+15, i13-imax+15, i14-imax+15, i15-imax+15> (b);
        }
        else {
            Vec16i b = Vec16i().load((int32_t const *)a + imin);
            return permute16i<i0-imin, i1-imin, i2-imin, i3-imin, i4-imin, i5-imin, i6-imin, i7-imin,
                i8-imin, i9-imin, i10-imin, i11-imin, i12-imin, i13-imin, i14-imin, i15-imin> (b);
        }
    }
    if ((i0<imin+16  || i0>imax-16)  && (i1<imin+16  || i1>imax-16)  && (i2<imin+16  || i2>imax-16)  && (i3<imin+16  || i3>imax-16)
    &&  (i4<imin+16  || i4>imax-16)  && (i5<imin+16  || i5>imax-16)  && (i6<imin+16  || i6>imax-16)  && (i7<imin+16  || i7>imax-16)    
    &&  (i8<imin+16  || i8>imax-16)  && (i9<imin+16  || i9>imax-16)  && (i10<imin+16 || i10>imax-16) && (i11<imin+16 || i11>imax-16)
    &&  (i12<imin+16 || i12>imax-16) && (i13<imin+16 || i13>imax-16) && (i14<imin+16 || i14>imax-16) && (i15<imin+16 || i15>imax-16) ) {
        // load two contiguous blocks and blend
        Vec16i b = Vec16i().load((int32_t const *)a + imin);
        Vec16i c = Vec16i().load((int32_t const *)a + imax-15);
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
        return blend16i<j0,j1,j2,j3,j4,j5,j6,j7,j8,j9,j10,j11,j12,j13,j14,j15>(b, c);
    }
    // use gather instruction
    return _mm512_i32gather_epi32(Vec16i(i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15), (const int *)a, 4);
}


template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8q gather8q(void const * a) {
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
            Vec8q b = Vec8q().load((int64_t const *)a + imax-7);
            return permute8q<i0-imax+7, i1-imax+7, i2-imax+7, i3-imax+7, i4-imax+7, i5-imax+7, i6-imax+7, i7-imax+7> (b);
        }
        else {
            Vec8q b = Vec8q().load((int64_t const *)a + imin);
            return permute8q<i0-imin, i1-imin, i2-imin, i3-imin, i4-imin, i5-imin, i6-imin, i7-imin> (b);
        }
    }
    if ((i0<imin+8 || i0>imax-8) && (i1<imin+8 || i1>imax-8) && (i2<imin+8 || i2>imax-8) && (i3<imin+8 || i3>imax-8)
    &&  (i4<imin+8 || i4>imax-8) && (i5<imin+8 || i5>imax-8) && (i6<imin+8 || i6>imax-8) && (i7<imin+8 || i7>imax-8)) {
        // load two contiguous blocks and blend
        Vec8q b = Vec8q().load((int64_t const *)a + imin);
        Vec8q c = Vec8q().load((int64_t const *)a + imax-7);
        const int j0 = i0<imin+8 ? i0-imin : 15-imax+i0;
        const int j1 = i1<imin+8 ? i1-imin : 15-imax+i1;
        const int j2 = i2<imin+8 ? i2-imin : 15-imax+i2;
        const int j3 = i3<imin+8 ? i3-imin : 15-imax+i3;
        const int j4 = i4<imin+8 ? i4-imin : 15-imax+i4;
        const int j5 = i5<imin+8 ? i5-imin : 15-imax+i5;
        const int j6 = i6<imin+8 ? i6-imin : 15-imax+i6;
        const int j7 = i7<imin+8 ? i7-imin : 15-imax+i7;
        return blend8q<j0, j1, j2, j3, j4, j5, j6, j7>(b, c);
    }
    // use gather instruction
    return _mm512_i64gather_epi64(Vec8q(i0,i1,i2,i3,i4,i5,i6,i7), (const long long *)a, 8);
}


/*****************************************************************************
*
*          Functions for conversion between integer sizes
*
*****************************************************************************/

// Extend 16-bit integers to 32-bit integers, signed and unsigned

// Function extend_to_int : extends Vec16s to Vec16i with sign extension
static inline Vec16i extend_to_int (Vec16s const & a) {
    return _mm512_cvtepi16_epi32(a);
}

// Function extend_to_int : extends Vec16us to Vec16ui with zero extension
static inline Vec16ui extend_to_int (Vec16us const & a) {
    return _mm512_cvtepu16_epi32(a);
}

// Function extend_to_int : extends Vec16c to Vec16i with sign extension
static inline Vec16i extend_to_int (Vec16c const & a) {
    return _mm512_cvtepi8_epi32(a);
}

// Function extend_to_int : extends Vec16uc to Vec16ui with zero extension
static inline Vec16ui extend_to_int (Vec16uc const & a) {
    return _mm512_cvtepu8_epi32(a);
}


// Extend 32-bit integers to 64-bit integers, signed and unsigned

// Function extend_low : extends the low 8 elements to 64 bits with sign extension
static inline Vec8q extend_low (Vec16i const & a) {
    return _mm512_cvtepi32_epi64(a.get_low());
}

// Function extend_high : extends the high 8 elements to 64 bits with sign extension
static inline Vec8q extend_high (Vec16i const & a) {
    return _mm512_cvtepi32_epi64(a.get_high());
}

// Function extend_low : extends the low 8 elements to 64 bits with zero extension
static inline Vec8uq extend_low (Vec16ui const & a) {
    return _mm512_cvtepu32_epi64(a.get_low());
}

// Function extend_high : extends the high 8 elements to 64 bits with zero extension
static inline Vec8uq extend_high (Vec16ui const & a) {
    return _mm512_cvtepu32_epi64(a.get_high());
}


// Compress 32-bit integers to 8-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Overflow wraps around
static inline Vec16c compress_to_int8 (Vec16i const & a) {
    return _mm512_cvtepi32_epi8(a);
}

static inline Vec16s compress_to_int16 (Vec16i const & a) {
    return _mm512_cvtepi32_epi16(a);
}

// with signed saturation
static inline Vec16c compress_to_int8_saturated (Vec16i const & a) {
    return _mm512_cvtsepi32_epi8(a);
}

static inline Vec16s compress_to_int16_saturated (Vec16i const & a) {
    return _mm512_cvtsepi32_epi16(a);
}

// with unsigned saturation
static inline Vec16uc compress_to_int8_saturated (Vec16ui const & a) {
    return _mm512_cvtusepi32_epi8(a);
}

static inline Vec16us compress_to_int16_saturated (Vec16ui const & a) {
    return _mm512_cvtusepi32_epi16(a);
}

// Compress 64-bit integers to 32-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Overflow wraps around
static inline Vec16i compress (Vec8q const & low, Vec8q const & high) {
    Vec8i low2   = _mm512_cvtepi64_epi32(low);
    Vec8i high2  = _mm512_cvtepi64_epi32(high);
    return Vec16i(low2, high2);
}

// Function compress_saturated : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Signed, with saturation
static inline Vec16i compress_saturated (Vec8q const & low, Vec8q const & high) {
    Vec8i low2   = _mm512_cvtsepi64_epi32(low);
    Vec8i high2  = _mm512_cvtsepi64_epi32(high);
    return Vec16i(low2, high2);
}

// Function compress_saturated : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Unsigned, with saturation
static inline Vec16ui compress_saturated (Vec8uq const & low, Vec8uq const & high) {
    Vec8ui low2   = _mm512_cvtusepi64_epi32(low);
    Vec8ui high2  = _mm512_cvtusepi64_epi32(high);
    return Vec16ui(low2, high2);
}


/*****************************************************************************
*
*          Integer division operators
*
*          Please see the file vectori128.h for explanation.
*
*****************************************************************************/

// vector operator / : divide each element by divisor

// vector of 16 32-bit signed integers
static inline Vec16i operator / (Vec16i const & a, Divisor_i const & d) {
    __m512i m   = _mm512_broadcast_i32x4(d.getm());        // broadcast multiplier
    __m512i sgn = _mm512_broadcast_i32x4(d.getsign());     // broadcast sign of d
    __m512i t1  = _mm512_mul_epi32(a,m);                   // 32x32->64 bit signed multiplication of even elements of a
    __m512i t3  = _mm512_srli_epi64(a,32);                 // get odd elements of a into position for multiplication
    __m512i t4  = _mm512_mul_epi32(t3,m);                  // 32x32->64 bit signed multiplication of odd elements
    __m512i t2  = _mm512_srli_epi64(t1,32);                // dword of even index results
    __m512i t7  = _mm512_mask_mov_epi32(t2, __mmask16(0xAAAA), t4);  // blend two results
    __m512i t8  = _mm512_add_epi32(t7,a);                  // add
    __m512i t9  = _mm512_sra_epi32(t8,d.gets1());          // shift right artihmetic
    __m512i t10 = _mm512_srai_epi32(a,31);                 // sign of a
    __m512i t11 = _mm512_sub_epi32(t10,sgn);               // sign of a - sign of d
    __m512i t12 = _mm512_sub_epi32(t9,t11);                // + 1 if a < 0, -1 if d < 0
    return        _mm512_xor_si512(t12,sgn);               // change sign if divisor negative
}

// vector of 16 32-bit unsigned integers
static inline Vec16ui operator / (Vec16ui const & a, Divisor_ui const & d) {
    __m512i m   = _mm512_broadcast_i32x4(d.getm());       // broadcast multiplier
    __m512i t1  = _mm512_mul_epu32(a,m);                   // 32x32->64 bit unsigned multiplication of even elements of a
    __m512i t3  = _mm512_srli_epi64(a,32);                 // get odd elements of a into position for multiplication
    __m512i t4  = _mm512_mul_epu32(t3,m);                  // 32x32->64 bit unsigned multiplication of odd elements
    __m512i t2  = _mm512_srli_epi64(t1,32);                // high dword of even index results
    __m512i t7  = _mm512_mask_mov_epi32(t2, __mmask16(0xAAAA), t4);  // blend two results
    __m512i t8  = _mm512_sub_epi32(a,t7);                  // subtract
    __m512i t9  = _mm512_srl_epi32(t8,d.gets1());          // shift right logical
    __m512i t10 = _mm512_add_epi32(t7,t9);                 // add
    return        _mm512_srl_epi32(t10,d.gets2());         // shift right logical 
}

// vector operator /= : divide
static inline Vec16i & operator /= (Vec16i & a, Divisor_i const & d) {
    a = a / d;
    return a;
}

// vector operator /= : divide
static inline Vec16ui & operator /= (Vec16ui & a, Divisor_ui const & d) {
    a = a / d;
    return a;
}


/*****************************************************************************
*
*          Integer division 2: divisor is a compile-time constant
*
*****************************************************************************/

// Divide Vec16i by compile-time constant
template <int32_t d>
static inline Vec16i divide_by_i(Vec16i const & x) {
    Static_error_check<(d!=0)> Dividing_by_zero;                     // Error message if dividing by zero
    if (d ==  1) return  x;
    if (d == -1) return -x;
    if (uint32_t(d) == 0x80000000u) {
        return _mm512_maskz_set1_epi32(x == Vec16i(0x80000000), 1);  // avoid overflow of abs(d). return (x == 0x80000000) ? 1 : 0;
    }
    const uint32_t d1 = d > 0 ? uint32_t(d) : -uint32_t(d);          // compile-time abs(d). (force GCC compiler to treat d as 32 bits, not 64 bits)
    if ((d1 & (d1-1)) == 0) {
        // d1 is a power of 2. use shift
        const int k = bit_scan_reverse_const(d1);
        __m512i sign;
        if (k > 1) sign = _mm512_srai_epi32(x, k-1); else sign = x;  // k copies of sign bit
        __m512i bias    = _mm512_srli_epi32(sign, 32-k);             // bias = x >= 0 ? 0 : k-1
        __m512i xpbias  = _mm512_add_epi32 (x, bias);                // x + bias
        __m512i q       = _mm512_srai_epi32(xpbias, k);              // (x + bias) >> k
        if (d > 0)      return q;                                    // d > 0: return  q
        return _mm512_sub_epi32(_mm512_setzero_epi32(), q);          // d < 0: return -q

    }
    // general case
    const int32_t sh = bit_scan_reverse_const(uint32_t(d1)-1);       // ceil(log2(d1)) - 1. (d1 < 2 handled by power of 2 case)
    const int32_t mult = int(1 + (uint64_t(1) << (32+sh)) / uint32_t(d1) - (int64_t(1) << 32));   // multiplier
    const Divisor_i div(mult, sh, d < 0 ? -1 : 0);
    return x / div;
}

// define Vec8i a / const_int(d)
template <int32_t d>
static inline Vec16i operator / (Vec16i const & a, Const_int_t<d>) {
    return divide_by_i<d>(a);
}

// define Vec16i a / const_uint(d)
template <uint32_t d>
static inline Vec16i operator / (Vec16i const & a, Const_uint_t<d>) {
    Static_error_check< (d<0x80000000u) > Error_overflow_dividing_signed_by_unsigned; // Error: dividing signed by overflowing unsigned
    return divide_by_i<int32_t(d)>(a);                               // signed divide
}

// vector operator /= : divide
template <int32_t d>
static inline Vec16i & operator /= (Vec16i & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec16i & operator /= (Vec16i & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}


// Divide Vec16ui by compile-time constant
template <uint32_t d>
static inline Vec16ui divide_by_ui(Vec16ui const & x) {
    Static_error_check<(d!=0)> Dividing_by_zero;                     // Error message if dividing by zero
    if (d == 1) return x;                                            // divide by 1
    const int b = bit_scan_reverse_const(d);                         // floor(log2(d))
    if ((uint32_t(d) & (uint32_t(d)-1)) == 0) {
        // d is a power of 2. use shift
        return  _mm512_srli_epi32(x, b);                             // x >> b
    }
    // general case (d > 2)
    uint32_t mult = uint32_t((uint64_t(1) << (b+32)) / d);           // multiplier = 2^(32+b) / d
    const uint64_t rem = (uint64_t(1) << (b+32)) - uint64_t(d)*mult; // remainder 2^(32+b) % d
    const bool round_down = (2*rem < d);                             // check if fraction is less than 0.5
    if (!round_down) {
        mult = mult + 1;                                             // round up mult
    }
    // do 32*32->64 bit unsigned multiplication and get high part of result
    const __m512i multv = Vec16ui(uint64_t(mult));                   // zero-extend mult and broadcast
    __m512i t1 = _mm512_mul_epu32(x,multv);                          // 32x32->64 bit unsigned multiplication of even elements
    if (round_down) {
        t1      = _mm512_add_epi64(t1,multv);                        // compensate for rounding error. (x+1)*m replaced by x*m+m to avoid overflow
    }
    __m512i t2 = _mm512_srli_epi64(t1,32);                           // high dword of result 0 and 2
    __m512i t3 = _mm512_srli_epi64(x,32);                            // get odd elements into position for multiplication
    __m512i t4 = _mm512_mul_epu32(t3,multv);                         // 32x32->64 bit unsigned multiplication of x[1] and x[3]
    if (round_down) {
        t4      = _mm512_add_epi64(t4,multv);                        // compensate for rounding error. (x+1)*m replaced by x*m+m to avoid overflow
    }
    __m512i t7 = _mm512_mask_mov_epi32(t2, __mmask16(0xAA), t4);     // blend two results
    Vec16ui q  = _mm512_srli_epi32(t7, b);                           // shift right by b
    return q;                                                        // no overflow possible
}

// define Vec8ui a / const_uint(d)
template <uint32_t d>
static inline Vec16ui operator / (Vec16ui const & a, Const_uint_t<d>) {
    return divide_by_ui<d>(a);
}

// define Vec8ui a / const_int(d)
template <int32_t d>
static inline Vec16ui operator / (Vec16ui const & a, Const_int_t<d>) {
    Static_error_check< (d>=0) > Error_dividing_unsigned_by_negative;// Error: dividing unsigned by negative is ambiguous
    return divide_by_ui<d>(a);                                       // unsigned divide
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec16ui & operator /= (Vec16ui & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <int32_t d>
static inline Vec16ui & operator /= (Vec16ui & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}

/*****************************************************************************
*
*          Horizontal scan functions
*
*****************************************************************************/

// Get index to the first element that is true. Return -1 if all are false

static inline int horizontal_find_first(Vec16ib const & x) {
    uint32_t b = uint16_t(__mmask16(x));
    if (b) {
        return bit_scan_forward(b);
    }
    else {
        return -1;
    }
}

static inline int horizontal_find_first(Vec8qb const & x) {
    uint32_t b = uint8_t(__mmask8(x));
    if (b) {
        return bit_scan_forward(b);
    }
    else {
        return -1;
    }
}

static inline uint32_t horizontal_count(Vec16ib const & x) {
    return vml_popcnt(uint32_t(uint16_t(__mmask16(x))));
}

static inline uint32_t horizontal_count(Vec8qb const & x) {
    return vml_popcnt(uint32_t(uint16_t(__mmask16(x))));
}


/*****************************************************************************
*
*          Boolean <-> bitfield conversion functions
*
*****************************************************************************/

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec4ib x) {
    __m512i a = _mm512_castsi128_si512(x);
    __mmask16 b = _mm512_mask_testn_epi32_mask(0xF, a, a);
    return uint8_t(b) ^ 0xF;
}

// to_Vec16c: convert integer bitfield to boolean vector
static inline Vec4ib to_Vec4ib(uint8_t x) {
    return _mm512_castsi512_si128(_mm512_maskz_set1_epi32(__mmask16(x), -1));
}

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec2qb x) {
    __m512i a = _mm512_castsi128_si512(x);
    __mmask16 b = _mm512_mask_testn_epi64_mask(0x3, a, a);
    return uint8_t(b) ^ 0x3;
}

// to_Vec16c: convert integer bitfield to boolean vector
static inline Vec2qb to_Vec2qb(uint8_t x) {
    return _mm512_castsi512_si128(_mm512_maskz_set1_epi64(__mmask16(x), -1LL));
}

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec8ib x) {
    __m512i a = _mm512_castsi256_si512(x);
    __mmask16 b = _mm512_mask_testn_epi32_mask(0xFF, a, a);
    return ~ uint8_t(b);
}

// to_Vec16c: convert integer bitfield to boolean vector
static inline Vec8ib to_Vec8ib(uint8_t x) {
    return _mm512_castsi512_si256(_mm512_maskz_set1_epi32(__mmask16(x), -1));
}

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec4qb x) {
    __m512i a = _mm512_castsi256_si512(x);
    __mmask16 b = _mm512_mask_testn_epi64_mask(0xF, a, a);
    return uint8_t(b) ^ 0xF;
}

// to_Vec16c: convert integer bitfield to boolean vector
static inline Vec4qb to_Vec4qb(uint8_t x) {
    return _mm512_castsi512_si256(_mm512_maskz_set1_epi64(__mmask16(x), -1LL));
}


// to_bits: convert to integer bitfield
static inline uint16_t to_bits(Vec16b a) {
    return (uint16_t)(__mmask16)a;
}

// to_Vec16b: convert integer bitfield to boolean vector
static inline Vec16b to_Vec16b(uint16_t x) {
    return (__mmask16)x;
}

// to_Vec16ib: convert integer bitfield to boolean vector
static inline Vec16ib to_Vec16ib(uint16_t x) {
    return to_Vec16b(x);
}

// to_Vec8b: convert integer bitfield to boolean vector
static inline Vec8qb to_Vec8qb(uint8_t x) {
    return (__mmask8)x;
}

#endif // VECTORI512_H
