/****************************  vectori128.h   *******************************
* Author:        Agner Fog
* Date created:  2012-05-30
* Last modified: 2014-10-24
* Version:       1.16
* Project:       vector classes
* Description:
* Header file defining integer vector classes as interface to intrinsic 
* functions in x86 microprocessors with SSE2 and later instruction sets
* up to AVX.
*
* Instructions:
* Use Gnu, Intel or Microsoft C++ compiler. Compile for the desired 
* instruction set, which must be at least SSE2. Specify the supported 
* instruction set by a command line define, e.g. __SSE4_1__ if the 
* compiler does not automatically do so.
*
* The following vector classes are defined here:
* Vec128b   Vector of 128  1-bit unsigned  integers or Booleans
* Vec16c    Vector of  16  8-bit signed    integers
* Vec16uc   Vector of  16  8-bit unsigned  integers
* Vec16cb   Vector of  16  Booleans for use with Vec16c and Vec16uc
* Vec8s     Vector of   8  16-bit signed   integers
* Vec8us    Vector of   8  16-bit unsigned integers
* Vec8sb    Vector of   8  Booleans for use with Vec8s and Vec8us
* Vec4i     Vector of   4  32-bit signed   integers
* Vec4ui    Vector of   4  32-bit unsigned integers
* Vec4ib    Vector of   4  Booleans for use with Vec4i and Vec4ui
* Vec2q     Vector of   2  64-bit signed   integers
* Vec2uq    Vector of   2  64-bit unsigned integers
* Vec2qb    Vector of   2  Booleans for use with Vec2q and Vec2uq
*
* Each vector object is represented internally in the CPU as a 128-bit register.
* This header file defines operators and functions for these vectors.
*
* For example:
* Vec4i a(1,2,3,4), b(5,6,7,8), c;
* c = a + b;     // now c contains (6,8,10,12)
*
* For detailed instructions, see VectorClass.pdf
*
* (c) Copyright 2012 - 2013 GNU General Public License http://www.gnu.org/licenses
*****************************************************************************/
#ifndef VECTORI128_H
#define VECTORI128_H

#include "instrset.h"  // Select supported instruction set

#if INSTRSET < 2   // SSE2 required
#error Please compile for the SSE2 instruction set or higher
#endif



/*****************************************************************************
*
*          Vector of 128 1-bit unsigned integers or Booleans
*
*****************************************************************************/
class Vec128b {
protected:
    __m128i xmm; // Integer vector
public:
    // Default constructor:
    Vec128b() {
    }
    // Constructor to broadcast the same value into all elements
    // Removed because of undesired implicit conversions
    // Vec128b(int i) {
    //     xmm = _mm_set1_epi32(-(i & 1));}

    // Constructor to convert from type __m128i used in intrinsics:
    Vec128b(__m128i const & x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec128b & operator = (__m128i const & x) {
        xmm = x;
        return *this;
    }
    // Type cast operator to convert to __m128i used in intrinsics
    operator __m128i() const {
        return xmm;
    }
    // Member function to load from array (unaligned)
    Vec128b & load(void const * p) {
        xmm = _mm_loadu_si128((__m128i const*)p);
        return *this;
    }
    // Member function to load from array, aligned by 16
    // "load_a" is faster than "load" on older Intel processors (Pentium 4, Pentium M, Core 1,
    // Merom, Wolfdale) and Atom, but not on other processors from Intel, AMD or VIA.
    // You may use load_a instead of load if you are certain that p points to an address
    // divisible by 16.
    void load_a(void const * p) {
        xmm = _mm_load_si128((__m128i const*)p);
    }
    // Member function to store into array (unaligned)
    void store(void * p) const {
        _mm_storeu_si128((__m128i*)p, xmm);
    }
    // Member function to store into array, aligned by 16
    // "store_a" is faster than "store" on older Intel processors (Pentium 4, Pentium M, Core 1,
    // Merom, Wolfdale) and Atom, but not on other processors from Intel, AMD or VIA.
    // You may use store_a instead of store if you are certain that p points to an address
    // divisible by 16.
    void store_a(void * p) const {
        _mm_store_si128((__m128i*)p, xmm);
    }
    // Member function to change a single bit
    // Note: This function is inefficient. Use load function if changing more than one bit
    Vec128b const & set_bit(uint32_t index, int value) {
        static const union {
            uint64_t i[4];
            __m128i  x[2];
        } u = {{1,0,0,1}};                 // 2 vectors with bit 0 and 64 set, respectively
        int w = (index >> 6) & 1;          // qword index
        int bi = index & 0x3F;             // bit index within qword w
        __m128i mask = u.x[w];
        mask = _mm_sll_epi64(mask,_mm_cvtsi32_si128(bi)); // mask with bit number b set
        if (value & 1) {
            xmm = _mm_or_si128(mask,xmm);
        }
        else {
            xmm = _mm_andnot_si128(mask,xmm);
        }
        return *this;
    }
    // Member function to get a single bit
    // Note: This function is inefficient. Use store function if reading more than one bit
    int get_bit(uint32_t index) const {
        union {
            __m128i x;
            uint8_t i[16];
        } u;
        u.x = xmm; 
        int w = (index >> 3) & 0xF;            // byte index
        int bi = index & 7;                    // bit index within byte w
        return (u.i[w] >> bi) & 1;
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    bool operator [] (uint32_t index) const {
        return get_bit(index) != 0;
    }
    static int size() {
        return 128;
    }
};


// Define operators for this class

// vector operator & : bitwise and
static inline Vec128b operator & (Vec128b const & a, Vec128b const & b) {
    return _mm_and_si128(a, b);
}
static inline Vec128b operator && (Vec128b const & a, Vec128b const & b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec128b operator | (Vec128b const & a, Vec128b const & b) {
    return _mm_or_si128(a, b);
}
static inline Vec128b operator || (Vec128b const & a, Vec128b const & b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec128b operator ^ (Vec128b const & a, Vec128b const & b) {
    return _mm_xor_si128(a, b);
}

// vector operator ~ : bitwise not
static inline Vec128b operator ~ (Vec128b const & a) {
    return _mm_xor_si128(a, _mm_set1_epi32(-1));
}

// vector operator &= : bitwise and
static inline Vec128b & operator &= (Vec128b & a, Vec128b const & b) {
    a = a & b;
    return a;
}

// vector operator |= : bitwise or
static inline Vec128b & operator |= (Vec128b & a, Vec128b const & b) {
    a = a | b;
    return a;
}

// vector operator ^= : bitwise xor
static inline Vec128b & operator ^= (Vec128b & a, Vec128b const & b) {
    a = a ^ b;
    return a;
}

// Define functions for this class

// function andnot: a & ~ b
static inline Vec128b andnot (Vec128b const & a, Vec128b const & b) {
    return _mm_andnot_si128(b, a);
}


/*****************************************************************************
*
*          Generate compile-time constant vector
*
*****************************************************************************/
// Generate a constant vector of 4 integers stored in memory.
// Can be converted to any integer vector type
template <int i0, int i1, int i2, int i3>
static inline __m128i constant4i() {
    static const union {
        int     i[4];
        __m128i xmm;
    } u = {{i0,i1,i2,i3}};
    return u.xmm;
}


/*****************************************************************************
*
*          selectb function
*
*****************************************************************************/
// Select between two sources, byte by byte. Used in various functions and operators
// Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or 0xFF (true). No other values are allowed.
// The implementation depends on the instruction set: 
// If SSE4.1 is supported then only bit 7 in each byte of s is checked, 
// otherwise all bits in s are used.
static inline __m128i selectb (__m128i const & s, __m128i const & a, __m128i const & b) {
#if INSTRSET >= 5   // SSE4.1 supported
    return _mm_blendv_epi8 (b, a, s);
#else
    return _mm_or_si128(
        _mm_and_si128(s,a),
        _mm_andnot_si128(s,b));
#endif
}



/*****************************************************************************
*
*          Horizontal Boolean functions
*
*****************************************************************************/

// horizontal_and. Returns true if all bits are 1
static inline bool horizontal_and (Vec128b const & a) {
#if INSTRSET >= 5   // SSE4.1 supported. Use PTEST
    return _mm_testc_si128(a,constant4i<-1,-1,-1,-1>()) != 0;
#else
    __m128i t1 = _mm_unpackhi_epi64(a,a);                  // get 64 bits down
    __m128i t2 = _mm_and_si128(a,t1);                      // and 64 bits
#ifdef __x86_64__
    int64_t t5 = _mm_cvtsi128_si64(t2);                    // transfer 64 bits to integer
    return  t5 == int64_t(-1);
#else
    __m128i t3 = _mm_srli_epi64(t2,32);                    // get 32 bits down
    __m128i t4 = _mm_and_si128(t2,t3);                     // and 32 bits
    int     t5 = _mm_cvtsi128_si32(t4);                    // transfer 32 bits to integer
    return  t5 == -1;
#endif  // __x86_64__
#endif  // INSTRSET
}

// horizontal_or. Returns true if at least one bit is 1
static inline bool horizontal_or (Vec128b const & a) {
#if INSTRSET >= 5   // SSE4.1 supported. Use PTEST
    return ! _mm_testz_si128(a,a);
#else
    __m128i t1 = _mm_unpackhi_epi64(a,a);                  // get 64 bits down
    __m128i t2 = _mm_or_si128(a,t1);                       // and 64 bits
#ifdef __x86_64__
    int64_t t5 = _mm_cvtsi128_si64(t2);                    // transfer 64 bits to integer
    return  t5 != int64_t(0);
#else
    __m128i t3 = _mm_srli_epi64(t2,32);                    // get 32 bits down
    __m128i t4 = _mm_or_si128(t2,t3);                      // and 32 bits
    int     t5 = _mm_cvtsi128_si32(t4);                    // transfer to integer
    return  t5 != 0;
#endif  // __x86_64__
#endif  // INSTRSET
}



/*****************************************************************************
*
*          Vector of 16 8-bit signed integers
*
*****************************************************************************/

class Vec16c : public Vec128b {
public:
    // Default constructor:
    Vec16c() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec16c(int i) {
        xmm = _mm_set1_epi8((char)i);
    }
    // Constructor to build from all elements:
    Vec16c(int8_t i0, int8_t i1, int8_t i2, int8_t i3, int8_t i4, int8_t i5, int8_t i6, int8_t i7,
        int8_t i8, int8_t i9, int8_t i10, int8_t i11, int8_t i12, int8_t i13, int8_t i14, int8_t i15) {
        xmm = _mm_setr_epi8(i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15);
    }
    // Constructor to convert from type __m128i used in intrinsics:
    Vec16c(__m128i const & x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec16c & operator = (__m128i const & x) {
        xmm = x;
        return *this;
    }
    // Type cast operator to convert to __m128i used in intrinsics
    operator __m128i() const {
        return xmm;
    }
    // Member function to load from array (unaligned)
    Vec16c & load(void const * p) {
        xmm = _mm_loadu_si128((__m128i const*)p);
        return *this;
    }
    // Member function to load from array (aligned)
    Vec16c & load_a(void const * p) {
        xmm = _mm_load_si128((__m128i const*)p);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec16c & load_partial(int n, void const * p) {
        if      (n >= 16) load(p);
        else if (n <= 0)  *this = 0;
        else if (((int)(intptr_t)p & 0xFFF) < 0xFF0) {
            // p is at least 16 bytes from a page boundary. OK to read 16 bytes
            load(p);
        }
        else {
            // worst case. read 1 byte at a time and suffer store forwarding penalty
            char x[16];
            for (int i = 0; i < n; i++) x[i] = ((char *)p)[i];
            load(x);
        }
        cutoff(n);
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
        if (n >= 16) {
            store(p);
            return;
        }
        if (n <= 0) return;
        // we are not using _mm_maskmoveu_si128 because it is too slow on many processors
        union {        
            int8_t  c[16];
            int16_t s[8];
            int32_t i[4];
            int64_t q[2];
        } u;
        store(u.c);
        int j = 0;
        if (n & 8) {
            *(int64_t*)p = u.q[0];
            j += 8;
        }
        if (n & 4) {
            ((int32_t*)p)[j/4] = u.i[j/4];
            j += 4;
        }
        if (n & 2) {
            ((int16_t*)p)[j/2] = u.s[j/2];
            j += 2;
        }
        if (n & 1) {
            ((int8_t*)p)[j]    = u.c[j];
        }
    }
    // cut off vector to n elements. The last 16-n elements are set to zero
    Vec16c & cutoff(int n) {
        if (uint32_t(n) >= 16) return *this;
        static const char mask[32] = {-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
        *this &= Vec16c().load(mask+16-n);
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec16c const & insert(uint32_t index, int8_t value) {
        static const int8_t maskl[32] = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            -1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
        __m128i broad = _mm_set1_epi8(value);  // broadcast value into all elements
        __m128i mask  = _mm_loadu_si128((__m128i const*)(maskl+16-(index & 0x0F))); // mask with FF at index position
        xmm = selectb(mask,broad,xmm);
        return *this;
    }
    // Member function extract a single element from vector
    int8_t extract(uint32_t index) const {
        int8_t x[16];
        store(x);
        return x[index & 0x0F];
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int8_t operator [] (uint32_t index) const {
        return extract(index);
    }
    static int size() {
        return 16;
    }
};

/*****************************************************************************
*
*          Vec16cb: Vector of 16 Booleans for use with Vec16c and Vec16uc
*
*****************************************************************************/

class Vec16cb : public Vec16c {
public:
    // Default constructor
    Vec16cb() {}
    // Constructor to build from all elements:
    Vec16cb(bool x0, bool x1, bool x2, bool x3, bool x4, bool x5, bool x6, bool x7,
        bool x8, bool x9, bool x10, bool x11, bool x12, bool x13, bool x14, bool x15) {
        xmm = Vec16c(-int8_t(x0), -int8_t(x1), -int8_t(x2), -int8_t(x3), -int8_t(x4), -int8_t(x5), -int8_t(x6), -int8_t(x7), 
            -int8_t(x8), -int8_t(x9), -int8_t(x10), -int8_t(x11), -int8_t(x12), -int8_t(x13), -int8_t(x14), -int8_t(x15));
    }
    // Constructor to convert from type __m128i used in intrinsics:
    Vec16cb(__m128i const & x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec16cb & operator = (__m128i const & x) {
        xmm = x;
        return *this;
    }
    // Constructor to broadcast scalar value:
    Vec16cb(bool b) : Vec16c(-int8_t(b)) {
    }
    // Assignment operator to broadcast scalar value:
    Vec16cb & operator = (bool b) {
        *this = Vec16cb(b);
        return *this;
    }
private: // Prevent constructing from int, etc.
    Vec16cb(int b);
    Vec16cb & operator = (int x);
public:
    Vec16cb & insert (int index, bool a) {
        Vec16c::insert(index, -(int)a);
        return *this;
    }
    // Member function extract a single element from vector
    bool extract(uint32_t index) const {
        return Vec16c::extract(index) != 0;
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    bool operator [] (uint32_t index) const {
        return extract(index);
    }
};


/*****************************************************************************
*
*          Define operators for Vec16cb
*
*****************************************************************************/

// vector operator & : bitwise and
static inline Vec16cb operator & (Vec16cb const & a, Vec16cb const & b) {
    return Vec16cb(Vec128b(a) & Vec128b(b));
}
static inline Vec16cb operator && (Vec16cb const & a, Vec16cb const & b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec16cb & operator &= (Vec16cb & a, Vec16cb const & b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec16cb operator | (Vec16cb const & a, Vec16cb const & b) {
    return Vec16cb(Vec128b(a) | Vec128b(b));
}
static inline Vec16cb operator || (Vec16cb const & a, Vec16cb const & b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec16cb & operator |= (Vec16cb & a, Vec16cb const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec16cb operator ^ (Vec16cb const & a, Vec16cb const & b) {
    return Vec16cb(Vec128b(a) ^ Vec128b(b));
}
// vector operator ^= : bitwise xor
static inline Vec16cb & operator ^= (Vec16cb & a, Vec16cb const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec16cb operator ~ (Vec16cb const & a) {
    return Vec16cb( ~ Vec128b(a));
}

// vector operator ! : element not
static inline Vec16cb operator ! (Vec16cb const & a) {
    return ~ a;
}

// vector function andnot
static inline Vec16cb andnot (Vec16cb const & a, Vec16cb const & b) {
    return Vec16cb(andnot(Vec128b(a), Vec128b(b)));
}


/*****************************************************************************
*
*          Define operators for Vec16c
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec16c operator + (Vec16c const & a, Vec16c const & b) {
    return _mm_add_epi8(a, b);
}

// vector operator += : add
static inline Vec16c & operator += (Vec16c & a, Vec16c const & b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec16c operator ++ (Vec16c & a, int) {
    Vec16c a0 = a;
    a = a + 1;
    return a0;
}

// prefix operator ++
static inline Vec16c & operator ++ (Vec16c & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec16c operator - (Vec16c const & a, Vec16c const & b) {
    return _mm_sub_epi8(a, b);
}

// vector operator - : unary minus
static inline Vec16c operator - (Vec16c const & a) {
    return _mm_sub_epi8(_mm_setzero_si128(), a);
}

// vector operator -= : add
static inline Vec16c & operator -= (Vec16c & a, Vec16c const & b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec16c operator -- (Vec16c & a, int) {
    Vec16c a0 = a;
    a = a - 1;
    return a0;
}

// prefix operator --
static inline Vec16c & operator -- (Vec16c & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec16c operator * (Vec16c const & a, Vec16c const & b) {
    // There is no 8-bit multiply in SSE2. Split into two 16-bit multiplies
    __m128i aodd    = _mm_srli_epi16(a,8);                 // odd numbered elements of a
    __m128i bodd    = _mm_srli_epi16(b,8);                 // odd numbered elements of b
    __m128i muleven = _mm_mullo_epi16(a,b);                // product of even numbered elements
    __m128i mulodd  = _mm_mullo_epi16(aodd,bodd);          // product of odd  numbered elements
            mulodd  = _mm_slli_epi16(mulodd,8);            // put odd numbered elements back in place
    __m128i mask    = _mm_set1_epi32(0x00FF00FF);          // mask for even positions
    __m128i product = selectb(mask,muleven,mulodd);        // interleave even and odd
    return product;
}

// vector operator *= : multiply
static inline Vec16c & operator *= (Vec16c & a, Vec16c const & b) {
    a = a * b;
    return a;
}

// vector operator << : shift left all elements
static inline Vec16c operator << (Vec16c const & a, int b) {
    uint32_t mask = (uint32_t)0xFF >> (uint32_t)b;         // mask to remove bits that are shifted out
    __m128i am    = _mm_and_si128(a,_mm_set1_epi8((char)mask));  // remove bits that will overflow
    __m128i res   = _mm_sll_epi16(am,_mm_cvtsi32_si128(b));// 16-bit shifts
    return res;
}

// vector operator <<= : shift left
static inline Vec16c & operator <<= (Vec16c & a, int b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic all elements
static inline Vec16c operator >> (Vec16c const & a, int b) {
    __m128i aeven = _mm_slli_epi16(a,8);                   // even numbered elements of a. get sign bit in position
            aeven = _mm_sra_epi16(aeven,_mm_cvtsi32_si128(b+8)); // shift arithmetic, back to position
    __m128i aodd  = _mm_sra_epi16(a,_mm_cvtsi32_si128(b)); // shift odd numbered elements arithmetic
    __m128i mask    = _mm_set1_epi32(0x00FF00FF);          // mask for even positions
    __m128i res     = selectb(mask,aeven,aodd);            // interleave even and odd
    return res;
}

// vector operator >>= : shift right arithmetic
static inline Vec16c & operator >>= (Vec16c & a, int b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec16cb operator == (Vec16c const & a, Vec16c const & b) {
    return _mm_cmpeq_epi8(a,b);
}

// vector operator != : returns true for elements for which a != b
static inline Vec16cb operator != (Vec16c const & a, Vec16c const & b) {
#ifdef __XOP__  // AMD XOP instruction set
    return _mm_comneq_epi8(a,b);
#else  // SSE2 instruction set
    return Vec16cb(Vec16c(~(a == b)));
#endif
}

// vector operator > : returns true for elements for which a > b (signed)
static inline Vec16cb operator > (Vec16c const & a, Vec16c const & b) {
    return _mm_cmpgt_epi8(a,b);
}

// vector operator < : returns true for elements for which a < b (signed)
static inline Vec16cb operator < (Vec16c const & a, Vec16c const & b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec16cb operator >= (Vec16c const & a, Vec16c const & b) {
#ifdef __XOP__  // AMD XOP instruction set
    return _mm_comge_epi8(a,b);
#else  // SSE2 instruction set
    return Vec16cb(Vec16c(~(b > a)));
#endif
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec16cb operator <= (Vec16c const & a, Vec16c const & b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec16c operator & (Vec16c const & a, Vec16c const & b) {
    return Vec16c(Vec128b(a) & Vec128b(b));
}
static inline Vec16c operator && (Vec16c const & a, Vec16c const & b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec16c & operator &= (Vec16c & a, Vec16c const & b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec16c operator | (Vec16c const & a, Vec16c const & b) {
    return Vec16c(Vec128b(a) | Vec128b(b));
}
static inline Vec16c operator || (Vec16c const & a, Vec16c const & b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec16c & operator |= (Vec16c & a, Vec16c const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec16c operator ^ (Vec16c const & a, Vec16c const & b) {
    return Vec16c(Vec128b(a) ^ Vec128b(b));
}
// vector operator ^= : bitwise xor
static inline Vec16c & operator ^= (Vec16c & a, Vec16c const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec16c operator ~ (Vec16c const & a) {
    return Vec16c( ~ Vec128b(a));
}

// vector operator ! : logical not, returns true for elements == 0
static inline Vec16cb operator ! (Vec16c const & a) {
    return _mm_cmpeq_epi8(a,_mm_setzero_si128());
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or -1 (true). No other values are allowed.
static inline Vec16c select (Vec16cb const & s, Vec16c const & a, Vec16c const & b) {
    return selectb(s,a,b);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec16c if_add (Vec16cb const & f, Vec16c const & a, Vec16c const & b) {
    return a + (Vec16c(f) & b);
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
static inline int32_t horizontal_add (Vec16c const & a) {
    __m128i sum1 = _mm_sad_epu8(a,_mm_setzero_si128());
    __m128i sum2 = _mm_shuffle_epi32(sum1,2);
    __m128i sum3 = _mm_add_epi16(sum1,sum2);
    int8_t  sum4 = (int8_t)_mm_cvtsi128_si32(sum3);        // truncate to 8 bits
    return  sum4;                                          // sign extend to 32 bits
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Each element is sign-extended before addition to avoid overflow
static inline int32_t horizontal_add_x (Vec16c const & a) {
#ifdef __XOP__       // AMD XOP instruction set
    __m128i sum1  = _mm_haddq_epi8(a);
    __m128i sum2  = _mm_shuffle_epi32(sum1,0x0E);          // high element
    __m128i sum3  = _mm_add_epi32(sum1,sum2);              // sum
    return          _mm_cvtsi128_si32(sum3);
#elif  INSTRSET >= 4  // SSSE3
    __m128i aeven = _mm_slli_epi16(a,8);                   // even numbered elements of a. get sign bit in position
            aeven = _mm_srai_epi16(aeven,8);               // sign extend even numbered elements
    __m128i aodd  = _mm_srai_epi16(a,8);                   // sign extend odd  numbered elements
    __m128i sum1  = _mm_add_epi16(aeven,aodd);             // add even and odd elements
    __m128i sum2  = _mm_hadd_epi16(sum1,sum1);             // horizontally add 8 elements in 3 steps
    __m128i sum3  = _mm_hadd_epi16(sum2,sum2);
    __m128i sum4  = _mm_hadd_epi16(sum3,sum3);
    int16_t sum5  = (int16_t)_mm_cvtsi128_si32(sum4);      // 16 bit sum
    return  sum5;                                          // sign extend to 32 bits
#else                 // SSE2
    __m128i aeven = _mm_slli_epi16(a,8);                   // even numbered elements of a. get sign bit in position
            aeven = _mm_srai_epi16(aeven,8);               // sign extend even numbered elements
    __m128i aodd  = _mm_srai_epi16(a,8);                   // sign extend odd  numbered elements
    __m128i sum1  = _mm_add_epi16(aeven,aodd);             // add even and odd elements
    __m128i sum2  = _mm_shuffle_epi32(sum1,0x0E);          // 4 high elements
    __m128i sum3  = _mm_add_epi16(sum1,sum2);              // 4 sums
    __m128i sum4  = _mm_shuffle_epi32(sum3,0x01);          // 2 high elements
    __m128i sum5  = _mm_add_epi16(sum3,sum4);              // 2 sums
    __m128i sum6  = _mm_shufflelo_epi16(sum5,0x01);        // 1 high element
    __m128i sum7  = _mm_add_epi16(sum5,sum6);              // 1 sum
    int16_t sum8  = _mm_cvtsi128_si32(sum7);               // 16 bit sum
    return  sum8;                                          // sign extend to 32 bits
#endif
}


// function add_saturated: add element by element, signed with saturation
static inline Vec16c add_saturated(Vec16c const & a, Vec16c const & b) {
    return _mm_adds_epi8(a, b);
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec16c sub_saturated(Vec16c const & a, Vec16c const & b) {
    return _mm_subs_epi8(a, b);
}

// function max: a > b ? a : b
static inline Vec16c max(Vec16c const & a, Vec16c const & b) {
#if INSTRSET >= 5   // SSE4.1
    return _mm_max_epi8(a,b);
#else  // SSE2
    __m128i signbit = _mm_set1_epi32(0x80808080);
    __m128i a1      = _mm_xor_si128(a,signbit);            // add 0x80
    __m128i b1      = _mm_xor_si128(b,signbit);            // add 0x80
    __m128i m1      = _mm_max_epu8(a1,b1);                 // unsigned max
    return  _mm_xor_si128(m1,signbit);                     // sub 0x80
#endif
}

// function min: a < b ? a : b
static inline Vec16c min(Vec16c const & a, Vec16c const & b) {
#if INSTRSET >= 5   // SSE4.1
    return _mm_min_epi8(a,b);
#else  // SSE2
    __m128i signbit = _mm_set1_epi32(0x80808080);
    __m128i a1      = _mm_xor_si128(a,signbit);            // add 0x80
    __m128i b1      = _mm_xor_si128(b,signbit);            // add 0x80
    __m128i m1      = _mm_min_epu8(a1,b1);                 // unsigned min
    return  _mm_xor_si128(m1,signbit);                     // sub 0x80
#endif
}

// function abs: a >= 0 ? a : -a
static inline Vec16c abs(Vec16c const & a) {
#if INSTRSET >= 4     // SSSE3 supported
    return _mm_sign_epi8(a,a);
#else                 // SSE2
    __m128i nega = _mm_sub_epi8(_mm_setzero_si128(), a);
    return _mm_min_epu8(a, nega);   // unsigned min (the negative value is bigger when compared as unsigned)
#endif
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec16c abs_saturated(Vec16c const & a) {
    __m128i absa   = abs(a);                               // abs(a)
    __m128i overfl = _mm_cmpgt_epi8(_mm_setzero_si128(),absa);// 0 > a
    return           _mm_add_epi8(absa,overfl);            // subtract 1 if 0x80
}

// function rotate_left: rotate each element left by b bits 
// Use negative count to rotate right
static inline Vec16c rotate_left(Vec16c const & a, int b) {
#ifdef __XOP__  // AMD XOP instruction set
    return _mm_rot_epi8(a,_mm_set1_epi8(b));
#else  // SSE2 instruction set
    __m128i bb        = _mm_cvtsi32_si128(b & 7);          // b modulo 8
    __m128i mbb       = _mm_cvtsi32_si128((8-b) & 7);      // 8-b modulo 8
    __m128i maskeven  = _mm_set1_epi32(0x00FF00FF);        // mask for even numbered bytes
    __m128i even      = _mm_and_si128(a,maskeven);         // even numbered bytes of a
    __m128i odd       = _mm_andnot_si128(maskeven,a);      // odd numbered bytes of a
    __m128i evenleft  = _mm_sll_epi16(even,bb);            // even bytes of a << b
    __m128i oddleft   = _mm_sll_epi16(odd,bb);             // odd  bytes of a << b
    __m128i evenright = _mm_srl_epi16(even,mbb);           // even bytes of a >> 8-b
    __m128i oddright  = _mm_srl_epi16(odd,mbb);            // odd  bytes of a >> 8-b
    __m128i evenrot   = _mm_or_si128(evenleft,evenright);  // even bytes of a rotated
    __m128i oddrot    = _mm_or_si128(oddleft,oddright);    // odd  bytes of a rotated
    __m128i allrot    = selectb(maskeven,evenrot,oddrot);  // all  bytes rotated
    return  allrot;
#endif
}


/*****************************************************************************
*
*          Vector of 16 8-bit unsigned integers
*
*****************************************************************************/

class Vec16uc : public Vec16c {
public:
    // Default constructor:
    Vec16uc() {
    };
    // Constructor to broadcast the same value into all elements:
    Vec16uc(uint32_t i) {
        xmm = _mm_set1_epi8((char)i);
    };
    // Constructor to build from all elements:
    Vec16uc(uint8_t i0, uint8_t i1, uint8_t i2, uint8_t i3, uint8_t i4, uint8_t i5, uint8_t i6, uint8_t i7,
        uint8_t i8, uint8_t i9, uint8_t i10, uint8_t i11, uint8_t i12, uint8_t i13, uint8_t i14, uint8_t i15) {
        xmm = _mm_setr_epi8(i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15);
    };
    // Constructor to convert from type __m128i used in intrinsics:
    Vec16uc(__m128i const & x) {
        xmm = x;
    };
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec16uc & operator = (__m128i const & x) {
        xmm = x;
        return *this;
    };
    // Member function to load from array (unaligned)
    Vec16uc & load(void const * p) {
        xmm = _mm_loadu_si128((__m128i const*)p);
        return *this;
    }
    // Member function to load from array (aligned)
    Vec16uc & load_a(void const * p) {
        xmm = _mm_load_si128((__m128i const*)p);
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec16uc const & insert(uint32_t index, uint8_t value) {
        Vec16c::insert(index, value);
        return *this;
    }
    // Member function extract a single element from vector
    uint8_t extract(uint32_t index) const {
        return Vec16c::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint8_t operator [] (uint32_t index) const {
        return extract(index);
    }
};

// Define operators for this class

// vector operator << : shift left all elements
static inline Vec16uc operator << (Vec16uc const & a, uint32_t b) {
    uint32_t mask = (uint32_t)0xFF >> (uint32_t)b;         // mask to remove bits that are shifted out
    __m128i am    = _mm_and_si128(a,_mm_set1_epi8((char)mask));  // remove bits that will overflow
    __m128i res   = _mm_sll_epi16(am,_mm_cvtsi32_si128(b));// 16-bit shifts
    return res;
}

// vector operator << : shift left all elements
static inline Vec16uc operator << (Vec16uc const & a, int32_t b) {
    return a << (uint32_t)b;
}

// vector operator >> : shift right logical all elements
static inline Vec16uc operator >> (Vec16uc const & a, uint32_t b) {
    uint32_t mask = (uint32_t)0xFF << (uint32_t)b;         // mask to remove bits that are shifted out
    __m128i am    = _mm_and_si128(a,_mm_set1_epi8((char)mask));  // remove bits that will overflow
    __m128i res   = _mm_srl_epi16(am,_mm_cvtsi32_si128(b));// 16-bit shifts
    return res;
}

// vector operator >> : shift right logical all elements
static inline Vec16uc operator >> (Vec16uc const & a, int32_t b) {
    return a >> (uint32_t)b;
}

// vector operator >>= : shift right logical
static inline Vec16uc & operator >>= (Vec16uc & a, int b) {
    a = a >> b;
    return a;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec16cb operator >= (Vec16uc const & a, Vec16uc const & b) {
#ifdef __XOP__  // AMD XOP instruction set
    return _mm_comge_epu8(a,b);
#else  // SSE2 instruction set
    return _mm_cmpeq_epi8(_mm_max_epu8(a,b),a); // a == max(a,b)
#endif
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec16cb operator <= (Vec16uc const & a, Vec16uc const & b) {
    return b >= a;
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec16cb operator > (Vec16uc const & a, Vec16uc const & b) {
#ifdef __XOP__  // AMD XOP instruction set
    return _mm_comgt_epu8(a,b);
#else  // SSE2 instruction set
    return Vec16cb(Vec16c(~(b >= a)));
#endif
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec16cb operator < (Vec16uc const & a, Vec16uc const & b) {
    return b > a;
}

// vector operator + : add
static inline Vec16uc operator + (Vec16uc const & a, Vec16uc const & b) {
    return Vec16uc (Vec16c(a) + Vec16c(b));
}

// vector operator - : subtract
static inline Vec16uc operator - (Vec16uc const & a, Vec16uc const & b) {
    return Vec16uc (Vec16c(a) - Vec16c(b));
}

// vector operator * : multiply
static inline Vec16uc operator * (Vec16uc const & a, Vec16uc const & b) {
    return Vec16uc (Vec16c(a) * Vec16c(b));
}

// vector operator & : bitwise and
static inline Vec16uc operator & (Vec16uc const & a, Vec16uc const & b) {
    return Vec16uc(Vec128b(a) & Vec128b(b));
}
static inline Vec16uc operator && (Vec16uc const & a, Vec16uc const & b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec16uc operator | (Vec16uc const & a, Vec16uc const & b) {
    return Vec16uc(Vec128b(a) | Vec128b(b));
}
static inline Vec16uc operator || (Vec16uc const & a, Vec16uc const & b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec16uc operator ^ (Vec16uc const & a, Vec16uc const & b) {
    return Vec16uc(Vec128b(a) ^ Vec128b(b));
}

// vector operator ~ : bitwise not
static inline Vec16uc operator ~ (Vec16uc const & a) {
    return Vec16uc( ~ Vec128b(a));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or -1 (true). No other values are allowed.
// (s is signed)
static inline Vec16uc select (Vec16cb const & s, Vec16uc const & a, Vec16uc const & b) {
    return selectb(s,a,b);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec16uc if_add (Vec16cb const & f, Vec16uc const & a, Vec16uc const & b) {
    return a + (Vec16uc(f) & b);
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
// (Note: horizontal_add_x(Vec16uc) is slightly faster)
static inline uint32_t horizontal_add (Vec16uc const & a) {
    __m128i sum1 = _mm_sad_epu8(a,_mm_setzero_si128());
    __m128i sum2 = _mm_shuffle_epi32(sum1,2);
    __m128i sum3 = _mm_add_epi16(sum1,sum2);
    uint16_t sum4 = (uint16_t)_mm_cvtsi128_si32(sum3);      // truncate to 16 bits
    return  sum4;
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Each element is zero-extended before addition to avoid overflow
static inline uint32_t horizontal_add_x (Vec16uc const & a) {
    __m128i sum1 = _mm_sad_epu8(a,_mm_setzero_si128());
    __m128i sum2 = _mm_shuffle_epi32(sum1,2);
    __m128i sum3 = _mm_add_epi16(sum1,sum2);
    return _mm_cvtsi128_si32(sum3);
}

// function add_saturated: add element by element, unsigned with saturation
static inline Vec16uc add_saturated(Vec16uc const & a, Vec16uc const & b) {
    return _mm_adds_epu8(a, b);
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec16uc sub_saturated(Vec16uc const & a, Vec16uc const & b) {
    return _mm_subs_epu8(a, b);
}

// function max: a > b ? a : b
static inline Vec16uc max(Vec16uc const & a, Vec16uc const & b) {
    return _mm_max_epu8(a,b);
}

// function min: a < b ? a : b
static inline Vec16uc min(Vec16uc const & a, Vec16uc const & b) {
    return _mm_min_epu8(a,b);
}


    
/*****************************************************************************
*
*          Vector of 8 16-bit signed integers
*
*****************************************************************************/

class Vec8s : public Vec128b {
public:
    // Default constructor:
    Vec8s() {
    };
    // Constructor to broadcast the same value into all elements:
    Vec8s(int i) {
        xmm = _mm_set1_epi16((int16_t)i);
    };
    // Constructor to build from all elements:
    Vec8s(int16_t i0, int16_t i1, int16_t i2, int16_t i3, int16_t i4, int16_t i5, int16_t i6, int16_t i7) {
        xmm = _mm_setr_epi16(i0, i1, i2, i3, i4, i5, i6, i7);
    };
    // Constructor to convert from type __m128i used in intrinsics:
    Vec8s(__m128i const & x) {
        xmm = x;
    };
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec8s & operator = (__m128i const & x) {
        xmm = x;
        return *this;
    };
    // Type cast operator to convert to __m128i used in intrinsics
    operator __m128i() const {
        return xmm;
    };
    // Member function to load from array (unaligned)
    Vec8s & load(void const * p) {
        xmm = _mm_loadu_si128((__m128i const*)p);
        return *this;
    }
    // Member function to load from array (aligned)
    Vec8s & load_a(void const * p) {
        xmm = _mm_load_si128((__m128i const*)p);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec8s & load_partial(int n, void const * p) {
        if      (n >= 8) load(p);
        else if (n <= 0)  *this = 0;
        else if (((int)(intptr_t)p & 0xFFF) < 0xFF0) {
            // p is at least 16 bytes from a page boundary. OK to read 16 bytes
            load(p);
        }
        else {
            // worst case. read 1 byte at a time and suffer store forwarding penalty
            int16_t x[8];
            for (int i = 0; i < n; i++) x[i] = ((int16_t *)p)[i];
            load(x);
        }
        cutoff(n);
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
        if (n >= 8) {
            store(p);
            return;
        }
        if (n <= 0) return;
        // we are not using _mm_maskmoveu_si128 because it is too slow on many processors
        union {        
            int8_t  c[16];
            int16_t s[8];
            int32_t i[4];
            int64_t q[2];
        } u;
        store(u.c);
        int j = 0;
        if (n & 4) {
            *(int64_t*)p = u.q[0];
            j += 8;
        }
        if (n & 2) {
            ((int32_t*)p)[j/4] = u.i[j/4];
            j += 4;
        }
        if (n & 1) {
            ((int16_t*)p)[j/2] = u.s[j/2];
        }
    }
    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec8s & cutoff(int n) {
        *this = Vec16c(xmm).cutoff(n * 2);
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec8s const & insert(uint32_t index, int16_t value) {
        switch(index) {
        case 0:
            xmm = _mm_insert_epi16(xmm,value,0);  break;
        case 1:
            xmm = _mm_insert_epi16(xmm,value,1);  break;
        case 2:
            xmm = _mm_insert_epi16(xmm,value,2);  break;
        case 3:
            xmm = _mm_insert_epi16(xmm,value,3);  break;
        case 4:
            xmm = _mm_insert_epi16(xmm,value,4);  break;
        case 5:
            xmm = _mm_insert_epi16(xmm,value,5);  break;
        case 6:
            xmm = _mm_insert_epi16(xmm,value,6);  break;
        case 7:
            xmm = _mm_insert_epi16(xmm,value,7);  break;
        }
        return *this;
    };
    // Member function extract a single element from vector
    // Note: This function is inefficient. Use store function if extracting more than one element
    int16_t extract(uint32_t index) const {
        switch(index) {
        case 0:
            return (int16_t)_mm_extract_epi16(xmm,0);
        case 1:
            return (int16_t)_mm_extract_epi16(xmm,1);
        case 2:
            return (int16_t)_mm_extract_epi16(xmm,2);
        case 3:
            return (int16_t)_mm_extract_epi16(xmm,3);
        case 4:
            return (int16_t)_mm_extract_epi16(xmm,4);
        case 5:
            return (int16_t)_mm_extract_epi16(xmm,5);
        case 6:
            return (int16_t)_mm_extract_epi16(xmm,6);
        case 7:
            return (int16_t)_mm_extract_epi16(xmm,7);
        }
        return 0;
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int16_t operator [] (uint32_t index) const {
        return extract(index);
    }
    static int size() {
        return 8;
    }
};

/*****************************************************************************
*
*          Vec8sb: Vector of 8 Booleans for use with Vec8s and Vec8us
*
*****************************************************************************/

class Vec8sb : public Vec8s {
public:
    // Constructor to build from all elements:
    Vec8sb(bool x0, bool x1, bool x2, bool x3, bool x4, bool x5, bool x6, bool x7) {
        xmm = Vec8s(-int16_t(x0), -int16_t(x1), -int16_t(x2), -int16_t(x3), -int16_t(x4), -int16_t(x5), -int16_t(x6), -int16_t(x7));
    }
    // Default constructor:
    Vec8sb() {
    }
    // Constructor to convert from type __m128i used in intrinsics:
    Vec8sb(__m128i const & x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec8sb & operator = (__m128i const & x) {
        xmm = x;
        return *this;
    }
    // Constructor to broadcast scalar value:
    Vec8sb(bool b) : Vec8s(-int16_t(b)) {
    }
    // Assignment operator to broadcast scalar value:
    Vec8sb & operator = (bool b) {
        *this = Vec8sb(b);
        return *this;
    }
private: // Prevent constructing from int, etc.
    Vec8sb(int b);
    Vec8sb & operator = (int x);
public:
    Vec8sb & insert (int index, bool a) {
        Vec8s::insert(index, -(int)a);
        return *this;
    }
    // Member function extract a single element from vector
    // Note: This function is inefficient. Use store function if extracting more than one element
    bool extract(uint32_t index) const {
        return Vec8s::extract(index) != 0;
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    bool operator [] (uint32_t index) const {
        return extract(index);
    }
};


/*****************************************************************************
*
*          Define operators for Vec8sb
*
*****************************************************************************/

// vector operator & : bitwise and
static inline Vec8sb operator & (Vec8sb const & a, Vec8sb const & b) {
    return Vec8sb(Vec128b(a) & Vec128b(b));
}
static inline Vec8sb operator && (Vec8sb const & a, Vec8sb const & b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec8sb & operator &= (Vec8sb & a, Vec8sb const & b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec8sb operator | (Vec8sb const & a, Vec8sb const & b) {
    return Vec8sb(Vec128b(a) | Vec128b(b));
}
static inline Vec8sb operator || (Vec8sb const & a, Vec8sb const & b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec8sb & operator |= (Vec8sb & a, Vec8sb const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec8sb operator ^ (Vec8sb const & a, Vec8sb const & b) {
    return Vec8sb(Vec128b(a) ^ Vec128b(b));
}
// vector operator ^= : bitwise xor
static inline Vec8sb & operator ^= (Vec8sb & a, Vec8sb const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec8sb operator ~ (Vec8sb const & a) {
    return Vec8sb( ~ Vec128b(a));
}

// vector operator ! : element not
static inline Vec8sb operator ! (Vec8sb const & a) {
    return ~ a;
}

// vector function andnot
static inline Vec8sb andnot (Vec8sb const & a, Vec8sb const & b) {
    return Vec8sb(andnot(Vec128b(a), Vec128b(b)));
}


/*****************************************************************************
*
*         operators for Vec8s
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec8s operator + (Vec8s const & a, Vec8s const & b) {
    return _mm_add_epi16(a, b);
}

// vector operator += : add
static inline Vec8s & operator += (Vec8s & a, Vec8s const & b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec8s operator ++ (Vec8s & a, int) {
    Vec8s a0 = a;
    a = a + 1;
    return a0;
}

// prefix operator ++
static inline Vec8s & operator ++ (Vec8s & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec8s operator - (Vec8s const & a, Vec8s const & b) {
    return _mm_sub_epi16(a, b);
}

// vector operator - : unary minus
static inline Vec8s operator - (Vec8s const & a) {
    return _mm_sub_epi16(_mm_setzero_si128(), a);
}

// vector operator -= : subtract
static inline Vec8s & operator -= (Vec8s & a, Vec8s const & b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec8s operator -- (Vec8s & a, int) {
    Vec8s a0 = a;
    a = a - 1;
    return a0;
}

// prefix operator --
static inline Vec8s & operator -- (Vec8s & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec8s operator * (Vec8s const & a, Vec8s const & b) {
    return _mm_mullo_epi16(a, b);
}

// vector operator *= : multiply
static inline Vec8s & operator *= (Vec8s & a, Vec8s const & b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer
// See bottom of file


// vector operator << : shift left
static inline Vec8s operator << (Vec8s const & a, int b) {
    return _mm_sll_epi16(a,_mm_cvtsi32_si128(b));
}

// vector operator <<= : shift left
static inline Vec8s & operator <<= (Vec8s & a, int b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec8s operator >> (Vec8s const & a, int b) {
    return _mm_sra_epi16(a,_mm_cvtsi32_si128(b));
}

// vector operator >>= : shift right arithmetic
static inline Vec8s & operator >>= (Vec8s & a, int b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec8s operator == (Vec8s const & a, Vec8s const & b) {
    return _mm_cmpeq_epi16(a, b);
}

// vector operator != : returns true for elements for which a != b
static inline Vec8s operator != (Vec8s const & a, Vec8s const & b) {
#ifdef __XOP__  // AMD XOP instruction set
    return _mm_comneq_epi16(a,b);
#else  // SSE2 instruction set
    return Vec8s (~(a == b));
#endif
}

// vector operator > : returns true for elements for which a > b
static inline Vec8s operator > (Vec8s const & a, Vec8s const & b) {
    return _mm_cmpgt_epi16(a, b);
}

// vector operator < : returns true for elements for which a < b
static inline Vec8s operator < (Vec8s const & a, Vec8s const & b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec8s operator >= (Vec8s const & a, Vec8s const & b) {
#ifdef __XOP__  // AMD XOP instruction set
    return _mm_comge_epi16(a,b);
#else  // SSE2 instruction set
    return Vec8s (~(b > a));
#endif
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec8s operator <= (Vec8s const & a, Vec8s const & b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec8s operator & (Vec8s const & a, Vec8s const & b) {
    return Vec8s(Vec128b(a) & Vec128b(b));
}
static inline Vec8s operator && (Vec8s const & a, Vec8s const & b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec8s & operator &= (Vec8s & a, Vec8s const & b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec8s operator | (Vec8s const & a, Vec8s const & b) {
    return Vec8s(Vec128b(a) | Vec128b(b));
}
static inline Vec8s operator || (Vec8s const & a, Vec8s const & b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec8s & operator |= (Vec8s & a, Vec8s const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec8s operator ^ (Vec8s const & a, Vec8s const & b) {
    return Vec8s(Vec128b(a) ^ Vec128b(b));
}
// vector operator ^= : bitwise xor
static inline Vec8s & operator ^= (Vec8s & a, Vec8s const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec8s operator ~ (Vec8s const & a) {
    return Vec8s( ~ Vec128b(a));
}

// vector operator ! : logical not, returns true for elements == 0
static inline Vec8s operator ! (Vec8s const & a) {
    return _mm_cmpeq_epi16(a,_mm_setzero_si128());
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 8; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or -1 (true). No other values are allowed.
// (s is signed)
static inline Vec8s select (Vec8s const & s, Vec8s const & a, Vec8s const & b) {
    return selectb(s,a,b);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec8s if_add (Vec8sb const & f, Vec8s const & a, Vec8s const & b) {
    return a + (Vec8s(f) & b);
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
static inline int32_t horizontal_add (Vec8s const & a) {
#ifdef __XOP__       // AMD XOP instruction set
    __m128i sum1  = _mm_haddq_epi16(a);
    __m128i sum2  = _mm_shuffle_epi32(sum1,0x0E);          // high element
    __m128i sum3  = _mm_add_epi32(sum1,sum2);              // sum
    int16_t sum4  = _mm_cvtsi128_si32(sum3);               // truncate to 16 bits
    return  sum4;                                          // sign extend to 32 bits
#elif  INSTRSET >= 4  // SSSE3
    __m128i sum1  = _mm_hadd_epi16(a,a);                   // horizontally add 8 elements in 3 steps
    __m128i sum2  = _mm_hadd_epi16(sum1,sum1);
    __m128i sum3  = _mm_hadd_epi16(sum2,sum2);
    int16_t sum4  = (int16_t)_mm_cvtsi128_si32(sum3);      // 16 bit sum
    return  sum4;                                          // sign extend to 32 bits
#else                 // SSE2
    __m128i sum1  = _mm_shuffle_epi32(a,0x0E);             // 4 high elements
    __m128i sum2  = _mm_add_epi16(a,sum1);                 // 4 sums
    __m128i sum3  = _mm_shuffle_epi32(sum2,0x01);          // 2 high elements
    __m128i sum4  = _mm_add_epi16(sum2,sum3);              // 2 sums
    __m128i sum5  = _mm_shufflelo_epi16(sum4,0x01);        // 1 high element
    __m128i sum6  = _mm_add_epi16(sum4,sum5);              // 1 sum
    int16_t sum7  = _mm_cvtsi128_si32(sum6);               // 16 bit sum
    return  sum7;                                          // sign extend to 32 bits
#endif
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Elements are sign extended before adding to avoid overflow
static inline int32_t horizontal_add_x (Vec8s const & a) {
#ifdef __XOP__       // AMD XOP instruction set
    __m128i sum1  = _mm_haddq_epi16(a);
    __m128i sum2  = _mm_shuffle_epi32(sum1,0x0E);          // high element
    __m128i sum3  = _mm_add_epi32(sum1,sum2);              // sum
    return          _mm_cvtsi128_si32(sum3);
#elif  INSTRSET >= 4  // SSSE3
    __m128i aeven = _mm_slli_epi32(a,16);                  // even numbered elements of a. get sign bit in position
            aeven = _mm_srai_epi32(aeven,16);              // sign extend even numbered elements
    __m128i aodd  = _mm_srai_epi32(a,16);                  // sign extend odd  numbered elements
    __m128i sum1  = _mm_add_epi32(aeven,aodd);             // add even and odd elements
    __m128i sum2  = _mm_hadd_epi32(sum1,sum1);             // horizontally add 4 elements in 2 steps
    __m128i sum3  = _mm_hadd_epi32(sum2,sum2);
    return  _mm_cvtsi128_si32(sum3);
#else                 // SSE2
    __m128i aeven = _mm_slli_epi32(a,16);                  // even numbered elements of a. get sign bit in position
            aeven = _mm_srai_epi32(aeven,16);              // sign extend even numbered elements
    __m128i aodd  = _mm_srai_epi32(a,16);                  // sign extend odd  numbered elements
    __m128i sum1  = _mm_add_epi32(aeven,aodd);             // add even and odd elements
    __m128i sum2  = _mm_shuffle_epi32(sum1,0x0E);          // 2 high elements
    __m128i sum3  = _mm_add_epi32(sum1,sum2);
    __m128i sum4  = _mm_shuffle_epi32(sum3,0x01);          // 1 high elements
    __m128i sum5  = _mm_add_epi32(sum3,sum4);
    return  _mm_cvtsi128_si32(sum5);                       // 32 bit sum
#endif
}

// function add_saturated: add element by element, signed with saturation
static inline Vec8s add_saturated(Vec8s const & a, Vec8s const & b) {
    return _mm_adds_epi16(a, b);
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec8s sub_saturated(Vec8s const & a, Vec8s const & b) {
    return _mm_subs_epi16(a, b);
}

// function max: a > b ? a : b
static inline Vec8s max(Vec8s const & a, Vec8s const & b) {
    return _mm_max_epi16(a,b);
}

// function min: a < b ? a : b
static inline Vec8s min(Vec8s const & a, Vec8s const & b) {
    return _mm_min_epi16(a,b);
}

// function abs: a >= 0 ? a : -a
static inline Vec8s abs(Vec8s const & a) {
#if INSTRSET >= 4     // SSSE3 supported
    return _mm_sign_epi16(a,a);
#else                 // SSE2
    __m128i nega = _mm_sub_epi16(_mm_setzero_si128(), a);
    return _mm_max_epi16(a, nega);
#endif
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec8s abs_saturated(Vec8s const & a) {
    __m128i absa   = abs(a);                               // abs(a)
    __m128i overfl = _mm_srai_epi16(absa,15);              // sign
    return           _mm_add_epi16(absa,overfl);           // subtract 1 if 0x8000
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec8s rotate_left(Vec8s const & a, int b) {
#ifdef __XOP__  // AMD XOP instruction set
    return _mm_rot_epi16(a,_mm_set1_epi16(b));
#else  // SSE2 instruction set
    __m128i left  = _mm_sll_epi16(a,_mm_cvtsi32_si128(b & 0x0F));      // a << b 
    __m128i right = _mm_srl_epi16(a,_mm_cvtsi32_si128((16-b) & 0x0F)); // a >> (16 - b)
    __m128i rot   = _mm_or_si128(left,right);                          // or
    return  rot;
#endif
}


/*****************************************************************************
*
*          Vector of 8 16-bit unsigned integers
*
*****************************************************************************/

class Vec8us : public Vec8s {
public:
    // Default constructor:
    Vec8us() {
    };
    // Constructor to broadcast the same value into all elements:
    Vec8us(uint32_t i) {
        xmm = _mm_set1_epi16((int16_t)i);
    };
    // Constructor to build from all elements:
    Vec8us(uint16_t i0, uint16_t i1, uint16_t i2, uint16_t i3, uint16_t i4, uint16_t i5, uint16_t i6, uint16_t i7) {
        xmm = _mm_setr_epi16(i0, i1, i2, i3, i4, i5, i6, i7);
    };
    // Constructor to convert from type __m128i used in intrinsics:
    Vec8us(__m128i const & x) {
        xmm = x;
    };
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec8us & operator = (__m128i const & x) {
        xmm = x;
        return *this;
    };
    // Member function to load from array (unaligned)
    Vec8us & load(void const * p) {
        xmm = _mm_loadu_si128((__m128i const*)p);
        return *this;
    }
    // Member function to load from array (aligned)
    Vec8us & load_a(void const * p) {
        xmm = _mm_load_si128((__m128i const*)p);
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec8us const & insert(uint32_t index, uint16_t value) {
        Vec8s::insert(index, value);
        return *this;
    };
    // Member function extract a single element from vector
    uint16_t extract(uint32_t index) const {
        return Vec8s::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint16_t operator [] (uint32_t index) const {
        return extract(index);
    }
};

// Define operators for this class

// vector operator + : add
static inline Vec8us operator + (Vec8us const & a, Vec8us const & b) {
    return Vec8us (Vec8s(a) + Vec8s(b));
}

// vector operator - : subtract
static inline Vec8us operator - (Vec8us const & a, Vec8us const & b) {
    return Vec8us (Vec8s(a) - Vec8s(b));
}

// vector operator * : multiply
static inline Vec8us operator * (Vec8us const & a, Vec8us const & b) {
    return Vec8us (Vec8s(a) * Vec8s(b));
}

// vector operator / : divide
// See bottom of file

// vector operator >> : shift right logical all elements
static inline Vec8us operator >> (Vec8us const & a, uint32_t b) {
    return _mm_srl_epi16(a,_mm_cvtsi32_si128(b)); 
}

// vector operator >> : shift right logical all elements
static inline Vec8us operator >> (Vec8us const & a, int32_t b) {
    return a >> (uint32_t)b;
}

// vector operator >>= : shift right logical
static inline Vec8us & operator >>= (Vec8us & a, int b) {
    a = a >> b;
    return a;
}

// vector operator << : shift left all elements
static inline Vec8us operator << (Vec8us const & a, uint32_t b) {
    return _mm_sll_epi16(a,_mm_cvtsi32_si128(b)); 
}

// vector operator << : shift left all elements
static inline Vec8us operator << (Vec8us const & a, int32_t b) {
    return a << (uint32_t)b;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec8s operator >= (Vec8us const & a, Vec8us const & b) {
#ifdef __XOP__  // AMD XOP instruction set
    return _mm_comge_epu16(a,b);
#elif INSTRSET >= 5   // SSE4.1
    __m128i max_ab = _mm_max_epu16(a,b);                   // max(a,b), unsigned
    return _mm_cmpeq_epi16(a,max_ab);                      // a == max(a,b)
#else  // SSE2 instruction set
    __m128i sub1 = _mm_sub_epi16(a,b);                     // a-b, wraparound
    __m128i sub2 = _mm_subs_epu16(a,b);                    // a-b, saturated
    return  _mm_cmpeq_epi16(sub1,sub2);                    // sub1 == sub2 if no carry
#endif
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec8s operator <= (Vec8us const & a, Vec8us const & b) {
    return b >= a;
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec8s operator > (Vec8us const & a, Vec8us const & b) {
#ifdef __XOP__  // AMD XOP instruction set
    return _mm_comgt_epu16(a,b);
#else  // SSE2 instruction set
    return Vec8s (~(b >= a));
#endif
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec8s operator < (Vec8us const & a, Vec8us const & b) {
    return b > a;
}

// vector operator & : bitwise and
static inline Vec8us operator & (Vec8us const & a, Vec8us const & b) {
    return Vec8us(Vec128b(a) & Vec128b(b));
}
static inline Vec8us operator && (Vec8us const & a, Vec8us const & b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec8us operator | (Vec8us const & a, Vec8us const & b) {
    return Vec8us(Vec128b(a) | Vec128b(b));
}
static inline Vec8us operator || (Vec8us const & a, Vec8us const & b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec8us operator ^ (Vec8us const & a, Vec8us const & b) {
    return Vec8us(Vec128b(a) ^ Vec128b(b));
}

// vector operator ~ : bitwise not
static inline Vec8us operator ~ (Vec8us const & a) {
    return Vec8us( ~ Vec128b(a));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 8; i++) result[i] = s[i] ? a[i] : b[i];
// Each word in s must be either 0 (false) or -1 (true). No other values are allowed.
// (s is signed)
static inline Vec8us select (Vec8s const & s, Vec8us const & a, Vec8us const & b) {
    return selectb(s,a,b);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec8us if_add (Vec8sb const & f, Vec8us const & a, Vec8us const & b) {
    return a + (Vec8us(f) & b);
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
static inline uint32_t horizontal_add (Vec8us const & a) {
#ifdef __XOP__     // AMD XOP instruction set
    __m128i sum1  = _mm_haddq_epu16(a);
    __m128i sum2  = _mm_shuffle_epi32(sum1,0x0E);          // high element
    __m128i sum3  = _mm_add_epi32(sum1,sum2);              // sum
    uint16_t sum4 = _mm_cvtsi128_si32(sum3);               // truncate to 16 bits
    return  sum4;                                          // zero extend to 32 bits
#elif  INSTRSET >= 4  // SSSE3
    __m128i sum1  = _mm_hadd_epi16(a,a);                   // horizontally add 8 elements in 3 steps
    __m128i sum2  = _mm_hadd_epi16(sum1,sum1);
    __m128i sum3  = _mm_hadd_epi16(sum2,sum2);
    uint16_t sum4 = (uint16_t)_mm_cvtsi128_si32(sum3);     // 16 bit sum
    return  sum4;                                          // zero extend to 32 bits
#else                 // SSE2
    __m128i sum1  = _mm_shuffle_epi32(a,0x0E);             // 4 high elements
    __m128i sum2  = _mm_add_epi16(a,sum1);                 // 4 sums
    __m128i sum3  = _mm_shuffle_epi32(sum2,0x01);          // 2 high elements
    __m128i sum4  = _mm_add_epi16(sum2,sum3);              // 2 sums
    __m128i sum5  = _mm_shufflelo_epi16(sum4,0x01);        // 1 high element
    __m128i sum6  = _mm_add_epi16(sum4,sum5);              // 1 sum
    uint16_t sum7 = _mm_cvtsi128_si32(sum6);               // 16 bit sum
    return  sum7;                                          // zero extend to 32 bits
#endif
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Each element is zero-extended before addition to avoid overflow
static inline uint32_t horizontal_add_x (Vec8us const & a) {
#ifdef __XOP__     // AMD XOP instruction set
    __m128i sum1  = _mm_haddq_epu16(a);
    __m128i sum2  = _mm_shuffle_epi32(sum1,0x0E);          // high element
    __m128i sum3  = _mm_add_epi32(sum1,sum2);              // sum
    return          _mm_cvtsi128_si32(sum3);
#elif INSTRSET >= 4  // SSSE3
    __m128i mask  = _mm_set1_epi32(0x0000FFFF);            // mask for even positions
    __m128i aeven = _mm_and_si128(a,mask);                 // even numbered elements of a
    __m128i aodd  = _mm_srli_epi32(a,16);                  // zero extend odd numbered elements
    __m128i sum1  = _mm_add_epi32(aeven,aodd);             // add even and odd elements
    __m128i sum2  = _mm_hadd_epi32(sum1,sum1);             // horizontally add 4 elements in 2 steps
    __m128i sum3  = _mm_hadd_epi32(sum2,sum2);
    return  _mm_cvtsi128_si32(sum3);
#else                 // SSE2
    __m128i mask  = _mm_set1_epi32(0x0000FFFF);            // mask for even positions
    __m128i aeven = _mm_and_si128(a,mask);                 // even numbered elements of a
    __m128i aodd  = _mm_srli_epi32(a,16);                  // zero extend odd numbered elements
    __m128i sum1  = _mm_add_epi32(aeven,aodd);             // add even and odd elements
    __m128i sum2  = _mm_shuffle_epi32(sum1,0x0E);          // 2 high elements
    __m128i sum3  = _mm_add_epi32(sum1,sum2);
    __m128i sum4  = _mm_shuffle_epi32(sum3,0x01);          // 1 high elements
    __m128i sum5  = _mm_add_epi32(sum3,sum4);
    return  _mm_cvtsi128_si32(sum5);               // 16 bit sum
#endif
}

// function add_saturated: add element by element, unsigned with saturation
static inline Vec8us add_saturated(Vec8us const & a, Vec8us const & b) {
    return _mm_adds_epu16(a, b);
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec8us sub_saturated(Vec8us const & a, Vec8us const & b) {
    return _mm_subs_epu16(a, b);
}

// function max: a > b ? a : b
static inline Vec8us max(Vec8us const & a, Vec8us const & b) {
#if INSTRSET >= 5   // SSE4.1
    return _mm_max_epu16(a,b);
#else  // SSE2
    __m128i signbit = _mm_set1_epi32(0x80008000);
    __m128i a1      = _mm_xor_si128(a,signbit);            // add 0x8000
    __m128i b1      = _mm_xor_si128(b,signbit);            // add 0x8000
    __m128i m1      = _mm_max_epi16(a1,b1);                // signed max
    return  _mm_xor_si128(m1,signbit);                     // sub 0x8000
#endif
}

// function min: a < b ? a : b
static inline Vec8us min(Vec8us const & a, Vec8us const & b) {
#if INSTRSET >= 5   // SSE4.1
    return _mm_min_epu16(a,b);
#else  // SSE2
    __m128i signbit = _mm_set1_epi32(0x80008000);
    __m128i a1      = _mm_xor_si128(a,signbit);            // add 0x8000
    __m128i b1      = _mm_xor_si128(b,signbit);            // add 0x8000
    __m128i m1      = _mm_min_epi16(a1,b1);                // signed min
    return  _mm_xor_si128(m1,signbit);                     // sub 0x8000
#endif
}



/*****************************************************************************
*
*          Vector of 4 32-bit signed integers
*
*****************************************************************************/

class Vec4i : public Vec128b {
public:
    // Default constructor:
    Vec4i() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec4i(int i) {
        xmm = _mm_set1_epi32(i);
    }
    // Constructor to build from all elements:
    Vec4i(int32_t i0, int32_t i1, int32_t i2, int32_t i3) {
        xmm = _mm_setr_epi32(i0, i1, i2, i3);
    }
    // Constructor to convert from type __m128i used in intrinsics:
    Vec4i(__m128i const & x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec4i & operator = (__m128i const & x) {
        xmm = x;
        return *this;
    }
    // Type cast operator to convert to __m128i used in intrinsics
    operator __m128i() const {
        return xmm;
    }
    // Member function to load from array (unaligned)
    Vec4i & load(void const * p) {
        xmm = _mm_loadu_si128((__m128i const*)p);
        return *this;
    }
    // Member function to load from array (aligned)
    Vec4i & load_a(void const * p) {
        xmm = _mm_load_si128((__m128i const*)p);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec4i & load_partial(int n, void const * p) {
        switch (n) {
        case 0:
            *this = 0;  break;
        case 1:
            xmm = _mm_cvtsi32_si128(*(int32_t*)p);  break;
        case 2:
            // intrinsic for movq is missing!
            xmm = _mm_setr_epi32(((int32_t*)p)[0], ((int32_t*)p)[1], 0, 0);  break;
        case 3:
            xmm = _mm_setr_epi32(((int32_t*)p)[0], ((int32_t*)p)[1], ((int32_t*)p)[2], 0);  break;
        case 4:
            load(p);  break;
        default: 
            break;
        }
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
        union {        
            int32_t i[4];
            int64_t q[2];
        } u;
        switch (n) {
        case 1:
            *(int32_t*)p = _mm_cvtsi128_si32(xmm);  break;
        case 2:
            // intrinsic for movq is missing!
            store(u.i);
            *(int64_t*)p = u.q[0];  break;
        case 3:
            store(u.i);
            *(int64_t*)p     = u.q[0];  
            ((int32_t*)p)[2] = u.i[2];  break;
        case 4:
            store(p);  break;
        default:
            break;
        }
    }
    // cut off vector to n elements. The last 4-n elements are set to zero
    Vec4i & cutoff(int n) {
        *this = Vec16c(xmm).cutoff(n * 4);
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec4i const & insert(uint32_t index, int32_t value) {
        static const int32_t maskl[8] = {0,0,0,0,-1,0,0,0};
        __m128i broad = _mm_set1_epi32(value);  // broadcast value into all elements
        __m128i mask  = _mm_loadu_si128((__m128i const*)(maskl+4-(index & 3))); // mask with FFFFFFFF at index position
        xmm = selectb(mask,broad,xmm);
        return *this;
    }
    // Member function extract a single element from vector
    int32_t extract(uint32_t index) const {
        int32_t x[4];
        store(x);
        return x[index & 3];
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int32_t operator [] (uint32_t index) const {
        return extract(index);
    }
    static int size() {
        return 4;
    }
};


/*****************************************************************************
*
*          Vec4ib: Vector of 4 Booleans for use with Vec4i and Vec4ui
*
*****************************************************************************/
class Vec4ib : public Vec4i {
public:
    // Default constructor:
    Vec4ib() {
    }
    // Constructor to build from all elements:
    Vec4ib(bool x0, bool x1, bool x2, bool x3) {
        xmm = Vec4i(-int32_t(x0), -int32_t(x1), -int32_t(x2), -int32_t(x3));
    }
    // Constructor to convert from type __m128i used in intrinsics:
    Vec4ib(__m128i const & x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec4ib & operator = (__m128i const & x) {
        xmm = x;
        return *this;
    }
    // Constructor to broadcast scalar value:
    Vec4ib(bool b) : Vec4i(-int32_t(b)) {
    }
    // Assignment operator to broadcast scalar value:
    Vec4ib & operator = (bool b) {
        *this = Vec4ib(b);
        return *this;
    }
private: // Prevent constructing from int, etc.
    Vec4ib(int b);
    Vec4ib & operator = (int x);
public:
    Vec4ib & insert (int index, bool a) {
        Vec4i::insert(index, -(int)a);
        return *this;
    }    
    // Member function extract a single element from vector
    bool extract(uint32_t index) const {
        return Vec4i::extract(index) != 0;
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    bool operator [] (uint32_t index) const {
        return extract(index);
    }
};


/*****************************************************************************
*
*          Define operators for Vec4ib
*
*****************************************************************************/

// vector operator & : bitwise and
static inline Vec4ib operator & (Vec4ib const & a, Vec4ib const & b) {
    return Vec4ib(Vec128b(a) & Vec128b(b));
}
static inline Vec4ib operator && (Vec4ib const & a, Vec4ib const & b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec4ib & operator &= (Vec4ib & a, Vec4ib const & b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec4ib operator | (Vec4ib const & a, Vec4ib const & b) {
    return Vec4ib(Vec128b(a) | Vec128b(b));
}
static inline Vec4ib operator || (Vec4ib const & a, Vec4ib const & b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec4ib & operator |= (Vec4ib & a, Vec4ib const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec4ib operator ^ (Vec4ib const & a, Vec4ib const & b) {
    return Vec4ib(Vec128b(a) ^ Vec128b(b));
}
// vector operator ^= : bitwise xor
static inline Vec4ib & operator ^= (Vec4ib & a, Vec4ib const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec4ib operator ~ (Vec4ib const & a) {
    return Vec4ib( ~ Vec128b(a));
}

// vector operator ! : element not
static inline Vec4ib operator ! (Vec4ib const & a) {
    return ~ a;
}

// vector function andnot
static inline Vec4ib andnot (Vec4ib const & a, Vec4ib const & b) {
    return Vec4ib(andnot(Vec128b(a), Vec128b(b)));
}


/*****************************************************************************
*
*          Operators for Vec4i
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec4i operator + (Vec4i const & a, Vec4i const & b) {
    return _mm_add_epi32(a, b);
}

// vector operator += : add
static inline Vec4i & operator += (Vec4i & a, Vec4i const & b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec4i operator ++ (Vec4i & a, int) {
    Vec4i a0 = a;
    a = a + 1;
    return a0;
}

// prefix operator ++
static inline Vec4i & operator ++ (Vec4i & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec4i operator - (Vec4i const & a, Vec4i const & b) {
    return _mm_sub_epi32(a, b);
}

// vector operator - : unary minus
static inline Vec4i operator - (Vec4i const & a) {
    return _mm_sub_epi32(_mm_setzero_si128(), a);
}

// vector operator -= : subtract
static inline Vec4i & operator -= (Vec4i & a, Vec4i const & b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec4i operator -- (Vec4i & a, int) {
    Vec4i a0 = a;
    a = a - 1;
    return a0;
}

// prefix operator --
static inline Vec4i & operator -- (Vec4i & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec4i operator * (Vec4i const & a, Vec4i const & b) {
#if INSTRSET >= 5  // SSE4.1 instruction set
    return _mm_mullo_epi32(a, b);
#else
   __m128i a13    = _mm_shuffle_epi32(a, 0xF5);          // (-,a3,-,a1)
   __m128i b13    = _mm_shuffle_epi32(b, 0xF5);          // (-,b3,-,b1)
   __m128i prod02 = _mm_mul_epu32(a, b);                 // (-,a2*b2,-,a0*b0)
   __m128i prod13 = _mm_mul_epu32(a13, b13);             // (-,a3*b3,-,a1*b1)
   __m128i prod01 = _mm_unpacklo_epi32(prod02,prod13);   // (-,-,a1*b1,a0*b0) 
   __m128i prod23 = _mm_unpackhi_epi32(prod02,prod13);   // (-,-,a3*b3,a2*b2) 
   return           _mm_unpacklo_epi64(prod01,prod23);   // (ab3,ab2,ab1,ab0)
#endif
}

// vector operator *= : multiply
static inline Vec4i & operator *= (Vec4i & a, Vec4i const & b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer
// See bottom of file


// vector operator << : shift left
static inline Vec4i operator << (Vec4i const & a, int32_t b) {
    return _mm_sll_epi32(a,_mm_cvtsi32_si128(b));
}

// vector operator <<= : shift left
static inline Vec4i & operator <<= (Vec4i & a, int32_t b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec4i operator >> (Vec4i const & a, int32_t b) {
    return _mm_sra_epi32(a,_mm_cvtsi32_si128(b));
}

// vector operator >>= : shift right arithmetic
static inline Vec4i & operator >>= (Vec4i & a, int32_t b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec4ib operator == (Vec4i const & a, Vec4i const & b) {
    return _mm_cmpeq_epi32(a, b);
}

// vector operator != : returns true for elements for which a != b
static inline Vec4ib operator != (Vec4i const & a, Vec4i const & b) {
#ifdef __XOP__  // AMD XOP instruction set
    return _mm_comneq_epi32(a,b);
#else  // SSE2 instruction set
    return Vec4ib(Vec4i (~(a == b)));
#endif
}
  
// vector operator > : returns true for elements for which a > b
static inline Vec4ib operator > (Vec4i const & a, Vec4i const & b) {
    return _mm_cmpgt_epi32(a, b);
}

// vector operator < : returns true for elements for which a < b
static inline Vec4ib operator < (Vec4i const & a, Vec4i const & b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec4ib operator >= (Vec4i const & a, Vec4i const & b) {
#ifdef __XOP__  // AMD XOP instruction set
    return _mm_comge_epi32(a,b);
#else  // SSE2 instruction set
    return Vec4ib(Vec4i (~(b > a)));
#endif
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec4ib operator <= (Vec4i const & a, Vec4i const & b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec4i operator & (Vec4i const & a, Vec4i const & b) {
    return Vec4i(Vec128b(a) & Vec128b(b));
}
static inline Vec4i operator && (Vec4i const & a, Vec4i const & b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec4i & operator &= (Vec4i & a, Vec4i const & b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec4i operator | (Vec4i const & a, Vec4i const & b) {
    return Vec4i(Vec128b(a) | Vec128b(b));
}
static inline Vec4i operator || (Vec4i const & a, Vec4i const & b) {
    return a | b;
}
// vector operator |= : bitwise and
static inline Vec4i & operator |= (Vec4i & a, Vec4i const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec4i operator ^ (Vec4i const & a, Vec4i const & b) {
    return Vec4i(Vec128b(a) ^ Vec128b(b));
}
// vector operator ^= : bitwise and
static inline Vec4i & operator ^= (Vec4i & a, Vec4i const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec4i operator ~ (Vec4i const & a) {
    return Vec4i( ~ Vec128b(a));
}

// vector operator ! : returns true for elements == 0
static inline Vec4ib operator ! (Vec4i const & a) {
    return _mm_cmpeq_epi32(a,_mm_setzero_si128());
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 4; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or -1 (true). No other values are allowed.
// (s is signed)
static inline Vec4i select (Vec4ib const & s, Vec4i const & a, Vec4i const & b) {
    return selectb(s,a,b);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec4i if_add (Vec4ib const & f, Vec4i const & a, Vec4i const & b) {
    return a + (Vec4i(f) & b);
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
static inline int32_t horizontal_add (Vec4i const & a) {
#ifdef __XOP__       // AMD XOP instruction set
    __m128i sum1  = _mm_haddq_epi32(a);
    __m128i sum2  = _mm_shuffle_epi32(sum1,0x0E);          // high element
    __m128i sum3  = _mm_add_epi32(sum1,sum2);              // sum
    return          _mm_cvtsi128_si32(sum3);               // truncate to 32 bits
#elif  INSTRSET >= 4  // SSSE3
    __m128i sum1  = _mm_hadd_epi32(a,a);                   // horizontally add 4 elements in 2 steps
    __m128i sum2  = _mm_hadd_epi32(sum1,sum1);
    return          _mm_cvtsi128_si32(sum2);               // 32 bit sum
#else                 // SSE2
    __m128i sum1  = _mm_shuffle_epi32(a,0x0E);             // 2 high elements
    __m128i sum2  = _mm_add_epi32(a,sum1);                 // 2 sums
    __m128i sum3  = _mm_shuffle_epi32(sum2,0x01);          // 1 high element
    __m128i sum4  = _mm_add_epi32(sum2,sum3);              // 2 sums
    return          _mm_cvtsi128_si32(sum4);               // 32 bit sum
#endif
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Elements are sign extended before adding to avoid overflow
static inline int64_t horizontal_add_x (Vec4i const & a) {
#ifdef __XOP__     // AMD XOP instruction set
    __m128i sum1  = _mm_haddq_epi32(a);
#else              // SSE2
    __m128i signs = _mm_srai_epi32(a,31);                  // sign of all elements
    __m128i a01   = _mm_unpacklo_epi32(a,signs);           // sign-extended a0, a1
    __m128i a23   = _mm_unpackhi_epi32(a,signs);           // sign-extended a2, a3
    __m128i sum1  = _mm_add_epi64(a01,a23);                // add
#endif
    __m128i sum2  = _mm_unpackhi_epi64(sum1,sum1);         // high qword
    __m128i sum3  = _mm_add_epi64(sum1,sum2);              // add
#if defined (__x86_64__)
    return          _mm_cvtsi128_si64(sum3);               // 64 bit mode
#else
    union {
        __m128i x;  // silly definition of _mm_storel_epi64 requires __m128i
        int64_t i;
    } u;
    _mm_storel_epi64(&u.x,sum3);
    return u.i;
#endif
}

// function add_saturated: add element by element, signed with saturation
static inline Vec4i add_saturated(Vec4i const & a, Vec4i const & b) {
    __m128i sum    = _mm_add_epi32(a, b);                  // a + b
    __m128i axb    = _mm_xor_si128(a, b);                  // check if a and b have different sign
    __m128i axs    = _mm_xor_si128(a, sum);                // check if a and sum have different sign
    __m128i overf1 = _mm_andnot_si128(axb,axs);            // check if sum has wrong sign
    __m128i overf2 = _mm_srai_epi32(overf1,31);            // -1 if overflow
    __m128i asign  = _mm_srli_epi32(a,31);                 // 1  if a < 0
    __m128i sat1   = _mm_srli_epi32(overf2,1);             // 7FFFFFFF if overflow
    __m128i sat2   = _mm_add_epi32(sat1,asign);            // 7FFFFFFF if positive overflow 80000000 if negative overflow
    return  selectb(overf2,sat2,sum);                      // sum if not overflow, else sat2
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec4i sub_saturated(Vec4i const & a, Vec4i const & b) {
    __m128i diff   = _mm_sub_epi32(a, b);                  // a + b
    __m128i axb    = _mm_xor_si128(a, b);                  // check if a and b have different sign
    __m128i axs    = _mm_xor_si128(a, diff);               // check if a and sum have different sign
    __m128i overf1 = _mm_and_si128(axb,axs);               // check if sum has wrong sign
    __m128i overf2 = _mm_srai_epi32(overf1,31);            // -1 if overflow
    __m128i asign  = _mm_srli_epi32(a,31);                 // 1  if a < 0
    __m128i sat1   = _mm_srli_epi32(overf2,1);             // 7FFFFFFF if overflow
    __m128i sat2   = _mm_add_epi32(sat1,asign);            // 7FFFFFFF if positive overflow 80000000 if negative overflow
    return  selectb(overf2,sat2,diff);                     // diff if not overflow, else sat2
}

// function max: a > b ? a : b
static inline Vec4i max(Vec4i const & a, Vec4i const & b) {
#if INSTRSET >= 5   // SSE4.1 supported
    return _mm_max_epi32(a,b);
#else
    __m128i greater = _mm_cmpgt_epi32(a,b);
    return selectb(greater,a,b);
#endif
}

// function min: a < b ? a : b
static inline Vec4i min(Vec4i const & a, Vec4i const & b) {
#if INSTRSET >= 5   // SSE4.1 supported
    return _mm_min_epi32(a,b);
#else
    __m128i greater = _mm_cmpgt_epi32(a,b);
    return selectb(greater,b,a);
#endif
}

// function abs: a >= 0 ? a : -a
static inline Vec4i abs(Vec4i const & a) {
#if INSTRSET >= 4     // SSSE3 supported
    return _mm_sign_epi32(a,a);
#else                 // SSE2
    __m128i sign = _mm_srai_epi32(a,31);                   // sign of a
    __m128i inv  = _mm_xor_si128(a,sign);                  // invert bits if negative
    return         _mm_sub_epi32(inv,sign);                // add 1
#endif
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec4i abs_saturated(Vec4i const & a) {
    __m128i absa   = abs(a);                               // abs(a)
    __m128i overfl = _mm_srai_epi32(absa,31);              // sign
    return           _mm_add_epi32(absa,overfl);           // subtract 1 if 0x80000000
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec4i rotate_left(Vec4i const & a, int b) {
#ifdef __XOP__  // AMD XOP instruction set
    return _mm_rot_epi32(a,_mm_set1_epi32(b));
#else  // SSE2 instruction set
    __m128i left  = _mm_sll_epi32(a,_mm_cvtsi32_si128(b & 0x1F));      // a << b 
    __m128i right = _mm_srl_epi32(a,_mm_cvtsi32_si128((32-b) & 0x1F)); // a >> (32 - b)
    __m128i rot   = _mm_or_si128(left,right);                          // or
    return  rot;
#endif
}


/*****************************************************************************
*
*          Vector of 4 32-bit unsigned integers
*
*****************************************************************************/

class Vec4ui : public Vec4i {
public:
    // Default constructor:
    Vec4ui() {
    };
    // Constructor to broadcast the same value into all elements:
    Vec4ui(uint32_t i) {
        xmm = _mm_set1_epi32(i);
    };
    // Constructor to build from all elements:
    Vec4ui(uint32_t i0, uint32_t i1, uint32_t i2, uint32_t i3) {
        xmm = _mm_setr_epi32(i0, i1, i2, i3);
    };
    // Constructor to convert from type __m128i used in intrinsics:
    Vec4ui(__m128i const & x) {
        xmm = x;
    };
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec4ui & operator = (__m128i const & x) {
        xmm = x;
        return *this;
    };
    // Member function to load from array (unaligned)
    Vec4ui & load(void const * p) {
        xmm = _mm_loadu_si128((__m128i const*)p);
        return *this;
    }
    // Member function to load from array (aligned)
    Vec4ui & load_a(void const * p) {
        xmm = _mm_load_si128((__m128i const*)p);
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec4ui const & insert(uint32_t index, uint32_t value) {
        Vec4i::insert(index, value);
        return *this;
    }
    // Member function extract a single element from vector
    uint32_t extract(uint32_t index) const {
        return Vec4i::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint32_t operator [] (uint32_t index) const {
        return extract(index);
    }
};

// Define operators for this class

// vector operator + : add
static inline Vec4ui operator + (Vec4ui const & a, Vec4ui const & b) {
    return Vec4ui (Vec4i(a) + Vec4i(b));
}

// vector operator - : subtract
static inline Vec4ui operator - (Vec4ui const & a, Vec4ui const & b) {
    return Vec4ui (Vec4i(a) - Vec4i(b));
}

// vector operator * : multiply
static inline Vec4ui operator * (Vec4ui const & a, Vec4ui const & b) {
    return Vec4ui (Vec4i(a) * Vec4i(b));
}

// vector operator / : divide
// See bottom of file

// vector operator >> : shift right logical all elements
static inline Vec4ui operator >> (Vec4ui const & a, uint32_t b) {
    return _mm_srl_epi32(a,_mm_cvtsi32_si128(b)); 
}

// vector operator >> : shift right logical all elements
static inline Vec4ui operator >> (Vec4ui const & a, int32_t b) {
    return a >> (uint32_t)b;
}

// vector operator >>= : shift right logical
static inline Vec4ui & operator >>= (Vec4ui & a, int b) {
    a = a >> b;
    return a;
}

// vector operator << : shift left all elements
static inline Vec4ui operator << (Vec4ui const & a, uint32_t b) {
    return Vec4ui ((Vec4i)a << (int32_t)b);
}

// vector operator << : shift left all elements
static inline Vec4ui operator << (Vec4ui const & a, int32_t b) {
    return Vec4ui ((Vec4i)a << (int32_t)b);
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec4ib operator > (Vec4ui const & a, Vec4ui const & b) {
#ifdef __XOP__  // AMD XOP instruction set
    return _mm_comgt_epu32(a,b);
#else  // SSE2 instruction set
    __m128i signbit = _mm_set1_epi32(0x80000000);
    __m128i a1      = _mm_xor_si128(a,signbit);
    __m128i b1      = _mm_xor_si128(b,signbit);
    return _mm_cmpgt_epi32(a1,b1);                         // signed compare
#endif
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec4ib operator < (Vec4ui const & a, Vec4ui const & b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec4ib operator >= (Vec4ui const & a, Vec4ui const & b) {
#ifdef __XOP__  // AMD XOP instruction set
    return _mm_comge_epu32(a,b);
#elif INSTRSET >= 5   // SSE4.1
    __m128i max_ab = _mm_max_epu32(a,b);                   // max(a,b), unsigned
    return _mm_cmpeq_epi32(a,max_ab);                      // a == max(a,b)
#else  // SSE2 instruction set
    return Vec4ib(Vec4i (~(b > a)));
#endif
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec4ib operator <= (Vec4ui const & a, Vec4ui const & b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec4ui operator & (Vec4ui const & a, Vec4ui const & b) {
    return Vec4ui(Vec128b(a) & Vec128b(b));
}
static inline Vec4ui operator && (Vec4ui const & a, Vec4ui const & b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec4ui operator | (Vec4ui const & a, Vec4ui const & b) {
    return Vec4ui(Vec128b(a) | Vec128b(b));
}
static inline Vec4ui operator || (Vec4ui const & a, Vec4ui const & b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec4ui operator ^ (Vec4ui const & a, Vec4ui const & b) {
    return Vec4ui(Vec128b(a) ^ Vec128b(b));
}

// vector operator ~ : bitwise not
static inline Vec4ui operator ~ (Vec4ui const & a) {
    return Vec4ui( ~ Vec128b(a));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 8; i++) result[i] = s[i] ? a[i] : b[i];
// Each word in s must be either 0 (false) or -1 (true). No other values are allowed.
// (s is signed)
static inline Vec4ui select (Vec4ib const & s, Vec4ui const & a, Vec4ui const & b) {
    return selectb(s,a,b);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec4ui if_add (Vec4ib const & f, Vec4ui const & a, Vec4ui const & b) {
    return a + (Vec4ui(f) & b);
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
static inline uint32_t horizontal_add (Vec4ui const & a) {
    return horizontal_add((Vec4i)a);
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Elements are zero extended before adding to avoid overflow
static inline uint64_t horizontal_add_x (Vec4ui const & a) {
#ifdef __XOP__     // AMD XOP instruction set
    __m128i sum1  = _mm_haddq_epu32(a);
#else              // SSE2
    __m128i zero  = _mm_setzero_si128();                   // 0
    __m128i a01   = _mm_unpacklo_epi32(a,zero);            // zero-extended a0, a1
    __m128i a23   = _mm_unpackhi_epi32(a,zero);            // zero-extended a2, a3
    __m128i sum1  = _mm_add_epi64(a01,a23);                // add
#endif
    __m128i sum2  = _mm_unpackhi_epi64(sum1,sum1);         // high qword
    __m128i sum3  = _mm_add_epi64(sum1,sum2);              // add
#if defined(_M_AMD64) || defined(_M_X64) || defined(__x86_64__) || defined(__amd64)
    return          _mm_cvtsi128_si64(sum3);               // 64 bit mode
#else
    union {
        __m128i x;  // silly definition of _mm_storel_epi64 requires __m128i
        uint64_t i;
    } u;
    _mm_storel_epi64(&u.x,sum3);
    return u.i;
#endif
}

// function add_saturated: add element by element, unsigned with saturation
static inline Vec4ui add_saturated(Vec4ui const & a, Vec4ui const & b) {
    Vec4ui sum      = a + b;
    Vec4ui aorb     = Vec4ui(a | b);
    Vec4ui overflow = Vec4ui(sum < aorb);                  // overflow if a + b < (a | b)
    return Vec4ui (sum | overflow);                        // return 0xFFFFFFFF if overflow
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec4ui sub_saturated(Vec4ui const & a, Vec4ui const & b) {
    Vec4ui diff      = a - b;
    Vec4ui underflow = Vec4ui(diff > a);                   // underflow if a - b > a
    return _mm_andnot_si128(underflow,diff);               // return 0 if underflow
}

// function max: a > b ? a : b
static inline Vec4ui max(Vec4ui const & a, Vec4ui const & b) {
#if INSTRSET >= 5   // SSE4.1
    return _mm_max_epu32(a,b);
#else  // SSE2
    return select(a > b, a, b);
#endif
}

// function min: a < b ? a : b
static inline Vec4ui min(Vec4ui const & a, Vec4ui const & b) {
#if INSTRSET >= 5   // SSE4.1
    return _mm_min_epu32(a,b);
#else  // SSE2
    return select(a > b, b, a);
#endif
}


/*****************************************************************************
*
*          Vector of 2 64-bit signed integers
*
*****************************************************************************/

class Vec2q : public Vec128b {
public:
    // Default constructor:
    Vec2q() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec2q(int64_t i) {
#if defined (_MSC_VER) && ! defined(__INTEL_COMPILER)
        // MS compiler has no _mm_set1_epi64x in 32 bit mode
#if defined(__x86_64__)                                    // 64 bit mode
#if _MSC_VER < 1700
        __m128i x1 = _mm_cvtsi64_si128(i);                 // 64 bit load
        xmm = _mm_unpacklo_epi64(x1,x1);                   // broadcast
#else
		xmm =  _mm_set1_epi64x(i);
#endif
#else
        union {
            int64_t q[2];
            int32_t r[4];
        } u;
        u.q[0] = u.q[1] = i;
        xmm = _mm_setr_epi32(u.r[0], u.r[1], u.r[2], u.r[3]);
        /*    // this will use an mm register and produce store forwarding stall:
        union {
            __m64 m;
            int64_t ii;
        } u;
        u.ii = i;
        xmm = _mm_set1_epi64(u.m);
		_m_empty();        */

#endif  // __x86_64__
#else   // Other compilers
        xmm = _mm_set1_epi64x(i);   // emmintrin.h
#endif
    }
    // Constructor to build from all elements:
    Vec2q(int64_t i0, int64_t i1) {
#if defined (_MSC_VER) && ! defined(__INTEL_COMPILER)
        // MS compiler has no _mm_set_epi64x in 32 bit mode
#if defined(__x86_64__)                                    // 64 bit mode
#if _MSC_VER < 1700
        __m128i x0 = _mm_cvtsi64_si128(i0);                // 64 bit load
        __m128i x1 = _mm_cvtsi64_si128(i1);                // 64 bit load
        xmm = _mm_unpacklo_epi64(x0,x1);                   // combine
#else
		xmm = _mm_set_epi64x(i1, i0);
#endif
#else   // MS compiler in 32-bit mode
        union {
            int64_t q[2];
            int32_t r[4];
        } u;
        u.q[0] = i0;  u.q[1] = i1;
		// this is inefficient, but other solutions are worse
        xmm = _mm_setr_epi32(u.r[0], u.r[1], u.r[2], u.r[3]);
#endif  // __x86_64__
#else   // Other compilers
        xmm = _mm_set_epi64x(i1, i0);
#endif
    }
    // Constructor to convert from type __m128i used in intrinsics:
    Vec2q(__m128i const & x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec2q & operator = (__m128i const & x) {
        xmm = x;
        return *this;
    }
    // Type cast operator to convert to __m128i used in intrinsics
    operator __m128i() const {
        return xmm;
    }
    // Member function to load from array (unaligned)
    Vec2q & load(void const * p) {
        xmm = _mm_loadu_si128((__m128i const*)p);
        return *this;
    }
    // Member function to load from array (aligned)
    Vec2q & load_a(void const * p) {
        xmm = _mm_load_si128((__m128i const*)p);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec2q & load_partial(int n, void const * p) {
        switch (n) {
        case 0:
            *this = 0;  break;
        case 1:
            // intrinsic for movq is missing!
            *this = Vec2q(*(int64_t*)p, 0);  break;
        case 2:
            load(p);  break;
        default: 
            break;
        }
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
        switch (n) {
        case 1:
            int64_t q[2];
            store(q);
            *(int64_t*)p = q[0];  break;
        case 2:
            store(p);  break;
        default:
            break;
        }
    }
    // cut off vector to n elements. The last 2-n elements are set to zero
    Vec2q & cutoff(int n) {
        *this = Vec16c(xmm).cutoff(n * 8);
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec2q const & insert(uint32_t index, int64_t value) {
#if INSTRSET >= 5 && defined(__x86_64__)  // SSE4.1 supported, 64 bit mode
        if (index == 0) {
            xmm = _mm_insert_epi64(xmm,value,0);
        }
        else {
            xmm = _mm_insert_epi64(xmm,value,1);
        }

#else               // SSE2
#if defined(__x86_64__)                                      // 64 bit mode
        __m128i v = _mm_cvtsi64_si128(value);                // 64 bit load
#else
        union {
            __m128i m;
            int64_t ii;
        } u;
        u.ii = value;
        __m128i v = _mm_loadl_epi64(&u.m);
#endif
        if (index == 0) {
            v = _mm_unpacklo_epi64(v,v);     
            xmm = _mm_unpackhi_epi64(v,xmm);
        }
        else {  // index = 1
            xmm = _mm_unpacklo_epi64(xmm,v);
        }
#endif
        return *this;
    }
    // Member function extract a single element from vector
    int64_t extract(uint32_t index) const {
        int64_t x[2];
        store(x);
        return x[index & 1];
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int64_t operator [] (uint32_t index) const {
        return extract(index);
    }
    static int size() {
        return 2;
    }
};

/*****************************************************************************
*
*          Vec2qb: Vector of 2 Booleans for use with Vec2q and Vec2uq
*
*****************************************************************************/
// Definition will be different for the AVX512 instruction set
class Vec2qb : public Vec2q {
public:
    // Default constructor:
    Vec2qb() {
    }
    // Constructor to build from all elements:
    Vec2qb(bool x0, bool x1) {
        xmm = Vec2q(-int64_t(x0), -int64_t(x1));
    }
    // Constructor to convert from type __m128i used in intrinsics:
    Vec2qb(__m128i const & x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec2qb & operator = (__m128i const & x) {
        xmm = x;
        return *this;
    }
    // Constructor to broadcast scalar value:
    Vec2qb(bool b) : Vec2q(-int64_t(b)) {
    }
    // Assignment operator to broadcast scalar value:
    Vec2qb & operator = (bool b) {
        *this = Vec2qb(b);
        return *this;
    }
private: // Prevent constructing from int, etc.
    Vec2qb(int b);
    Vec2qb & operator = (int x);
public:
    Vec2qb & insert (int index, bool a) {
        Vec2q::insert(index, -(int64_t)a);
        return *this;
    }    
    // Member function extract a single element from vector
    bool extract(uint32_t index) const {
        return Vec2q::extract(index) != 0;
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    bool operator [] (uint32_t index) const {
        return extract(index);
    }
};


/*****************************************************************************
*
*          Define operators for Vec2qb
*
*****************************************************************************/

// vector operator & : bitwise and
static inline Vec2qb operator & (Vec2qb const & a, Vec2qb const & b) {
    return Vec2qb(Vec128b(a) & Vec128b(b));
}
static inline Vec2qb operator && (Vec2qb const & a, Vec2qb const & b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec2qb & operator &= (Vec2qb & a, Vec2qb const & b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec2qb operator | (Vec2qb const & a, Vec2qb const & b) {
    return Vec2qb(Vec128b(a) | Vec128b(b));
}
static inline Vec2qb operator || (Vec2qb const & a, Vec2qb const & b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec2qb & operator |= (Vec2qb & a, Vec2qb const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec2qb operator ^ (Vec2qb const & a, Vec2qb const & b) {
    return Vec2qb(Vec128b(a) ^ Vec128b(b));
}
// vector operator ^= : bitwise xor
static inline Vec2qb & operator ^= (Vec2qb & a, Vec2qb const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec2qb operator ~ (Vec2qb const & a) {
    return Vec2qb( ~ Vec128b(a));
}

// vector operator ! : element not
static inline Vec2qb operator ! (Vec2qb const & a) {
    return ~ a;
}

// vector function andnot
static inline Vec2qb andnot (Vec2qb const & a, Vec2qb const & b) {
    return Vec2qb(andnot(Vec128b(a), Vec128b(b)));
}


/*****************************************************************************
*
*          Operators for Vec2q
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec2q operator + (Vec2q const & a, Vec2q const & b) {
    return _mm_add_epi64(a, b);
}

// vector operator += : add
static inline Vec2q & operator += (Vec2q & a, Vec2q const & b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec2q operator ++ (Vec2q & a, int) {
    Vec2q a0 = a;
    a = a + 1;
    return a0;
}

// prefix operator ++
static inline Vec2q & operator ++ (Vec2q & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec2q operator - (Vec2q const & a, Vec2q const & b) {
    return _mm_sub_epi64(a, b);
}

// vector operator - : unary minus
static inline Vec2q operator - (Vec2q const & a) {
    return _mm_sub_epi64(_mm_setzero_si128(), a);
}

// vector operator -= : subtract
static inline Vec2q & operator -= (Vec2q & a, Vec2q const & b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec2q operator -- (Vec2q & a, int) {
    Vec2q a0 = a;
    a = a - 1;
    return a0;
}

// prefix operator --
static inline Vec2q & operator -- (Vec2q & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec2q operator * (Vec2q const & a, Vec2q const & b) {
#if INSTRSET >= 5   // SSE4.1 supported
    // instruction does not exist. Split into 32-bit multiplies
    __m128i bswap   = _mm_shuffle_epi32(b,0xB1);           // b0H,b0L,b1H,b1L (swap H<->L)
    __m128i prodlh  = _mm_mullo_epi32(a,bswap);            // a0Lb0H,a0Hb0L,a1Lb1H,a1Hb1L, 32 bit L*H products
    __m128i zero    = _mm_setzero_si128();                 // 0
    __m128i prodlh2 = _mm_hadd_epi32(prodlh,zero);         // a0Lb0H+a0Hb0L,a1Lb1H+a1Hb1L,0,0
    __m128i prodlh3 = _mm_shuffle_epi32(prodlh2,0x73);     // 0, a0Lb0H+a0Hb0L, 0, a1Lb1H+a1Hb1L
    __m128i prodll  = _mm_mul_epu32(a,b);                  // a0Lb0L,a1Lb1L, 64 bit unsigned products
    __m128i prod    = _mm_add_epi64(prodll,prodlh3);       // a0Lb0L+(a0Lb0H+a0Hb0L)<<32, a1Lb1L+(a1Lb1H+a1Hb1L)<<32
    return  prod;
#else               // SSE2
    int64_t aa[2], bb[2];
    a.store(aa);                                           // split into elements
    b.store(bb);
    return Vec2q(aa[0]*bb[0], aa[1]*bb[1]);                // multiply elements separetely
#endif
}

// vector operator *= : multiply
static inline Vec2q & operator *= (Vec2q & a, Vec2q const & b) {
    a = a * b;
    return a;
}

// vector operator << : shift left
static inline Vec2q operator << (Vec2q const & a, int32_t b) {
    return _mm_sll_epi64(a,_mm_cvtsi32_si128(b));
}

// vector operator <<= : shift left
static inline Vec2q & operator <<= (Vec2q & a, int32_t b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec2q operator >> (Vec2q const & a, int32_t b) {
    // instruction does not exist. Split into 32-bit shifts
    if (b <= 32) {
        __m128i bb   = _mm_cvtsi32_si128(b);               // b
        __m128i sra  = _mm_sra_epi32(a,bb);                // a >> b signed dwords
        __m128i srl  = _mm_srl_epi64(a,bb);                // a >> b unsigned qwords
        __m128i mask = _mm_setr_epi32(0,-1,0,-1);          // mask for signed high part
        return  selectb(mask,sra,srl);
    }
    else {  // b > 32
        __m128i bm32 = _mm_cvtsi32_si128(b-32);            // b - 32
        __m128i sign = _mm_srai_epi32(a,31);               // sign of a
        __m128i sra2 = _mm_sra_epi32(a,bm32);              // a >> (b-32) signed dwords
        __m128i sra3 = _mm_srli_epi64(sra2,32);            // a >> (b-32) >> 32 (second shift unsigned qword)
        __m128i mask = _mm_setr_epi32(0,-1,0,-1);          // mask for high part containing only sign
        return  selectb(mask,sign,sra3);
    }
}

// vector operator >>= : shift right arithmetic
static inline Vec2q & operator >>= (Vec2q & a, int32_t b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec2qb operator == (Vec2q const & a, Vec2q const & b) {
#if INSTRSET >= 5   // SSE4.1 supported
    return _mm_cmpeq_epi64(a, b);
#else               // SSE2
    // no 64 compare instruction. Do two 32 bit compares
    __m128i com32  = _mm_cmpeq_epi32(a,b);                 // 32 bit compares
    __m128i com32s = _mm_shuffle_epi32(com32,0xB1);        // swap low and high dwords
    __m128i test   = _mm_and_si128(com32,com32s);          // low & high
    __m128i teste  = _mm_srai_epi32(test,31);              // extend sign bit to 32 bits
    __m128i testee = _mm_shuffle_epi32(teste,0xF5);        // extend sign bit to 64 bits
    return  Vec2qb(Vec2q(testee));
#endif
}

// vector operator != : returns true for elements for which a != b
static inline Vec2qb operator != (Vec2q const & a, Vec2q const & b) {
#ifdef __XOP__  // AMD XOP instruction set
    return Vec2q(_mm_comneq_epi64(a,b));
#else  // SSE2 instruction set
    return Vec2qb(Vec2q(~(a == b)));
#endif
}
  
// vector operator < : returns true for elements for which a < b
static inline Vec2qb operator < (Vec2q const & a, Vec2q const & b) {
#if INSTRSET >= 6   // SSE4.2 supported
    return Vec2qb(Vec2q(_mm_cmpgt_epi64(b, a)));
#else               // SSE2
    // no 64 compare instruction. Subtract
    __m128i s      = _mm_sub_epi64(a,b);                   // a-b
    // a < b if a and b have same sign and s < 0 or (a < 0 and b >= 0)
    // The latter () corrects for overflow
    __m128i axb    = _mm_xor_si128(a,b);                   // a ^ b
    __m128i anb    = _mm_andnot_si128(b,a);                // a & ~b
    __m128i snaxb  = _mm_andnot_si128(axb,s);              // s & ~(a ^ b)
    __m128i or1    = _mm_or_si128(anb,snaxb);              // (a & ~b) | (s & ~(a ^ b))
    __m128i teste  = _mm_srai_epi32(or1,31);               // extend sign bit to 32 bits
    __m128i testee = _mm_shuffle_epi32(teste,0xF5);        // extend sign bit to 64 bits
    return  testee;
#endif
}

// vector operator > : returns true for elements for which a > b
static inline Vec2qb operator > (Vec2q const & a, Vec2q const & b) {
    return b < a;
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec2qb operator >= (Vec2q const & a, Vec2q const & b) {
#ifdef __XOP__  // AMD XOP instruction set
    return Vec2q(_mm_comge_epi64(a,b));
#else  // SSE2 instruction set
    return Vec2qb(Vec2q(~(a < b)));
#endif
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec2qb operator <= (Vec2q const & a, Vec2q const & b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec2q operator & (Vec2q const & a, Vec2q const & b) {
    return Vec2q(Vec128b(a) & Vec128b(b));
}
static inline Vec2q operator && (Vec2q const & a, Vec2q const & b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec2q & operator &= (Vec2q & a, Vec2q const & b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec2q operator | (Vec2q const & a, Vec2q const & b) {
    return Vec2q(Vec128b(a) | Vec128b(b));
}
static inline Vec2q operator || (Vec2q const & a, Vec2q const & b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec2q & operator |= (Vec2q & a, Vec2q const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec2q operator ^ (Vec2q const & a, Vec2q const & b) {
    return Vec2q(Vec128b(a) ^ Vec128b(b));
}
// vector operator ^= : bitwise xor
static inline Vec2q & operator ^= (Vec2q & a, Vec2q const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec2q operator ~ (Vec2q const & a) {
    return Vec2q( ~ Vec128b(a));
}

// vector operator ! : logical not, returns true for elements == 0
static inline Vec2qb operator ! (Vec2q const & a) {
    return a == Vec2q(_mm_setzero_si128());
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 8; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or -1 (true). No other values are allowed.
// (s is signed)
static inline Vec2q select (Vec2qb const & s, Vec2q const & a, Vec2q const & b) {
    return selectb(s,a,b);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec2q if_add (Vec2qb const & f, Vec2q const & a, Vec2q const & b) {
    return a + (Vec2q(f) & b);
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
static inline int64_t horizontal_add (Vec2q const & a) {
    __m128i sum1  = _mm_shuffle_epi32(a,0x0E);             // high element
    __m128i sum2  = _mm_add_epi64(a,sum1);                 // sum
#if defined(__x86_64__)
    return          _mm_cvtsi128_si64(sum2);               // 64 bit mode
#else
    union {
        __m128i x;  // silly definition of _mm_storel_epi64 requires __m128i
        int64_t i;
    } u;
    _mm_storel_epi64(&u.x,sum2);
    return u.i;
#endif
}

// function max: a > b ? a : b
static inline Vec2q max(Vec2q const & a, Vec2q const & b) {
    return select(a > b, a, b);
}

// function min: a < b ? a : b
static inline Vec2q min(Vec2q const & a, Vec2q const & b) {
    return select(a < b, a, b);
}

// function abs: a >= 0 ? a : -a
static inline Vec2q abs(Vec2q const & a) {
#if INSTRSET >= 6     // SSE4.2 supported
    __m128i sign  = _mm_cmpgt_epi64(_mm_setzero_si128(),a);// 0 > a
#else                 // SSE2
    __m128i signh = _mm_srai_epi32(a,31);                  // sign in high dword
    __m128i sign  = _mm_shuffle_epi32(signh,0xF5);         // copy sign to low dword
#endif
    __m128i inv   = _mm_xor_si128(a,sign);                 // invert bits if negative
    return          _mm_sub_epi64(inv,sign);               // add 1
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec2q abs_saturated(Vec2q const & a) {
    __m128i absa   = abs(a);                               // abs(a)
#if INSTRSET >= 6     // SSE4.2 supported
    __m128i overfl = _mm_cmpgt_epi64(_mm_setzero_si128(),absa);// 0 > a
#else                 // SSE2
    __m128i signh = _mm_srai_epi32(absa,31);               // sign in high dword
    __m128i overfl= _mm_shuffle_epi32(signh,0xF5);         // copy sign to low dword
#endif
    return           _mm_add_epi64(absa,overfl);           // subtract 1 if 0x8000000000000000
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec2q rotate_left(Vec2q const & a, int b) {
#ifdef __XOP__  // AMD XOP instruction set
    return _mm_rot_epi64(a,Vec2q(b));
#else  // SSE2 instruction set
    __m128i left  = _mm_sll_epi64(a,_mm_cvtsi32_si128(b & 0x3F));      // a << b 
    __m128i right = _mm_srl_epi64(a,_mm_cvtsi32_si128((64-b) & 0x3F)); // a >> (64 - b)
    __m128i rot   = _mm_or_si128(left,right);                          // or
    return  rot;
#endif
}


/*****************************************************************************
*
*          Vector of 2 64-bit unsigned integers
*
*****************************************************************************/

class Vec2uq : public Vec2q {
public:
    // Default constructor:
    Vec2uq() {
    };
    // Constructor to broadcast the same value into all elements:
    Vec2uq(uint64_t i) {
        xmm = Vec2q(i);
    };
    // Constructor to build from all elements:
    Vec2uq(uint64_t i0, uint64_t i1) {
        xmm = Vec2q(i0, i1);
    };
    // Constructor to convert from type __m128i used in intrinsics:
    Vec2uq(__m128i const & x) {
        xmm = x;
    };
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec2uq & operator = (__m128i const & x) {
        xmm = x;
        return *this;
    };
    // Member function to load from array (unaligned)
    Vec2uq & load(void const * p) {
        xmm = _mm_loadu_si128((__m128i const*)p);
        return *this;
    }
    // Member function to load from array (aligned)
    Vec2uq & load_a(void const * p) {
        xmm = _mm_load_si128((__m128i const*)p);
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec2uq const & insert(uint32_t index, uint64_t value) {
        Vec2q::insert(index, value);
        return *this;
    }
    // Member function extract a single element from vector
    uint64_t extract(uint32_t index) const {
        return Vec2q::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint64_t operator [] (uint32_t index) const {
        return extract(index);
    }
};

// Define operators for this class

// vector operator + : add
static inline Vec2uq operator + (Vec2uq const & a, Vec2uq const & b) {
    return Vec2uq (Vec2q(a) + Vec2q(b));
}

// vector operator - : subtract
static inline Vec2uq operator - (Vec2uq const & a, Vec2uq const & b) {
    return Vec2uq (Vec2q(a) - Vec2q(b));
}

// vector operator * : multiply element by element
static inline Vec2uq operator * (Vec2uq const & a, Vec2uq const & b) {
    return Vec2uq (Vec2q(a) * Vec2q(b));
}

// vector operator >> : shift right logical all elements
static inline Vec2uq operator >> (Vec2uq const & a, uint32_t b) {
    return _mm_srl_epi64(a,_mm_cvtsi32_si128(b)); 
}

// vector operator >> : shift right logical all elements
static inline Vec2uq operator >> (Vec2uq const & a, int32_t b) {
    return a >> (uint32_t)b;
}

// vector operator >>= : shift right logical
static inline Vec2uq & operator >>= (Vec2uq & a, int b) {
    a = a >> b;
    return a;
}

// vector operator << : shift left all elements
static inline Vec2uq operator << (Vec2uq const & a, uint32_t b) {
    return Vec2uq ((Vec2q)a << (int32_t)b);
}

// vector operator << : shift left all elements
static inline Vec2uq operator << (Vec2uq const & a, int32_t b) {
    return Vec2uq ((Vec2q)a << b);
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec2qb operator > (Vec2uq const & a, Vec2uq const & b) {
#ifdef __XOP__  // AMD XOP instruction set
    return Vec2q(_mm_comgt_epu64(a,b));
#else  // SSE2 instruction set
    __m128i sign32  = _mm_set1_epi32(0x80000000);          // sign bit of each dword
    __m128i aflip   = _mm_xor_si128(a,sign32);             // a with sign bits flipped
    __m128i bflip   = _mm_xor_si128(b,sign32);             // b with sign bits flipped
    __m128i equal   = _mm_cmpeq_epi32(a,b);                // a == b, dwords
    __m128i bigger  = _mm_cmpgt_epi32(aflip,bflip);        // a > b, dwords
    __m128i biggerl = _mm_shuffle_epi32(bigger,0xA0);      // a > b, low dwords copied to high dwords
    __m128i eqbig   = _mm_and_si128(equal,biggerl);        // high part equal and low part bigger
    __m128i hibig   = _mm_or_si128(bigger,eqbig);          // high part bigger or high part equal and low part bigger
    __m128i big     = _mm_shuffle_epi32(hibig,0xF5);       // result copied to low part
    return  Vec2qb(Vec2q(big));
#endif
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec2qb operator < (Vec2uq const & a, Vec2uq const & b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec2qb operator >= (Vec2uq const & a, Vec2uq const & b) {
#ifdef __XOP__  // AMD XOP instruction set
    return Vec2q(_mm_comge_epu64(a,b));
#else  // SSE2 instruction set
    return  Vec2qb(Vec2q(~(b > a)));
#endif
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec2qb operator <= (Vec2uq const & a, Vec2uq const & b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec2uq operator & (Vec2uq const & a, Vec2uq const & b) {
    return Vec2uq(Vec128b(a) & Vec128b(b));
}
static inline Vec2uq operator && (Vec2uq const & a, Vec2uq const & b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec2uq operator | (Vec2uq const & a, Vec2uq const & b) {
    return Vec2uq(Vec128b(a) | Vec128b(b));
}
static inline Vec2uq operator || (Vec2uq const & a, Vec2uq const & b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec2uq operator ^ (Vec2uq const & a, Vec2uq const & b) {
    return Vec2uq(Vec128b(a) ^ Vec128b(b));
}

// vector operator ~ : bitwise not
static inline Vec2uq operator ~ (Vec2uq const & a) {
    return Vec2uq( ~ Vec128b(a));
}


// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 2; i++) result[i] = s[i] ? a[i] : b[i];
// Each word in s must be either 0 (false) or -1 (true). No other values are allowed.
// (s is signed)
static inline Vec2uq select (Vec2qb const & s, Vec2uq const & a, Vec2uq const & b) {
    return selectb(s,a,b);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec2uq if_add (Vec2qb const & f, Vec2uq const & a, Vec2uq const & b) {
    return a + (Vec2uq(f) & b);
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
static inline uint64_t horizontal_add (Vec2uq const & a) {
    return horizontal_add((Vec2q)a);
}

// function max: a > b ? a : b
static inline Vec2uq max(Vec2uq const & a, Vec2uq const & b) {
    return select(a > b, a, b);
}

// function min: a < b ? a : b
static inline Vec2uq min(Vec2uq const & a, Vec2uq const & b) {
    return select(a > b, b, a);
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
* constants. Each template parameter is an index to the element you want to 
* select. A negative index will generate zero. an index of -256 means don't care.
*
* Example:
* Vec4i a(10,11,12,13);         // a is (10,11,12,13)
* Vec4i b, c;
* b = permute4i<0,0,2,2>(a);    // b is (10,10,12,12)
* c = permute4i<3,2,-1,-1>(a);  // c is (13,12, 0, 0)
*
* The permute functions for vectors of 8-bit integers are inefficient if 
* the SSSE3 instruction set or later is not enabled.
*
* A lot of the code here is metaprogramming aiming to find the instructions
* that best fit the template parameters and instruction set. The metacode
* will be reduced out to leave only a few vector instructions in release
* mode with optimization on.
*****************************************************************************/

template <int i0, int i1>
static inline Vec2q permute2q(Vec2q const & a) {
    if (i0 == 0) {
        if (i1 == 0) {       // 0,0
            return _mm_unpacklo_epi64(a, a);
        }
        else if (i1 == 1 || i1 == -0x100) {  // 0,1
            return a;
        }
        else {               // 0,-1
            // return _mm_mov_epi64(a); // doesn't work with MS VS 2008
            return _mm_and_si128(a, constant4i<-1,-1,0,0>());
        }
    }
    else if (i0 == 1) {
        if (i1 == 0) {       // 1,0
            return _mm_shuffle_epi32(a, 0x4E);
        }
        else if (i1 == 1) {  // 1,1
            return _mm_unpackhi_epi64(a, a);
        }
        else {               // 1,-1
            return _mm_srli_si128(a, 8);
        }
    }
    else { // i0 < 0
        if (i1 == 0) {       // -1,0
            return _mm_slli_si128(a, 8);
        }
        else if (i1 == 1) {  // -1,1
            if (i0 == -0x100) return a;
            return _mm_and_si128(a, constant4i<0,0,-1,-1>());
        }
        else {               // -1,-1
            return _mm_setzero_si128();
        }
    }
}

template <int i0, int i1>
static inline Vec2uq permute2uq(Vec2uq const & a) {
    return Vec2uq (permute2q <i0, i1> ((__m128i)a));
}

// permute vector Vec4i
template <int i0, int i1, int i2, int i3>
static inline Vec4i permute4i(Vec4i const & a) {

    // Combine all the indexes into a single bitfield, with 4 bits for each
    const int m1 = (i0&3) | (i1&3)<<4 | (i2&3)<<8 | (i3&3)<<12; 

    // Mask to zero out negative indexes
    const int mz = (i0<0?0:0xF) | (i1<0?0:0xF)<<4 | (i2<0?0:0xF)<<8 | (i3<0?0:0xF)<<12;

    // Mask indicating required zeroing of all indexes, with 4 bits for each, 0 for index = -1, 0xF for index >= 0 or -256
    const int ssz = ((i0 & 0x80) ? 0 : 0xF) | ((i1 & 0x80) ? 0 : 0xF) << 4 | ((i2 & 0x80) ? 0 : 0xF) << 8 | ((i3 & 0x80) ? 0 : 0xF) << 12;

    // Mask indicating 0 for don't care, 0xF for non-negative value of required zeroing
    const int md = mz | ~ ssz;

    // Test if permutation needed
    const bool do_shuffle = ((m1 ^ 0x00003210) & mz) != 0;

    // is zeroing needed
    const bool do_zero    = (ssz != 0xFFFF);

    if (mz == 0) {
        return _mm_setzero_si128();    // special case: all zero or don't care
    }
    // Test if we can do with 64-bit permute only
    if ((m1 & 0x0101 & mz) == 0        // even indexes are even or negative
    && (~m1 & 0x1010 & mz) == 0        // odd  indexes are odd  or negative
    && ((m1 ^ ((m1 + 0x0101) << 4)) & 0xF0F0 & mz & (mz << 4)) == 0  // odd index == preceding even index +1 or at least one of them negative
    && ((mz ^ (mz << 4)) & 0xF0F0 & md & md << 4) == 0) {      // each pair of indexes are both negative or both positive or one of them don't care
        const int j0 = i0 >= 0 ? i0 / 2 : (i0 & 0x80) ? i0 : i1 >= 0 ? i1/2 : i1;
        const int j1 = i2 >= 0 ? i2 / 2 : (i2 & 0x80) ? i2 : i3 >= 0 ? i3/2 : i3;
        return Vec4i(permute2q<j0, j1> (Vec2q(a)));    // 64 bit permute
    }
#if  INSTRSET >= 4  // SSSE3
    if (do_shuffle && do_zero) {
        // With SSSE3 we can do both with the PSHUFB instruction
        const int j0 = (i0 & 3) << 2;
        const int j1 = (i1 & 3) << 2;
        const int j2 = (i2 & 3) << 2;
        const int j3 = (i3 & 3) << 2;
        __m128i mask1 = constant4i <
            i0 < 0 ? -1 : j0 | (j0+1)<<8 | (j0+2)<<16 | (j0+3) << 24,
            i1 < 0 ? -1 : j1 | (j1+1)<<8 | (j1+2)<<16 | (j1+3) << 24,
            i2 < 0 ? -1 : j2 | (j2+1)<<8 | (j2+2)<<16 | (j2+3) << 24,
            i3 < 0 ? -1 : j3 | (j3+1)<<8 | (j3+2)<<16 | (j3+3) << 24 > ();
        return _mm_shuffle_epi8(a,mask1);
    }
#endif
    __m128i t1;

    if (do_shuffle) {  // permute
        t1 = _mm_shuffle_epi32(a, (i0&3) | (i1&3)<<2 | (i2&3)<<4 | (i3&3)<<6);
    }
    else {
        t1 = a;
    }
    if (do_zero) {     // set some elements to zero
        __m128i mask2 = constant4i< -int(i0>=0), -int(i1>=0), -int(i2>=0), -int(i3>=0) >();
        t1 = _mm_and_si128(t1,mask2);
    }
    return t1;
}

template <int i0, int i1, int i2, int i3>
static inline Vec4ui permute4ui(Vec4ui const & a) {
    return Vec4ui (permute4i <i0,i1,i2,i3> (a));
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8s permute8s(Vec8s const & a) {
    if ((i0 & i1 & i2 & i3 & i4 & i5 & i6 & i7) < 0) {
        return _mm_setzero_si128();  // special case: all zero
    }
#if  INSTRSET >= 4  // SSSE3

    // special case: rotate
    if (i0>=0 && i0 < 8 && i1==((i0+1)&7) && i2==((i0+2)&7) && i3==((i0+3)&7) && i4==((i0+4)&7) && i5==((i0+5)&7) && i6==((i0+6)&7) && i7==((i0+7)&7)) {
        if (i0 == 0) return a;  // do nothing
        return _mm_alignr_epi8(a, a, (i0 & 7) * 2);
    }    
    
    // General case: Use PSHUFB
    const int j0 = i0 < 0 ? 0xFFFF : ( (i0 & 7) * 2 | ((i0 & 7) * 2 + 1) << 8 );
    const int j1 = i1 < 0 ? 0xFFFF : ( (i1 & 7) * 2 | ((i1 & 7) * 2 + 1) << 8 );
    const int j2 = i2 < 0 ? 0xFFFF : ( (i2 & 7) * 2 | ((i2 & 7) * 2 + 1) << 8 );
    const int j3 = i3 < 0 ? 0xFFFF : ( (i3 & 7) * 2 | ((i3 & 7) * 2 + 1) << 8 );
    const int j4 = i4 < 0 ? 0xFFFF : ( (i4 & 7) * 2 | ((i4 & 7) * 2 + 1) << 8 );
    const int j5 = i5 < 0 ? 0xFFFF : ( (i5 & 7) * 2 | ((i5 & 7) * 2 + 1) << 8 );
    const int j6 = i6 < 0 ? 0xFFFF : ( (i6 & 7) * 2 | ((i6 & 7) * 2 + 1) << 8 );
    const int j7 = i7 < 0 ? 0xFFFF : ( (i7 & 7) * 2 | ((i7 & 7) * 2 + 1) << 8 );
    __m128i mask = constant4i < j0 | j1 << 16, j2 | j3 << 16, j4 | j5 << 16, j6 | j7 << 16 > ();
    return _mm_shuffle_epi8(a,mask);

#else   // SSE2 has no simple solution. Find the optimal permute method.
    // Without proper metaprogramming features, we have to use constant expressions 
    // and if-statements to make sure these calculations are resolved at compile time.
    // All this should produce at most 8 instructions in the final code, depending
    // on the template parameters.

    // Temporary vectors
    __m128i t1, t2, t3, t4, t5, t6, t7;

    // Combine all the indexes into a single bitfield, with 4 bits for each
    const int m1 = (i0&7) | (i1&7)<<4 | (i2&7)<<8 | (i3&7)<<12 
        | (i4&7)<<16 | (i5&7)<<20 | (i6&7)<<24 | (i7&7)<<28; 

    // Mask to zero out negative indexes
    const int m2 = (i0<0?0:0xF) | (i1<0?0:0xF)<<4 | (i2<0?0:0xF)<<8 | (i3<0?0:0xF)<<12
        | (i4<0?0:0xF)<<16 | (i5<0?0:0xF)<<20 | (i6<0?0:0xF)<<24 | (i7<0?0:0xF)<<28;

    // Test if we can do without permute
    const bool case0 = ((m1 ^ 0x76543210) & m2) == 0; // all indexes point to their own place or negative

    // Test if we can do with 32-bit permute only
    const bool case1 = 
        (m1 & 0x01010101 & m2) == 0        // even indexes are even or negative
        && (~m1 & 0x10101010 & m2) == 0    // odd  indexes are odd  or negative
        && ((m1 ^ ((m1 + 0x01010101) << 4)) & 0xF0F0F0F0 & m2 & (m2 << 4)) == 0; // odd index == preceding even index +1 or at least one of them negative

    // Test if we can do with 16-bit permute only
    const bool case2 = 
        (((m1 & 0x44444444) ^ 0x44440000) & m2) == 0;  // indexes 0-3 point to lower 64 bits, 1-7 to higher 64 bits, or negative

    if (case0) {
        // no permute needed
        t7 = a;
    }
    else if (case1) {
        // 32 bit permute only
        const int j0 = i0 >= 0 ? i0/2 : i1 >= 0 ? i1/2 : 0;
        const int j1 = i2 >= 0 ? i2/2 : i3 >= 0 ? i3/2 : 0;
        const int j2 = i4 >= 0 ? i4/2 : i5 >= 0 ? i5/2 : 0;
        const int j3 = i6 >= 0 ? i6/2 : i7 >= 0 ? i7/2 : 0;
        t7 = _mm_shuffle_epi32(a, (j0&3) | (j1&3)<<2 | (j2&3)<<4 | (j3&3)<<6 );
    }
    else if (case2) {
        // 16 bit permute only
        const int j0 = i0 >= 0 ? i0&3 : 0;
        const int j1 = i1 >= 0 ? i1&3 : 1;
        const int j2 = i2 >= 0 ? i2&3 : 2;
        const int j3 = i3 >= 0 ? i3&3 : 3;
        const int j4 = i4 >= 0 ? i4&3 : 0;
        const int j5 = i5 >= 0 ? i5&3 : 1;
        const int j6 = i6 >= 0 ? i6&3 : 2;
        const int j7 = i7 >= 0 ? i7&3 : 3;
        if (j0!=0 || j1!=1 || j2!=2 || j3!=3) {            
            t1 = _mm_shufflelo_epi16(a, j0 | j1<<2 | j2<<4 | j3<<6);
        }
        else t1 = a;
        if (j4!=0 || j5!=1 || j6!=2 || j7!=3) {            
            t7 = _mm_shufflehi_epi16(t1, j4 | j5<<2 | j6<<4 | j7<<6);
        }
        else t7 = t1;
    }
    else {
        // Need at least two permute steps

        // Index to where each dword of a is needed
        const int nn = (m1 & 0x66666666) | 0x88888888; // indicate which dwords are needed
        const int n0 = ((((uint32_t)(nn ^ 0x00000000) - 0x22222222) & 0x88888888) ^ 0x88888888) & m2;
        const int n1 = ((((uint32_t)(nn ^ 0x22222222) - 0x22222222) & 0x88888888) ^ 0x88888888) & m2;
        const int n2 = ((((uint32_t)(nn ^ 0x44444444) - 0x22222222) & 0x88888888) ^ 0x88888888) & m2;
        const int n3 = ((((uint32_t)(nn ^ 0x66666666) - 0x22222222) & 0x88888888) ^ 0x88888888) & m2;
        // indicate which dwords are needed in low half
        const int l0 = (n0 & 0xFFFF) != 0;
        const int l1 = (n1 & 0xFFFF) != 0;
        const int l2 = (n2 & 0xFFFF) != 0;
        const int l3 = (n3 & 0xFFFF) != 0;
        // indicate which dwords are needed in high half
        const int h0 = (n0 & 0xFFFF0000) != 0;
        const int h1 = (n1 & 0xFFFF0000) != 0;
        const int h2 = (n2 & 0xFFFF0000) != 0;
        const int h3 = (n3 & 0xFFFF0000) != 0;

        // Test if we can do with two permute steps
        const bool case3 = l0 + l1 + l2 + l3 <= 2  &&  h0 + h1 + h2 + h3 <= 2;

        if (case3) {
            // one 32-bit permute followed by one 16-bit permute in each half.
            // Find permute indices for 32-bit permute
            const int j0 = l0 ? 0 : l1 ? 1 : l2 ? 2 : 3;
            const int j1 = l3 ? 3 : l2 ? 2 : l1 ? 1 : 0;
            const int j2 = h0 ? 0 : h1 ? 1 : h2 ? 2 : 3;
            const int j3 = h3 ? 3 : h2 ? 2 : h1 ? 1 : 0;

            // Find permute indices for low 16-bit permute
            const int r0 = i0 < 0 ? 0 : (i0>>1 == j0 ? 0 : 2) + (i0 & 1);
            const int r1 = i1 < 0 ? 1 : (i1>>1 == j0 ? 0 : 2) + (i1 & 1);
            const int r2 = i2 < 0 ? 2 : (i2>>1 == j1 ? 2 : 0) + (i2 & 1);
            const int r3 = i3 < 0 ? 3 : (i3>>1 == j1 ? 2 : 0) + (i3 & 1);

            // Find permute indices for high 16-bit permute
            const int s0 = i4 < 0 ? 0 : (i4>>1 == j2 ? 0 : 2) + (i4 & 1);
            const int s1 = i5 < 0 ? 1 : (i5>>1 == j2 ? 0 : 2) + (i5 & 1);
            const int s2 = i6 < 0 ? 2 : (i6>>1 == j3 ? 2 : 0) + (i6 & 1);
            const int s3 = i7 < 0 ? 3 : (i7>>1 == j3 ? 2 : 0) + (i7 & 1);

            // 32-bit permute
            t1 = _mm_shuffle_epi32 (a, j0 | j1<<2 | j2<<4 | j3<<6);
            // 16-bit permutes
            if (r0!=0 || r1!=1 || r2!=2 || r3!=3) {  // 16 bit permute of low  half
                t2 = _mm_shufflelo_epi16(t1, r0 | r1<<2 | r2<<4 | r3<<6);
            }
            else t2 = t1;
            if (s0!=0 || s1!=1 || s2!=2 || s3!=3) {  // 16 bit permute of high half                
                t7 = _mm_shufflehi_epi16(t2, s0 | s1<<2 | s2<<4 | s3<<6);
            }
            else t7 = t2;
        }
        else {
            // Worst case. We need two sets of 16-bit permutes
            t1 = _mm_shuffle_epi32(a, 0x4E);  // swap low and high 64-bits

            // Find permute indices for low 16-bit permute from swapped t1
            const int r0 = i0 < 4 ? 0 : i0 & 3;
            const int r1 = i1 < 4 ? 1 : i1 & 3;
            const int r2 = i2 < 4 ? 2 : i2 & 3;
            const int r3 = i3 < 4 ? 3 : i3 & 3;
            // Find permute indices for high 16-bit permute from swapped t1
            const int s0 = i4 < 0 || i4 >= 4 ? 0 : i4 & 3;
            const int s1 = i5 < 0 || i5 >= 4 ? 1 : i5 & 3;
            const int s2 = i6 < 0 || i6 >= 4 ? 2 : i6 & 3;
            const int s3 = i7 < 0 || i7 >= 4 ? 3 : i7 & 3;
            // Find permute indices for low 16-bit permute from direct a
            const int u0 = i0 < 0 || i0 >= 4 ? 0 : i0 & 3;
            const int u1 = i1 < 0 || i1 >= 4 ? 1 : i1 & 3;
            const int u2 = i2 < 0 || i2 >= 4 ? 2 : i2 & 3;
            const int u3 = i3 < 0 || i3 >= 4 ? 3 : i3 & 3;
            // Find permute indices for high 16-bit permute from direct a
            const int v0 = i4 < 4 ? 0 : i4 & 3;
            const int v1 = i5 < 4 ? 1 : i5 & 3;
            const int v2 = i6 < 4 ? 2 : i6 & 3;
            const int v3 = i7 < 4 ? 3 : i7 & 3;

            // 16-bit permutes
            if (r0!=0 || r1!=1 || r2!=2 || r3!=3) {  // 16 bit permute of low  half
                t2 = _mm_shufflelo_epi16(t1, r0 | r1<<2 | r2<<4 | r3<<6);
            }
            else t2 = t1;
            if (u0!=0 || u1!=1 || u2!=2 || u3!=3) {  // 16 bit permute of low  half
                t3 = _mm_shufflelo_epi16(a, u0 | u1<<2 | u2<<4 | u3<<6);
            }
            else t3 = a;
            if (s0!=0 || s1!=1 || s2!=2 || s3!=3) {  // 16 bit permute of low  half
                t4 = _mm_shufflehi_epi16(t2, s0 | s1<<2 | s2<<4 | s3<<6);
            }
            else t4 = t2;
            if (v0!=0 || v1!=1 || v2!=2 || v3!=3) {  // 16 bit permute of low  half
                t5 = _mm_shufflehi_epi16(t3, v0 | v1<<2 | v2<<4 | v3<<6);
            }
            else t5 = t3;
            // merge data from t4 and t5
            t6  = constant4i <
                ((i0 & 4) ? 0xFFFF : 0) | ((i1 & 4) ? 0xFFFF0000 : 0),
                ((i2 & 4) ? 0xFFFF : 0) | ((i3 & 4) ? 0xFFFF0000 : 0),
                ((i4 & 4) ? 0 : 0xFFFF) | ((i5 & 4) ? 0 : 0xFFFF0000),
                ((i6 & 4) ? 0 : 0xFFFF) | ((i7 & 4) ? 0 : 0xFFFF0000) > ();
            t7 = selectb(t6,t4,t5);  // select between permuted data t4 and t5
        }
    }
    // Set any elements to zero if required
    if (m2 != -1 && ((i0 | i1 | i2 | i3 | i4 | i5 | i6 | i7) & 0x80)) {
        // some elements need to be set to 0
        __m128i mask = constant4i <
            (i0 < 0 ? 0xFFFF0000 : -1) & (i1 < 0 ? 0x0000FFFF : -1),
            (i2 < 0 ? 0xFFFF0000 : -1) & (i3 < 0 ? 0x0000FFFF : -1),
            (i4 < 0 ? 0xFFFF0000 : -1) & (i5 < 0 ? 0x0000FFFF : -1),
            (i6 < 0 ? 0xFFFF0000 : -1) & (i7 < 0 ? 0x0000FFFF : -1) > ();
        return  _mm_and_si128(t7,mask);
    }
    else {
        return  t7;
    }
#endif
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8us permute8us(Vec8us const & a) {
    return Vec8us (permute8s <i0,i1,i2,i3,i4,i5,i6,i7> (a));
}


template <int i0, int i1, int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15 > 
static inline Vec16c permute16c(Vec16c const & a) {

    __m128i temp;

    // Combine all even indexes into a single bitfield, with 4 bits for each
    const uint32_t me = (i0&15) | (i2&15)<<4 | (i4&15)<<8 | (i6&15)<<12 
        | (i8&15)<<16 | (i10&15)<<20 | (i12&15)<<24 | (i14&15)<<28; 

    // Combine all odd indexes into a single bitfield, with 4 bits for each
    const uint32_t mo = (i1&15) | (i3&15)<<4 | (i5&15)<<8 | (i7&15)<<12 
        | (i9&15)<<16 | (i11&15)<<20 | (i13&15)<<24 | (i15&15)<<28; 

    // Mask indicating sign of all even indexes, with 4 bits for each, 0 for negative, 0xF for non-negative
    const uint32_t se = (i0<0?0:0xF) | (i2<0?0:0xF)<<4 | (i4<0?0:0xF)<<8 | (i6<0?0:0xF)<<12
        | (i8<0?0:0xF)<<16 | (i10<0?0:0xF)<<20 | (i12<0?0:0xF)<<24 | (i14<0?0:0xF)<<28;

    // Mask indicating sign of all odd indexes, with 4 bits for each, 0 for negative, 0xF for non-negative
    const uint32_t so = (i1<0?0:0xF) | (i3<0?0:0xF)<<4 | (i5<0?0:0xF)<<8 | (i7<0?0:0xF)<<12
        | (i9<0?0:0xF)<<16 | (i11<0?0:0xF)<<20 | (i13<0?0:0xF)<<24 | (i15<0?0:0xF)<<28;

    // Mask indicating sign of all indexes, with 2 bits for each, 0 for negative (means set to zero or don't care), 0x3 for non-negative
    const uint32_t ss = (se & 0x33333333) | (so & 0xCCCCCCCC);

    // Mask indicating required zeroing of all indexes, with 2 bits for each, 0 for index = -1, 3 for index >= 0 or -256
    const uint32_t ssz = ((i0&0x80)?0:3) | ((i1 &0x80)?0:3)<< 2 | ((i2 &0x80)?0:3)<< 4 | ((i3 &0x80)?0:3)<< 6 | 
                    ((i4 &0x80)?0:3)<< 8 | ((i5 &0x80)?0:3)<<10 | ((i6 &0x80)?0:3)<<12 | ((i7 &0x80)?0:3)<<14 | 
                    ((i8 &0x80)?0:3)<<16 | ((i9 &0x80)?0:3)<<18 | ((i10&0x80)?0:3)<<20 | ((i11&0x80)?0:3)<<22 | 
                    ((i12&0x80)?0:3)<<24 | ((i13&0x80)?0:3)<<26 | ((i14&0x80)?0:3)<<28 | ((i15&0x80)?0:3)<<30 ;

    // These indexes are used only to avoid bogus compiler warnings in false branches
    const int I0  = i0  > 0 ? (i0  & 0xF) : 0;
    const int I15 = i15 > 0 ? (i15 & 0xF) : 0;

    // special case: all zero
    if (ss == 0) {
        return _mm_setzero_si128();  
    }

    // remember if extra zeroing is needed
    bool do_and_zero = (ssz != 0xFFFFFFFFu);

    // check for special shortcut cases
    int shortcut = 0;

    // check if any permutation
    if (((me ^ 0xECA86420) & se) == 0 && ((mo ^ 0xFDB97531) & so) == 0) {
        shortcut = 1;
    }
    // check if we can use punpcklbw
    else if (((me ^ 0x76543210) & se) == 0 && ((mo ^ 0x76543210) & so) == 0) {
        shortcut = 2;
    }
    // check if we can use punpckhbw
    else if (((me ^ 0xFEDCBA98) & se) == 0 && ((mo ^ 0xFEDCBA98) & so) == 0) {
        shortcut = 3;
    }

    #if defined (_MSC_VER) && ! defined(__INTEL_COMPILER)
    #pragma warning(disable: 4307)  // disable MS warning C4307: '+' : integral constant overflow
    #endif

    // check if we can use byte shift right
    else if (i0 > 0 && ((me ^ (uint32_t(I0)*0x11111111u + 0xECA86420u)) & se) == 0 && 
    ((mo ^ (uint32_t(I0)*0x11111111u + 0xFDB97531u)) & so) == 0) {
        shortcut = 4;
        do_and_zero = ((0xFFFFFFFFu >> 2*I0) & ~ ssz) != 0;
    }
    // check if we can use byte shift left
    else if (i15 >= 0 && i15 < 15 &&         
    ((mo ^ (uint32_t(I15*0x11111111u) - (0x02468ACEu & so))) & so) == 0 && 
    ((me ^ (uint32_t(I15*0x11111111u) - (0x13579BDFu & se))) & se) == 0) {
        shortcut = 5;
        do_and_zero = ((0xFFFFFFFFu << 2*(15-I15)) & ~ ssz) != 0;
    }

#if  INSTRSET >= 4  // SSSE3 (PSHUFB available only under SSSE3)

    // special case: rotate
    if (i0>0 && i0 < 16    && i1==((i0+1)&15) && i2 ==((i0+2 )&15) && i3 ==((i0+3 )&15) && i4 ==((i0+4 )&15) && i5 ==((i0+5 )&15) && i6 ==((i0+6 )&15) && i7 ==((i0+7 )&15) 
    && i8==((i0+8)&15) && i9==((i0+9)&15) && i10==((i0+10)&15) && i11==((i0+11)&15) && i12==((i0+12)&15) && i13==((i0+13)&15) && i14==((i0+14)&15) && i15==((i0+15)&15)) {
        temp = _mm_alignr_epi8(a, a, i0 & 15);
        shortcut = -1;
    }
    if (shortcut == 0 || do_and_zero) {
        // general case: use PSHUFB
        __m128i mask = constant4i< 
            (i0  & 0xFF) | (i1  & 0xFF) << 8 | (i2  & 0xFF) << 16 | (i3  & 0xFF) << 24 ,
            (i4  & 0xFF) | (i5  & 0xFF) << 8 | (i6  & 0xFF) << 16 | (i7  & 0xFF) << 24 ,
            (i8  & 0xFF) | (i9  & 0xFF) << 8 | (i10 & 0xFF) << 16 | (i11 & 0xFF) << 24 ,
            (i12 & 0xFF) | (i13 & 0xFF) << 8 | (i14 & 0xFF) << 16 | (i15 & 0xFF) << 24 > ();
        temp = _mm_shuffle_epi8(a,mask);
        shortcut = -1;
        do_and_zero = false;
    }

#endif

    // Check if we can use 16-bit permute. Even numbered indexes must be even and odd numbered
    // indexes must be equal to the preceding index + 1, except for negative indexes.
    if (shortcut == 0 && (me & 0x11111111 & se) == 0 && ((mo ^ 0x11111111) & 0x11111111 & so) == 0 && ((me ^ mo) & 0xEEEEEEEE & se & so) == 0) {
        temp = permute8s <
            i0  >= 0 ? i0 /2 : i1  >= 0 ? i1 /2 : (i0  | i1 ),
            i2  >= 0 ? i2 /2 : i3  >= 0 ? i3 /2 : (i2  | i3 ),
            i4  >= 0 ? i4 /2 : i5  >= 0 ? i5 /2 : (i4  | i5 ),
            i6  >= 0 ? i6 /2 : i7  >= 0 ? i7 /2 : (i6  | i7 ),
            i8  >= 0 ? i8 /2 : i9  >= 0 ? i9 /2 : (i8  | i9 ),
            i10 >= 0 ? i10/2 : i11 >= 0 ? i11/2 : (i10 | i11),
            i12 >= 0 ? i12/2 : i13 >= 0 ? i13/2 : (i12 | i13),
            i14 >= 0 ? i14/2 : i15 >= 0 ? i15/2 : (i14 | i15) > (Vec8s(a));
        shortcut = 100;
        do_and_zero = (se != so && ssz != 0xFFFFFFFFu);
    }
  
    // Check if we can use 16-bit permute with bytes swapped. Even numbered indexes must be odd and odd 
    // numbered indexes must be equal to the preceding index - 1, except for negative indexes.
    // (this case occurs when reversing byte order)
    if (shortcut == 0 && ((me ^ 0x11111111) & 0x11111111 & se) == 0 && (mo & 0x11111111 & so) == 0 && ((me ^ mo) & 0xEEEEEEEE & se & so) == 0) {
        Vec16c swapped = Vec16c(rotate_left(Vec8s(a), 8)); // swap odd and even bytes
        temp = permute8s <
            i0  >= 0 ? i0 /2 : i1  >= 0 ? i1 /2 : (i0  | i1 ),
            i2  >= 0 ? i2 /2 : i3  >= 0 ? i3 /2 : (i2  | i3 ),
            i4  >= 0 ? i4 /2 : i5  >= 0 ? i5 /2 : (i4  | i5 ),
            i6  >= 0 ? i6 /2 : i7  >= 0 ? i7 /2 : (i6  | i7 ),
            i8  >= 0 ? i8 /2 : i9  >= 0 ? i9 /2 : (i8  | i9 ),
            i10 >= 0 ? i10/2 : i11 >= 0 ? i11/2 : (i10 | i11),
            i12 >= 0 ? i12/2 : i13 >= 0 ? i13/2 : (i12 | i13),
            i14 >= 0 ? i14/2 : i15 >= 0 ? i15/2 : (i14 | i15) > (Vec8s(swapped));
        shortcut = 101;
        do_and_zero = (se != so && ssz != 0xFFFFFFFFu);
    }

    // all shortcuts end here
    if (shortcut) {
        switch (shortcut) {
        case 1:
            temp = a;  break;
        case 2:
            temp = _mm_unpacklo_epi8(a,a);  break;
        case 3:
            temp = _mm_unpackhi_epi8(a,a);  break;
        case 4:
            temp = _mm_srli_si128(a, I0);  break;
        case 5:
            temp = _mm_slli_si128(a, 15-I15);  break;
        default:
            break;  // result is already in temp
        }
        if (do_and_zero) {
            // additional zeroing needed
            __m128i maskz = constant4i < 
                (i0  < 0 ? 0 : 0xFF) | (i1  < 0 ? 0 : 0xFF00) | (i2  < 0 ? 0 : 0xFF0000) | (i3  < 0 ? 0 : 0xFF000000) ,
                (i4  < 0 ? 0 : 0xFF) | (i5  < 0 ? 0 : 0xFF00) | (i6  < 0 ? 0 : 0xFF0000) | (i7  < 0 ? 0 : 0xFF000000) ,
                (i8  < 0 ? 0 : 0xFF) | (i9  < 0 ? 0 : 0xFF00) | (i10 < 0 ? 0 : 0xFF0000) | (i11 < 0 ? 0 : 0xFF000000) ,
                (i12 < 0 ? 0 : 0xFF) | (i13 < 0 ? 0 : 0xFF00) | (i14 < 0 ? 0 : 0xFF0000) | (i15 < 0 ? 0 : 0xFF000000) > ();
            temp = _mm_and_si128(temp, maskz);
        }
        return temp;
    }

    // complicated cases: use 16-bit permute up to four times
    const bool e2e = (~me & 0x11111111 & se) != 0;  // even bytes of source to even bytes of destination
    const bool e2o = (~mo & 0x11111111 & so) != 0;  // even bytes of source to odd  bytes of destination
    const bool o2e = (me  & 0x11111111 & se) != 0;  // odd  bytes of source to even bytes of destination
    const bool o2o = (mo  & 0x11111111 & so) != 0;  // odd  bytes of source to odd  bytes of destination
    
    Vec16c swapped, te2e, te2o, to2e, to2o, combeven, combodd;

    if (e2o || o2e) swapped = rotate_left(Vec8s(a), 8); // swap odd and even bytes

    // even-to-even bytes
    if (e2e) te2e = permute8s <(i0&1)?-1:i0/2, (i2&1)?-1:i2/2, (i4&1)?-1:i4/2, (i6&1)?-1:i6/2,
        (i8&1)?-1:i8/2, (i10&1)?-1:i10/2, (i12&1)?-1:i12/2, (i14&1)?-1:i14/2> (Vec8s(a));                 
    // odd-to-even bytes
    if (o2e) to2e = permute8s <(i0&1)?i0/2:-1, (i2&1)?i2/2:-1, (i4&1)?i4/2:-1, (i6&1)?i6/2:-1,
        (i8&1)?i8/2:-1, (i10&1)?i10/2:-1, (i12&1)?i12/2:-1, (i14&1)?i14/2:-1> (Vec8s(swapped));
    // even-to-odd bytes
    if (e2o) te2o = permute8s <(i1&1)?-1:i1/2, (i3&1)?-1:i3/2, (i5&1)?-1:i5/2, (i7&1)?-1:i7/2, 
        (i9&1)?-1:i9/2, (i11&1)?-1:i11/2, (i13&1)?-1:i13/2, (i15&1)?-1:i15/2> (Vec8s(swapped));
    // odd-to-odd bytes
    if (o2o) to2o = permute8s <(i1&1)?i1/2:-1, (i3&1)?i3/2:-1, (i5&1)?i5/2:-1, (i7&1)?i7/2:-1,
        (i9&1)?i9/2:-1, (i11&1)?i11/2:-1, (i13&1)?i13/2:-1, (i15&1)?i15/2:-1> (Vec8s(a));

    if (e2e && o2e) combeven = te2e | to2e;
    else if (e2e)   combeven = te2e;
    else if (o2e)   combeven = to2e;
    else            combeven = _mm_setzero_si128();

    if (e2o && o2o) combodd  = te2o | to2o;
    else if (e2o)   combodd  = te2o;
    else if (o2o)   combodd  = to2o;
    else            combodd  = _mm_setzero_si128();

    __m128i maske = constant4i <     // mask used even bytes
        (i0  < 0 ? 0 : 0xFF) | (i2  < 0 ? 0 : 0xFF0000),
        (i4  < 0 ? 0 : 0xFF) | (i6  < 0 ? 0 : 0xFF0000),
        (i8  < 0 ? 0 : 0xFF) | (i10 < 0 ? 0 : 0xFF0000),
        (i12 < 0 ? 0 : 0xFF) | (i14 < 0 ? 0 : 0xFF0000) > ();
    __m128i masko = constant4i <     // mask used odd bytes
        (i1  < 0 ? 0 : 0xFF00) | (i3  < 0 ? 0 : 0xFF000000),
        (i5  < 0 ? 0 : 0xFF00) | (i7  < 0 ? 0 : 0xFF000000),
        (i9  < 0 ? 0 : 0xFF00) | (i11 < 0 ? 0 : 0xFF000000),
        (i13 < 0 ? 0 : 0xFF00) | (i15 < 0 ? 0 : 0xFF000000) > ();

    return  _mm_or_si128(            // combine even and odd bytes
        _mm_and_si128(combeven, maske),
        _mm_and_si128(combodd, masko));
}

template <int i0, int i1, int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15 > 
static inline Vec16uc permute16uc(Vec16uc const & a) {
    return Vec16uc (permute16c <i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15> (a));
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
* vector. For example, if each vector has 4 elements, then indexes 0 - 3
* will select an element from the first vector and indexes 4 - 7 will select 
* an element from the second vector. A negative index will generate zero.
*
* The blend functions for vectors of 8-bit integers are inefficient if 
* the SSSE3 instruction set or later is not enabled.
*
* Example:
* Vec4i a(100,101,102,103);         // a is (100, 101, 102, 103)
* Vec4i b(200,201,202,203);         // b is (200, 201, 202, 203)
* Vec4i c;
* c = blend4i<1,4,-1,7> (a,b);      // c is (101, 200,   0, 203)
*
* A lot of the code here is metaprogramming aiming to find the instructions
* that best fit the template parameters and instruction set. The metacode
* will be reduced out to leave only a few vector instructions in release
* mode with optimization on.
*****************************************************************************/

template <int i0, int i1, int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15 > 
static inline Vec16c blend16c(Vec16c const & a, Vec16c const & b) {

    // Combine bit 0-3 of all even indexes into a single bitfield, with 4 bits for each
    const int me = (i0&15) | (i2&15)<<4 | (i4&15)<<8 | (i6&15)<<12 
        | (i8&15)<<16 | (i10&15)<<20 | (i12&15)<<24 | (i14&15)<<28; 

    // Combine bit 0-3 of all odd indexes into a single bitfield, with 4 bits for each
    const int mo = (i1&15) | (i3&15)<<4 | (i5&15)<<8 | (i7&15)<<12 
        | (i9&15)<<16 | (i11&15)<<20 | (i13&15)<<24 | (i15&15)<<28; 

    // Mask indicating sign of all even indexes, with 4 bits for each, 0 for negative, 0xF for non-negative
    const int se = (i0<0?0:0xF) | (i2<0?0:0xF)<<4 | (i4<0?0:0xF)<<8 | (i6<0?0:0xF)<<12
        | (i8<0?0:0xF)<<16 | (i10<0?0:0xF)<<20 | (i12<0?0:0xF)<<24 | (i14<0?0:0xF)<<28;

    // Mask indicating sign of all odd indexes, with 4 bits for each, 0 for negative, 0xF for non-negative
    const int so = (i1<0?0:0xF) | (i3<0?0:0xF)<<4 | (i5<0?0:0xF)<<8 | (i7<0?0:0xF)<<12
        | (i9<0?0:0xF)<<16 | (i11<0?0:0xF)<<20 | (i13<0?0:0xF)<<24 | (i15<0?0:0xF)<<28;

    // Combine bit 4 of all even indexes into a single bitfield, with 4 bits for each
    const int ne = (i0&16)>>4 | (i2&16) | (i4&16)<<4 | (i6&16)<<8 
        | (i8&16)<<12 | (i10&16)<<16 | (i12&16)<<20 | (i14&16)<<24; 

    // Combine bit 4 of all odd indexes into a single bitfield, with 4 bits for each
    const int no = (i1&16)>>4 | (i3&16) | (i5&16)<<4 | (i7&16)<<8
        | (i9&16)<<12 | (i11&16)<<16 | (i13&16)<<20 | (i15&16)<<24; 

    // Check if zeroing needed
    const bool do_zero = ((i0|i1|i2|i3|i4|i5|i6|i7|i8|i9|i10|i11|i12|i13|i14|i15) & 0x80) != 0; // needs zeroing

    // no elements from b
    if (((ne & se) | (no & so)) == 0) {
        return permute16c <i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15> (a);
    }

    // no elements from a
    if ((((ne^0x11111111) & se) | ((no^0x11111111) & so)) == 0) {
        return permute16c <i0^16, i1^16, i2^16, i3^16, i4^16, i5^16, i6^16, i7^16, i8^16, i9^16, i10^16, i11^16, i12^16, i13^16, i14^16, i15^16> (b);
    }
    __m128i t;

    // check if we can use punpcklbw
    if (((me ^ 0x76543210) & se) == 0 && ((mo ^ 0x76543210) & so) == 0) {
        if ((ne & se) == 0 && ((no ^ 0x11111111) & so) == 0) {        
            t = _mm_unpacklo_epi8(a,b);
        }
        if ((no & so) == 0 && ((ne ^ 0x11111111) & se) == 0) {        
            t = _mm_unpacklo_epi8(b,a);
        }
        if (do_zero) {
            // additional zeroing needed
            __m128i maskz = constant4i < 
                (i0  < 0 ? 0 : 0xFF) | (i1  < 0 ? 0 : 0xFF00) | (i2  < 0 ? 0 : 0xFF0000) | (i3  < 0 ? 0 : 0xFF000000) ,
                (i4  < 0 ? 0 : 0xFF) | (i5  < 0 ? 0 : 0xFF00) | (i6  < 0 ? 0 : 0xFF0000) | (i7  < 0 ? 0 : 0xFF000000) ,
                (i8  < 0 ? 0 : 0xFF) | (i9  < 0 ? 0 : 0xFF00) | (i10 < 0 ? 0 : 0xFF0000) | (i11 < 0 ? 0 : 0xFF000000) ,
                (i12 < 0 ? 0 : 0xFF) | (i13 < 0 ? 0 : 0xFF00) | (i14 < 0 ? 0 : 0xFF0000) | (i15 < 0 ? 0 : 0xFF000000) > ();
            t = _mm_and_si128(t, maskz);
        }
        return t;
    }

    // check if we can use punpckhbw
    if (((me ^ 0xFEDCBA98) & se) == 0 && ((mo ^ 0xFEDCBA98) & so) == 0) {
        if ((ne & se) == 0 && ((no ^ 0x11111111) & so) == 0) {        
            t = _mm_unpackhi_epi8(a,b);
        }
        if ((no & so) == 0 && ((ne ^ 0x11111111) & se) == 0) {        
            t = _mm_unpackhi_epi8(b,a);
        }
        if (do_zero) {
            // additional zeroing needed
            __m128i maskz = constant4i < 
                (i0  < 0 ? 0 : 0xFF) | (i1  < 0 ? 0 : 0xFF00) | (i2  < 0 ? 0 : 0xFF0000) | (i3  < 0 ? 0 : 0xFF000000) ,
                (i4  < 0 ? 0 : 0xFF) | (i5  < 0 ? 0 : 0xFF00) | (i6  < 0 ? 0 : 0xFF0000) | (i7  < 0 ? 0 : 0xFF000000) ,
                (i8  < 0 ? 0 : 0xFF) | (i9  < 0 ? 0 : 0xFF00) | (i10 < 0 ? 0 : 0xFF0000) | (i11 < 0 ? 0 : 0xFF000000) ,
                (i12 < 0 ? 0 : 0xFF) | (i13 < 0 ? 0 : 0xFF00) | (i14 < 0 ? 0 : 0xFF0000) | (i15 < 0 ? 0 : 0xFF000000) > ();
            t = _mm_and_si128(t, maskz);
        }
        return t;
    }
    
#if  INSTRSET >= 4  // SSSE3
    // special case: shift left
    if (i0 > 0 && i0 < 16 && i1==i0+1 && i2==i0+2 && i3==i0+3 && i4==i0+4 && i5==i0+5 && i6==i0+6 && i7==i0+7 && 
        i8==i0+8 && i9==i0+9 && i10==i0+10 && i11==i0+11 && i12==i0+12 && i13==i0+13 && i14==i0+14 && i15==i0+15) {
        return _mm_alignr_epi8(b, a, (i0 & 15));
    }

    // special case: shift right
    if (i0 > 15 && i0 < 32 && i1==((i0+1)&31) && i2 ==((i0+2 )&31) && i3 ==((i0+3 )&31) && i4 ==((i0+4 )&31) && i5 ==((i0+5 )&31) && i6 ==((i0+6 )&31) && i7 ==((i0+7 )&31) && 
        i8==((i0+8 )&31)   && i9==((i0+9)&31) && i10==((i0+10)&31) && i11==((i0+11)&31) && i12==((i0+12)&31) && i13==((i0+13)&31) && i14==((i0+14)&31) && i15==((i0+15)&31)) {
        return _mm_alignr_epi8(a, b, (i0 & 15));
    }
#endif

#if INSTRSET >= 5   // SSE4.1 supported
    // special case: blend without permute
    if (((me ^ 0xECA86420) & se) == 0 && ((mo ^ 0xFDB97531) & so) == 0) {
        __m128i maskbl = constant4i<
            ((i0 & 16) ? 0xFF : 0) | ((i1 & 16) ? 0xFF00 : 0) | ((i2 & 16) ? 0xFF0000 : 0) | ((i3 & 16) ? 0xFF000000 : 0) ,
            ((i4 & 16) ? 0xFF : 0) | ((i5 & 16) ? 0xFF00 : 0) | ((i6 & 16) ? 0xFF0000 : 0) | ((i7 & 16) ? 0xFF000000 : 0) ,
            ((i8 & 16) ? 0xFF : 0) | ((i9 & 16) ? 0xFF00 : 0) | ((i10& 16) ? 0xFF0000 : 0) | ((i11& 16) ? 0xFF000000 : 0) ,
            ((i12& 16) ? 0xFF : 0) | ((i13& 16) ? 0xFF00 : 0) | ((i14& 16) ? 0xFF0000 : 0) | ((i15& 16) ? 0xFF000000 : 0) > ();
        t = _mm_blendv_epi8(a, b, maskbl);
        if (do_zero) {
            // additional zeroing needed
            __m128i maskz = constant4i < 
                (i0  < 0 ? 0 : 0xFF) | (i1  < 0 ? 0 : 0xFF00) | (i2  < 0 ? 0 : 0xFF0000) | (i3  < 0 ? 0 : 0xFF000000) ,
                (i4  < 0 ? 0 : 0xFF) | (i5  < 0 ? 0 : 0xFF00) | (i6  < 0 ? 0 : 0xFF0000) | (i7  < 0 ? 0 : 0xFF000000) ,
                (i8  < 0 ? 0 : 0xFF) | (i9  < 0 ? 0 : 0xFF00) | (i10 < 0 ? 0 : 0xFF0000) | (i11 < 0 ? 0 : 0xFF000000) ,
                (i12 < 0 ? 0 : 0xFF) | (i13 < 0 ? 0 : 0xFF00) | (i14 < 0 ? 0 : 0xFF0000) | (i15 < 0 ? 0 : 0xFF000000) > ();
            t = _mm_and_si128(t, maskz);
        }
        return t;
    }
#endif // SSE4.1

#if defined ( __XOP__ )    // Use AMD XOP instruction VPPERM
    __m128i mask = constant4i<
        (i0 <0 ? 0x80 : (i0 &31)) | (i1 <0 ? 0x80 : (i1 &31)) << 8 | (i2 <0 ? 0x80 : (i2 &31)) << 16 | (i3 <0 ? 0x80 : (i3 &31)) << 24,
        (i4 <0 ? 0x80 : (i4 &31)) | (i5 <0 ? 0x80 : (i5 &31)) << 8 | (i6 <0 ? 0x80 : (i6 &31)) << 16 | (i7 <0 ? 0x80 : (i7 &31)) << 24,
        (i8 <0 ? 0x80 : (i8 &31)) | (i9 <0 ? 0x80 : (i9 &31)) << 8 | (i10<0 ? 0x80 : (i10&31)) << 16 | (i11<0 ? 0x80 : (i11&31)) << 24,
        (i12<0 ? 0x80 : (i12&31)) | (i13<0 ? 0x80 : (i13&31)) << 8 | (i14<0 ? 0x80 : (i14&31)) << 16 | (i15<0 ? 0x80 : (i15&31)) << 24 > ();
    return _mm_perm_epi8(a, b, mask);

#elif  INSTRSET >= 4  // SSSE3
   
    // general case. Use PSHUFB
    __m128i maska = constant4i<
        ((i0 & 0x90) ? 0xFF : (i0 &15)) | ((i1 & 0x90) ? 0xFF : (i1 &15)) << 8 | ((i2 & 0x90) ? 0xFF : (i2 &15)) << 16 | ((i3 & 0x90) ? 0xFF : (i3 &15)) << 24,
        ((i4 & 0x90) ? 0xFF : (i4 &15)) | ((i5 & 0x90) ? 0xFF : (i5 &15)) << 8 | ((i6 & 0x90) ? 0xFF : (i6 &15)) << 16 | ((i7 & 0x90) ? 0xFF : (i7 &15)) << 24,
        ((i8 & 0x90) ? 0xFF : (i8 &15)) | ((i9 & 0x90) ? 0xFF : (i9 &15)) << 8 | ((i10& 0x90) ? 0xFF : (i10&15)) << 16 | ((i11& 0x90) ? 0xFF : (i11&15)) << 24,
        ((i12& 0x90) ? 0xFF : (i12&15)) | ((i13& 0x90) ? 0xFF : (i13&15)) << 8 | ((i14& 0x90) ? 0xFF : (i14&15)) << 16 | ((i15& 0x90) ? 0xFF : (i15&15)) << 24 > ();
    __m128i maskb = constant4i<
        (((i0^0x10) & 0x90) ? 0xFF : (i0 &15)) | (((i1^0x10) & 0x90) ? 0xFF : (i1 &15)) << 8 | (((i2^0x10) & 0x90) ? 0xFF : (i2 &15)) << 16 | (((i3^0x10) & 0x90) ? 0xFF : (i3 &15)) << 24,
        (((i4^0x10) & 0x90) ? 0xFF : (i4 &15)) | (((i5^0x10) & 0x90) ? 0xFF : (i5 &15)) << 8 | (((i6^0x10) & 0x90) ? 0xFF : (i6 &15)) << 16 | (((i7^0x10) & 0x90) ? 0xFF : (i7 &15)) << 24,
        (((i8^0x10) & 0x90) ? 0xFF : (i8 &15)) | (((i9^0x10) & 0x90) ? 0xFF : (i9 &15)) << 8 | (((i10^0x10)& 0x90) ? 0xFF : (i10&15)) << 16 | (((i11^0x10)& 0x90) ? 0xFF : (i11&15)) << 24,
        (((i12^0x10)& 0x90) ? 0xFF : (i12&15)) | (((i13^0x10)& 0x90) ? 0xFF : (i13&15)) << 8 | (((i14^0x10)& 0x90) ? 0xFF : (i14&15)) << 16 | (((i15^0x10)& 0x90) ? 0xFF : (i15&15)) << 24 > ();
    __m128i a1 = _mm_shuffle_epi8(a,maska);
    __m128i b1 = _mm_shuffle_epi8(b,maskb);
    return       _mm_or_si128(a1,b1);

#else                 // SSE2
    // combine two permutes
    __m128i a1 = permute16c <
        (uint32_t)i0  < 16 ? i0  : -1,
        (uint32_t)i1  < 16 ? i1  : -1,
        (uint32_t)i2  < 16 ? i2  : -1,
        (uint32_t)i3  < 16 ? i3  : -1,
        (uint32_t)i4  < 16 ? i4  : -1,
        (uint32_t)i5  < 16 ? i5  : -1,
        (uint32_t)i6  < 16 ? i6  : -1,
        (uint32_t)i7  < 16 ? i7  : -1,
        (uint32_t)i8  < 16 ? i8  : -1,
        (uint32_t)i9  < 16 ? i9  : -1,
        (uint32_t)i10 < 16 ? i10 : -1,
        (uint32_t)i11 < 16 ? i11 : -1,
        (uint32_t)i12 < 16 ? i12 : -1,
        (uint32_t)i13 < 16 ? i13 : -1,
        (uint32_t)i14 < 16 ? i14 : -1,
        (uint32_t)i15 < 16 ? i15 : -1 > (a);
    __m128i b1 = permute16c <
        (uint32_t)(i0 ^16) < 16 ? (i0 ^16) : -1,
        (uint32_t)(i1 ^16) < 16 ? (i1 ^16) : -1,
        (uint32_t)(i2 ^16) < 16 ? (i2 ^16) : -1,
        (uint32_t)(i3 ^16) < 16 ? (i3 ^16) : -1,
        (uint32_t)(i4 ^16) < 16 ? (i4 ^16) : -1,
        (uint32_t)(i5 ^16) < 16 ? (i5 ^16) : -1,
        (uint32_t)(i6 ^16) < 16 ? (i6 ^16) : -1,
        (uint32_t)(i7 ^16) < 16 ? (i7 ^16) : -1,        
        (uint32_t)(i8 ^16) < 16 ? (i8 ^16) : -1,
        (uint32_t)(i9 ^16) < 16 ? (i9 ^16) : -1,
        (uint32_t)(i10^16) < 16 ? (i10^16) : -1,
        (uint32_t)(i11^16) < 16 ? (i11^16) : -1,
        (uint32_t)(i12^16) < 16 ? (i12^16) : -1,
        (uint32_t)(i13^16) < 16 ? (i13^16) : -1,
        (uint32_t)(i14^16) < 16 ? (i14^16) : -1,
        (uint32_t)(i15^16) < 16 ? (i15^16) : -1 > (b);
    return   _mm_or_si128(a1,b1);

#endif
}

template <int i0, int i1, int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15 > 
static inline Vec16uc blend16uc(Vec16uc const & a, Vec16uc const & b) {
    return Vec16uc( blend16c<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15> (a,b));
}


template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8s blend8s(Vec8s const & a, Vec8s const & b) {

    // Combine all the indexes into a single bitfield, with 4 bits for each
    const int m1 = (i0&0xF) | (i1&0xF)<<4 | (i2&0xF)<<8 | (i3&0xF)<<12 
        | (i4&0xF)<<16 | (i5&0xF)<<20 | (i6&0xF)<<24 | (i7&0xF)<<28; 

    // Mask to zero out negative indexes
    const int mz = (i0<0?0:0xF) | (i1<0?0:0xF)<<4 | (i2<0?0:0xF)<<8 | (i3<0?0:0xF)<<12
        | (i4<0?0:0xF)<<16 | (i5<0?0:0xF)<<20 | (i6<0?0:0xF)<<24 | (i7<0?0:0xF)<<28;

    // Some elements must be set to zero
    const bool do_zero = (mz != -1) && ((i0 | i1 | i2 | i3 | i4 | i5 | i6 | i7) & 0x80) != 0;

    // temp contains temporary result, some zeroing needs to be done
    bool zeroing_pending = false;

    // partially finished result
    __m128i temp;

    if ((m1 & 0x88888888 & mz) == 0) {
        // no elements from b
        return permute8s <i0, i1, i2, i3, i4, i5, i6, i7> (a);
    }

    if (((m1^0x88888888) & 0x88888888 & mz) == 0) {
        // no elements from a
        return permute8s <i0&~8, i1&~8, i2&~8, i3&~8, i4&~8, i5&~8, i6&~8, i7&~8> (b);
    }

    // special case: PUNPCKLWD 
    if (((m1 ^ 0xB3A29180) & mz) == 0) {
        temp = _mm_unpacklo_epi16(a, b);
        if (do_zero) zeroing_pending = true; else return temp;
    }
    if (((m1 ^ 0x3B2A1908) & mz) == 0) {
        temp = _mm_unpacklo_epi16(b, a);
        if (do_zero) zeroing_pending = true; else return temp;
    }
    // special case: PUNPCKHWD 
    if (((m1 ^ 0xF7E6D5C4) & mz) == 0) {
        temp = _mm_unpackhi_epi16(a, b);
        if (do_zero) zeroing_pending = true; else return temp;
    }
    if (((m1 ^ 0x7F6E5D4C) & mz) == 0) {
        temp = _mm_unpackhi_epi16(b, a);
        if (do_zero) zeroing_pending = true; else return temp;
    }

#if  INSTRSET >= 4  // SSSE3
    // special case: shift left
    if (i0 > 0 && i0 < 8 && ((m1 ^ ((i0 & 7) * 0x11111111u + 0x76543210u)) & mz) == 0) {
        temp = _mm_alignr_epi8(b, a, (i0 & 7) * 2);
        if (do_zero) zeroing_pending = true; else return temp;
    }

    // special case: shift right
    if (i0 > 8 && i0 < 16 && ((m1 ^ 0x88888888 ^ ((i0 & 7) * 0x11111111u + 0x76543210u)) & mz) == 0) {
        temp = _mm_alignr_epi8(a, b, (i0 & 7) * 2);
        if (do_zero) zeroing_pending = true; else return temp;
    }
#endif // SSSE3

#if INSTRSET >= 5   // SSE4.1 supported
    // special case: blending without permuting
    if ((((m1 & ~0x88888888) ^ 0x76543210) & mz) == 0) {
        temp = _mm_blend_epi16(a, b, (i0>>3&1) | (i1>>3&1)<<1 | (i2>>3&1)<<2 | (i3>>3&1)<<3 
            | (i4>>3&1)<<4 | (i5>>3&1)<<5 | (i6>>3&1)<<6 | (i7>>3&1)<<7);
        if (do_zero) zeroing_pending = true; else return temp;
    }
#endif // SSE4.1

    if (zeroing_pending) {
        // additional zeroing of temp needed
        __m128i maskz = constant4i < 
            (i0 < 0 ? 0 : 0xFFFF) | (i1 < 0 ? 0 : 0xFFFF0000) ,
            (i2 < 0 ? 0 : 0xFFFF) | (i3 < 0 ? 0 : 0xFFFF0000) ,
            (i4 < 0 ? 0 : 0xFFFF) | (i5 < 0 ? 0 : 0xFFFF0000) ,
            (i6 < 0 ? 0 : 0xFFFF) | (i7 < 0 ? 0 : 0xFFFF0000) > ();
        return _mm_and_si128(temp, maskz);
    }        

    // general case
#ifdef __XOP__     // Use AMD XOP instruction PPERM
    __m128i mask = constant4i <
        (i0 < 0 ? 0x8080 : (i0*2 & 31) | ((i0*2 & 31)+1)<<8) | (i1 < 0 ? 0x80800000 : ((i1*2 & 31)<<16) | ((i1*2 & 31)+1)<<24),
        (i2 < 0 ? 0x8080 : (i2*2 & 31) | ((i2*2 & 31)+1)<<8) | (i3 < 0 ? 0x80800000 : ((i3*2 & 31)<<16) | ((i3*2 & 31)+1)<<24),
        (i4 < 0 ? 0x8080 : (i4*2 & 31) | ((i4*2 & 31)+1)<<8) | (i5 < 0 ? 0x80800000 : ((i5*2 & 31)<<16) | ((i5*2 & 31)+1)<<24),
        (i6 < 0 ? 0x8080 : (i6*2 & 31) | ((i6*2 & 31)+1)<<8) | (i7 < 0 ? 0x80800000 : ((i7*2 & 31)<<16) | ((i7*2 & 31)+1)<<24) > ();
    return _mm_perm_epi8(a, b, mask);
#else  
    // combine two permutes
    __m128i a1 = permute8s <
        (uint32_t)i0 < 8 ? i0 : -1,
        (uint32_t)i1 < 8 ? i1 : -1,
        (uint32_t)i2 < 8 ? i2 : -1,
        (uint32_t)i3 < 8 ? i3 : -1,
        (uint32_t)i4 < 8 ? i4 : -1,
        (uint32_t)i5 < 8 ? i5 : -1,
        (uint32_t)i6 < 8 ? i6 : -1,
        (uint32_t)i7 < 8 ? i7 : -1 > (a);
    __m128i b1 = permute8s <
        (uint32_t)(i0^8) < 8 ? (i0^8) : -1,
        (uint32_t)(i1^8) < 8 ? (i1^8) : -1,
        (uint32_t)(i2^8) < 8 ? (i2^8) : -1,
        (uint32_t)(i3^8) < 8 ? (i3^8) : -1,
        (uint32_t)(i4^8) < 8 ? (i4^8) : -1,
        (uint32_t)(i5^8) < 8 ? (i5^8) : -1,
        (uint32_t)(i6^8) < 8 ? (i6^8) : -1,
        (uint32_t)(i7^8) < 8 ? (i7^8) : -1 > (b);
    return   _mm_or_si128(a1,b1);

#endif
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8us blend8us(Vec8us const & a, Vec8us const & b) {
    return Vec8us(blend8s<i0,i1,i2,i3,i4,i5,i6,i7> (a,b));
}

template <int i0, int i1, int i2, int i3>
static inline Vec4i blend4i(Vec4i const & a, Vec4i const & b) {

    // Combine all the indexes into a single bitfield, with 8 bits for each
    const int m1 = (i0 & 7) | (i1 & 7) << 8 | (i2 & 7) << 16 | (i3 & 7) << 24; 

    // Mask to zero out negative indexes
    const int mz = (i0 < 0 ? 0 : 0xFF) | (i1 < 0 ? 0 : 0xFF) << 8 | (i2 < 0 ? 0 : 0xFF) << 16 | (i3 < 0 ? 0 : 0xFF) << 24;

    // Some elements must be set to zero
    const bool do_zero = (mz != -1) && ((i0 | i1 | i2 | i3) & 0x80) != 0;

    // temp contains temporary result, some zeroing needs to be done
    bool zeroing_pending = false;

    // partially finished result
    __m128i temp;
#if defined (_MSC_VER) || defined (__clang__)
    temp = a;  // avoid spurious warning message for temp unused
#endif

    // special case: no elements from b
    if ((m1 & 0x04040404 & mz) == 0) {
        return permute4i<i0,i1,i2,i3>(a);
    }

    // special case: no elements from a
    if (((m1^0x04040404) & 0x04040404 & mz) == 0) {
        return permute4i<i0&~4, i1&~4, i2&~4, i3&~4>(b);
    }

    // special case: PUNPCKLDQ
    if (((m1 ^ 0x05010400) & mz) == 0) {
        temp = _mm_unpacklo_epi32(a, b);
        if (do_zero) zeroing_pending = true; else return temp;
    }
    if (((m1 ^ 0x01050004) & mz) == 0) {
        temp = _mm_unpacklo_epi32(b, a);
        if (do_zero) zeroing_pending = true; else return temp;
    }

    // special case: PUNPCKHDQ 
    if (((m1 ^ 0x07030602) & mz) == 0) {
        temp = _mm_unpackhi_epi32(a, b);
        if (do_zero) zeroing_pending = true; else return temp;
    }
    if (((m1 ^ 0x03070206) & mz) == 0) {
        temp = _mm_unpackhi_epi32(b, a);
        if (do_zero) zeroing_pending = true; else return temp;
    }

#if  INSTRSET >= 4  // SSSE3
    // special case: shift left
    if (i0 > 0 && i0 < 4 && ((m1 ^ ((i0 & 3) * 0x01010101u + 0x03020100u)) & mz) == 0) {
        temp = _mm_alignr_epi8(b, a, (i0 & 3) * 4);
        if (do_zero) zeroing_pending = true; else return temp;
    }

    // special case: shift right
    if (i0 > 4 && i0 < 8 && ((m1 ^ 0x04040404 ^ ((i0 & 3) * 0x01010101u + 0x03020100u)) & mz) == 0) {
        temp = _mm_alignr_epi8(a, b, (i0 & 3) * 4);
        if (do_zero) zeroing_pending = true; else return temp;
    }
#endif // SSSE3

#if INSTRSET >= 5   // SSE4.1 supported
    if ((((m1 & ~0x04040404) ^ 0x03020100) & mz) == 0) {
        // blending without permuting
        temp = _mm_blend_epi16(a, b, ((i0>>2)&1)*3 | ((((i1>>2)&1)*3)<<2) | ((((i2>>2)&1)*3)<<4) | ((((i3>>2)&1)*3)<<6));
        if (do_zero) zeroing_pending = true; else return temp;
    }
#endif // SSE4.1

    if (zeroing_pending) {
        // additional zeroing of temp needed
        __m128i maskz = constant4i < (i0 < 0 ? 0 : -1), (i1 < 0 ? 0 : -1), (i2 < 0 ? 0 : -1), (i3 < 0 ? 0 : -1) > ();
        return _mm_and_si128(temp, maskz);
    }        

    // general case
#ifdef __XOP__     // Use AMD XOP instruction PPERM
    __m128i mask = constant4i <
        i0 < 0 ? 0x80808080 : (i0*4 & 31) + (((i0*4 & 31) + 1) << 8) + (((i0*4 & 31) + 2) << 16) + (((i0*4 & 31) + 3) << 24),
        i1 < 0 ? 0x80808080 : (i1*4 & 31) + (((i1*4 & 31) + 1) << 8) + (((i1*4 & 31) + 2) << 16) + (((i1*4 & 31) + 3) << 24),
        i2 < 0 ? 0x80808080 : (i2*4 & 31) + (((i2*4 & 31) + 1) << 8) + (((i2*4 & 31) + 2) << 16) + (((i2*4 & 31) + 3) << 24),
        i3 < 0 ? 0x80808080 : (i3*4 & 31) + (((i3*4 & 31) + 1) << 8) + (((i3*4 & 31) + 2) << 16) + (((i3*4 & 31) + 3) << 24) > ();
    return _mm_perm_epi8(a, b, mask);

#else  // combine two permutes
    __m128i a1 = permute4i <
        (uint32_t)i0 < 4 ? i0 : -1,
        (uint32_t)i1 < 4 ? i1 : -1,
        (uint32_t)i2 < 4 ? i2 : -1,
        (uint32_t)i3 < 4 ? i3 : -1  > (a);
    __m128i b1 = permute4i <
        (uint32_t)(i0^4) < 4 ? (i0^4) : -1,
        (uint32_t)(i1^4) < 4 ? (i1^4) : -1,
        (uint32_t)(i2^4) < 4 ? (i2^4) : -1,
        (uint32_t)(i3^4) < 4 ? (i3^4) : -1  > (b);
    return  _mm_or_si128(a1,b1);
#endif
}

template <int i0, int i1, int i2, int i3>
static inline Vec4ui blend4ui(Vec4ui const & a, Vec4ui const & b) {
    return Vec4ui (blend4i<i0,i1,i2,i3> (a,b));
}

template <int i0, int i1>
static inline Vec2q blend2q(Vec2q const & a, Vec2q const & b) {

    // Combine all the indexes into a single bitfield, with 8 bits for each
    const int m1 = (i0&3) | (i1&3)<<8; 

    // Mask to zero out negative indexes
    const int mz = (i0 < 0 ? 0 : 0xFF) | (i1 < 0 ? 0 : 0xFF) << 8;

    // no elements from b
    if ((m1 & 0x0202 & mz) == 0) {
        return permute2q <i0, i1> (a);
    }
    // no elements from a
    if (((m1^0x0202) & 0x0202 & mz) == 0) {
        return permute2q <i0 & ~2, i1 & ~2> (b);
    }
    // (all cases where one index is -1 or -256 would go to the above cases)

    // special case: PUNPCKLQDQ 
    if (i0 == 0 && i1 == 2) {
        return _mm_unpacklo_epi64(a, b);
    }
    if (i0 == 2 && i1 == 0) {
        return _mm_unpacklo_epi64(b, a);
    }
    // special case: PUNPCKHQDQ 
    if (i0 == 1 && i1 == 3) {
        return _mm_unpackhi_epi64(a, b);
    }
    if (i0 == 3 && i1 == 1) {
        return _mm_unpackhi_epi64(b, a);
    }

#if  INSTRSET >= 4  // SSSE3
    // special case: shift left
    if (i0 == 1 && i1 == 2) {
        return _mm_alignr_epi8(b, a, 8);
    }
    // special case: shift right
    if (i0 == 3 && i1 == 0) {
        return _mm_alignr_epi8(a, b, 8);
    }
#endif // SSSE3

#if INSTRSET >= 5   // SSE4.1 supported
    if (((m1 & ~0x0202) ^ 0x0100) == 0 && mz == 0xFFFF) {
        // blending without permuting
        return _mm_blend_epi16(a, b, (i0>>1 & 1) * 0xF | ((i1>>1 & 1) * 0xF) << 4 );
    }
#endif // SSE4.1

    // general case. combine two permutes 
    // (all cases are caught by the above special cases if SSE4.1 or higher is supported)
    __m128i a1, b1;
    a1 = permute2q <(uint32_t)i0 < 2 ? i0 : -1, (uint32_t)i1 < 2 ? i1 : -1 > (a);
    b1 = permute2q <(uint32_t)(i0^2) < 2 ? (i0^2) : -1, (uint32_t)(i1^2) < 2 ? (i1^2) : -1 > (b);
    return  _mm_or_si128(a1,b1);
}

template <int i0, int i1>
static inline Vec2uq blend2uq(Vec2uq const & a, Vec2uq const & b) {
    return Vec2uq (blend2q <i0, i1> ((__m128i)a, (__m128i)b));
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
* Vec4i a(2,0,0,3);           // index a is (  2,   0,   0,   3)
* Vec4i b(100,101,102,103);   // table b is (100, 101, 102, 103)
* Vec4i c;
* c = lookup4 (a,b);          // c is (102, 100, 100, 103)
*
*****************************************************************************/

static inline Vec16c lookup16(Vec16c const & index, Vec16c const & table) {
#if INSTRSET >= 5  // SSSE3
    return _mm_shuffle_epi8(table, index);
#else
    uint8_t ii[16];
    int8_t  tt[16], rr[16];
    table.store(tt);  index.store(ii);
    for (int j = 0; j < 16; j++) rr[j] = tt[ii[j] & 0x0F];
    return Vec16c().load(rr);
#endif
}

static inline Vec16c lookup32(Vec16c const & index, Vec16c const & table0, Vec16c const & table1) {
#ifdef __XOP__  // AMD XOP instruction set. Use VPPERM
    return _mm_perm_epi8(table0, table1, index);
#elif INSTRSET >= 5  // SSSE3
    Vec16c r0 = _mm_shuffle_epi8(table0, index + 0x70);           // make negative index for values >= 16
    Vec16c r1 = _mm_shuffle_epi8(table1, (index ^ 0x10) + 0x70);  // make negative index for values <  16
    return r0 | r1;
#else
    uint8_t ii[16];
    int8_t  tt[16], rr[16];
    table0.store(tt);  table1.store(tt+16);  index.store(ii);
    for (int j = 0; j < 16; j++) rr[j] = tt[ii[j] & 0x1F];
    return Vec16c().load(rr);
#endif
}

template <int n>
static inline Vec16c lookup(Vec16c const & index, void const * table) {
    if (n <=  0) return 0;
    if (n <= 16) return lookup16(index, Vec16c().load(table));
    if (n <= 32) return lookup32(index, Vec16c().load(table), Vec16c().load((int8_t*)table + 16));
    // n > 32. Limit index
    Vec16uc index1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec16uc(index) & uint8_t(n-1);
    }
    else {
        // n is not a power of 2, limit to n-1
        index1 = min(Vec16uc(index), uint8_t(n-1));
    }
    uint8_t ii[16];  index1.store(ii);
    int8_t  rr[16];
    for (int j = 0; j < 16; j++) {
        rr[j] = ((int8_t*)table)[ii[j]];
    }
    return Vec16c().load(rr);
}

static inline Vec8s lookup8(Vec8s const & index, Vec8s const & table) {
#if INSTRSET >= 5  // SSSE3
    return _mm_shuffle_epi8(table, index * 0x202 + 0x100);
#else
    int16_t ii[8], tt[8], rr[8];
    table.store(tt);  index.store(ii);
    for (int j = 0; j < 8; j++) rr[j] = tt[ii[j] & 0x07];
    return Vec8s().load(rr);
#endif
}

static inline Vec8s lookup16(Vec8s const & index, Vec8s const & table0, Vec8s const & table1) {
#ifdef __XOP__  // AMD XOP instruction set. Use VPPERM
    return _mm_perm_epi8(table0, table1, index * 0x202 + 0x100);
#elif INSTRSET >= 5  // SSSE3
    Vec8s r0 = _mm_shuffle_epi8(table0, Vec16c(index * 0x202) + Vec16c(Vec8s(0x7170)));
    Vec8s r1 = _mm_shuffle_epi8(table1, Vec16c(index * 0x202 ^ 0x1010) + Vec16c(Vec8s(0x7170)));
    return r0 | r1;
#else
    int16_t ii[16], tt[32], rr[16];
    table0.store(tt);  table1.store(tt+8);  index.store(ii);
    for (int j = 0; j < 16; j++) rr[j] = tt[ii[j] & 0x1F];
    return Vec8s().load(rr);
#endif
}

template <int n>
static inline Vec8s lookup(Vec8s const & index, void const * table) {
    if (n <=  0) return 0;
    if (n <=  8) return lookup8 (index, Vec8s().load(table));
    if (n <= 16) return lookup16(index, Vec8s().load(table), Vec8s().load((int16_t*)table + 8));
    // n > 16. Limit index
    Vec8us index1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec8us(index) & (n-1);
    }
    else {
        // n is not a power of 2, limit to n-1
        index1 = min(Vec8us(index), n-1);
    }
#if INSTRSET >= 8 // AVX2. Use VPERMD
    Vec8s t1 = _mm_i32gather_epi32((const int *)table, __m128i((Vec4i(index1)) & (Vec4i(0x0000FFFF))), 2);  // even positions
    Vec8s t2 = _mm_i32gather_epi32((const int *)table, _mm_srli_epi32(index1, 16) , 2);  // odd  positions
    return blend8s<0,8,2,10,4,12,6,14>(t1, t2);
#else
    uint16_t ii[8];  index1.store(ii);
    return Vec8s(((int16_t*)table)[ii[0]], ((int16_t*)table)[ii[1]], ((int16_t*)table)[ii[2]], ((int16_t*)table)[ii[3]],
                 ((int16_t*)table)[ii[4]], ((int16_t*)table)[ii[5]], ((int16_t*)table)[ii[6]], ((int16_t*)table)[ii[7]]);
#endif
}


static inline Vec4i lookup4(Vec4i const & index, Vec4i const & table) {
#if INSTRSET >= 5  // SSSE3
    return _mm_shuffle_epi8(table, index * 0x04040404 + 0x03020100);
#else
    return Vec4i(table[index[0]],table[index[1]],table[index[2]],table[index[3]]);
#endif
}

static inline Vec4i lookup8(Vec4i const & index, Vec4i const & table0, Vec4i const & table1) {
    // return Vec4i(lookup16(Vec8s(index * 0x20002 + 0x10000), Vec8s(table0), Vec8s(table1)));
#ifdef __XOP__  // AMD XOP instruction set. Use VPPERM
    return _mm_perm_epi8(table0, table1, index * 0x04040404 + 0x03020100);
#elif INSTRSET >= 8 // AVX2. Use VPERMD
    __m256i table01 = _mm256_inserti128_si256(_mm256_castsi128_si256(table0), table1, 1); // join tables into 256 bit vector

#if defined (_MSC_VER) && _MSC_VER < 1700 && ! defined(__INTEL_COMPILER)
    // bug in MS VS 11 beta: operands in wrong order
    return _mm256_castsi256_si128(_mm256_permutevar8x32_epi32(_mm256_castsi128_si256(index), table01));
#elif defined (GCC_VERSION) && GCC_VERSION <= 40700 && !defined(__INTEL_COMPILER) && !defined(__clang__)
    // Gcc 4.7.0 also has operands in wrong order
    return _mm256_castsi256_si128(_mm256_permutevar8x32_epi32(_mm256_castsi128_si256(index), table01));
#else
    return _mm256_castsi256_si128(_mm256_permutevar8x32_epi32(table01, _mm256_castsi128_si256(index)));
#endif // bug

#elif INSTRSET >= 4  // SSSE3
    Vec4i r0 = _mm_shuffle_epi8(table0, Vec16c(index * 0x04040404) + Vec16c(Vec4i(0x73727170)));
    Vec4i r1 = _mm_shuffle_epi8(table1, Vec16c(index * 0x04040404 ^ 0x10101010) + Vec16c(Vec4i(0x73727170)));
    return r0 | r1;
#else    // SSE2
    int32_t ii[4], tt[8], rr[4];
    table0.store(tt);  table1.store(tt+4);  index.store(ii);
    for (int j = 0; j < 4; j++) rr[j] = tt[ii[j] & 0x07];
    return Vec4i().load(rr);
#endif
}

static inline Vec4i lookup16(Vec4i const & index, Vec4i const & table0, Vec4i const & table1, Vec4i const & table2, Vec4i const & table3) {
#if INSTRSET >= 8 // AVX2. Use VPERMD
    __m256i table01 = _mm256_inserti128_si256(_mm256_castsi128_si256(table0), table1, 1); // join tables into 256 bit vector
    __m256i table23 = _mm256_inserti128_si256(_mm256_castsi128_si256(table2), table3, 1); // join tables into 256 bit vector
#if defined (_MSC_VER) && _MSC_VER < 1700 && ! defined(__INTEL_COMPILER)
    // bug in MS VS 11 beta: operands in wrong order
    __m128i r0 = _mm256_castsi256_si128(_mm256_permutevar8x32_epi32(_mm256_castsi128_si256(index    ), table01));
    __m128i r1 = _mm256_castsi256_si128(_mm256_permutevar8x32_epi32(_mm256_castsi128_si256(index ^ 8), table23));
#elif defined (GCC_VERSION) && GCC_VERSION <= 40700 && !defined(__INTEL_COMPILER) && !defined(__clang__)
    // Gcc 4.7.0 also has operands in wrong order
    __m128i r0 = _mm256_castsi256_si128(_mm256_permutevar8x32_epi32(_mm256_castsi128_si256(index    ), table01));
    __m128i r1 = _mm256_castsi256_si128(_mm256_permutevar8x32_epi32(_mm256_castsi128_si256(index ^ 8), table23));
#else
    __m128i r0 = _mm256_castsi256_si128(_mm256_permutevar8x32_epi32(table01, _mm256_castsi128_si256(index)));
    __m128i r1 = _mm256_castsi256_si128(_mm256_permutevar8x32_epi32(table23, _mm256_castsi128_si256(index ^ 8)));
#endif // bug
    return _mm_blendv_epi8(r0, r1, index > 8);

#elif defined (__XOP__)  // AMD XOP instruction set. Use VPPERM
    Vec4i r0 = _mm_perm_epi8(table0, table1, ((index    ) * 0x04040404u + 0x63626160u) & 0X9F9F9F9Fu);
    Vec4i r1 = _mm_perm_epi8(table2, table3, ((index ^ 8) * 0x04040404u + 0x63626160u) & 0X9F9F9F9Fu);
    return r0 | r1;

#elif INSTRSET >= 5  // SSSE3
    Vec16c aa = Vec16c(Vec4i(0x73727170));
    Vec4i r0 = _mm_shuffle_epi8(table0, Vec16c((index     ) * 0x04040404) + aa);
    Vec4i r1 = _mm_shuffle_epi8(table1, Vec16c((index ^  4) * 0x04040404) + aa);
    Vec4i r2 = _mm_shuffle_epi8(table2, Vec16c((index ^  8) * 0x04040404) + aa);
    Vec4i r3 = _mm_shuffle_epi8(table3, Vec16c((index ^ 12) * 0x04040404) + aa);
    return (r0 | r1) | (r2 | r3);

#else    // SSE2
    int32_t ii[4], tt[16], rr[4];
    table0.store(tt);  table1.store(tt+4);  table2.store(tt+8);  table3.store(tt+12);
    index.store(ii);
    for (int j = 0; j < 4; j++) rr[j] = tt[ii[j] & 0x0F];
    return Vec4i().load(rr);
#endif
}

template <int n>
static inline Vec4i lookup(Vec4i const & index, void const * table) {
    if (n <= 0) return 0;
    if (n <= 4) return lookup4(index, Vec4i().load(table));
    if (n <= 8) return lookup8(index, Vec4i().load(table), Vec4i().load((int32_t*)table + 4));
    // n > 8. Limit index
    Vec4ui index1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec4ui(index) & (n-1);
    }
    else {
        // n is not a power of 2, limit to n-1
        index1 = min(Vec4ui(index), n-1);
    }
#if INSTRSET >= 8 // AVX2. Use VPERMD
    return _mm_i32gather_epi32((const int *)table, index1, 4);
#else
    uint32_t ii[4];  index1.store(ii);
    return Vec4i(((int32_t*)table)[ii[0]], ((int32_t*)table)[ii[1]], ((int32_t*)table)[ii[2]], ((int32_t*)table)[ii[3]]);
#endif
}


static inline Vec2q lookup2(Vec2q const & index, Vec2q const & table) {
#if INSTRSET >= 5  // SSSE3
    return _mm_shuffle_epi8(table, index * 0x0808080808080808ll + 0x0706050403020100ll);
#else
    int64_t ii[2], tt[2];
    table.store(tt);  index.store(ii);
    return Vec2q(tt[int(ii[0])], tt[int(ii[1])]);
#endif
}

template <int n>
static inline Vec2q lookup(Vec2q const & index, void const * table) {
    if (n <= 0) return 0;
    // n > 0. Limit index
    Vec2uq index1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec2uq(index) & (n-1);
    }
    else {
        // n is not a power of 2, limit to n-1.
        // There is no 64-bit min instruction, but we can use the 32-bit unsigned min,
        // since n is a 32-bit integer
        index1 = Vec2uq(min(Vec2uq(index), constant4i<n-1, 0, n-1, 0>()));
    }
    uint32_t ii[4];  index1.store(ii);  // use only lower 32 bits of each index
    int64_t const * tt = (int64_t const *)table;
    return Vec2q(tt[ii[0]], tt[ii[2]]);
}


/*****************************************************************************
*
*          Other permutations with variable indexes
*
*****************************************************************************/

// Function shift_bytes_up: shift whole vector left by b bytes.
// You may use a permute function instead if b is a compile-time constant
static inline Vec16c shift_bytes_up(Vec16c const & a, int b) {
    if ((uint32_t)b > 15) return _mm_setzero_si128();
#if INSTRSET >= 4    // SSSE3
    static const char mask[32] = {-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1, 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15};    
    return Vec16c(_mm_shuffle_epi8(a, Vec16c().load(mask+16-b)));
#else
    Vec2uq a1 = Vec2uq(a);
    if (b < 8) {    
        a1 = (a1 << (b*8)) | (permute2uq<-1,0>(a1) >> (64 - (b*8)));
    }
    else {
        a1 = permute2uq<-1,0>(a1) << ((b-8)*8);
    }
    return Vec16c(a1);
#endif
}

// Function shift_bytes_down: shift whole vector right by b bytes
// You may use a permute function instead if b is a compile-time constant
static inline Vec16c shift_bytes_down(Vec16c const & a, int b) {
    if ((uint32_t)b > 15) return _mm_setzero_si128();
#if INSTRSET >= 4    // SSSE3
    static const char mask[32] = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1};
    return Vec16c(_mm_shuffle_epi8(a, Vec16c().load(mask+b)));
#else
    Vec2uq a1 = Vec2uq(a);
    if (b < 8) {    
        a1 = (a1 >> (b*8)) | (permute2uq<1,-1>(a1) << (64 - (b*8)));
    }
    else {
        a1 = permute2uq<1,-1>(a1) >> ((b-8)*8); 
    }
    return Vec16c(a1);
#endif
}

/*****************************************************************************
*
*          Gather functions with fixed indexes
*
*****************************************************************************/
// Load elements from array a with indices i0, i1, i2, i3
template <int i0, int i1, int i2, int i3>
static inline Vec4i gather4i(void const * a) {
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
            Vec4i b = Vec4i().load((int32_t const *)a + imax-3);
            return permute4i<i0-imax+3, i1-imax+3, i2-imax+3, i3-imax+3>(b);
        }
        else {
            Vec4i b = Vec4i().load((int32_t const *)a + imin);
            return permute4i<i0-imin, i1-imin, i2-imin, i3-imin>(b);
        }
    }
    if ((i0<imin+4 || i0>imax-4) && (i1<imin+4 || i1>imax-4) && (i2<imin+4 || i2>imax-4) && (i3<imin+4 || i3>imax-4)) {
        // load two contiguous blocks and blend
        Vec4i b = Vec4i().load((int32_t const *)a + imin);
        Vec4i c = Vec4i().load((int32_t const *)a + imax-3);
        const int j0 = i0<imin+4 ? i0-imin : 7-imax+i0;
        const int j1 = i1<imin+4 ? i1-imin : 7-imax+i1;
        const int j2 = i2<imin+4 ? i2-imin : 7-imax+i2;
        const int j3 = i3<imin+4 ? i3-imin : 7-imax+i3;
        return blend4i<j0, j1, j2, j3>(b, c);
    }
    // use AVX2 gather if available
#if INSTRSET >= 8
    return _mm_i32gather_epi32((const int *)a, Vec4i(i0,i1,i2,i3), 4);
#else
    return lookup<imax+1>(Vec4i(i0,i1,i2,i3), a);
#endif
}

// Load elements from array a with indices i0, i1
template <int i0, int i1>
static inline Vec2q gather2q(void const * a) {
    Static_error_check<(i0|i1)>=0> Negative_array_index;  // Error message if index is negative
    const int imin = i0 < i1 ? i0 : i1;
    const int imax = i0 > i1 ? i0 : i1;
    if (imax - imin <= 1) {
        // load one contiguous block and permute
        if (imax > 1) {
            // make sure we don't read past the end of the array
            Vec2q b = Vec2q().load((int64_t const *)a + imax-1);
            return permute2q<i0-imax+1, i1-imax+1>(b);
        }
        else {
            Vec2q b = Vec2q().load((int64_t const *)a + imin);
            return permute2q<i0-imin, i1-imin>(b);
        }
    }
    return Vec2q(((int64_t*)a)[i0], ((int64_t*)a)[i1]);
}


/*****************************************************************************
*
*          Functions for conversion between integer sizes
*
*****************************************************************************/

// Extend 8-bit integers to 16-bit integers, signed and unsigned

// Function extend_low : extends the low 8 elements to 16 bits with sign extension
static inline Vec8s extend_low (Vec16c const & a) {
    __m128i sign = _mm_cmpgt_epi8(_mm_setzero_si128(),a);  // 0 > a
    return         _mm_unpacklo_epi8(a,sign);              // interleave with sign extensions
}

// Function extend_high : extends the high 8 elements to 16 bits with sign extension
static inline Vec8s extend_high (Vec16c const & a) {
    __m128i sign = _mm_cmpgt_epi8(_mm_setzero_si128(),a);  // 0 > a
    return         _mm_unpackhi_epi8(a,sign);              // interleave with sign extensions
}

// Function extend_low : extends the low 8 elements to 16 bits with zero extension
static inline Vec8us extend_low (Vec16uc const & a) {
    return    _mm_unpacklo_epi8(a,_mm_setzero_si128());    // interleave with zero extensions
}

// Function extend_high : extends the high 8 elements to 16 bits with zero extension
static inline Vec8us extend_high (Vec16uc const & a) {
    return    _mm_unpackhi_epi8(a,_mm_setzero_si128());    // interleave with zero extensions
}

// Extend 16-bit integers to 32-bit integers, signed and unsigned

// Function extend_low : extends the low 4 elements to 32 bits with sign extension
static inline Vec4i extend_low (Vec8s const & a) {
    __m128i sign = _mm_srai_epi16(a,15);                   // sign bit
    return         _mm_unpacklo_epi16(a,sign);             // interleave with sign extensions
}

// Function extend_high : extends the high 4 elements to 32 bits with sign extension
static inline Vec4i extend_high (Vec8s const & a) {
    __m128i sign = _mm_srai_epi16(a,15);                   // sign bit
    return         _mm_unpackhi_epi16(a,sign);             // interleave with sign extensions
}

// Function extend_low : extends the low 4 elements to 32 bits with zero extension
static inline Vec4ui extend_low (Vec8us const & a) {
    return    _mm_unpacklo_epi16(a,_mm_setzero_si128());   // interleave with zero extensions
}

// Function extend_high : extends the high 4 elements to 32 bits with zero extension
static inline Vec4ui extend_high (Vec8us const & a) {
    return    _mm_unpackhi_epi16(a,_mm_setzero_si128());   // interleave with zero extensions
}

// Extend 32-bit integers to 64-bit integers, signed and unsigned

// Function extend_low : extends the low 2 elements to 64 bits with sign extension
static inline Vec2q extend_low (Vec4i const & a) {
    __m128i sign = _mm_srai_epi32(a,31);                   // sign bit
    return         _mm_unpacklo_epi32(a,sign);             // interleave with sign extensions
}

// Function extend_high : extends the high 2 elements to 64 bits with sign extension
static inline Vec2q extend_high (Vec4i const & a) {
    __m128i sign = _mm_srai_epi32(a,31);                   // sign bit
    return         _mm_unpackhi_epi32(a,sign);             // interleave with sign extensions
}

// Function extend_low : extends the low 2 elements to 64 bits with zero extension
static inline Vec2uq extend_low (Vec4ui const & a) {
    return    _mm_unpacklo_epi32(a,_mm_setzero_si128());   // interleave with zero extensions
}

// Function extend_high : extends the high 2 elements to 64 bits with zero extension
static inline Vec2uq extend_high (Vec4ui const & a) {
    return    _mm_unpackhi_epi32(a,_mm_setzero_si128());   // interleave with zero extensions
}

// Compress 16-bit integers to 8-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Overflow wraps around
static inline Vec16c compress (Vec8s const & low, Vec8s const & high) {
    __m128i mask  = _mm_set1_epi32(0x00FF00FF);            // mask for low bytes
    __m128i lowm  = _mm_and_si128(low,mask);               // bytes of low
    __m128i highm = _mm_and_si128(high,mask);              // bytes of high
    return  _mm_packus_epi16(lowm,highm);                  // unsigned pack
}

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Signed, with saturation
static inline Vec16c compress_saturated (Vec8s const & low, Vec8s const & high) {
    return  _mm_packs_epi16(low,high);
}

// Function compress : packs two vectors of 16-bit integers to one vector of 8-bit integers
// Unsigned, overflow wraps around
static inline Vec16uc compress (Vec8us const & low, Vec8us const & high) {
    return  Vec16uc (compress((Vec8s)low, (Vec8s)high));
}

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Unsigned, with saturation
static inline Vec16uc compress_saturated (Vec8us const & low, Vec8us const & high) {
#if INSTRSET >= 5   // SSE4.1 supported
    __m128i maxval  = _mm_set1_epi32(0x00FF00FF);          // maximum value
    __m128i minval  = _mm_setzero_si128();                 // minimum value = 0
    __m128i low1    = _mm_min_epu16(low,maxval);           // upper limit
    __m128i high1   = _mm_min_epu16(high,maxval);          // upper limit
    __m128i low2    = _mm_max_epu16(low1,minval);          // lower limit
    __m128i high2   = _mm_max_epu16(high1,minval);         // lower limit
    return            _mm_packus_epi16(low2,high2);        // this instruction saturates from signed 32 bit to unsigned 16 bit
#else
    __m128i zero    = _mm_setzero_si128();                 // 0
    __m128i signlow = _mm_cmpgt_epi16(zero,low);           // sign bit of low
    __m128i signhi  = _mm_cmpgt_epi16(zero,high);          // sign bit of high
    __m128i slow2   = _mm_srli_epi16(signlow,8);           // FF if low negative
    __m128i shigh2  = _mm_srli_epi16(signhi,8);            // FF if high negative
    __m128i maskns  = _mm_set1_epi32(0x7FFF7FFF);          // mask for removing sign bit
    __m128i lowns   = _mm_and_si128(low,maskns);           // low,  with sign bit removed
    __m128i highns  = _mm_and_si128(high,maskns);          // high, with sign bit removed
    __m128i lowo    = _mm_or_si128(lowns,slow2);           // low,  sign bit replaced by 00FF
    __m128i higho   = _mm_or_si128(highns,shigh2);         // high, sign bit replaced by 00FF
    return            _mm_packus_epi16(lowo,higho);        // this instruction saturates from signed 16 bit to unsigned 8 bit
#endif
}

// Compress 32-bit integers to 16-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Overflow wraps around
static inline Vec8s compress (Vec4i const & low, Vec4i const & high) {
#if INSTRSET >= 5   // SSE4.1 supported
    __m128i mask  = _mm_set1_epi32(0x0000FFFF);            // mask for low words
    __m128i lowm  = _mm_and_si128(low,mask);               // bytes of low
    __m128i highm = _mm_and_si128(high,mask);              // bytes of high
    return  _mm_packus_epi32(lowm,highm);                  // unsigned pack
#else
    __m128i low1  = _mm_shufflelo_epi16(low,0xD8);         // low words in place
    __m128i high1 = _mm_shufflelo_epi16(high,0xD8);        // low words in place
    __m128i low2  = _mm_shufflehi_epi16(low1,0xD8);        // low words in place
    __m128i high2 = _mm_shufflehi_epi16(high1,0xD8);       // low words in place
    __m128i low3  = _mm_shuffle_epi32(low2,0xD8);          // low dwords of low  to pos. 0 and 32
    __m128i high3 = _mm_shuffle_epi32(high2,0xD8);         // low dwords of high to pos. 0 and 32
    return  _mm_unpacklo_epi64(low3,high3);                // interleave
#endif
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Signed with saturation
static inline Vec8s compress_saturated (Vec4i const & low, Vec4i const & high) {
    return  _mm_packs_epi32(low,high);                     // pack with signed saturation
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Overflow wraps around
static inline Vec8us compress (Vec4ui const & low, Vec4ui const & high) {
    return Vec8us (compress((Vec4i)low, (Vec4i)high));
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Unsigned, with saturation
static inline Vec8us compress_saturated (Vec4ui const & low, Vec4ui const & high) {
#if INSTRSET >= 5   // SSE4.1 supported
    __m128i maxval  = _mm_set1_epi32(0x0000FFFF);          // maximum value
    __m128i minval  = _mm_setzero_si128();                 // minimum value = 0
    __m128i low1    = _mm_min_epu32(low,maxval);           // upper limit
    __m128i high1   = _mm_min_epu32(high,maxval);          // upper limit
    __m128i low2    = _mm_max_epu32(low1,minval);          // lower limit
    __m128i high2   = _mm_max_epu32(high1,minval);         // lower limit
    return            _mm_packus_epi32(low2,high2);        // this instruction saturates from signed 32 bit to unsigned 16 bit
#else
    __m128i zero     = _mm_setzero_si128();                // 0
    __m128i lowzero  = _mm_cmpeq_epi16(low,zero);          // for each word is zero
    __m128i highzero = _mm_cmpeq_epi16(high,zero);         // for each word is zero
    __m128i mone     = _mm_set1_epi32(-1);                 // FFFFFFFF
    __m128i lownz    = _mm_xor_si128(lowzero,mone);        // for each word is nonzero
    __m128i highnz   = _mm_xor_si128(highzero,mone);       // for each word is nonzero
    __m128i lownz2   = _mm_srli_epi32(lownz,16);           // shift down to low dword
    __m128i highnz2  = _mm_srli_epi32(highnz,16);          // shift down to low dword
    __m128i lowsatur = _mm_or_si128(low,lownz2);           // low, saturated
    __m128i hisatur  = _mm_or_si128(high,highnz2);         // high, saturated
    return  Vec8us (compress(Vec4i(lowsatur), Vec4i(hisatur)));
#endif
}

// Compress 64-bit integers to 32-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Overflow wraps around
static inline Vec4i compress (Vec2q const & low, Vec2q const & high) {
    __m128i low2  = _mm_shuffle_epi32(low,0xD8);           // low dwords of low  to pos. 0 and 32
    __m128i high2 = _mm_shuffle_epi32(high,0xD8);          // low dwords of high to pos. 0 and 32
    return  _mm_unpacklo_epi64(low2,high2);                // interleave
}

// Function compress : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Signed, with saturation
// This function is very inefficient unless the SSE4.2 instruction set is supported
static inline Vec4i compress_saturated (Vec2q const & low, Vec2q const & high) {
    Vec2q maxval = _mm_set_epi32(0,0x7FFFFFFF,0,0x7FFFFFFF);
    Vec2q minval = _mm_set_epi32(-1,0x80000000,-1,0x80000000);
    Vec2q low1   = min(low,maxval);
    Vec2q high1  = min(high,maxval);
    Vec2q low2   = max(low1,minval);
    Vec2q high2  = max(high1,minval);
    return compress(low2,high2);
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Overflow wraps around
static inline Vec4ui compress (Vec2uq const & low, Vec2uq const & high) {
    return Vec4ui (compress((Vec2q)low, (Vec2q)high));
}

// Function compress : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Unsigned, with saturation
static inline Vec4ui compress_saturated (Vec2uq const & low, Vec2uq const & high) {
    __m128i zero     = _mm_setzero_si128();                // 0
    __m128i lowzero  = _mm_cmpeq_epi32(low,zero);          // for each dword is zero
    __m128i highzero = _mm_cmpeq_epi32(high,zero);         // for each dword is zero
    __m128i mone     = _mm_set1_epi32(-1);                 // FFFFFFFF
    __m128i lownz    = _mm_xor_si128(lowzero,mone);        // for each dword is nonzero
    __m128i highnz   = _mm_xor_si128(highzero,mone);       // for each dword is nonzero
    __m128i lownz2   = _mm_srli_epi64(lownz,32);           // shift down to low dword
    __m128i highnz2  = _mm_srli_epi64(highnz,32);          // shift down to low dword
    __m128i lowsatur = _mm_or_si128(low,lownz2);           // low, saturated
    __m128i hisatur  = _mm_or_si128(high,highnz2);         // high, saturated
    return  Vec4ui (compress(Vec2q(lowsatur), Vec2q(hisatur)));
}

/*****************************************************************************
*
*          Helper functions for division and bit scan
*
*****************************************************************************/

// Define popcount function. Gives sum of bits
#if INSTRSET >= 6   // SSE4.2
    // popcnt instruction is not officially part of the SSE4.2 instruction set,
    // but available in all known processors with SSE4.2
#if defined (__GNUC__) || defined(__clang__)
static inline uint32_t vml_popcnt (uint32_t a) __attribute__ ((pure));
static inline uint32_t vml_popcnt (uint32_t a) {	
    uint32_t r;
    __asm("popcnt %1, %0" : "=r"(r) : "r"(a) : );
    return r;
}
#else
static inline uint32_t vml_popcnt (uint32_t a) {	
    return _mm_popcnt_u32(a);  // MS intrinsic
}
#endif // platform
#else  // no SSE4.2
static inline uint32_t vml_popcnt (uint32_t a) {	
    // popcnt instruction not available
    uint32_t b = a - ((a >> 1) & 0x55555555);
    uint32_t c = (b & 0x33333333) + ((b >> 2) & 0x33333333);
    uint32_t d = (c + (c >> 4)) & 0x0F0F0F0F;
    uint32_t e = d * 0x01010101;
    return   e >> 24;
}
#endif


// Define bit-scan-forward function. Gives index to lowest set bit
#if defined (__GNUC__) || defined(__clang__)
static inline uint32_t bit_scan_reverse (uint32_t a) __attribute__ ((pure));
static inline uint32_t bit_scan_forward (uint32_t a) {	
    uint32_t r;
    __asm("bsfl %1, %0" : "=r"(r) : "r"(a) : );
    return r;
}
#else
static inline uint32_t bit_scan_forward (uint32_t a) {	
    unsigned long r;
    _BitScanForward(&r, a);                      // defined in intrin.h for MS and Intel compilers
    return r;
}
#endif

// Define bit-scan-reverse function. Gives index to highest set bit = floor(log2(a))
#if defined (__GNUC__) || defined(__clang__)
static inline uint32_t bit_scan_reverse (uint32_t a) __attribute__ ((pure));
static inline uint32_t bit_scan_reverse (uint32_t a) {	
    uint32_t r;
    __asm("bsrl %1, %0" : "=r"(r) : "r"(a) : );
    return r;
}
#else
static inline uint32_t bit_scan_reverse (uint32_t a) {	
    unsigned long r;
    _BitScanReverse(&r, a);                      // defined in intrin.h for MS and Intel compilers
    return r;
}
#endif

// Same function, for compile-time constants.
// We need template metaprogramming for calculating this function at compile time.
// This may take a long time to compile because of the template recursion.
// Todo: replace this with a constexpr function when C++14 becomes available
template <uint32_t n> 
struct BitScanR {
    enum {val = (
        n >= 0x10 ? 4 + (BitScanR<(n>>4)>::val) :
        n  <    2 ? 0 :
        n  <    4 ? 1 :
        n  <    8 ? 2 : 3 )                       };
};
template <> struct BitScanR<0> {enum {val = 0};};          // Avoid infinite template recursion

#define bit_scan_reverse_const(n)  (BitScanR<n>::val)      // n must be a valid compile-time constant


/*****************************************************************************
*
*          Integer division operators
*
******************************************************************************
*
* The instruction set does not support integer vector division. Instead, we
* are using a method for fast integer division based on multiplication and
* shift operations. This method is faster than simple integer division if the
* same divisor is used multiple times.
*
* All elements in a vector are divided by the same divisor. It is not possible
* to divide different elements of the same vector by different divisors.
*
* The parameters used for fast division are stored in an object of a 
* Divisor class. This object can be created implicitly, for example in:
*        Vec4i a, b; int c;
*        a = b / c;
* or explicitly as:
*        a = b / Divisor_i(c);
*
* It takes more time to compute the parameters used for fast division than to
* do the division. Therefore, it is advantageous to use the same divisor object
* multiple times. For example, to divide 80 unsigned short integers by 10:
*
*        uint16_t dividends[80], quotients[80];         // numbers to work with
*        Divisor_us div10(10);                          // make divisor object for dividing by 10
*        Vec8us temp;                                   // temporary vector
*        for (int i = 0; i < 80; i += 8) {              // loop for 4 elements per iteration
*            temp.load(dividends+i);                    // load 4 elements
*            temp /= div10;                             // divide each element by 10
*            temp.store(quotients+i);                   // store 4 elements
*        }
* 
* The parameters for fast division can also be computed at compile time. This is
* an advantage if the divisor is known at compile time. Use the const_int or const_uint
* macro to do this. For example, for signed integers:
*        Vec8s a, b;
*        a = b / const_int(10);
* Or, for unsigned integers:
*        Vec8us a, b;
*        a = b / const_uint(10);
*
* The division of a vector of 16-bit integers is faster than division of a vector 
* of other integer sizes.
*
* 
* Mathematical formula, used for signed division with fixed or variable divisor:
* (From T. Granlund and P. L. Montgomery: Division by Invariant Integers Using Multiplication,
* Proceedings of the SIGPLAN 1994 Conference on Programming Language Design and Implementation.
* http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.1.2556 )
* x = dividend
* d = abs(divisor)
* w = integer word size, bits
* L = ceil(log2(d)) = bit_scan_reverse(d-1)+1
* L = max(L,1)
* m = 1 + 2^(w+L-1)/d - 2^w                      [division should overflow to 0 if d = 1]
* sh1 = L-1
* q = x + (m*x >> w)                             [high part of signed multiplication with 2w bits]
* q = (q >> sh1) - (x<0 ? -1 : 0)
* if (divisor < 0) q = -q 
* result trunc(x/d) = q
*
* Mathematical formula, used for unsigned division with variable divisor:
* (Also from T. Granlund and P. L. Montgomery)
* x = dividend
* d = divisor
* w = integer word size, bits
* L = ceil(log2(d)) = bit_scan_reverse(d-1)+1
* m = 1 + 2^w * (2^L-d) / d                      [2^L should overflow to 0 if L = w]
* sh1 = min(L,1)
* sh2 = max(L-1,0)
* t = m*x >> w                                   [high part of unsigned multiplication with 2w bits]
* result floor(x/d) = (((x-t) >> sh1) + t) >> sh2
*
* Mathematical formula, used for unsigned division with fixed divisor:
* (From Terje Mathisen, unpublished)
* x = dividend
* d = divisor
* w = integer word size, bits
* b = floor(log2(d)) = bit_scan_reverse(d)
* f = 2^(w+b) / d                                [exact division]
* If f is an integer then d is a power of 2 then go to case A
* If the fractional part of f is < 0.5 then go to case B
* If the fractional part of f is > 0.5 then go to case C
* Case A:  [shift only]
* result = x >> b
* Case B:  [round down f and compensate by adding one to x]
* result = ((x+1)*floor(f)) >> (w+b)             [high part of unsigned multiplication with 2w bits]
* Case C:  [round up f, no compensation for rounding error]
* result = (x*ceil(f)) >> (w+b)                  [high part of unsigned multiplication with 2w bits]
*
*
*****************************************************************************/

// encapsulate parameters for fast division on vector of 4 32-bit signed integers
class Divisor_i {
protected:
    __m128i multiplier;                                    // multiplier used in fast division
    __m128i shift1;                                        // shift count used in fast division
    __m128i sign;                                          // sign of divisor
public:
    Divisor_i() {};                                        // Default constructor
    Divisor_i(int32_t d) {                                 // Constructor with divisor
        set(d);
    }
    Divisor_i(int m, int s1, int sgn) {                    // Constructor with precalculated multiplier, shift and sign
        multiplier = _mm_set1_epi32(m);
        shift1     = _mm_cvtsi32_si128(s1);
        sign       = _mm_set1_epi32(sgn);
    }
    void set(int32_t d) {                                  // Set or change divisor, calculate parameters
        const int32_t d1 = abs(d);
        int32_t sh, m;
        if (d1 > 1) {
            sh = bit_scan_reverse(d1-1);                   // shift count = ceil(log2(d1))-1 = (bit_scan_reverse(d1-1)+1)-1
            m = int32_t((int64_t(1) << (32+sh)) / d1 - ((int64_t(1) << 32) - 1)); // calculate multiplier
        }
        else {
            m  = 1;                                        // for d1 = 1
            sh = 0;
            if (d == 0) m /= d;                            // provoke error here if d = 0
            if (uint32_t(d) == 0x80000000u) {              // fix overflow for this special case
                m  = 0x80000001;
                sh = 30;
            }
        }
        multiplier = _mm_set1_epi32(m);                    // broadcast multiplier
        shift1     = _mm_setr_epi32(sh, 0, 0, 0);          // shift count
        sign       = _mm_set1_epi32(d < 0 ? -1 : 0);       // sign of divisor
    }
    __m128i getm() const {                                 // get multiplier
        return multiplier;
    }
    __m128i gets1() const {                                // get shift count
        return shift1;
    }
    __m128i getsign() const {                              // get sign of divisor
        return sign;
    }
};

// encapsulate parameters for fast division on vector of 4 32-bit unsigned integers
class Divisor_ui {
protected:
    __m128i multiplier;                                    // multiplier used in fast division
    __m128i shift1;                                        // shift count 1 used in fast division
    __m128i shift2;                                        // shift count 2 used in fast division
public:
    Divisor_ui() {};                                       // Default constructor
    Divisor_ui(uint32_t d) {                               // Constructor with divisor
        set(d);
    }
    Divisor_ui(uint32_t m, int s1, int s2) {               // Constructor with precalculated multiplier and shifts
        multiplier = _mm_set1_epi32(m);
        shift1     = _mm_setr_epi32(s1, 0, 0, 0);
        shift2     = _mm_setr_epi32(s2, 0, 0, 0);
    }
    void set(uint32_t d) {                                 // Set or change divisor, calculate parameters
        uint32_t L, L2, sh1, sh2, m;
        switch (d) {
        case 0:
            m = sh1 = sh2 = 1 / d;                         // provoke error for d = 0
            break;
        case 1:
            m = 1; sh1 = sh2 = 0;                          // parameters for d = 1
            break;
        case 2:
            m = 1; sh1 = 1; sh2 = 0;                       // parameters for d = 2
            break;
        default:                                           // general case for d > 2
            L  = bit_scan_reverse(d-1)+1;                  // ceil(log2(d))
            L2 = L < 32 ? 1 << L : 0;                      // 2^L, overflow to 0 if L = 32
            m  = 1 + uint32_t((uint64_t(L2 - d) << 32) / d); // multiplier
            sh1 = 1;  sh2 = L - 1;                         // shift counts
        }
        multiplier = _mm_set1_epi32(m);
        shift1     = _mm_setr_epi32(sh1, 0, 0, 0);
        shift2     = _mm_setr_epi32(sh2, 0, 0, 0);
    }
    __m128i getm() const {                                 // get multiplier
        return multiplier;
    }
    __m128i gets1() const {                                // get shift count 1
        return shift1;
    }
    __m128i gets2() const {                                // get shift count 2
        return shift2;
    }
};


// encapsulate parameters for fast division on vector of 8 16-bit signed integers
class Divisor_s {
protected:
    __m128i multiplier;                                    // multiplier used in fast division
    __m128i shift1;                                        // shift count used in fast division
    __m128i sign;                                          // sign of divisor
public:
    Divisor_s() {};                                        // Default constructor
    Divisor_s(int16_t d) {                                 // Constructor with divisor
        set(d);
    }
    Divisor_s(int16_t m, int s1, int sgn) {                // Constructor with precalculated multiplier, shift and sign
        multiplier = _mm_set1_epi16(m);
        shift1     = _mm_setr_epi32(s1, 0, 0, 0);
        sign       = _mm_set1_epi32(sgn);
    }
    void set(int16_t d) {                                  // Set or change divisor, calculate parameters
        const int32_t d1 = abs(d);
        int32_t sh, m;
        if (d1 > 1) {
            sh = bit_scan_reverse(d1-1);                   // shift count = ceil(log2(d1))-1 = (bit_scan_reverse(d1-1)+1)-1
            m = ((int32_t(1) << (16+sh)) / d1 - ((int32_t(1) << 16) - 1)); // calculate multiplier
        }
        else {
            m  = 1;                                        // for d1 = 1
            sh = 0;
            if (d == 0) m /= d;                            // provoke error here if d = 0
            if (uint16_t(d) == 0x8000u) {                  // fix overflow for this special case
                m  = 0x8001;
                sh = 14;
            }
        }
        multiplier = _mm_set1_epi16(int16_t(m));           // broadcast multiplier
        shift1     = _mm_setr_epi32(sh, 0, 0, 0);          // shift count
        sign       = _mm_set1_epi32(d < 0 ? -1 : 0);       // sign of divisor
    }
    __m128i getm() const {                                 // get multiplier
        return multiplier;
    }
    __m128i gets1() const {                                // get shift count
        return shift1;
    }
    __m128i getsign() const {                              // get sign of divisor
        return sign;
    }
};


// encapsulate parameters for fast division on vector of 8 16-bit unsigned integers
class Divisor_us {
protected:
    __m128i multiplier;                                    // multiplier used in fast division
    __m128i shift1;                                        // shift count 1 used in fast division
    __m128i shift2;                                        // shift count 2 used in fast division
public:
    Divisor_us() {};                                       // Default constructor
    Divisor_us(uint16_t d) {                               // Constructor with divisor
        set(d);
    }
    Divisor_us(uint16_t m, int s1, int s2) {               // Constructor with precalculated multiplier and shifts
        multiplier = _mm_set1_epi16(m);
        shift1     = _mm_setr_epi32(s1, 0, 0, 0);
        shift2     = _mm_setr_epi32(s2, 0, 0, 0);
    }
    void set(uint16_t d) {                                 // Set or change divisor, calculate parameters
        uint16_t L, L2, sh1, sh2, m;
        switch (d) {
        case 0:
            m = sh1 = sh2 = 1 / d;                         // provoke error for d = 0
            break;
        case 1:
            m = 1; sh1 = sh2 = 0;                          // parameters for d = 1
            break;
        case 2:
            m = 1; sh1 = 1; sh2 = 0;                       // parameters for d = 2
            break;
        default:                                           // general case for d > 2
            L  = (uint16_t)bit_scan_reverse(d-1)+1;        // ceil(log2(d))
            L2 = uint16_t(1 << L);                         // 2^L, overflow to 0 if L = 16
            m  = 1 + uint16_t((uint32_t(L2 - d) << 16) / d); // multiplier
            sh1 = 1;  sh2 = L - 1;                         // shift counts
        }
        multiplier = _mm_set1_epi16(m);
        shift1     = _mm_setr_epi32(sh1, 0, 0, 0);
        shift2     = _mm_setr_epi32(sh2, 0, 0, 0);
    }
    __m128i getm() const {                                 // get multiplier
        return multiplier;
    }
    __m128i gets1() const {                                // get shift count 1
        return shift1;
    }
    __m128i gets2() const {                                // get shift count 2
        return shift2;
    }
};


// vector operator / : divide each element by divisor

// vector of 4 32-bit signed integers
static inline Vec4i operator / (Vec4i const & a, Divisor_i const & d) {
#if defined (__XOP__) && defined (GCC_VERSION) && GCC_VERSION <= 40702/*??*/ && !defined(__INTEL_COMPILER) && !defined(__clang__)
#define XOP_MUL_BUG                                       // GCC has bug in XOP multiply
// Bug found in GCC version 4.7.0 and 4.7.1
#endif
// todo: test this when GCC bug is fixed
#if defined (__XOP__) && !defined (XOP_MUL_BUG)
    __m128i t1  = _mm_mul_epi32(a,d.getm());               // 32x32->64 bit signed multiplication of a[0] and a[2]
    __m128i t2  = _mm_srli_epi64(t1,32);                   // high dword of result 0 and 2
    __m128i t3  = _mm_macchi_epi32(a,d.getm(),_mm_setzero_si128());// 32x32->64 bit signed multiplication of a[1] and a[3]
    __m128i t5  = _mm_set_epi32(-1,0,-1,0);                // mask of dword 1 and 3
    __m128i t7  = _mm_blendv_epi8(t2,t3,t5);               // blend two results
    __m128i t8  = _mm_add_epi32(t7,a);                     // add
    __m128i t9  = _mm_sra_epi32(t8,d.gets1());             // shift right arithmetic
    __m128i t10 = _mm_srai_epi32(a,31);                    // sign of a
    __m128i t11 = _mm_sub_epi32(t10,d.getsign());          // sign of a - sign of d
    __m128i t12 = _mm_sub_epi32(t9,t11);                   // + 1 if a < 0, -1 if d < 0
    return        _mm_xor_si128(t12,d.getsign());          // change sign if divisor negative

#elif INSTRSET >= 5 && !defined (XOP_MUL_BUG)  // SSE4.1 supported 
    __m128i t1  = _mm_mul_epi32(a,d.getm());               // 32x32->64 bit signed multiplication of a[0] and a[2]
    __m128i t2  = _mm_srli_epi64(t1,32);                   // high dword of result 0 and 2
    __m128i t3  = _mm_srli_epi64(a,32);                    // get a[1] and a[3] into position for multiplication
    __m128i t4  = _mm_mul_epi32(t3,d.getm());              // 32x32->64 bit signed multiplication of a[1] and a[3]
    __m128i t5  = _mm_set_epi32(-1,0,-1,0);                // mask of dword 1 and 3
    __m128i t7  = _mm_blendv_epi8(t2,t4,t5);               // blend two results
    __m128i t8  = _mm_add_epi32(t7,a);                     // add
    __m128i t9  = _mm_sra_epi32(t8,d.gets1());             // shift right arithmetic
    __m128i t10 = _mm_srai_epi32(a,31);                    // sign of a
    __m128i t11 = _mm_sub_epi32(t10,d.getsign());          // sign of a - sign of d
    __m128i t12 = _mm_sub_epi32(t9,t11);                   // + 1 if a < 0, -1 if d < 0
    return        _mm_xor_si128(t12,d.getsign());          // change sign if divisor negative
#else  // not SSE4.1
    __m128i t1  = _mm_mul_epu32(a,d.getm());               // 32x32->64 bit unsigned multiplication of a[0] and a[2]
    __m128i t2  = _mm_srli_epi64(t1,32);                   // high dword of result 0 and 2
    __m128i t3  = _mm_srli_epi64(a,32);                    // get a[1] and a[3] into position for multiplication
    __m128i t4  = _mm_mul_epu32(t3,d.getm());              // 32x32->64 bit unsigned multiplication of a[1] and a[3]
    __m128i t5  = _mm_set_epi32(-1,0,-1,0);                // mask of dword 1 and 3
    __m128i t6  = _mm_and_si128(t4,t5);                    // high dword of result 1 and 3
    __m128i t7  = _mm_or_si128(t2,t6);                     // combine all four results of unsigned high mul into one vector
    // convert unsigned to signed high multiplication (from: H S Warren: Hacker's delight, 2003, p. 132)
    __m128i u1  = _mm_srai_epi32(a,31);                    // sign of a
    __m128i u2  = _mm_srai_epi32(d.getm(),31);             // sign of m [ m is always negative, except for abs(d) = 1 ]
    __m128i u3  = _mm_and_si128 (d.getm(),u1);             // m * sign of a
    __m128i u4  = _mm_and_si128 (a,u2);                    // a * sign of m
    __m128i u5  = _mm_add_epi32 (u3,u4);                   // sum of sign corrections
    __m128i u6  = _mm_sub_epi32 (t7,u5);                   // high multiplication result converted to signed
    __m128i t8  = _mm_add_epi32(u6,a);                     // add a
    __m128i t9  = _mm_sra_epi32(t8,d.gets1());             // shift right arithmetic
    __m128i t10 = _mm_sub_epi32(u1,d.getsign());           // sign of a - sign of d
    __m128i t11 = _mm_sub_epi32(t9,t10);                   // + 1 if a < 0, -1 if d < 0
    return        _mm_xor_si128(t11,d.getsign());          // change sign if divisor negative
#endif
}

// vector of 4 32-bit unsigned integers
static inline Vec4ui operator / (Vec4ui const & a, Divisor_ui const & d) {
    __m128i t1  = _mm_mul_epu32(a,d.getm());               // 32x32->64 bit unsigned multiplication of a[0] and a[2]
    __m128i t2  = _mm_srli_epi64(t1,32);                   // high dword of result 0 and 2
    __m128i t3  = _mm_srli_epi64(a,32);                    // get a[1] and a[3] into position for multiplication
    __m128i t4  = _mm_mul_epu32(t3,d.getm());              // 32x32->64 bit unsigned multiplication of a[1] and a[3]
    __m128i t5  = _mm_set_epi32(-1,0,-1,0);                // mask of dword 1 and 3
#if INSTRSET >= 5   // SSE4.1 supported
    __m128i t7  = _mm_blendv_epi8(t2,t4,t5);               // blend two results
#else
    __m128i t6  = _mm_and_si128(t4,t5);                    // high dword of result 1 and 3
    __m128i t7  = _mm_or_si128(t2,t6);                     // combine all four results into one vector
#endif
    __m128i t8  = _mm_sub_epi32(a,t7);                     // subtract
    __m128i t9  = _mm_srl_epi32(t8,d.gets1());             // shift right logical
    __m128i t10 = _mm_add_epi32(t7,t9);                    // add
    return        _mm_srl_epi32(t10,d.gets2());            // shift right logical 
}

// vector of 8 16-bit signed integers
static inline Vec8s operator / (Vec8s const & a, Divisor_s const & d) {
    __m128i t1  = _mm_mulhi_epi16(a, d.getm());            // multiply high signed words
    __m128i t2  = _mm_add_epi16(t1,a);                     // + a
    __m128i t3  = _mm_sra_epi16(t2,d.gets1());             // shift right arithmetic
    __m128i t4  = _mm_srai_epi16(a,15);                    // sign of a
    __m128i t5  = _mm_sub_epi16(t4,d.getsign());           // sign of a - sign of d
    __m128i t6  = _mm_sub_epi16(t3,t5);                    // + 1 if a < 0, -1 if d < 0
    return        _mm_xor_si128(t6,d.getsign());           // change sign if divisor negative
}

// vector of 8 16-bit unsigned integers
static inline Vec8us operator / (Vec8us const & a, Divisor_us const & d) {
    __m128i t1  = _mm_mulhi_epu16(a, d.getm());            // multiply high unsigned words
    __m128i t2  = _mm_sub_epi16(a,t1);                     // subtract
    __m128i t3  = _mm_srl_epi16(t2,d.gets1());             // shift right logical
    __m128i t4  = _mm_add_epi16(t1,t3);                    // add
    return        _mm_srl_epi16(t4,d.gets2());             // shift right logical 
}

 
// vector of 16 8-bit signed integers
static inline Vec16c operator / (Vec16c const & a, Divisor_s const & d) {
    // expand into two Vec8s
    Vec8s low  = extend_low(a)  / d;
    Vec8s high = extend_high(a) / d;
    return compress(low,high);
}

// vector of 16 8-bit unsigned integers
static inline Vec16uc operator / (Vec16uc const & a, Divisor_us const & d) {
    // expand into two Vec8s
    Vec8us low  = extend_low(a)  / d;
    Vec8us high = extend_high(a) / d;
    return compress(low,high);
}

// vector operator /= : divide
static inline Vec8s & operator /= (Vec8s & a, Divisor_s const & d) {
    a = a / d;
    return a;
}

// vector operator /= : divide
static inline Vec8us & operator /= (Vec8us & a, Divisor_us const & d) {
    a = a / d;
    return a;
}

// vector operator /= : divide
static inline Vec4i & operator /= (Vec4i & a, Divisor_i const & d) {
    a = a / d;
    return a;
}

// vector operator /= : divide
static inline Vec4ui & operator /= (Vec4ui & a, Divisor_ui const & d) {
    a = a / d;
    return a;
}

// vector operator /= : divide
static inline Vec16c & operator /= (Vec16c & a, Divisor_s const & d) {
    a = a / d;
    return a;
}

// vector operator /= : divide
static inline Vec16uc & operator /= (Vec16uc & a, Divisor_us const & d) {
    a = a / d;
    return a;
}

/*****************************************************************************
*
*          Integer division 2: divisor is a compile-time constant
*
*****************************************************************************/

// Divide Vec4i by compile-time constant
template <int32_t d>
static inline Vec4i divide_by_i(Vec4i const & x) {
    Static_error_check<(d!=0)> Dividing_by_zero;                     // Error message if dividing by zero
    if (d ==  1) return  x;
    if (d == -1) return -x;
    if (uint32_t(d) == 0x80000000u) return Vec4i(x == Vec4i(0x80000000)) & 1; // prevent overflow when changing sign
    const uint32_t d1 = d > 0 ? uint32_t(d) : uint32_t(-d);          // compile-time abs(d). (force GCC compiler to treat d as 32 bits, not 64 bits)
    if ((d1 & (d1-1)) == 0) {
        // d1 is a power of 2. use shift
        const int k = bit_scan_reverse_const(d1);
        __m128i sign;
        if (k > 1) sign = _mm_srai_epi32(x, k-1); else sign = x;     // k copies of sign bit
        __m128i bias    = _mm_srli_epi32(sign, 32-k);                // bias = x >= 0 ? 0 : k-1
        __m128i xpbias  = _mm_add_epi32 (x, bias);                   // x + bias
        __m128i q       = _mm_srai_epi32(xpbias, k);                 // (x + bias) >> k
        if (d > 0)      return q;                                    // d > 0: return  q
        return _mm_sub_epi32(_mm_setzero_si128(), q);                // d < 0: return -q
    }
    // general case
    const int32_t sh = bit_scan_reverse_const(uint32_t(d1)-1);            // ceil(log2(d1)) - 1. (d1 < 2 handled by power of 2 case)
    const int32_t mult = int(1 + (uint64_t(1) << (32+sh)) / uint32_t(d1) - (int64_t(1) << 32));   // multiplier
    const Divisor_i div(mult, sh, d < 0 ? -1 : 0);
    return x / div;
}

// define Vec4i a / const_int(d)
template <int32_t d>
static inline Vec4i operator / (Vec4i const & a, Const_int_t<d>) {
    return divide_by_i<d>(a);
}

// define Vec4i a / const_uint(d)
template <uint32_t d>
static inline Vec4i operator / (Vec4i const & a, Const_uint_t<d>) {
    Static_error_check< (d<0x80000000u) > Error_overflow_dividing_signed_by_unsigned; // Error: dividing signed by overflowing unsigned
    return divide_by_i<int32_t(d)>(a);                               // signed divide
}

// vector operator /= : divide
template <int32_t d>
static inline Vec4i & operator /= (Vec4i & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec4i & operator /= (Vec4i & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}


// Divide Vec4ui by compile-time constant
template <uint32_t d>
static inline Vec4ui divide_by_ui(Vec4ui const & x) {
    Static_error_check<(d!=0)> Dividing_by_zero;                     // Error message if dividing by zero
    if (d == 1) return x;                                            // divide by 1
    const int b = bit_scan_reverse_const(d);                         // floor(log2(d))
    if ((uint32_t(d) & (uint32_t(d)-1)) == 0) {
        // d is a power of 2. use shift
        return    _mm_srli_epi32(x, b);                              // x >> b
    }
    // general case (d > 2)
    uint32_t mult = uint32_t((uint64_t(1) << (b+32)) / d);           // multiplier = 2^(32+b) / d
    const uint64_t rem = (uint64_t(1) << (b+32)) - uint64_t(d)*mult; // remainder 2^(32+b) % d
    const bool round_down = (2*rem < d);                             // check if fraction is less than 0.5
    if (!round_down) {
        mult = mult + 1;                                             // round up mult
    }
    // do 32*32->64 bit unsigned multiplication and get high part of result
    const __m128i multv = _mm_set_epi32(0,mult,0,mult);              // zero-extend mult and broadcast
    __m128i t1  = _mm_mul_epu32(x,multv);                            // 32x32->64 bit unsigned multiplication of x[0] and x[2]
    if (round_down) {
        t1      = _mm_add_epi64(t1,multv);                           // compensate for rounding error. (x+1)*m replaced by x*m+m to avoid overflow
    }
    __m128i t2  = _mm_srli_epi64(t1,32);                             // high dword of result 0 and 2
    __m128i t3  = _mm_srli_epi64(x,32);                              // get x[1] and x[3] into position for multiplication
    __m128i t4  = _mm_mul_epu32(t3,multv);                           // 32x32->64 bit unsigned multiplication of x[1] and x[3]
    if (round_down) {
        t4      = _mm_add_epi64(t4,multv);                           // compensate for rounding error. (x+1)*m replaced by x*m+m to avoid overflow
    }
    __m128i t5  = _mm_set_epi32(-1,0,-1,0);                          // mask of dword 1 and 3
#if INSTRSET >= 5   // SSE4.1 supported
    __m128i t7  = _mm_blendv_epi8(t2,t4,t5);                         // blend two results
#else
    __m128i t6  = _mm_and_si128(t4,t5);                              // high dword of result 1 and 3
    __m128i t7  = _mm_or_si128(t2,t6);                               // combine all four results into one vector
#endif
    Vec4ui q    = _mm_srli_epi32(t7, b);                             // shift right by b
    return q;                                                    // no overflow possible
}

// define Vec4ui a / const_uint(d)
template <uint32_t d>
static inline Vec4ui operator / (Vec4ui const & a, Const_uint_t<d>) {
    return divide_by_ui<d>(a);
}

// define Vec4ui a / const_int(d)
template <int32_t d>
static inline Vec4ui operator / (Vec4ui const & a, Const_int_t<d>) {
    Static_error_check< (d>=0) > Error_dividing_unsigned_by_negative;// Error: dividing unsigned by negative is ambiguous
    return divide_by_ui<d>(a);                                       // unsigned divide
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec4ui & operator /= (Vec4ui & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <int32_t d>
static inline Vec4ui & operator /= (Vec4ui & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}


// Divide Vec8s by compile-time constant 
template <int d>
static inline Vec8s divide_by_i(Vec8s const & x) {
    const int16_t d0 = int16_t(d);                                   // truncate d to 16 bits
    Static_error_check<(d0 != 0)> Dividing_by_zero;                  // Error message if dividing by zero
    if (d0 ==  1) return  x;                                         // divide by  1
    if (d0 == -1) return -x;                                         // divide by -1
    if (uint16_t(d0) == 0x8000u) return (x == Vec8s(0x8000)) & 1;    // prevent overflow when changing sign
    // if (d > 0x7FFF || d < -0x8000) return 0;                      // not relevant when d truncated to 16 bits
    const uint16_t d1 = d0 > 0 ? d0 : -d0;                           // compile-time abs(d0)
    if ((d1 & (d1-1)) == 0) {
        // d is a power of 2. use shift
        const int k = bit_scan_reverse_const(uint32_t(d1));
        __m128i sign;
        if (k > 1) sign = _mm_srai_epi16(x, k-1); else sign = x;     // k copies of sign bit
        __m128i bias    = _mm_srli_epi16(sign, 16-k);                // bias = x >= 0 ? 0 : k-1
        __m128i xpbias  = _mm_add_epi16 (x, bias);                   // x + bias
        __m128i q       = _mm_srai_epi16(xpbias, k);                 // (x + bias) >> k
        if (d0 > 0)  return q;                                       // d0 > 0: return  q
        return _mm_sub_epi16(_mm_setzero_si128(), q);                // d0 < 0: return -q
    }
    // general case
    const int L = bit_scan_reverse_const(uint16_t(d1-1)) + 1;        // ceil(log2(d)). (d < 2 handled above)
    const int16_t mult = int16_t(1 + (1u << (15+L)) / uint32_t(d1) - 0x10000);// multiplier
    const int shift1 = L - 1;
    const Divisor_s div(mult, shift1, d0 > 0 ? 0 : -1);
    return x / div;
}

// define Vec8s a / const_int(d)
template <int d>
static inline Vec8s operator / (Vec8s const & a, Const_int_t<d>) {
    return divide_by_i<d>(a);
}

// define Vec8s a / const_uint(d)
template <uint32_t d>
static inline Vec8s operator / (Vec8s const & a, Const_uint_t<d>) {
    Static_error_check< (d<0x8000u) > Error_overflow_dividing_signed_by_unsigned; // Error: dividing signed by overflowing unsigned
    return divide_by_i<int(d)>(a);                                   // signed divide
}

// vector operator /= : divide
template <int32_t d>
static inline Vec8s & operator /= (Vec8s & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec8s & operator /= (Vec8s & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}


// Divide Vec8us by compile-time constant
template <uint32_t d>
static inline Vec8us divide_by_ui(Vec8us const & x) {
    const uint16_t d0 = uint16_t(d);                                 // truncate d to 16 bits
    Static_error_check<(d0 != 0)> Dividing_by_zero;                  // Error message if dividing by zero
    if (d0 == 1) return x;                                           // divide by 1
    const int b = bit_scan_reverse_const(d0);                        // floor(log2(d))
    if ((d0 & (d0-1)) == 0) {
        // d is a power of 2. use shift
        return  _mm_srli_epi16(x, b);                                // x >> b
    }
    // general case (d > 2)
    uint16_t mult = uint16_t((uint32_t(1) << (b+16)) / d0);          // multiplier = 2^(32+b) / d
    const uint32_t rem = (uint32_t(1) << (b+16)) - uint32_t(d0)*mult;// remainder 2^(32+b) % d
    const bool round_down = (2*rem < d0);                            // check if fraction is less than 0.5
    Vec8us x1 = x;
    if (round_down) {
        x1 = x1 + 1;                                                 // round down mult and compensate by adding 1 to x
    }
    else {
        mult = mult + 1;                                             // round up mult. no compensation needed
    }
    const __m128i multv = _mm_set1_epi16(mult);                      // broadcast mult
    __m128i xm = _mm_mulhi_epu16(x1, multv);                         // high part of 16x16->32 bit unsigned multiplication
    Vec8us q    = _mm_srli_epi16(xm, b);                             // shift right by b
    if (round_down) {
        Vec8s overfl = (x1 == (Vec8us)_mm_setzero_si128());                  // check for overflow of x+1
        return select(overfl, Vec8us(mult >> b), q);                 // deal with overflow (rarely needed)
    }
    else {
        return q;                                                    // no overflow possible
    }
}

// define Vec8us a / const_uint(d)
template <uint32_t d>
static inline Vec8us operator / (Vec8us const & a, Const_uint_t<d>) {
    return divide_by_ui<d>(a);
}

// define Vec8us a / const_int(d)
template <int d>
static inline Vec8us operator / (Vec8us const & a, Const_int_t<d>) {
    Static_error_check< (d>=0) > Error_dividing_unsigned_by_negative;// Error: dividing unsigned by negative is ambiguous
    return divide_by_ui<d>(a);                                       // unsigned divide
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec8us & operator /= (Vec8us & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <int32_t d>
static inline Vec8us & operator /= (Vec8us & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}


// define Vec16c a / const_int(d)
template <int d>
static inline Vec16c operator / (Vec16c const & a, Const_int_t<d>) {
    // expand into two Vec8s
    Vec8s low  = extend_low(a)  / Const_int_t<d>();
    Vec8s high = extend_high(a) / Const_int_t<d>();
    return compress(low,high);
}

// define Vec16c a / const_uint(d)
template <uint32_t d>
static inline Vec16c operator / (Vec16c const & a, Const_uint_t<d>) {
    Static_error_check< (uint8_t(d)<0x80u) > Error_overflow_dividing_signed_by_unsigned; // Error: dividing signed by overflowing unsigned
    return a / Const_int_t<d>();                              // signed divide
}

// vector operator /= : divide
template <int32_t d>
static inline Vec16c & operator /= (Vec16c & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}
// vector operator /= : divide
template <uint32_t d>
static inline Vec16c & operator /= (Vec16c & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// define Vec16uc a / const_uint(d)
template <uint32_t d>
static inline Vec16uc operator / (Vec16uc const & a, Const_uint_t<d>) {
    // expand into two Vec8usc
    Vec8us low  = extend_low(a)  / Const_uint_t<d>();
    Vec8us high = extend_high(a) / Const_uint_t<d>();
    return compress(low,high);
}

// define Vec16uc a / const_int(d)
template <int d>
static inline Vec16uc operator / (Vec16uc const & a, Const_int_t<d>) {
    Static_error_check< (int8_t(d)>=0) > Error_dividing_unsigned_by_negative;// Error: dividing unsigned by negative is ambiguous
    return a / Const_uint_t<d>();                         // unsigned divide
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec16uc & operator /= (Vec16uc & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <int32_t d>
static inline Vec16uc & operator /= (Vec16uc & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}

/*****************************************************************************
*
*          Horizontal scan functions
*
*****************************************************************************/

// Get index to the first element that is true. Return -1 if all are false
static inline int horizontal_find_first(Vec16cb const & x) {
    uint32_t a = _mm_movemask_epi8(x);
    if (a == 0) return -1;
    int32_t b = bit_scan_forward(a);
    return b;
}

static inline int horizontal_find_first(Vec8sb const & x) {
    return horizontal_find_first(Vec16cb(x)) >> 1;   // must use signed shift
}

static inline int horizontal_find_first(Vec4ib const & x) {
    return horizontal_find_first(Vec16cb(x)) >> 2;   // must use signed shift
}

static inline int horizontal_find_first(Vec2qb const & x) {
    return horizontal_find_first(Vec16cb(x)) >> 3;   // must use signed shift
}

// Count the number of elements that are true
static inline uint32_t horizontal_count(Vec16cb const & x) {
    uint32_t a = _mm_movemask_epi8(x);
    return vml_popcnt(a);
}

static inline uint32_t horizontal_count(Vec8sb const & x) {
    return horizontal_count(Vec16cb(x)) >> 1;
}

static inline uint32_t horizontal_count(Vec4ib const & x) {
    return horizontal_count(Vec16cb(x)) >> 2;
}

static inline uint32_t horizontal_count(Vec2qb const & x) {
    return horizontal_count(Vec16cb(x)) >> 3;
}


/*****************************************************************************
*
*          Boolean <-> bitfield conversion functions
*
*****************************************************************************/

// to_bits: convert boolean vector to integer bitfield
static inline uint16_t to_bits(Vec16cb const & x) {
    return (uint16_t)_mm_movemask_epi8(x);
}

// to_Vec16bc: convert integer bitfield to boolean vector
static inline Vec16cb to_Vec16cb(uint16_t x) {
    static const uint32_t table[16] = {  // lookup-table
        0x00000000, 0x000000FF, 0x0000FF00, 0x0000FFFF, 
        0x00FF0000, 0x00FF00FF, 0x00FFFF00, 0x00FFFFFF, 
        0xFF000000, 0xFF0000FF, 0xFF00FF00, 0xFF00FFFF, 
        0xFFFF0000, 0xFFFF00FF, 0xFFFFFF00, 0xFFFFFFFF}; 
    uint32_t a0 = table[x       & 0xF];
    uint32_t a1 = table[(x>>4)  & 0xF];
    uint32_t a2 = table[(x>>8)  & 0xF];
    uint32_t a3 = table[(x>>12) & 0xF];
    return Vec16cb(Vec16c(Vec4ui(a0, a1, a2, a3)));
}

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec8sb const & x) {
    __m128i a = _mm_packs_epi16(x, x);  // 16-bit words to bytes
    return (uint8_t)_mm_movemask_epi8(a);
}

// to_Vec8sb: convert integer bitfield to boolean vector
static inline Vec8sb to_Vec8sb(uint8_t x) {
    static const uint32_t table[16] = {  // lookup-table
        0x00000000, 0x000000FF, 0x0000FF00, 0x0000FFFF, 
        0x00FF0000, 0x00FF00FF, 0x00FFFF00, 0x00FFFFFF, 
        0xFF000000, 0xFF0000FF, 0xFF00FF00, 0xFF00FFFF, 
        0xFFFF0000, 0xFFFF00FF, 0xFFFFFF00, 0xFFFFFFFF}; 
    uint32_t a0 = table[x       & 0xF];
    uint32_t a1 = table[(x>>4)  & 0xF];
    Vec4ui   b  = Vec4ui(a0, a1, a0, a1);
    return _mm_unpacklo_epi8(b, b);  // duplicate bytes to 16-bit words
}

#if INSTRSET < 9 || MAX_VECTOR_SIZE < 512
// These functions are defined in Vectori512.h if AVX512 instruction set is used

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec4ib const & x) {
    __m128i a = _mm_packs_epi32(x, x);  // 32-bit dwords to 16-bit words
    __m128i b = _mm_packs_epi16(a, a);  // 16-bit words to bytes
    return _mm_movemask_epi8(b) & 0xF;
}

// to_Vec4ib: convert integer bitfield to boolean vector
static inline Vec4ib to_Vec4ib(uint8_t x) {
    static const uint32_t table[16] = {    // lookup-table
        0x00000000, 0x000000FF, 0x0000FF00, 0x0000FFFF, 
        0x00FF0000, 0x00FF00FF, 0x00FFFF00, 0x00FFFFFF, 
        0xFF000000, 0xFF0000FF, 0xFF00FF00, 0xFF00FFFF, 
        0xFFFF0000, 0xFFFF00FF, 0xFFFFFF00, 0xFFFFFFFF}; 
    uint32_t a = table[x & 0xF];           // 4 bytes
    __m128i b = _mm_cvtsi32_si128(a);      // transfer to vector register
    __m128i c = _mm_unpacklo_epi8(b, b);   // duplicate bytes to 16-bit words
    __m128i d = _mm_unpacklo_epi16(c, c);  // duplicate 16-bit words to 32-bit dwords
    return d;
}

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec2qb const & x) {
    uint32_t a = _mm_movemask_epi8(x);
    return (a & 1) | ((a >> 7) & 2);
}

// to_Vec2qb: convert integer bitfield to boolean vector
static inline Vec2qb to_Vec2qb(uint8_t x) {
    return Vec2qb(Vec2q(-(x&1), -((x>>1)&1)));
}

#else  // function prototypes here only

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec4ib x);

// to_Vec4ib: convert integer bitfield to boolean vector
static inline Vec4ib to_Vec4ib(uint8_t x);

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec2qb x);

// to_Vec2qb: convert integer bitfield to boolean vector
static inline Vec2qb to_Vec2qb(uint8_t x);

#endif  // INSTRSET < 9 || MAX_VECTOR_SIZE < 512

#endif // VECTORI128_H
