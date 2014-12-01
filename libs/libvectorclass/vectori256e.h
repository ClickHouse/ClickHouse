/****************************  vectori256e.h   *******************************
* Author:        Agner Fog
* Date created:  2012-05-30
* Last modified: 2014-10-16
* Version:       1.16
* Project:       vector classes
* Description:
* Header file defining 256-bit integer point vector classes as interface
* to intrinsic functions. Emulated for processors without AVX2 instruction set.
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
* For detailed instructions, see VectorClass.pdf
*
* (c) Copyright 2012 - 2014 GNU General Public License http://www.gnu.org/licenses
*****************************************************************************/

// check combination of header files
#if defined (VECTORI256_H)
#if    VECTORI256_H != 1
#error Two different versions of vectori256.h included
#endif
#else
#define VECTORI256_H  1

#ifdef VECTORF256_H
#error Please put header file vectori256.h or vectori256e.h before vectorf256e.h
#endif


#include "vectori128.h"


/*****************************************************************************
*
*          base class Vec256ie
*
*****************************************************************************/
// base class to replace Vec256ie when AVX2 is not supported
class Vec256ie {
protected:
    __m128i y0;                         // low half
    __m128i y1;                         // high half
public:
    Vec256ie(void) {};                  // default constructor
    Vec256ie(__m128i x0, __m128i x1) {  // constructor to build from two __m128i
        y0 = x0;  y1 = x1;
    }
    __m128i get_low() const {           // get low half
        return y0;
    }
    __m128i get_high() const {          // get high half
        return y1;
    }
};


/*****************************************************************************
*
*          Vector of 256 1-bit unsigned integers or Booleans
*
*****************************************************************************/

class Vec256b : public Vec256ie {
public:
    // Default constructor:
    Vec256b() {
    }
    // Constructor to broadcast the same value into all elements
    // Removed because of undesired implicit conversions
    //Vec256b(int i) {
    //    y1 = y0 = _mm_set1_epi32(-(i & 1));}

    // Constructor to build from two Vec128b:
    Vec256b(Vec128b const & a0, Vec128b const & a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256ie
    Vec256b(Vec256ie const & x) {
        y0 = x.get_low();  y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256ie
    Vec256b & operator = (Vec256ie const & x) {
        y0 = x.get_low();  y1 = x.get_high();
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec256b & load(void const * p) {
        y0 = _mm_loadu_si128((__m128i const*)p);
        y1 = _mm_loadu_si128((__m128i const*)p + 1);
        return *this;
    }
    // Member function to load from array, aligned by 32
    // You may use load_a instead of load if you are certain that p points to an address
    // divisible by 32, but there is hardly any speed advantage of load_a on modern processors
    Vec256b & load_a(void const * p) {
        y0 = _mm_load_si128((__m128i const*)p);
        y1 = _mm_load_si128((__m128i const*)p + 1);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(void * p) const {
        _mm_storeu_si128((__m128i*)p,     y0);
        _mm_storeu_si128((__m128i*)p + 1, y1);
    }
    // Member function to store into array, aligned by 32
    // You may use store_a instead of store if you are certain that p points to an address
    // divisible by 32, but there is hardly any speed advantage of load_a on modern processors
    void store_a(void * p) const {
        _mm_store_si128((__m128i*)p,     y0);
        _mm_store_si128((__m128i*)p + 1, y1);
    }
    // Member function to change a single bit
    // Note: This function is inefficient. Use load function if changing more than one bit
    Vec256b const & set_bit(uint32_t index, int value) {
        if (index < 128) {
            y0 = Vec128b(y0).set_bit(index, value);
        }
        else {
            y1 = Vec128b(y1).set_bit(index-128, value);
        }
        return *this;
    }
    // Member function to get a single bit
    // Note: This function is inefficient. Use store function if reading more than one bit
    int get_bit(uint32_t index) const {
        if (index < 128) {
            return Vec128b(y0).get_bit(index);
        }
        else {
            return Vec128b(y1).get_bit(index-128);
        }
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    bool operator [] (uint32_t index) const {
        return get_bit(index) != 0;
    }
    // Member functions to split into two Vec128b:
    Vec128b get_low() const {
        return y0;
    }
    Vec128b get_high() const {
        return y1;
    }
    static int size () {
        return 256;
    }
};

// Define operators for this class

// vector operator & : bitwise and
static inline Vec256b operator & (Vec256b const & a, Vec256b const & b) {
    return Vec256b(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec256b operator && (Vec256b const & a, Vec256b const & b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec256b operator | (Vec256b const & a, Vec256b const & b) {
    return Vec256b(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}
static inline Vec256b operator || (Vec256b const & a, Vec256b const & b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec256b operator ^ (Vec256b const & a, Vec256b const & b) {
    return Vec256b(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ~ : bitwise not
static inline Vec256b operator ~ (Vec256b const & a) {
    return Vec256b(~a.get_low(), ~a.get_high());
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
    return Vec256b(andnot(a.get_low(), b.get_low()), andnot(a.get_high(), b.get_high()));
}


/*****************************************************************************
*
*          Generate compile-time constant vector
*
*****************************************************************************/
// Generate a constant vector of 8 integers stored in memory.
// Can be converted to any integer vector type
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec256ie constant8i() {
    static const union {
        int32_t i[8];
        __m128i y[2];
    } u = {{i0,i1,i2,i3,i4,i5,i6,i7}};
    return Vec256ie(u.y[0], u.y[1]);
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
static inline Vec256ie selectb (Vec256ie const & s, Vec256ie const & a, Vec256ie const & b) {
    return Vec256ie(selectb(s.get_low(),  a.get_low(),  b.get_low()), 
                    selectb(s.get_high(), a.get_high(), b.get_high()));
}



/*****************************************************************************
*
*          Horizontal Boolean functions
*
*****************************************************************************/

// horizontal_and. Returns true if all bits are 1
static inline bool horizontal_and (Vec256b const & a) {
    return horizontal_and(a.get_low() & a.get_high());
}

// horizontal_or. Returns true if at least one bit is 1
static inline bool horizontal_or (Vec256b const & a) {
    return horizontal_or(a.get_low() | a.get_high());
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
        y1 = y0 = _mm_set1_epi8((char)i);
    };
    // Constructor to build from all elements:
    Vec32c(int8_t i0, int8_t i1, int8_t i2, int8_t i3, int8_t i4, int8_t i5, int8_t i6, int8_t i7,
        int8_t i8, int8_t i9, int8_t i10, int8_t i11, int8_t i12, int8_t i13, int8_t i14, int8_t i15,        
        int8_t i16, int8_t i17, int8_t i18, int8_t i19, int8_t i20, int8_t i21, int8_t i22, int8_t i23,
        int8_t i24, int8_t i25, int8_t i26, int8_t i27, int8_t i28, int8_t i29, int8_t i30, int8_t i31) {
        y0 = _mm_setr_epi8(i0,  i1,  i2,  i3,  i4,  i5,  i6,  i7,  i8,  i9,  i10, i11, i12, i13, i14, i15);
        y1 = _mm_setr_epi8(i16, i17, i18, i19, i20, i21, i22, i23, i24, i25, i26, i27, i28, i29, i30, i31);
    };
    // Constructor to build from two Vec16c:
    Vec32c(Vec16c const & a0, Vec16c const & a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256ie
    Vec32c(Vec256ie const & x) {
        y0 = x.get_low();
        y1 = x.get_high();
    };
    // Assignment operator to convert from type Vec256ie
    Vec32c & operator = (Vec256ie const & x) {
        y0 = x.get_low();
        y1 = x.get_high();
        return *this;
    };
    // Member function to load from array (unaligned)
    Vec32c & load(void const * p) {
        y0 = _mm_loadu_si128((__m128i const*)p);
        y1 = _mm_loadu_si128((__m128i const*)p + 1);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec32c & load_a(void const * p) {
        y0 = _mm_load_si128((__m128i const*)p);
        y1 = _mm_load_si128((__m128i const*)p + 1);
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
        if (index < 16) {
            y0 = Vec16c(y0).insert(index, value);
        }
        else {
            y1 = Vec16c(y1).insert(index-16, value);
        }
        return *this;
    }
    // Member function extract a single element from vector
    int8_t extract(uint32_t index) const {
        if (index < 16) {
            return Vec16c(y0).extract(index);
        }
        else {
            return Vec16c(y1).extract(index-16);
        }
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int8_t operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec16c:
    Vec16c get_low() const {
        return y0;
    }
    Vec16c get_high() const {
        return y1;
    }
    static int size () {
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
    Vec32cb(){}
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
    // Constructor to convert from type Vec256ie
    Vec32cb(Vec256ie const & x) {
        y0 = x.get_low();
        y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256ie
    Vec32cb & operator = (Vec256ie const & x) {
        y0 = x.get_low();
        y1 = x.get_high();
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
        return y0;
    }
    Vec16cb get_high() const {
        return y1;
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
    return Vec32c(a.get_low() + b.get_low(), a.get_high() + b.get_high());
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
    return Vec32c(a.get_low() - b.get_low(), a.get_high() - b.get_high());
}

// vector operator - : unary minus
static inline Vec32c operator - (Vec32c const & a) {
    return Vec32c(-a.get_low(), -a.get_high());
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
    return Vec32c(a.get_low() * b.get_low(), a.get_high() * b.get_high());
}

// vector operator *= : multiply
static inline Vec32c & operator *= (Vec32c & a, Vec32c const & b) {
    a = a * b;
    return a;
}

// vector of 32 8-bit signed integers
static inline Vec32c operator / (Vec32c const & a, Divisor_s const & d) {
    return Vec32c(a.get_low() / d, a.get_high() / d);
}

// vector operator /= : divide
static inline Vec32c & operator /= (Vec32c & a, Divisor_s const & d) {
    a = a / d;
    return a;
}

// vector operator << : shift left all elements
static inline Vec32c operator << (Vec32c const & a, int b) {
    return Vec32c(a.get_low() << b, a.get_high() << b);
}

// vector operator <<= : shift left
static inline Vec32c & operator <<= (Vec32c & a, int b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic all elements
static inline Vec32c operator >> (Vec32c const & a, int b) {
    return Vec32c(a.get_low() >> b, a.get_high() >> b);
}

// vector operator >>= : shift right artihmetic
static inline Vec32c & operator >>= (Vec32c & a, int b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec32cb operator == (Vec32c const & a, Vec32c const & b) {
    return Vec32c(a.get_low() == b.get_low(), a.get_high() == b.get_high());
}

// vector operator != : returns true for elements for which a != b
static inline Vec32cb operator != (Vec32c const & a, Vec32c const & b) {
    return Vec32c(a.get_low() != b.get_low(), a.get_high() != b.get_high());
}

// vector operator > : returns true for elements for which a > b (signed)
static inline Vec32cb operator > (Vec32c const & a, Vec32c const & b) {
    return Vec32c(a.get_low() > b.get_low(), a.get_high() > b.get_high());
}

// vector operator < : returns true for elements for which a < b (signed)
static inline Vec32cb operator < (Vec32c const & a, Vec32c const & b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec32cb operator >= (Vec32c const & a, Vec32c const & b) {
    return Vec32c(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec32cb operator <= (Vec32c const & a, Vec32c const & b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec32c operator & (Vec32c const & a, Vec32c const & b) {
    return Vec32c(a.get_low() & b.get_low(), a.get_high() & b.get_high());
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
    return Vec32c(a.get_low() | b.get_low(), a.get_high() | b.get_high());
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
    return Vec32c(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}
// vector operator ^= : bitwise xor
static inline Vec32c & operator ^= (Vec32c & a, Vec32c const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec32c operator ~ (Vec32c const & a) {
    return Vec32c(~a.get_low(), ~a.get_high());
}

// vector operator ! : logical not, returns true for elements == 0
static inline Vec32cb operator ! (Vec32c const & a) {
    return Vec32c(!a.get_low(), !a.get_high());
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
    return horizontal_add(a.get_low() + a.get_high());
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Each element is sign-extended before addition to avoid overflow
static inline int32_t horizontal_add_x (Vec32c const & a) {
    return horizontal_add_x(a.get_low()) + horizontal_add_x(a.get_high());
}


// function add_saturated: add element by element, signed with saturation
static inline Vec32c add_saturated(Vec32c const & a, Vec32c const & b) {
    return Vec32c(add_saturated(a.get_low(),b.get_low()), add_saturated(a.get_high(),b.get_high()));
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec32c sub_saturated(Vec32c const & a, Vec32c const & b) {
    return Vec32c(sub_saturated(a.get_low(),b.get_low()), sub_saturated(a.get_high(),b.get_high()));
}

// function max: a > b ? a : b
static inline Vec32c max(Vec32c const & a, Vec32c const & b) {
    return Vec32c(max(a.get_low(),b.get_low()), max(a.get_high(),b.get_high()));
}

// function min: a < b ? a : b
static inline Vec32c min(Vec32c const & a, Vec32c const & b) {
    return Vec32c(min(a.get_low(),b.get_low()), min(a.get_high(),b.get_high()));
}

// function abs: a >= 0 ? a : -a
static inline Vec32c abs(Vec32c const & a) {
    return Vec32c(abs(a.get_low()), abs(a.get_high()));
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec32c abs_saturated(Vec32c const & a) {
    return Vec32c(abs_saturated(a.get_low()), abs_saturated(a.get_high()));
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec32c rotate_left(Vec32c const & a, int b) {
    return Vec32c(rotate_left(a.get_low(),b), rotate_left(a.get_high(),b));
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
        y1 = y0 = _mm_set1_epi8((char)i);
    };
    // Constructor to build from all elements:
    Vec32uc(uint8_t i0, uint8_t i1, uint8_t i2, uint8_t i3, uint8_t i4, uint8_t i5, uint8_t i6, uint8_t i7,
        uint8_t i8, uint8_t i9, uint8_t i10, uint8_t i11, uint8_t i12, uint8_t i13, uint8_t i14, uint8_t i15,        
        uint8_t i16, uint8_t i17, uint8_t i18, uint8_t i19, uint8_t i20, uint8_t i21, uint8_t i22, uint8_t i23,
        uint8_t i24, uint8_t i25, uint8_t i26, uint8_t i27, uint8_t i28, uint8_t i29, uint8_t i30, uint8_t i31) {
        y0 = _mm_setr_epi8(i0,  i1,  i2,  i3,  i4,  i5,  i6,  i7,  i8,  i9,  i10, i11, i12, i13, i14, i15);
        y1 = _mm_setr_epi8(i16, i17, i18, i19, i20, i21, i22, i23, i24, i25, i26, i27, i28, i29, i30, i31);
    };
    // Constructor to build from two Vec16uc:
    Vec32uc(Vec16uc const & a0, Vec16uc const & a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256ie
    Vec32uc(Vec256ie const & x) {
        y0 = x.get_low();  y1 = x.get_high();
    };
    // Assignment operator to convert from type Vec256ie
    Vec32uc & operator = (Vec256ie const & x) {
        y0 = x.get_low();  y1 = x.get_high();
        return *this;
    };
    // Member function to load from array (unaligned)
    Vec32uc & load(void const * p) {
        y0 = _mm_loadu_si128((__m128i const*)p);
        y1 = _mm_loadu_si128((__m128i const*)p + 1);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec32uc & load_a(void const * p) {
        y0 = _mm_load_si128((__m128i const*)p);
        y1 = _mm_load_si128((__m128i const*)p + 1);
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
        return y0;
    }
    Vec16uc get_high() const {
        return y1;
    }
};

// Define operators for this class

// vector operator + : add
static inline Vec32uc operator + (Vec32uc const & a, Vec32uc const & b) {
    return Vec32uc(a.get_low() + b.get_low(), a.get_high() + b.get_high()); 
}

// vector operator - : subtract
static inline Vec32uc operator - (Vec32uc const & a, Vec32uc const & b) {
    return Vec32uc(a.get_low() - b.get_low(), a.get_high() - b.get_high()); 
}

// vector operator * : multiply
static inline Vec32uc operator * (Vec32uc const & a, Vec32uc const & b) {
    return Vec32uc(a.get_low() * b.get_low(), a.get_high() * b.get_high()); 
}

// vector operator / : divide
static inline Vec32uc operator / (Vec32uc const & a, Divisor_us const & d) {
    return Vec32uc(a.get_low() / d, a.get_high() / d);
}

// vector operator /= : divide
static inline Vec32uc & operator /= (Vec32uc & a, Divisor_us const & d) {
    a = a / d;
    return a;
}

// vector operator << : shift left all elements
static inline Vec32uc operator << (Vec32uc const & a, uint32_t b) {
    return Vec32uc(a.get_low() << b, a.get_high() << b); 
}

// vector operator << : shift left all elements
static inline Vec32uc operator << (Vec32uc const & a, int32_t b) {
    return a << (uint32_t)b;
}

// vector operator >> : shift right logical all elements
static inline Vec32uc operator >> (Vec32uc const & a, uint32_t b) {
    return Vec32uc(a.get_low() >> b, a.get_high() >> b); 
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
    return Vec32c(a.get_low() >= b.get_low(), a.get_high() >= b.get_high()); 
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec32cb operator <= (Vec32uc const & a, Vec32uc const & b) {
    return b >= a;
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec32cb operator > (Vec32uc const & a, Vec32uc const & b) {
    return Vec32c(a.get_low() > b.get_low(), a.get_high() > b.get_high()); 
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec32cb operator < (Vec32uc const & a, Vec32uc const & b) {
    return b > a;
}

// vector operator & : bitwise and
static inline Vec32uc operator & (Vec32uc const & a, Vec32uc const & b) {
    return Vec32uc(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec32uc operator && (Vec32uc const & a, Vec32uc const & b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec32uc operator | (Vec32uc const & a, Vec32uc const & b) {
    return Vec32uc(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}
static inline Vec32uc operator || (Vec32uc const & a, Vec32uc const & b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec32uc operator ^ (Vec32uc const & a, Vec32uc const & b) {
    return Vec32uc(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ~ : bitwise not
static inline Vec32uc operator ~ (Vec32uc const & a) {
    return Vec32uc(~a.get_low(), ~a.get_high());
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
    return horizontal_add(a.get_low() + a.get_high());
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Each element is zero-extended before addition to avoid overflow
static inline uint32_t horizontal_add_x (Vec32uc const & a) {
    return horizontal_add_x(a.get_low()) + horizontal_add_x(a.get_high());
}

// function add_saturated: add element by element, unsigned with saturation
static inline Vec32uc add_saturated(Vec32uc const & a, Vec32uc const & b) {
    return Vec32uc(add_saturated(a.get_low(),b.get_low()), add_saturated(a.get_high(),b.get_high())); 
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec32uc sub_saturated(Vec32uc const & a, Vec32uc const & b) {
    return Vec32uc(sub_saturated(a.get_low(),b.get_low()), sub_saturated(a.get_high(),b.get_high())); 
}

// function max: a > b ? a : b
static inline Vec32uc max(Vec32uc const & a, Vec32uc const & b) {
    return Vec32uc(max(a.get_low(),b.get_low()), max(a.get_high(),b.get_high())); 
}

// function min: a < b ? a : b
static inline Vec32uc min(Vec32uc const & a, Vec32uc const & b) {
    return Vec32uc(min(a.get_low(),b.get_low()), min(a.get_high(),b.get_high())); 
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
        y1 = y0 = _mm_set1_epi16((int16_t)i);
    };
    // Constructor to build from all elements:
    Vec16s(int16_t i0, int16_t i1, int16_t i2,  int16_t i3,  int16_t i4,  int16_t i5,  int16_t i6,  int16_t i7,
           int16_t i8, int16_t i9, int16_t i10, int16_t i11, int16_t i12, int16_t i13, int16_t i14, int16_t i15) {
        y0 = _mm_setr_epi16(i0, i1, i2,  i3,  i4,  i5,  i6,  i7);
        y1 = _mm_setr_epi16(i8, i9, i10, i11, i12, i13, i14, i15);
    };
    // Constructor to build from two Vec8s:
    Vec16s(Vec8s const & a0, Vec8s const & a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256ie
    Vec16s(Vec256ie const & x) {
        y0 = x.get_low();  y1 = x.get_high();
    };
    // Assignment operator to convert from type Vec256ie
    Vec16s & operator = (Vec256ie const & x) {
        y0 = x.get_low();  y1 = x.get_high();
        return *this;
    };
    // Member function to load from array (unaligned)
    Vec16s & load(void const * p) {
        y0 = _mm_loadu_si128((__m128i const*)p);
        y1 = _mm_loadu_si128((__m128i const*)p + 1);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec16s & load_a(void const * p) {
        y0 = _mm_load_si128((__m128i const*)p);
        y1 = _mm_load_si128((__m128i const*)p + 1);
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
        if (index < 8) {
            y0 = Vec8s(y0).insert(index, value);
        }
        else {
            y1 = Vec8s(y1).insert(index-8, value);
        }
        return *this;
    };
    // Member function extract a single element from vector
    int16_t extract(uint32_t index) const {
        if (index < 8) {
            return Vec8s(y0).extract(index);
        }
        else {
            return Vec8s(y1).extract(index-8);
        }
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int16_t operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec8s:
    Vec8s get_low() const {
        return y0;
    }
    Vec8s get_high() const {
        return y1;
    }
    static int size () {
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
    // Constructor to convert from type Vec256ie
    Vec16sb(Vec256ie const & x) {
        y0 = x.get_low();  y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256ie
    Vec16sb & operator = (Vec256ie const & x) {
        y0 = x.get_low();  y1 = x.get_high();
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
    // Member functions to split into two Vec8s:
    Vec8sb get_low() const {
        return y0;
    }
    Vec8sb get_high() const {
        return y1;
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
    return Vec16s(a.get_low() + b.get_low(), a.get_high() + b.get_high());
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
    return Vec16s(a.get_low() - b.get_low(), a.get_high() - b.get_high());
}

// vector operator - : unary minus
static inline Vec16s operator - (Vec16s const & a) {
    return Vec16s(-a.get_low(), -a.get_high());
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
    return Vec16s(a.get_low() * b.get_low(), a.get_high() * b.get_high());
}

// vector operator *= : multiply
static inline Vec16s & operator *= (Vec16s & a, Vec16s const & b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer
static inline Vec16s operator / (Vec16s const & a, Divisor_s const & d) {
    return Vec16s(a.get_low() / d, a.get_high() / d);
}

// vector operator /= : divide
static inline Vec16s & operator /= (Vec16s & a, Divisor_s const & d) {
    a = a / d;
    return a;
}

// vector operator << : shift left
static inline Vec16s operator << (Vec16s const & a, int b) {
    return Vec16s(a.get_low() << b, a.get_high() << b);
}

// vector operator <<= : shift left
static inline Vec16s & operator <<= (Vec16s & a, int b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec16s operator >> (Vec16s const & a, int b) {
    return Vec16s(a.get_low() >> b, a.get_high() >> b);
}

// vector operator >>= : shift right arithmetic
static inline Vec16s & operator >>= (Vec16s & a, int b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec16sb operator == (Vec16s const & a, Vec16s const & b) {
    return Vec16s(a.get_low() == b.get_low(), a.get_high() == b.get_high());
}

// vector operator != : returns true for elements for which a != b
static inline Vec16sb operator != (Vec16s const & a, Vec16s const & b) {
    return Vec16s(a.get_low() != b.get_low(), a.get_high() != b.get_high());
}

// vector operator > : returns true for elements for which a > b
static inline Vec16sb operator > (Vec16s const & a, Vec16s const & b) {
    return Vec16s(a.get_low() > b.get_low(), a.get_high() > b.get_high());
}

// vector operator < : returns true for elements for which a < b
static inline Vec16sb operator < (Vec16s const & a, Vec16s const & b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec16sb operator >= (Vec16s const & a, Vec16s const & b) {
    return Vec16s(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec16sb operator <= (Vec16s const & a, Vec16s const & b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec16s operator & (Vec16s const & a, Vec16s const & b) {
    return Vec16s(a.get_low() & b.get_low(), a.get_high() & b.get_high());
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
    return Vec16s(a.get_low() | b.get_low(), a.get_high() | b.get_high());
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
    return Vec16s(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}
// vector operator ^= : bitwise xor
static inline Vec16s & operator ^= (Vec16s & a, Vec16s const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec16s operator ~ (Vec16s const & a) {
    return Vec16s(~Vec256b(a));
}

// vector operator ! : logical not, returns true for elements == 0
static inline Vec16sb operator ! (Vec16s const & a) {
    return Vec16s(!a.get_low(), !a.get_high());
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
    return horizontal_add(a.get_low() + a.get_high());
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Elements are sign extended before adding to avoid overflow
static inline int32_t horizontal_add_x (Vec16s const & a) {
    return horizontal_add_x(a.get_low()) + horizontal_add_x(a.get_high());
}

// function add_saturated: add element by element, signed with saturation
static inline Vec16s add_saturated(Vec16s const & a, Vec16s const & b) {
    return Vec16s(add_saturated(a.get_low(),b.get_low()), add_saturated(a.get_high(),b.get_high()));
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec16s sub_saturated(Vec16s const & a, Vec16s const & b) {
    return Vec16s(sub_saturated(a.get_low(),b.get_low()), sub_saturated(a.get_high(),b.get_high()));
}

// function max: a > b ? a : b
static inline Vec16s max(Vec16s const & a, Vec16s const & b) {
    return Vec16s(max(a.get_low(),b.get_low()), max(a.get_high(),b.get_high()));
}

// function min: a < b ? a : b
static inline Vec16s min(Vec16s const & a, Vec16s const & b) {
    return Vec16s(min(a.get_low(),b.get_low()), min(a.get_high(),b.get_high()));
}

// function abs: a >= 0 ? a : -a
static inline Vec16s abs(Vec16s const & a) {
    return Vec16s(abs(a.get_low()), abs(a.get_high()));
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec16s abs_saturated(Vec16s const & a) {
    return Vec16s(abs_saturated(a.get_low()), abs_saturated(a.get_high()));
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec16s rotate_left(Vec16s const & a, int b) {
    return Vec16s(rotate_left(a.get_low(),b), rotate_left(a.get_high(),b));
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
        y1 = y0 = _mm_set1_epi16((int16_t)i);
    };
    // Constructor to build from all elements:
    Vec16us(uint16_t i0, uint16_t i1, uint16_t i2,  uint16_t i3,  uint16_t i4,  uint16_t i5,  uint16_t i6,  uint16_t i7,
            uint16_t i8, uint16_t i9, uint16_t i10, uint16_t i11, uint16_t i12, uint16_t i13, uint16_t i14, uint16_t i15) {
        y0 = _mm_setr_epi16(i0, i1, i2,  i3,  i4,  i5,  i6,  i7);
        y1 = _mm_setr_epi16(i8, i9, i10, i11, i12, i13, i14, i15 );
    };
    // Constructor to build from two Vec8us:
    Vec16us(Vec8us const & a0, Vec8us const & a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256ie
    Vec16us(Vec256ie const & x) {
        y0 = x.get_low();  y1 = x.get_high();
    };
    // Assignment operator to convert from type Vec256ie
    Vec16us & operator = (Vec256ie const & x) {
        y0 = x.get_low();  y1 = x.get_high();
        return *this;
    };
    // Member function to load from array (unaligned)
    Vec16us & load(void const * p) {
        y0 = _mm_loadu_si128((__m128i const*)p);
        y1 = _mm_loadu_si128((__m128i const*)p + 1);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec16us & load_a(void const * p) {
        y0 = _mm_load_si128((__m128i const*)p);
        y1 = _mm_load_si128((__m128i const*)p + 1);
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
        return y0;
    }
    Vec8us get_high() const {
        return y1;
    }
};

// Define operators for this class

// vector operator + : add
static inline Vec16us operator + (Vec16us const & a, Vec16us const & b) {
    return Vec16us(a.get_low() + b.get_low(), a.get_high() + b.get_high());
}

// vector operator - : subtract
static inline Vec16us operator - (Vec16us const & a, Vec16us const & b) {
    return Vec16us(a.get_low() - b.get_low(), a.get_high() - b.get_high());
}

// vector operator * : multiply
static inline Vec16us operator * (Vec16us const & a, Vec16us const & b) {
    return Vec16us(a.get_low() * b.get_low(), a.get_high() * b.get_high());
}

// vector operator / : divide
static inline Vec16us operator / (Vec16us const & a, Divisor_us const & d) {
    return Vec16us(a.get_low() / d, a.get_high() / d);
}

// vector operator /= : divide
static inline Vec16us & operator /= (Vec16us & a, Divisor_us const & d) {
    a = a / d;
    return a;
}

// vector operator >> : shift right logical all elements
static inline Vec16us operator >> (Vec16us const & a, uint32_t b) {
    return Vec16us(a.get_low() >> b, a.get_high() >> b);
}

// vector operator >> : shift right logical all elements
static inline Vec16us operator >> (Vec16us const & a, int b) {
    return a >> (uint32_t)b;
}

// vector operator >>= : shift right artihmetic
static inline Vec16us & operator >>= (Vec16us & a, uint32_t b) {
    a = a >> b;
    return a;
}

// vector operator << : shift left all elements
static inline Vec16us operator << (Vec16us const & a, uint32_t b) {
    return Vec16us(a.get_low() << b, a.get_high() << b);
}

// vector operator << : shift left all elements
static inline Vec16us operator << (Vec16us const & a, int32_t b) {
    return a << (uint32_t)b;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec16sb operator >= (Vec16us const & a, Vec16us const & b) {
    return Vec16s(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec16sb operator <= (Vec16us const & a, Vec16us const & b) {
    return b >= a;
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec16sb operator > (Vec16us const & a, Vec16us const & b) {
    return Vec16s(a.get_low() > b.get_low(), a.get_high() > b.get_high());
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec16sb operator < (Vec16us const & a, Vec16us const & b) {
    return b > a;
}

// vector operator & : bitwise and
static inline Vec16us operator & (Vec16us const & a, Vec16us const & b) {
    return Vec16us(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec16us operator && (Vec16us const & a, Vec16us const & b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec16us operator | (Vec16us const & a, Vec16us const & b) {
    return Vec16us(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}
static inline Vec16us operator || (Vec16us const & a, Vec16us const & b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec16us operator ^ (Vec16us const & a, Vec16us const & b) {
    return Vec16us(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ~ : bitwise not
static inline Vec16us operator ~ (Vec16us const & a) {
    return Vec16us(~ Vec256b(a));
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
    return horizontal_add(a.get_low() + a.get_high());
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Each element is zero-extended before addition to avoid overflow
static inline uint32_t horizontal_add_x (Vec16us const & a) {
    return horizontal_add_x(a.get_low()) + horizontal_add_x(a.get_high());
}

// function add_saturated: add element by element, unsigned with saturation
static inline Vec16us add_saturated(Vec16us const & a, Vec16us const & b) {
    return Vec16us(add_saturated(a.get_low(),b.get_low()), add_saturated(a.get_high(),b.get_high()));
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec16us sub_saturated(Vec16us const & a, Vec16us const & b) {
    return Vec16us(sub_saturated(a.get_low(),b.get_low()), sub_saturated(a.get_high(),b.get_high()));
}

// function max: a > b ? a : b
static inline Vec16us max(Vec16us const & a, Vec16us const & b) {
    return Vec16us(max(a.get_low(),b.get_low()), max(a.get_high(),b.get_high()));
}

// function min: a < b ? a : b
static inline Vec16us min(Vec16us const & a, Vec16us const & b) {
    return Vec16us(min(a.get_low(),b.get_low()), min(a.get_high(),b.get_high()));
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
        y1 = y0 = _mm_set1_epi32(i);
    }
    // Constructor to build from all elements:
    Vec8i(int32_t i0, int32_t i1, int32_t i2, int32_t i3, int32_t i4, int32_t i5, int32_t i6, int32_t i7) {
        y0 = _mm_setr_epi32(i0, i1, i2, i3);
        y1 = _mm_setr_epi32(i4, i5, i6, i7);
    }
    // Constructor to build from two Vec4i:
    Vec8i(Vec4i const & a0, Vec4i const & a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256ie
    Vec8i(Vec256ie const & x) {
        y0 = x.get_low();  y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256ie
    Vec8i & operator = (Vec256ie const & x) {
        y0 = x.get_low();  y1 = x.get_high();
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec8i & load(void const * p) {
        y0 = _mm_loadu_si128((__m128i const*)p);
        y1 = _mm_loadu_si128((__m128i const*)p + 1);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec8i & load_a(void const * p) {
        y0 = _mm_load_si128((__m128i const*)p);
        y1 = _mm_load_si128((__m128i const*)p + 1);
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
        if (index < 4) {
            y0 = Vec4i(y0).insert(index, value);
        }
        else {
            y1 = Vec4i(y1).insert(index-4, value);
        }
        return *this;
    }
    // Member function extract a single element from vector
    // Note: This function is inefficient. Use store function if extracting more than one element
    int32_t extract(uint32_t index) const {
        if (index < 4) {
            return Vec4i(y0).extract(index);
        }
        else {
            return Vec4i(y1).extract(index-4);
        }
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int32_t operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4i:
    Vec4i get_low() const {
        return y0;
    }
    Vec4i get_high() const {
        return y1;
    }
    static int size () {
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
    // Constructor to convert from type Vec256ie
    Vec8ib(Vec256ie const & x) {
        y0 = x.get_low();  y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256ie
    Vec8ib & operator = (Vec256ie const & x) {
        y0 = x.get_low();  y1 = x.get_high();
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
    // Member functions to split into two Vec4i:
    Vec4ib get_low() const {
        return y0;
    }
    Vec4ib get_high() const {
        return y1;
    }
    Vec8ib & insert (int index, bool a) {
        Vec8i::insert(index, -(int)a);
        return *this;
    };
    // Member function extract a single element from vector
    // Note: This function is inefficient. Use store function if extracting more than one element
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
    return Vec8i(a.get_low() + b.get_low(), a.get_high() + b.get_high());
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
    return Vec8i(a.get_low() - b.get_low(), a.get_high() - b.get_high());
}

// vector operator - : unary minus
static inline Vec8i operator - (Vec8i const & a) {
    return Vec8i(-a.get_low(), -a.get_high());
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
    return Vec8i(a.get_low() * b.get_low(), a.get_high() * b.get_high());
}

// vector operator *= : multiply
static inline Vec8i & operator *= (Vec8i & a, Vec8i const & b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer
static inline Vec8i operator / (Vec8i const & a, Divisor_i const & d) {
    return Vec8i(a.get_low() / d, a.get_high() / d);
}

// vector operator /= : divide
static inline Vec8i & operator /= (Vec8i & a, Divisor_i const & d) {
    a = a / d;
    return a;
}

// vector operator << : shift left
static inline Vec8i operator << (Vec8i const & a, int32_t b) {
    return Vec8i(a.get_low() << b, a.get_high() << b);
}

// vector operator <<= : shift left
static inline Vec8i & operator <<= (Vec8i & a, int32_t b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec8i operator >> (Vec8i const & a, int32_t b) {
    return Vec8i(a.get_low() >> b, a.get_high() >> b);
}

// vector operator >>= : shift right arithmetic
static inline Vec8i & operator >>= (Vec8i & a, int32_t b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec8ib operator == (Vec8i const & a, Vec8i const & b) {
    return Vec8i(a.get_low() == b.get_low(), a.get_high() == b.get_high());
}

// vector operator != : returns true for elements for which a != b
static inline Vec8ib operator != (Vec8i const & a, Vec8i const & b) {
    return Vec8i(a.get_low() != b.get_low(), a.get_high() != b.get_high());
}
  
// vector operator > : returns true for elements for which a > b
static inline Vec8ib operator > (Vec8i const & a, Vec8i const & b) {
    return Vec8i(a.get_low() > b.get_low(), a.get_high() > b.get_high());
}

// vector operator < : returns true for elements for which a < b
static inline Vec8ib operator < (Vec8i const & a, Vec8i const & b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec8ib operator >= (Vec8i const & a, Vec8i const & b) {
    return Vec8i(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec8ib operator <= (Vec8i const & a, Vec8i const & b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec8i operator & (Vec8i const & a, Vec8i const & b) {
    return Vec8i(a.get_low() & b.get_low(), a.get_high() & b.get_high());
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
    return Vec8i(a.get_low() | b.get_low(), a.get_high() | b.get_high());
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
    return Vec8i(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}
// vector operator ^= : bitwise xor
static inline Vec8i & operator ^= (Vec8i & a, Vec8i const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec8i operator ~ (Vec8i const & a) {
    return Vec8i(~a.get_low(), ~a.get_high());
}

// vector operator ! : returns true for elements == 0
static inline Vec8ib operator ! (Vec8i const & a) {
    return Vec8i(!a.get_low(), !a.get_high());
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
    return horizontal_add(a.get_low() + a.get_high());
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Elements are sign extended before adding to avoid overflow
static inline int64_t horizontal_add_x (Vec8i const & a) {
    return horizontal_add_x(a.get_low()) + horizontal_add_x(a.get_high());
}

// function add_saturated: add element by element, signed with saturation
static inline Vec8i add_saturated(Vec8i const & a, Vec8i const & b) {
    return Vec8i(add_saturated(a.get_low(),b.get_low()), add_saturated(a.get_high(),b.get_high()));
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec8i sub_saturated(Vec8i const & a, Vec8i const & b) {
    return Vec8i(sub_saturated(a.get_low(),b.get_low()), sub_saturated(a.get_high(),b.get_high()));
}

// function max: a > b ? a : b
static inline Vec8i max(Vec8i const & a, Vec8i const & b) {
    return Vec8i(max(a.get_low(),b.get_low()), max(a.get_high(),b.get_high()));
}

// function min: a < b ? a : b
static inline Vec8i min(Vec8i const & a, Vec8i const & b) {
    return Vec8i(min(a.get_low(),b.get_low()), min(a.get_high(),b.get_high()));
}

// function abs: a >= 0 ? a : -a
static inline Vec8i abs(Vec8i const & a) {
    return Vec8i(abs(a.get_low()), abs(a.get_high()));
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec8i abs_saturated(Vec8i const & a) {
    return Vec8i(abs_saturated(a.get_low()), abs_saturated(a.get_high()));
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec8i rotate_left(Vec8i const & a, int b) {
    return Vec8i(rotate_left(a.get_low(),b), rotate_left(a.get_high(),b));
}


/*****************************************************************************
*
*          Vector of 4 32-bit unsigned integers
*
*****************************************************************************/

class Vec8ui : public Vec8i {
public:
    // Default constructor:
    Vec8ui() {
    };
    // Constructor to broadcast the same value into all elements:
    Vec8ui(uint32_t i) {
        y1 = y0 = _mm_set1_epi32(i);
    };
    // Constructor to build from all elements:
    Vec8ui(uint32_t i0, uint32_t i1, uint32_t i2, uint32_t i3, uint32_t i4, uint32_t i5, uint32_t i6, uint32_t i7) {
        y0 = _mm_setr_epi32(i0, i1, i2, i3);
        y1 = _mm_setr_epi32(i4, i5, i6, i7);
    };
    // Constructor to build from two Vec4ui:
    Vec8ui(Vec4ui const & a0, Vec4ui const & a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256ie
    Vec8ui(Vec256ie const & x) {
        y0 = x.get_low();  y1 = x.get_high();
    };
    // Assignment operator to convert from type Vec256ie
    Vec8ui & operator = (Vec256ie const & x) {
        y0 = x.get_low();  y1 = x.get_high();
        return *this;
    };
    // Member function to load from array (unaligned)
    Vec8ui & load(void const * p) {
        y0 = _mm_loadu_si128((__m128i const*)p);
        y1 = _mm_loadu_si128((__m128i const*)p + 1);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec8ui & load_a(void const * p) {
        y0 = _mm_load_si128((__m128i const*)p);
        y1 = _mm_load_si128((__m128i const*)p + 1);
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
        return y0;
    }
    Vec4ui get_high() const {
        return y1;
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

// vector operator / : divide all elements by same integer
static inline Vec8ui operator / (Vec8ui const & a, Divisor_ui const & d) {
    return Vec8ui(a.get_low() / d, a.get_high() / d);
}

// vector operator /= : divide
static inline Vec8ui & operator /= (Vec8ui & a, Divisor_ui const & d) {
    a = a / d;
    return a;
}

// vector operator >> : shift right logical all elements
static inline Vec8ui operator >> (Vec8ui const & a, uint32_t b) {
    return Vec8ui(a.get_low() >> b, a.get_high() >> b);
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

// vector operator >>= : shift right logical
static inline Vec8ui & operator >>= (Vec8ui & a, int32_t b) {
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
    return Vec8i(a.get_low() > b.get_low(), a.get_high() > b.get_high());
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec8ib operator < (Vec8ui const & a, Vec8ui const & b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec8ib operator >= (Vec8ui const & a, Vec8ui const & b) {
    return Vec8i(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec8ib operator <= (Vec8ui const & a, Vec8ui const & b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec8ui operator & (Vec8ui const & a, Vec8ui const & b) {
    return Vec8ui(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec8ui operator && (Vec8ui const & a, Vec8ui const & b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec8ui operator | (Vec8ui const & a, Vec8ui const & b) {
    return Vec8ui(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}
static inline Vec8ui operator || (Vec8ui const & a, Vec8ui const & b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec8ui operator ^ (Vec8ui const & a, Vec8ui const & b) {
    return Vec8ui(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ~ : bitwise not
static inline Vec8ui operator ~ (Vec8ui const & a) {
    return Vec8ui(~a.get_low(), ~a.get_high());
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
static inline uint64_t horizontal_add_x (Vec8ui const & a) {
    return horizontal_add_x(a.get_low()) + horizontal_add_x(a.get_high());
}

// function add_saturated: add element by element, unsigned with saturation
static inline Vec8ui add_saturated(Vec8ui const & a, Vec8ui const & b) {
    return Vec8ui(add_saturated(a.get_low(),b.get_low()), add_saturated(a.get_high(),b.get_high()));
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec8ui sub_saturated(Vec8ui const & a, Vec8ui const & b) {
    return Vec8ui(sub_saturated(a.get_low(),b.get_low()), sub_saturated(a.get_high(),b.get_high()));
}

// function max: a > b ? a : b
static inline Vec8ui max(Vec8ui const & a, Vec8ui const & b) {
    return Vec8ui(max(a.get_low(),b.get_low()), max(a.get_high(),b.get_high()));
}

// function min: a < b ? a : b
static inline Vec8ui min(Vec8ui const & a, Vec8ui const & b) {
    return Vec8ui(min(a.get_low(),b.get_low()), min(a.get_high(),b.get_high()));
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
        y0 = y1 = Vec2q(i);
    }
    // Constructor to build from all elements:
    Vec4q(int64_t i0, int64_t i1, int64_t i2, int64_t i3) {
        y0 = Vec2q(i0,i1);
        y1 = Vec2q(i2,i3);
    }
    // Constructor to build from two Vec2q:
    Vec4q(Vec2q const & a0, Vec2q const & a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256ie
    Vec4q(Vec256ie const & x) {
        y0 = x.get_low();  y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256ie
    Vec4q & operator = (Vec256ie const & x) {
        y0 = x.get_low();  y1 = x.get_high();
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec4q & load(void const * p) {
        y0 = _mm_loadu_si128((__m128i const*)p);
        y1 = _mm_loadu_si128((__m128i const*)p + 1);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec4q & load_a(void const * p) {
        y0 = _mm_load_si128((__m128i const*)p);
        y1 = _mm_load_si128((__m128i const*)p + 1);
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
        if (index < 2) {
            y0 = Vec2q(y0).insert(index, value);
        }
        else {
            y1 = Vec2q(y1).insert(index-2, value);
        }
        return *this;
    }
    // Member function extract a single element from vector
    // Note: This function is inefficient. Use store function if extracting more than one element
    int64_t extract(uint32_t index) const {
        if (index < 2) {
            return Vec2q(y0).extract(index);
        }
        else {
            return Vec2q(y1).extract(index-2);
        }
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int64_t operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec2q:
    Vec2q get_low() const {
        return y0;
    }
    Vec2q get_high() const {
        return y1;
    }
    static int size () {
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
    // Constructor to convert from type Vec256ie
    Vec4qb(Vec256ie const & x) {
        y0 = x.get_low();  y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256ie
    Vec4qb & operator = (Vec256ie const & x) {
        y0 = x.get_low();  y1 = x.get_high();
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
        return y0;
    }
    Vec2qb get_high() const {
        return y1;
    }
    Vec4qb & insert (int index, bool a) {
        Vec4q::insert(index, -(int64_t)a);
        return *this;
    };    
    // Member function extract a single element from vector
    // Note: This function is inefficient. Use store function if extracting more than one element
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
    return Vec4q(a.get_low() + b.get_low(), a.get_high() + b.get_high());
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
    return Vec4q(a.get_low() - b.get_low(), a.get_high() - b.get_high());
}

// vector operator - : unary minus
static inline Vec4q operator - (Vec4q const & a) {
    return Vec4q(-a.get_low(), -a.get_high());
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
    return Vec4q(a.get_low() * b.get_low(), a.get_high() * b.get_high());
}

// vector operator *= : multiply
static inline Vec4q & operator *= (Vec4q & a, Vec4q const & b) {
    a = a * b;
    return a;
}

// vector operator << : shift left
static inline Vec4q operator << (Vec4q const & a, int32_t b) {
    return Vec4q(a.get_low() << b, a.get_high() << b);
}

// vector operator <<= : shift left
static inline Vec4q & operator <<= (Vec4q & a, int32_t b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec4q operator >> (Vec4q const & a, int32_t b) {
    return Vec4q(a.get_low() >> b, a.get_high() >> b);
}

// vector operator >>= : shift right arithmetic
static inline Vec4q & operator >>= (Vec4q & a, int32_t b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec4qb operator == (Vec4q const & a, Vec4q const & b) {
    return Vec4q(a.get_low() == b.get_low(), a.get_high() == b.get_high());
}

// vector operator != : returns true for elements for which a != b
static inline Vec4qb operator != (Vec4q const & a, Vec4q const & b) {
    return Vec4q(a.get_low() != b.get_low(), a.get_high() != b.get_high());
}
  
// vector operator < : returns true for elements for which a < b
static inline Vec4qb operator < (Vec4q const & a, Vec4q const & b) {
    return Vec4q(a.get_low() < b.get_low(), a.get_high() < b.get_high());
}

// vector operator > : returns true for elements for which a > b
static inline Vec4qb operator > (Vec4q const & a, Vec4q const & b) {
    return b < a;
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec4qb operator >= (Vec4q const & a, Vec4q const & b) {
    return Vec4q(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec4qb operator <= (Vec4q const & a, Vec4q const & b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec4q operator & (Vec4q const & a, Vec4q const & b) {
    return Vec4q(a.get_low() & b.get_low(), a.get_high() & b.get_high());
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
    return Vec4q(a.get_low() | b.get_low(), a.get_high() | b.get_high());
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
    return Vec4q(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}
// vector operator ^= : bitwise xor
static inline Vec4q & operator ^= (Vec4q & a, Vec4q const & b) {
    a = a ^ b;
    return a;
}


// vector operator ~ : bitwise not
static inline Vec4q operator ~ (Vec4q const & a) {
    return Vec4q(~a.get_low(), ~a.get_high());
}

// vector operator ! : logical not, returns true for elements == 0
static inline Vec4qb operator ! (Vec4q const & a) {
    return Vec4q(!a.get_low(), !a.get_high());
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
    return horizontal_add(a.get_low() + a.get_high());
}

// function max: a > b ? a : b
static inline Vec4q max(Vec4q const & a, Vec4q const & b) {
    return Vec4q(max(a.get_low(),b.get_low()), max(a.get_high(),b.get_high()));
}

// function min: a < b ? a : b
static inline Vec4q min(Vec4q const & a, Vec4q const & b) {
    return Vec4q(min(a.get_low(),b.get_low()), min(a.get_high(),b.get_high()));
}

// function abs: a >= 0 ? a : -a
static inline Vec4q abs(Vec4q const & a) {
    return Vec4q(abs(a.get_low()), abs(a.get_high()));
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec4q abs_saturated(Vec4q const & a) {
    return Vec4q(abs_saturated(a.get_low()), abs_saturated(a.get_high()));
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec4q rotate_left(Vec4q const & a, int b) {
    return Vec4q(rotate_left(a.get_low(),b), rotate_left(a.get_high(),b));
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
        y1 = y0 = Vec2q(i);
    };
    // Constructor to build from all elements:
    Vec4uq(uint64_t i0, uint64_t i1, uint64_t i2, uint64_t i3) {
        y0 = Vec2q(i0, i1);
        y1 = Vec2q(i2, i3);
    };
    // Constructor to build from two Vec2uq:
    Vec4uq(Vec2uq const & a0, Vec2uq const & a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256ie
    Vec4uq(Vec256ie const & x) {
        y0 = x.get_low();  y1 = x.get_high();
    };
    // Assignment operator to convert from type Vec256ie
    Vec4uq & operator = (Vec256ie const & x) {
        y0 = x.get_low();  y1 = x.get_high();
        return *this;
    };
    // Member function to load from array (unaligned)
    Vec4uq & load(void const * p) {
        y0 = _mm_loadu_si128((__m128i const*)p);
        y1 = _mm_loadu_si128((__m128i const*)p + 1);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec4uq & load_a(void const * p) {
        y0 = _mm_load_si128((__m128i const*)p);
        y1 = _mm_load_si128((__m128i const*)p + 1);
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
        return y0;
    }
    Vec2uq get_high() const {
        return y1;
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
    return Vec4uq(a.get_low() >> b, a.get_high() >> b);
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
    return Vec4q(a.get_low() > b.get_low(), a.get_high() > b.get_high());
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec4qb operator < (Vec4uq const & a, Vec4uq const & b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec4qb operator >= (Vec4uq const & a, Vec4uq const & b) {
    return Vec4q(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec4qb operator <= (Vec4uq const & a, Vec4uq const & b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec4uq operator & (Vec4uq const & a, Vec4uq const & b) {
    return Vec4uq(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec4uq operator && (Vec4uq const & a, Vec4uq const & b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec4uq operator | (Vec4uq const & a, Vec4uq const & b) {
    return Vec4q(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}
static inline Vec4uq operator || (Vec4uq const & a, Vec4uq const & b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec4uq operator ^ (Vec4uq const & a, Vec4uq const & b) {
    return Vec4uq(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ~ : bitwise not
static inline Vec4uq operator ~ (Vec4uq const & a) {
    return Vec4uq(~a.get_low(), ~a.get_high());
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

// function max: a > b ? a : b
static inline Vec4uq max(Vec4uq const & a, Vec4uq const & b) {
    return Vec4uq(max(a.get_low(),b.get_low()), max(a.get_high(),b.get_high()));
}

// function min: a < b ? a : b
static inline Vec4uq min(Vec4uq const & a, Vec4uq const & b) {
    return Vec4uq(min(a.get_low(),b.get_low()), min(a.get_high(),b.get_high()));
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


// Shuffle vector of 4 64-bit integers.
// Index -1 gives 0, index -256 means don't care.
template <int i0, int i1, int i2, int i3 >
static inline Vec4q permute4q(Vec4q const & a) {
    return Vec4q(blend2q<i0,i1> (a.get_low(), a.get_high()),
                 blend2q<i2,i3> (a.get_low(), a.get_high()));
}

template <int i0, int i1, int i2, int i3>
static inline Vec4uq permute4uq(Vec4uq const & a) {
    return Vec4uq (permute4q<i0,i1,i2,i3> (a));
}

// Shuffle vector of 8 32-bit integers.
// Index -1 gives 0, index -256 means don't care.
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7 >
static inline Vec8i permute8i(Vec8i const & a) {
    return Vec8i(blend4i<i0,i1,i2,i3> (a.get_low(), a.get_high()), 
                 blend4i<i4,i5,i6,i7> (a.get_low(), a.get_high()));
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7 >
static inline Vec8ui permute8ui(Vec8ui const & a) {
    return Vec8ui (permute8i<i0,i1,i2,i3,i4,i5,i6,i7> (a));
}

// Shuffle vector of 16 16-bit integers.
// Index -1 gives 0, index -256 means don't care.
template <int i0, int i1, int i2,  int i3,  int i4,  int i5,  int i6,  int i7,
          int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15 >
static inline Vec16s permute16s(Vec16s const & a) {
    return Vec16s(blend8s<i0,i1,i2 ,i3 ,i4 ,i5 ,i6 ,i7 > (a.get_low(), a.get_high()), 
                  blend8s<i8,i9,i10,i11,i12,i13,i14,i15> (a.get_low(), a.get_high()));
}

template <int i0, int i1, int i2,  int i3,  int i4,  int i5,  int i6,  int i7,
          int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15 >
static inline Vec16us permute16us(Vec16us const & a) {
    return Vec16us (permute16s<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15> (a));
}

template <int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15,
          int i16, int i17, int i18, int i19, int i20, int i21, int i22, int i23,
          int i24, int i25, int i26, int i27, int i28, int i29, int i30, int i31 >
static inline Vec32c permute32c(Vec32c const & a) {
    return Vec32c(blend16c<i0, i1, i2 ,i3 ,i4 ,i5 ,i6 ,i7, i8, i9, i10,i11,i12,i13,i14,i15> (a.get_low(), a.get_high()), 
                  blend16c<i16,i17,i18,i19,i20,i21,i22,i23,i24,i25,i26,i27,i28,i29,i30,i31> (a.get_low(), a.get_high()));
}

template <int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
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

// helper function used below
template <int n>
static inline Vec2q select4(Vec4q const & a, Vec4q const & b) {
    switch (n) {
    case 0:
        return a.get_low();
    case 1:
        return a.get_high();
    case 2:
        return b.get_low();
    case 3:
        return b.get_high();
    }
    return _mm_setzero_si128();
}

// blend vectors Vec4q
template <int i0, int i1, int i2, int i3>
static inline Vec4q blend4q(Vec4q const & a, Vec4q const & b) {
    const int j0 = i0 >= 0 ? i0/2 : i0;
    const int j1 = i1 >= 0 ? i1/2 : i1;
    const int j2 = i2 >= 0 ? i2/2 : i2;
    const int j3 = i3 >= 0 ? i3/2 : i3;    
    Vec2q x0, x1;

    if (j0 == j1 || i0 < 0 || i1 < 0) {  // both from same
        const int k0 = j0 >= 0 ? j0 : j1;
        x0 = permute2q<i0 & -7, i1 & -7> (select4<k0> (a,b));
    }
    else {
        x0 = blend2q<i0 & -7, (i1 & -7) | 2> (select4<j0>(a,b), select4<j1>(a,b));
    }
    if (j2 == j3 || i2 < 0 || i3 < 0) {  // both from same
        const int k1 = j2 >= 0 ? j2 : j3;
        x1 = permute2q<i2 & -7, i3 & -7> (select4<k1> (a,b));
    }
    else {
        x1 = blend2q<i2 & -7, (i3 & -7) | 2> (select4<j2>(a,b), select4<j3>(a,b));
    }
    return Vec4q(x0,x1);
}

template <int i0, int i1, int i2, int i3> 
static inline Vec4uq blend4uq(Vec4uq const & a, Vec4uq const & b) {
    return Vec4uq( blend4q<i0,i1,i2,i3> (a,b));
}

// helper function used below
template <int n>
static inline Vec4i select4(Vec8i const & a, Vec8i const & b) {
    switch (n) {
    case 0:
        return a.get_low();
    case 1:
        return a.get_high();
    case 2:
        return b.get_low();
    case 3:
        return b.get_high();
    }
    return _mm_setzero_si128();
}

// blend vectors Vec8i
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8i blend8i(Vec8i const & a, Vec8i const & b) {
    const int j0 = i0 >= 0 ? i0/4 : i0;
    const int j1 = i1 >= 0 ? i1/4 : i1;
    const int j2 = i2 >= 0 ? i2/4 : i2;
    const int j3 = i3 >= 0 ? i3/4 : i3;
    const int j4 = i4 >= 0 ? i4/4 : i4;
    const int j5 = i5 >= 0 ? i5/4 : i5;
    const int j6 = i6 >= 0 ? i6/4 : i6;
    const int j7 = i7 >= 0 ? i7/4 : i7;
    Vec4i x0, x1;

    const int r0 = j0 >= 0 ? j0 : j1 >= 0 ? j1 : j2 >= 0 ? j2 : j3;
    const int r1 = j4 >= 0 ? j4 : j5 >= 0 ? j5 : j6 >= 0 ? j6 : j7;
    const int s0 = (j1 >= 0 && j1 != r0) ? j1 : (j2 >= 0 && j2 != r0) ? j2 : j3;
    const int s1 = (j5 >= 0 && j5 != r1) ? j5 : (j6 >= 0 && j6 != r1) ? j6 : j7;

    // Combine all the indexes into a single bitfield, with 4 bits for each
    const int m1 = (i0&0xF) | (i1&0xF)<<4 | (i2&0xF)<<8 | (i3&0xF)<<12 | (i4&0xF)<<16 | (i5&0xF)<<20 | (i6&0xF)<<24 | (i7&0xF)<<28;

    // Mask to zero out negative indexes
    const int mz = (i0<0?0:0xF) | (i1<0?0:0xF)<<4 | (i2<0?0:0xF)<<8 | (i3<0?0:0xF)<<12 | (i4<0?0:0xF)<<16 | (i5<0?0:0xF)<<20 | (i6<0?0:0xF)<<24 | (i7<0?0:0xF)<<28;

    if (r0 < 0) {
        x0 = _mm_setzero_si128();
    }
    else if (((m1 ^ r0*0x4444) & 0xCCCC & mz) == 0) { 
        // i0 - i3 all from same source
        x0 = permute4i<i0 & -13, i1 & -13, i2 & -13, i3 & -13> (select4<r0> (a,b));
    }
    else if ((j2 < 0 || j2 == r0 || j2 == s0) && (j3 < 0 || j3 == r0 || j3 == s0)) { 
        // i0 - i3 all from two sources
        const int k0 =  i0 >= 0 ? i0 & 3 : i0;
        const int k1 = (i1 >= 0 ? i1 & 3 : i1) | (j1 == s0 ? 4 : 0);
        const int k2 = (i2 >= 0 ? i2 & 3 : i2) | (j2 == s0 ? 4 : 0);
        const int k3 = (i3 >= 0 ? i3 & 3 : i3) | (j3 == s0 ? 4 : 0);
        x0 = blend4i<k0,k1,k2,k3> (select4<r0>(a,b), select4<s0>(a,b));
    }
    else {
        // i0 - i3 from three or four different sources
        x0 = blend4i<0,1,6,7> (
             blend4i<i0 & -13, (i1 & -13) | 4, -0x100, -0x100> (select4<j0>(a,b), select4<j1>(a,b)),
             blend4i<-0x100, -0x100, i2 & -13, (i3 & -13) | 4> (select4<j2>(a,b), select4<j3>(a,b)));
    }

    if (r1 < 0) {
        x1 = _mm_setzero_si128();
    }
    else if (((m1 ^ uint32_t(r1)*0x44440000u) & 0xCCCC0000 & mz) == 0) { 
        // i4 - i7 all from same source
        x1 = permute4i<i4 & -13, i5 & -13, i6 & -13, i7 & -13> (select4<r1> (a,b));
    }
    else if ((j6 < 0 || j6 == r1 || j6 == s1) && (j7 < 0 || j7 == r1 || j7 == s1)) { 
        // i4 - i7 all from two sources
        const int k4 =  i4 >= 0 ? i4 & 3 : i4;
        const int k5 = (i5 >= 0 ? i5 & 3 : i5) | (j5 == s1 ? 4 : 0);
        const int k6 = (i6 >= 0 ? i6 & 3 : i6) | (j6 == s1 ? 4 : 0);
        const int k7 = (i7 >= 0 ? i7 & 3 : i7) | (j7 == s1 ? 4 : 0);
        x1 = blend4i<k4,k5,k6,k7> (select4<r1>(a,b), select4<s1>(a,b));
    }
    else {
        // i4 - i7 from three or four different sources
        x1 = blend4i<0,1,6,7> (
             blend4i<i4 & -13, (i5 & -13) | 4, -0x100, -0x100> (select4<j4>(a,b), select4<j5>(a,b)),
             blend4i<-0x100, -0x100, i6 & -13, (i7 & -13) | 4> (select4<j6>(a,b), select4<j7>(a,b)));
    }

    return Vec8i(x0,x1);
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7> 
static inline Vec8ui blend8ui(Vec8ui const & a, Vec8ui const & b) {
    return Vec8ui( blend8i<i0,i1,i2,i3,i4,i5,i6,i7> (a,b));
}

// helper function used below
template <int n>
static inline Vec8s select4(Vec16s const & a, Vec16s const & b) {
    switch (n) {
    case 0:
        return a.get_low();
    case 1:
        return a.get_high();
    case 2:
        return b.get_low();
    case 3:
        return b.get_high();
    }
    return _mm_setzero_si128();
}

template <int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15 > 
static inline Vec16s blend16s(Vec16s const & a, Vec16s const & b) {

    const int j0  = i0  >= 0 ? i0 /8 : i0;
    const int j1  = i1  >= 0 ? i1 /8 : i1;
    const int j2  = i2  >= 0 ? i2 /8 : i2;
    const int j3  = i3  >= 0 ? i3 /8 : i3;
    const int j4  = i4  >= 0 ? i4 /8 : i4;
    const int j5  = i5  >= 0 ? i5 /8 : i5;
    const int j6  = i6  >= 0 ? i6 /8 : i6;
    const int j7  = i7  >= 0 ? i7 /8 : i7;
    const int j8  = i8  >= 0 ? i8 /8 : i8;
    const int j9  = i9  >= 0 ? i9 /8 : i9;
    const int j10 = i10 >= 0 ? i10/8 : i10;
    const int j11 = i11 >= 0 ? i11/8 : i11;
    const int j12 = i12 >= 0 ? i12/8 : i12;
    const int j13 = i13 >= 0 ? i13/8 : i13;
    const int j14 = i14 >= 0 ? i14/8 : i14;
    const int j15 = i15 >= 0 ? i15/8 : i15;

    Vec8s x0, x1;

    const int r0 = j0 >= 0 ? j0 : j1 >= 0 ? j1 : j2  >= 0 ? j2  : j3  >= 0 ? j3  : j4  >= 0 ? j4  : j5  >= 0 ? j5  : j6  >= 0 ? j6  : j7;
    const int r1 = j8 >= 0 ? j8 : j9 >= 0 ? j9 : j10 >= 0 ? j10 : j11 >= 0 ? j11 : j12 >= 0 ? j12 : j13 >= 0 ? j13 : j14 >= 0 ? j14 : j15;
    const int s0 = (j1 >= 0 && j1 != r0) ? j1 : (j2 >= 0 && j2 != r0) ? j2  : (j3 >= 0 && j3 != r0) ? j3 : (j4 >= 0 && j4 != r0) ? j4 : (j5 >= 0 && j5 != r0) ? j5 : (j6 >= 0 && j6 != r0) ? j6 : j7;
    const int s1 = (j9 >= 0 && j9 != r1) ? j9 : (j10>= 0 && j10!= r1) ? j10 : (j11>= 0 && j11!= r1) ? j11: (j12>= 0 && j12!= r1) ? j12: (j13>= 0 && j13!= r1) ? j13: (j14>= 0 && j14!= r1) ? j14: j15;

    if (r0 < 0) {
        x0 = _mm_setzero_si128();
    }
    else if (r0 == s0) {
        // i0 - i7 all from same source
        x0 = permute8s<i0&-25, i1&-25, i2&-25, i3&-25, i4&-25, i5&-25, i6&-25, i7&-25> (select4<r0> (a,b));
    }
    else if ((j2<0||j2==r0||j2==s0) && (j3<0||j3==r0||j3 == s0) && (j4<0||j4==r0||j4 == s0) && (j5<0||j5==r0||j5 == s0) && (j6<0||j6==r0||j6 == s0) && (j7<0||j7==r0||j7 == s0)) { 
        // i0 - i7 all from two sources
        const int k0 =  i0 >= 0 ? i0 & 7 : i0;
        const int k1 = (i1 >= 0 ? i1 & 7 : i1) | (j1 == s0 ? 8 : 0);
        const int k2 = (i2 >= 0 ? i2 & 7 : i2) | (j2 == s0 ? 8 : 0);
        const int k3 = (i3 >= 0 ? i3 & 7 : i3) | (j3 == s0 ? 8 : 0);
        const int k4 = (i4 >= 0 ? i4 & 7 : i4) | (j4 == s0 ? 8 : 0);
        const int k5 = (i5 >= 0 ? i5 & 7 : i5) | (j5 == s0 ? 8 : 0);
        const int k6 = (i6 >= 0 ? i6 & 7 : i6) | (j6 == s0 ? 8 : 0);
        const int k7 = (i7 >= 0 ? i7 & 7 : i7) | (j7 == s0 ? 8 : 0);
        x0 = blend8s<k0,k1,k2,k3,k4,k5,k6,k7> (select4<r0>(a,b), select4<s0>(a,b));
    }
    else {
        // i0 - i7 from three or four different sources
        const int n0 = j0 >= 0 ? j0 /2*8 + 0 : j0;
        const int n1 = j1 >= 0 ? j1 /2*8 + 1 : j1;
        const int n2 = j2 >= 0 ? j2 /2*8 + 2 : j2;
        const int n3 = j3 >= 0 ? j3 /2*8 + 3 : j3;
        const int n4 = j4 >= 0 ? j4 /2*8 + 4 : j4;
        const int n5 = j5 >= 0 ? j5 /2*8 + 5 : j5;
        const int n6 = j6 >= 0 ? j6 /2*8 + 6 : j6;
        const int n7 = j7 >= 0 ? j7 /2*8 + 7 : j7;
        x0 = blend8s<n0, n1, n2, n3, n4, n5, n6, n7> (
             blend8s< j0   & 2 ? -256 : i0 &15,  j1   & 2 ? -256 : i1 &15,  j2   & 2 ? -256 : i2 &15,  j3   & 2 ? -256 : i3 &15,  j4   & 2 ? -256 : i4 &15,  j5   & 2 ? -256 : i5 &15,  j6   & 2 ? -256 : i6 &15,  j7   & 2 ? -256 : i7 &15> (a.get_low(),a.get_high()),
             blend8s<(j0^2)& 6 ? -256 : i0 &15, (j1^2)& 6 ? -256 : i1 &15, (j2^2)& 6 ? -256 : i2 &15, (j3^2)& 6 ? -256 : i3 &15, (j4^2)& 6 ? -256 : i4 &15, (j5^2)& 6 ? -256 : i5 &15, (j6^2)& 6 ? -256 : i6 &15, (j7^2)& 6 ? -256 : i7 &15> (b.get_low(),b.get_high()));
    }

    if (r1 < 0) {
        x1 = _mm_setzero_si128();
    }
    else if (r1 == s1) {
        // i8 - i15 all from same source
        x1 = permute8s<i8&-25, i9&-25, i10&-25, i11&-25, i12&-25, i13&-25, i14&-25, i15&-25> (select4<r1> (a,b));
    }
    else if ((j10<0||j10==r1||j10==s1) && (j11<0||j11==r1||j11==s1) && (j12<0||j12==r1||j12==s1) && (j13<0||j13==r1||j13==s1) && (j14<0||j14==r1||j14==s1) && (j15<0||j15==r1||j15==s1)) { 
        // i8 - i15 all from two sources
        const int k8 =  i8 >= 0 ? i8 & 7 : i8;
        const int k9 = (i9 >= 0 ? i9 & 7 : i9 ) | (j9 == s1 ? 8 : 0);
        const int k10= (i10>= 0 ? i10& 7 : i10) | (j10== s1 ? 8 : 0);
        const int k11= (i11>= 0 ? i11& 7 : i11) | (j11== s1 ? 8 : 0);
        const int k12= (i12>= 0 ? i12& 7 : i12) | (j12== s1 ? 8 : 0);
        const int k13= (i13>= 0 ? i13& 7 : i13) | (j13== s1 ? 8 : 0);
        const int k14= (i14>= 0 ? i14& 7 : i14) | (j14== s1 ? 8 : 0);
        const int k15= (i15>= 0 ? i15& 7 : i15) | (j15== s1 ? 8 : 0);
        x1 = blend8s<k8,k9,k10,k11,k12,k13,k14,k15> (select4<r1>(a,b), select4<s1>(a,b));
    }
    else {
        // i8 - i15 from three or four different sources
        const int n8 = j8 >= 0 ? j8 /2*8 + 0 : j8 ;
        const int n9 = j9 >= 0 ? j9 /2*8 + 1 : j9 ;
        const int n10= j10>= 0 ? j10/2*8 + 2 : j10;
        const int n11= j11>= 0 ? j11/2*8 + 3 : j11;
        const int n12= j12>= 0 ? j12/2*8 + 4 : j12;
        const int n13= j13>= 0 ? j13/2*8 + 5 : j13;
        const int n14= j14>= 0 ? j14/2*8 + 6 : j14;
        const int n15= j15>= 0 ? j15/2*8 + 7 : j15;
        x1 = blend8s<n8, n9, n10, n11, n12, n13, n14, n15> (
             blend8s< j8   & 2 ? -256 : i8 &15,  j9   & 2 ? -256 : i9 &15,  j10   & 2 ? -256 : i10 &15,  j11   & 2 ? -256 : i11 &15,  j12   & 2 ? -256 : i12 &15,  j13   & 2 ? -256 : i13 &15,  j14   & 2 ? -256 : i14 &15,  j15   & 2 ? -256 : i15 &15> (a.get_low(),a.get_high()),
             blend8s<(j8^2)& 6 ? -256 : i8 &15, (j9^2)& 6 ? -256 : i9 &15, (j10^2)& 6 ? -256 : i10 &15, (j11^2)& 6 ? -256 : i11 &15, (j12^2)& 6 ? -256 : i12 &15, (j13^2)& 6 ? -256 : i13 &15, (j14^2)& 6 ? -256 : i14 &15, (j15^2)& 6 ? -256 : i15 &15> (b.get_low(),b.get_high()));
    }
    return Vec16s(x0,x1);
}

template <int i0, int i1, int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15 > 
static inline Vec16us blend16us(Vec16us const & a, Vec16us const & b) {
    return Vec16us( blend16s<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15> (a,b));
}

// helper function used below
template <int n>
static inline Vec16c select4(Vec32c const & a, Vec32c const & b) {
    switch (n) {
    case 0:
        return a.get_low();
    case 1:
        return a.get_high();
    case 2:
        return b.get_low();
    case 3:
        return b.get_high();
    }
    return _mm_setzero_si128();
}

template <int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15,
          int i16, int i17, int i18, int i19, int i20, int i21, int i22, int i23,
          int i24, int i25, int i26, int i27, int i28, int i29, int i30, int i31 > 
static inline Vec32c blend32c(Vec32c const & a, Vec32c const & b) {  

    // j0 - j31 indicate one of four 16-byte sources
    const int j0  = i0  >= 0 ? i0 /16 : i0;
    const int j1  = i1  >= 0 ? i1 /16 : i1;
    const int j2  = i2  >= 0 ? i2 /16 : i2;
    const int j3  = i3  >= 0 ? i3 /16 : i3;
    const int j4  = i4  >= 0 ? i4 /16 : i4;
    const int j5  = i5  >= 0 ? i5 /16 : i5;
    const int j6  = i6  >= 0 ? i6 /16 : i6;
    const int j7  = i7  >= 0 ? i7 /16 : i7;
    const int j8  = i8  >= 0 ? i8 /16 : i8;
    const int j9  = i9  >= 0 ? i9 /16 : i9;
    const int j10 = i10 >= 0 ? i10/16 : i10;
    const int j11 = i11 >= 0 ? i11/16 : i11;
    const int j12 = i12 >= 0 ? i12/16 : i12;
    const int j13 = i13 >= 0 ? i13/16 : i13;
    const int j14 = i14 >= 0 ? i14/16 : i14;
    const int j15 = i15 >= 0 ? i15/16 : i15;
    const int j16 = i16 >= 0 ? i16/16 : i16;
    const int j17 = i17 >= 0 ? i17/16 : i17;
    const int j18 = i18 >= 0 ? i18/16 : i18;
    const int j19 = i19 >= 0 ? i19/16 : i19;
    const int j20 = i20 >= 0 ? i20/16 : i20;
    const int j21 = i21 >= 0 ? i21/16 : i21;
    const int j22 = i22 >= 0 ? i22/16 : i22;
    const int j23 = i23 >= 0 ? i23/16 : i23;
    const int j24 = i24 >= 0 ? i24/16 : i24;
    const int j25 = i25 >= 0 ? i25/16 : i25;
    const int j26 = i26 >= 0 ? i26/16 : i26;
    const int j27 = i27 >= 0 ? i27/16 : i27;
    const int j28 = i28 >= 0 ? i28/16 : i28;
    const int j29 = i29 >= 0 ? i29/16 : i29;
    const int j30 = i30 >= 0 ? i30/16 : i30;
    const int j31 = i31 >= 0 ? i31/16 : i31;

    Vec16c x0, x1;

    // r0, s0 = first two sources of low  destination (i0  - i15)
    // r1, s1 = first two sources of high destination (i16 - i31)
    const int r0 = j0 >= 0 ? j0 : j1 >= 0 ? j1 : j2  >= 0 ? j2  : j3  >= 0 ? j3  : j4  >= 0 ? j4  : j5  >= 0 ? j5  : j6  >= 0 ? j6  : j7 >= 0 ? j7 : 
                   j8 >= 0 ? j8 : j9 >= 0 ? j9 : j10 >= 0 ? j10 : j11 >= 0 ? j11 : j12 >= 0 ? j12 : j13 >= 0 ? j13 : j14 >= 0 ? j14 : j15;
    const int r1 = j16>= 0 ? j16: j17>= 0 ? j17: j18 >= 0 ? j18 : j19 >= 0 ? j19 : j20 >= 0 ? j20 : j21 >= 0 ? j21 : j22 >= 0 ? j22 : j23>= 0 ? j23: 
                   j24>= 0 ? j24: j25>= 0 ? j25: j26 >= 0 ? j26 : j27 >= 0 ? j27 : j28 >= 0 ? j28 : j29 >= 0 ? j29 : j30 >= 0 ? j30 : j31;
    const int s0 = (j1 >=0&&j1 !=r0)?j1  : (j2 >=0&&j2 !=r0)?j2  : (j3 >=0&&j3 !=r0)?j3  : (j4 >=0&&j4 !=r0)?j4  : (j5 >=0&&j5 !=r0)?j5  : (j6 >=0&&j6 !=r0)?j6 : (j7 >=0&&j7 !=r0)?j7 : 
                   (j8 >=0&&j8 !=r0)?j8  : (j9 >=0&&j9 !=r0)?j9  : (j10>=0&&j10!=r0)?j10 : (j11>=0&&j11!=r0)?j11 : (j12>=0&&j12!=r0)?j12 : (j13>=0&&j13!=r0)?j13: (j14>=0&&j14!=r0)?j14: j15;
    const int s1 = (j17>=0&&j17!=r1)?j17 : (j18>=0&&j18!=r1)?j18 : (j19>=0&&j19!=r1)?j19 : (j20>=0&&j20!=r1)?j20 : (j21>=0&&j21!=r1)?j21 : (j22>=0&&j22!=r1)?j22: (j23>=0&&j23!=r1)?j23: 
                   (j24>=0&&j24!=r1)?j24 : (j25>=0&&j25!=r1)?j25 : (j26>=0&&j26!=r1)?j26 : (j27>=0&&j27!=r1)?j27 : (j28>=0&&j28!=r1)?j28 : (j29>=0&&j29!=r1)?j29: (j30>=0&&j30!=r1)?j30: j31;

    if (r0 < 0) {
        x0 = _mm_setzero_si128();
    }
    else if (r0 == s0) {
        // i0 - i15 all from same source
        x0 = permute16c< i0&-49, i1&-49, i2 &-49, i3 &-49, i4 &-49, i5 &-49, i6 &-49, i7 &-49,
                         i8&-49, i9&-49, i10&-49, i11&-49, i12&-49, i13&-49, i14&-49, i15&-49 >
             (select4<r0> (a,b));
    }
    else if ((j2 <0||j2 ==r0||j2 ==s0) && (j3 <0||j3 ==r0||j3 ==s0) && (j4 <0||j4 ==r0||j4 ==s0) && (j5 <0||j5 ==r0||j5 ==s0) && (j6 <0||j6 ==r0||j6 ==s0) && (j7 <0||j7 ==r0||j7 ==s0) && (j8 <0||j8 ==r0||j8 ==s0) && 
             (j9 <0||j9 ==r0||j9 ==s0) && (j10<0||j10==r0||j10==s0) && (j11<0||j11==r0||j11==s0) && (j12<0||j12==r0||j12==s0) && (j13<0||j13==r0||j13==s0) && (j14<0||j14==r0||j14==s0) && (j15<0||j15==r0||j15==s0)) {
        // i0 - i15 all from two sources
        const int k0 =  i0 >= 0 ? i0 & 15 : i0;
        const int k1 = (i1 >= 0 ? i1 & 15 : i1 ) | (j1 == s0 ? 16 : 0);
        const int k2 = (i2 >= 0 ? i2 & 15 : i2 ) | (j2 == s0 ? 16 : 0);
        const int k3 = (i3 >= 0 ? i3 & 15 : i3 ) | (j3 == s0 ? 16 : 0);
        const int k4 = (i4 >= 0 ? i4 & 15 : i4 ) | (j4 == s0 ? 16 : 0);
        const int k5 = (i5 >= 0 ? i5 & 15 : i5 ) | (j5 == s0 ? 16 : 0);
        const int k6 = (i6 >= 0 ? i6 & 15 : i6 ) | (j6 == s0 ? 16 : 0);
        const int k7 = (i7 >= 0 ? i7 & 15 : i7 ) | (j7 == s0 ? 16 : 0);
        const int k8 = (i8 >= 0 ? i8 & 15 : i8 ) | (j8 == s0 ? 16 : 0);
        const int k9 = (i9 >= 0 ? i9 & 15 : i9 ) | (j9 == s0 ? 16 : 0);
        const int k10= (i10>= 0 ? i10& 15 : i10) | (j10== s0 ? 16 : 0);
        const int k11= (i11>= 0 ? i11& 15 : i11) | (j11== s0 ? 16 : 0);
        const int k12= (i12>= 0 ? i12& 15 : i12) | (j12== s0 ? 16 : 0);
        const int k13= (i13>= 0 ? i13& 15 : i13) | (j13== s0 ? 16 : 0);
        const int k14= (i14>= 0 ? i14& 15 : i14) | (j14== s0 ? 16 : 0);
        const int k15= (i15>= 0 ? i15& 15 : i15) | (j15== s0 ? 16 : 0);
        x0 = blend16c<k0,k1,k2,k3,k4,k5,k6,k7,k8,k9,k10,k11,k12,k13,k14,k15> (select4<r0>(a,b), select4<s0>(a,b));
    }
    else {
        // i0 - i15 from three or four different sources
        const int n0 = j0 >= 0 ? j0 /2*16 + 0 : j0;
        const int n1 = j1 >= 0 ? j1 /2*16 + 1 : j1;
        const int n2 = j2 >= 0 ? j2 /2*16 + 2 : j2;
        const int n3 = j3 >= 0 ? j3 /2*16 + 3 : j3;
        const int n4 = j4 >= 0 ? j4 /2*16 + 4 : j4;
        const int n5 = j5 >= 0 ? j5 /2*16 + 5 : j5;
        const int n6 = j6 >= 0 ? j6 /2*16 + 6 : j6;
        const int n7 = j7 >= 0 ? j7 /2*16 + 7 : j7;
        const int n8 = j8 >= 0 ? j8 /2*16 + 8 : j8;
        const int n9 = j9 >= 0 ? j9 /2*16 + 9 : j9;
        const int n10= j10>= 0 ? j10/2*16 +10 : j10;
        const int n11= j11>= 0 ? j11/2*16 +11 : j11;
        const int n12= j12>= 0 ? j12/2*16 +12 : j12;
        const int n13= j13>= 0 ? j13/2*16 +13 : j13;
        const int n14= j14>= 0 ? j14/2*16 +14 : j14;
        const int n15= j15>= 0 ? j15/2*16 +15 : j15;

        Vec16c x0a = blend16c< j0   & 2 ? -256 : i0 & 31,  j1   & 2 ? -256 : i1 & 31,  j2    & 2 ? -256 : i2 & 31,  j3    & 2 ? -256 : i3 & 31,  j4    & 2 ? -256 : i4 & 31,  j5    & 2 ? -256 : i5 & 31,  j6    & 2 ? -256 : i6 & 31,  j7    & 2 ? -256 : i7 & 31,
                               j8   & 2 ? -256 : i8 & 31,  j9   & 2 ? -256 : i9 & 31,  j10   & 2 ? -256 : i10& 31,  j11   & 2 ? -256 : i11& 31,  j12   & 2 ? -256 : i12& 31,  j13   & 2 ? -256 : i13& 31,  j14   & 2 ? -256 : i14& 31,  j15   & 2 ? -256 : i15& 31 > (a.get_low(),a.get_high());
        Vec16c x0b = blend16c<(j0^2)& 6 ? -256 : i0 & 31, (j1^2)& 6 ? -256 : i1 & 31, (j2 ^2)& 6 ? -256 : i2 & 31, (j3 ^2)& 6 ? -256 : i3 & 31, (j4 ^2)& 6 ? -256 : i4 & 31, (j5 ^2)& 6 ? -256 : i5 & 31, (j6 ^2)& 6 ? -256 : i6 & 31, (j7 ^2)& 6 ? -256 : i7 & 31,
                              (j8^2)& 6 ? -256 : i8 & 31, (j9^2)& 6 ? -256 : i9 & 31, (j10^2)& 6 ? -256 : i10& 31, (j11^2)& 6 ? -256 : i11& 31, (j12^2)& 6 ? -256 : i12& 31, (j13^2)& 6 ? -256 : i13& 31, (j14^2)& 6 ? -256 : i14& 31, (j15^2)& 6 ? -256 : i15& 31 > (b.get_low(),b.get_high());
        x0         = blend16c<n0, n1, n2, n3, n4, n5, n6, n7, n8, n9, n10, n11, n12, n13, n14, n15> (x0a, x0b);
    }

    if (r1 < 0) {
        x1 = _mm_setzero_si128();
    }
    else if (r1 == s1) {
        // i16 - i31 all from same source
        x1 = permute16c< i16&-49, i17&-49, i18&-49, i19&-49, i20&-49, i21&-49, i22&-49, i23&-49,
                         i24&-49, i25&-49, i26&-49, i27&-49, i28&-49, i29&-49, i30&-49, i31&-49 >
             (select4<r1> (a,b));
    }
    else if ((j18<0||j18==r1||j18==s1) && (j19<0||j19==r1||j19==s1) && (j20<0||j20==r1||j20==s1) && (j21<0||j21==r1||j21==s1) && (j22<0||j22==r1||j22==s1) && (j23<0||j23==r1||j23==s1) && (j24<0||j24==r1||j24==s1) && 
             (j25<0||j25==r1||j25==s1) && (j26<0||j26==r1||j26==s1) && (j27<0||j27==r1||j27==s1) && (j28<0||j28==r1||j28==s1) && (j29<0||j29==r1||j29==s1) && (j30<0||j30==r1||j30==s1) && (j31<0||j31==r1||j31==s1)) {
        // i16 - i31 all from two sources
        const int k16=  i16>= 0 ? i16& 15 : i16;
        const int k17= (i17>= 0 ? i17& 15 : i17) | (j17== s1 ? 16 : 0);
        const int k18= (i18>= 0 ? i18& 15 : i18) | (j18== s1 ? 16 : 0);
        const int k19= (i19>= 0 ? i19& 15 : i19) | (j19== s1 ? 16 : 0);
        const int k20= (i20>= 0 ? i20& 15 : i20) | (j20== s1 ? 16 : 0);
        const int k21= (i21>= 0 ? i21& 15 : i21) | (j21== s1 ? 16 : 0);
        const int k22= (i22>= 0 ? i22& 15 : i22) | (j22== s1 ? 16 : 0);
        const int k23= (i23>= 0 ? i23& 15 : i23) | (j23== s1 ? 16 : 0);
        const int k24= (i24>= 0 ? i24& 15 : i24) | (j24== s1 ? 16 : 0);
        const int k25= (i25>= 0 ? i25& 15 : i25) | (j25== s1 ? 16 : 0);
        const int k26= (i26>= 0 ? i26& 15 : i26) | (j26== s1 ? 16 : 0);
        const int k27= (i27>= 0 ? i27& 15 : i27) | (j27== s1 ? 16 : 0);
        const int k28= (i28>= 0 ? i28& 15 : i28) | (j28== s1 ? 16 : 0);
        const int k29= (i29>= 0 ? i29& 15 : i29) | (j29== s1 ? 16 : 0);
        const int k30= (i30>= 0 ? i30& 15 : i30) | (j30== s1 ? 16 : 0);
        const int k31= (i31>= 0 ? i31& 15 : i31) | (j31== s1 ? 16 : 0);
        x1 = blend16c<k16,k17,k18,k19,k20,k21,k22,k23,k24,k25,k26,k27,k28,k29,k30,k31> (select4<r1>(a,b), select4<s1>(a,b));
    }
    else {
        // i16 - i31 from three or four different sources
        const int n16= j16>= 0 ? j16/2*16 + 0 : j16;
        const int n17= j17>= 0 ? j17/2*16 + 1 : j17;
        const int n18= j18>= 0 ? j18/2*16 + 2 : j18;
        const int n19= j19>= 0 ? j19/2*16 + 3 : j19;
        const int n20= j20>= 0 ? j20/2*16 + 4 : j20;
        const int n21= j21>= 0 ? j21/2*16 + 5 : j21;
        const int n22= j22>= 0 ? j22/2*16 + 6 : j22;
        const int n23= j23>= 0 ? j23/2*16 + 7 : j23;
        const int n24= j24>= 0 ? j24/2*16 + 8 : j24;
        const int n25= j25>= 0 ? j25/2*16 + 9 : j25; 
        const int n26= j26>= 0 ? j26/2*16 +10 : j26;
        const int n27= j27>= 0 ? j27/2*16 +11 : j27;
        const int n28= j28>= 0 ? j28/2*16 +12 : j28;
        const int n29= j29>= 0 ? j29/2*16 +13 : j29;
        const int n30= j30>= 0 ? j30/2*16 +14 : j30;
        const int n31= j31>= 0 ? j31/2*16 +15 : j31;
        x1 = blend16c<n16, n17, n18, n19, n20, n21, n22, n23, n24, n25, n26, n27, n28, n29, n30, n31> (
             blend16c< j16   & 2 ? -256 : i16& 31,  j17   & 2 ? -256 : i17& 31,  j18   & 2 ? -256 : i18& 31,  j19   & 2 ? -256 : i19& 31,  j20   & 2 ? -256 : i20& 31,  j21   & 2 ? -256 : i21& 31,  j22   & 2 ? -256 : i22& 31,  j23   & 2 ? -256 : i23& 31,
                       j24   & 2 ? -256 : i24& 31,  j25   & 2 ? -256 : i25& 31,  j26   & 2 ? -256 : i26& 31,  j27   & 2 ? -256 : i27& 31,  j28   & 2 ? -256 : i28& 31,  j29   & 2 ? -256 : i29& 31,  j30   & 2 ? -256 : i30& 31,  j31   & 2 ? -256 : i31& 31 > (a.get_low(),a.get_high()),
             blend16c<(j16^2)& 6 ? -256 : i16& 31, (j17^2)& 6 ? -256 : i17& 31, (j18^2)& 6 ? -256 : i18& 31, (j19^2)& 6 ? -256 : i19& 31, (j20^2)& 6 ? -256 : i20& 31, (j21^2)& 6 ? -256 : i21& 31, (j22^2)& 6 ? -256 : i22& 31, (j23^2)& 6 ? -256 : i23& 31,
                      (j24^2)& 6 ? -256 : i24& 31, (j25^2)& 6 ? -256 : i25& 31, (j26^2)& 6 ? -256 : i26& 31, (j27^2)& 6 ? -256 : i27& 31, (j28^2)& 6 ? -256 : i28& 31, (j29^2)& 6 ? -256 : i29& 31, (j30^2)& 6 ? -256 : i30& 31, (j31^2)& 6 ? -256 : i31& 31 > (b.get_low(),b.get_high()));
    }
    return Vec32c(x0,x1);
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
#if defined (__XOP__)   // AMD XOP instruction set. Use VPPERM
    Vec16c t0 = _mm_perm_epi8(table.get_low(), table.get_high(), index.get_low());
    Vec16c t1 = _mm_perm_epi8(table.get_low(), table.get_high(), index.get_high());
    return Vec32c(t0, t1);
#else
    Vec16c t0 = lookup32(index.get_low() , table.get_low(), table.get_high());
    Vec16c t1 = lookup32(index.get_high(), table.get_low(), table.get_high());
    return Vec32c(t0, t1);
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
    uint8_t ii[32];  index1.store(ii);
    int8_t  rr[32];
    for (int j = 0; j < 32; j++) {
        rr[j] = ((int8_t*)table)[ii[j]];
    }
    return Vec32c().load(rr);
}

template <int n>
static inline Vec32c lookup(Vec32c const & index, void const * table) {
    return lookup<n>(Vec32uc(index), table);
}


static inline Vec16s lookup16(Vec16s const & index, Vec16s const & table) {
    Vec8s t0 = lookup16(index.get_low() , table.get_low(), table.get_high());
    Vec8s t1 = lookup16(index.get_high(), table.get_low(), table.get_high());
    return Vec16s(t0, t1);
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
    Vec16us i1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        i1 = Vec16us(index) & (n-1);
    }
    else {
        // n is not a power of 2, limit to n-1
        i1 = min(Vec16us(index), n-1);
    }
    int16_t const * t = (int16_t const *)table;
    return Vec16s(t[i1[0]],t[i1[1]],t[i1[2]],t[i1[3]],t[i1[4]],t[i1[5]],t[i1[6]],t[i1[7]],
        t[i1[8]],t[i1[9]],t[i1[10]],t[i1[11]],t[i1[12]],t[i1[13]],t[i1[14]],t[i1[15]]);
}

static inline Vec8i lookup8(Vec8i const & index, Vec8i const & table) {
    Vec4i t0 = lookup8(index.get_low() , table.get_low(), table.get_high());
    Vec4i t1 = lookup8(index.get_high(), table.get_low(), table.get_high());
    return Vec8i(t0, t1);
}

template <int n>
static inline Vec8i lookup(Vec8i const & index, void const * table) {
    if (n <= 0) return 0;
    if (n <= 4) {
        Vec4i table1 = Vec4i().load(table);        
        return Vec8i(       
            lookup4 (index.get_low(),  table1),
            lookup4 (index.get_high(), table1));
    }
    if (n <= 8) {
        return lookup8(index, Vec8i().load(table));
    }
    // n > 8. Limit index
    Vec8ui i1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        i1 = Vec8ui(index) & (n-1);
    }
    else {
        // n is not a power of 2, limit to n-1
        i1 = min(Vec8ui(index), n-1);
    }
    int32_t const * t = (int32_t const *)table;
    return Vec8i(t[i1[0]],t[i1[1]],t[i1[2]],t[i1[3]],t[i1[4]],t[i1[5]],t[i1[6]],t[i1[7]]);
}

static inline Vec4q lookup4(Vec4q const & index, Vec4q const & table) {
    return lookup8(Vec8i(index * 0x200000002ll + 0x100000000ll), Vec8i(table));
}

template <int n>
static inline Vec4q lookup(Vec4q const & index, void const * table) {
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
    uint32_t ii[8];  index1.store(ii);  // use only lower 32 bits of each index
    int64_t const * tt = (int64_t const *)table;
    return Vec4q(tt[ii[0]], tt[ii[2]], tt[ii[4]], tt[ii[6]]);    
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
    // use lookup function
    return lookup<imax+1>(Vec8i(i0,i1,i2,i3,i4,i5,i6,i7), a);
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
    // use lookup function
    return lookup<imax+1>(Vec4q(i0,i1,i2,i3), a);
}




/*****************************************************************************
*
*          Functions for conversion between integer sizes
*
*****************************************************************************/

// Extend 8-bit integers to 16-bit integers, signed and unsigned

// Function extend_low : extends the low 16 elements to 16 bits with sign extension
static inline Vec16s extend_low (Vec32c const & a) {
    return Vec16s(extend_low(a.get_low()), extend_high(a.get_low()));
}

// Function extend_high : extends the high 16 elements to 16 bits with sign extension
static inline Vec16s extend_high (Vec32c const & a) {
    return Vec16s(extend_low(a.get_high()), extend_high(a.get_high()));
}

// Function extend_low : extends the low 16 elements to 16 bits with zero extension
static inline Vec16us extend_low (Vec32uc const & a) {
    return Vec16us(extend_low(a.get_low()), extend_high(a.get_low()));
}

// Function extend_high : extends the high 19 elements to 16 bits with zero extension
static inline Vec16us extend_high (Vec32uc const & a) {
    return Vec16us(extend_low(a.get_high()), extend_high(a.get_high()));
}

// Extend 16-bit integers to 32-bit integers, signed and unsigned

// Function extend_low : extends the low 8 elements to 32 bits with sign extension
static inline Vec8i extend_low (Vec16s const & a) {
    return Vec8i(extend_low(a.get_low()), extend_high(a.get_low()));
}

// Function extend_high : extends the high 8 elements to 32 bits with sign extension
static inline Vec8i extend_high (Vec16s const & a) {
    return Vec8i(extend_low(a.get_high()), extend_high(a.get_high()));
}

// Function extend_low : extends the low 8 elements to 32 bits with zero extension
static inline Vec8ui extend_low (Vec16us const & a) {
    return Vec8ui(extend_low(a.get_low()), extend_high(a.get_low()));
}

// Function extend_high : extends the high 8 elements to 32 bits with zero extension
static inline Vec8ui extend_high (Vec16us const & a) {
    return Vec8ui(extend_low(a.get_high()), extend_high(a.get_high()));
}

// Extend 32-bit integers to 64-bit integers, signed and unsigned

// Function extend_low : extends the low 4 elements to 64 bits with sign extension
static inline Vec4q extend_low (Vec8i const & a) {
    return Vec4q(extend_low(a.get_low()), extend_high(a.get_low()));
}

// Function extend_high : extends the high 4 elements to 64 bits with sign extension
static inline Vec4q extend_high (Vec8i const & a) {
    return Vec4q(extend_low(a.get_high()), extend_high(a.get_high()));
}

// Function extend_low : extends the low 4 elements to 64 bits with zero extension
static inline Vec4uq extend_low (Vec8ui const & a) {
    return Vec4uq(extend_low(a.get_low()), extend_high(a.get_low()));
}

// Function extend_high : extends the high 4 elements to 64 bits with zero extension
static inline Vec4uq extend_high (Vec8ui const & a) {
    return Vec4uq(extend_low(a.get_high()), extend_high(a.get_high()));
}

// Compress 16-bit integers to 8-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Overflow wraps around
static inline Vec32c compress (Vec16s const & low, Vec16s const & high) {
    return Vec32c(compress(low.get_low(),low.get_high()), compress(high.get_low(),high.get_high()));
}

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Signed, with saturation
static inline Vec32c compress_saturated (Vec16s const & low, Vec16s const & high) {
    return Vec32c(compress_saturated(low.get_low(),low.get_high()), compress_saturated(high.get_low(),high.get_high()));
}

// Function compress : packs two vectors of 16-bit integers to one vector of 8-bit integers
// Unsigned, overflow wraps around
static inline Vec32uc compress (Vec16us const & low, Vec16us const & high) {
    return Vec32uc(compress(low.get_low(),low.get_high()), compress(high.get_low(),high.get_high()));
}

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Unsigned, with saturation
static inline Vec32uc compress_saturated (Vec16us const & low, Vec16us const & high) {
    return Vec32uc(compress_saturated(low.get_low(),low.get_high()), compress_saturated(high.get_low(),high.get_high()));
}

// Compress 32-bit integers to 16-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Overflow wraps around
static inline Vec16s compress (Vec8i const & low, Vec8i const & high) {
    return Vec16s(compress(low.get_low(),low.get_high()), compress(high.get_low(),high.get_high()));
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Signed with saturation
static inline Vec16s compress_saturated (Vec8i const & low, Vec8i const & high) {
    return Vec16s(compress_saturated(low.get_low(),low.get_high()), compress_saturated(high.get_low(),high.get_high()));
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Overflow wraps around
static inline Vec16us compress (Vec8ui const & low, Vec8ui const & high) {
    return Vec16us(compress(low.get_low(),low.get_high()), compress(high.get_low(),high.get_high()));
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Unsigned, with saturation
static inline Vec16us compress_saturated (Vec8ui const & low, Vec8ui const & high) {
    return Vec16us(compress_saturated(low.get_low(),low.get_high()), compress_saturated(high.get_low(),high.get_high()));
}

// Compress 64-bit integers to 32-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Overflow wraps around
static inline Vec8i compress (Vec4q const & low, Vec4q const & high) {
    return Vec8i(compress(low.get_low(),low.get_high()), compress(high.get_low(),high.get_high()));
}

// Function compress : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Signed, with saturation
static inline Vec8i compress_saturated (Vec4q const & low, Vec4q const & high) {
    return Vec8i(compress_saturated(low.get_low(),low.get_high()), compress_saturated(high.get_low(),high.get_high()));
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Overflow wraps around
static inline Vec8ui compress (Vec4uq const & low, Vec4uq const & high) {
    return Vec8ui (compress((Vec4q)low, (Vec4q)high));
}

// Function compress : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Unsigned, with saturation
static inline Vec8ui compress_saturated (Vec4uq const & low, Vec4uq const & high) {
    return Vec8ui(compress_saturated(low.get_low(),low.get_high()), compress_saturated(high.get_low(),high.get_high()));
}


/*****************************************************************************
*
*          Integer division 2: divisor is a compile-time constant
*
*****************************************************************************/

// Divide Vec8i by compile-time constant
template <int32_t d>
static inline Vec8i divide_by_i(Vec8i const & a) {
    return Vec8i( divide_by_i<d>(a.get_low()), divide_by_i<d>(a.get_high()));
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
static inline Vec8ui divide_by_ui(Vec8ui const & a) {
    return Vec8ui( divide_by_ui<d>(a.get_low()), divide_by_ui<d>(a.get_high()));
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
static inline Vec16s divide_by_i(Vec16s const & a) {
    return Vec16s( divide_by_i<d>(a.get_low()), divide_by_i<d>(a.get_high()));
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
static inline Vec16us divide_by_ui(Vec16us const & a) {
    return Vec16us( divide_by_ui<d>(a.get_low()), divide_by_ui<d>(a.get_high()));
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
    int a1 = horizontal_find_first(x.get_low());
    if (a1 >= 0) return a1;
    int a2 = horizontal_find_first(x.get_high());
    if (a2 < 0) return a2;
    return a2 + 16;;
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
    return horizontal_count(x.get_low()) + horizontal_count(x.get_high());
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
    return to_bits(x.get_low()) | (uint32_t)to_bits(x.get_high()) << 16;
}

// to_Vec16c: convert integer bitfield to boolean vector
static inline Vec32cb to_Vec32cb(uint32_t x) {
    return Vec32c(to_Vec16cb(uint16_t(x)), to_Vec16cb(uint16_t(x>>16)));
}

// to_bits: convert boolean vector to integer bitfield
static inline uint16_t to_bits(Vec16sb const & x) {
    return to_bits(x.get_low()) | (uint16_t)to_bits(x.get_high()) << 8;
}

// to_Vec16sb: convert integer bitfield to boolean vector
static inline Vec16sb to_Vec16sb(uint16_t x) {
    return Vec16s(to_Vec8sb(uint8_t(x)), to_Vec8sb(uint8_t(x>>8)));
}

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec8ib const & x) {
    return to_bits(x.get_low()) | (uint8_t)to_bits(x.get_high()) << 4;
}

// to_Vec8ib: convert integer bitfield to boolean vector
static inline Vec8ib to_Vec8ib(uint8_t x) {
    return Vec8i(to_Vec4ib(x), to_Vec4ib(x>>4));
}

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec4qb const & x) {
    return to_bits(x.get_low()) | to_bits(x.get_high()) << 2;
}

// to_Vec16c: convert integer bitfield to boolean vector
static inline Vec4qb to_Vec4qb(uint8_t x) {
    return Vec4q(to_Vec2qb(x), to_Vec2qb(x>>2));
}

#endif // VECTORI256_H
