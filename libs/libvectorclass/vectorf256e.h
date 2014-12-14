/****************************  vectorf256e.h   *******************************
* Author:        Agner Fog
* Date created:  2012-05-30
* Last modified: 2014-10-22
* Version:       1.16
* Project:       vector classes
* Description:
* Header file defining 256-bit floating point vector classes as interface
* to intrinsic functions. Emulated for processors without AVX instruction set.
*
* The following vector classes are defined here:
* Vec8f     Vector of 8 single precision floating point numbers
* Vec8fb    Vector of 8 Booleans for use with Vec8f
* Vec4d     Vector of 4 double precision floating point numbers
* Vec4db    Vector of 4 Booleans for use with Vec4d
*
* For detailed instructions, see VectorClass.pdf
*
* (c) Copyright 2012 - 2014 GNU General Public License http://www.gnu.org/licenses
*****************************************************************************/

// check combination of header files
#ifdef VECTORF256_H
#if    VECTORF256_H != 1
#error Two different versions of vectorf256.h included
#endif
#else
#define VECTORF256_H  1

#if defined (VECTORI256_H) &&  VECTORI256_H >= 2
#error wrong combination of header files. Use vectorf256.h instead of vectorf256e.h if you have AVX2
#endif


#include "vectorf128.h"  // Define 128-bit vectors


/*****************************************************************************
*
*          base class Vec256fe and Vec256de
*
*****************************************************************************/
// base class to replace __m256 when AVX is not supported
class Vec256fe {
protected:
    __m128 y0;                         // low half
    __m128 y1;                         // high half
public:
    Vec256fe(void) {};                 // default constructor
    Vec256fe(__m128 x0, __m128 x1) {   // constructor to build from two __m128
        y0 = x0;  y1 = x1;
    }
    __m128 get_low() const {           // get low half
        return y0;
    }
    __m128 get_high() const {          // get high half
        return y1;
    }
};

// base class to replace __m256d when AVX is not supported
class Vec256de {
public:
    Vec256de() {};                     // default constructor
    Vec256de(__m128d x0, __m128d x1) { // constructor to build from two __m128d
        y0 = x0;  y1 = x1;
    }
    __m128d get_low() const {          // get low half
        return y0;
    }
    __m128d get_high() const {         // get high half
        return y1;
    }
protected:
    __m128d y0;                        // low half
    __m128d y1;                        // high half
};


/*****************************************************************************
*
*          select functions
*
*****************************************************************************/
// Select between two Vec256fe sources, element by element. Used in various functions 
// and operators. Corresponds to this pseudocode:
// for (int i = 0; i < 8; i++) result[i] = s[i] ? a[i] : b[i];
// Each element in s must be either 0 (false) or 0xFFFFFFFF (true).
static inline Vec256fe selectf (Vec256fe const & s, Vec256fe const & a, Vec256fe const & b) {
    return Vec256fe(selectf(b.get_low(), a.get_low(), s.get_low()), selectf(b.get_high(), a.get_high(), s.get_high()));
}

// Same, with two Vec256de sources.
// and operators. Corresponds to this pseudocode:
// for (int i = 0; i < 4; i++) result[i] = s[i] ? a[i] : b[i];
// Each element in s must be either 0 (false) or 0xFFFFFFFFFFFFFFFF (true). No other 
// values are allowed.
static inline Vec256de selectd (Vec256de const & s, Vec256de const & a, Vec256de const & b) {
    return Vec256de(selectd(b.get_low(), a.get_low(), s.get_low()), selectd(b.get_high(), a.get_high(), s.get_high()));
}



/*****************************************************************************
*
*          Generate compile-time constant vector
*
*****************************************************************************/
// Generate a constant vector of 8 integers stored in memory,
// load as __m256
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec256fe constant8f() {
    static const union {
        int      i[8];
        __m128   y[2];
    } u = {{i0,i1,i2,i3,i4,i5,i6,i7}};
    return Vec256fe(u.y[0], u.y[1]);
}


/*****************************************************************************
*
*          Vec8fb: Vector of 8 Booleans for use with Vec8f
*
*****************************************************************************/

class Vec8fb : public Vec256fe {
public:
    // Default constructor:
    Vec8fb() {
    }
    // Constructor to build from all elements:
    Vec8fb(bool b0, bool b1, bool b2, bool b3, bool b4, bool b5, bool b6, bool b7) {
        y0 = Vec4fb(b0, b1, b2, b3);
        y1 = Vec4fb(b4, b5, b6, b7);
    }
    // Constructor to build from two Vec4fb:
    Vec8fb(Vec4fb const & a0, Vec4fb const & a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256fe
    Vec8fb(Vec256fe const & x) {
        y0 = x.get_low();  y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256fe
    Vec8fb & operator = (Vec256fe const & x) {
        y0 = x.get_low();  y1 = x.get_high();
        return *this;
    }
#ifdef VECTORI256_H  // 256 bit integer vectors are available
    // Constructor to convert from type Vec8ib used as Boolean for integer vectors
    Vec8fb(Vec8ib const & x) {
        y0 = _mm_castsi128_ps(Vec8i(x).get_low());
        y1 = _mm_castsi128_ps(Vec8i(x).get_high());
    }
    // Assignment operator to convert from type Vec8ib used as Boolean for integer vectors
    Vec8fb & operator = (Vec8ib const & x) {
        y0 = _mm_castsi128_ps(Vec8i(x).get_low());
        y1 = _mm_castsi128_ps(Vec8i(x).get_high());
        return *this;
    }
    // Constructor to broadcast the same value into all elements:
    Vec8fb(bool b) {
        y1 = y0 = Vec4fb(b);
    }
    // Assignment operator to broadcast scalar value:
    Vec8fb & operator = (bool b) {
        y0 = y1 = Vec4fb(b);
        return *this;
    }
private: // Prevent constructing from int, etc.
    Vec8fb(int b);
    Vec8fb & operator = (int x);
public:
    // Type cast operator to convert to type Vec8ib used as Boolean for integer vectors
    operator Vec8ib() const {
        return Vec8i(_mm_castps_si128(y0), _mm_castps_si128(y1));
    }
#endif // VECTORI256_H
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec8fb const & insert(uint32_t index, bool value) {
        if (index < 4) {
            y0 = Vec4fb(y0).insert(index, value);
        }
        else {
            y1 = Vec4fb(y1).insert(index-4, value);
        }
        return *this;
    }
    // Member function extract a single element from vector
    // Note: This function is inefficient. Use store function if extracting more than one element
    bool extract(uint32_t index) const {
        if (index < 4) {
            return Vec4fb(y0).extract(index);
        }
        else {
            return Vec4fb(y1).extract(index-4);
        }
    }
    // Extract a single element. Operator [] can only read an element, not write.
    bool operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4fb:
    Vec4fb get_low() const {
        return y0;
    }
    Vec4fb get_high() const {
        return y1;
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
    return Vec8fb(a.get_low() & b.get_low(), a.get_high() & b.get_high());
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
    return Vec8fb(a.get_low() | b.get_low(), a.get_high() | b.get_high());
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
    return Vec8fb(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ^= : bitwise xor
static inline Vec8fb & operator ^= (Vec8fb & a, Vec8fb const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec8fb operator ~ (Vec8fb const & a) {
    return Vec8fb(~a.get_low(), ~a.get_high());
}

// vector operator ! : logical not
// (operator ! is less efficient than operator ~. Use only where not
// all bits in an element are the same)
static inline Vec8fb operator ! (Vec8fb const & a) {
    return Vec8fb(!a.get_low(), !a.get_high());
}

// Functions for Vec8fb

// andnot: a & ~ b
static inline Vec8fb andnot(Vec8fb const & a, Vec8fb const & b) {
    return Vec8fb(andnot(a.get_low(), b.get_low()), andnot(a.get_high(), b.get_high()));
}



/*****************************************************************************
*
*          Horizontal Boolean functions
*
*****************************************************************************/

// horizontal_and. Returns true if all bits are 1
static inline bool horizontal_and (Vec8fb const & a) {
    return horizontal_and(a.get_low() & a.get_high());
}

// horizontal_or. Returns true if at least one bit is 1
static inline bool horizontal_or (Vec8fb const & a) {
    return horizontal_or(a.get_low() | a.get_high());
}



/*****************************************************************************
*
*          Vec4db: Vector of 4 Booleans for use with Vec4d
*
*****************************************************************************/

class Vec4db : public Vec256de {
public:
    // Default constructor:
    Vec4db() {
    }
    // Constructor to build from all elements:
    Vec4db(bool b0, bool b1, bool b2, bool b3) {
        y0 = Vec2db(b0, b1);
        y1 = Vec2db(b2, b3);
    }
    // Constructor to build from two Vec2db:
    Vec4db(Vec2db const & a0, Vec2db const & a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256de
    Vec4db(Vec256de const & x) {
        y0 = x.get_low();  y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256de
    Vec4db & operator = (Vec256de const & x) {
        y0 = x.get_low();  y1 = x.get_high();
        return *this;
    }
#ifdef VECTORI256_H  // 256 bit integer vectors are available
    // Constructor to convert from type Vec4qb used as Boolean for integer vectors
    Vec4db(Vec4qb const & x) {
        y0 = _mm_castsi128_pd(Vec4q(x).get_low());
        y1 = _mm_castsi128_pd(Vec4q(x).get_high());
    }
    // Assignment operator to convert from type Vec4qb used as Boolean for integer vectors
    Vec4db & operator = (Vec4qb const & x) {
        y0 = _mm_castsi128_pd(Vec4q(x).get_low());
        y1 = _mm_castsi128_pd(Vec4q(x).get_high());
        return *this;
    }
    // Constructor to broadcast the same value into all elements:
    Vec4db(bool b) {
        y1 = y0 = Vec2db(b);
    }
    // Assignment operator to broadcast scalar value:
    Vec4db & operator = (bool b) {
        y0 = y1 = Vec2db(b);
        return *this;
    }
private: // Prevent constructing from int, etc.
    Vec4db(int b);
    Vec4db & operator = (int x);
public:
    // Type cast operator to convert to type Vec4qb used as Boolean for integer vectors
    operator Vec4qb() const {
        return Vec4q(_mm_castpd_si128(y0), _mm_castpd_si128(y1));
    }
#endif // VECTORI256_H
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec4db const & insert(uint32_t index, bool value) {
        if (index < 2) {
            y0 = Vec2db(y0).insert(index, value);
        }
        else {
            y1 = Vec2db(y1).insert(index - 2, value);
        }
        return *this;
    }
    // Member function extract a single element from vector
    // Note: This function is inefficient. Use store function if extracting more than one element
    bool extract(uint32_t index) const {
        if (index < 2) {
            return Vec2db(y0).extract(index);
        }
        else {
            return Vec2db(y1).extract(index - 2);
        }
    }
    // Extract a single element. Operator [] can only read an element, not write.
    bool operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4fb:
    Vec2db get_low() const {
        return y0;
    }
    Vec2db get_high() const {
        return y1;
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
    return Vec4db(a.get_low() & b.get_low(), a.get_high() & b.get_high());
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
    return Vec4db(a.get_low() | b.get_low(), a.get_high() | b.get_high());
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
    return Vec4db(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());

}

// vector operator ^= : bitwise xor
static inline Vec4db & operator ^= (Vec4db & a, Vec4db const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec4db operator ~ (Vec4db const & a) {
    return Vec4db(~a.get_low(), ~a.get_high());
}

// vector operator ! : logical not
// (operator ! is less efficient than operator ~. Use only where not
// all bits in an element are the same)
static inline Vec4db operator ! (Vec4db const & a) {
    return Vec4db(!a.get_low(), !a.get_high());
}

// Functions for Vec4db

// andnot: a & ~ b
static inline Vec4db andnot(Vec4db const & a, Vec4db const & b) {
    return Vec4db(andnot(a.get_low(), b.get_low()), andnot(a.get_high(), b.get_high()));
}


/*****************************************************************************
*
*          Horizontal Boolean functions
*
*****************************************************************************/

// horizontal_and. Returns true if all bits are 1
static inline bool horizontal_and (Vec4db const & a) {
    return horizontal_and(a.get_low() & a.get_high());
}

// horizontal_or. Returns true if at least one bit is 1
static inline bool horizontal_or (Vec4db const & a) {
    return horizontal_or(a.get_low() | a.get_high());
}



/*****************************************************************************
*
*          Vec8f: Vector of 8 single precision floating point values
*
*****************************************************************************/

class Vec8f : public Vec256fe {
public:
    // Default constructor:
    Vec8f() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec8f(float f) {
        y1 = y0 = _mm_set1_ps(f);
    }
    // Constructor to build from all elements:
    Vec8f(float f0, float f1, float f2, float f3, float f4, float f5, float f6, float f7) {
        y0 = _mm_setr_ps(f0, f1, f2, f3);
        y1 = _mm_setr_ps(f4, f5, f6, f7); 
    }
    // Constructor to build from two Vec4f:
    Vec8f(Vec4f const & a0, Vec4f const & a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256fe
    Vec8f(Vec256fe const & x) {
        y0 = x.get_low();  y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256fe
    Vec8f & operator = (Vec256fe const & x) {
        y0 = x.get_low();  y1 = x.get_high();
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec8f & load(float const * p) {
        y0 = _mm_loadu_ps(p);
        y1 = _mm_loadu_ps(p+4);
        return *this;
    }
    // Member function to load from array, aligned by 32
    // You may use load_a instead of load if you are certain that p points to an address
    // divisible by 32.
    Vec8f & load_a(float const * p) {
        y0 = _mm_load_ps(p);
        y1 = _mm_load_ps(p+4);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(float * p) const {
        _mm_storeu_ps(p,   y0);
        _mm_storeu_ps(p+4, y1);
    }
    // Member function to store into array, aligned by 32
    // You may use store_a instead of store if you are certain that p points to an address
    // divisible by 32.
    void store_a(float * p) const {
        _mm_store_ps(p,   y0);
        _mm_store_ps(p+4, y1);
    }
    // Partial load. Load n elements and set the rest to 0
    Vec8f & load_partial(int n, float const * p) {
        if (n > 0 && n <= 4) {
            *this = Vec8f(Vec4f().load_partial(n, p),_mm_setzero_ps());
        }
        else if (n > 4 && n <= 8) {
            *this = Vec8f(Vec4f().load(p), Vec4f().load_partial(n - 4, p + 4));
        }
        else {
            y1 = y0 = _mm_setzero_ps();
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
        else if (n >= 4) {
            y1 = Vec4f(y1).cutoff(n - 4);
        }
        else {
            y0 = Vec4f(y0).cutoff(n);
            y1 = Vec4f(0.0f);
        }
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec8f const & insert(uint32_t index, float value) {
        if (index < 4) {
            y0 = Vec4f(y0).insert(index, value);
        }
        else {
            y1 = Vec4f(y1).insert(index - 4, value);
        }
        return *this;
    }
    // Member function extract a single element from vector
    // Note: This function is inefficient. Use store function if extracting more than one element
    float extract(uint32_t index) const {
        if (index < 4) {
            return Vec4f(y0).extract(index);
        }
        else {
            return Vec4f(y1).extract(index - 4);
        }
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    float operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4f:
    Vec4f get_low() const {
        return y0;
    }
    Vec4f get_high() const {
        return y1;
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
    return Vec8f(a.get_low() + b.get_low(), a.get_high() + b.get_high());
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
    return Vec8f(a.get_low() - b.get_low(), a.get_high() - b.get_high());
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
    return Vec8f(-a.get_low(), -a.get_high());
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
    return Vec8f(a.get_low() * b.get_low(), a.get_high() * b.get_high());
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
    return Vec8f(a.get_low() / b.get_low(), a.get_high() / b.get_high());
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
    return Vec8fb(a.get_low() == b.get_low(), a.get_high() == b.get_high());
}

// vector operator != : returns true for elements for which a != b
static inline Vec8fb operator != (Vec8f const & a, Vec8f const & b) {
    return Vec8fb(a.get_low() != b.get_low(), a.get_high() != b.get_high());
}

// vector operator < : returns true for elements for which a < b
static inline Vec8fb operator < (Vec8f const & a, Vec8f const & b) {
    return Vec8fb(a.get_low() < b.get_low(), a.get_high() < b.get_high());
}

// vector operator <= : returns true for elements for which a <= b
static inline Vec8fb operator <= (Vec8f const & a, Vec8f const & b) {
    return Vec8fb(a.get_low() <= b.get_low(), a.get_high() <= b.get_high());
}

// vector operator > : returns true for elements for which a > b
static inline Vec8fb operator > (Vec8f const & a, Vec8f const & b) {
    return Vec8fb(a.get_low() > b.get_low(), a.get_high() > b.get_high());
}

// vector operator >= : returns true for elements for which a >= b
static inline Vec8fb operator >= (Vec8f const & a, Vec8f const & b) {
    return Vec8fb(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// Bitwise logical operators

// vector operator & : bitwise and
static inline Vec8f operator & (Vec8f const & a, Vec8f const & b) {
    return Vec8f(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}

// vector operator &= : bitwise and
static inline Vec8f & operator &= (Vec8f & a, Vec8f const & b) {
    a = a & b;
    return a;
}

// vector operator & : bitwise and of Vec8f and Vec8fb
static inline Vec8f operator & (Vec8f const & a, Vec8fb const & b) {
    return Vec8f(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec8f operator & (Vec8fb const & a, Vec8f const & b) {
    return Vec8f(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}

// vector operator | : bitwise or
static inline Vec8f operator | (Vec8f const & a, Vec8f const & b) {
    return Vec8f(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}

// vector operator |= : bitwise or
static inline Vec8f & operator |= (Vec8f & a, Vec8f const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec8f operator ^ (Vec8f const & a, Vec8f const & b) {
    return Vec8f(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ^= : bitwise xor
static inline Vec8f & operator ^= (Vec8f & a, Vec8f const & b) {
    a = a ^ b;
    return a;
}

// vector operator ! : logical not. Returns Boolean vector
static inline Vec8fb operator ! (Vec8f const & a) {
    return Vec8fb(!a.get_low(), !a.get_high());
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
    return Vec8f(select(s.get_low(),a.get_low(),b.get_low()), select(s.get_high(),a.get_high(),b.get_high()));
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
    return horizontal_add(a.get_low() + a.get_high());
}

// function max: a > b ? a : b
static inline Vec8f max(Vec8f const & a, Vec8f const & b) {
    return Vec8f(max(a.get_low(),b.get_low()), max(a.get_high(),b.get_high()));
}

// function min: a < b ? a : b
static inline Vec8f min(Vec8f const & a, Vec8f const & b) {
    return Vec8f(min(a.get_low(),b.get_low()), min(a.get_high(),b.get_high()));
}

// function abs: absolute value
// Removes sign bit, even for -0.0f, -INF and -NAN
static inline Vec8f abs(Vec8f const & a) {
    return Vec8f(abs(a.get_low()), abs(a.get_high()));
}

// function sqrt: square root
static inline Vec8f sqrt(Vec8f const & a) {
    return Vec8f(sqrt(a.get_low()), sqrt(a.get_high()));
}

// function square: a * a
static inline Vec8f square(Vec8f const & a) {
    return Vec8f(square(a.get_low()), square(a.get_high()));
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
    return Vec8f(pow_n<n>(a.get_low()), pow_n<n>(a.get_high()));
}

template <int n>
static inline Vec8f pow(Vec8f const & a, Const_int_t<n>) {
    return pow_n<n>(a);
}


// function round: round to nearest integer (even). (result as float vector)
static inline Vec8f round(Vec8f const & a) {
    return Vec8f(round(a.get_low()), round(a.get_high()));
}

// function truncate: round towards zero. (result as float vector)
static inline Vec8f truncate(Vec8f const & a) {
    return Vec8f(truncate(a.get_low()), truncate(a.get_high()));
}

// function floor: round towards minus infinity. (result as float vector)
static inline Vec8f floor(Vec8f const & a) {
    return Vec8f(floor(a.get_low()), floor(a.get_high()));
}

// function ceil: round towards plus infinity. (result as float vector)
static inline Vec8f ceil(Vec8f const & a) {
    return Vec8f(ceil(a.get_low()), ceil(a.get_high()));
}

#ifdef VECTORI256_H  // 256 bit integer vectors are available
// function round_to_int: round to nearest integer (even). (result as integer vector)
static inline Vec8i round_to_int(Vec8f const & a) {
    // Note: assume MXCSR control register is set to rounding
    return Vec8i(round_to_int(a.get_low()), round_to_int(a.get_high()));
}

// function truncate_to_int: round towards zero. (result as integer vector)
static inline Vec8i truncate_to_int(Vec8f const & a) {
    return Vec8i(truncate_to_int(a.get_low()), truncate_to_int(a.get_high()));
}

// function to_float: convert integer vector to float vector
static inline Vec8f to_float(Vec8i const & a) {
    return Vec8f(to_float(a.get_low()), to_float(a.get_high()));
}
#endif // VECTORI256_H 


// Approximate math functions

// approximate reciprocal (Faster than 1.f / a. relative accuracy better than 2^-11)
static inline Vec8f approx_recipr(Vec8f const & a) {
    return Vec8f(approx_recipr(a.get_low()), approx_recipr(a.get_high()));
}

// approximate reciprocal squareroot (Faster than 1.f / sqrt(a). Relative accuracy better than 2^-11)
static inline Vec8f approx_rsqrt(Vec8f const & a) {
    return Vec8f(approx_rsqrt(a.get_low()), approx_rsqrt(a.get_high()));
}

// Fused multiply and add functions

// Multiply and add
static inline Vec8f mul_add(Vec8f const & a, Vec8f const & b, Vec8f const & c) {
    return Vec8f(mul_add(a.get_low(),b.get_low(),c.get_low()), mul_add(a.get_high(),b.get_high(),c.get_high()));
}

// Multiply and subtract
static inline Vec8f mul_sub(Vec8f const & a, Vec8f const & b, Vec8f const & c) {
    return Vec8f(mul_sub(a.get_low(),b.get_low(),c.get_low()), mul_sub(a.get_high(),b.get_high(),c.get_high()));
}

// Multiply and inverse subtract
static inline Vec8f nmul_add(Vec8f const & a, Vec8f const & b, Vec8f const & c) {
    return Vec8f(nmul_add(a.get_low(),b.get_low(),c.get_low()), nmul_add(a.get_high(),b.get_high(),c.get_high()));
}


// Multiply and subtract with extra precision on the intermediate calculations, 
// even if FMA instructions not supported, using Veltkamp-Dekker split
static inline Vec8f mul_sub_x(Vec8f const & a, Vec8f const & b, Vec8f const & c) {
    return Vec8f(mul_sub_x(a.get_low(),b.get_low(),c.get_low()), mul_sub_x(a.get_high(),b.get_high(),c.get_high()));
}


// Math functions using fast bit manipulation

#ifdef VECTORI256_H  // 256 bit integer vectors are available
// Extract the exponent as an integer
// exponent(a) = floor(log2(abs(a)));
// exponent(1.0f) = 0, exponent(0.0f) = -127, exponent(INF) = +128, exponent(NAN) = +128
static inline Vec8i exponent(Vec8f const & a) {
    return Vec8i(exponent(a.get_low()), exponent(a.get_high()));
}
#endif

// Extract the fraction part of a floating point number
// a = 2^exponent(a) * fraction(a), except for a = 0
// fraction(1.0f) = 1.0f, fraction(5.0f) = 1.25f 
static inline Vec8f fraction(Vec8f const & a) {
    return Vec8f(fraction(a.get_low()), fraction(a.get_high()));
}

#ifdef VECTORI256_H  // 256 bit integer vectors are available
// Fast calculation of pow(2,n) with n integer
// n  =    0 gives 1.0f
// n >=  128 gives +INF
// n <= -127 gives 0.0f
// This function will never produce denormals, and never raise exceptions
static inline Vec8f exp2(Vec8i const & a) {
    return Vec8f(exp2(a.get_low()), exp2(a.get_high()));
}
//static Vec8f exp2(Vec8f const & x); // defined in vectormath_exp.h
#endif // VECTORI256_H



// Categorization functions

// Function sign_bit: gives true for elements that have the sign bit set
// even for -0.0f, -INF and -NAN
// Note that sign_bit(Vec8f(-0.0f)) gives true, while Vec8f(-0.0f) < Vec8f(0.0f) gives false
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec8fb sign_bit(Vec8f const & a) {
    return Vec8fb(sign_bit(a.get_low()), sign_bit(a.get_high()));
}

// Function sign_combine: changes the sign of a when b has the sign bit set
// same as select(sign_bit(b), -a, a)
static inline Vec8f sign_combine(Vec8f const & a, Vec8f const & b) {
    return Vec8f(sign_combine(a.get_low(), b.get_low()), sign_combine(a.get_high(), b.get_high()));
}

// Function is_finite: gives true for elements that are normal, denormal or zero, 
// false for INF and NAN
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec8fb is_finite(Vec8f const & a) {
    return Vec8fb(is_finite(a.get_low()), is_finite(a.get_high()));
}

// Function is_inf: gives true for elements that are +INF or -INF
// false for finite numbers and NAN
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec8fb is_inf(Vec8f const & a) {
    return Vec8fb(is_inf(a.get_low()), is_inf(a.get_high()));
}

// Function is_nan: gives true for elements that are +NAN or -NAN
// false for finite numbers and +/-INF
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec8fb is_nan(Vec8f const & a) {
    return Vec8fb(is_nan(a.get_low()), is_nan(a.get_high()));
}

// Function is_subnormal: gives true for elements that are denormal (subnormal)
// false for finite numbers, zero, NAN and INF
static inline Vec8fb is_subnormal(Vec8f const & a) {
    return Vec8fb(is_subnormal(a.get_low()), is_subnormal(a.get_high()));
}

// Function is_zero_or_subnormal: gives true for elements that are zero or subnormal (denormal)
// false for finite numbers, NAN and INF
static inline Vec8fb is_zero_or_subnormal(Vec8f const & a) {
    return Vec8fb(is_zero_or_subnormal(a.get_low()), is_zero_or_subnormal(a.get_high()));
}

// Function infinite4f: returns a vector where all elements are +INF
static inline Vec8f infinite8f() {
    return constant8f<0x7F800000,0x7F800000,0x7F800000,0x7F800000,0x7F800000,0x7F800000,0x7F800000,0x7F800000>();
}

// Function nan4f: returns a vector where all elements are +NAN (quiet)
static inline Vec8f nan8f(int n = 0x10) {
    return Vec8f(nan4f(n), nan4f(n));
}

// change signs on vectors Vec8f
// Each index i0 - i7 is 1 for changing sign on the corresponding element, 0 for no change
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8f change_sign(Vec8f const & a) {
    if ((i0 | i1 | i2 | i3 | i4 | i5 | i6 | i7) == 0) return a;
    Vec4f lo = change_sign<i0,i1,i2,i3>(a.get_low());
    Vec4f hi = change_sign<i4,i5,i6,i7>(a.get_high());
    return Vec8f(lo, hi);
}


/*****************************************************************************
*
*          Vec2d: Vector of 2 double precision floating point values
*
*****************************************************************************/

class Vec4d : public Vec256de {
public:
    // Default constructor:
    Vec4d() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec4d(double d) {
        y1 = y0 = _mm_set1_pd(d);
    }
    // Constructor to build from all elements:
    Vec4d(double d0, double d1, double d2, double d3) {
        y0 = _mm_setr_pd(d0, d1); 
        y1 = _mm_setr_pd(d2, d3); 
    }
    // Constructor to build from two Vec4f:
    Vec4d(Vec2d const & a0, Vec2d const & a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256de
    Vec4d(Vec256de const & x) {
        y0 = x.get_low();
        y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256de
    Vec4d & operator = (Vec256de const & x) {
        y0 = x.get_low();
        y1 = x.get_high();
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec4d & load(double const * p) {
        y0 = _mm_loadu_pd(p);
        y1 = _mm_loadu_pd(p+2);
        return *this;
    }
    // Member function to load from array, aligned by 32
    // You may use load_a instead of load if you are certain that p points to an address
    // divisible by 32
    Vec4d & load_a(double const * p) {
        y0 = _mm_load_pd(p);
        y1 = _mm_load_pd(p+2);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(double * p) const {
        _mm_storeu_pd(p,   y0);
        _mm_storeu_pd(p+2, y1);
    }
    // Member function to store into array, aligned by 32
    // You may use store_a instead of store if you are certain that p points to an address
    // divisible by 32
    void store_a(double * p) const {
        _mm_store_pd(p,   y0);
        _mm_store_pd(p+2, y1);
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
            y1 = y0 = _mm_setzero_pd();
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
    Vec4d & cutoff(int n) {
        if (uint32_t(n) >= 4) return *this;
        else if (n >= 2) {
            y1 = Vec2d(y1).cutoff(n - 2);
        }
        else {
            y0 = Vec2d(y0).cutoff(n);
            y1 = Vec2d(0.0);
        }
        return *this;
    }    
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec4d const & insert(uint32_t index, double value) {
        if (index < 2) {
            y0 = Vec2d(y0).insert(index, value);
        }
        else {
            y1 = Vec2d(y1).insert(index-2, value);
        }
        return *this;
    }
    // Member function extract a single element from vector
    // Note: This function is inefficient. Use store function if extracting more than one element
    double extract(uint32_t index) const {
        if (index < 2) {
            return Vec2d(y0).extract(index);
        }
        else {
            return Vec2d(y1).extract(index-2);
        }
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    double operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec2d:
    Vec2d get_low() const {
        return y0;
    }
    Vec2d get_high() const {
        return y1;
    }
    static int size () {
        return 2;
    }
};



/*****************************************************************************
*
*          Operators for Vec4d
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec4d operator + (Vec4d const & a, Vec4d const & b) {
    return Vec4d(a.get_low() + b.get_low(), a.get_high() + b.get_high());
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
    return Vec4d(a.get_low() - b.get_low(), a.get_high() - b.get_high());
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
    return Vec4d(-a.get_low(), -a.get_high());
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
    return Vec4d(a.get_low() * b.get_low(), a.get_high() * b.get_high());
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
    return Vec4d(a.get_low() / b.get_low(), a.get_high() / b.get_high());
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
    return Vec4db(a.get_low() == b.get_low(), a.get_high() == b.get_high());
}

// vector operator != : returns true for elements for which a != b
static inline Vec4db operator != (Vec4d const & a, Vec4d const & b) {
    return Vec4db(a.get_low() != b.get_low(), a.get_high() != b.get_high());
}

// vector operator < : returns true for elements for which a < b
static inline Vec4db operator < (Vec4d const & a, Vec4d const & b) {
    return Vec4db(a.get_low() < b.get_low(), a.get_high() < b.get_high());
}

// vector operator <= : returns true for elements for which a <= b
static inline Vec4db operator <= (Vec4d const & a, Vec4d const & b) {
    return Vec4db(a.get_low() <= b.get_low(), a.get_high() <= b.get_high());
}

// vector operator > : returns true for elements for which a > b
static inline Vec4db operator > (Vec4d const & a, Vec4d const & b) {
    return Vec4db(a.get_low() > b.get_low(), a.get_high() > b.get_high());
}

// vector operator >= : returns true for elements for which a >= b
static inline Vec4db operator >= (Vec4d const & a, Vec4d const & b) {
    return Vec4db(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// Bitwise logical operators

// vector operator & : bitwise and
static inline Vec4d operator & (Vec4d const & a, Vec4d const & b) {
    return Vec4d(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}

// vector operator &= : bitwise and
static inline Vec4d & operator &= (Vec4d & a, Vec4d const & b) {
    a = a & b;
    return a;
}

// vector operator & : bitwise and of Vec4d and Vec4db
static inline Vec4d operator & (Vec4d const & a, Vec4db const & b) {
    return Vec4d(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec4d operator & (Vec4db const & a, Vec4d const & b) {
    return Vec4d(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}

// vector operator | : bitwise or
static inline Vec4d operator | (Vec4d const & a, Vec4d const & b) {
    return Vec4d(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}

// vector operator |= : bitwise or
static inline Vec4d & operator |= (Vec4d & a, Vec4d const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec4d operator ^ (Vec4d const & a, Vec4d const & b) {
    return Vec4d(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ^= : bitwise xor
static inline Vec4d & operator ^= (Vec4d & a, Vec4d const & b) {
    a = a ^ b;
    return a;
}

// vector operator ! : logical not. Returns Boolean vector
static inline Vec4db operator ! (Vec4d const & a) {
    return Vec4db(!a.get_low(), !a.get_high());
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
    return Vec4d(select(s.get_low(), a.get_low(), b.get_low()), select(s.get_high(), a.get_high(), b.get_high()));
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec4d if_add (Vec4db const & f, Vec4d const & a, Vec4d const & b) {
    return a + (Vec4d(f) & b);
}

// Conditional multiply: For all vector elements i: result[i] = f[i] ? (a[i] * b[i]) : a[i]
static inline Vec4d if_mul (Vec4db const & f, Vec4d const & a, Vec4d const & b) {
    return a * select(f, b, 1.f);
}

// General arithmetic functions, etc.

// Horizontal add: Calculates the sum of all vector elements.
static inline double horizontal_add (Vec4d const & a) {
    return horizontal_add(a.get_low() + a.get_high());
}

// function max: a > b ? a : b
static inline Vec4d max(Vec4d const & a, Vec4d const & b) {
    return Vec4d(max(a.get_low(),b.get_low()), max(a.get_high(),b.get_high()));
}

// function min: a < b ? a : b
static inline Vec4d min(Vec4d const & a, Vec4d const & b) {
    return Vec4d(min(a.get_low(),b.get_low()), min(a.get_high(),b.get_high()));
}

// function abs: absolute value
// Removes sign bit, even for -0.0f, -INF and -NAN
static inline Vec4d abs(Vec4d const & a) {
    return Vec4d(abs(a.get_low()), abs(a.get_high()));
}

// function sqrt: square root
static inline Vec4d sqrt(Vec4d const & a) {
    return Vec4d(sqrt(a.get_low()), sqrt(a.get_high()));
}

// function square: a * a
static inline Vec4d square(Vec4d const & a) {
    return Vec4d(square(a.get_low()), square(a.get_high()));
}

// pow(Vec4d, int):
// Raise floating point numbers to integer power n
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
    return Vec4d(pow_n<n>(a.get_low()), pow_n<n>(a.get_high()));
}

template <int n>
static inline Vec4d pow(Vec4d const & a, Const_int_t<n>) {
    return pow_n<n>(a);
}


// function round: round to nearest integer (even). (result as double vector)
static inline Vec4d round(Vec4d const & a) {
    return Vec4d(round(a.get_low()), round(a.get_high()));
}

// function truncate: round towards zero. (result as double vector)
static inline Vec4d truncate(Vec4d const & a) {
    return Vec4d(truncate(a.get_low()), truncate(a.get_high()));
}

// function floor: round towards minus infinity. (result as double vector)
static inline Vec4d floor(Vec4d const & a) {
    return Vec4d(floor(a.get_low()), floor(a.get_high()));
}

// function ceil: round towards plus infinity. (result as double vector)
static inline Vec4d ceil(Vec4d const & a) {
    return Vec4d(ceil(a.get_low()), ceil(a.get_high()));
}

// function round_to_int: round to nearest integer (even). (result as integer vector)
static inline Vec4i round_to_int(Vec4d const & a) {
    // Note: assume MXCSR control register is set to rounding
    return round_to_int(a.get_low(), a.get_high());
}

// function truncate_to_int: round towards zero. (result as integer vector)
static inline Vec4i truncate_to_int(Vec4d const & a) {
    return truncate_to_int(a.get_low(), a.get_high());
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
    return Vec4q(truncate_to_int64_limited(a.get_low()), truncate_to_int64_limited(a.get_high()));
}

// function round_to_int64: round to nearest or even. (inefficient)
static inline Vec4q round_to_int64(Vec4d const & a) {
    return truncate_to_int64(round(a));
}

// function round_to_int64_limited: round to nearest integer
// result as 64-bit integer vector, but with limited range
static inline Vec4q round_to_int64_limited(Vec4d const & a) {
    return Vec4q(round_to_int64_limited(a.get_low()), round_to_int64_limited(a.get_high()));
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
    return Vec4d(to_double_limited(x.get_low()),to_double_limited(x.get_high()));
}

#endif  // VECTORI256_H

// function to_double: convert integer vector to double vector
static inline Vec4d to_double(Vec4i const & a) {
    return Vec4d(to_double_low(a), to_double_high(a));
}

// function compress: convert two Vec4d to one Vec8f
static inline Vec8f compress (Vec4d const & low, Vec4d const & high) {
    return Vec8f(compress(low.get_low(), low.get_high()), compress(high.get_low(), high.get_high()));
}

// Function extend_low : convert Vec8f vector elements 0 - 3 to Vec4d
static inline Vec4d extend_low (Vec8f const & a) {
    return Vec4d(extend_low(a.get_low()), extend_high(a.get_low()));
}

// Function extend_high : convert Vec8f vector elements 4 - 7 to Vec4d
static inline Vec4d extend_high (Vec8f const & a) {
    return Vec4d(extend_low(a.get_high()), extend_high(a.get_high()));
}


// Fused multiply and add functions

// Multiply and add
static inline Vec4d mul_add(Vec4d const & a, Vec4d const & b, Vec4d const & c) {
    return Vec4d(mul_add(a.get_low(),b.get_low(),c.get_low()), mul_add(a.get_high(),b.get_high(),c.get_high()));
}

// Multiply and subtract
static inline Vec4d mul_sub(Vec4d const & a, Vec4d const & b, Vec4d const & c) {
    return Vec4d(mul_sub(a.get_low(),b.get_low(),c.get_low()), mul_sub(a.get_high(),b.get_high(),c.get_high()));
}

// Multiply and inverse subtract
static inline Vec4d nmul_add(Vec4d const & a, Vec4d const & b, Vec4d const & c) {
    return Vec4d(nmul_add(a.get_low(),b.get_low(),c.get_low()), nmul_add(a.get_high(),b.get_high(),c.get_high()));
}

// Multiply and subtract with extra precision on the intermediate calculations, 
// even if FMA instructions not supported, using Veltkamp-Dekker split
static inline Vec4d mul_sub_x(Vec4d const & a, Vec4d const & b, Vec4d const & c) {
    return Vec4d(mul_sub_x(a.get_low(),b.get_low(),c.get_low()), mul_sub_x(a.get_high(),b.get_high(),c.get_high()));
}


// Math functions using fast bit manipulation

#ifdef VECTORI256_H  // 256 bit integer vectors are available, AVX2
// Extract the exponent as an integer
// exponent(a) = floor(log2(abs(a)));
// exponent(1.0) = 0, exponent(0.0) = -1023, exponent(INF) = +1024, exponent(NAN) = +1024
static inline Vec4q exponent(Vec4d const & a) {
    return Vec4q(exponent(a.get_low()), exponent(a.get_high()));
}

// Extract the fraction part of a floating point number
// a = 2^exponent(a) * fraction(a), except for a = 0
// fraction(1.0) = 1.0, fraction(5.0) = 1.25 
static inline Vec4d fraction(Vec4d const & a) {
    return Vec4d(fraction(a.get_low()), fraction(a.get_high()));
}

// Fast calculation of pow(2,n) with n integer
// n  =     0 gives 1.0
// n >=  1024 gives +INF
// n <= -1023 gives 0.0
// This function will never produce denormals, and never raise exceptions
static inline Vec4d exp2(Vec4q const & a) {
    return Vec4d(exp2(a.get_low()), exp2(a.get_high()));
}
//static Vec4d exp2(Vec4d const & x); // defined in vectormath_exp.h
#endif


// Categorization functions

// Function sign_bit: gives true for elements that have the sign bit set
// even for -0.0, -INF and -NAN
// Note that sign_bit(Vec4d(-0.0)) gives true, while Vec4d(-0.0) < Vec4d(0.0) gives false
static inline Vec4db sign_bit(Vec4d const & a) {
    return Vec4db(sign_bit(a.get_low()), sign_bit(a.get_high()));
}

// Function sign_combine: changes the sign of a when b has the sign bit set
// same as select(sign_bit(b), -a, a)
static inline Vec4d sign_combine(Vec4d const & a, Vec4d const & b) {
    return Vec4d(sign_combine(a.get_low(), b.get_low()), sign_combine(a.get_high(), b.get_high()));
}

// Function is_finite: gives true for elements that are normal, denormal or zero, 
// false for INF and NAN
static inline Vec4db is_finite(Vec4d const & a) {
    return Vec4db(is_finite(a.get_low()), is_finite(a.get_high()));
}

// Function is_inf: gives true for elements that are +INF or -INF
// false for finite numbers and NAN
static inline Vec4db is_inf(Vec4d const & a) {
    return Vec4db(is_inf(a.get_low()), is_inf(a.get_high()));
}

// Function is_nan: gives true for elements that are +NAN or -NAN
// false for finite numbers and +/-INF
static inline Vec4db is_nan(Vec4d const & a) {
    return Vec4db(is_nan(a.get_low()), is_nan(a.get_high()));
}

// Function is_subnormal: gives true for elements that are denormal (subnormal)
// false for finite numbers, zero, NAN and INF
static inline Vec4db is_subnormal(Vec4d const & a) {
    return Vec4db(is_subnormal(a.get_low()), is_subnormal(a.get_high()));
}

// Function is_zero_or_subnormal: gives true for elements that are zero or subnormal (denormal)
// false for finite numbers, NAN and INF
static inline Vec4db is_zero_or_subnormal(Vec4d const & a) {
    return Vec4db(is_zero_or_subnormal(a.get_low()),is_zero_or_subnormal(a.get_high()));
}

// Function infinite2d: returns a vector where all elements are +INF
static inline Vec4d infinite4d() {
    return Vec4d(infinite2d(), infinite2d());
}

// Function nan2d: returns a vector where all elements are +NAN (quiet)
static inline Vec4d nan4d(int n = 0x10) {
    return Vec4d(nan2d(n), nan2d(n));
}

// change signs on vectors Vec4d
// Each index i0 - i3 is 1 for changing sign on the corresponding element, 0 for no change
template <int i0, int i1, int i2, int i3>
static inline Vec4d change_sign(Vec4d const & a) {
    if ((i0 | i1 | i2 | i3) == 0) return a;
    Vec2d lo = change_sign<i0,i1>(a.get_low());
    Vec2d hi = change_sign<i2,i3>(a.get_high());
    return Vec4d(lo, hi);
}


/*****************************************************************************
*
*          Functions for reinterpretation between vector types
*
*****************************************************************************/

static inline Vec256ie reinterpret_i (Vec256ie const & x) {
    return x;
}

static inline Vec256ie reinterpret_i (Vec256fe  const & x) {
    return Vec256ie(reinterpret_i(x.get_low()), reinterpret_i(x.get_high()));
}

static inline Vec256ie reinterpret_i (Vec256de const & x) {
    return Vec256ie(reinterpret_i(x.get_low()), reinterpret_i(x.get_high()));
}

static inline Vec256fe  reinterpret_f (Vec256ie const & x) {
    return Vec256fe(reinterpret_f(x.get_low()), reinterpret_f(x.get_high()));
}

static inline Vec256fe  reinterpret_f (Vec256fe  const & x) {
    return x;
}

static inline Vec256fe  reinterpret_f (Vec256de const & x) {
    return Vec256fe(reinterpret_f(x.get_low()), reinterpret_f(x.get_high()));
}

static inline Vec256de reinterpret_d (Vec256ie const & x) {
    return Vec256de(reinterpret_d(x.get_low()), reinterpret_d(x.get_high()));
}

static inline Vec256de reinterpret_d (Vec256fe  const & x) {
    return Vec256de(reinterpret_d(x.get_low()), reinterpret_d(x.get_high()));
}

static inline Vec256de reinterpret_d (Vec256de const & x) {
    return x;
}


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
*****************************************************************************/

// permute vector Vec4d
template <int i0, int i1, int i2, int i3>
static inline Vec4d permute4d(Vec4d const & a) {
    return Vec4d(blend2d<i0,i1> (a.get_low(), a.get_high()), 
           blend2d<i2,i3> (a.get_low(), a.get_high()));
}

// helper function used below
template <int n>
static inline Vec2d select4(Vec4d const & a, Vec4d const & b) {
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
    return _mm_setzero_pd();
}

// blend vectors Vec4d
template <int i0, int i1, int i2, int i3>
static inline Vec4d blend4d(Vec4d const & a, Vec4d const & b) {
    const int j0 = i0 >= 0 ? i0/2 : i0;
    const int j1 = i1 >= 0 ? i1/2 : i1;
    const int j2 = i2 >= 0 ? i2/2 : i2;
    const int j3 = i3 >= 0 ? i3/2 : i3;    
    Vec2d x0, x1;

    if (j0 == j1 || i0 < 0 || i1 < 0) {  // both from same
        const int k0 = j0 >= 0 ? j0 : j1;
        x0 = permute2d<i0 & -7, i1 & -7> (select4<k0> (a,b));
    }
    else {
        x0 = blend2d<i0 & -7, (i1 & -7) | 2> (select4<j0>(a,b), select4<j1>(a,b));
    }
    if (j2 == j3 || i2 < 0 || i3 < 0) {  // both from same
        const int k1 = j2 >= 0 ? j2 : j3;
        x1 = permute2d<i2 & -7, i3 & -7> (select4<k1> (a,b));
    }
    else {
        x1 = blend2d<i2 & -7, (i3 & -7) | 2> (select4<j2>(a,b), select4<j3>(a,b));
    }
    return Vec4d(x0,x1);
}

/*****************************************************************************
*
*          Vector Vec8f permute and blend functions
*
*****************************************************************************/

// permute vector Vec8f
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8f permute8f(Vec8f const & a) {
    return Vec8f(blend4f<i0,i1,i2,i3> (a.get_low(), a.get_high()), 
                 blend4f<i4,i5,i6,i7> (a.get_low(), a.get_high()));
}

// helper function used below
template <int n>
static inline Vec4f select4(Vec8f const & a, Vec8f const & b) {
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
    return _mm_setzero_ps();
}

// blend vectors Vec8f
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8f blend8f(Vec8f const & a, Vec8f const & b) {
    const int j0 = i0 >= 0 ? i0/4 : i0;
    const int j1 = i1 >= 0 ? i1/4 : i1;
    const int j2 = i2 >= 0 ? i2/4 : i2;
    const int j3 = i3 >= 0 ? i3/4 : i3;
    const int j4 = i4 >= 0 ? i4/4 : i4;
    const int j5 = i5 >= 0 ? i5/4 : i5;
    const int j6 = i6 >= 0 ? i6/4 : i6;
    const int j7 = i7 >= 0 ? i7/4 : i7;
    Vec4f x0, x1;

    const int r0 = j0 >= 0 ? j0 : j1 >= 0 ? j1 : j2 >= 0 ? j2 : j3;
    const int r1 = j4 >= 0 ? j4 : j5 >= 0 ? j5 : j6 >= 0 ? j6 : j7;
    const int s0 = (j1 >= 0 && j1 != r0) ? j1 : (j2 >= 0 && j2 != r0) ? j2 : j3;
    const int s1 = (j5 >= 0 && j5 != r1) ? j5 : (j6 >= 0 && j6 != r1) ? j6 : j7;

    // Combine all the indexes into a single bitfield, with 4 bits for each
    const int m1 = (i0&0xF) | (i1&0xF)<<4 | (i2&0xF)<<8 | (i3&0xF)<<12 | (i4&0xF)<<16 | (i5&0xF)<<20 | (i6&0xF)<<24 | (i7&0xF)<<28;

    // Mask to zero out negative indexes
    const int mz = (i0<0?0:0xF) | (i1<0?0:0xF)<<4 | (i2<0?0:0xF)<<8 | (i3<0?0:0xF)<<12 | (i4<0?0:0xF)<<16 | (i5<0?0:0xF)<<20 | (i6<0?0:0xF)<<24 | (i7<0?0:0xF)<<28;

    if (r0 < 0) {
        x0 = _mm_setzero_ps();
    }
    else if (((m1 ^ r0*0x4444) & 0xCCCC & mz) == 0) { 
        // i0 - i3 all from same source
        x0 = permute4f<i0 & -13, i1 & -13, i2 & -13, i3 & -13> (select4<r0> (a,b));
    }
    else if ((j2 < 0 || j2 == r0 || j2 == s0) && (j3 < 0 || j3 == r0 || j3 == s0)) { 
        // i0 - i3 all from two sources
        const int k0 =  i0 >= 0 ? i0 & 3 : i0;
        const int k1 = (i1 >= 0 ? i1 & 3 : i1) | (j1 == s0 ? 4 : 0);
        const int k2 = (i2 >= 0 ? i2 & 3 : i2) | (j2 == s0 ? 4 : 0);
        const int k3 = (i3 >= 0 ? i3 & 3 : i3) | (j3 == s0 ? 4 : 0);
        x0 = blend4f<k0,k1,k2,k3> (select4<r0>(a,b), select4<s0>(a,b));
    }
    else {
        // i0 - i3 from three or four different sources
        x0 = blend4f<0,1,6,7> (
             blend4f<i0 & -13, (i1 & -13) | 4, -0x100, -0x100> (select4<j0>(a,b), select4<j1>(a,b)),
             blend4f<-0x100, -0x100, i2 & -13, (i3 & -13) | 4> (select4<j2>(a,b), select4<j3>(a,b)));
    }

    if (r1 < 0) {
        x1 = _mm_setzero_ps();
    }
    else if (((m1 ^ r1*0x44440000u) & 0xCCCC0000 & mz) == 0) { 
        // i4 - i7 all from same source
        x1 = permute4f<i4 & -13, i5 & -13, i6 & -13, i7 & -13> (select4<r1> (a,b));
    }
    else if ((j6 < 0 || j6 == r1 || j6 == s1) && (j7 < 0 || j7 == r1 || j7 == s1)) { 
        // i4 - i7 all from two sources
        const int k4 =  i4 >= 0 ? i4 & 3 : i4;
        const int k5 = (i5 >= 0 ? i5 & 3 : i5) | (j5 == s1 ? 4 : 0);
        const int k6 = (i6 >= 0 ? i6 & 3 : i6) | (j6 == s1 ? 4 : 0);
        const int k7 = (i7 >= 0 ? i7 & 3 : i7) | (j7 == s1 ? 4 : 0);
        x1 = blend4f<k4,k5,k6,k7> (select4<r1>(a,b), select4<s1>(a,b));
    }
    else {
        // i4 - i7 from three or four different sources
        x1 = blend4f<0,1,6,7> (
             blend4f<i4 & -13, (i5 & -13) | 4, -0x100, -0x100> (select4<j4>(a,b), select4<j5>(a,b)),
             blend4f<-0x100, -0x100, i6 & -13, (i7 & -13) | 4> (select4<j6>(a,b), select4<j7>(a,b)));
    }

    return Vec8f(x0,x1);
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
    Vec4f  r0 = lookup8(index.get_low() , table.get_low(), table.get_high());
    Vec4f  r1 = lookup8(index.get_high(), table.get_low(), table.get_high());
    return Vec8f(r0, r1);
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
    if (n <= 8) {
        return lookup8(index, Vec8f().load(table));
    }
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
    return Vec8f(table[index1[0]],table[index1[1]],table[index1[2]],table[index1[3]],
    table[index1[4]],table[index1[5]],table[index1[6]],table[index1[7]]);
}

static inline Vec4d lookup4(Vec4q const & index, Vec4d const & table) {
    Vec2d  r0 = lookup4(index.get_low() , table.get_low(), table.get_high());
    Vec2d  r1 = lookup4(index.get_high(), table.get_low(), table.get_high());
    return Vec4d(r0, r1);
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
    Vec4q index2 = Vec4q(index1);
    return Vec4d(table[index2[0]],table[index2[1]],table[index2[2]],table[index2[3]]);
}
#endif  // VECTORI256_H

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
