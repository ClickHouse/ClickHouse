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
#if    VECTORF512_H != 1
#error Two different versions of vectorf512.h included
#endif
#else
#define VECTORF512_H 1

#include "vectori512e.h"


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
    // Constructor to build from all elements:
    Vec16fb(bool x0, bool x1, bool x2, bool x3, bool x4, bool x5, bool x6, bool x7,
        bool x8, bool x9, bool x10, bool x11, bool x12, bool x13, bool x14, bool x15) :
        Vec16b(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15) {
    }
    // Constructor from Vec16b
    Vec16fb (Vec16b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
    }
    // Constructor from two Vec8fb
    Vec16fb (Vec8fb const & x0, Vec8fb const & x1) {
        z0 = x0;
        z1 = x1;
    }
    // Constructor to broadcast scalar value:
    Vec16fb(bool b) : Vec16b(b) {
    }
    // Assignment operator to broadcast scalar value:
    Vec16fb & operator = (bool b) {
        *this = Vec16b(b);
        return *this;
    }
private: // Prevent constructing from int, etc.
    Vec16fb(int b);
    Vec16fb & operator = (int x);
public:

    // Get low and high half
    Vec8fb get_low() const {
        return reinterpret_f(Vec8i(z0));
    }
    Vec8fb get_high() const {
        return reinterpret_f(Vec8i(z1));
    }
};

// Define operators for Vec16fb

// vector operator & : bitwise and
static inline Vec16fb operator & (Vec16fb const & a, Vec16fb const & b) {
    return Vec16fb(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec16fb operator && (Vec16fb const & a, Vec16fb const & b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec16fb operator | (Vec16fb const & a, Vec16fb const & b) {
    return Vec16fb(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}
static inline Vec16fb operator || (Vec16fb const & a, Vec16fb const & b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec16fb operator ^ (Vec16fb const & a, Vec16fb const & b) {
    return Vec16fb(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ~ : bitwise not
static inline Vec16fb operator ~ (Vec16fb const & a) {
    return Vec16fb(~a.get_low(), ~a.get_high());
}

// vector operator ! : element not
static inline Vec16fb operator ! (Vec16fb const & a) {
    return ~a;
}

// vector operator &= : bitwise and
static inline Vec16fb & operator &= (Vec16fb & a, Vec16fb const & b) {
    a = a & b;
    return a;
}

// vector operator |= : bitwise or
static inline Vec16fb & operator |= (Vec16fb & a, Vec16fb const & b) {
    a = a | b;
    return a;
}

// vector operator ^= : bitwise xor
static inline Vec16fb & operator ^= (Vec16fb & a, Vec16fb const & b) {
    a = a ^ b;
    return a;
}


/*****************************************************************************
*
*          Vec8db: Vector of 8 Booleans for use with Vec8d
*
*****************************************************************************/

class Vec8db : public Vec512b {
public:
    // Default constructor:
    Vec8db () {
    }
    // Constructor to build from all elements:
    Vec8db(bool x0, bool x1, bool x2, bool x3, bool x4, bool x5, bool x6, bool x7) {
        z0 = Vec4qb(x0, x1, x2, x3);
        z1 = Vec4qb(x4, x5, x6, x7);
    }
    // Construct from Vec512b
    Vec8db (Vec512b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
    }
    // Constructor from two Vec4db
    Vec8db (Vec4db const & x0, Vec4db const & x1) {
        z0 = x0;
        z1 = x1;
    }
    // Constructor to broadcast single value:
    Vec8db(bool b) {
        z0 = z1 = Vec8i(-int32_t(b));
    }
    // Assignment operator to broadcast scalar value:
    Vec8db & operator = (bool b) {
        *this = Vec8db(b);
        return *this;
    }
private: 
    // Prevent constructing from int, etc. because of ambiguity
    Vec8db(int b);
    // Prevent assigning int because of ambiguity
    Vec8db & operator = (int x);
public:
    Vec8db & insert (int index, bool a) {
        if (index < 4) {
            z0 = Vec4q(z0).insert(index, -(int64_t)a);
        }
        else {
            z1 = Vec4q(z1).insert(index-4, -(int64_t)a);
        }
        return *this;
    }
    // Member function extract a single element from vector
    bool extract(uint32_t index) const {
        if (index < 4) {
            return Vec4q(z0).extract(index) != 0;
        }
        else {
            return Vec4q(z1).extract(index-4) != 0;
        }
    }
    // Extract a single element. Operator [] can only read an element, not write.
    bool operator [] (uint32_t index) const {
        return extract(index);
    }
    // Get low and high half
    Vec4db get_low() const {
        return reinterpret_d(Vec4q(z0));
    }
    Vec4db get_high() const {
        return reinterpret_d(Vec4q(z1));
    }
    static int size () {
        return 8;
    }
};

// Define operators for Vec8db

// vector operator & : bitwise and
static inline Vec8db operator & (Vec8db const & a, Vec8db const & b) {
    return Vec8db(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec8db operator && (Vec8db const & a, Vec8db const & b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec8db operator | (Vec8db const & a, Vec8db const & b) {
    return Vec8db(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}
static inline Vec8db operator || (Vec8db const & a, Vec8db const & b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec8db operator ^ (Vec8db const & a, Vec8db const & b) {
    return Vec8db(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ~ : bitwise not
static inline Vec8db operator ~ (Vec8db const & a) {
    return Vec8db(~a.get_low(), ~a.get_high());
}

// vector operator ! : element not
static inline Vec8db operator ! (Vec8db const & a) {
    return ~a;
}

// vector operator &= : bitwise and
static inline Vec8db & operator &= (Vec8db & a, Vec8db const & b) {
    a = a & b;
    return a;
}

// vector operator |= : bitwise or
static inline Vec8db & operator |= (Vec8db & a, Vec8db const & b) {
    a = a | b;
    return a;
}

// vector operator ^= : bitwise xor
static inline Vec8db & operator ^= (Vec8db & a, Vec8db const & b) {
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
    Vec8f z0;
    Vec8f z1;
public:
    // Default constructor:
    Vec16f() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec16f(float f) {
        z0 = z1 = Vec8f(f);
    }
    // Constructor to build from all elements:
    Vec16f(float f0, float f1, float f2, float f3, float f4, float f5, float f6, float f7,
    float f8, float f9, float f10, float f11, float f12, float f13, float f14, float f15) {
        z0 = Vec8f(f0, f1, f2, f3, f4, f5, f6, f7);
        z1 = Vec8f(f8, f9, f10, f11, f12, f13, f14, f15);
    }
    // Constructor to build from two Vec8f:
    Vec16f(Vec8f const & a0, Vec8f const & a1) {
        z0 = a0;
        z1 = a1;
    }
    // split into two halves
    Vec8f get_low() const {
        return z0;
    }
    Vec8f get_high() const {
        return z1;
    }
    // Member function to load from array (unaligned)
    Vec16f & load(float const * p) {
        z0 = Vec8f().load(p);
        z1 = Vec8f().load(p+8);
        return *this;
    }
    // Member function to load from array, aligned by 64
    // You may use load_a instead of load if you are certain that p points to an address
    // divisible by 64.
    Vec16f & load_a(float const * p) {
        z0 = Vec8f().load_a(p);
        z1 = Vec8f().load_a(p+8);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(float * p) const {
        Vec8f(z0).store(p);
        Vec8f(z1).store(p+8);
    }
    // Member function to store into array, aligned by 64
    // You may use store_a instead of store if you are certain that p points to an address
    // divisible by 64.
    void store_a(float * p) const {
        Vec8f(z0).store_a(p);
        Vec8f(z1).store_a(p+8);
    }
    // Partial load. Load n elements and set the rest to 0
    Vec16f & load_partial(int n, float const * p) {
        if (n < 8) {
            z0 = Vec8f().load_partial(n, p);
            z1 = Vec8f(0.f);
        }
        else {
            z0 = Vec8f().load(p);
            z1 = Vec8f().load_partial(n-8, p + 8);
        }
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, float * p) const {
        if (n < 8) {
            Vec8f(z0).store_partial(n, p);
        }
        else {
            Vec8f(z0).store(p);
            Vec8f(z1).store_partial(n-8, p+8);
        }
    }
    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec16f & cutoff(int n) {
        if (n < 8) {
            z0 = Vec8f(z0).cutoff(n);
            z1 = Vec8f(0.f);
        }
        else {
            z1 = Vec8f(z1).cutoff(n-8);
        }
        return *this;
    }
    // Member function to change a single element in vector
    Vec16f const & insert(uint32_t index, float value) {
        if (index < 8) {
            z0 = Vec8f(z0).insert(index, value);
        }
        else {
            z1 = Vec8f(z1).insert(index-8, value);
        }
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
    return Vec16f(a.get_low() + b.get_low(), a.get_high() + b.get_high());
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
    return Vec16f(a.get_low() - b.get_low(), a.get_high() - b.get_high());
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
    return Vec16f(-a.get_low(), -a.get_high());
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
    return Vec16f(a.get_low() * b.get_low(), a.get_high() * b.get_high());
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
    return Vec16f(a.get_low() / b.get_low(), a.get_high() / b.get_high());
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
    return Vec16fb(a.get_low() == b.get_low(), a.get_high() == b.get_high());
}

// vector operator != : returns true for elements for which a != b
static inline Vec16fb operator != (Vec16f const & a, Vec16f const & b) {
    return Vec16fb(a.get_low() != b.get_low(), a.get_high() != b.get_high());
}

// vector operator < : returns true for elements for which a < b
static inline Vec16fb operator < (Vec16f const & a, Vec16f const & b) {
    return Vec16fb(a.get_low() < b.get_low(), a.get_high() < b.get_high());
}

// vector operator <= : returns true for elements for which a <= b
static inline Vec16fb operator <= (Vec16f const & a, Vec16f const & b) {
    return Vec16fb(a.get_low() <= b.get_low(), a.get_high() <= b.get_high());
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
    return Vec16f(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}

// vector operator &= : bitwise and
static inline Vec16f & operator &= (Vec16f & a, Vec16f const & b) {
    a = a & b;
    return a;
}

// vector operator & : bitwise and of Vec16f and Vec16fb
static inline Vec16f operator & (Vec16f const & a, Vec16fb const & b) {
    return Vec16f(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec16f operator & (Vec16fb const & a, Vec16f const & b) {
    return b & a;
}

// vector operator | : bitwise or
static inline Vec16f operator | (Vec16f const & a, Vec16f const & b) {
    return Vec16f(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}

// vector operator |= : bitwise or
static inline Vec16f & operator |= (Vec16f & a, Vec16f const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec16f operator ^ (Vec16f const & a, Vec16f const & b) {
    return Vec16f(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ^= : bitwise xor
static inline Vec16f & operator ^= (Vec16f & a, Vec16f const & b) {
    a = a ^ b;
    return a;
}

// vector operator ! : logical not. Returns Boolean vector
static inline Vec16fb operator ! (Vec16f const & a) {
    return Vec16fb(!a.get_low(), !a.get_high());
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
    return Vec16f(select(s.get_low(), a.get_low(), b.get_low()), select(s.get_high(), a.get_high(), b.get_high()));
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec16f if_add (Vec16fb const & f, Vec16f const & a, Vec16f const & b) {
    return Vec16f(if_add(f.get_low(), a.get_low(), b.get_low()), if_add(f.get_high(), a.get_high(), b.get_high()));
}

// Conditional multiply: For all vector elements i: result[i] = f[i] ? (a[i] * b[i]) : a[i]
static inline Vec16f if_mul (Vec16fb const & f, Vec16f const & a, Vec16f const & b) {
    return Vec16f(if_mul(f.get_low(), a.get_low(), b.get_low()), if_mul(f.get_high(), a.get_high(), b.get_high()));
}

// Horizontal add: Calculates the sum of all vector elements.
static inline float horizontal_add (Vec16f const & a) {
    return horizontal_add(a.get_low() + a.get_high());
}

// function max: a > b ? a : b
static inline Vec16f max(Vec16f const & a, Vec16f const & b) {
    return Vec16f(max(a.get_low(), b.get_low()), max(a.get_high(), b.get_high()));
}

// function min: a < b ? a : b
static inline Vec16f min(Vec16f const & a, Vec16f const & b) {
    return Vec16f(min(a.get_low(), b.get_low()), min(a.get_high(), b.get_high()));
}

// function abs: absolute value
// Removes sign bit, even for -0.0f, -INF and -NAN
static inline Vec16f abs(Vec16f const & a) {
    return Vec16f(abs(a.get_low()), abs(a.get_high()));
}

// function sqrt: square root
static inline Vec16f sqrt(Vec16f const & a) {
    return Vec16f(sqrt(a.get_low()), sqrt(a.get_high()));
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
    return Vec16f(round(a.get_low()), round(a.get_high()));
}

// function truncate: round towards zero. (result as float vector)
static inline Vec16f truncate(Vec16f const & a) {
    return Vec16f(truncate(a.get_low()), truncate(a.get_high()));
}

// function floor: round towards minus infinity. (result as float vector)
static inline Vec16f floor(Vec16f const & a) {
    return Vec16f(floor(a.get_low()), floor(a.get_high()));
}

// function ceil: round towards plus infinity. (result as float vector)
static inline Vec16f ceil(Vec16f const & a) {
    return Vec16f(ceil(a.get_low()), ceil(a.get_high()));
}

// function round_to_int: round to nearest integer (even). (result as integer vector)
static inline Vec16i round_to_int(Vec16f const & a) {
    return Vec16i(round_to_int(a.get_low()), round_to_int(a.get_high()));
}

// function truncate_to_int: round towards zero. (result as integer vector)
static inline Vec16i truncate_to_int(Vec16f const & a) {
    return Vec16i(truncate_to_int(a.get_low()), truncate_to_int(a.get_high()));
}

// function to_float: convert integer vector to float vector
static inline Vec16f to_float(Vec16i const & a) {
    return Vec16f(to_float(a.get_low()), to_float(a.get_high()));
}


// Approximate math functions

// approximate reciprocal (Faster than 1.f / a.
// relative accuracy better than 2^-11 without AVX512, 2^-14 with AVX512)
static inline Vec16f approx_recipr(Vec16f const & a) {
    return Vec16f(approx_recipr(a.get_low()), approx_recipr(a.get_high()));
}

// approximate reciprocal squareroot (Faster than 1.f / sqrt(a).
// Relative accuracy better than 2^-11 without AVX512, 2^-14 with AVX512)
static inline Vec16f approx_rsqrt(Vec16f const & a) {
    return Vec16f(approx_rsqrt(a.get_low()), approx_rsqrt(a.get_high()));
}


// Fused multiply and add functions

// Multiply and add
static inline Vec16f mul_add(Vec16f const & a, Vec16f const & b, Vec16f const & c) {
    return Vec16f(mul_add(a.get_low(), b.get_low(), c.get_low()), mul_add(a.get_high(), b.get_high(), c.get_high()));
}

// Multiply and subtract
static inline Vec16f mul_sub(Vec16f const & a, Vec16f const & b, Vec16f const & c) {
    return Vec16f(mul_sub(a.get_low(), b.get_low(), c.get_low()), mul_sub(a.get_high(), b.get_high(), c.get_high()));
}

// Multiply and inverse subtract
static inline Vec16f nmul_add(Vec16f const & a, Vec16f const & b, Vec16f const & c) {
    return Vec16f(nmul_add(a.get_low(), b.get_low(), c.get_low()), nmul_add(a.get_high(), b.get_high(), c.get_high()));
}

// Multiply and subtract with extra precision on the intermediate calculations, 
// even if FMA instructions not supported, using Veltkamp-Dekker split
static inline Vec16f mul_sub_x(Vec16f const & a, Vec16f const & b, Vec16f const & c) {
    return Vec16f(mul_sub_x(a.get_low(), b.get_low(), c.get_low()), mul_sub_x(a.get_high(), b.get_high(), c.get_high()));
}


// Math functions using fast bit manipulation

// Extract the exponent as an integer
// exponent(a) = floor(log2(abs(a)));
// exponent(1.0f) = 0, exponent(0.0f) = -127, exponent(INF) = +128, exponent(NAN) = +128
static inline Vec16i exponent(Vec16f const & a) {
    return Vec16i(exponent(a.get_low()), exponent(a.get_high()));
}

// Extract the fraction part of a floating point number
// a = 2^exponent(a) * fraction(a), except for a = 0
// fraction(1.0f) = 1.0f, fraction(5.0f) = 1.25f 
static inline Vec16f fraction(Vec16f const & a) {
    return Vec16f(fraction(a.get_low()), fraction(a.get_high()));
}

// Fast calculation of pow(2,n) with n integer
// n  =    0 gives 1.0f
// n >=  128 gives +INF
// n <= -127 gives 0.0f
// This function will never produce denormals, and never raise exceptions
static inline Vec16f exp2(Vec16i const & n) {
    return Vec16f(exp2(n.get_low()), exp2(n.get_high()));
}
//static Vec16f exp2(Vec16f const & x); // defined in vectormath_exp.h


// Categorization functions

// Function sign_bit: gives true for elements that have the sign bit set
// even for -0.0f, -INF and -NAN
// Note that sign_bit(Vec16f(-0.0f)) gives true, while Vec16f(-0.0f) < Vec16f(0.0f) gives false
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec16fb sign_bit(Vec16f const & a) {
    return Vec16fb(sign_bit(a.get_low()), sign_bit(a.get_high()));
}

// Function sign_combine: changes the sign of a when b has the sign bit set
// same as select(sign_bit(b), -a, a)
static inline Vec16f sign_combine(Vec16f const & a, Vec16f const & b) {
    return Vec16f(sign_combine(a.get_low(), b.get_low()), sign_combine(a.get_high(), b.get_high()));
}

// Function is_finite: gives true for elements that are normal, denormal or zero, 
// false for INF and NAN
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec16fb is_finite(Vec16f const & a) {
    return Vec16fb(is_finite(a.get_low()), is_finite(a.get_high()));
}

// Function is_inf: gives true for elements that are +INF or -INF
// false for finite numbers and NAN
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec16fb is_inf(Vec16f const & a) {
    return Vec16fb(is_inf(a.get_low()), is_inf(a.get_high()));
}

// Function is_nan: gives true for elements that are +NAN or -NAN
// false for finite numbers and +/-INF
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec16fb is_nan(Vec16f const & a) {
    return Vec16fb(is_nan(a.get_low()), is_nan(a.get_high()));
}

// Function is_subnormal: gives true for elements that are denormal (subnormal)
// false for finite numbers, zero, NAN and INF
static inline Vec16fb is_subnormal(Vec16f const & a) {
    return Vec16fb(is_subnormal(a.get_low()), is_subnormal(a.get_high()));
}

// Function is_zero_or_subnormal: gives true for elements that are zero or subnormal (denormal)
// false for finite numbers, NAN and INF
static inline Vec16fb is_zero_or_subnormal(Vec16f const & a) {
    return Vec16fb(is_zero_or_subnormal(a.get_low()), is_zero_or_subnormal(a.get_high()));
}

// Function infinite4f: returns a vector where all elements are +INF
static inline Vec16f infinite16f() {
    Vec8f inf = infinite8f();
    return Vec16f(inf, inf);
}

// Function nan4f: returns a vector where all elements are +NAN (quiet)
static inline Vec16f nan16f(int n = 0x10) {
    Vec8f nan = nan8f(n);
    return Vec16f(nan, nan);
}

// change signs on vectors Vec16f
// Each index i0 - i7 is 1 for changing sign on the corresponding element, 0 for no change
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7, int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
static inline Vec16f change_sign(Vec16f const & a) {
    return Vec16f(change_sign<i0,i1,i2,i3,i4,i5,i6,i7>(a.get_low()), change_sign<i8,i9,i10,i11,i12,i13,i14,i15>(a.get_high()));
}



/*****************************************************************************
*
*          Vec8d: Vector of 8 double precision floating point values
*
*****************************************************************************/

class Vec8d {
protected:
    Vec4d z0;
    Vec4d z1;
public:
    // Default constructor:
    Vec8d() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec8d(double d) {
        z0 = z1 = Vec4d(d);
    }
    // Constructor to build from all elements:
    Vec8d(double d0, double d1, double d2, double d3, double d4, double d5, double d6, double d7) {
        z0 = Vec4d(d0, d1, d2, d3);
        z1 = Vec4d(d4, d5, d6, d7);
    }
    // Constructor to build from two Vec4d:
    Vec8d(Vec4d const & a0, Vec4d const & a1) {
        z0 = a0;
        z1 = a1;
    }
    // Member function to load from array (unaligned)
    Vec8d & load(double const * p) {
        z0.load(p);
        z1.load(p+4);
        return *this;
    }
    // Member function to load from array, aligned by 64
    // You may use load_a instead of load if you are certain that p points to an address
    // divisible by 64
    Vec8d & load_a(double const * p) {
        z0.load_a(p);
        z1.load_a(p+4);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(double * p) const {
        z0.store(p);
        z1.store(p+4);
    }
    // Member function to store into array, aligned by 64
    // You may use store_a instead of store if you are certain that p points to an address
    // divisible by 64
    void store_a(double * p) const {
        z0.store_a(p);
        z1.store_a(p+4);
    }
    // Partial load. Load n elements and set the rest to 0
    Vec8d & load_partial(int n, double const * p) {
        if (n < 4) {
            z0.load_partial(n, p);
            z1 = Vec4d(0.);
        }
        else {
            z0.load(p);
            z1.load_partial(n-4, p+4);
        }
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, double * p) const {
        if (n < 4) {
            z0.store_partial(n, p);
        }
        else {
            z0.store(p);
            z1.store_partial(n-4, p+4);
        }
    }
    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec8d & cutoff(int n) {
        if (n < 4) {
            z0.cutoff(n);
            z1 = Vec4d(0.);
        }
        else {
            z1.cutoff(n-4);
        }
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec8d const & insert(uint32_t index, double value) {
        if (index < 4) {
            z0.insert(index, value);
        }
        else {
            z1.insert(index-4, value);
        }
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
        return z0;
    }
    Vec4d get_high() const {
        return z1;
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
    return Vec8d(a.get_low() + b.get_low(), a.get_high() + b.get_high());
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
    return Vec8d(a.get_low() - b.get_low(), a.get_high() - b.get_high());
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
    return Vec8d(-a.get_low(), -a.get_high());
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
    return Vec8d(a.get_low() * b.get_low(), a.get_high() * b.get_high());
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
    return Vec8d(a.get_low() / b.get_low(), a.get_high() / b.get_high());
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
    return Vec8db(a.get_low() == b.get_low(), a.get_high() == b.get_high());
}

// vector operator != : returns true for elements for which a != b
static inline Vec8db operator != (Vec8d const & a, Vec8d const & b) {
    return Vec8db(a.get_low() != b.get_low(), a.get_high() != b.get_high());
}

// vector operator < : returns true for elements for which a < b
static inline Vec8db operator < (Vec8d const & a, Vec8d const & b) {
    return Vec8db(a.get_low() < b.get_low(), a.get_high() < b.get_high());
}

// vector operator <= : returns true for elements for which a <= b
static inline Vec8db operator <= (Vec8d const & a, Vec8d const & b) {
    return Vec8db(a.get_low() <= b.get_low(), a.get_high() <= b.get_high());
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
    return Vec8d(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}

// vector operator &= : bitwise and
static inline Vec8d & operator &= (Vec8d & a, Vec8d const & b) {
    a = a & b;
    return a;
}

// vector operator & : bitwise and of Vec8d and Vec8db
static inline Vec8d operator & (Vec8d const & a, Vec8db const & b) {
    return Vec8d(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}

static inline Vec8d operator & (Vec8db const & a, Vec8d const & b) {
    return b & a;
}

// vector operator | : bitwise or
static inline Vec8d operator | (Vec8d const & a, Vec8d const & b) {
    return Vec8d(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}

// vector operator |= : bitwise or
static inline Vec8d & operator |= (Vec8d & a, Vec8d const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec8d operator ^ (Vec8d const & a, Vec8d const & b) {
    return Vec8d(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ^= : bitwise xor
static inline Vec8d & operator ^= (Vec8d & a, Vec8d const & b) {
    a = a ^ b;
    return a;
}

// vector operator ! : logical not. Returns Boolean vector
static inline Vec8db operator ! (Vec8d const & a) {
    return Vec8db(!a.get_low(), !a.get_high());
}


/*****************************************************************************
*
*          Functions for Vec8d
*
*****************************************************************************/

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 2; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec8d select (Vec8db const & s, Vec8d const & a, Vec8d const & b) {
    return Vec8d(select(s.get_low(), a.get_low(), b.get_low()), select(s.get_high(), a.get_high(), b.get_high()));
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec8d if_add (Vec8db const & f, Vec8d const & a, Vec8d const & b) {
    return Vec8d(if_add(f.get_low(), a.get_low(), b.get_low()), if_add(f.get_high(), a.get_high(), b.get_high()));
}

// Conditional multiply: For all vector elements i: result[i] = f[i] ? (a[i] * b[i]) : a[i]
static inline Vec8d if_mul (Vec8db const & f, Vec8d const & a, Vec8d const & b) {
    return Vec8d(if_mul(f.get_low(), a.get_low(), b.get_low()), if_mul(f.get_high(), a.get_high(), b.get_high()));
}


// General arithmetic functions, etc.

// Horizontal add: Calculates the sum of all vector elements.
static inline double horizontal_add (Vec8d const & a) {
    return horizontal_add(a.get_low() + a.get_high());
}

// function max: a > b ? a : b
static inline Vec8d max(Vec8d const & a, Vec8d const & b) {
    return Vec8d(max(a.get_low(), b.get_low()), max(a.get_high(), b.get_high()));
}

// function min: a < b ? a : b
static inline Vec8d min(Vec8d const & a, Vec8d const & b) {
    return Vec8d(min(a.get_low(), b.get_low()), min(a.get_high(), b.get_high()));
}

// function abs: absolute value
// Removes sign bit, even for -0.0f, -INF and -NAN
static inline Vec8d abs(Vec8d const & a) {
    return Vec8d(abs(a.get_low()), abs(a.get_high()));
}

// function sqrt: square root
static inline Vec8d sqrt(Vec8d const & a) {
    return Vec8d(sqrt(a.get_low()), sqrt(a.get_high()));
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
    return Vec8d(round(a.get_low()), round(a.get_high()));
}

// function truncate: round towards zero. (result as double vector)
static inline Vec8d truncate(Vec8d const & a) {
    return Vec8d(truncate(a.get_low()), truncate(a.get_high()));
}

// function floor: round towards minus infinity. (result as double vector)
static inline Vec8d floor(Vec8d const & a) {
    return Vec8d(floor(a.get_low()), floor(a.get_high()));
}

// function ceil: round towards plus infinity. (result as double vector)
static inline Vec8d ceil(Vec8d const & a) {
    return Vec8d(ceil(a.get_low()), ceil(a.get_high()));
}

// function round_to_int: round to nearest integer (even). (result as integer vector)
static inline Vec8i round_to_int(Vec8d const & a) {
    // Note: assume MXCSR control register is set to rounding
    return Vec8i(round_to_int(a.get_low()), round_to_int(a.get_high()));
}

// function truncate_to_int: round towards zero. (result as integer vector)
static inline Vec8i truncate_to_int(Vec8d const & a) {
    return Vec8i(truncate_to_int(a.get_low()), truncate_to_int(a.get_high()));
}

// function truncate_to_int64: round towards zero. (inefficient)
static inline Vec8q truncate_to_int64(Vec8d const & a) {
    return Vec8q(truncate_to_int64(a.get_low()), truncate_to_int64(a.get_high()));
}

// function truncate_to_int64_limited: round towards zero.
// result as 64-bit integer vector, but with limited range
static inline Vec8q truncate_to_int64_limited(Vec8d const & a) {
    // Note: assume MXCSR control register is set to rounding
    return Vec8q(truncate_to_int64_limited(a.get_low()), truncate_to_int64_limited(a.get_high()));
} 

// function round_to_int64: round to nearest or even. (inefficient)
static inline Vec8q round_to_int64(Vec8d const & a) {
    return Vec8q(round_to_int64(a.get_low()), round_to_int64(a.get_high()));
}

// function round_to_int64_limited: round to nearest integer (even)
// result as 64-bit integer vector, but with limited range
static inline Vec8q round_to_int64_limited(Vec8d const & a) {
    // Note: assume MXCSR control register is set to rounding
    return Vec8q(round_to_int64_limited(a.get_low()), round_to_int64_limited(a.get_high()));
}

// function to_double: convert integer vector elements to double vector (inefficient)
static inline Vec8d to_double(Vec8q const & a) {
    return Vec8d(to_double(a.get_low()), to_double(a.get_high()));
}

// function to_double_limited: convert integer vector elements to double vector
// limited to abs(x) < 2^31
static inline Vec8d to_double_limited(Vec8q const & a) {
    return Vec8d(to_double_limited(a.get_low()), to_double_limited(a.get_high()));
}

// function to_double: convert integer vector to double vector
static inline Vec8d to_double(Vec8i const & a) {
    return Vec8d(to_double(a.get_low()), to_double(a.get_high()));
}

// function compress: convert two Vec8d to one Vec16f
static inline Vec16f compress (Vec8d const & low, Vec8d const & high) {
    return Vec16f(compress(low.get_low(), low.get_high()), compress(high.get_low(), high.get_high()));
}

// Function extend_low : convert Vec16f vector elements 0 - 3 to Vec8d
static inline Vec8d extend_low(Vec16f const & a) {
    return Vec8d(extend_low(a.get_low()), extend_high(a.get_low()));
}

// Function extend_high : convert Vec16f vector elements 4 - 7 to Vec8d
static inline Vec8d extend_high (Vec16f const & a) {
    return Vec8d(extend_low(a.get_high()), extend_high(a.get_high()));
}


// Fused multiply and add functions

// Multiply and add
static inline Vec8d mul_add(Vec8d const & a, Vec8d const & b, Vec8d const & c) {
    return Vec8d(mul_add(a.get_low(), b.get_low(), c.get_low()), mul_add(a.get_high(), b.get_high(), c.get_high()));
}

// Multiply and subtract
static inline Vec8d mul_sub(Vec8d const & a, Vec8d const & b, Vec8d const & c) {
    return Vec8d(mul_sub(a.get_low(), b.get_low(), c.get_low()), mul_sub(a.get_high(), b.get_high(), c.get_high()));
}

// Multiply and inverse subtract
static inline Vec8d nmul_add(Vec8d const & a, Vec8d const & b, Vec8d const & c) {
    return Vec8d(nmul_add(a.get_low(), b.get_low(), c.get_low()), nmul_add(a.get_high(), b.get_high(), c.get_high()));
}

// Multiply and subtract with extra precision on the intermediate calculations, 
// even if FMA instructions not supported, using Veltkamp-Dekker split
static inline Vec8d mul_sub_x(Vec8d const & a, Vec8d const & b, Vec8d const & c) {
    return Vec8d(mul_sub_x(a.get_low(), b.get_low(), c.get_low()), mul_sub_x(a.get_high(), b.get_high(), c.get_high()));
}


// Math functions using fast bit manipulation

// Extract the exponent as an integer
// exponent(a) = floor(log2(abs(a)));
// exponent(1.0) = 0, exponent(0.0) = -1023, exponent(INF) = +1024, exponent(NAN) = +1024
static inline Vec8q exponent(Vec8d const & a) {
    return Vec8q(exponent(a.get_low()), exponent(a.get_high()));
}

// Extract the fraction part of a floating point number
// a = 2^exponent(a) * fraction(a), except for a = 0
// fraction(1.0) = 1.0, fraction(5.0) = 1.25 
static inline Vec8d fraction(Vec8d const & a) {
    return Vec8d(fraction(a.get_low()), fraction(a.get_high()));
}

// Fast calculation of pow(2,n) with n integer
// n  =     0 gives 1.0
// n >=  1024 gives +INF
// n <= -1023 gives 0.0
// This function will never produce denormals, and never raise exceptions
static inline Vec8d exp2(Vec8q const & n) {
    return Vec8d(exp2(n.get_low()), exp2(n.get_high()));
}
//static Vec8d exp2(Vec8d const & x); // defined in vectormath_exp.h


// Categorization functions

// Function sign_bit: gives true for elements that have the sign bit set
// even for -0.0, -INF and -NAN
// Note that sign_bit(Vec8d(-0.0)) gives true, while Vec8d(-0.0) < Vec8d(0.0) gives false
static inline Vec8db sign_bit(Vec8d const & a) {
    return Vec8db(sign_bit(a.get_low()), sign_bit(a.get_high()));
}

// Function sign_combine: changes the sign of a when b has the sign bit set
// same as select(sign_bit(b), -a, a)
static inline Vec8d sign_combine(Vec8d const & a, Vec8d const & b) {
    return Vec8d(sign_combine(a.get_low(), b.get_low()), sign_combine(a.get_high(), b.get_high()));
}

// Function is_finite: gives true for elements that are normal, denormal or zero, 
// false for INF and NAN
static inline Vec8db is_finite(Vec8d const & a) {
    return Vec8db(is_finite(a.get_low()), is_finite(a.get_high()));
}

// Function is_inf: gives true for elements that are +INF or -INF
// false for finite numbers and NAN
static inline Vec8db is_inf(Vec8d const & a) {
    return Vec8db(is_inf(a.get_low()), is_inf(a.get_high()));
}

// Function is_nan: gives true for elements that are +NAN or -NAN
// false for finite numbers and +/-INF
static inline Vec8db is_nan(Vec8d const & a) {
    return Vec8db(is_nan(a.get_low()), is_nan(a.get_high()));
}

// Function is_subnormal: gives true for elements that are denormal (subnormal)
// false for finite numbers, zero, NAN and INF
static inline Vec8db is_subnormal(Vec8d const & a) {
    return Vec8db(is_subnormal(a.get_low()), is_subnormal(a.get_high()));
}

// Function is_zero_or_subnormal: gives true for elements that are zero or subnormal (denormal)
// false for finite numbers, NAN and INF
static inline Vec8db is_zero_or_subnormal(Vec8d const & a) {
    return Vec8db(is_zero_or_subnormal(a.get_low()), is_zero_or_subnormal(a.get_high()));
}

// Function infinite2d: returns a vector where all elements are +INF
static inline Vec8d infinite8d() {
    Vec4d inf = infinite4d();
    return Vec8d(inf, inf);
}

// Function nan8d: returns a vector where all elements are +NAN (quiet NAN)
static inline Vec8d nan8d(int n = 0x10) {
    Vec4d nan = nan4d(n);
    return Vec8d(nan, nan);
}

// change signs on vectors Vec8d
// Each index i0 - i3 is 1 for changing sign on the corresponding element, 0 for no change
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8d change_sign(Vec8d const & a) {
    return Vec8d(change_sign<i0,i1,i2,i3>(a.get_low()), change_sign<i4,i5,i6,i7>(a.get_high()));
}


/*****************************************************************************
*
*          Functions for reinterpretation between vector types
*
*****************************************************************************/

static inline Vec512ie reinterpret_i (Vec512ie const & x) {
    return x;
}

static inline Vec512ie reinterpret_i (Vec16f  const & x) {
    return Vec512ie(reinterpret_i(x.get_low()), reinterpret_i(x.get_high()));
}

static inline Vec512ie reinterpret_i (Vec8d const & x) {
    return Vec512ie(reinterpret_i(x.get_low()), reinterpret_i(x.get_high()));
}

static inline Vec16f  reinterpret_f (Vec512ie const & x) {
    return Vec16f(Vec8f(reinterpret_f(x.get_low())), Vec8f(reinterpret_f(x.get_high())));
}

static inline Vec16f  reinterpret_f (Vec16f  const & x) {
    return x;
}

static inline Vec16f  reinterpret_f (Vec8d const & x) {
    return Vec16f(Vec8f(reinterpret_f(x.get_low())), Vec8f(reinterpret_f(x.get_high())));
}

static inline Vec8d reinterpret_d (Vec512ie const & x) {
    return Vec8d(Vec4d(reinterpret_d(x.get_low())), Vec4d(reinterpret_d(x.get_high())));
}

static inline Vec8d reinterpret_d (Vec16f  const & x) {
    return Vec8d(Vec4d(reinterpret_d(x.get_low())), Vec4d(reinterpret_d(x.get_high())));
}

static inline Vec8d reinterpret_d (Vec8d const & x) {
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

// Permute vector of 8 double
// Index -1 gives 0, index -256 means don't care.
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8d permute8d(Vec8d const & a) {
    return Vec8d(blend4d<i0,i1,i2,i3> (a.get_low(), a.get_high()),
                 blend4d<i4,i5,i6,i7> (a.get_low(), a.get_high()));
}

// Permute vector of 16 float
// Index -1 gives 0, index -256 means don't care.
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7, int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
static inline Vec16f permute16f(Vec16f const & a) {
    return Vec16f(blend8f<i0,i1,i2 ,i3 ,i4 ,i5 ,i6 ,i7 > (a.get_low(), a.get_high()),
                  blend8f<i8,i9,i10,i11,i12,i13,i14,i15> (a.get_low(), a.get_high()));
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

// helper function used below
template <int n>
static inline Vec4d select4(Vec8d const & a, Vec8d const & b) {
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
    return Vec4d(0.);
}

// blend vectors Vec8d
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7> 
static inline Vec8d blend8d(Vec8d const & a, Vec8d const & b) {  
    const int j0 = i0 >= 0 ? i0/4 : i0;
    const int j1 = i1 >= 0 ? i1/4 : i1;
    const int j2 = i2 >= 0 ? i2/4 : i2;
    const int j3 = i3 >= 0 ? i3/4 : i3;
    const int j4 = i4 >= 0 ? i4/4 : i4;
    const int j5 = i5 >= 0 ? i5/4 : i5;
    const int j6 = i6 >= 0 ? i6/4 : i6;
    const int j7 = i7 >= 0 ? i7/4 : i7;
    Vec4d x0, x1;

    const int r0 = j0 >= 0 ? j0 : j1 >= 0 ? j1 : j2 >= 0 ? j2 : j3;
    const int r1 = j4 >= 0 ? j4 : j5 >= 0 ? j5 : j6 >= 0 ? j6 : j7;
    const int s0 = (j1 >= 0 && j1 != r0) ? j1 : (j2 >= 0 && j2 != r0) ? j2 : j3;
    const int s1 = (j5 >= 0 && j5 != r1) ? j5 : (j6 >= 0 && j6 != r1) ? j6 : j7;

    // Combine all the indexes into a single bitfield, with 4 bits for each
    const int m1 = (i0&0xF) | (i1&0xF)<<4 | (i2&0xF)<<8 | (i3&0xF)<<12 | (i4&0xF)<<16 | (i5&0xF)<<20 | (i6&0xF)<<24 | (i7&0xF)<<28;

    // Mask to zero out negative indexes
    const int mz = (i0<0?0:0xF) | (i1<0?0:0xF)<<4 | (i2<0?0:0xF)<<8 | (i3<0?0:0xF)<<12 | (i4<0?0:0xF)<<16 | (i5<0?0:0xF)<<20 | (i6<0?0:0xF)<<24 | (i7<0?0:0xF)<<28;

    if (r0 < 0) {
        x0 = Vec4d(0.);
    }
    else if (((m1 ^ r0*0x4444) & 0xCCCC & mz) == 0) { 
        // i0 - i3 all from same source
        x0 = permute4d<i0 & -13, i1 & -13, i2 & -13, i3 & -13> (select4<r0> (a,b));
    }
    else if ((j2 < 0 || j2 == r0 || j2 == s0) && (j3 < 0 || j3 == r0 || j3 == s0)) { 
        // i0 - i3 all from two sources
        const int k0 =  i0 >= 0 ? i0 & 3 : i0;
        const int k1 = (i1 >= 0 ? i1 & 3 : i1) | (j1 == s0 ? 4 : 0);
        const int k2 = (i2 >= 0 ? i2 & 3 : i2) | (j2 == s0 ? 4 : 0);
        const int k3 = (i3 >= 0 ? i3 & 3 : i3) | (j3 == s0 ? 4 : 0);
        x0 = blend4d<k0,k1,k2,k3> (select4<r0>(a,b), select4<s0>(a,b));
    }
    else {
        // i0 - i3 from three or four different sources
        x0 = blend4d<0,1,6,7> (
             blend4d<i0 & -13, (i1 & -13) | 4, -0x100, -0x100> (select4<j0>(a,b), select4<j1>(a,b)),
             blend4d<-0x100, -0x100, i2 & -13, (i3 & -13) | 4> (select4<j2>(a,b), select4<j3>(a,b)));
    }

    if (r1 < 0) {
        x1 = Vec4d(0.);
    }
    else if (((m1 ^ uint32_t(r1)*0x44440000u) & 0xCCCC0000 & mz) == 0) { 
        // i4 - i7 all from same source
        x1 = permute4d<i4 & -13, i5 & -13, i6 & -13, i7 & -13> (select4<r1> (a,b));
    }
    else if ((j6 < 0 || j6 == r1 || j6 == s1) && (j7 < 0 || j7 == r1 || j7 == s1)) { 
        // i4 - i7 all from two sources
        const int k4 =  i4 >= 0 ? i4 & 3 : i4;
        const int k5 = (i5 >= 0 ? i5 & 3 : i5) | (j5 == s1 ? 4 : 0);
        const int k6 = (i6 >= 0 ? i6 & 3 : i6) | (j6 == s1 ? 4 : 0);
        const int k7 = (i7 >= 0 ? i7 & 3 : i7) | (j7 == s1 ? 4 : 0);
        x1 = blend4d<k4,k5,k6,k7> (select4<r1>(a,b), select4<s1>(a,b));
    }
    else {
        // i4 - i7 from three or four different sources
        x1 = blend4d<0,1,6,7> (
             blend4d<i4 & -13, (i5 & -13) | 4, -0x100, -0x100> (select4<j4>(a,b), select4<j5>(a,b)),
             blend4d<-0x100, -0x100, i6 & -13, (i7 & -13) | 4> (select4<j6>(a,b), select4<j7>(a,b)));
    }

    return Vec8d(x0,x1);
}

// helper function used below
template <int n>
static inline Vec8f select4(Vec16f const & a, Vec16f const & b) {
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
    return Vec8f(0.f);
}

template <int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15 > 
static inline Vec16f blend16f(Vec16f const & a, Vec16f const & b) {

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

    Vec8f x0, x1;

    const int r0 = j0 >= 0 ? j0 : j1 >= 0 ? j1 : j2  >= 0 ? j2  : j3  >= 0 ? j3  : j4  >= 0 ? j4  : j5  >= 0 ? j5  : j6  >= 0 ? j6  : j7;
    const int r1 = j8 >= 0 ? j8 : j9 >= 0 ? j9 : j10 >= 0 ? j10 : j11 >= 0 ? j11 : j12 >= 0 ? j12 : j13 >= 0 ? j13 : j14 >= 0 ? j14 : j15;
    const int s0 = (j1 >= 0 && j1 != r0) ? j1 : (j2 >= 0 && j2 != r0) ? j2  : (j3 >= 0 && j3 != r0) ? j3 : (j4 >= 0 && j4 != r0) ? j4 : (j5 >= 0 && j5 != r0) ? j5 : (j6 >= 0 && j6 != r0) ? j6 : j7;
    const int s1 = (j9 >= 0 && j9 != r1) ? j9 : (j10>= 0 && j10!= r1) ? j10 : (j11>= 0 && j11!= r1) ? j11: (j12>= 0 && j12!= r1) ? j12: (j13>= 0 && j13!= r1) ? j13: (j14>= 0 && j14!= r1) ? j14: j15;

    if (r0 < 0) {
        x0 = Vec8f(0.f);
    }
    else if (r0 == s0) {
        // i0 - i7 all from same source
        x0 = permute8f<i0&-25, i1&-25, i2&-25, i3&-25, i4&-25, i5&-25, i6&-25, i7&-25> (select4<r0> (a,b));
    }
    else if ((j2<0||j2==r0||j2==s0) && (j3<0||j3==r0||j3==s0) && (j4<0||j4==r0||j4==s0) && (j5<0||j5==r0||j5==s0) && (j6<0||j6==r0||j6==s0) && (j7<0||j7==r0||j7==s0)) {
        // i0 - i7 all from two sources
        const int k0 =  i0 >= 0 ? (i0 & 7) : i0;
        const int k1 = (i1 >= 0 ? (i1 & 7) : i1) | (j1 == s0 ? 8 : 0);
        const int k2 = (i2 >= 0 ? (i2 & 7) : i2) | (j2 == s0 ? 8 : 0);
        const int k3 = (i3 >= 0 ? (i3 & 7) : i3) | (j3 == s0 ? 8 : 0);
        const int k4 = (i4 >= 0 ? (i4 & 7) : i4) | (j4 == s0 ? 8 : 0);
        const int k5 = (i5 >= 0 ? (i5 & 7) : i5) | (j5 == s0 ? 8 : 0);
        const int k6 = (i6 >= 0 ? (i6 & 7) : i6) | (j6 == s0 ? 8 : 0);
        const int k7 = (i7 >= 0 ? (i7 & 7) : i7) | (j7 == s0 ? 8 : 0);
        x0 = blend8f<k0,k1,k2,k3,k4,k5,k6,k7> (select4<r0>(a,b), select4<s0>(a,b));
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
        x0 = blend8f<n0, n1, n2, n3, n4, n5, n6, n7> (
             blend8f< j0   & 2 ? -256 : i0 &15,  j1   & 2 ? -256 : i1 &15,  j2   & 2 ? -256 : i2 &15,  j3   & 2 ? -256 : i3 &15,  j4   & 2 ? -256 : i4 &15,  j5   & 2 ? -256 : i5 &15,  j6   & 2 ? -256 : i6 &15,  j7   & 2 ? -256 : i7 &15> (a.get_low(),a.get_high()),
             blend8f<(j0^2)& 6 ? -256 : i0 &15, (j1^2)& 6 ? -256 : i1 &15, (j2^2)& 6 ? -256 : i2 &15, (j3^2)& 6 ? -256 : i3 &15, (j4^2)& 6 ? -256 : i4 &15, (j5^2)& 6 ? -256 : i5 &15, (j6^2)& 6 ? -256 : i6 &15, (j7^2)& 6 ? -256 : i7 &15> (b.get_low(),b.get_high()));
    }

    if (r1 < 0) {
        x1 = Vec8f(0.f);
    }
    else if (r1 == s1) {
        // i8 - i15 all from same source
        x1 = permute8f<i8&-25, i9&-25, i10&-25, i11&-25, i12&-25, i13&-25, i14&-25, i15&-25> (select4<r1> (a,b));
    }
    else if ((j10<0||j10==r1||j10==s1) && (j11<0||j11==r1||j11==s1) && (j12<0||j12==r1||j12==s1) && (j13<0||j13==r1||j13==s1) && (j14<0||j14==r1||j14==s1) && (j15<0||j15==r1||j15==s1)) {
        // i8 - i15 all from two sources
        const int k8 =  i8 >= 0 ? (i8 & 7) : i8;
        const int k9 = (i9 >= 0 ? (i9 & 7) : i9 ) | (j9 == s1 ? 8 : 0);
        const int k10= (i10>= 0 ? (i10& 7) : i10) | (j10== s1 ? 8 : 0);
        const int k11= (i11>= 0 ? (i11& 7) : i11) | (j11== s1 ? 8 : 0);
        const int k12= (i12>= 0 ? (i12& 7) : i12) | (j12== s1 ? 8 : 0);
        const int k13= (i13>= 0 ? (i13& 7) : i13) | (j13== s1 ? 8 : 0);
        const int k14= (i14>= 0 ? (i14& 7) : i14) | (j14== s1 ? 8 : 0);
        const int k15= (i15>= 0 ? (i15& 7) : i15) | (j15== s1 ? 8 : 0);
        x1 = blend8f<k8,k9,k10,k11,k12,k13,k14,k15> (select4<r1>(a,b), select4<s1>(a,b));
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
        x1 = blend8f<n8, n9, n10, n11, n12, n13, n14, n15> (
             blend8f< j8   & 2 ? -256 : i8 &15,  j9   & 2 ? -256 : i9 &15,  j10   & 2 ? -256 : i10 &15,  j11   & 2 ? -256 : i11 &15,  j12   & 2 ? -256 : i12 &15,  j13   & 2 ? -256 : i13 &15,  j14   & 2 ? -256 : i14 &15,  j15   & 2 ? -256 : i15 &15> (a.get_low(),a.get_high()),
             blend8f<(j8^2)& 6 ? -256 : i8 &15, (j9^2)& 6 ? -256 : i9 &15, (j10^2)& 6 ? -256 : i10 &15, (j11^2)& 6 ? -256 : i11 &15, (j12^2)& 6 ? -256 : i12 &15, (j13^2)& 6 ? -256 : i13 &15, (j14^2)& 6 ? -256 : i14 &15, (j15^2)& 6 ? -256 : i15 &15> (b.get_low(),b.get_high()));
    }
    return Vec16f(x0,x1);
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
    float tab[16];
    table.store(tab);
    Vec8f t0 = lookup<16>(index.get_low(), tab);
    Vec8f t1 = lookup<16>(index.get_high(), tab);
    return Vec16f(t0, t1);
}

template <int n>
static inline Vec16f lookup(Vec16i const & index, float const * table) {
    if (n <=  0) return 0;
    if (n <=  8) {
        Vec8f table1 = Vec8f().load(table);        
        return Vec16f(       
            lookup8 (index.get_low(),  table1),
            lookup8 (index.get_high(), table1));
    }
    if (n <= 16) return lookup16(index, Vec16f().load(table));
    // n > 16. Limit index
    Vec16ui i1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        i1 = Vec16ui(index) & (n-1);
    }
    else {
        // n is not a power of 2, limit to n-1
        i1 = min(Vec16ui(index), n-1);
    }
    float const * t = table;
    return Vec16f(t[i1[0]],t[i1[1]],t[i1[2]],t[i1[3]],t[i1[4]],t[i1[5]],t[i1[6]],t[i1[7]],
        t[i1[8]],t[i1[9]],t[i1[10]],t[i1[11]],t[i1[12]],t[i1[13]],t[i1[14]],t[i1[15]]);
}


static inline Vec8d lookup8(Vec8q const & index, Vec8d const & table) {
    double tab[8];
    table.store(tab);
    Vec4d t0 = lookup<8>(index.get_low(), tab);
    Vec4d t1 = lookup<8>(index.get_high(), tab);
    return Vec8d(t0, t1);
} 

template <int n>
static inline Vec8d lookup(Vec8q const & index, double const * table) {
    if (n <= 0) return 0;
    if (n <= 4) {
        Vec4d table1 = Vec4d().load(table);        
        return Vec8d(       
            lookup4 (index.get_low(),  table1),
            lookup4 (index.get_high(), table1));
    }
    if (n <= 8) {
        return lookup8(index, Vec8d().load(table));
    }
    // n > 8. Limit index
    Vec8uq i1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        i1 = Vec8uq(index) & (n-1);
    }
    else {
        // n is not a power of 2, limit to n-1
        i1 = min(Vec8uq(index), n-1);
    }
    double const * t = table;
    return Vec8d(t[i1[0]],t[i1[1]],t[i1[2]],t[i1[3]],t[i1[4]],t[i1[5]],t[i1[6]],t[i1[7]]);
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
    // use lookup function
    return lookup<imax+1>(Vec16i(i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15), (const float *)a);
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
    // use lookup function
    return lookup<imax+1>(Vec8q(i0,i1,i2,i3,i4,i5,i6,i7), (const double *)a);
}


/*****************************************************************************
*
*          Horizontal scan functions
*
*****************************************************************************/

// Get index to the first element that is true. Return -1 if all are false
static inline int horizontal_find_first(Vec16fb const & x) {
    int a1 = horizontal_find_first(x.get_low());
    if (a1 >= 0) return a1;
    int a2 = horizontal_find_first(x.get_high());
    if (a2 < 0) return a2;
    return a2 + 8;
}

static inline int horizontal_find_first(Vec8db const & x) {
    int a1 = horizontal_find_first(x.get_low());
    if (a1 >= 0) return a1;
    int a2 = horizontal_find_first(x.get_high());
    if (a2 < 0) return a2;
    return a2 + 4;
}

// count the number of true elements
static inline uint32_t horizontal_count(Vec16fb const & x) {
    return horizontal_count(x.get_low()) + horizontal_count(x.get_high());
}

static inline uint32_t horizontal_count(Vec8db const & x) {
    return horizontal_count(x.get_low()) + horizontal_count(x.get_high());
}


/*****************************************************************************
*
*          Boolean <-> bitfield conversion functions
*
*****************************************************************************/

// to_bits: convert boolean vector to integer bitfield
static inline uint16_t to_bits(Vec16fb const & x) {
    return to_bits(Vec16ib(x));
}

// to_Vec16fb: convert integer bitfield to boolean vector
static inline Vec16fb to_Vec16fb(uint16_t x) {
    return Vec16fb(to_Vec16ib(x));
}

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec8db const & x) {
    return to_bits(Vec8qb(x));
}

// to_Vec8db: convert integer bitfield to boolean vector
static inline Vec8db to_Vec8db(uint8_t x) {
    return Vec8db(to_Vec8qb(x));
}

#endif // VECTORF512_H
