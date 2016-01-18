/****************************  vectori512e.h   *******************************
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
#if    VECTORI512_H != 1
#error Two different versions of vectori512.h included
#endif
#else
#define VECTORI512_H  1


/*****************************************************************************
*
*          base class Vec512ie
*
*****************************************************************************/
// base class to replace _mm512i when AVX512 is not supported
class Vec512ie {
protected:
    Vec256b z0;                         // low half
    Vec256b z1;                         // high half
public:
    Vec512ie(void) {};                  // default constructor
    Vec512ie(Vec8i const & x0, Vec8i const & x1) {      // constructor to build from two Vec8i
        z0 = x0;  z1 = x1;
    }
    Vec8i get_low() const {            // get low half
        return Vec8i(z0);
    }
    Vec8i get_high() const {           // get high half
        return Vec8i(z1);
    }
};


/*****************************************************************************
*
*          Vector of 512 1-bit unsigned integers or Booleans
*
*****************************************************************************/
class Vec512b : public Vec512ie {
public:
    // Default constructor:
    Vec512b() {
    }
    // Constructor to build from two Vec256b:
    Vec512b(Vec256b const & a0, Vec256b const & a1) {
        z0 = a0;  z1 = a1;
    }
    // Constructor to convert from type Vec512ie
    Vec512b(Vec512ie const & x) {
        z0 = x.get_low();  z1 = x.get_high();
    }
    // Assignment operator to convert from type Vec512ie
    Vec512b & operator = (Vec512ie const & x) {
        z0 = x.get_low();  z1 = x.get_high();
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec512b & load(void const * p) {
        z0 = Vec8i().load(p);
        z1 = Vec8i().load((int32_t*)p+8);
        return *this;
    }
    // Member function to load from array, aligned by 64
    Vec512b & load_a(void const * p) {
        z0 = Vec8i().load_a(p);
        z1 = Vec8i().load_a((int32_t*)p+8);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(void * p) const {
        Vec8i(z0).store(p);
        Vec8i(z1).store((int32_t*)p+8);
    }
    // Member function to store into array, aligned by 64
    void store_a(void * p) const {
        Vec8i(z0).store_a(p);
        Vec8i(z1).store_a((int32_t*)p+8);
    }
    // Member function to change a single bit
    // Note: This function is inefficient. Use load function if changing more than one bit
    Vec512b const & set_bit(uint32_t index, int value) {
        if (index < 256) {
            z0 = Vec8i(z0).set_bit(index, value);
        }
        else {
            z1 = Vec8i(z1).set_bit(index-256, value);
        }
        return *this;
    }
    // Member function to get a single bit
    // Note: This function is inefficient. Use store function if reading more than one bit
    int get_bit(uint32_t index) const {
        if (index < 256) {
            return Vec8i(z0).get_bit(index);
        }
        else {
            return Vec8i(z1).get_bit(index-256);
        }
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    bool operator [] (uint32_t index) const {
        return get_bit(index) != 0;
    }
    // Member functions to split into two Vec128b:
    Vec256b get_low() const {
        return z0;
    }
    Vec256b get_high() const {
        return z1;
    }
    static int size () {
        return 512;
    }
};

// Define operators for this class

// vector operator & : bitwise and
static inline Vec512b operator & (Vec512b const & a, Vec512b const & b) {
    return Vec512b(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec512b operator && (Vec512b const & a, Vec512b const & b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec512b operator | (Vec512b const & a, Vec512b const & b) {
    return Vec512b(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}
static inline Vec512b operator || (Vec512b const & a, Vec512b const & b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec512b operator ^ (Vec512b const & a, Vec512b const & b) {
    return Vec512b(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ~ : bitwise not
static inline Vec512b operator ~ (Vec512b const & a) {
    return Vec512b(~a.get_low(), ~a.get_high());
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
    return Vec512b(andnot(a.get_low(), b.get_low()), andnot(a.get_high(), b.get_high()));
}



/*****************************************************************************
*
*          Generate compile-time constant vector
*
*****************************************************************************/
// Generate a constant vector of 8 integers stored in memory.
// Can be converted to any integer vector type
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7, int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
static inline Vec512ie constant16i() {
    static const union {
        int32_t i[16];
        Vec256b y[2];  // note: requires C++0x or later. Use option -std=c++0x
    } u = {{i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15}};
    return Vec512ie(u.y[0], u.y[1]);
}


/*****************************************************************************
*
*          Boolean vector base classes for AVX512
*
*****************************************************************************/

class Vec16b : public Vec512b {
public:
    // Default constructor:
    Vec16b () {
    }
    // Constructor to build from all elements:
    Vec16b(bool b0, bool b1, bool b2, bool b3, bool b4, bool b5, bool b6, bool b7, 
    bool b8, bool b9, bool b10, bool b11, bool b12, bool b13, bool b14, bool b15) {
        *this = Vec512b(Vec8i(-(int)b0, -(int)b1, -(int)b2, -(int)b3, -(int)b4, -(int)b5, -(int)b6, -(int)b7), Vec8i(-(int)b8, -(int)b9, -(int)b10, -(int)b11, -(int)b12, -(int)b13, -(int)b14, -(int)b15));
    }
    // Constructor to convert from type Vec512b
    Vec16b (Vec512b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
    }
    // Constructor to make from two halves
    Vec16b (Vec8ib const & x0, Vec8ib const & x1) {
        z0 = x0;
        z1 = x1;
    }        
    // Constructor to make from two halves
    Vec16b (Vec8i const & x0, Vec8i const & x1) {
        z0 = x0;
        z1 = x1;
    }        
    // Constructor to broadcast single value:
    Vec16b(bool b) {
        z0 = z1 = Vec8i(-int32_t(b));
    }
    // Assignment operator to broadcast scalar value:
    Vec16b & operator = (bool b) {
        z0 = z1 = Vec8i(-int32_t(b));
        return *this;
    }
private: 
    // Prevent constructing from int, etc. because of ambiguity
    Vec16b(int b);
    // Prevent assigning int because of ambiguity
    Vec16b & operator = (int x);
public:
    // split into two halves
    Vec8ib get_low() const {
        return Vec8ib(z0);
    }
    Vec8ib get_high() const {
        return Vec8ib(z1);
    }
    // Assignment operator to convert from type Vec512b
    Vec16b & operator = (Vec512b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec16b const & insert(uint32_t index, bool value) {
        if (index < 8) {
            z0 = Vec8ib(z0).insert(index, value);
        }
        else {
            z1 = Vec8ib(z1).insert(index-8, value);
        }
        return *this;
    }
    // Member function extract a single element from vector
    bool extract(uint32_t index) const {
        if (index < 8) {
            return Vec8ib(z0).extract(index);
        }
        else {
            return Vec8ib(z1).extract(index-8);
        }
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
static inline Vec16b operator & (Vec16b const & a, Vec16b const & b) {
    return Vec16b(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec16b operator && (Vec16b const & a, Vec16b const & b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec16b operator | (Vec16b const & a, Vec16b const & b) {
    return Vec16b(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}
static inline Vec16b operator || (Vec16b const & a, Vec16b const & b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec16b operator ^ (Vec16b const & a, Vec16b const & b) {
    return Vec16b(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ~ : bitwise not
static inline Vec16b operator ~ (Vec16b const & a) {
    return Vec16b(~(a.get_low()), ~(a.get_high()));
}

// vector operator ! : element not
static inline Vec16b operator ! (Vec16b const & a) {
    return ~a;
}

// vector operator &= : bitwise and
static inline Vec16b & operator &= (Vec16b & a, Vec16b const & b) {
    a = a & b;
    return a;
}

// vector operator |= : bitwise or
static inline Vec16b & operator |= (Vec16b & a, Vec16b const & b) {
    a = a | b;
    return a;
}

// vector operator ^= : bitwise xor
static inline Vec16b & operator ^= (Vec16b & a, Vec16b const & b) {
    a = a ^ b;
    return a;
}

/*****************************************************************************
*
*          Functions for boolean vectors
*
*****************************************************************************/

// function andnot: a & ~ b
static inline Vec16b andnot (Vec16b const & a, Vec16b const & b) {
    return Vec16b(Vec8ib(andnot(a.get_low(),b.get_low())), Vec8ib(andnot(a.get_high(),b.get_high())));
}

// horizontal_and. Returns true if all bits are 1
static inline bool horizontal_and (Vec16b const & a) {
    return  horizontal_and(a.get_low() & a.get_high());
}

// horizontal_or. Returns true if at least one bit is 1
static inline bool horizontal_or (Vec16b const & a) {
    return  horizontal_or(a.get_low() | a.get_high());
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
    Vec16ib (Vec16b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
    }
    // Constructor to build from all elements:
    Vec16ib(bool x0, bool x1, bool x2, bool x3, bool x4, bool x5, bool x6, bool x7,
        bool x8, bool x9, bool x10, bool x11, bool x12, bool x13, bool x14, bool x15) {
        z0 = Vec8ib(x0, x1, x2, x3, x4, x5, x6, x7);
        z1 = Vec8ib(x8, x9, x10, x11, x12, x13, x14, x15);
    }
    // Constructor to convert from type Vec512b
    Vec16ib (Vec512b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
    }
    // Construct from two halves
    Vec16ib (Vec8ib const & x0, Vec8ib const & x1) {
        z0 = x0;
        z1 = x1;
    }
    // Assignment operator to convert from type Vec512b
    Vec16ib & operator = (Vec512b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
        return *this;
    }
    // Constructor to broadcast scalar value:
    Vec16ib(bool b) : Vec16b(b) {
    }
    // Assignment operator to broadcast scalar value:
    Vec16ib & operator = (bool b) {
        *this = Vec16b(b);
        return *this;
    }
private: // Prevent constructing from int, etc.
    Vec16ib(int b);
    Vec16ib & operator = (int x);
public:
};

// Define operators for Vec16ib

// vector operator & : bitwise and
static inline Vec16ib operator & (Vec16ib const & a, Vec16ib const & b) {
    return Vec16b(a) & Vec16b(b);
}
static inline Vec16ib operator && (Vec16ib const & a, Vec16ib const & b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec16ib operator | (Vec16ib const & a, Vec16ib const & b) {
    return Vec16b(a) | Vec16b(b);
}
static inline Vec16ib operator || (Vec16ib const & a, Vec16ib const & b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec16ib operator ^ (Vec16ib const & a, Vec16ib const & b) {
    return Vec16b(a) ^ Vec16b(b);
}

// vector operator ~ : bitwise not
static inline Vec16ib operator ~ (Vec16ib const & a) {
    return ~Vec16b(a);
}

// vector operator ! : element not
static inline Vec16ib operator ! (Vec16ib const & a) {
    return ~a;
}

// vector operator &= : bitwise and
static inline Vec16ib & operator &= (Vec16ib & a, Vec16ib const & b) {
    a = a & b;
    return a;
}

// vector operator |= : bitwise or
static inline Vec16ib & operator |= (Vec16ib & a, Vec16ib const & b) {
    a = a | b;
    return a;
}

// vector operator ^= : bitwise xor
static inline Vec16ib & operator ^= (Vec16ib & a, Vec16ib const & b) {
    a = a ^ b;
    return a;
}

// vector function andnot
static inline Vec16ib andnot (Vec16ib const & a, Vec16ib const & b) {
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
    Vec8b (Vec16b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
    }
    // Constructor to convert from type Vec512b
    Vec8b (Vec512b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
    }
    // construct from two halves
    Vec8b (Vec4qb const & x0, Vec4qb const & x1) {
        z0 = x0;
        z1 = x1;
    }
    // Constructor to broadcast single value:
    Vec8b(bool b) {
        z0 = z1 = Vec8i(-int32_t(b));
    }
    // Assignment operator to broadcast scalar value:
    Vec8b & operator = (bool b) {
        z0 = z1 = Vec8i(-int32_t(b));
        return *this;
    }
private: 
    // Prevent constructing from int, etc. because of ambiguity
    Vec8b(int b);
    // Prevent assigning int because of ambiguity
    Vec8b & operator = (int x);
public:
    // split into two halves
    Vec4qb get_low() const {
        return Vec4qb(z0);
    }
    Vec4qb get_high() const {
        return Vec4qb(z1);
    }
    // Assignment operator to convert from type Vec512b
    Vec8b & operator = (Vec512b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec8b const & insert(uint32_t index, bool value) {
        if (index < 4) {
            z0 = Vec4qb(z0).insert(index, value);
        }
        else {
            z1 = Vec4qb(z1).insert(index-4, value);
        }
        return *this;
    }
    bool extract(uint32_t index) const {
        if (index < 4) {
            return Vec4qb(Vec4q(z0)).extract(index);
        }
        else {
            return Vec4qb(Vec4q(z1)).extract(index-4);
        }
    }
    bool operator [] (uint32_t index) const {
        return extract(index);
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
    Vec8qb (Vec16b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
    }
    // Constructor to build from all elements:
    Vec8qb(bool x0, bool x1, bool x2, bool x3, bool x4, bool x5, bool x6, bool x7) {
        z0 = Vec4qb(x0, x1, x2, x3);
        z1 = Vec4qb(x4, x5, x6, x7);
    }
    // Constructor to convert from type Vec512b
    Vec8qb (Vec512b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
    }
    // construct from two halves
    Vec8qb (Vec4qb const & x0, Vec4qb const & x1) {
        z0 = x0;
        z1 = x1;
    }
    // Assignment operator to convert from type Vec512b
    Vec8qb & operator = (Vec512b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
        return *this;
    }
    // Constructor to broadcast single value:
    Vec8qb(bool b) : Vec8b(b) {
    }
    // Assignment operator to broadcast scalar value:
    Vec8qb & operator = (bool b) {
        *this = Vec8b(b);
        return *this;
    }
private: 
    // Prevent constructing from int, etc. because of ambiguity
    Vec8qb(int b);
    // Prevent assigning int because of ambiguity
    Vec8qb & operator = (int x);
public:
};

// Define operators for Vec8qb

// vector operator & : bitwise and
static inline Vec8qb operator & (Vec8qb const & a, Vec8qb const & b) {
    return Vec16b(a) & Vec16b(b);
}
static inline Vec8qb operator && (Vec8qb const & a, Vec8qb const & b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec8qb operator | (Vec8qb const & a, Vec8qb const & b) {
    return Vec16b(a) | Vec16b(b);
}
static inline Vec8qb operator || (Vec8qb const & a, Vec8qb const & b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec8qb operator ^ (Vec8qb const & a, Vec8qb const & b) {
    return Vec16b(a) ^ Vec16b(b);
}

// vector operator ~ : bitwise not
static inline Vec8qb operator ~ (Vec8qb const & a) {
    return ~Vec16b(a);
}

// vector operator ! : element not
static inline Vec8qb operator ! (Vec8qb const & a) {
    return ~a;
}

// vector operator &= : bitwise and
static inline Vec8qb & operator &= (Vec8qb & a, Vec8qb const & b) {
    a = a & b;
    return a;
}

// vector operator |= : bitwise or
static inline Vec8qb & operator |= (Vec8qb & a, Vec8qb const & b) {
    a = a | b;
    return a;
}

// vector operator ^= : bitwise xor
static inline Vec8qb & operator ^= (Vec8qb & a, Vec8qb const & b) {
    a = a ^ b;
    return a;
}

// vector function andnot
static inline Vec8qb andnot (Vec8qb const & a, Vec8qb const & b) {
    return Vec8qb(andnot(Vec16b(a), Vec16b(b)));
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
    }
    // Constructor to broadcast the same value into all elements:
    Vec16i(int i) {
        z0 = z1 = Vec8i(i);
    }
    // Constructor to build from all elements:
    Vec16i(int32_t i0, int32_t i1, int32_t i2, int32_t i3, int32_t i4, int32_t i5, int32_t i6, int32_t i7,
    int32_t i8, int32_t i9, int32_t i10, int32_t i11, int32_t i12, int32_t i13, int32_t i14, int32_t i15) {
        z0 = Vec8i(i0, i1, i2, i3, i4, i5, i6, i7);
        z1 = Vec8i(i8, i9, i10, i11, i12, i13, i14, i15);
    }
    // Constructor to build from two Vec8i:
    Vec16i(Vec8i const & a0, Vec8i const & a1) {
        *this = Vec512b(a0, a1);
    }
    // Constructor to convert from type Vec512b
    Vec16i(Vec512b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
    }
    // Assignment operator to convert from type Vec512b
    Vec16i & operator = (Vec512b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec16i & load(void const * p) {
        Vec512b::load(p);
        return *this;
    }
    // Member function to load from array, aligned by 64
    Vec16i & load_a(void const * p) {
        Vec512b::load_a(p);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec16i & load_partial(int n, void const * p) {
        if (n < 8) {
            z0 = Vec8i().load_partial(n, p);
            z1 = Vec8i(0);
        }
        else {
            z0 = Vec8i().load(p);
            z1 = Vec8i().load_partial(n - 8, (int32_t *)p + 8);
        }
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
        if (n < 8) {
            Vec8i(get_low()).store_partial(n, p);
        }
        else {
            Vec8i(get_low()).store(p);
            Vec8i(get_high()).store_partial(n - 8, (int32_t *)p + 8);
        }
    }
    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec16i & cutoff(int n) {
        if (n < 8) {
            z0 = Vec8i(z0).cutoff(n);
            z1 = Vec8i(0);
        }
        else {
            z1 = Vec8i(z1).cutoff(n - 8);
        }
        return *this;
    }
    // Member function to change a single element in vector
    Vec16i const & insert(uint32_t index, int32_t value) {
        if (index < 8) {
            z0 = Vec8i(z0).insert(index, value);
        }
        else {
            z1 = Vec8i(z1).insert(index - 8, value);
        }
        return *this;
    }
    // Member function extract a single element from vector
    int32_t extract(uint32_t index) const {
        if (index < 8) {
            return Vec8i(z0).extract(index);
        }
        else {
            return Vec8i(z1).extract(index - 8);
        }
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int32_t operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec8i:
    Vec8i get_low() const {
        return Vec8i(z0);
    }
    Vec8i get_high() const {
        return Vec8i(z1);
    }
    static int size () {
        return 16;
    }
};


// Define operators for Vec16i

// vector operator + : add element by element
static inline Vec16i operator + (Vec16i const & a, Vec16i const & b) {
    return Vec16i(a.get_low() + b.get_low(), a.get_high() + b.get_high());
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
    return Vec16i(a.get_low() - b.get_low(), a.get_high() - b.get_high());
}

// vector operator - : unary minus
static inline Vec16i operator - (Vec16i const & a) {
    return Vec16i(-a.get_low(), -a.get_high());
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
    return Vec16i(a.get_low() * b.get_low(), a.get_high() * b.get_high());
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
    return Vec16i(a.get_low() << b, a.get_high() << b);
}

// vector operator <<= : shift left
static inline Vec16i & operator <<= (Vec16i & a, int32_t b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec16i operator >> (Vec16i const & a, int32_t b) {
    return Vec16i(a.get_low() >> b, a.get_high() >> b);
}

// vector operator >>= : shift right arithmetic
static inline Vec16i & operator >>= (Vec16i & a, int32_t b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec16ib operator == (Vec16i const & a, Vec16i const & b) {
    return Vec16ib(a.get_low() == b.get_low(), a.get_high() == b.get_high());
}

// vector operator != : returns true for elements for which a != b
static inline Vec16ib operator != (Vec16i const & a, Vec16i const & b) {
    return Vec16ib(a.get_low() != b.get_low(), a.get_high() != b.get_high());
}
  
// vector operator > : returns true for elements for which a > b
static inline Vec16ib operator > (Vec16i const & a, Vec16i const & b) {
    return Vec16ib(a.get_low() > b.get_low(), a.get_high() > b.get_high());
}

// vector operator < : returns true for elements for which a < b
static inline Vec16ib operator < (Vec16i const & a, Vec16i const & b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec16ib operator >= (Vec16i const & a, Vec16i const & b) {
    return Vec16ib(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec16ib operator <= (Vec16i const & a, Vec16i const & b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec16i operator & (Vec16i const & a, Vec16i const & b) {
    return Vec16i(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}

// vector operator &= : bitwise and
static inline Vec16i & operator &= (Vec16i & a, Vec16i const & b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec16i operator | (Vec16i const & a, Vec16i const & b) {
    return Vec16i(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}

// vector operator |= : bitwise or
static inline Vec16i & operator |= (Vec16i & a, Vec16i const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec16i operator ^ (Vec16i const & a, Vec16i const & b) {
    return Vec16i(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ^= : bitwise xor
static inline Vec16i & operator ^= (Vec16i & a, Vec16i const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec16i operator ~ (Vec16i const & a) {
    return Vec16i(~(a.get_low()), ~(a.get_high()));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec16i select (Vec16ib const & s, Vec16i const & a, Vec16i const & b) {
    return Vec16i(select(s.get_low(), a.get_low(), b.get_low()), select(s.get_high(), a.get_high(), b.get_high()));
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec16i if_add (Vec16ib const & f, Vec16i const & a, Vec16i const & b) {
    return Vec16i(if_add(f.get_low(), a.get_low(), b.get_low()), if_add(f.get_high(), a.get_high(), b.get_high()));
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
static inline int32_t horizontal_add (Vec16i const & a) {
    return horizontal_add(a.get_low() + a.get_high());
}

// function add_saturated: add element by element, signed with saturation
static inline Vec16i add_saturated(Vec16i const & a, Vec16i const & b) {
    return Vec16i(add_saturated(a.get_low(), b.get_low()), add_saturated(a.get_high(), b.get_high()));
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec16i sub_saturated(Vec16i const & a, Vec16i const & b) {
    return Vec16i(sub_saturated(a.get_low(), b.get_low()), sub_saturated(a.get_high(), b.get_high()));
}

// function max: a > b ? a : b
static inline Vec16i max(Vec16i const & a, Vec16i const & b) {
    return Vec16i(max(a.get_low(), b.get_low()), max(a.get_high(), b.get_high()));
}

// function min: a < b ? a : b
static inline Vec16i min(Vec16i const & a, Vec16i const & b) {
    return Vec16i(min(a.get_low(), b.get_low()), min(a.get_high(), b.get_high()));
}

// function abs: a >= 0 ? a : -a
static inline Vec16i abs(Vec16i const & a) {
    return Vec16i(abs(a.get_low()), abs(a.get_high()));
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec16i abs_saturated(Vec16i const & a) {
    return Vec16i(abs_saturated(a.get_low()), abs_saturated(a.get_high()));
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec16i rotate_left(Vec16i const & a, int b) {
    return Vec16i(rotate_left(a.get_low(), b), rotate_left(a.get_high(), b));
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
        z0 = z1 = Vec8ui(i);
    };
    // Constructor to build from all elements:
    Vec16ui(uint32_t i0, uint32_t i1, uint32_t i2, uint32_t i3, uint32_t i4, uint32_t i5, uint32_t i6, uint32_t i7,
    uint32_t i8, uint32_t i9, uint32_t i10, uint32_t i11, uint32_t i12, uint32_t i13, uint32_t i14, uint32_t i15) {
        z0 = Vec8ui(i0, i1, i2, i3, i4, i5, i6, i7);
        z1 = Vec8ui(i8, i9, i10, i11, i12, i13, i14, i15);
    };
    // Constructor to build from two Vec8ui:
    Vec16ui(Vec8ui const & a0, Vec8ui const & a1) {
        z0 = a0;
        z1 = a1;
    }
    // Constructor to convert from type Vec512b
    Vec16ui(Vec512b const & x) {
        *this = x;
    };
    // Assignment operator to convert from type Vec512b
    Vec16ui & operator = (Vec512b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
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
    return Vec16ui(a.get_low() >> b, a.get_high() >> b);
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
    return Vec16ib(a.get_low() < b.get_low(), a.get_high() < b.get_high());
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec16ib operator > (Vec16ui const & a, Vec16ui const & b) {
    return b < a;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec16ib operator >= (Vec16ui const & a, Vec16ui const & b) {
    return Vec16ib(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
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
    return Vec16ui(add_saturated(a.get_low(), b.get_low()), add_saturated(a.get_high(), b.get_high()));
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec16ui sub_saturated(Vec16ui const & a, Vec16ui const & b) {
    return Vec16ui(sub_saturated(a.get_low(), b.get_low()), sub_saturated(a.get_high(), b.get_high()));
}

// function max: a > b ? a : b
static inline Vec16ui max(Vec16ui const & a, Vec16ui const & b) {
    return Vec16ui(max(a.get_low(), b.get_low()), max(a.get_high(), b.get_high()));
}

// function min: a < b ? a : b
static inline Vec16ui min(Vec16ui const & a, Vec16ui const & b) {
    return Vec16ui(min(a.get_low(), b.get_low()), min(a.get_high(), b.get_high()));
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
        z0 = z1 = Vec4q(i);
    }
    // Constructor to build from all elements:
    Vec8q(int64_t i0, int64_t i1, int64_t i2, int64_t i3, int64_t i4, int64_t i5, int64_t i6, int64_t i7) {
        z0 = Vec4q(i0, i1, i2, i3);
        z1 = Vec4q(i4, i5, i6, i7);
    }
    // Constructor to build from two Vec4q:
    Vec8q(Vec4q const & a0, Vec4q const & a1) {
        z0 = a0;
        z1 = a1;
    }
    // Constructor to convert from type Vec512b
    Vec8q(Vec512b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
    }
    // Assignment operator to convert from type Vec512b
    Vec8q & operator = (Vec512b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec8q & load(void const * p) {
        z0 = Vec4q().load(p);
        z1 = Vec4q().load((int64_t*)p+4);
        return *this;
    }
    // Member function to load from array, aligned by 64
    Vec8q & load_a(void const * p) {
        z0 = Vec4q().load_a(p);
        z1 = Vec4q().load_a((int64_t*)p+4);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec8q & load_partial(int n, void const * p) {
        if (n < 4) {
            z0 = Vec4q().load_partial(n, p);
            z1 = Vec4q(0);
        }
        else {
            z0 = Vec4q().load(p);
            z1 = Vec4q().load_partial(n - 4, (int64_t *)p + 4);
        }
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
        if (n < 4) {
            Vec4q(get_low()).store_partial(n, p);
        }
        else {
            Vec4q(get_low()).store(p);
            Vec4q(get_high()).store_partial(n - 4, (int64_t *)p + 4);
        }
    }
    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec8q & cutoff(int n) {
        if (n < 4) {
            z0 = Vec4q(z0).cutoff(n);
            z1 = Vec4q(0);
        }
        else {
            z1 = Vec4q(z1).cutoff(n - 4);
        }
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec8q const & insert(uint32_t index, int64_t value) {
        if (index < 4) {
            z0 = Vec4q(z0).insert(index, value);
        }
        else {
            z1 = Vec4q(z1).insert(index-4, value);
        }
        return *this;
    }
    // Member function extract a single element from vector
    int64_t extract(uint32_t index) const {
        if (index < 4) {
            return Vec4q(z0).extract(index);
        }
        else {
            return Vec4q(z1).extract(index - 4);
        }
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int64_t operator [] (uint32_t index) const {
        return extract(index);
    }
    // Member functions to split into two Vec2q:
    Vec4q get_low() const {
        return Vec4q(z0);
    }
    Vec4q get_high() const {
        return Vec4q(z1);
    }
    static int size () {
        return 8;
    }
};


// Define operators for Vec8q

// vector operator + : add element by element
static inline Vec8q operator + (Vec8q const & a, Vec8q const & b) {
    return Vec8q(a.get_low() + b.get_low(), a.get_high() + b.get_high());
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
    return Vec8q(a.get_low() - b.get_low(), a.get_high() - b.get_high());
}

// vector operator - : unary minus
static inline Vec8q operator - (Vec8q const & a) {
    return Vec8q(- a.get_low(), - a.get_high());
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
    return Vec8q(a.get_low() * b.get_low(), a.get_high() * b.get_high());
}

// vector operator *= : multiply
static inline Vec8q & operator *= (Vec8q & a, Vec8q const & b) {
    a = a * b;
    return a;
}

// vector operator << : shift left
static inline Vec8q operator << (Vec8q const & a, int32_t b) {
    return Vec8q(a.get_low() << b, a.get_high() << b);
}

// vector operator <<= : shift left
static inline Vec8q & operator <<= (Vec8q & a, int32_t b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec8q operator >> (Vec8q const & a, int32_t b) {
    return Vec8q(a.get_low() >> b, a.get_high() >> b);
}

// vector operator >>= : shift right arithmetic
static inline Vec8q & operator >>= (Vec8q & a, int32_t b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec8qb operator == (Vec8q const & a, Vec8q const & b) {
    return Vec8qb(a.get_low() == b.get_low(), a.get_high() == b.get_high());
}

// vector operator != : returns true for elements for which a != b
static inline Vec8qb operator != (Vec8q const & a, Vec8q const & b) {
    return Vec8qb(a.get_low() != b.get_low(), a.get_high() != b.get_high());
}
  
// vector operator < : returns true for elements for which a < b
static inline Vec8qb operator < (Vec8q const & a, Vec8q const & b) {
    return Vec8qb(a.get_low() < b.get_low(), a.get_high() < b.get_high());
}

// vector operator > : returns true for elements for which a > b
static inline Vec8qb operator > (Vec8q const & a, Vec8q const & b) {
    return b < a;
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec8qb operator >= (Vec8q const & a, Vec8q const & b) {
    return Vec8qb(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec8qb operator <= (Vec8q const & a, Vec8q const & b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec8q operator & (Vec8q const & a, Vec8q const & b) {
    return Vec8q(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}

// vector operator &= : bitwise and
static inline Vec8q & operator &= (Vec8q & a, Vec8q const & b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec8q operator | (Vec8q const & a, Vec8q const & b) {
    return Vec8q(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}

// vector operator |= : bitwise or
static inline Vec8q & operator |= (Vec8q & a, Vec8q const & b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec8q operator ^ (Vec8q const & a, Vec8q const & b) {
    return Vec8q(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}
// vector operator ^= : bitwise xor
static inline Vec8q & operator ^= (Vec8q & a, Vec8q const & b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec8q operator ~ (Vec8q const & a) {
    return Vec8q(~(a.get_low()), ~(a.get_high()));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 4; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec8q select (Vec8qb const & s, Vec8q const & a, Vec8q const & b) {
    return Vec8q(select(s.get_low(), a.get_low(), b.get_low()), select(s.get_high(), a.get_high(), b.get_high()));
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec8q if_add (Vec8qb const & f, Vec8q const & a, Vec8q const & b) {
    return Vec8q(if_add(f.get_low(), a.get_low(), b.get_low()), if_add(f.get_high(), a.get_high(), b.get_high()));
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
static inline int64_t horizontal_add (Vec8q const & a) {
    return horizontal_add(a.get_low() + a.get_high());
}

// Horizontal add extended: Calculates the sum of all vector elements
// Elements are sign extended before adding to avoid overflow
static inline int64_t horizontal_add_x (Vec16i const & x) {
    return horizontal_add_x(x.get_low()) + horizontal_add_x(x.get_high());
}

// Horizontal add extended: Calculates the sum of all vector elements
// Elements are zero extended before adding to avoid overflow
static inline uint64_t horizontal_add_x (Vec16ui const & x) {
    return horizontal_add_x(x.get_low()) + horizontal_add_x(x.get_high());
}

// function max: a > b ? a : b
static inline Vec8q max(Vec8q const & a, Vec8q const & b) {
    return Vec8q(max(a.get_low(), b.get_low()), max(a.get_high(), b.get_high()));
}

// function min: a < b ? a : b
static inline Vec8q min(Vec8q const & a, Vec8q const & b) {
    return Vec8q(min(a.get_low(), b.get_low()), min(a.get_high(), b.get_high()));
}

// function abs: a >= 0 ? a : -a
static inline Vec8q abs(Vec8q const & a) {
    return Vec8q(abs(a.get_low()), abs(a.get_high()));
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec8q abs_saturated(Vec8q const & a) {
    return Vec8q(abs_saturated(a.get_low()), abs_saturated(a.get_high()));
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec8q rotate_left(Vec8q const & a, int b) {
    return Vec8q(rotate_left(a.get_low(), b), rotate_left(a.get_high(), b));
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
        z0 = z1 = Vec4uq(i);
    }
    // Constructor to convert from Vec8q:
    Vec8uq(Vec8q const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
    }
    // Constructor to convert from type Vec512b
    Vec8uq(Vec512b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
    }
    // Constructor to build from all elements:
    Vec8uq(uint64_t i0, uint64_t i1, uint64_t i2, uint64_t i3, uint64_t i4, uint64_t i5, uint64_t i6, uint64_t i7) {
        z0 = Vec4q(i0, i1, i2, i3);
        z0 = Vec4q(i4, i5, i6, i7);
    }
    // Constructor to build from two Vec4uq:
    Vec8uq(Vec4uq const & a0, Vec4uq const & a1) {
        z0 = a0;
        z1 = a1;
    }
    // Assignment operator to convert from Vec8q:
    Vec8uq  & operator = (Vec8q const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
        return *this;
    }
    // Assignment operator to convert from type Vec512b
    Vec8uq & operator = (Vec512b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
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
    return Vec8uq(a.get_low() >> b, a.get_high() >> b);
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
    return Vec8qb(a.get_low() < b.get_low(), a.get_high() < b.get_high());
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec8qb operator > (Vec8uq const & a, Vec8uq const & b) {
    return b < a;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec8qb operator >= (Vec8uq const & a, Vec8uq const & b) {
    return Vec8qb(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
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
    return Vec8uq(if_add(f.get_low(), a.get_low(), b.get_low()), if_add(f.get_high(), a.get_high(), b.get_high()));
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
static inline uint64_t horizontal_add (Vec8uq const & a) {
    return horizontal_add(Vec8q(a));
}

// function max: a > b ? a : b
static inline Vec8uq max(Vec8uq const & a, Vec8uq const & b) {
    return Vec8uq(max(a.get_low(), b.get_low()), max(a.get_high(), b.get_high()));
}

// function min: a < b ? a : b
static inline Vec8uq min(Vec8uq const & a, Vec8uq const & b) {
    return Vec8uq(min(a.get_low(), b.get_low()), min(a.get_high(), b.get_high()));
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
    return Vec8q(blend4q<i0,i1,i2,i3> (a.get_low(), a.get_high()),
                 blend4q<i4,i5,i6,i7> (a.get_low(), a.get_high()));
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8uq permute8uq(Vec8uq const & a) {
    return Vec8uq (permute8q<i0,i1,i2,i3,i4,i5,i6,i7> (a));
}


// Permute vector of 16 32-bit integers.
// Index -1 gives 0, index -256 means don't care.
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7, int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
static inline Vec16i permute16i(Vec16i const & a) {
    return Vec16i(blend8i<i0,i1,i2 ,i3 ,i4 ,i5 ,i6 ,i7 > (a.get_low(), a.get_high()),
                  blend8i<i8,i9,i10,i11,i12,i13,i14,i15> (a.get_low(), a.get_high()));
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


// helper function used below
template <int n>
static inline Vec4q select4(Vec8q const & a, Vec8q const & b) {
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
    return Vec4q(0);
}

// blend vectors Vec8q
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7> 
static inline Vec8q blend8q(Vec8q const & a, Vec8q const & b) {  
    const int j0 = i0 >= 0 ? i0/4 : i0;
    const int j1 = i1 >= 0 ? i1/4 : i1;
    const int j2 = i2 >= 0 ? i2/4 : i2;
    const int j3 = i3 >= 0 ? i3/4 : i3;
    const int j4 = i4 >= 0 ? i4/4 : i4;
    const int j5 = i5 >= 0 ? i5/4 : i5;
    const int j6 = i6 >= 0 ? i6/4 : i6;
    const int j7 = i7 >= 0 ? i7/4 : i7;
    Vec4q x0, x1;

    const int r0 = j0 >= 0 ? j0 : j1 >= 0 ? j1 : j2 >= 0 ? j2 : j3;
    const int r1 = j4 >= 0 ? j4 : j5 >= 0 ? j5 : j6 >= 0 ? j6 : j7;
    const int s0 = (j1 >= 0 && j1 != r0) ? j1 : (j2 >= 0 && j2 != r0) ? j2 : j3;
    const int s1 = (j5 >= 0 && j5 != r1) ? j5 : (j6 >= 0 && j6 != r1) ? j6 : j7;

    // Combine all the indexes into a single bitfield, with 4 bits for each
    const int m1 = (i0&0xF) | (i1&0xF)<<4 | (i2&0xF)<<8 | (i3&0xF)<<12 | (i4&0xF)<<16 | (i5&0xF)<<20 | (i6&0xF)<<24 | (i7&0xF)<<28;

    // Mask to zero out negative indexes
    const int mz = (i0<0?0:0xF) | (i1<0?0:0xF)<<4 | (i2<0?0:0xF)<<8 | (i3<0?0:0xF)<<12 | (i4<0?0:0xF)<<16 | (i5<0?0:0xF)<<20 | (i6<0?0:0xF)<<24 | (i7<0?0:0xF)<<28;

    if (r0 < 0) {
        x0 =  Vec4q(0);
    }
    else if (((m1 ^ r0*0x4444) & 0xCCCC & mz) == 0) { 
        // i0 - i3 all from same source
        x0 = permute4q<i0 & -13, i1 & -13, i2 & -13, i3 & -13> (select4<r0> (a,b));
    }
    else if ((j2 < 0 || j2 == r0 || j2 == s0) && (j3 < 0 || j3 == r0 || j3 == s0)) { 
        // i0 - i3 all from two sources
        const int k0 =  i0 >= 0 ? i0 & 3 : i0;
        const int k1 = (i1 >= 0 ? i1 & 3 : i1) | (j1 == s0 ? 4 : 0);
        const int k2 = (i2 >= 0 ? i2 & 3 : i2) | (j2 == s0 ? 4 : 0);
        const int k3 = (i3 >= 0 ? i3 & 3 : i3) | (j3 == s0 ? 4 : 0);
        x0 = blend4q<k0,k1,k2,k3> (select4<r0>(a,b), select4<s0>(a,b));
    }
    else {
        // i0 - i3 from three or four different sources
        x0 = blend4q<0,1,6,7> (
             blend4q<i0 & -13, (i1 & -13) | 4, -0x100, -0x100> (select4<j0>(a,b), select4<j1>(a,b)),
             blend4q<-0x100, -0x100, i2 & -13, (i3 & -13) | 4> (select4<j2>(a,b), select4<j3>(a,b)));
    }

    if (r1 < 0) {
        x1 =  Vec4q(0);
    }
    else if (((m1 ^ uint32_t(r1)*0x44440000u) & 0xCCCC0000 & mz) == 0) { 
        // i4 - i7 all from same source
        x1 = permute4q<i4 & -13, i5 & -13, i6 & -13, i7 & -13> (select4<r1> (a,b));
    }
    else if ((j6 < 0 || j6 == r1 || j6 == s1) && (j7 < 0 || j7 == r1 || j7 == s1)) { 
        // i4 - i7 all from two sources
        const int k4 =  i4 >= 0 ? i4 & 3 : i4;
        const int k5 = (i5 >= 0 ? i5 & 3 : i5) | (j5 == s1 ? 4 : 0);
        const int k6 = (i6 >= 0 ? i6 & 3 : i6) | (j6 == s1 ? 4 : 0);
        const int k7 = (i7 >= 0 ? i7 & 3 : i7) | (j7 == s1 ? 4 : 0);
        x1 = blend4q<k4,k5,k6,k7> (select4<r1>(a,b), select4<s1>(a,b));
    }
    else {
        // i4 - i7 from three or four different sources
        x1 = blend4q<0,1,6,7> (
             blend4q<i4 & -13, (i5 & -13) | 4, -0x100, -0x100> (select4<j4>(a,b), select4<j5>(a,b)),
             blend4q<-0x100, -0x100, i6 & -13, (i7 & -13) | 4> (select4<j6>(a,b), select4<j7>(a,b)));
    }

    return Vec8q(x0,x1);
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7> 
static inline Vec8uq blend8uq(Vec8uq const & a, Vec8uq const & b) {
    return Vec8uq( blend8q<i0,i1,i2,i3,i4,i5,i6,i7> (a,b));
}


// helper function used below
template <int n>
static inline Vec8i select4(Vec16i const & a, Vec16i const & b) {
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
    return  Vec8i(0);
}

template <int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15 > 
static inline Vec16i blend16i(Vec16i const & a, Vec16i const & b) {

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

    Vec8i x0, x1;

    const int r0 = j0 >= 0 ? j0 : j1 >= 0 ? j1 : j2  >= 0 ? j2  : j3  >= 0 ? j3  : j4  >= 0 ? j4  : j5  >= 0 ? j5  : j6  >= 0 ? j6  : j7;
    const int r1 = j8 >= 0 ? j8 : j9 >= 0 ? j9 : j10 >= 0 ? j10 : j11 >= 0 ? j11 : j12 >= 0 ? j12 : j13 >= 0 ? j13 : j14 >= 0 ? j14 : j15;
    const int s0 = (j1 >= 0 && j1 != r0) ? j1 : (j2 >= 0 && j2 != r0) ? j2  : (j3 >= 0 && j3 != r0) ? j3 : (j4 >= 0 && j4 != r0) ? j4 : (j5 >= 0 && j5 != r0) ? j5 : (j6 >= 0 && j6 != r0) ? j6 : j7;
    const int s1 = (j9 >= 0 && j9 != r1) ? j9 : (j10>= 0 && j10!= r1) ? j10 : (j11>= 0 && j11!= r1) ? j11: (j12>= 0 && j12!= r1) ? j12: (j13>= 0 && j13!= r1) ? j13: (j14>= 0 && j14!= r1) ? j14: j15;

    if (r0 < 0) {
        x0 = Vec8i(0);
    }
    else if (r0 == s0) {
        // i0 - i7 all from same source
        x0 = permute8i<i0&-25, i1&-25, i2&-25, i3&-25, i4&-25, i5&-25, i6&-25, i7&-25> (select4<r0> (a,b));
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
        x0 = blend8i<k0,k1,k2,k3,k4,k5,k6,k7> (select4<r0>(a,b), select4<s0>(a,b));
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
        x0 = blend8i<n0, n1, n2, n3, n4, n5, n6, n7> (
             blend8i< j0   & 2 ? -256 : i0 &15,  j1   & 2 ? -256 : i1 &15,  j2   & 2 ? -256 : i2 &15,  j3   & 2 ? -256 : i3 &15,  j4   & 2 ? -256 : i4 &15,  j5   & 2 ? -256 : i5 &15,  j6   & 2 ? -256 : i6 &15,  j7   & 2 ? -256 : i7 &15> (a.get_low(),a.get_high()),
             blend8i<(j0^2)& 6 ? -256 : i0 &15, (j1^2)& 6 ? -256 : i1 &15, (j2^2)& 6 ? -256 : i2 &15, (j3^2)& 6 ? -256 : i3 &15, (j4^2)& 6 ? -256 : i4 &15, (j5^2)& 6 ? -256 : i5 &15, (j6^2)& 6 ? -256 : i6 &15, (j7^2)& 6 ? -256 : i7 &15> (b.get_low(),b.get_high()));
    }

    if (r1 < 0) {
        x1 = Vec8i(0);
    }
    else if (r1 == s1) {
        // i8 - i15 all from same source
        x1 = permute8i<i8&-25, i9&-25, i10&-25, i11&-25, i12&-25, i13&-25, i14&-25, i15&-25> (select4<r1> (a,b));
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
        x1 = blend8i<k8,k9,k10,k11,k12,k13,k14,k15> (select4<r1>(a,b), select4<s1>(a,b));
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
        x1 = blend8i<n8, n9, n10, n11, n12, n13, n14, n15> (
             blend8i< j8   & 2 ? -256 : i8 &15,  j9   & 2 ? -256 : i9 &15,  j10   & 2 ? -256 : i10 &15,  j11   & 2 ? -256 : i11 &15,  j12   & 2 ? -256 : i12 &15,  j13   & 2 ? -256 : i13 &15,  j14   & 2 ? -256 : i14 &15,  j15   & 2 ? -256 : i15 &15> (a.get_low(),a.get_high()),
             blend8i<(j8^2)& 6 ? -256 : i8 &15, (j9^2)& 6 ? -256 : i9 &15, (j10^2)& 6 ? -256 : i10 &15, (j11^2)& 6 ? -256 : i11 &15, (j12^2)& 6 ? -256 : i12 &15, (j13^2)& 6 ? -256 : i13 &15, (j14^2)& 6 ? -256 : i14 &15, (j15^2)& 6 ? -256 : i15 &15> (b.get_low(),b.get_high()));
    }
    return Vec16i(x0,x1);
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
    int32_t tab[16];
    table.store(tab);
    Vec8i t0 = lookup<16>(index.get_low(), tab);
    Vec8i t1 = lookup<16>(index.get_high(), tab);
    return Vec16i(t0, t1);
}

template <int n>
static inline Vec16i lookup(Vec16i const & index, void const * table) {
    if (n <=  0) return 0;
    if (n <=  8) {
        Vec8i table1 = Vec8i().load(table);        
        return Vec16i(       
            lookup8 (index.get_low(),  table1),
            lookup8 (index.get_high(), table1));
    }
    if (n <= 16) return lookup16(index, Vec16i().load(table));
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
    int32_t const * t = (int32_t const *)table;
    return Vec16i(t[i1[0]],t[i1[1]],t[i1[2]],t[i1[3]],t[i1[4]],t[i1[5]],t[i1[6]],t[i1[7]],
        t[i1[8]],t[i1[9]],t[i1[10]],t[i1[11]],t[i1[12]],t[i1[13]],t[i1[14]],t[i1[15]]);
}

static inline Vec8q lookup8(Vec8q const & index, Vec8q const & table) {
    int64_t tab[8];
    table.store(tab);
    Vec4q t0 = lookup<8>(index.get_low(), tab);
    Vec4q t1 = lookup<8>(index.get_high(), tab);
    return Vec8q(t0, t1);
}

template <int n>
static inline Vec8q lookup(Vec8q const & index, void const * table) {
    if (n <= 0) return 0;
    if (n <= 4) {
        Vec4q table1 = Vec4q().load(table);        
        return Vec8q(       
            lookup4 (index.get_low(),  table1),
            lookup4 (index.get_high(), table1));
    }
    if (n <= 8) {
        return lookup8(index, Vec8q().load(table));
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
    int64_t const * t = (int64_t const *)table;
    return Vec8q(t[i1[0]],t[i1[1]],t[i1[2]],t[i1[3]],t[i1[4]],t[i1[5]],t[i1[6]],t[i1[7]]);
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
    // use lookup function
    return lookup<imax+1>(Vec16i(i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15), a);
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
    // use lookup function
    return lookup<imax+1>(Vec8q(i0,i1,i2,i3,i4,i5,i6,i7), a);
}


/*****************************************************************************
*
*          Functions for conversion between integer sizes
*
*****************************************************************************/

// Extend 16-bit integers to 32-bit integers, signed and unsigned

// Function extend_to_int : extends Vec16s to Vec16i with sign extension
static inline Vec16i extend_to_int (Vec16s const & a) {
    return Vec16i(extend_low(a), extend_high(a));
}

// Function extend_to_int : extends Vec16us to Vec16ui with zero extension
static inline Vec16ui extend_to_int (Vec16us const & a) {
    return Vec16i(extend_low(a), extend_high(a));
}

// Function extend_to_int : extends Vec16c to Vec16i with sign extension
static inline Vec16i extend_to_int (Vec16c const & a) {
    return extend_to_int(Vec16s(extend_low(a), extend_high(a)));
}

// Function extend_to_int : extends Vec16uc to Vec16ui with zero extension
static inline Vec16ui extend_to_int (Vec16uc const & a) {
    return extend_to_int(Vec16s(extend_low(a), extend_high(a)));
}


// Extend 32-bit integers to 64-bit integers, signed and unsigned

// Function extend_low : extends the low 8 elements to 64 bits with sign extension
static inline Vec8q extend_low (Vec16i const & a) {
    return Vec8q(extend_low(a.get_low()), extend_high(a.get_low()));
}

// Function extend_high : extends the high 8 elements to 64 bits with sign extension
static inline Vec8q extend_high (Vec16i const & a) {
    return Vec8q(extend_low(a.get_high()), extend_high(a.get_high()));
}

// Function extend_low : extends the low 8 elements to 64 bits with zero extension
static inline Vec8uq extend_low (Vec16ui const & a) {
    return Vec8q(extend_low(a.get_low()), extend_high(a.get_low()));
}

// Function extend_high : extends the high 8 elements to 64 bits with zero extension
static inline Vec8uq extend_high (Vec16ui const & a) {
    return Vec8q(extend_low(a.get_high()), extend_high(a.get_high()));
}


// Compress 32-bit integers to 8-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Overflow wraps around
static inline Vec16c compress_to_int8 (Vec16i const & a) {
    Vec16s b = compress(a.get_low(), a.get_high());
    Vec16c c = compress(b.get_low(), b.get_high());
    return c;
}

static inline Vec16s compress_to_int16 (Vec16i const & a) {
    return compress(a.get_low(), a.get_high());
}

// with signed saturation
static inline Vec16c compress_to_int8_saturated (Vec16i const & a) {
    Vec16s b = compress_saturated(a.get_low(), a.get_high());
    Vec16c c = compress_saturated(b.get_low(), b.get_high());
    return c;
}

static inline Vec16s compress_to_int16_saturated (Vec16i const & a) {
    return compress_saturated(a.get_low(), a.get_high());
}

// with unsigned saturation
static inline Vec16uc compress_to_int8_saturated (Vec16ui const & a) {
    Vec16us b = compress_saturated(a.get_low(), a.get_high());
    Vec16uc c = compress_saturated(b.get_low(), b.get_high());
    return c;
}

static inline Vec16us compress_to_int16_saturated (Vec16ui const & a) {
    return compress_saturated(a.get_low(), a.get_high());
}

// Compress 64-bit integers to 32-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Overflow wraps around
static inline Vec16i compress (Vec8q const & low, Vec8q const & high) {
    return Vec16i(compress(low.get_low(),low.get_high()), compress(high.get_low(),high.get_high()));
}

// Function compress_saturated : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Signed, with saturation
static inline Vec16i compress_saturated (Vec8q const & low, Vec8q const & high) {
    return Vec16i(compress_saturated(low.get_low(),low.get_high()), compress_saturated(high.get_low(),high.get_high()));
}

// Function compress_saturated : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Unsigned, with saturation
static inline Vec16ui compress_saturated (Vec8uq const & low, Vec8uq const & high) {
    return Vec16ui(compress_saturated(low.get_low(),low.get_high()), compress_saturated(high.get_low(),high.get_high()));
}


/*****************************************************************************
*
*          Integer division operators
*
*          Please see the file vectori128.h for explanation.
*
*****************************************************************************/

// vector operator / : divide each element by divisor

// vector operator / : divide all elements by same integer
static inline Vec16i operator / (Vec16i const & a, Divisor_i const & d) {
    return Vec16i(a.get_low() / d, a.get_high() / d);
}

// vector operator /= : divide
static inline Vec16i & operator /= (Vec16i & a, Divisor_i const & d) {
    a = a / d;
    return a;
}

// vector operator / : divide all elements by same integer
static inline Vec16ui operator / (Vec16ui const & a, Divisor_ui const & d) {
    return Vec16ui(a.get_low() / d, a.get_high() / d);
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
static inline Vec16i divide_by_i(Vec16i const & a) {
    return Vec16i(divide_by_i<d>(a.get_low()), divide_by_i<d>(a.get_high()));
}

// define Vec16i a / const_int(d)
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
static inline Vec16ui divide_by_ui(Vec16ui const & a) {
    return Vec16ui( divide_by_ui<d>(a.get_low()), divide_by_ui<d>(a.get_high()));
}

// define Vec16ui a / const_uint(d)
template <uint32_t d>
static inline Vec16ui operator / (Vec16ui const & a, Const_uint_t<d>) {
    return divide_by_ui<d>(a);
}

// define Vec16ui a / const_int(d)
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
    int a1 = horizontal_find_first(x.get_low());
    if (a1 >= 0) return a1;
    int a2 = horizontal_find_first(x.get_high());
    if (a2 < 0) return a2;
    return a2 + 8;
}

static inline int horizontal_find_first(Vec8qb const & x) {
    int a1 = horizontal_find_first(x.get_low());
    if (a1 >= 0) return a1;
    int a2 = horizontal_find_first(x.get_high());
    if (a2 < 0) return a2;
    return a2 + 4;
}

// count the number of true elements
static inline uint32_t horizontal_count(Vec16ib const & x) {
    return horizontal_count(x.get_low()) + horizontal_count(x.get_high());
}

static inline uint32_t horizontal_count(Vec8qb const & x) {
    return horizontal_count(x.get_low()) + horizontal_count(x.get_high());
}


/*****************************************************************************
*
*          Boolean <-> bitfield conversion functions
*
*****************************************************************************/

// to_bits: convert to integer bitfield
static inline uint16_t to_bits(Vec16b const & a) {
    return to_bits(a.get_low()) | ((uint16_t)to_bits(a.get_high()) << 8);
}

// to_bits: convert to integer bitfield
static inline uint16_t to_bits(Vec16ib const & a) {
    return to_bits(a.get_low()) | ((uint16_t)to_bits(a.get_high()) << 8);
}

// to_Vec16ib: convert integer bitfield to boolean vector
static inline Vec16ib to_Vec16ib(uint16_t const & x) {
    return Vec16i(to_Vec8ib(uint8_t(x)), to_Vec8ib(uint8_t(x>>8)));
}

// to_bits: convert to integer bitfield
static inline uint8_t to_bits(Vec8b const & a) {
    return to_bits(a.get_low()) | (to_bits(a.get_high()) << 4);
}

// to_Vec8qb: convert integer bitfield to boolean vector
static inline Vec8qb to_Vec8qb(uint8_t x) {
    return Vec8q(to_Vec4qb(x), to_Vec4qb(x>>4));
}

#endif // VECTORI512_H
