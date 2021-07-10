/* Operations with long integers.
   Copyright (C) 2006-2018 Free Software Foundation, Inc.

This file is part of GCC.

GCC is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation; either version 3, or (at your option) any
later version.

GCC is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
for more details.

You should have received a copy of the GNU General Public License
along with GCC; see the file COPYING3.  If not see
<http://www.gnu.org/licenses/>.  */

#ifndef DOUBLE_INT_H
#define DOUBLE_INT_H

/* A large integer is currently represented as a pair of HOST_WIDE_INTs.
   It therefore represents a number with precision of
   2 * HOST_BITS_PER_WIDE_INT bits (it is however possible that the
   internal representation will change, if numbers with greater precision
   are needed, so the users should not rely on it).  The representation does
   not contain any information about signedness of the represented value, so
   it can be used to represent both signed and unsigned numbers.  For
   operations where the results depend on signedness (division, comparisons),
   it must be specified separately.  For each such operation, there are three
   versions of the function -- double_int_op, that takes an extra UNS argument
   giving the signedness of the values, and double_int_sop and double_int_uop
   that stand for its specializations for signed and unsigned values.

   You may also represent with numbers in smaller precision using double_int.
   You however need to use double_int_ext (that fills in the bits of the
   number over the prescribed precision with zeros or with the sign bit) before
   operations that do not perform arithmetics modulo 2^precision (comparisons,
   division), and possibly before storing the results, if you want to keep
   them in some canonical form).  In general, the signedness of double_int_ext
   should match the signedness of the operation.

   ??? The components of double_int differ in signedness mostly for
   historical reasons (they replace an older structure used to represent
   numbers with precision higher than HOST_WIDE_INT).  It might be less
   confusing to have them both signed or both unsigned.  */

struct double_int
{
  /* Normally, we would define constructors to create instances.
     Two things prevent us from doing so.
     First, defining a constructor makes the class non-POD in C++03,
     and we certainly want double_int to be a POD.
     Second, the GCC conding conventions prefer explicit conversion,
     and explicit conversion operators are not available until C++11.  */

  static double_int from_uhwi (unsigned HOST_WIDE_INT cst);
  static double_int from_shwi (HOST_WIDE_INT cst);
  static double_int from_pair (HOST_WIDE_INT high, unsigned HOST_WIDE_INT low);

  /* Construct from a fuffer of length LEN.  BUFFER will be read according
     to byte endianness and word endianness.  */
  static double_int from_buffer (const unsigned char *buffer, int len);

  /* No copy assignment operator or destructor to keep the type a POD.  */

  /* There are some special value-creation static member functions.  */

  static double_int mask (unsigned prec);
  static double_int max_value (unsigned int prec, bool uns);
  static double_int min_value (unsigned int prec, bool uns);

  /* The following functions are mutating operations.  */

  double_int &operator ++ (); // prefix
  double_int &operator -- (); // prefix
  double_int &operator *= (double_int);
  double_int &operator += (double_int);
  double_int &operator -= (double_int);
  double_int &operator &= (double_int);
  double_int &operator ^= (double_int);
  double_int &operator |= (double_int);

  /* The following functions are non-mutating operations.  */

  /* Conversion functions.  */

  HOST_WIDE_INT to_shwi () const;
  unsigned HOST_WIDE_INT to_uhwi () const;

  /* Conversion query functions.  */

  bool fits_uhwi () const;
  bool fits_shwi () const;
  bool fits_hwi (bool uns) const;

  /* Attribute query functions.  */

  int trailing_zeros () const;
  int popcount () const;

  /* Arithmetic query operations.  */

  bool multiple_of (double_int, bool, double_int *) const;

  /* Arithmetic operation functions.  */

  /* The following operations perform arithmetics modulo 2^precision, so you
     do not need to call .ext between them, even if you are representing
     numbers with precision less than HOST_BITS_PER_DOUBLE_INT bits.  */

  double_int set_bit (unsigned) const;
  double_int mul_with_sign (double_int, bool unsigned_p, bool *overflow) const;
  double_int wide_mul_with_sign (double_int, bool unsigned_p,
				 double_int *higher, bool *overflow) const;
  double_int add_with_sign (double_int, bool unsigned_p, bool *overflow) const;
  double_int sub_with_overflow (double_int, bool *overflow) const;
  double_int neg_with_overflow (bool *overflow) const;

  double_int operator * (double_int) const;
  double_int operator + (double_int) const;
  double_int operator - (double_int) const;
  double_int operator - () const;
  double_int operator ~ () const;
  double_int operator & (double_int) const;
  double_int operator | (double_int) const;
  double_int operator ^ (double_int) const;
  double_int and_not (double_int) const;

  double_int lshift (HOST_WIDE_INT count) const;
  double_int lshift (HOST_WIDE_INT count, unsigned int prec, bool arith) const;
  double_int rshift (HOST_WIDE_INT count) const;
  double_int rshift (HOST_WIDE_INT count, unsigned int prec, bool arith) const;
  double_int alshift (HOST_WIDE_INT count, unsigned int prec) const;
  double_int arshift (HOST_WIDE_INT count, unsigned int prec) const;
  double_int llshift (HOST_WIDE_INT count, unsigned int prec) const;
  double_int lrshift (HOST_WIDE_INT count, unsigned int prec) const;
  double_int lrotate (HOST_WIDE_INT count, unsigned int prec) const;
  double_int rrotate (HOST_WIDE_INT count, unsigned int prec) const;

  /* You must ensure that double_int::ext is called on the operands
     of the following operations, if the precision of the numbers
     is less than HOST_BITS_PER_DOUBLE_INT bits.  */

  double_int div (double_int, bool, unsigned) const;
  double_int sdiv (double_int, unsigned) const;
  double_int udiv (double_int, unsigned) const;
  double_int mod (double_int, bool, unsigned) const;
  double_int smod (double_int, unsigned) const;
  double_int umod (double_int, unsigned) const;
  double_int divmod_with_overflow (double_int, bool, unsigned,
				   double_int *, bool *) const;
  double_int divmod (double_int, bool, unsigned, double_int *) const;
  double_int sdivmod (double_int, unsigned, double_int *) const;
  double_int udivmod (double_int, unsigned, double_int *) const;

  /* Precision control functions.  */

  double_int ext (unsigned prec, bool uns) const;
  double_int zext (unsigned prec) const;
  double_int sext (unsigned prec) const;

  /* Comparative functions.  */

  bool is_zero () const;
  bool is_one () const;
  bool is_minus_one () const;
  bool is_negative () const;

  int cmp (double_int b, bool uns) const;
  int ucmp (double_int b) const;
  int scmp (double_int b) const;

  bool ult (double_int b) const;
  bool ule (double_int b) const;
  bool ugt (double_int b) const;
  bool slt (double_int b) const;
  bool sle (double_int b) const;
  bool sgt (double_int b) const;

  double_int max (double_int b, bool uns);
  double_int smax (double_int b);
  double_int umax (double_int b);

  double_int min (double_int b, bool uns);
  double_int smin (double_int b);
  double_int umin (double_int b);

  bool operator == (double_int cst2) const;
  bool operator != (double_int cst2) const;

  /* Please migrate away from using these member variables publicly.  */

  unsigned HOST_WIDE_INT low;
  HOST_WIDE_INT high;

};

#define HOST_BITS_PER_DOUBLE_INT (2 * HOST_BITS_PER_WIDE_INT)

/* Constructors and conversions.  */

/* Constructs double_int from integer CST.  The bits over the precision of
   HOST_WIDE_INT are filled with the sign bit.  */

inline double_int
double_int::from_shwi (HOST_WIDE_INT cst)
{
  double_int r;
  r.low = (unsigned HOST_WIDE_INT) cst;
  r.high = cst < 0 ? -1 : 0;
  return r;
}

/* Some useful constants.  */
/* FIXME(crowl): Maybe remove after converting callers?
   The problem is that a named constant would not be as optimizable,
   while the functional syntax is more verbose.  */

#define double_int_minus_one (double_int::from_shwi (-1))
#define double_int_zero (double_int::from_shwi (0))
#define double_int_one (double_int::from_shwi (1))
#define double_int_two (double_int::from_shwi (2))
#define double_int_ten (double_int::from_shwi (10))

/* Constructs double_int from unsigned integer CST.  The bits over the
   precision of HOST_WIDE_INT are filled with zeros.  */

inline double_int
double_int::from_uhwi (unsigned HOST_WIDE_INT cst)
{
  double_int r;
  r.low = cst;
  r.high = 0;
  return r;
}

inline double_int
double_int::from_pair (HOST_WIDE_INT high, unsigned HOST_WIDE_INT low)
{
  double_int r;
  r.low = low;
  r.high = high;
  return r;
}

inline double_int &
double_int::operator ++ ()
{
  *this += double_int_one;
  return *this;
}

inline double_int &
double_int::operator -- ()
{
  *this -= double_int_one;
  return *this;
}

inline double_int &
double_int::operator &= (double_int b)
{
  *this = *this & b;
  return *this;
}

inline double_int &
double_int::operator ^= (double_int b)
{
  *this = *this ^ b;
  return *this;
}

inline double_int &
double_int::operator |= (double_int b)
{
  *this = *this | b;
  return *this;
}

/* Returns value of CST as a signed number.  CST must satisfy
   double_int::fits_signed.  */

inline HOST_WIDE_INT
double_int::to_shwi () const
{
  return (HOST_WIDE_INT) low;
}

/* Returns value of CST as an unsigned number.  CST must satisfy
   double_int::fits_unsigned.  */

inline unsigned HOST_WIDE_INT
double_int::to_uhwi () const
{
  return low;
}

/* Returns true if CST fits in unsigned HOST_WIDE_INT.  */

inline bool
double_int::fits_uhwi () const
{
  return high == 0;
}

/* Logical operations.  */

/* Returns ~A.  */

inline double_int
double_int::operator ~ () const
{
  double_int result;
  result.low = ~low;
  result.high = ~high;
  return result;
}

/* Returns A | B.  */

inline double_int
double_int::operator | (double_int b) const
{
  double_int result;
  result.low = low | b.low;
  result.high = high | b.high;
  return result;
}

/* Returns A & B.  */

inline double_int
double_int::operator & (double_int b) const
{
  double_int result;
  result.low = low & b.low;
  result.high = high & b.high;
  return result;
}

/* Returns A & ~B.  */

inline double_int
double_int::and_not (double_int b) const
{
  double_int result;
  result.low = low & ~b.low;
  result.high = high & ~b.high;
  return result;
}

/* Returns A ^ B.  */

inline double_int
double_int::operator ^ (double_int b) const
{
  double_int result;
  result.low = low ^ b.low;
  result.high = high ^ b.high;
  return result;
}

void dump_double_int (FILE *, double_int, bool);

#define ALL_ONES HOST_WIDE_INT_M1U

/* The operands of the following comparison functions must be processed
   with double_int_ext, if their precision is less than
   HOST_BITS_PER_DOUBLE_INT bits.  */

/* Returns true if CST is zero.  */

inline bool
double_int::is_zero () const
{
  return low == 0 && high == 0;
}

/* Returns true if CST is one.  */

inline bool
double_int::is_one () const
{
  return low == 1 && high == 0;
}

/* Returns true if CST is minus one.  */

inline bool
double_int::is_minus_one () const
{
  return low == ALL_ONES && high == -1;
}

/* Returns true if CST is negative.  */

inline bool
double_int::is_negative () const
{
  return high < 0;
}

/* Returns true if CST1 == CST2.  */

inline bool
double_int::operator == (double_int cst2) const
{
  return low == cst2.low && high == cst2.high;
}

/* Returns true if CST1 != CST2.  */

inline bool
double_int::operator != (double_int cst2) const
{
  return low != cst2.low || high != cst2.high;
}

/* Return number of set bits of CST.  */

inline int
double_int::popcount () const
{
  return popcount_hwi (high) + popcount_hwi (low);
}


#ifndef GENERATOR_FILE
/* Conversion to and from GMP integer representations.  */

void mpz_set_double_int (mpz_t, double_int, bool);
double_int mpz_get_double_int (const_tree, mpz_t, bool);
#endif

namespace wi
{
  template <>
  struct int_traits <double_int>
  {
    static const enum precision_type precision_type = CONST_PRECISION;
    static const bool host_dependent_precision = true;
    static const unsigned int precision = HOST_BITS_PER_DOUBLE_INT;
    static unsigned int get_precision (const double_int &);
    static wi::storage_ref decompose (HOST_WIDE_INT *, unsigned int,
				      const double_int &);
  };
}

inline unsigned int
wi::int_traits <double_int>::get_precision (const double_int &)
{
  return precision;
}

inline wi::storage_ref
wi::int_traits <double_int>::decompose (HOST_WIDE_INT *scratch, unsigned int p,
					const double_int &x)
{
  gcc_checking_assert (precision == p);
  scratch[0] = x.low;
  if ((x.high == 0 && scratch[0] >= 0) || (x.high == -1 && scratch[0] < 0))
    return wi::storage_ref (scratch, 1, precision);
  scratch[1] = x.high;
  return wi::storage_ref (scratch, 2, precision);
}

#endif /* DOUBLE_INT_H */
