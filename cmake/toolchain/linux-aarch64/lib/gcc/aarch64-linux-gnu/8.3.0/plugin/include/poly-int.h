/* Polynomial integer classes.
   Copyright (C) 2014-2018 Free Software Foundation, Inc.

This file is part of GCC.

GCC is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free
Software Foundation; either version 3, or (at your option) any later
version.

GCC is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
for more details.

You should have received a copy of the GNU General Public License
along with GCC; see the file COPYING3.  If not see
<http://www.gnu.org/licenses/>.  */

/* This file provides a representation of sizes and offsets whose exact
   values depend on certain runtime properties.  The motivating example
   is the Arm SVE ISA, in which the number of vector elements is only
   known at runtime.  See doc/poly-int.texi for more details.

   Tests for poly-int.h are located in testsuite/gcc.dg/plugin,
   since they are too expensive (in terms of binary size) to be
   included as selftests.  */

#ifndef HAVE_POLY_INT_H
#define HAVE_POLY_INT_H

template<unsigned int N, typename T> class poly_int_pod;
template<unsigned int N, typename T> class poly_int;

/* poly_coeff_traiits<T> describes the properties of a poly_int
   coefficient type T:

   - poly_coeff_traits<T1>::rank is less than poly_coeff_traits<T2>::rank
     if T1 can promote to T2.  For C-like types the rank is:

       (2 * number of bytes) + (unsigned ? 1 : 0)

     wide_ints don't have a normal rank and so use a value of INT_MAX.
     Any fixed-width integer should be promoted to wide_int if possible
     and lead to an error otherwise.

   - poly_coeff_traits<T>::int_type is the type to which an integer
     literal should be cast before comparing it with T.

   - poly_coeff_traits<T>::precision is the number of bits that T can hold.

   - poly_coeff_traits<T>::signedness is:
	0 if T is unsigned
	1 if T is signed
       -1 if T has no inherent sign (as for wide_int).

   - poly_coeff_traits<T>::max_value, if defined, is the maximum value of T.

   - poly_coeff_traits<T>::result is a type that can hold results of
     operations on T.  This is different from T itself in cases where T
     is the result of an accessor like wi::to_offset.  */
template<typename T, wi::precision_type = wi::int_traits<T>::precision_type>
struct poly_coeff_traits;

template<typename T>
struct poly_coeff_traits<T, wi::FLEXIBLE_PRECISION>
{
  typedef T result;
  typedef T int_type;
  static const int signedness = (T (0) >= T (-1));
  static const int precision = sizeof (T) * CHAR_BIT;
  static const T max_value = (signedness
			      ? ((T (1) << (precision - 2))
				 + ((T (1) << (precision - 2)) - 1))
			      : T (-1));
  static const int rank = sizeof (T) * 2 + !signedness;
};

template<typename T>
struct poly_coeff_traits<T, wi::VAR_PRECISION>
{
  typedef T result;
  typedef int int_type;
  static const int signedness = -1;
  static const int precision = WIDE_INT_MAX_PRECISION;
  static const int rank = INT_MAX;
};

template<typename T>
struct poly_coeff_traits<T, wi::CONST_PRECISION>
{
  typedef WI_UNARY_RESULT (T) result;
  typedef int int_type;
  /* These types are always signed.  */
  static const int signedness = 1;
  static const int precision = wi::int_traits<T>::precision;
  static const int rank = precision * 2 / CHAR_BIT;
};

/* Information about a pair of coefficient types.  */
template<typename T1, typename T2>
struct poly_coeff_pair_traits
{
  /* True if T1 can represent all the values of T2.

     Either:

     - T1 should be a type with the same signedness as T2 and no less
       precision.  This allows things like int16_t -> int16_t and
       uint32_t -> uint64_t.

     - T1 should be signed, T2 should be unsigned, and T1 should be
       wider than T2.  This allows things like uint16_t -> int32_t.

     This rules out cases in which T1 has less precision than T2 or where
     the conversion would reinterpret the top bit.  E.g. int16_t -> uint32_t
     can be dangerous and should have an explicit cast if deliberate.  */
  static const bool lossless_p = (poly_coeff_traits<T1>::signedness
				  == poly_coeff_traits<T2>::signedness
				  ? (poly_coeff_traits<T1>::precision
				     >= poly_coeff_traits<T2>::precision)
				  : (poly_coeff_traits<T1>::signedness == 1
				     && poly_coeff_traits<T2>::signedness == 0
				     && (poly_coeff_traits<T1>::precision
					 > poly_coeff_traits<T2>::precision)));

  /* 0 if T1 op T2 should promote to HOST_WIDE_INT,
     1 if T1 op T2 should promote to unsigned HOST_WIDE_INT,
     2 if T1 op T2 should use wide-int rules.  */
#define RANK(X) poly_coeff_traits<X>::rank
  static const int result_kind
    = ((RANK (T1) <= RANK (HOST_WIDE_INT)
	&& RANK (T2) <= RANK (HOST_WIDE_INT))
       ? 0
       : (RANK (T1) <= RANK (unsigned HOST_WIDE_INT)
	  && RANK (T2) <= RANK (unsigned HOST_WIDE_INT))
       ? 1 : 2);
#undef RANK
};

/* SFINAE class that makes T3 available as "type" if T2 can represent all the
   values in T1.  */
template<typename T1, typename T2, typename T3,
	 bool lossless_p = poly_coeff_pair_traits<T1, T2>::lossless_p>
struct if_lossless;
template<typename T1, typename T2, typename T3>
struct if_lossless<T1, T2, T3, true>
{
  typedef T3 type;
};

/* poly_int_traits<T> describes an integer type T that might be polynomial
   or non-polynomial:

   - poly_int_traits<T>::is_poly is true if T is a poly_int-based type
     and false otherwise.

   - poly_int_traits<T>::num_coeffs gives the number of coefficients in T
     if T is a poly_int and 1 otherwise.

   - poly_int_traits<T>::coeff_type gives the coefficent type of T if T
     is a poly_int and T itself otherwise

   - poly_int_traits<T>::int_type is a shorthand for
     typename poly_coeff_traits<coeff_type>::int_type.  */
template<typename T>
struct poly_int_traits
{
  static const bool is_poly = false;
  static const unsigned int num_coeffs = 1;
  typedef T coeff_type;
  typedef typename poly_coeff_traits<T>::int_type int_type;
};
template<unsigned int N, typename C>
struct poly_int_traits<poly_int_pod<N, C> >
{
  static const bool is_poly = true;
  static const unsigned int num_coeffs = N;
  typedef C coeff_type;
  typedef typename poly_coeff_traits<C>::int_type int_type;
};
template<unsigned int N, typename C>
struct poly_int_traits<poly_int<N, C> > : poly_int_traits<poly_int_pod<N, C> >
{
};

/* SFINAE class that makes T2 available as "type" if T1 is a non-polynomial
   type.  */
template<typename T1, typename T2 = T1,
	 bool is_poly = poly_int_traits<T1>::is_poly>
struct if_nonpoly {};
template<typename T1, typename T2>
struct if_nonpoly<T1, T2, false>
{
  typedef T2 type;
};

/* SFINAE class that makes T3 available as "type" if both T1 and T2 are
   non-polynomial types.  */
template<typename T1, typename T2, typename T3,
	 bool is_poly1 = poly_int_traits<T1>::is_poly,
	 bool is_poly2 = poly_int_traits<T2>::is_poly>
struct if_nonpoly2 {};
template<typename T1, typename T2, typename T3>
struct if_nonpoly2<T1, T2, T3, false, false>
{
  typedef T3 type;
};

/* SFINAE class that makes T2 available as "type" if T1 is a polynomial
   type.  */
template<typename T1, typename T2 = T1,
	 bool is_poly = poly_int_traits<T1>::is_poly>
struct if_poly {};
template<typename T1, typename T2>
struct if_poly<T1, T2, true>
{
  typedef T2 type;
};

/* poly_result<T1, T2> describes the result of an operation on two
   types T1 and T2, where at least one of the types is polynomial:

   - poly_result<T1, T2>::type gives the result type for the operation.
     The intention is to provide normal C-like rules for integer ranks,
     except that everything smaller than HOST_WIDE_INT promotes to
     HOST_WIDE_INT.

   - poly_result<T1, T2>::cast is the type to which an operand of type
     T1 should be cast before doing the operation, to ensure that
     the operation is done at the right precision.  Casting to
     poly_result<T1, T2>::type would also work, but casting to this
     type is more efficient.  */
template<typename T1, typename T2 = T1,
	 int result_kind = poly_coeff_pair_traits<T1, T2>::result_kind>
struct poly_result;

/* Promote pair to HOST_WIDE_INT.  */
template<typename T1, typename T2>
struct poly_result<T1, T2, 0>
{
  typedef HOST_WIDE_INT type;
  /* T1 and T2 are primitive types, so cast values to T before operating
     on them.  */
  typedef type cast;
};

/* Promote pair to unsigned HOST_WIDE_INT.  */
template<typename T1, typename T2>
struct poly_result<T1, T2, 1>
{
  typedef unsigned HOST_WIDE_INT type;
  /* T1 and T2 are primitive types, so cast values to T before operating
     on them.  */
  typedef type cast;
};

/* Use normal wide-int rules.  */
template<typename T1, typename T2>
struct poly_result<T1, T2, 2>
{
  typedef WI_BINARY_RESULT (T1, T2) type;
  /* Don't cast values before operating on them; leave the wi:: routines
     to handle promotion as necessary.  */
  typedef const T1 &cast;
};

/* The coefficient type for the result of a binary operation on two
   poly_ints, the first of which has coefficients of type C1 and the
   second of which has coefficients of type C2.  */
#define POLY_POLY_COEFF(C1, C2) typename poly_result<C1, C2>::type

/* Enforce that T2 is non-polynomial and provide the cofficient type of
   the result of a binary operation in which the first operand is a
   poly_int with coefficients of type C1 and the second operand is
   a constant of type T2.  */
#define POLY_CONST_COEFF(C1, T2) \
  POLY_POLY_COEFF (C1, typename if_nonpoly<T2>::type)

/* Likewise in reverse.  */
#define CONST_POLY_COEFF(T1, C2) \
  POLY_POLY_COEFF (typename if_nonpoly<T1>::type, C2)

/* The result type for a binary operation on poly_int<N, C1> and
   poly_int<N, C2>.  */
#define POLY_POLY_RESULT(N, C1, C2) poly_int<N, POLY_POLY_COEFF (C1, C2)>

/* Enforce that T2 is non-polynomial and provide the result type
   for a binary operation on poly_int<N, C1> and T2.  */
#define POLY_CONST_RESULT(N, C1, T2) poly_int<N, POLY_CONST_COEFF (C1, T2)>

/* Enforce that T1 is non-polynomial and provide the result type
   for a binary operation on T1 and poly_int<N, C2>.  */
#define CONST_POLY_RESULT(N, T1, C2) poly_int<N, CONST_POLY_COEFF (T1, C2)>

/* Enforce that T1 and T2 are non-polynomial and provide the result type
   for a binary operation on T1 and T2.  */
#define CONST_CONST_RESULT(N, T1, T2) \
  POLY_POLY_COEFF (typename if_nonpoly<T1>::type, \
		   typename if_nonpoly<T2>::type)

/* The type to which a coefficient of type C1 should be cast before
   using it in a binary operation with a coefficient of type C2.  */
#define POLY_CAST(C1, C2) typename poly_result<C1, C2>::cast

/* Provide the coefficient type for the result of T1 op T2, where T1
   and T2 can be polynomial or non-polynomial.  */
#define POLY_BINARY_COEFF(T1, T2) \
  typename poly_result<typename poly_int_traits<T1>::coeff_type, \
		       typename poly_int_traits<T2>::coeff_type>::type

/* The type to which an integer constant should be cast before
   comparing it with T.  */
#define POLY_INT_TYPE(T) typename poly_int_traits<T>::int_type

/* RES is a poly_int result that has coefficients of type C and that
   is being built up a coefficient at a time.  Set coefficient number I
   to VALUE in the most efficient way possible.

   For primitive C it is better to assign directly, since it avoids
   any further calls and so is more efficient when the compiler is
   built at -O0.  But for wide-int based C it is better to construct
   the value in-place.  This means that calls out to a wide-int.cc
   routine can take the address of RES rather than the address of
   a temporary.

   The dummy comparison against a null C * is just a way of checking
   that C gives the right type.  */
#define POLY_SET_COEFF(C, RES, I, VALUE) \
  ((void) (&(RES).coeffs[0] == (C *) 0), \
   wi::int_traits<C>::precision_type == wi::FLEXIBLE_PRECISION \
   ? (void) ((RES).coeffs[I] = VALUE) \
   : (void) ((RES).coeffs[I].~C (), new (&(RES).coeffs[I]) C (VALUE)))

/* A base POD class for polynomial integers.  The polynomial has N
   coefficients of type C.  */
template<unsigned int N, typename C>
class poly_int_pod
{
public:
  template<typename Ca>
  poly_int_pod &operator = (const poly_int_pod<N, Ca> &);
  template<typename Ca>
  typename if_nonpoly<Ca, poly_int_pod>::type &operator = (const Ca &);

  template<typename Ca>
  poly_int_pod &operator += (const poly_int_pod<N, Ca> &);
  template<typename Ca>
  typename if_nonpoly<Ca, poly_int_pod>::type &operator += (const Ca &);

  template<typename Ca>
  poly_int_pod &operator -= (const poly_int_pod<N, Ca> &);
  template<typename Ca>
  typename if_nonpoly<Ca, poly_int_pod>::type &operator -= (const Ca &);

  template<typename Ca>
  typename if_nonpoly<Ca, poly_int_pod>::type &operator *= (const Ca &);

  poly_int_pod &operator <<= (unsigned int);

  bool is_constant () const;

  template<typename T>
  typename if_lossless<T, C, bool>::type is_constant (T *) const;

  C to_constant () const;

  template<typename Ca>
  static poly_int<N, C> from (const poly_int_pod<N, Ca> &, unsigned int,
			      signop);
  template<typename Ca>
  static poly_int<N, C> from (const poly_int_pod<N, Ca> &, signop);

  bool to_shwi (poly_int_pod<N, HOST_WIDE_INT> *) const;
  bool to_uhwi (poly_int_pod<N, unsigned HOST_WIDE_INT> *) const;
  poly_int<N, HOST_WIDE_INT> force_shwi () const;
  poly_int<N, unsigned HOST_WIDE_INT> force_uhwi () const;

#if POLY_INT_CONVERSION
  operator C () const;
#endif

  C coeffs[N];
};

template<unsigned int N, typename C>
template<typename Ca>
inline poly_int_pod<N, C>&
poly_int_pod<N, C>::operator = (const poly_int_pod<N, Ca> &a)
{
  for (unsigned int i = 0; i < N; i++)
    POLY_SET_COEFF (C, *this, i, a.coeffs[i]);
  return *this;
}

template<unsigned int N, typename C>
template<typename Ca>
inline typename if_nonpoly<Ca, poly_int_pod<N, C> >::type &
poly_int_pod<N, C>::operator = (const Ca &a)
{
  POLY_SET_COEFF (C, *this, 0, a);
  if (N >= 2)
    for (unsigned int i = 1; i < N; i++)
      POLY_SET_COEFF (C, *this, i, wi::ints_for<C>::zero (this->coeffs[0]));
  return *this;
}

template<unsigned int N, typename C>
template<typename Ca>
inline poly_int_pod<N, C>&
poly_int_pod<N, C>::operator += (const poly_int_pod<N, Ca> &a)
{
  for (unsigned int i = 0; i < N; i++)
    this->coeffs[i] += a.coeffs[i];
  return *this;
}

template<unsigned int N, typename C>
template<typename Ca>
inline typename if_nonpoly<Ca, poly_int_pod<N, C> >::type &
poly_int_pod<N, C>::operator += (const Ca &a)
{
  this->coeffs[0] += a;
  return *this;
}

template<unsigned int N, typename C>
template<typename Ca>
inline poly_int_pod<N, C>&
poly_int_pod<N, C>::operator -= (const poly_int_pod<N, Ca> &a)
{
  for (unsigned int i = 0; i < N; i++)
    this->coeffs[i] -= a.coeffs[i];
  return *this;
}

template<unsigned int N, typename C>
template<typename Ca>
inline typename if_nonpoly<Ca, poly_int_pod<N, C> >::type &
poly_int_pod<N, C>::operator -= (const Ca &a)
{
  this->coeffs[0] -= a;
  return *this;
}

template<unsigned int N, typename C>
template<typename Ca>
inline typename if_nonpoly<Ca, poly_int_pod<N, C> >::type &
poly_int_pod<N, C>::operator *= (const Ca &a)
{
  for (unsigned int i = 0; i < N; i++)
    this->coeffs[i] *= a;
  return *this;
}

template<unsigned int N, typename C>
inline poly_int_pod<N, C>&
poly_int_pod<N, C>::operator <<= (unsigned int a)
{
  for (unsigned int i = 0; i < N; i++)
    this->coeffs[i] <<= a;
  return *this;
}

/* Return true if the polynomial value is a compile-time constant.  */

template<unsigned int N, typename C>
inline bool
poly_int_pod<N, C>::is_constant () const
{
  if (N >= 2)
    for (unsigned int i = 1; i < N; i++)
      if (this->coeffs[i] != 0)
	return false;
  return true;
}

/* Return true if the polynomial value is a compile-time constant,
   storing its value in CONST_VALUE if so.  */

template<unsigned int N, typename C>
template<typename T>
inline typename if_lossless<T, C, bool>::type
poly_int_pod<N, C>::is_constant (T *const_value) const
{
  if (is_constant ())
    {
      *const_value = this->coeffs[0];
      return true;
    }
  return false;
}

/* Return the value of a polynomial that is already known to be a
   compile-time constant.

   NOTE: When using this function, please add a comment above the call
   explaining why we know the value is constant in that context.  */

template<unsigned int N, typename C>
inline C
poly_int_pod<N, C>::to_constant () const
{
  gcc_checking_assert (is_constant ());
  return this->coeffs[0];
}

/* Convert X to a wide_int-based polynomial in which each coefficient
   has BITSIZE bits.  If X's coefficients are smaller than BITSIZE,
   extend them according to SGN.  */

template<unsigned int N, typename C>
template<typename Ca>
inline poly_int<N, C>
poly_int_pod<N, C>::from (const poly_int_pod<N, Ca> &a,
			  unsigned int bitsize, signop sgn)
{
  poly_int<N, C> r;
  for (unsigned int i = 0; i < N; i++)
    POLY_SET_COEFF (C, r, i, C::from (a.coeffs[i], bitsize, sgn));
  return r;
}

/* Convert X to a fixed_wide_int-based polynomial, extending according
   to SGN.  */

template<unsigned int N, typename C>
template<typename Ca>
inline poly_int<N, C>
poly_int_pod<N, C>::from (const poly_int_pod<N, Ca> &a, signop sgn)
{
  poly_int<N, C> r;
  for (unsigned int i = 0; i < N; i++)
    POLY_SET_COEFF (C, r, i, C::from (a.coeffs[i], sgn));
  return r;
}

/* Return true if the coefficients of this generic_wide_int-based
   polynomial can be represented as signed HOST_WIDE_INTs without loss
   of precision.  Store the HOST_WIDE_INT representation in *R if so.  */

template<unsigned int N, typename C>
inline bool
poly_int_pod<N, C>::to_shwi (poly_int_pod<N, HOST_WIDE_INT> *r) const
{
  for (unsigned int i = 0; i < N; i++)
    if (!wi::fits_shwi_p (this->coeffs[i]))
      return false;
  for (unsigned int i = 0; i < N; i++)
    r->coeffs[i] = this->coeffs[i].to_shwi ();
  return true;
}

/* Return true if the coefficients of this generic_wide_int-based
   polynomial can be represented as unsigned HOST_WIDE_INTs without
   loss of precision.  Store the unsigned HOST_WIDE_INT representation
   in *R if so.  */

template<unsigned int N, typename C>
inline bool
poly_int_pod<N, C>::to_uhwi (poly_int_pod<N, unsigned HOST_WIDE_INT> *r) const
{
  for (unsigned int i = 0; i < N; i++)
    if (!wi::fits_uhwi_p (this->coeffs[i]))
      return false;
  for (unsigned int i = 0; i < N; i++)
    r->coeffs[i] = this->coeffs[i].to_uhwi ();
  return true;
}

/* Force a generic_wide_int-based constant to HOST_WIDE_INT precision,
   truncating if necessary.  */

template<unsigned int N, typename C>
inline poly_int<N, HOST_WIDE_INT>
poly_int_pod<N, C>::force_shwi () const
{
  poly_int_pod<N, HOST_WIDE_INT> r;
  for (unsigned int i = 0; i < N; i++)
    r.coeffs[i] = this->coeffs[i].to_shwi ();
  return r;
}

/* Force a generic_wide_int-based constant to unsigned HOST_WIDE_INT precision,
   truncating if necessary.  */

template<unsigned int N, typename C>
inline poly_int<N, unsigned HOST_WIDE_INT>
poly_int_pod<N, C>::force_uhwi () const
{
  poly_int_pod<N, unsigned HOST_WIDE_INT> r;
  for (unsigned int i = 0; i < N; i++)
    r.coeffs[i] = this->coeffs[i].to_uhwi ();
  return r;
}

#if POLY_INT_CONVERSION
/* Provide a conversion operator to constants.  */

template<unsigned int N, typename C>
inline
poly_int_pod<N, C>::operator C () const
{
  gcc_checking_assert (this->is_constant ());
  return this->coeffs[0];
}
#endif

/* The main class for polynomial integers.  The class provides
   constructors that are necessarily missing from the POD base.  */
template<unsigned int N, typename C>
class poly_int : public poly_int_pod<N, C>
{
public:
  poly_int () {}

  template<typename Ca>
  poly_int (const poly_int<N, Ca> &);
  template<typename Ca>
  poly_int (const poly_int_pod<N, Ca> &);
  template<typename C0>
  poly_int (const C0 &);
  template<typename C0, typename C1>
  poly_int (const C0 &, const C1 &);

  template<typename Ca>
  poly_int &operator = (const poly_int_pod<N, Ca> &);
  template<typename Ca>
  typename if_nonpoly<Ca, poly_int>::type &operator = (const Ca &);

  template<typename Ca>
  poly_int &operator += (const poly_int_pod<N, Ca> &);
  template<typename Ca>
  typename if_nonpoly<Ca, poly_int>::type &operator += (const Ca &);

  template<typename Ca>
  poly_int &operator -= (const poly_int_pod<N, Ca> &);
  template<typename Ca>
  typename if_nonpoly<Ca, poly_int>::type &operator -= (const Ca &);

  template<typename Ca>
  typename if_nonpoly<Ca, poly_int>::type &operator *= (const Ca &);

  poly_int &operator <<= (unsigned int);
};

template<unsigned int N, typename C>
template<typename Ca>
inline
poly_int<N, C>::poly_int (const poly_int<N, Ca> &a)
{
  for (unsigned int i = 0; i < N; i++)
    POLY_SET_COEFF (C, *this, i, a.coeffs[i]);
}

template<unsigned int N, typename C>
template<typename Ca>
inline
poly_int<N, C>::poly_int (const poly_int_pod<N, Ca> &a)
{
  for (unsigned int i = 0; i < N; i++)
    POLY_SET_COEFF (C, *this, i, a.coeffs[i]);
}

template<unsigned int N, typename C>
template<typename C0>
inline
poly_int<N, C>::poly_int (const C0 &c0)
{
  POLY_SET_COEFF (C, *this, 0, c0);
  for (unsigned int i = 1; i < N; i++)
    POLY_SET_COEFF (C, *this, i, wi::ints_for<C>::zero (this->coeffs[0]));
}

template<unsigned int N, typename C>
template<typename C0, typename C1>
inline
poly_int<N, C>::poly_int (const C0 &c0, const C1 &c1)
{
  STATIC_ASSERT (N >= 2);
  POLY_SET_COEFF (C, *this, 0, c0);
  POLY_SET_COEFF (C, *this, 1, c1);
  for (unsigned int i = 2; i < N; i++)
    POLY_SET_COEFF (C, *this, i, wi::ints_for<C>::zero (this->coeffs[0]));
}

template<unsigned int N, typename C>
template<typename Ca>
inline poly_int<N, C>&
poly_int<N, C>::operator = (const poly_int_pod<N, Ca> &a)
{
  for (unsigned int i = 0; i < N; i++)
    this->coeffs[i] = a.coeffs[i];
  return *this;
}

template<unsigned int N, typename C>
template<typename Ca>
inline typename if_nonpoly<Ca, poly_int<N, C> >::type &
poly_int<N, C>::operator = (const Ca &a)
{
  this->coeffs[0] = a;
  if (N >= 2)
    for (unsigned int i = 1; i < N; i++)
      this->coeffs[i] = wi::ints_for<C>::zero (this->coeffs[0]);
  return *this;
}

template<unsigned int N, typename C>
template<typename Ca>
inline poly_int<N, C>&
poly_int<N, C>::operator += (const poly_int_pod<N, Ca> &a)
{
  for (unsigned int i = 0; i < N; i++)
    this->coeffs[i] += a.coeffs[i];
  return *this;
}

template<unsigned int N, typename C>
template<typename Ca>
inline typename if_nonpoly<Ca, poly_int<N, C> >::type &
poly_int<N, C>::operator += (const Ca &a)
{
  this->coeffs[0] += a;
  return *this;
}

template<unsigned int N, typename C>
template<typename Ca>
inline poly_int<N, C>&
poly_int<N, C>::operator -= (const poly_int_pod<N, Ca> &a)
{
  for (unsigned int i = 0; i < N; i++)
    this->coeffs[i] -= a.coeffs[i];
  return *this;
}

template<unsigned int N, typename C>
template<typename Ca>
inline typename if_nonpoly<Ca, poly_int<N, C> >::type &
poly_int<N, C>::operator -= (const Ca &a)
{
  this->coeffs[0] -= a;
  return *this;
}

template<unsigned int N, typename C>
template<typename Ca>
inline typename if_nonpoly<Ca, poly_int<N, C> >::type &
poly_int<N, C>::operator *= (const Ca &a)
{
  for (unsigned int i = 0; i < N; i++)
    this->coeffs[i] *= a;
  return *this;
}

template<unsigned int N, typename C>
inline poly_int<N, C>&
poly_int<N, C>::operator <<= (unsigned int a)
{
  for (unsigned int i = 0; i < N; i++)
    this->coeffs[i] <<= a;
  return *this;
}

/* Return true if every coefficient of A is in the inclusive range [B, C].  */

template<typename Ca, typename Cb, typename Cc>
inline typename if_nonpoly<Ca, bool>::type
coeffs_in_range_p (const Ca &a, const Cb &b, const Cc &c)
{
  return a >= b && a <= c;
}

template<unsigned int N, typename Ca, typename Cb, typename Cc>
inline typename if_nonpoly<Ca, bool>::type
coeffs_in_range_p (const poly_int_pod<N, Ca> &a, const Cb &b, const Cc &c)
{
  for (unsigned int i = 0; i < N; i++)
    if (a.coeffs[i] < b || a.coeffs[i] > c)
      return false;
  return true;
}

namespace wi {
/* Poly version of wi::shwi, with the same interface.  */

template<unsigned int N>
inline poly_int<N, hwi_with_prec>
shwi (const poly_int_pod<N, HOST_WIDE_INT> &a, unsigned int precision)
{
  poly_int<N, hwi_with_prec> r;
  for (unsigned int i = 0; i < N; i++)
    POLY_SET_COEFF (hwi_with_prec, r, i, wi::shwi (a.coeffs[i], precision));
  return r;
}

/* Poly version of wi::uhwi, with the same interface.  */

template<unsigned int N>
inline poly_int<N, hwi_with_prec>
uhwi (const poly_int_pod<N, unsigned HOST_WIDE_INT> &a, unsigned int precision)
{
  poly_int<N, hwi_with_prec> r;
  for (unsigned int i = 0; i < N; i++)
    POLY_SET_COEFF (hwi_with_prec, r, i, wi::uhwi (a.coeffs[i], precision));
  return r;
}

/* Poly version of wi::sext, with the same interface.  */

template<unsigned int N, typename Ca>
inline POLY_POLY_RESULT (N, Ca, Ca)
sext (const poly_int_pod<N, Ca> &a, unsigned int precision)
{
  typedef POLY_POLY_COEFF (Ca, Ca) C;
  poly_int<N, C> r;
  for (unsigned int i = 0; i < N; i++)
    POLY_SET_COEFF (C, r, i, wi::sext (a.coeffs[i], precision));
  return r;
}

/* Poly version of wi::zext, with the same interface.  */

template<unsigned int N, typename Ca>
inline POLY_POLY_RESULT (N, Ca, Ca)
zext (const poly_int_pod<N, Ca> &a, unsigned int precision)
{
  typedef POLY_POLY_COEFF (Ca, Ca) C;
  poly_int<N, C> r;
  for (unsigned int i = 0; i < N; i++)
    POLY_SET_COEFF (C, r, i, wi::zext (a.coeffs[i], precision));
  return r;
}
}

template<unsigned int N, typename Ca, typename Cb>
inline POLY_POLY_RESULT (N, Ca, Cb)
operator + (const poly_int_pod<N, Ca> &a, const poly_int_pod<N, Cb> &b)
{
  typedef POLY_CAST (Ca, Cb) NCa;
  typedef POLY_POLY_COEFF (Ca, Cb) C;
  poly_int<N, C> r;
  for (unsigned int i = 0; i < N; i++)
    POLY_SET_COEFF (C, r, i, NCa (a.coeffs[i]) + b.coeffs[i]);
  return r;
}

template<unsigned int N, typename Ca, typename Cb>
inline POLY_CONST_RESULT (N, Ca, Cb)
operator + (const poly_int_pod<N, Ca> &a, const Cb &b)
{
  typedef POLY_CAST (Ca, Cb) NCa;
  typedef POLY_CONST_COEFF (Ca, Cb) C;
  poly_int<N, C> r;
  POLY_SET_COEFF (C, r, 0, NCa (a.coeffs[0]) + b);
  if (N >= 2)
    for (unsigned int i = 1; i < N; i++)
      POLY_SET_COEFF (C, r, i, NCa (a.coeffs[i]));
  return r;
}

template<unsigned int N, typename Ca, typename Cb>
inline CONST_POLY_RESULT (N, Ca, Cb)
operator + (const Ca &a, const poly_int_pod<N, Cb> &b)
{
  typedef POLY_CAST (Cb, Ca) NCb;
  typedef CONST_POLY_COEFF (Ca, Cb) C;
  poly_int<N, C> r;
  POLY_SET_COEFF (C, r, 0, a + NCb (b.coeffs[0]));
  if (N >= 2)
    for (unsigned int i = 1; i < N; i++)
      POLY_SET_COEFF (C, r, i, NCb (b.coeffs[i]));
  return r;
}

namespace wi {
/* Poly versions of wi::add, with the same interface.  */

template<unsigned int N, typename Ca, typename Cb>
inline poly_int<N, WI_BINARY_RESULT (Ca, Cb)>
add (const poly_int_pod<N, Ca> &a, const poly_int_pod<N, Cb> &b)
{
  typedef WI_BINARY_RESULT (Ca, Cb) C;
  poly_int<N, C> r;
  for (unsigned int i = 0; i < N; i++)
    POLY_SET_COEFF (C, r, i, wi::add (a.coeffs[i], b.coeffs[i]));
  return r;
}

template<unsigned int N, typename Ca, typename Cb>
inline poly_int<N, WI_BINARY_RESULT (Ca, Cb)>
add (const poly_int_pod<N, Ca> &a, const Cb &b)
{
  typedef WI_BINARY_RESULT (Ca, Cb) C;
  poly_int<N, C> r;
  POLY_SET_COEFF (C, r, 0, wi::add (a.coeffs[0], b));
  for (unsigned int i = 1; i < N; i++)
    POLY_SET_COEFF (C, r, i, wi::add (a.coeffs[i],
				      wi::ints_for<Cb>::zero (b)));
  return r;
}

template<unsigned int N, typename Ca, typename Cb>
inline poly_int<N, WI_BINARY_RESULT (Ca, Cb)>
add (const Ca &a, const poly_int_pod<N, Cb> &b)
{
  typedef WI_BINARY_RESULT (Ca, Cb) C;
  poly_int<N, C> r;
  POLY_SET_COEFF (C, r, 0, wi::add (a, b.coeffs[0]));
  for (unsigned int i = 1; i < N; i++)
    POLY_SET_COEFF (C, r, i, wi::add (wi::ints_for<Ca>::zero (a),
				      b.coeffs[i]));
  return r;
}

template<unsigned int N, typename Ca, typename Cb>
inline poly_int<N, WI_BINARY_RESULT (Ca, Cb)>
add (const poly_int_pod<N, Ca> &a, const poly_int_pod<N, Cb> &b,
     signop sgn, bool *overflow)
{
  typedef WI_BINARY_RESULT (Ca, Cb) C;
  poly_int<N, C> r;
  POLY_SET_COEFF (C, r, 0, wi::add (a.coeffs[0], b.coeffs[0], sgn, overflow));
  for (unsigned int i = 1; i < N; i++)
    {
      bool suboverflow;
      POLY_SET_COEFF (C, r, i, wi::add (a.coeffs[i], b.coeffs[i], sgn,
					&suboverflow));
      *overflow |= suboverflow;
    }
  return r;
}
}

template<unsigned int N, typename Ca, typename Cb>
inline POLY_POLY_RESULT (N, Ca, Cb)
operator - (const poly_int_pod<N, Ca> &a, const poly_int_pod<N, Cb> &b)
{
  typedef POLY_CAST (Ca, Cb) NCa;
  typedef POLY_POLY_COEFF (Ca, Cb) C;
  poly_int<N, C> r;
  for (unsigned int i = 0; i < N; i++)
    POLY_SET_COEFF (C, r, i, NCa (a.coeffs[i]) - b.coeffs[i]);
  return r;
}

template<unsigned int N, typename Ca, typename Cb>
inline POLY_CONST_RESULT (N, Ca, Cb)
operator - (const poly_int_pod<N, Ca> &a, const Cb &b)
{
  typedef POLY_CAST (Ca, Cb) NCa;
  typedef POLY_CONST_COEFF (Ca, Cb) C;
  poly_int<N, C> r;
  POLY_SET_COEFF (C, r, 0, NCa (a.coeffs[0]) - b);
  if (N >= 2)
    for (unsigned int i = 1; i < N; i++)
      POLY_SET_COEFF (C, r, i, NCa (a.coeffs[i]));
  return r;
}

template<unsigned int N, typename Ca, typename Cb>
inline CONST_POLY_RESULT (N, Ca, Cb)
operator - (const Ca &a, const poly_int_pod<N, Cb> &b)
{
  typedef POLY_CAST (Cb, Ca) NCb;
  typedef CONST_POLY_COEFF (Ca, Cb) C;
  poly_int<N, C> r;
  POLY_SET_COEFF (C, r, 0, a - NCb (b.coeffs[0]));
  if (N >= 2)
    for (unsigned int i = 1; i < N; i++)
      POLY_SET_COEFF (C, r, i, -NCb (b.coeffs[i]));
  return r;
}

namespace wi {
/* Poly versions of wi::sub, with the same interface.  */

template<unsigned int N, typename Ca, typename Cb>
inline poly_int<N, WI_BINARY_RESULT (Ca, Cb)>
sub (const poly_int_pod<N, Ca> &a, const poly_int_pod<N, Cb> &b)
{
  typedef WI_BINARY_RESULT (Ca, Cb) C;
  poly_int<N, C> r;
  for (unsigned int i = 0; i < N; i++)
    POLY_SET_COEFF (C, r, i, wi::sub (a.coeffs[i], b.coeffs[i]));
  return r;
}

template<unsigned int N, typename Ca, typename Cb>
inline poly_int<N, WI_BINARY_RESULT (Ca, Cb)>
sub (const poly_int_pod<N, Ca> &a, const Cb &b)
{
  typedef WI_BINARY_RESULT (Ca, Cb) C;
  poly_int<N, C> r;
  POLY_SET_COEFF (C, r, 0, wi::sub (a.coeffs[0], b));
  for (unsigned int i = 1; i < N; i++)
    POLY_SET_COEFF (C, r, i, wi::sub (a.coeffs[i],
				      wi::ints_for<Cb>::zero (b)));
  return r;
}

template<unsigned int N, typename Ca, typename Cb>
inline poly_int<N, WI_BINARY_RESULT (Ca, Cb)>
sub (const Ca &a, const poly_int_pod<N, Cb> &b)
{
  typedef WI_BINARY_RESULT (Ca, Cb) C;
  poly_int<N, C> r;
  POLY_SET_COEFF (C, r, 0, wi::sub (a, b.coeffs[0]));
  for (unsigned int i = 1; i < N; i++)
    POLY_SET_COEFF (C, r, i, wi::sub (wi::ints_for<Ca>::zero (a),
				      b.coeffs[i]));
  return r;
}

template<unsigned int N, typename Ca, typename Cb>
inline poly_int<N, WI_BINARY_RESULT (Ca, Cb)>
sub (const poly_int_pod<N, Ca> &a, const poly_int_pod<N, Cb> &b,
     signop sgn, bool *overflow)
{
  typedef WI_BINARY_RESULT (Ca, Cb) C;
  poly_int<N, C> r;
  POLY_SET_COEFF (C, r, 0, wi::sub (a.coeffs[0], b.coeffs[0], sgn, overflow));
  for (unsigned int i = 1; i < N; i++)
    {
      bool suboverflow;
      POLY_SET_COEFF (C, r, i, wi::sub (a.coeffs[i], b.coeffs[i], sgn,
					&suboverflow));
      *overflow |= suboverflow;
    }
  return r;
}
}

template<unsigned int N, typename Ca>
inline POLY_POLY_RESULT (N, Ca, Ca)
operator - (const poly_int_pod<N, Ca> &a)
{
  typedef POLY_CAST (Ca, Ca) NCa;
  typedef POLY_POLY_COEFF (Ca, Ca) C;
  poly_int<N, C> r;
  for (unsigned int i = 0; i < N; i++)
    POLY_SET_COEFF (C, r, i, -NCa (a.coeffs[i]));
  return r;
}

namespace wi {
/* Poly version of wi::neg, with the same interface.  */

template<unsigned int N, typename Ca>
inline poly_int<N, WI_UNARY_RESULT (Ca)>
neg (const poly_int_pod<N, Ca> &a)
{
  typedef WI_UNARY_RESULT (Ca) C;
  poly_int<N, C> r;
  for (unsigned int i = 0; i < N; i++)
    POLY_SET_COEFF (C, r, i, wi::neg (a.coeffs[i]));
  return r;
}

template<unsigned int N, typename Ca>
inline poly_int<N, WI_UNARY_RESULT (Ca)>
neg (const poly_int_pod<N, Ca> &a, bool *overflow)
{
  typedef WI_UNARY_RESULT (Ca) C;
  poly_int<N, C> r;
  POLY_SET_COEFF (C, r, 0, wi::neg (a.coeffs[0], overflow));
  for (unsigned int i = 1; i < N; i++)
    {
      bool suboverflow;
      POLY_SET_COEFF (C, r, i, wi::neg (a.coeffs[i], &suboverflow));
      *overflow |= suboverflow;
    }
  return r;
}
}

template<unsigned int N, typename Ca>
inline POLY_POLY_RESULT (N, Ca, Ca)
operator ~ (const poly_int_pod<N, Ca> &a)
{
  if (N >= 2)
    return -1 - a;
  return ~a.coeffs[0];
}

template<unsigned int N, typename Ca, typename Cb>
inline POLY_CONST_RESULT (N, Ca, Cb)
operator * (const poly_int_pod<N, Ca> &a, const Cb &b)
{
  typedef POLY_CAST (Ca, Cb) NCa;
  typedef POLY_CONST_COEFF (Ca, Cb) C;
  poly_int<N, C> r;
  for (unsigned int i = 0; i < N; i++)
    POLY_SET_COEFF (C, r, i, NCa (a.coeffs[i]) * b);
  return r;
}

template<unsigned int N, typename Ca, typename Cb>
inline CONST_POLY_RESULT (N, Ca, Cb)
operator * (const Ca &a, const poly_int_pod<N, Cb> &b)
{
  typedef POLY_CAST (Ca, Cb) NCa;
  typedef CONST_POLY_COEFF (Ca, Cb) C;
  poly_int<N, C> r;
  for (unsigned int i = 0; i < N; i++)
    POLY_SET_COEFF (C, r, i, NCa (a) * b.coeffs[i]);
  return r;
}

namespace wi {
/* Poly versions of wi::mul, with the same interface.  */

template<unsigned int N, typename Ca, typename Cb>
inline poly_int<N, WI_BINARY_RESULT (Ca, Cb)>
mul (const poly_int_pod<N, Ca> &a, const Cb &b)
{
  typedef WI_BINARY_RESULT (Ca, Cb) C;
  poly_int<N, C> r;
  for (unsigned int i = 0; i < N; i++)
    POLY_SET_COEFF (C, r, i, wi::mul (a.coeffs[i], b));
  return r;
}

template<unsigned int N, typename Ca, typename Cb>
inline poly_int<N, WI_BINARY_RESULT (Ca, Cb)>
mul (const Ca &a, const poly_int_pod<N, Cb> &b)
{
  typedef WI_BINARY_RESULT (Ca, Cb) C;
  poly_int<N, C> r;
  for (unsigned int i = 0; i < N; i++)
    POLY_SET_COEFF (C, r, i, wi::mul (a, b.coeffs[i]));
  return r;
}

template<unsigned int N, typename Ca, typename Cb>
inline poly_int<N, WI_BINARY_RESULT (Ca, Cb)>
mul (const poly_int_pod<N, Ca> &a, const Cb &b,
     signop sgn, bool *overflow)
{
  typedef WI_BINARY_RESULT (Ca, Cb) C;
  poly_int<N, C> r;
  POLY_SET_COEFF (C, r, 0, wi::mul (a.coeffs[0], b, sgn, overflow));
  for (unsigned int i = 1; i < N; i++)
    {
      bool suboverflow;
      POLY_SET_COEFF (C, r, i, wi::mul (a.coeffs[i], b, sgn, &suboverflow));
      *overflow |= suboverflow;
    }
  return r;
}
}

template<unsigned int N, typename Ca, typename Cb>
inline POLY_POLY_RESULT (N, Ca, Ca)
operator << (const poly_int_pod<N, Ca> &a, const Cb &b)
{
  typedef POLY_CAST (Ca, Ca) NCa;
  typedef POLY_POLY_COEFF (Ca, Ca) C;
  poly_int<N, C> r;
  for (unsigned int i = 0; i < N; i++)
    POLY_SET_COEFF (C, r, i, NCa (a.coeffs[i]) << b);
  return r;
}

namespace wi {
/* Poly version of wi::lshift, with the same interface.  */

template<unsigned int N, typename Ca, typename Cb>
inline poly_int<N, WI_BINARY_RESULT (Ca, Ca)>
lshift (const poly_int_pod<N, Ca> &a, const Cb &b)
{
  typedef WI_BINARY_RESULT (Ca, Ca) C;
  poly_int<N, C> r;
  for (unsigned int i = 0; i < N; i++)
    POLY_SET_COEFF (C, r, i, wi::lshift (a.coeffs[i], b));
  return r;
}
}

/* Return true if a0 + a1 * x might equal b0 + b1 * x for some nonnegative
   integer x.  */

template<typename Ca, typename Cb>
inline bool
maybe_eq_2 (const Ca &a0, const Ca &a1, const Cb &b0, const Cb &b1)
{
  if (a1 != b1)
     /*      a0 + a1 * x == b0 + b1 * x
       ==> (a1 - b1) * x == b0 - a0
       ==>             x == (b0 - a0) / (a1 - b1)

       We need to test whether that's a valid value of x.
       (b0 - a0) and (a1 - b1) must not have opposite signs
       and the result must be integral.  */
    return (a1 < b1
	    ? b0 <= a0 && (a0 - b0) % (b1 - a1) == 0
	    : b0 >= a0 && (b0 - a0) % (a1 - b1) == 0);
  return a0 == b0;
}

/* Return true if a0 + a1 * x might equal b for some nonnegative
   integer x.  */

template<typename Ca, typename Cb>
inline bool
maybe_eq_2 (const Ca &a0, const Ca &a1, const Cb &b)
{
  if (a1 != 0)
     /*      a0 + a1 * x == b
       ==>             x == (b - a0) / a1

       We need to test whether that's a valid value of x.
       (b - a0) and a1 must not have opposite signs and the
       result must be integral.  */
    return (a1 < 0
	    ? b <= a0 && (a0 - b) % a1 == 0
	    : b >= a0 && (b - a0) % a1 == 0);
  return a0 == b;
}

/* Return true if A might equal B for some indeterminate values.  */

template<unsigned int N, typename Ca, typename Cb>
inline bool
maybe_eq (const poly_int_pod<N, Ca> &a, const poly_int_pod<N, Cb> &b)
{
  STATIC_ASSERT (N <= 2);
  if (N == 2)
    return maybe_eq_2 (a.coeffs[0], a.coeffs[1], b.coeffs[0], b.coeffs[1]);
  return a.coeffs[0] == b.coeffs[0];
}

template<unsigned int N, typename Ca, typename Cb>
inline typename if_nonpoly<Cb, bool>::type
maybe_eq (const poly_int_pod<N, Ca> &a, const Cb &b)
{
  STATIC_ASSERT (N <= 2);
  if (N == 2)
    return maybe_eq_2 (a.coeffs[0], a.coeffs[1], b);
  return a.coeffs[0] == b;
}

template<unsigned int N, typename Ca, typename Cb>
inline typename if_nonpoly<Ca, bool>::type
maybe_eq (const Ca &a, const poly_int_pod<N, Cb> &b)
{
  STATIC_ASSERT (N <= 2);
  if (N == 2)
    return maybe_eq_2 (b.coeffs[0], b.coeffs[1], a);
  return a == b.coeffs[0];
}

template<typename Ca, typename Cb>
inline typename if_nonpoly2<Ca, Cb, bool>::type
maybe_eq (const Ca &a, const Cb &b)
{
  return a == b;
}

/* Return true if A might not equal B for some indeterminate values.  */

template<unsigned int N, typename Ca, typename Cb>
inline bool
maybe_ne (const poly_int_pod<N, Ca> &a, const poly_int_pod<N, Cb> &b)
{
  if (N >= 2)
    for (unsigned int i = 1; i < N; i++)
      if (a.coeffs[i] != b.coeffs[i])
	return true;
  return a.coeffs[0] != b.coeffs[0];
}

template<unsigned int N, typename Ca, typename Cb>
inline typename if_nonpoly<Cb, bool>::type
maybe_ne (const poly_int_pod<N, Ca> &a, const Cb &b)
{
  if (N >= 2)
    for (unsigned int i = 1; i < N; i++)
      if (a.coeffs[i] != 0)
	return true;
  return a.coeffs[0] != b;
}

template<unsigned int N, typename Ca, typename Cb>
inline typename if_nonpoly<Ca, bool>::type
maybe_ne (const Ca &a, const poly_int_pod<N, Cb> &b)
{
  if (N >= 2)
    for (unsigned int i = 1; i < N; i++)
      if (b.coeffs[i] != 0)
	return true;
  return a != b.coeffs[0];
}

template<typename Ca, typename Cb>
inline typename if_nonpoly2<Ca, Cb, bool>::type
maybe_ne (const Ca &a, const Cb &b)
{
  return a != b;
}

/* Return true if A is known to be equal to B.  */
#define known_eq(A, B) (!maybe_ne (A, B))

/* Return true if A is known to be unequal to B.  */
#define known_ne(A, B) (!maybe_eq (A, B))

/* Return true if A might be less than or equal to B for some
   indeterminate values.  */

template<unsigned int N, typename Ca, typename Cb>
inline bool
maybe_le (const poly_int_pod<N, Ca> &a, const poly_int_pod<N, Cb> &b)
{
  if (N >= 2)
    for (unsigned int i = 1; i < N; i++)
      if (a.coeffs[i] < b.coeffs[i])
	return true;
  return a.coeffs[0] <= b.coeffs[0];
}

template<unsigned int N, typename Ca, typename Cb>
inline typename if_nonpoly<Cb, bool>::type
maybe_le (const poly_int_pod<N, Ca> &a, const Cb &b)
{
  if (N >= 2)
    for (unsigned int i = 1; i < N; i++)
      if (a.coeffs[i] < 0)
	return true;
  return a.coeffs[0] <= b;
}

template<unsigned int N, typename Ca, typename Cb>
inline typename if_nonpoly<Ca, bool>::type
maybe_le (const Ca &a, const poly_int_pod<N, Cb> &b)
{
  if (N >= 2)
    for (unsigned int i = 1; i < N; i++)
      if (b.coeffs[i] > 0)
	return true;
  return a <= b.coeffs[0];
}

template<typename Ca, typename Cb>
inline typename if_nonpoly2<Ca, Cb, bool>::type
maybe_le (const Ca &a, const Cb &b)
{
  return a <= b;
}

/* Return true if A might be less than B for some indeterminate values.  */

template<unsigned int N, typename Ca, typename Cb>
inline bool
maybe_lt (const poly_int_pod<N, Ca> &a, const poly_int_pod<N, Cb> &b)
{
  if (N >= 2)
    for (unsigned int i = 1; i < N; i++)
      if (a.coeffs[i] < b.coeffs[i])
	return true;
  return a.coeffs[0] < b.coeffs[0];
}

template<unsigned int N, typename Ca, typename Cb>
inline typename if_nonpoly<Cb, bool>::type
maybe_lt (const poly_int_pod<N, Ca> &a, const Cb &b)
{
  if (N >= 2)
    for (unsigned int i = 1; i < N; i++)
      if (a.coeffs[i] < 0)
	return true;
  return a.coeffs[0] < b;
}

template<unsigned int N, typename Ca, typename Cb>
inline typename if_nonpoly<Ca, bool>::type
maybe_lt (const Ca &a, const poly_int_pod<N, Cb> &b)
{
  if (N >= 2)
    for (unsigned int i = 1; i < N; i++)
      if (b.coeffs[i] > 0)
	return true;
  return a < b.coeffs[0];
}

template<typename Ca, typename Cb>
inline typename if_nonpoly2<Ca, Cb, bool>::type
maybe_lt (const Ca &a, const Cb &b)
{
  return a < b;
}

/* Return true if A may be greater than or equal to B.  */
#define maybe_ge(A, B) maybe_le (B, A)

/* Return true if A may be greater than B.  */
#define maybe_gt(A, B) maybe_lt (B, A)

/* Return true if A is known to be less than or equal to B.  */
#define known_le(A, B) (!maybe_gt (A, B))

/* Return true if A is known to be less than B.  */
#define known_lt(A, B) (!maybe_ge (A, B))

/* Return true if A is known to be greater than B.  */
#define known_gt(A, B) (!maybe_le (A, B))

/* Return true if A is known to be greater than or equal to B.  */
#define known_ge(A, B) (!maybe_lt (A, B))

/* Return true if A and B are ordered by the partial ordering known_le.  */

template<typename T1, typename T2>
inline bool
ordered_p (const T1 &a, const T2 &b)
{
  return ((poly_int_traits<T1>::num_coeffs == 1
	   && poly_int_traits<T2>::num_coeffs == 1)
	  || known_le (a, b)
	  || known_le (b, a));
}

/* Assert that A and B are known to be ordered and return the minimum
   of the two.

   NOTE: When using this function, please add a comment above the call
   explaining why we know the values are ordered in that context.  */

template<unsigned int N, typename Ca, typename Cb>
inline POLY_POLY_RESULT (N, Ca, Cb)
ordered_min (const poly_int_pod<N, Ca> &a, const poly_int_pod<N, Cb> &b)
{
  if (known_le (a, b))
    return a;
  else
    {
      if (N > 1)
	gcc_checking_assert (known_le (b, a));
      return b;
    }
}

template<unsigned int N, typename Ca, typename Cb>
inline CONST_POLY_RESULT (N, Ca, Cb)
ordered_min (const Ca &a, const poly_int_pod<N, Cb> &b)
{
  if (known_le (a, b))
    return a;
  else
    {
      if (N > 1)
	gcc_checking_assert (known_le (b, a));
      return b;
    }
}

template<unsigned int N, typename Ca, typename Cb>
inline POLY_CONST_RESULT (N, Ca, Cb)
ordered_min (const poly_int_pod<N, Ca> &a, const Cb &b)
{
  if (known_le (a, b))
    return a;
  else
    {
      if (N > 1)
	gcc_checking_assert (known_le (b, a));
      return b;
    }
}

/* Assert that A and B are known to be ordered and return the maximum
   of the two.

   NOTE: When using this function, please add a comment above the call
   explaining why we know the values are ordered in that context.  */

template<unsigned int N, typename Ca, typename Cb>
inline POLY_POLY_RESULT (N, Ca, Cb)
ordered_max (const poly_int_pod<N, Ca> &a, const poly_int_pod<N, Cb> &b)
{
  if (known_le (a, b))
    return b;
  else
    {
      if (N > 1)
	gcc_checking_assert (known_le (b, a));
      return a;
    }
}

template<unsigned int N, typename Ca, typename Cb>
inline CONST_POLY_RESULT (N, Ca, Cb)
ordered_max (const Ca &a, const poly_int_pod<N, Cb> &b)
{
  if (known_le (a, b))
    return b;
  else
    {
      if (N > 1)
	gcc_checking_assert (known_le (b, a));
      return a;
    }
}

template<unsigned int N, typename Ca, typename Cb>
inline POLY_CONST_RESULT (N, Ca, Cb)
ordered_max (const poly_int_pod<N, Ca> &a, const Cb &b)
{
  if (known_le (a, b))
    return b;
  else
    {
      if (N > 1)
	gcc_checking_assert (known_le (b, a));
      return a;
    }
}

/* Return a constant lower bound on the value of A, which is known
   to be nonnegative.  */

template<unsigned int N, typename Ca>
inline Ca
constant_lower_bound (const poly_int_pod<N, Ca> &a)
{
  gcc_checking_assert (known_ge (a, POLY_INT_TYPE (Ca) (0)));
  return a.coeffs[0];
}

/* Return a value that is known to be no greater than A and B.  This
   will be the greatest lower bound for some indeterminate values but
   not necessarily for all.  */

template<unsigned int N, typename Ca, typename Cb>
inline POLY_CONST_RESULT (N, Ca, Cb)
lower_bound (const poly_int_pod<N, Ca> &a, const Cb &b)
{
  typedef POLY_CAST (Ca, Cb) NCa;
  typedef POLY_CAST (Cb, Ca) NCb;
  typedef POLY_INT_TYPE (Cb) ICb;
  typedef POLY_CONST_COEFF (Ca, Cb) C;

  poly_int<N, C> r;
  POLY_SET_COEFF (C, r, 0, MIN (NCa (a.coeffs[0]), NCb (b)));
  if (N >= 2)
    for (unsigned int i = 1; i < N; i++)
      POLY_SET_COEFF (C, r, i, MIN (NCa (a.coeffs[i]), ICb (0)));
  return r;
}

template<unsigned int N, typename Ca, typename Cb>
inline CONST_POLY_RESULT (N, Ca, Cb)
lower_bound (const Ca &a, const poly_int_pod<N, Cb> &b)
{
  return lower_bound (b, a);
}

template<unsigned int N, typename Ca, typename Cb>
inline POLY_POLY_RESULT (N, Ca, Cb)
lower_bound (const poly_int_pod<N, Ca> &a, const poly_int_pod<N, Cb> &b)
{
  typedef POLY_CAST (Ca, Cb) NCa;
  typedef POLY_CAST (Cb, Ca) NCb;
  typedef POLY_POLY_COEFF (Ca, Cb) C;

  poly_int<N, C> r;
  for (unsigned int i = 0; i < N; i++)
    POLY_SET_COEFF (C, r, i, MIN (NCa (a.coeffs[i]), NCb (b.coeffs[i])));
  return r;
}

template<typename Ca, typename Cb>
inline CONST_CONST_RESULT (N, Ca, Cb)
lower_bound (const Ca &a, const Cb &b)
{
  return a < b ? a : b;
}

/* Return a value that is known to be no less than A and B.  This will
   be the least upper bound for some indeterminate values but not
   necessarily for all.  */

template<unsigned int N, typename Ca, typename Cb>
inline POLY_CONST_RESULT (N, Ca, Cb)
upper_bound (const poly_int_pod<N, Ca> &a, const Cb &b)
{
  typedef POLY_CAST (Ca, Cb) NCa;
  typedef POLY_CAST (Cb, Ca) NCb;
  typedef POLY_INT_TYPE (Cb) ICb;
  typedef POLY_CONST_COEFF (Ca, Cb) C;

  poly_int<N, C> r;
  POLY_SET_COEFF (C, r, 0, MAX (NCa (a.coeffs[0]), NCb (b)));
  if (N >= 2)
    for (unsigned int i = 1; i < N; i++)
      POLY_SET_COEFF (C, r, i, MAX (NCa (a.coeffs[i]), ICb (0)));
  return r;
}

template<unsigned int N, typename Ca, typename Cb>
inline CONST_POLY_RESULT (N, Ca, Cb)
upper_bound (const Ca &a, const poly_int_pod<N, Cb> &b)
{
  return upper_bound (b, a);
}

template<unsigned int N, typename Ca, typename Cb>
inline POLY_POLY_RESULT (N, Ca, Cb)
upper_bound (const poly_int_pod<N, Ca> &a, const poly_int_pod<N, Cb> &b)
{
  typedef POLY_CAST (Ca, Cb) NCa;
  typedef POLY_CAST (Cb, Ca) NCb;
  typedef POLY_POLY_COEFF (Ca, Cb) C;

  poly_int<N, C> r;
  for (unsigned int i = 0; i < N; i++)
    POLY_SET_COEFF (C, r, i, MAX (NCa (a.coeffs[i]), NCb (b.coeffs[i])));
  return r;
}

/* Return the greatest common divisor of all nonzero coefficients, or zero
   if all coefficients are zero.  */

template<unsigned int N, typename Ca>
inline POLY_BINARY_COEFF (Ca, Ca)
coeff_gcd (const poly_int_pod<N, Ca> &a)
{
  /* Find the first nonzero coefficient, stopping at 0 whatever happens.  */
  unsigned int i;
  for (i = N - 1; i > 0; --i)
    if (a.coeffs[i] != 0)
      break;
  typedef POLY_BINARY_COEFF (Ca, Ca) C;
  C r = a.coeffs[i];
  for (unsigned int j = 0; j < i; ++j)
    if (a.coeffs[j] != 0)
      r = gcd (r, C (a.coeffs[j]));
  return r;
}

/* Return a value that is a multiple of both A and B.  This will be the
   least common multiple for some indeterminate values but necessarily
   for all.  */

template<unsigned int N, typename Ca, typename Cb>
POLY_CONST_RESULT (N, Ca, Cb)
common_multiple (const poly_int_pod<N, Ca> &a, Cb b)
{
  POLY_BINARY_COEFF (Ca, Ca) xgcd = coeff_gcd (a);
  return a * (least_common_multiple (xgcd, b) / xgcd);
}

template<unsigned int N, typename Ca, typename Cb>
inline CONST_POLY_RESULT (N, Ca, Cb)
common_multiple (const Ca &a, const poly_int_pod<N, Cb> &b)
{
  return common_multiple (b, a);
}

/* Return a value that is a multiple of both A and B, asserting that
   such a value exists.  The result will be the least common multiple
   for some indeterminate values but necessarily for all.

   NOTE: When using this function, please add a comment above the call
   explaining why we know the values have a common multiple (which might
   for example be because we know A / B is rational).  */

template<unsigned int N, typename Ca, typename Cb>
POLY_POLY_RESULT (N, Ca, Cb)
force_common_multiple (const poly_int_pod<N, Ca> &a,
		       const poly_int_pod<N, Cb> &b)
{
  if (b.is_constant ())
    return common_multiple (a, b.coeffs[0]);
  if (a.is_constant ())
    return common_multiple (a.coeffs[0], b);

  typedef POLY_CAST (Ca, Cb) NCa;
  typedef POLY_CAST (Cb, Ca) NCb;
  typedef POLY_BINARY_COEFF (Ca, Cb) C;
  typedef POLY_INT_TYPE (Ca) ICa;

  for (unsigned int i = 1; i < N; ++i)
    if (a.coeffs[i] != ICa (0))
      {
	C lcm = least_common_multiple (NCa (a.coeffs[i]), NCb (b.coeffs[i]));
	C amul = lcm / a.coeffs[i];
	C bmul = lcm / b.coeffs[i];
	for (unsigned int j = 0; j < N; ++j)
	  gcc_checking_assert (a.coeffs[j] * amul == b.coeffs[j] * bmul);
	return a * amul;
      }
  gcc_unreachable ();
}

/* Compare A and B for sorting purposes, returning -1 if A should come
   before B, 0 if A and B are identical, and 1 if A should come after B.
   This is a lexicographical compare of the coefficients in reverse order.

   A consequence of this is that all constant sizes come before all
   non-constant ones, regardless of magnitude (since a size is never
   negative).  This is what most callers want.  For example, when laying
   data out on the stack, it's better to keep all the constant-sized
   data together so that it can be accessed as a constant offset from a
   single base.  */

template<unsigned int N, typename Ca, typename Cb>
inline int
compare_sizes_for_sort (const poly_int_pod<N, Ca> &a,
			const poly_int_pod<N, Cb> &b)
{
  for (unsigned int i = N; i-- > 0; )
    if (a.coeffs[i] != b.coeffs[i])
      return a.coeffs[i] < b.coeffs[i] ? -1 : 1;
  return 0;
}

/* Return true if we can calculate VALUE & (ALIGN - 1) at compile time.  */

template<unsigned int N, typename Ca, typename Cb>
inline bool
can_align_p (const poly_int_pod<N, Ca> &value, Cb align)
{
  for (unsigned int i = 1; i < N; i++)
    if ((value.coeffs[i] & (align - 1)) != 0)
      return false;
  return true;
}

/* Return true if we can align VALUE up to the smallest multiple of
   ALIGN that is >= VALUE.  Store the aligned value in *ALIGNED if so.  */

template<unsigned int N, typename Ca, typename Cb>
inline bool
can_align_up (const poly_int_pod<N, Ca> &value, Cb align,
	      poly_int_pod<N, Ca> *aligned)
{
  if (!can_align_p (value, align))
    return false;
  *aligned = value + (-value.coeffs[0] & (align - 1));
  return true;
}

/* Return true if we can align VALUE down to the largest multiple of
   ALIGN that is <= VALUE.  Store the aligned value in *ALIGNED if so.  */

template<unsigned int N, typename Ca, typename Cb>
inline bool
can_align_down (const poly_int_pod<N, Ca> &value, Cb align,
		poly_int_pod<N, Ca> *aligned)
{
  if (!can_align_p (value, align))
    return false;
  *aligned = value - (value.coeffs[0] & (align - 1));
  return true;
}

/* Return true if we can align A and B up to the smallest multiples of
   ALIGN that are >= A and B respectively, and if doing so gives the
   same value.  */

template<unsigned int N, typename Ca, typename Cb, typename Cc>
inline bool
known_equal_after_align_up (const poly_int_pod<N, Ca> &a,
			    const poly_int_pod<N, Cb> &b,
			    Cc align)
{
  poly_int<N, Ca> aligned_a;
  poly_int<N, Cb> aligned_b;
  return (can_align_up (a, align, &aligned_a)
	  && can_align_up (b, align, &aligned_b)
	  && known_eq (aligned_a, aligned_b));
}

/* Return true if we can align A and B down to the largest multiples of
   ALIGN that are <= A and B respectively, and if doing so gives the
   same value.  */

template<unsigned int N, typename Ca, typename Cb, typename Cc>
inline bool
known_equal_after_align_down (const poly_int_pod<N, Ca> &a,
			      const poly_int_pod<N, Cb> &b,
			      Cc align)
{
  poly_int<N, Ca> aligned_a;
  poly_int<N, Cb> aligned_b;
  return (can_align_down (a, align, &aligned_a)
	  && can_align_down (b, align, &aligned_b)
	  && known_eq (aligned_a, aligned_b));
}

/* Assert that we can align VALUE to ALIGN at compile time and return
   the smallest multiple of ALIGN that is >= VALUE.

   NOTE: When using this function, please add a comment above the call
   explaining why we know the non-constant coefficients must already
   be a multiple of ALIGN.  */

template<unsigned int N, typename Ca, typename Cb>
inline poly_int<N, Ca>
force_align_up (const poly_int_pod<N, Ca> &value, Cb align)
{
  gcc_checking_assert (can_align_p (value, align));
  return value + (-value.coeffs[0] & (align - 1));
}

/* Assert that we can align VALUE to ALIGN at compile time and return
   the largest multiple of ALIGN that is <= VALUE.

   NOTE: When using this function, please add a comment above the call
   explaining why we know the non-constant coefficients must already
   be a multiple of ALIGN.  */

template<unsigned int N, typename Ca, typename Cb>
inline poly_int<N, Ca>
force_align_down (const poly_int_pod<N, Ca> &value, Cb align)
{
  gcc_checking_assert (can_align_p (value, align));
  return value - (value.coeffs[0] & (align - 1));
}

/* Return a value <= VALUE that is a multiple of ALIGN.  It will be the
   greatest such value for some indeterminate values but not necessarily
   for all.  */

template<unsigned int N, typename Ca, typename Cb>
inline poly_int<N, Ca>
aligned_lower_bound (const poly_int_pod<N, Ca> &value, Cb align)
{
  poly_int<N, Ca> r;
  for (unsigned int i = 0; i < N; i++)
    /* This form copes correctly with more type combinations than
       value.coeffs[i] & -align would.  */
    POLY_SET_COEFF (Ca, r, i, (value.coeffs[i]
			       - (value.coeffs[i] & (align - 1))));
  return r;
}

/* Return a value >= VALUE that is a multiple of ALIGN.  It will be the
   least such value for some indeterminate values but not necessarily
   for all.  */

template<unsigned int N, typename Ca, typename Cb>
inline poly_int<N, Ca>
aligned_upper_bound (const poly_int_pod<N, Ca> &value, Cb align)
{
  poly_int<N, Ca> r;
  for (unsigned int i = 0; i < N; i++)
    POLY_SET_COEFF (Ca, r, i, (value.coeffs[i]
			       + (-value.coeffs[i] & (align - 1))));
  return r;
}

/* Assert that we can align VALUE to ALIGN at compile time.  Align VALUE
   down to the largest multiple of ALIGN that is <= VALUE, then divide by
   ALIGN.

   NOTE: When using this function, please add a comment above the call
   explaining why we know the non-constant coefficients must already
   be a multiple of ALIGN.  */

template<unsigned int N, typename Ca, typename Cb>
inline poly_int<N, Ca>
force_align_down_and_div (const poly_int_pod<N, Ca> &value, Cb align)
{
  gcc_checking_assert (can_align_p (value, align));

  poly_int<N, Ca> r;
  POLY_SET_COEFF (Ca, r, 0, ((value.coeffs[0]
			      - (value.coeffs[0] & (align - 1)))
			     / align));
  if (N >= 2)
    for (unsigned int i = 1; i < N; i++)
      POLY_SET_COEFF (Ca, r, i, value.coeffs[i] / align);
  return r;
}

/* Assert that we can align VALUE to ALIGN at compile time.  Align VALUE
   up to the smallest multiple of ALIGN that is >= VALUE, then divide by
   ALIGN.

   NOTE: When using this function, please add a comment above the call
   explaining why we know the non-constant coefficients must already
   be a multiple of ALIGN.  */

template<unsigned int N, typename Ca, typename Cb>
inline poly_int<N, Ca>
force_align_up_and_div (const poly_int_pod<N, Ca> &value, Cb align)
{
  gcc_checking_assert (can_align_p (value, align));

  poly_int<N, Ca> r;
  POLY_SET_COEFF (Ca, r, 0, ((value.coeffs[0]
			      + (-value.coeffs[0] & (align - 1)))
			     / align));
  if (N >= 2)
    for (unsigned int i = 1; i < N; i++)
      POLY_SET_COEFF (Ca, r, i, value.coeffs[i] / align);
  return r;
}

/* Return true if we know at compile time the difference between VALUE
   and the equal or preceding multiple of ALIGN.  Store the value in
   *MISALIGN if so.  */

template<unsigned int N, typename Ca, typename Cb, typename Cm>
inline bool
known_misalignment (const poly_int_pod<N, Ca> &value, Cb align, Cm *misalign)
{
  gcc_checking_assert (align != 0);
  if (!can_align_p (value, align))
    return false;
  *misalign = value.coeffs[0] & (align - 1);
  return true;
}

/* Return X & (Y - 1), asserting that this value is known.  Please add
   an a comment above callers to this function to explain why the condition
   is known to hold.  */

template<unsigned int N, typename Ca, typename Cb>
inline POLY_BINARY_COEFF (Ca, Ca)
force_get_misalignment (const poly_int_pod<N, Ca> &a, Cb align)
{
  gcc_checking_assert (can_align_p (a, align));
  return a.coeffs[0] & (align - 1);
}

/* Return the maximum alignment that A is known to have.  Return 0
   if A is known to be zero.  */

template<unsigned int N, typename Ca>
inline POLY_BINARY_COEFF (Ca, Ca)
known_alignment (const poly_int_pod<N, Ca> &a)
{
  typedef POLY_BINARY_COEFF (Ca, Ca) C;
  C r = a.coeffs[0];
  for (unsigned int i = 1; i < N; ++i)
    r |= a.coeffs[i];
  return r & -r;
}

/* Return true if we can compute A | B at compile time, storing the
   result in RES if so.  */

template<unsigned int N, typename Ca, typename Cb, typename Cr>
inline typename if_nonpoly<Cb, bool>::type
can_ior_p (const poly_int_pod<N, Ca> &a, Cb b, Cr *result)
{
  /* Coefficients 1 and above must be a multiple of something greater
     than B.  */
  typedef POLY_INT_TYPE (Ca) int_type;
  if (N >= 2)
    for (unsigned int i = 1; i < N; i++)
      if ((-(a.coeffs[i] & -a.coeffs[i]) & b) != int_type (0))
	return false;
  *result = a;
  result->coeffs[0] |= b;
  return true;
}

/* Return true if A is a constant multiple of B, storing the
   multiple in *MULTIPLE if so.  */

template<unsigned int N, typename Ca, typename Cb, typename Cm>
inline typename if_nonpoly<Cb, bool>::type
constant_multiple_p (const poly_int_pod<N, Ca> &a, Cb b, Cm *multiple)
{
  typedef POLY_CAST (Ca, Cb) NCa;
  typedef POLY_CAST (Cb, Ca) NCb;

  /* Do the modulus before the constant check, to catch divide by
     zero errors.  */
  if (NCa (a.coeffs[0]) % NCb (b) != 0 || !a.is_constant ())
    return false;
  *multiple = NCa (a.coeffs[0]) / NCb (b);
  return true;
}

template<unsigned int N, typename Ca, typename Cb, typename Cm>
inline typename if_nonpoly<Ca, bool>::type
constant_multiple_p (Ca a, const poly_int_pod<N, Cb> &b, Cm *multiple)
{
  typedef POLY_CAST (Ca, Cb) NCa;
  typedef POLY_CAST (Cb, Ca) NCb;
  typedef POLY_INT_TYPE (Ca) int_type;

  /* Do the modulus before the constant check, to catch divide by
     zero errors.  */
  if (NCa (a) % NCb (b.coeffs[0]) != 0
      || (a != int_type (0) && !b.is_constant ()))
    return false;
  *multiple = NCa (a) / NCb (b.coeffs[0]);
  return true;
}

template<unsigned int N, typename Ca, typename Cb, typename Cm>
inline bool
constant_multiple_p (const poly_int_pod<N, Ca> &a,
		     const poly_int_pod<N, Cb> &b, Cm *multiple)
{
  typedef POLY_CAST (Ca, Cb) NCa;
  typedef POLY_CAST (Cb, Ca) NCb;
  typedef POLY_INT_TYPE (Ca) ICa;
  typedef POLY_INT_TYPE (Cb) ICb;
  typedef POLY_BINARY_COEFF (Ca, Cb) C;

  if (NCa (a.coeffs[0]) % NCb (b.coeffs[0]) != 0)
    return false;

  C r = NCa (a.coeffs[0]) / NCb (b.coeffs[0]);
  for (unsigned int i = 1; i < N; ++i)
    if (b.coeffs[i] == ICb (0)
	? a.coeffs[i] != ICa (0)
	: (NCa (a.coeffs[i]) % NCb (b.coeffs[i]) != 0
	   || NCa (a.coeffs[i]) / NCb (b.coeffs[i]) != r))
      return false;

  *multiple = r;
  return true;
}

/* Return true if A is a multiple of B.  */

template<typename Ca, typename Cb>
inline typename if_nonpoly2<Ca, Cb, bool>::type
multiple_p (Ca a, Cb b)
{
  return a % b == 0;
}

/* Return true if A is a (polynomial) multiple of B.  */

template<unsigned int N, typename Ca, typename Cb>
inline typename if_nonpoly<Cb, bool>::type
multiple_p (const poly_int_pod<N, Ca> &a, Cb b)
{
  for (unsigned int i = 0; i < N; ++i)
    if (a.coeffs[i] % b != 0)
      return false;
  return true;
}

/* Return true if A is a (constant) multiple of B.  */

template<unsigned int N, typename Ca, typename Cb>
inline typename if_nonpoly<Ca, bool>::type
multiple_p (Ca a, const poly_int_pod<N, Cb> &b)
{
  typedef POLY_INT_TYPE (Ca) int_type;

  /* Do the modulus before the constant check, to catch divide by
     potential zeros.  */
  return a % b.coeffs[0] == 0 && (a == int_type (0) || b.is_constant ());
}

/* Return true if A is a (polynomial) multiple of B.  This handles cases
   where either B is constant or the multiple is constant.  */

template<unsigned int N, typename Ca, typename Cb>
inline bool
multiple_p (const poly_int_pod<N, Ca> &a, const poly_int_pod<N, Cb> &b)
{
  if (b.is_constant ())
    return multiple_p (a, b.coeffs[0]);
  POLY_BINARY_COEFF (Ca, Ca) tmp;
  return constant_multiple_p (a, b, &tmp);
}

/* Return true if A is a (constant) multiple of B, storing the
   multiple in *MULTIPLE if so.  */

template<typename Ca, typename Cb, typename Cm>
inline typename if_nonpoly2<Ca, Cb, bool>::type
multiple_p (Ca a, Cb b, Cm *multiple)
{
  if (a % b != 0)
    return false;
  *multiple = a / b;
  return true;
}

/* Return true if A is a (polynomial) multiple of B, storing the
   multiple in *MULTIPLE if so.  */

template<unsigned int N, typename Ca, typename Cb, typename Cm>
inline typename if_nonpoly<Cb, bool>::type
multiple_p (const poly_int_pod<N, Ca> &a, Cb b, poly_int_pod<N, Cm> *multiple)
{
  if (!multiple_p (a, b))
    return false;
  for (unsigned int i = 0; i < N; ++i)
    multiple->coeffs[i] = a.coeffs[i] / b;
  return true;
}

/* Return true if B is a constant and A is a (constant) multiple of B,
   storing the multiple in *MULTIPLE if so.  */

template<unsigned int N, typename Ca, typename Cb, typename Cm>
inline typename if_nonpoly<Ca, bool>::type
multiple_p (Ca a, const poly_int_pod<N, Cb> &b, Cm *multiple)
{
  typedef POLY_CAST (Ca, Cb) NCa;

  /* Do the modulus before the constant check, to catch divide by
     potential zeros.  */
  if (a % b.coeffs[0] != 0 || (NCa (a) != 0 && !b.is_constant ()))
    return false;
  *multiple = a / b.coeffs[0];
  return true;
}

/* Return true if A is a (polynomial) multiple of B, storing the
   multiple in *MULTIPLE if so.  This handles cases where either
   B is constant or the multiple is constant.  */

template<unsigned int N, typename Ca, typename Cb, typename Cm>
inline bool
multiple_p (const poly_int_pod<N, Ca> &a, const poly_int_pod<N, Cb> &b,
	    poly_int_pod<N, Cm> *multiple)
{
  if (b.is_constant ())
    return multiple_p (a, b.coeffs[0], multiple);
  return constant_multiple_p (a, b, multiple);
}

/* Return A / B, given that A is known to be a multiple of B.  */

template<unsigned int N, typename Ca, typename Cb>
inline POLY_CONST_RESULT (N, Ca, Cb)
exact_div (const poly_int_pod<N, Ca> &a, Cb b)
{
  typedef POLY_CONST_COEFF (Ca, Cb) C;
  poly_int<N, C> r;
  for (unsigned int i = 0; i < N; i++)
    {
      gcc_checking_assert (a.coeffs[i] % b == 0);
      POLY_SET_COEFF (C, r, i, a.coeffs[i] / b);
    }
  return r;
}

/* Return A / B, given that A is known to be a multiple of B.  */

template<unsigned int N, typename Ca, typename Cb>
inline POLY_POLY_RESULT (N, Ca, Cb)
exact_div (const poly_int_pod<N, Ca> &a, const poly_int_pod<N, Cb> &b)
{
  if (b.is_constant ())
    return exact_div (a, b.coeffs[0]);

  typedef POLY_CAST (Ca, Cb) NCa;
  typedef POLY_CAST (Cb, Ca) NCb;
  typedef POLY_BINARY_COEFF (Ca, Cb) C;
  typedef POLY_INT_TYPE (Cb) int_type;

  gcc_checking_assert (a.coeffs[0] % b.coeffs[0] == 0);
  C r = NCa (a.coeffs[0]) / NCb (b.coeffs[0]);
  for (unsigned int i = 1; i < N; ++i)
    gcc_checking_assert (b.coeffs[i] == int_type (0)
			 ? a.coeffs[i] == int_type (0)
			 : (a.coeffs[i] % b.coeffs[i] == 0
			    && NCa (a.coeffs[i]) / NCb (b.coeffs[i]) == r));

  return r;
}

/* Return true if there is some constant Q and polynomial r such that:

     (1) a = b * Q + r
     (2) |b * Q| <= |a|
     (3) |r| < |b|

   Store the value Q in *QUOTIENT if so.  */

template<unsigned int N, typename Ca, typename Cb, typename Cq>
inline typename if_nonpoly2<Cb, Cq, bool>::type
can_div_trunc_p (const poly_int_pod<N, Ca> &a, Cb b, Cq *quotient)
{
  typedef POLY_CAST (Ca, Cb) NCa;
  typedef POLY_CAST (Cb, Ca) NCb;

  /* Do the division before the constant check, to catch divide by
     zero errors.  */
  Cq q = NCa (a.coeffs[0]) / NCb (b);
  if (!a.is_constant ())
    return false;
  *quotient = q;
  return true;
}

template<unsigned int N, typename Ca, typename Cb, typename Cq>
inline typename if_nonpoly<Cq, bool>::type
can_div_trunc_p (const poly_int_pod<N, Ca> &a,
		 const poly_int_pod<N, Cb> &b,
		 Cq *quotient)
{
  /* We can calculate Q from the case in which the indeterminates
     are zero.  */
  typedef POLY_CAST (Ca, Cb) NCa;
  typedef POLY_CAST (Cb, Ca) NCb;
  typedef POLY_INT_TYPE (Ca) ICa;
  typedef POLY_INT_TYPE (Cb) ICb;
  typedef POLY_BINARY_COEFF (Ca, Cb) C;
  C q = NCa (a.coeffs[0]) / NCb (b.coeffs[0]);

  /* Check the other coefficients and record whether the division is exact.
     The only difficult case is when it isn't.  If we require a and b to
     ordered wrt zero, there can be no two coefficients of the same value
     that have opposite signs.  This means that:

	 |a| = |a0| + |a1 * x1| + |a2 * x2| + ...
	 |b| = |b0| + |b1 * x1| + |b2 * x2| + ...

      The Q we've just calculated guarantees:

	 |b0 * Q| <= |a0|
	 |a0 - b0 * Q| < |b0|

      and so:

	 (2) |b * Q| <= |a|

      is satisfied if:

	 |bi * xi * Q| <= |ai * xi|

      for each i in [1, N].  This is trivially true when xi is zero.
      When it isn't we need:

	 (2') |bi * Q| <= |ai|

      r is calculated as:

	 r = r0 + r1 * x1 + r2 * x2 + ...
	 where ri = ai - bi * Q

      Restricting to ordered a and b also guarantees that no two ris
      have opposite signs, so we have:

	 |r| = |r0| + |r1 * x1| + |r2 * x2| + ...

      We know from the calculation of Q that |r0| < |b0|, so:

	 (3) |r| < |b|

      is satisfied if:

	 (3') |ai - bi * Q| <= |bi|

      for each i in [1, N].  */
  bool rem_p = NCa (a.coeffs[0]) % NCb (b.coeffs[0]) != 0;
  for (unsigned int i = 1; i < N; ++i)
    {
      if (b.coeffs[i] == ICb (0))
	{
	  /* For bi == 0 we simply need: (3') |ai| == 0.  */
	  if (a.coeffs[i] != ICa (0))
	    return false;
	}
      else
	{
	  if (q == 0)
	    {
	      /* For Q == 0 we simply need: (3') |ai| <= |bi|.  */
	      if (a.coeffs[i] != ICa (0))
		{
		  /* Use negative absolute to avoid overflow, i.e.
		     -|ai| >= -|bi|.  */
		  C neg_abs_a = (a.coeffs[i] < 0 ? a.coeffs[i] : -a.coeffs[i]);
		  C neg_abs_b = (b.coeffs[i] < 0 ? b.coeffs[i] : -b.coeffs[i]);
		  if (neg_abs_a < neg_abs_b)
		    return false;
		  rem_p = true;
		}
	    }
	  else
	    {
	      /* Otherwise just check for the case in which ai / bi == Q.  */
	      if (NCa (a.coeffs[i]) / NCb (b.coeffs[i]) != q)
		return false;
	      if (NCa (a.coeffs[i]) % NCb (b.coeffs[i]) != 0)
		rem_p = true;
	    }
	}
    }

  /* If the division isn't exact, require both values to be ordered wrt 0,
     so that we can guarantee conditions (2) and (3) for all indeterminate
     values.  */
  if (rem_p && (!ordered_p (a, ICa (0)) || !ordered_p (b, ICb (0))))
    return false;

  *quotient = q;
  return true;
}

/* Likewise, but also store r in *REMAINDER.  */

template<unsigned int N, typename Ca, typename Cb, typename Cq, typename Cr>
inline typename if_nonpoly<Cq, bool>::type
can_div_trunc_p (const poly_int_pod<N, Ca> &a,
		 const poly_int_pod<N, Cb> &b,
		 Cq *quotient, Cr *remainder)
{
  if (!can_div_trunc_p (a, b, quotient))
    return false;
  *remainder = a - *quotient * b;
  return true;
}

/* Return true if there is some polynomial q and constant R such that:

     (1) a = B * q + R
     (2) |B * q| <= |a|
     (3) |R| < |B|

   Store the value q in *QUOTIENT if so.  */

template<unsigned int N, typename Ca, typename Cb, typename Cq>
inline typename if_nonpoly<Cb, bool>::type
can_div_trunc_p (const poly_int_pod<N, Ca> &a, Cb b,
		 poly_int_pod<N, Cq> *quotient)
{
  /* The remainder must be constant.  */
  for (unsigned int i = 1; i < N; ++i)
    if (a.coeffs[i] % b != 0)
      return false;
  for (unsigned int i = 0; i < N; ++i)
    quotient->coeffs[i] = a.coeffs[i] / b;
  return true;
}

/* Likewise, but also store R in *REMAINDER.  */

template<unsigned int N, typename Ca, typename Cb, typename Cq, typename Cr>
inline typename if_nonpoly<Cb, bool>::type
can_div_trunc_p (const poly_int_pod<N, Ca> &a, Cb b,
		 poly_int_pod<N, Cq> *quotient, Cr *remainder)
{
  if (!can_div_trunc_p (a, b, quotient))
    return false;
  *remainder = a.coeffs[0] % b;
  return true;
}

/* Return true if there is some constant Q and polynomial r such that:

     (1) a = b * Q + r
     (2) |a| <= |b * Q|
     (3) |r| < |b|

   Store the value Q in *QUOTIENT if so.  */

template<unsigned int N, typename Ca, typename Cb, typename Cq>
inline typename if_nonpoly<Cq, bool>::type
can_div_away_from_zero_p (const poly_int_pod<N, Ca> &a,
			  const poly_int_pod<N, Cb> &b,
			  Cq *quotient)
{
  if (!can_div_trunc_p (a, b, quotient))
    return false;
  if (maybe_ne (*quotient * b, a))
    *quotient += (*quotient < 0 ? -1 : 1);
  return true;
}

/* Use print_dec to print VALUE to FILE, where SGN is the sign
   of the values.  */

template<unsigned int N, typename C>
void
print_dec (const poly_int_pod<N, C> &value, FILE *file, signop sgn)
{
  if (value.is_constant ())
    print_dec (value.coeffs[0], file, sgn);
  else
    {
      fprintf (file, "[");
      for (unsigned int i = 0; i < N; ++i)
	{
	  print_dec (value.coeffs[i], file, sgn);
	  fputc (i == N - 1 ? ']' : ',', file);
	}
    }
}

/* Likewise without the signop argument, for coefficients that have an
   inherent signedness.  */

template<unsigned int N, typename C>
void
print_dec (const poly_int_pod<N, C> &value, FILE *file)
{
  STATIC_ASSERT (poly_coeff_traits<C>::signedness >= 0);
  print_dec (value, file,
	     poly_coeff_traits<C>::signedness ? SIGNED : UNSIGNED);
}

/* Helper for calculating the distance between two points P1 and P2,
   in cases where known_le (P1, P2).  T1 and T2 are the types of the
   two positions, in either order.  The coefficients of P2 - P1 have
   type unsigned HOST_WIDE_INT if the coefficients of both T1 and T2
   have C++ primitive type, otherwise P2 - P1 has its usual
   wide-int-based type.

   The actual subtraction should look something like this:

     typedef poly_span_traits<T1, T2> span_traits;
     span_traits::cast (P2) - span_traits::cast (P1)

   Applying the cast before the subtraction avoids undefined overflow
   for signed T1 and T2.

   The implementation of the cast tries to avoid unnecessary arithmetic
   or copying.  */
template<typename T1, typename T2,
	 typename Res = POLY_BINARY_COEFF (POLY_BINARY_COEFF (T1, T2),
					   unsigned HOST_WIDE_INT)>
struct poly_span_traits
{
  template<typename T>
  static const T &cast (const T &x) { return x; }
};

template<typename T1, typename T2>
struct poly_span_traits<T1, T2, unsigned HOST_WIDE_INT>
{
  template<typename T>
  static typename if_nonpoly<T, unsigned HOST_WIDE_INT>::type
  cast (const T &x) { return x; }

  template<unsigned int N, typename T>
  static poly_int<N, unsigned HOST_WIDE_INT>
  cast (const poly_int_pod<N, T> &x) { return x; }
};

/* Return true if SIZE represents a known size, assuming that all-ones
   indicates an unknown size.  */

template<typename T>
inline bool
known_size_p (const T &a)
{
  return maybe_ne (a, POLY_INT_TYPE (T) (-1));
}

/* Return true if range [POS, POS + SIZE) might include VAL.
   SIZE can be the special value -1, in which case the range is
   open-ended.  */

template<typename T1, typename T2, typename T3>
inline bool
maybe_in_range_p (const T1 &val, const T2 &pos, const T3 &size)
{
  typedef poly_span_traits<T1, T2> start_span;
  typedef poly_span_traits<T3, T3> size_span;
  if (known_lt (val, pos))
    return false;
  if (!known_size_p (size))
    return true;
  if ((poly_int_traits<T1>::num_coeffs > 1
       || poly_int_traits<T2>::num_coeffs > 1)
      && maybe_lt (val, pos))
    /* In this case we don't know whether VAL >= POS is true at compile
       time, so we can't prove that VAL >= POS + SIZE.  */
    return true;
  return maybe_lt (start_span::cast (val) - start_span::cast (pos),
		   size_span::cast (size));
}

/* Return true if range [POS, POS + SIZE) is known to include VAL.
   SIZE can be the special value -1, in which case the range is
   open-ended.  */

template<typename T1, typename T2, typename T3>
inline bool
known_in_range_p (const T1 &val, const T2 &pos, const T3 &size)
{
  typedef poly_span_traits<T1, T2> start_span;
  typedef poly_span_traits<T3, T3> size_span;
  return (known_size_p (size)
	  && known_ge (val, pos)
	  && known_lt (start_span::cast (val) - start_span::cast (pos),
		       size_span::cast (size)));
}

/* Return true if the two ranges [POS1, POS1 + SIZE1) and [POS2, POS2 + SIZE2)
   might overlap.  SIZE1 and/or SIZE2 can be the special value -1, in which
   case the range is open-ended.  */

template<typename T1, typename T2, typename T3, typename T4>
inline bool
ranges_maybe_overlap_p (const T1 &pos1, const T2 &size1,
			const T3 &pos2, const T4 &size2)
{
  if (maybe_in_range_p (pos2, pos1, size1))
    return maybe_ne (size2, POLY_INT_TYPE (T4) (0));
  if (maybe_in_range_p (pos1, pos2, size2))
    return maybe_ne (size1, POLY_INT_TYPE (T2) (0));
  return false;
}

/* Return true if the two ranges [POS1, POS1 + SIZE1) and [POS2, POS2 + SIZE2)
   are known to overlap.  SIZE1 and/or SIZE2 can be the special value -1,
   in which case the range is open-ended.  */

template<typename T1, typename T2, typename T3, typename T4>
inline bool
ranges_known_overlap_p (const T1 &pos1, const T2 &size1,
			const T3 &pos2, const T4 &size2)
{
  typedef poly_span_traits<T1, T3> start_span;
  typedef poly_span_traits<T2, T2> size1_span;
  typedef poly_span_traits<T4, T4> size2_span;
  /* known_gt (POS1 + SIZE1, POS2)                         [infinite precision]
     --> known_gt (SIZE1, POS2 - POS1)                     [infinite precision]
     --> known_gt (SIZE1, POS2 - lower_bound (POS1, POS2)) [infinite precision]
                          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ always nonnegative
     --> known_gt (SIZE1, span1::cast (POS2 - lower_bound (POS1, POS2))).

     Using the saturating subtraction enforces that SIZE1 must be
     nonzero, since known_gt (0, x) is false for all nonnegative x.
     If POS2.coeff[I] < POS1.coeff[I] for some I > 0, increasing
     indeterminate number I makes the unsaturated condition easier to
     satisfy, so using a saturated coefficient of zero tests the case in
     which the indeterminate is zero (the minimum value).  */
  return (known_size_p (size1)
	  && known_size_p (size2)
	  && known_lt (start_span::cast (pos2)
		       - start_span::cast (lower_bound (pos1, pos2)),
		       size1_span::cast (size1))
	  && known_lt (start_span::cast (pos1)
		       - start_span::cast (lower_bound (pos1, pos2)),
		       size2_span::cast (size2)));
}

/* Return true if range [POS1, POS1 + SIZE1) is known to be a subrange of
   [POS2, POS2 + SIZE2).  SIZE1 and/or SIZE2 can be the special value -1,
   in which case the range is open-ended.  */

template<typename T1, typename T2, typename T3, typename T4>
inline bool
known_subrange_p (const T1 &pos1, const T2 &size1,
		  const T3 &pos2, const T4 &size2)
{
  typedef typename poly_int_traits<T2>::coeff_type C2;
  typedef poly_span_traits<T1, T3> start_span;
  typedef poly_span_traits<T2, T4> size_span;
  return (known_gt (size1, POLY_INT_TYPE (T2) (0))
	  && (poly_coeff_traits<C2>::signedness > 0
	      || known_size_p (size1))
	  && known_size_p (size2)
	  && known_ge (pos1, pos2)
	  && known_le (size1, size2)
	  && known_le (start_span::cast (pos1) - start_span::cast (pos2),
		       size_span::cast (size2) - size_span::cast (size1)));
}

/* Return true if the endpoint of the range [POS, POS + SIZE) can be
   stored in a T, or if SIZE is the special value -1, which makes the
   range open-ended.  */

template<typename T>
inline typename if_nonpoly<T, bool>::type
endpoint_representable_p (const T &pos, const T &size)
{
  return (!known_size_p (size)
	  || pos <= poly_coeff_traits<T>::max_value - size);
}

template<unsigned int N, typename C>
inline bool
endpoint_representable_p (const poly_int_pod<N, C> &pos,
			  const poly_int_pod<N, C> &size)
{
  if (known_size_p (size))
    for (unsigned int i = 0; i < N; ++i)
      if (pos.coeffs[i] > poly_coeff_traits<C>::max_value - size.coeffs[i])
	return false;
  return true;
}

template<unsigned int N, typename C>
void
gt_ggc_mx (poly_int_pod<N, C> *)
{
}

template<unsigned int N, typename C>
void
gt_pch_nx (poly_int_pod<N, C> *)
{
}

template<unsigned int N, typename C>
void
gt_pch_nx (poly_int_pod<N, C> *, void (*) (void *, void *), void *)
{
}

#undef POLY_SET_COEFF
#undef POLY_INT_TYPE
#undef POLY_BINARY_COEFF
#undef CONST_CONST_RESULT
#undef POLY_CONST_RESULT
#undef CONST_POLY_RESULT
#undef POLY_POLY_RESULT
#undef POLY_CONST_COEFF
#undef CONST_POLY_COEFF
#undef POLY_POLY_COEFF

#endif
