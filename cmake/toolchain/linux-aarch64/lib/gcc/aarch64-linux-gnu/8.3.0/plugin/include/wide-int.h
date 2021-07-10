/* Operations with very long integers.  -*- C++ -*-
   Copyright (C) 2012-2018 Free Software Foundation, Inc.

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

#ifndef WIDE_INT_H
#define WIDE_INT_H

/* wide-int.[cc|h] implements a class that efficiently performs
   mathematical operations on finite precision integers.  wide_ints
   are designed to be transient - they are not for long term storage
   of values.  There is tight integration between wide_ints and the
   other longer storage GCC representations (rtl and tree).

   The actual precision of a wide_int depends on the flavor.  There
   are three predefined flavors:

     1) wide_int (the default).  This flavor does the math in the
     precision of its input arguments.  It is assumed (and checked)
     that the precisions of the operands and results are consistent.
     This is the most efficient flavor.  It is not possible to examine
     bits above the precision that has been specified.  Because of
     this, the default flavor has semantics that are simple to
     understand and in general model the underlying hardware that the
     compiler is targetted for.

     This flavor must be used at the RTL level of gcc because there
     is, in general, not enough information in the RTL representation
     to extend a value beyond the precision specified in the mode.

     This flavor should also be used at the TREE and GIMPLE levels of
     the compiler except for the circumstances described in the
     descriptions of the other two flavors.

     The default wide_int representation does not contain any
     information inherent about signedness of the represented value,
     so it can be used to represent both signed and unsigned numbers.
     For operations where the results depend on signedness (full width
     multiply, division, shifts, comparisons, and operations that need
     overflow detected), the signedness must be specified separately.

     2) offset_int.  This is a fixed-precision integer that can hold
     any address offset, measured in either bits or bytes, with at
     least one extra sign bit.  At the moment the maximum address
     size GCC supports is 64 bits.  With 8-bit bytes and an extra
     sign bit, offset_int therefore needs to have at least 68 bits
     of precision.  We round this up to 128 bits for efficiency.
     Values of type T are converted to this precision by sign- or
     zero-extending them based on the signedness of T.

     The extra sign bit means that offset_int is effectively a signed
     128-bit integer, i.e. it behaves like int128_t.

     Since the values are logically signed, there is no need to
     distinguish between signed and unsigned operations.  Sign-sensitive
     comparison operators <, <=, > and >= are therefore supported.
     Shift operators << and >> are also supported, with >> being
     an _arithmetic_ right shift.

     [ Note that, even though offset_int is effectively int128_t,
       it can still be useful to use unsigned comparisons like
       wi::leu_p (a, b) as a more efficient short-hand for
       "a >= 0 && a <= b". ]

     3) widest_int.  This representation is an approximation of
     infinite precision math.  However, it is not really infinite
     precision math as in the GMP library.  It is really finite
     precision math where the precision is 4 times the size of the
     largest integer that the target port can represent.

     Like offset_int, widest_int is wider than all the values that
     it needs to represent, so the integers are logically signed.
     Sign-sensitive comparison operators <, <=, > and >= are supported,
     as are << and >>.

     There are several places in the GCC where this should/must be used:

     * Code that does induction variable optimizations.  This code
       works with induction variables of many different types at the
       same time.  Because of this, it ends up doing many different
       calculations where the operands are not compatible types.  The
       widest_int makes this easy, because it provides a field where
       nothing is lost when converting from any variable,

     * There are a small number of passes that currently use the
       widest_int that should use the default.  These should be
       changed.

   There are surprising features of offset_int and widest_int
   that the users should be careful about:

     1) Shifts and rotations are just weird.  You have to specify a
     precision in which the shift or rotate is to happen in.  The bits
     above this precision are zeroed.  While this is what you
     want, it is clearly non obvious.

     2) Larger precision math sometimes does not produce the same
     answer as would be expected for doing the math at the proper
     precision.  In particular, a multiply followed by a divide will
     produce a different answer if the first product is larger than
     what can be represented in the input precision.

   The offset_int and the widest_int flavors are more expensive
   than the default wide int, so in addition to the caveats with these
   two, the default is the prefered representation.

   All three flavors of wide_int are represented as a vector of
   HOST_WIDE_INTs.  The default and widest_int vectors contain enough elements
   to hold a value of MAX_BITSIZE_MODE_ANY_INT bits.  offset_int contains only
   enough elements to hold ADDR_MAX_PRECISION bits.  The values are stored
   in the vector with the least significant HOST_BITS_PER_WIDE_INT bits
   in element 0.

   The default wide_int contains three fields: the vector (VAL),
   the precision and a length (LEN).  The length is the number of HWIs
   needed to represent the value.  widest_int and offset_int have a
   constant precision that cannot be changed, so they only store the
   VAL and LEN fields.

   Since most integers used in a compiler are small values, it is
   generally profitable to use a representation of the value that is
   as small as possible.  LEN is used to indicate the number of
   elements of the vector that are in use.  The numbers are stored as
   sign extended numbers as a means of compression.  Leading
   HOST_WIDE_INTs that contain strings of either -1 or 0 are removed
   as long as they can be reconstructed from the top bit that is being
   represented.

   The precision and length of a wide_int are always greater than 0.
   Any bits in a wide_int above the precision are sign-extended from the
   most significant bit.  For example, a 4-bit value 0x8 is represented as
   VAL = { 0xf...fff8 }.  However, as an optimization, we allow other integer
   constants to be represented with undefined bits above the precision.
   This allows INTEGER_CSTs to be pre-extended according to TYPE_SIGN,
   so that the INTEGER_CST representation can be used both in TYPE_PRECISION
   and in wider precisions.

   There are constructors to create the various forms of wide_int from
   trees, rtl and constants.  For trees the options are:

	     tree t = ...;
	     wi::to_wide (t)     // Treat T as a wide_int
	     wi::to_offset (t)   // Treat T as an offset_int
	     wi::to_widest (t)   // Treat T as a widest_int

   All three are light-weight accessors that should have no overhead
   in release builds.  If it is useful for readability reasons to
   store the result in a temporary variable, the preferred method is:

	     wi::tree_to_wide_ref twide = wi::to_wide (t);
	     wi::tree_to_offset_ref toffset = wi::to_offset (t);
	     wi::tree_to_widest_ref twidest = wi::to_widest (t);

   To make an rtx into a wide_int, you have to pair it with a mode.
   The canonical way to do this is with rtx_mode_t as in:

	     rtx r = ...
	     wide_int x = rtx_mode_t (r, mode);

   Similarly, a wide_int can only be constructed from a host value if
   the target precision is given explicitly, such as in:

	     wide_int x = wi::shwi (c, prec); // sign-extend C if necessary
	     wide_int y = wi::uhwi (c, prec); // zero-extend C if necessary

   However, offset_int and widest_int have an inherent precision and so
   can be initialized directly from a host value:

	     offset_int x = (int) c;          // sign-extend C
	     widest_int x = (unsigned int) c; // zero-extend C

   It is also possible to do arithmetic directly on rtx_mode_ts and
   constants.  For example:

	     wi::add (r1, r2);    // add equal-sized rtx_mode_ts r1 and r2
	     wi::add (r1, 1);     // add 1 to rtx_mode_t r1
	     wi::lshift (1, 100); // 1 << 100 as a widest_int

   Many binary operations place restrictions on the combinations of inputs,
   using the following rules:

   - {rtx, wide_int} op {rtx, wide_int} -> wide_int
       The inputs must be the same precision.  The result is a wide_int
       of the same precision

   - {rtx, wide_int} op (un)signed HOST_WIDE_INT -> wide_int
     (un)signed HOST_WIDE_INT op {rtx, wide_int} -> wide_int
       The HOST_WIDE_INT is extended or truncated to the precision of
       the other input.  The result is a wide_int of the same precision
       as that input.

   - (un)signed HOST_WIDE_INT op (un)signed HOST_WIDE_INT -> widest_int
       The inputs are extended to widest_int precision and produce a
       widest_int result.

   - offset_int op offset_int -> offset_int
     offset_int op (un)signed HOST_WIDE_INT -> offset_int
     (un)signed HOST_WIDE_INT op offset_int -> offset_int

   - widest_int op widest_int -> widest_int
     widest_int op (un)signed HOST_WIDE_INT -> widest_int
     (un)signed HOST_WIDE_INT op widest_int -> widest_int

   Other combinations like:

   - widest_int op offset_int and
   - wide_int op offset_int

   are not allowed.  The inputs should instead be extended or truncated
   so that they match.

   The inputs to comparison functions like wi::eq_p and wi::lts_p
   follow the same compatibility rules, although their return types
   are different.  Unary functions on X produce the same result as
   a binary operation X + X.  Shift functions X op Y also produce
   the same result as X + X; the precision of the shift amount Y
   can be arbitrarily different from X.  */

/* The MAX_BITSIZE_MODE_ANY_INT is automatically generated by a very
   early examination of the target's mode file.  The WIDE_INT_MAX_ELTS
   can accomodate at least 1 more bit so that unsigned numbers of that
   mode can be represented as a signed value.  Note that it is still
   possible to create fixed_wide_ints that have precisions greater than
   MAX_BITSIZE_MODE_ANY_INT.  This can be useful when representing a
   double-width multiplication result, for example.  */
#define WIDE_INT_MAX_ELTS \
  ((MAX_BITSIZE_MODE_ANY_INT + HOST_BITS_PER_WIDE_INT) / HOST_BITS_PER_WIDE_INT)

#define WIDE_INT_MAX_PRECISION (WIDE_INT_MAX_ELTS * HOST_BITS_PER_WIDE_INT)

/* This is the max size of any pointer on any machine.  It does not
   seem to be as easy to sniff this out of the machine description as
   it is for MAX_BITSIZE_MODE_ANY_INT since targets may support
   multiple address sizes and may have different address sizes for
   different address spaces.  However, currently the largest pointer
   on any platform is 64 bits.  When that changes, then it is likely
   that a target hook should be defined so that targets can make this
   value larger for those targets.  */
#define ADDR_MAX_BITSIZE 64

/* This is the internal precision used when doing any address
   arithmetic.  The '4' is really 3 + 1.  Three of the bits are for
   the number of extra bits needed to do bit addresses and the other bit
   is to allow everything to be signed without loosing any precision.
   Then everything is rounded up to the next HWI for efficiency.  */
#define ADDR_MAX_PRECISION \
  ((ADDR_MAX_BITSIZE + 4 + HOST_BITS_PER_WIDE_INT - 1) \
   & ~(HOST_BITS_PER_WIDE_INT - 1))

/* The number of HWIs needed to store an offset_int.  */
#define OFFSET_INT_ELTS (ADDR_MAX_PRECISION / HOST_BITS_PER_WIDE_INT)

/* The type of result produced by a binary operation on types T1 and T2.
   Defined purely for brevity.  */
#define WI_BINARY_RESULT(T1, T2) \
  typename wi::binary_traits <T1, T2>::result_type

/* Likewise for binary operators, which excludes the case in which neither
   T1 nor T2 is a wide-int-based type.  */
#define WI_BINARY_OPERATOR_RESULT(T1, T2) \
  typename wi::binary_traits <T1, T2>::operator_result

/* The type of result produced by T1 << T2.  Leads to substitution failure
   if the operation isn't supported.  Defined purely for brevity.  */
#define WI_SIGNED_SHIFT_RESULT(T1, T2) \
  typename wi::binary_traits <T1, T2>::signed_shift_result_type

/* The type of result produced by a sign-agnostic binary predicate on
   types T1 and T2.  This is bool if wide-int operations make sense for
   T1 and T2 and leads to substitution failure otherwise.  */
#define WI_BINARY_PREDICATE_RESULT(T1, T2) \
  typename wi::binary_traits <T1, T2>::predicate_result

/* The type of result produced by a signed binary predicate on types T1 and T2.
   This is bool if signed comparisons make sense for T1 and T2 and leads to
   substitution failure otherwise.  */
#define WI_SIGNED_BINARY_PREDICATE_RESULT(T1, T2) \
  typename wi::binary_traits <T1, T2>::signed_predicate_result

/* The type of result produced by a unary operation on type T.  */
#define WI_UNARY_RESULT(T) \
  typename wi::binary_traits <T, T>::result_type

/* Define a variable RESULT to hold the result of a binary operation on
   X and Y, which have types T1 and T2 respectively.  Define VAL to
   point to the blocks of RESULT.  Once the user of the macro has
   filled in VAL, it should call RESULT.set_len to set the number
   of initialized blocks.  */
#define WI_BINARY_RESULT_VAR(RESULT, VAL, T1, X, T2, Y) \
  WI_BINARY_RESULT (T1, T2) RESULT = \
    wi::int_traits <WI_BINARY_RESULT (T1, T2)>::get_binary_result (X, Y); \
  HOST_WIDE_INT *VAL = RESULT.write_val ()

/* Similar for the result of a unary operation on X, which has type T.  */
#define WI_UNARY_RESULT_VAR(RESULT, VAL, T, X) \
  WI_UNARY_RESULT (T) RESULT = \
    wi::int_traits <WI_UNARY_RESULT (T)>::get_binary_result (X, X); \
  HOST_WIDE_INT *VAL = RESULT.write_val ()

template <typename T> class generic_wide_int;
template <int N> class fixed_wide_int_storage;
class wide_int_storage;

/* An N-bit integer.  Until we can use typedef templates, use this instead.  */
#define FIXED_WIDE_INT(N) \
  generic_wide_int < fixed_wide_int_storage <N> >

typedef generic_wide_int <wide_int_storage> wide_int;
typedef FIXED_WIDE_INT (ADDR_MAX_PRECISION) offset_int;
typedef FIXED_WIDE_INT (WIDE_INT_MAX_PRECISION) widest_int;

/* wi::storage_ref can be a reference to a primitive type,
   so this is the conservatively-correct setting.  */
template <bool SE, bool HDP = true>
struct wide_int_ref_storage;

typedef generic_wide_int <wide_int_ref_storage <false> > wide_int_ref;

/* This can be used instead of wide_int_ref if the referenced value is
   known to have type T.  It carries across properties of T's representation,
   such as whether excess upper bits in a HWI are defined, and can therefore
   help avoid redundant work.

   The macro could be replaced with a template typedef, once we're able
   to use those.  */
#define WIDE_INT_REF_FOR(T) \
  generic_wide_int \
    <wide_int_ref_storage <wi::int_traits <T>::is_sign_extended, \
			   wi::int_traits <T>::host_dependent_precision> >

namespace wi
{
  /* Classifies an integer based on its precision.  */
  enum precision_type {
    /* The integer has both a precision and defined signedness.  This allows
       the integer to be converted to any width, since we know whether to fill
       any extra bits with zeros or signs.  */
    FLEXIBLE_PRECISION,

    /* The integer has a variable precision but no defined signedness.  */
    VAR_PRECISION,

    /* The integer has a constant precision (known at GCC compile time)
       and is signed.  */
    CONST_PRECISION
  };

  /* This class, which has no default implementation, is expected to
     provide the following members:

     static const enum precision_type precision_type;
       Classifies the type of T.

     static const unsigned int precision;
       Only defined if precision_type == CONST_PRECISION.  Specifies the
       precision of all integers of type T.

     static const bool host_dependent_precision;
       True if the precision of T depends (or can depend) on the host.

     static unsigned int get_precision (const T &x)
       Return the number of bits in X.

     static wi::storage_ref *decompose (HOST_WIDE_INT *scratch,
					unsigned int precision, const T &x)
       Decompose X as a PRECISION-bit integer, returning the associated
       wi::storage_ref.  SCRATCH is available as scratch space if needed.
       The routine should assert that PRECISION is acceptable.  */
  template <typename T> struct int_traits;

  /* This class provides a single type, result_type, which specifies the
     type of integer produced by a binary operation whose inputs have
     types T1 and T2.  The definition should be symmetric.  */
  template <typename T1, typename T2,
	    enum precision_type P1 = int_traits <T1>::precision_type,
	    enum precision_type P2 = int_traits <T2>::precision_type>
  struct binary_traits;

  /* Specify the result type for each supported combination of binary
     inputs.  Note that CONST_PRECISION and VAR_PRECISION cannot be
     mixed, in order to give stronger type checking.  When both inputs
     are CONST_PRECISION, they must have the same precision.  */
  template <typename T1, typename T2>
  struct binary_traits <T1, T2, FLEXIBLE_PRECISION, FLEXIBLE_PRECISION>
  {
    typedef widest_int result_type;
    /* Don't define operators for this combination.  */
  };

  template <typename T1, typename T2>
  struct binary_traits <T1, T2, FLEXIBLE_PRECISION, VAR_PRECISION>
  {
    typedef wide_int result_type;
    typedef result_type operator_result;
    typedef bool predicate_result;
  };

  template <typename T1, typename T2>
  struct binary_traits <T1, T2, FLEXIBLE_PRECISION, CONST_PRECISION>
  {
    /* Spelled out explicitly (rather than through FIXED_WIDE_INT)
       so as not to confuse gengtype.  */
    typedef generic_wide_int < fixed_wide_int_storage
			       <int_traits <T2>::precision> > result_type;
    typedef result_type operator_result;
    typedef bool predicate_result;
    typedef result_type signed_shift_result_type;
    typedef bool signed_predicate_result;
  };

  template <typename T1, typename T2>
  struct binary_traits <T1, T2, VAR_PRECISION, FLEXIBLE_PRECISION>
  {
    typedef wide_int result_type;
    typedef result_type operator_result;
    typedef bool predicate_result;
  };

  template <typename T1, typename T2>
  struct binary_traits <T1, T2, CONST_PRECISION, FLEXIBLE_PRECISION>
  {
    /* Spelled out explicitly (rather than through FIXED_WIDE_INT)
       so as not to confuse gengtype.  */
    typedef generic_wide_int < fixed_wide_int_storage
			       <int_traits <T1>::precision> > result_type;
    typedef result_type operator_result;
    typedef bool predicate_result;
    typedef result_type signed_shift_result_type;
    typedef bool signed_predicate_result;
  };

  template <typename T1, typename T2>
  struct binary_traits <T1, T2, CONST_PRECISION, CONST_PRECISION>
  {
    STATIC_ASSERT (int_traits <T1>::precision == int_traits <T2>::precision);
    /* Spelled out explicitly (rather than through FIXED_WIDE_INT)
       so as not to confuse gengtype.  */
    typedef generic_wide_int < fixed_wide_int_storage
			       <int_traits <T1>::precision> > result_type;
    typedef result_type operator_result;
    typedef bool predicate_result;
    typedef result_type signed_shift_result_type;
    typedef bool signed_predicate_result;
  };

  template <typename T1, typename T2>
  struct binary_traits <T1, T2, VAR_PRECISION, VAR_PRECISION>
  {
    typedef wide_int result_type;
    typedef result_type operator_result;
    typedef bool predicate_result;
  };
}

/* Public functions for querying and operating on integers.  */
namespace wi
{
  template <typename T>
  unsigned int get_precision (const T &);

  template <typename T1, typename T2>
  unsigned int get_binary_precision (const T1 &, const T2 &);

  template <typename T1, typename T2>
  void copy (T1 &, const T2 &);

#define UNARY_PREDICATE \
  template <typename T> bool
#define UNARY_FUNCTION \
  template <typename T> WI_UNARY_RESULT (T)
#define BINARY_PREDICATE \
  template <typename T1, typename T2> bool
#define BINARY_FUNCTION \
  template <typename T1, typename T2> WI_BINARY_RESULT (T1, T2)
#define SHIFT_FUNCTION \
  template <typename T1, typename T2> WI_UNARY_RESULT (T1)

  UNARY_PREDICATE fits_shwi_p (const T &);
  UNARY_PREDICATE fits_uhwi_p (const T &);
  UNARY_PREDICATE neg_p (const T &, signop = SIGNED);

  template <typename T>
  HOST_WIDE_INT sign_mask (const T &);

  BINARY_PREDICATE eq_p (const T1 &, const T2 &);
  BINARY_PREDICATE ne_p (const T1 &, const T2 &);
  BINARY_PREDICATE lt_p (const T1 &, const T2 &, signop);
  BINARY_PREDICATE lts_p (const T1 &, const T2 &);
  BINARY_PREDICATE ltu_p (const T1 &, const T2 &);
  BINARY_PREDICATE le_p (const T1 &, const T2 &, signop);
  BINARY_PREDICATE les_p (const T1 &, const T2 &);
  BINARY_PREDICATE leu_p (const T1 &, const T2 &);
  BINARY_PREDICATE gt_p (const T1 &, const T2 &, signop);
  BINARY_PREDICATE gts_p (const T1 &, const T2 &);
  BINARY_PREDICATE gtu_p (const T1 &, const T2 &);
  BINARY_PREDICATE ge_p (const T1 &, const T2 &, signop);
  BINARY_PREDICATE ges_p (const T1 &, const T2 &);
  BINARY_PREDICATE geu_p (const T1 &, const T2 &);

  template <typename T1, typename T2>
  int cmp (const T1 &, const T2 &, signop);

  template <typename T1, typename T2>
  int cmps (const T1 &, const T2 &);

  template <typename T1, typename T2>
  int cmpu (const T1 &, const T2 &);

  UNARY_FUNCTION bit_not (const T &);
  UNARY_FUNCTION neg (const T &);
  UNARY_FUNCTION neg (const T &, bool *);
  UNARY_FUNCTION abs (const T &);
  UNARY_FUNCTION ext (const T &, unsigned int, signop);
  UNARY_FUNCTION sext (const T &, unsigned int);
  UNARY_FUNCTION zext (const T &, unsigned int);
  UNARY_FUNCTION set_bit (const T &, unsigned int);

  BINARY_FUNCTION min (const T1 &, const T2 &, signop);
  BINARY_FUNCTION smin (const T1 &, const T2 &);
  BINARY_FUNCTION umin (const T1 &, const T2 &);
  BINARY_FUNCTION max (const T1 &, const T2 &, signop);
  BINARY_FUNCTION smax (const T1 &, const T2 &);
  BINARY_FUNCTION umax (const T1 &, const T2 &);

  BINARY_FUNCTION bit_and (const T1 &, const T2 &);
  BINARY_FUNCTION bit_and_not (const T1 &, const T2 &);
  BINARY_FUNCTION bit_or (const T1 &, const T2 &);
  BINARY_FUNCTION bit_or_not (const T1 &, const T2 &);
  BINARY_FUNCTION bit_xor (const T1 &, const T2 &);
  BINARY_FUNCTION add (const T1 &, const T2 &);
  BINARY_FUNCTION add (const T1 &, const T2 &, signop, bool *);
  BINARY_FUNCTION sub (const T1 &, const T2 &);
  BINARY_FUNCTION sub (const T1 &, const T2 &, signop, bool *);
  BINARY_FUNCTION mul (const T1 &, const T2 &);
  BINARY_FUNCTION mul (const T1 &, const T2 &, signop, bool *);
  BINARY_FUNCTION smul (const T1 &, const T2 &, bool *);
  BINARY_FUNCTION umul (const T1 &, const T2 &, bool *);
  BINARY_FUNCTION mul_high (const T1 &, const T2 &, signop);
  BINARY_FUNCTION div_trunc (const T1 &, const T2 &, signop, bool * = 0);
  BINARY_FUNCTION sdiv_trunc (const T1 &, const T2 &);
  BINARY_FUNCTION udiv_trunc (const T1 &, const T2 &);
  BINARY_FUNCTION div_floor (const T1 &, const T2 &, signop, bool * = 0);
  BINARY_FUNCTION udiv_floor (const T1 &, const T2 &);
  BINARY_FUNCTION sdiv_floor (const T1 &, const T2 &);
  BINARY_FUNCTION div_ceil (const T1 &, const T2 &, signop, bool * = 0);
  BINARY_FUNCTION udiv_ceil (const T1 &, const T2 &);
  BINARY_FUNCTION div_round (const T1 &, const T2 &, signop, bool * = 0);
  BINARY_FUNCTION divmod_trunc (const T1 &, const T2 &, signop,
				WI_BINARY_RESULT (T1, T2) *);
  BINARY_FUNCTION gcd (const T1 &, const T2 &, signop = UNSIGNED);
  BINARY_FUNCTION mod_trunc (const T1 &, const T2 &, signop, bool * = 0);
  BINARY_FUNCTION smod_trunc (const T1 &, const T2 &);
  BINARY_FUNCTION umod_trunc (const T1 &, const T2 &);
  BINARY_FUNCTION mod_floor (const T1 &, const T2 &, signop, bool * = 0);
  BINARY_FUNCTION umod_floor (const T1 &, const T2 &);
  BINARY_FUNCTION mod_ceil (const T1 &, const T2 &, signop, bool * = 0);
  BINARY_FUNCTION mod_round (const T1 &, const T2 &, signop, bool * = 0);

  template <typename T1, typename T2>
  bool multiple_of_p (const T1 &, const T2 &, signop);

  template <typename T1, typename T2>
  bool multiple_of_p (const T1 &, const T2 &, signop,
		      WI_BINARY_RESULT (T1, T2) *);

  SHIFT_FUNCTION lshift (const T1 &, const T2 &);
  SHIFT_FUNCTION lrshift (const T1 &, const T2 &);
  SHIFT_FUNCTION arshift (const T1 &, const T2 &);
  SHIFT_FUNCTION rshift (const T1 &, const T2 &, signop sgn);
  SHIFT_FUNCTION lrotate (const T1 &, const T2 &, unsigned int = 0);
  SHIFT_FUNCTION rrotate (const T1 &, const T2 &, unsigned int = 0);

#undef SHIFT_FUNCTION
#undef BINARY_PREDICATE
#undef BINARY_FUNCTION
#undef UNARY_PREDICATE
#undef UNARY_FUNCTION

  bool only_sign_bit_p (const wide_int_ref &, unsigned int);
  bool only_sign_bit_p (const wide_int_ref &);
  int clz (const wide_int_ref &);
  int clrsb (const wide_int_ref &);
  int ctz (const wide_int_ref &);
  int exact_log2 (const wide_int_ref &);
  int floor_log2 (const wide_int_ref &);
  int ffs (const wide_int_ref &);
  int popcount (const wide_int_ref &);
  int parity (const wide_int_ref &);

  template <typename T>
  unsigned HOST_WIDE_INT extract_uhwi (const T &, unsigned int, unsigned int);

  template <typename T>
  unsigned int min_precision (const T &, signop);
}

namespace wi
{
  /* Contains the components of a decomposed integer for easy, direct
     access.  */
  struct storage_ref
  {
    storage_ref () {}
    storage_ref (const HOST_WIDE_INT *, unsigned int, unsigned int);

    const HOST_WIDE_INT *val;
    unsigned int len;
    unsigned int precision;

    /* Provide enough trappings for this class to act as storage for
       generic_wide_int.  */
    unsigned int get_len () const;
    unsigned int get_precision () const;
    const HOST_WIDE_INT *get_val () const;
  };
}

inline::wi::storage_ref::storage_ref (const HOST_WIDE_INT *val_in,
				      unsigned int len_in,
				      unsigned int precision_in)
  : val (val_in), len (len_in), precision (precision_in)
{
}

inline unsigned int
wi::storage_ref::get_len () const
{
  return len;
}

inline unsigned int
wi::storage_ref::get_precision () const
{
  return precision;
}

inline const HOST_WIDE_INT *
wi::storage_ref::get_val () const
{
  return val;
}

/* This class defines an integer type using the storage provided by the
   template argument.  The storage class must provide the following
   functions:

   unsigned int get_precision () const
     Return the number of bits in the integer.

   HOST_WIDE_INT *get_val () const
     Return a pointer to the array of blocks that encodes the integer.

   unsigned int get_len () const
     Return the number of blocks in get_val ().  If this is smaller
     than the number of blocks implied by get_precision (), the
     remaining blocks are sign extensions of block get_len () - 1.

   Although not required by generic_wide_int itself, writable storage
   classes can also provide the following functions:

   HOST_WIDE_INT *write_val ()
     Get a modifiable version of get_val ()

   unsigned int set_len (unsigned int len)
     Set the value returned by get_len () to LEN.  */
template <typename storage>
class GTY(()) generic_wide_int : public storage
{
public:
  generic_wide_int ();

  template <typename T>
  generic_wide_int (const T &);

  template <typename T>
  generic_wide_int (const T &, unsigned int);

  /* Conversions.  */
  HOST_WIDE_INT to_shwi (unsigned int) const;
  HOST_WIDE_INT to_shwi () const;
  unsigned HOST_WIDE_INT to_uhwi (unsigned int) const;
  unsigned HOST_WIDE_INT to_uhwi () const;
  HOST_WIDE_INT to_short_addr () const;

  /* Public accessors for the interior of a wide int.  */
  HOST_WIDE_INT sign_mask () const;
  HOST_WIDE_INT elt (unsigned int) const;
  unsigned HOST_WIDE_INT ulow () const;
  unsigned HOST_WIDE_INT uhigh () const;
  HOST_WIDE_INT slow () const;
  HOST_WIDE_INT shigh () const;

  template <typename T>
  generic_wide_int &operator = (const T &);

#define ASSIGNMENT_OPERATOR(OP, F) \
  template <typename T> \
    generic_wide_int &OP (const T &c) { return (*this = wi::F (*this, c)); }

/* Restrict these to cases where the shift operator is defined.  */
#define SHIFT_ASSIGNMENT_OPERATOR(OP, OP2) \
  template <typename T> \
    generic_wide_int &OP (const T &c) { return (*this = *this OP2 c); }

#define INCDEC_OPERATOR(OP, DELTA) \
  generic_wide_int &OP () { *this += DELTA; return *this; }

  ASSIGNMENT_OPERATOR (operator &=, bit_and)
  ASSIGNMENT_OPERATOR (operator |=, bit_or)
  ASSIGNMENT_OPERATOR (operator ^=, bit_xor)
  ASSIGNMENT_OPERATOR (operator +=, add)
  ASSIGNMENT_OPERATOR (operator -=, sub)
  ASSIGNMENT_OPERATOR (operator *=, mul)
  ASSIGNMENT_OPERATOR (operator <<=, lshift)
  SHIFT_ASSIGNMENT_OPERATOR (operator >>=, >>)
  INCDEC_OPERATOR (operator ++, 1)
  INCDEC_OPERATOR (operator --, -1)

#undef SHIFT_ASSIGNMENT_OPERATOR
#undef ASSIGNMENT_OPERATOR
#undef INCDEC_OPERATOR

  /* Debugging functions.  */
  void dump () const;

  static const bool is_sign_extended
    = wi::int_traits <generic_wide_int <storage> >::is_sign_extended;
};

template <typename storage>
inline generic_wide_int <storage>::generic_wide_int () {}

template <typename storage>
template <typename T>
inline generic_wide_int <storage>::generic_wide_int (const T &x)
  : storage (x)
{
}

template <typename storage>
template <typename T>
inline generic_wide_int <storage>::generic_wide_int (const T &x,
						     unsigned int precision)
  : storage (x, precision)
{
}

/* Return THIS as a signed HOST_WIDE_INT, sign-extending from PRECISION.
   If THIS does not fit in PRECISION, the information is lost.  */
template <typename storage>
inline HOST_WIDE_INT
generic_wide_int <storage>::to_shwi (unsigned int precision) const
{
  if (precision < HOST_BITS_PER_WIDE_INT)
    return sext_hwi (this->get_val ()[0], precision);
  else
    return this->get_val ()[0];
}

/* Return THIS as a signed HOST_WIDE_INT, in its natural precision.  */
template <typename storage>
inline HOST_WIDE_INT
generic_wide_int <storage>::to_shwi () const
{
  if (is_sign_extended)
    return this->get_val ()[0];
  else
    return to_shwi (this->get_precision ());
}

/* Return THIS as an unsigned HOST_WIDE_INT, zero-extending from
   PRECISION.  If THIS does not fit in PRECISION, the information
   is lost.  */
template <typename storage>
inline unsigned HOST_WIDE_INT
generic_wide_int <storage>::to_uhwi (unsigned int precision) const
{
  if (precision < HOST_BITS_PER_WIDE_INT)
    return zext_hwi (this->get_val ()[0], precision);
  else
    return this->get_val ()[0];
}

/* Return THIS as an signed HOST_WIDE_INT, in its natural precision.  */
template <typename storage>
inline unsigned HOST_WIDE_INT
generic_wide_int <storage>::to_uhwi () const
{
  return to_uhwi (this->get_precision ());
}

/* TODO: The compiler is half converted from using HOST_WIDE_INT to
   represent addresses to using offset_int to represent addresses.
   We use to_short_addr at the interface from new code to old,
   unconverted code.  */
template <typename storage>
inline HOST_WIDE_INT
generic_wide_int <storage>::to_short_addr () const
{
  return this->get_val ()[0];
}

/* Return the implicit value of blocks above get_len ().  */
template <typename storage>
inline HOST_WIDE_INT
generic_wide_int <storage>::sign_mask () const
{
  unsigned int len = this->get_len ();
  unsigned HOST_WIDE_INT high = this->get_val ()[len - 1];
  if (!is_sign_extended)
    {
      unsigned int precision = this->get_precision ();
      int excess = len * HOST_BITS_PER_WIDE_INT - precision;
      if (excess > 0)
	high <<= excess;
    }
  return (HOST_WIDE_INT) (high) < 0 ? -1 : 0;
}

/* Return the signed value of the least-significant explicitly-encoded
   block.  */
template <typename storage>
inline HOST_WIDE_INT
generic_wide_int <storage>::slow () const
{
  return this->get_val ()[0];
}

/* Return the signed value of the most-significant explicitly-encoded
   block.  */
template <typename storage>
inline HOST_WIDE_INT
generic_wide_int <storage>::shigh () const
{
  return this->get_val ()[this->get_len () - 1];
}

/* Return the unsigned value of the least-significant
   explicitly-encoded block.  */
template <typename storage>
inline unsigned HOST_WIDE_INT
generic_wide_int <storage>::ulow () const
{
  return this->get_val ()[0];
}

/* Return the unsigned value of the most-significant
   explicitly-encoded block.  */
template <typename storage>
inline unsigned HOST_WIDE_INT
generic_wide_int <storage>::uhigh () const
{
  return this->get_val ()[this->get_len () - 1];
}

/* Return block I, which might be implicitly or explicit encoded.  */
template <typename storage>
inline HOST_WIDE_INT
generic_wide_int <storage>::elt (unsigned int i) const
{
  if (i >= this->get_len ())
    return sign_mask ();
  else
    return this->get_val ()[i];
}

template <typename storage>
template <typename T>
inline generic_wide_int <storage> &
generic_wide_int <storage>::operator = (const T &x)
{
  storage::operator = (x);
  return *this;
}

/* Dump the contents of the integer to stderr, for debugging.  */
template <typename storage>
void
generic_wide_int <storage>::dump () const
{
  unsigned int len = this->get_len ();
  const HOST_WIDE_INT *val = this->get_val ();
  unsigned int precision = this->get_precision ();
  fprintf (stderr, "[");
  if (len * HOST_BITS_PER_WIDE_INT < precision)
    fprintf (stderr, "...,");
  for (unsigned int i = 0; i < len - 1; ++i)
    fprintf (stderr, HOST_WIDE_INT_PRINT_HEX ",", val[len - 1 - i]);
  fprintf (stderr, HOST_WIDE_INT_PRINT_HEX "], precision = %d\n",
	   val[0], precision);
}

namespace wi
{
  template <typename storage>
  struct int_traits < generic_wide_int <storage> >
    : public wi::int_traits <storage>
  {
    static unsigned int get_precision (const generic_wide_int <storage> &);
    static wi::storage_ref decompose (HOST_WIDE_INT *, unsigned int,
				      const generic_wide_int <storage> &);
  };
}

template <typename storage>
inline unsigned int
wi::int_traits < generic_wide_int <storage> >::
get_precision (const generic_wide_int <storage> &x)
{
  return x.get_precision ();
}

template <typename storage>
inline wi::storage_ref
wi::int_traits < generic_wide_int <storage> >::
decompose (HOST_WIDE_INT *, unsigned int precision,
	   const generic_wide_int <storage> &x)
{
  gcc_checking_assert (precision == x.get_precision ());
  return wi::storage_ref (x.get_val (), x.get_len (), precision);
}

/* Provide the storage for a wide_int_ref.  This acts like a read-only
   wide_int, with the optimization that VAL is normally a pointer to
   another integer's storage, so that no array copy is needed.  */
template <bool SE, bool HDP>
struct wide_int_ref_storage : public wi::storage_ref
{
private:
  /* Scratch space that can be used when decomposing the original integer.
     It must live as long as this object.  */
  HOST_WIDE_INT scratch[2];

public:
  wide_int_ref_storage () {}

  wide_int_ref_storage (const wi::storage_ref &);

  template <typename T>
  wide_int_ref_storage (const T &);

  template <typename T>
  wide_int_ref_storage (const T &, unsigned int);
};

/* Create a reference from an existing reference.  */
template <bool SE, bool HDP>
inline wide_int_ref_storage <SE, HDP>::
wide_int_ref_storage (const wi::storage_ref &x)
  : storage_ref (x)
{}

/* Create a reference to integer X in its natural precision.  Note
   that the natural precision is host-dependent for primitive
   types.  */
template <bool SE, bool HDP>
template <typename T>
inline wide_int_ref_storage <SE, HDP>::wide_int_ref_storage (const T &x)
  : storage_ref (wi::int_traits <T>::decompose (scratch,
						wi::get_precision (x), x))
{
}

/* Create a reference to integer X in precision PRECISION.  */
template <bool SE, bool HDP>
template <typename T>
inline wide_int_ref_storage <SE, HDP>::
wide_int_ref_storage (const T &x, unsigned int precision)
  : storage_ref (wi::int_traits <T>::decompose (scratch, precision, x))
{
}

namespace wi
{
  template <bool SE, bool HDP>
  struct int_traits <wide_int_ref_storage <SE, HDP> >
  {
    static const enum precision_type precision_type = VAR_PRECISION;
    static const bool host_dependent_precision = HDP;
    static const bool is_sign_extended = SE;
  };
}

namespace wi
{
  unsigned int force_to_size (HOST_WIDE_INT *, const HOST_WIDE_INT *,
			      unsigned int, unsigned int, unsigned int,
			      signop sgn);
  unsigned int from_array (HOST_WIDE_INT *, const HOST_WIDE_INT *,
			   unsigned int, unsigned int, bool = true);
}

/* The storage used by wide_int.  */
class GTY(()) wide_int_storage
{
private:
  HOST_WIDE_INT val[WIDE_INT_MAX_ELTS];
  unsigned int len;
  unsigned int precision;

public:
  wide_int_storage ();
  template <typename T>
  wide_int_storage (const T &);

  /* The standard generic_wide_int storage methods.  */
  unsigned int get_precision () const;
  const HOST_WIDE_INT *get_val () const;
  unsigned int get_len () const;
  HOST_WIDE_INT *write_val ();
  void set_len (unsigned int, bool = false);

  template <typename T>
  wide_int_storage &operator = (const T &);

  static wide_int from (const wide_int_ref &, unsigned int, signop);
  static wide_int from_array (const HOST_WIDE_INT *, unsigned int,
			      unsigned int, bool = true);
  static wide_int create (unsigned int);

  /* FIXME: target-dependent, so should disappear.  */
  wide_int bswap () const;
};

namespace wi
{
  template <>
  struct int_traits <wide_int_storage>
  {
    static const enum precision_type precision_type = VAR_PRECISION;
    /* Guaranteed by a static assert in the wide_int_storage constructor.  */
    static const bool host_dependent_precision = false;
    static const bool is_sign_extended = true;
    template <typename T1, typename T2>
    static wide_int get_binary_result (const T1 &, const T2 &);
  };
}

inline wide_int_storage::wide_int_storage () {}

/* Initialize the storage from integer X, in its natural precision.
   Note that we do not allow integers with host-dependent precision
   to become wide_ints; wide_ints must always be logically independent
   of the host.  */
template <typename T>
inline wide_int_storage::wide_int_storage (const T &x)
{
  { STATIC_ASSERT (!wi::int_traits<T>::host_dependent_precision); }
  { STATIC_ASSERT (wi::int_traits<T>::precision_type != wi::CONST_PRECISION); }
  WIDE_INT_REF_FOR (T) xi (x);
  precision = xi.precision;
  wi::copy (*this, xi);
}

template <typename T>
inline wide_int_storage&
wide_int_storage::operator = (const T &x)
{
  { STATIC_ASSERT (!wi::int_traits<T>::host_dependent_precision); }
  { STATIC_ASSERT (wi::int_traits<T>::precision_type != wi::CONST_PRECISION); }
  WIDE_INT_REF_FOR (T) xi (x);
  precision = xi.precision;
  wi::copy (*this, xi);
  return *this;
}

inline unsigned int
wide_int_storage::get_precision () const
{
  return precision;
}

inline const HOST_WIDE_INT *
wide_int_storage::get_val () const
{
  return val;
}

inline unsigned int
wide_int_storage::get_len () const
{
  return len;
}

inline HOST_WIDE_INT *
wide_int_storage::write_val ()
{
  return val;
}

inline void
wide_int_storage::set_len (unsigned int l, bool is_sign_extended)
{
  len = l;
  if (!is_sign_extended && len * HOST_BITS_PER_WIDE_INT > precision)
    val[len - 1] = sext_hwi (val[len - 1],
			     precision % HOST_BITS_PER_WIDE_INT);
}

/* Treat X as having signedness SGN and convert it to a PRECISION-bit
   number.  */
inline wide_int
wide_int_storage::from (const wide_int_ref &x, unsigned int precision,
			signop sgn)
{
  wide_int result = wide_int::create (precision);
  result.set_len (wi::force_to_size (result.write_val (), x.val, x.len,
				     x.precision, precision, sgn));
  return result;
}

/* Create a wide_int from the explicit block encoding given by VAL and
   LEN.  PRECISION is the precision of the integer.  NEED_CANON_P is
   true if the encoding may have redundant trailing blocks.  */
inline wide_int
wide_int_storage::from_array (const HOST_WIDE_INT *val, unsigned int len,
			      unsigned int precision, bool need_canon_p)
{
  wide_int result = wide_int::create (precision);
  result.set_len (wi::from_array (result.write_val (), val, len, precision,
				  need_canon_p));
  return result;
}

/* Return an uninitialized wide_int with precision PRECISION.  */
inline wide_int
wide_int_storage::create (unsigned int precision)
{
  wide_int x;
  x.precision = precision;
  return x;
}

template <typename T1, typename T2>
inline wide_int
wi::int_traits <wide_int_storage>::get_binary_result (const T1 &x, const T2 &y)
{
  /* This shouldn't be used for two flexible-precision inputs.  */
  STATIC_ASSERT (wi::int_traits <T1>::precision_type != FLEXIBLE_PRECISION
		 || wi::int_traits <T2>::precision_type != FLEXIBLE_PRECISION);
  if (wi::int_traits <T1>::precision_type == FLEXIBLE_PRECISION)
    return wide_int::create (wi::get_precision (y));
  else
    return wide_int::create (wi::get_precision (x));
}

/* The storage used by FIXED_WIDE_INT (N).  */
template <int N>
class GTY(()) fixed_wide_int_storage
{
private:
  HOST_WIDE_INT val[(N + HOST_BITS_PER_WIDE_INT + 1) / HOST_BITS_PER_WIDE_INT];
  unsigned int len;

public:
  fixed_wide_int_storage ();
  template <typename T>
  fixed_wide_int_storage (const T &);

  /* The standard generic_wide_int storage methods.  */
  unsigned int get_precision () const;
  const HOST_WIDE_INT *get_val () const;
  unsigned int get_len () const;
  HOST_WIDE_INT *write_val ();
  void set_len (unsigned int, bool = false);

  static FIXED_WIDE_INT (N) from (const wide_int_ref &, signop);
  static FIXED_WIDE_INT (N) from_array (const HOST_WIDE_INT *, unsigned int,
					bool = true);
};

namespace wi
{
  template <int N>
  struct int_traits < fixed_wide_int_storage <N> >
  {
    static const enum precision_type precision_type = CONST_PRECISION;
    static const bool host_dependent_precision = false;
    static const bool is_sign_extended = true;
    static const unsigned int precision = N;
    template <typename T1, typename T2>
    static FIXED_WIDE_INT (N) get_binary_result (const T1 &, const T2 &);
  };
}

template <int N>
inline fixed_wide_int_storage <N>::fixed_wide_int_storage () {}

/* Initialize the storage from integer X, in precision N.  */
template <int N>
template <typename T>
inline fixed_wide_int_storage <N>::fixed_wide_int_storage (const T &x)
{
  /* Check for type compatibility.  We don't want to initialize a
     fixed-width integer from something like a wide_int.  */
  WI_BINARY_RESULT (T, FIXED_WIDE_INT (N)) *assertion ATTRIBUTE_UNUSED;
  wi::copy (*this, WIDE_INT_REF_FOR (T) (x, N));
}

template <int N>
inline unsigned int
fixed_wide_int_storage <N>::get_precision () const
{
  return N;
}

template <int N>
inline const HOST_WIDE_INT *
fixed_wide_int_storage <N>::get_val () const
{
  return val;
}

template <int N>
inline unsigned int
fixed_wide_int_storage <N>::get_len () const
{
  return len;
}

template <int N>
inline HOST_WIDE_INT *
fixed_wide_int_storage <N>::write_val ()
{
  return val;
}

template <int N>
inline void
fixed_wide_int_storage <N>::set_len (unsigned int l, bool)
{
  len = l;
  /* There are no excess bits in val[len - 1].  */
  STATIC_ASSERT (N % HOST_BITS_PER_WIDE_INT == 0);
}

/* Treat X as having signedness SGN and convert it to an N-bit number.  */
template <int N>
inline FIXED_WIDE_INT (N)
fixed_wide_int_storage <N>::from (const wide_int_ref &x, signop sgn)
{
  FIXED_WIDE_INT (N) result;
  result.set_len (wi::force_to_size (result.write_val (), x.val, x.len,
				     x.precision, N, sgn));
  return result;
}

/* Create a FIXED_WIDE_INT (N) from the explicit block encoding given by
   VAL and LEN.  NEED_CANON_P is true if the encoding may have redundant
   trailing blocks.  */
template <int N>
inline FIXED_WIDE_INT (N)
fixed_wide_int_storage <N>::from_array (const HOST_WIDE_INT *val,
					unsigned int len,
					bool need_canon_p)
{
  FIXED_WIDE_INT (N) result;
  result.set_len (wi::from_array (result.write_val (), val, len,
				  N, need_canon_p));
  return result;
}

template <int N>
template <typename T1, typename T2>
inline FIXED_WIDE_INT (N)
wi::int_traits < fixed_wide_int_storage <N> >::
get_binary_result (const T1 &, const T2 &)
{
  return FIXED_WIDE_INT (N) ();
}

/* A reference to one element of a trailing_wide_ints structure.  */
class trailing_wide_int_storage
{
private:
  /* The precision of the integer, which is a fixed property of the
     parent trailing_wide_ints.  */
  unsigned int m_precision;

  /* A pointer to the length field.  */
  unsigned char *m_len;

  /* A pointer to the HWI array.  There are enough elements to hold all
     values of precision M_PRECISION.  */
  HOST_WIDE_INT *m_val;

public:
  trailing_wide_int_storage (unsigned int, unsigned char *, HOST_WIDE_INT *);

  /* The standard generic_wide_int storage methods.  */
  unsigned int get_len () const;
  unsigned int get_precision () const;
  const HOST_WIDE_INT *get_val () const;
  HOST_WIDE_INT *write_val ();
  void set_len (unsigned int, bool = false);

  template <typename T>
  trailing_wide_int_storage &operator = (const T &);
};

typedef generic_wide_int <trailing_wide_int_storage> trailing_wide_int;

/* trailing_wide_int behaves like a wide_int.  */
namespace wi
{
  template <>
  struct int_traits <trailing_wide_int_storage>
    : public int_traits <wide_int_storage> {};
}

/* An array of N wide_int-like objects that can be put at the end of
   a variable-sized structure.  Use extra_size to calculate how many
   bytes beyond the sizeof need to be allocated.  Use set_precision
   to initialize the structure.  */
template <int N>
class GTY((user)) trailing_wide_ints
{
private:
  /* The shared precision of each number.  */
  unsigned short m_precision;

  /* The shared maximum length of each number.  */
  unsigned char m_max_len;

  /* The current length of each number.  */
  unsigned char m_len[N];

  /* The variable-length part of the structure, which always contains
     at least one HWI.  Element I starts at index I * M_MAX_LEN.  */
  HOST_WIDE_INT m_val[1];

public:
  typedef WIDE_INT_REF_FOR (trailing_wide_int_storage) const_reference;

  void set_precision (unsigned int);
  unsigned int get_precision () const { return m_precision; }
  trailing_wide_int operator [] (unsigned int);
  const_reference operator [] (unsigned int) const;
  static size_t extra_size (unsigned int);
  size_t extra_size () const { return extra_size (m_precision); }
};

inline trailing_wide_int_storage::
trailing_wide_int_storage (unsigned int precision, unsigned char *len,
			   HOST_WIDE_INT *val)
  : m_precision (precision), m_len (len), m_val (val)
{
}

inline unsigned int
trailing_wide_int_storage::get_len () const
{
  return *m_len;
}

inline unsigned int
trailing_wide_int_storage::get_precision () const
{
  return m_precision;
}

inline const HOST_WIDE_INT *
trailing_wide_int_storage::get_val () const
{
  return m_val;
}

inline HOST_WIDE_INT *
trailing_wide_int_storage::write_val ()
{
  return m_val;
}

inline void
trailing_wide_int_storage::set_len (unsigned int len, bool is_sign_extended)
{
  *m_len = len;
  if (!is_sign_extended && len * HOST_BITS_PER_WIDE_INT > m_precision)
    m_val[len - 1] = sext_hwi (m_val[len - 1],
			       m_precision % HOST_BITS_PER_WIDE_INT);
}

template <typename T>
inline trailing_wide_int_storage &
trailing_wide_int_storage::operator = (const T &x)
{
  WIDE_INT_REF_FOR (T) xi (x, m_precision);
  wi::copy (*this, xi);
  return *this;
}

/* Initialize the structure and record that all elements have precision
   PRECISION.  */
template <int N>
inline void
trailing_wide_ints <N>::set_precision (unsigned int precision)
{
  m_precision = precision;
  m_max_len = ((precision + HOST_BITS_PER_WIDE_INT - 1)
	       / HOST_BITS_PER_WIDE_INT);
}

/* Return a reference to element INDEX.  */
template <int N>
inline trailing_wide_int
trailing_wide_ints <N>::operator [] (unsigned int index)
{
  return trailing_wide_int_storage (m_precision, &m_len[index],
				    &m_val[index * m_max_len]);
}

template <int N>
inline typename trailing_wide_ints <N>::const_reference
trailing_wide_ints <N>::operator [] (unsigned int index) const
{
  return wi::storage_ref (&m_val[index * m_max_len],
			  m_len[index], m_precision);
}

/* Return how many extra bytes need to be added to the end of the structure
   in order to handle N wide_ints of precision PRECISION.  */
template <int N>
inline size_t
trailing_wide_ints <N>::extra_size (unsigned int precision)
{
  unsigned int max_len = ((precision + HOST_BITS_PER_WIDE_INT - 1)
			  / HOST_BITS_PER_WIDE_INT);
  return (N * max_len - 1) * sizeof (HOST_WIDE_INT);
}

/* This macro is used in structures that end with a trailing_wide_ints field
   called FIELD.  It declares get_NAME() and set_NAME() methods to access
   element I of FIELD.  */
#define TRAILING_WIDE_INT_ACCESSOR(NAME, FIELD, I) \
  trailing_wide_int get_##NAME () { return FIELD[I]; } \
  template <typename T> void set_##NAME (const T &x) { FIELD[I] = x; }

namespace wi
{
  /* Implementation of int_traits for primitive integer types like "int".  */
  template <typename T, bool signed_p>
  struct primitive_int_traits
  {
    static const enum precision_type precision_type = FLEXIBLE_PRECISION;
    static const bool host_dependent_precision = true;
    static const bool is_sign_extended = true;
    static unsigned int get_precision (T);
    static wi::storage_ref decompose (HOST_WIDE_INT *, unsigned int, T);
  };
}

template <typename T, bool signed_p>
inline unsigned int
wi::primitive_int_traits <T, signed_p>::get_precision (T)
{
  return sizeof (T) * CHAR_BIT;
}

template <typename T, bool signed_p>
inline wi::storage_ref
wi::primitive_int_traits <T, signed_p>::decompose (HOST_WIDE_INT *scratch,
						   unsigned int precision, T x)
{
  scratch[0] = x;
  if (signed_p || scratch[0] >= 0 || precision <= HOST_BITS_PER_WIDE_INT)
    return wi::storage_ref (scratch, 1, precision);
  scratch[1] = 0;
  return wi::storage_ref (scratch, 2, precision);
}

/* Allow primitive C types to be used in wi:: routines.  */
namespace wi
{
  template <>
  struct int_traits <unsigned char>
    : public primitive_int_traits <unsigned char, false> {};

  template <>
  struct int_traits <unsigned short>
    : public primitive_int_traits <unsigned short, false> {};

  template <>
  struct int_traits <int>
    : public primitive_int_traits <int, true> {};

  template <>
  struct int_traits <unsigned int>
    : public primitive_int_traits <unsigned int, false> {};

  template <>
  struct int_traits <long>
    : public primitive_int_traits <long, true> {};

  template <>
  struct int_traits <unsigned long>
    : public primitive_int_traits <unsigned long, false> {};

#if defined HAVE_LONG_LONG
  template <>
  struct int_traits <long long>
    : public primitive_int_traits <long long, true> {};

  template <>
  struct int_traits <unsigned long long>
    : public primitive_int_traits <unsigned long long, false> {};
#endif
}

namespace wi
{
  /* Stores HWI-sized integer VAL, treating it as having signedness SGN
     and precision PRECISION.  */
  struct hwi_with_prec
  {
    hwi_with_prec () {}
    hwi_with_prec (HOST_WIDE_INT, unsigned int, signop);
    HOST_WIDE_INT val;
    unsigned int precision;
    signop sgn;
  };

  hwi_with_prec shwi (HOST_WIDE_INT, unsigned int);
  hwi_with_prec uhwi (unsigned HOST_WIDE_INT, unsigned int);

  hwi_with_prec minus_one (unsigned int);
  hwi_with_prec zero (unsigned int);
  hwi_with_prec one (unsigned int);
  hwi_with_prec two (unsigned int);
}

inline wi::hwi_with_prec::hwi_with_prec (HOST_WIDE_INT v, unsigned int p,
					 signop s)
  : precision (p), sgn (s)
{
  if (precision < HOST_BITS_PER_WIDE_INT)
    val = sext_hwi (v, precision);
  else
    val = v;
}

/* Return a signed integer that has value VAL and precision PRECISION.  */
inline wi::hwi_with_prec
wi::shwi (HOST_WIDE_INT val, unsigned int precision)
{
  return hwi_with_prec (val, precision, SIGNED);
}

/* Return an unsigned integer that has value VAL and precision PRECISION.  */
inline wi::hwi_with_prec
wi::uhwi (unsigned HOST_WIDE_INT val, unsigned int precision)
{
  return hwi_with_prec (val, precision, UNSIGNED);
}

/* Return a wide int of -1 with precision PRECISION.  */
inline wi::hwi_with_prec
wi::minus_one (unsigned int precision)
{
  return wi::shwi (-1, precision);
}

/* Return a wide int of 0 with precision PRECISION.  */
inline wi::hwi_with_prec
wi::zero (unsigned int precision)
{
  return wi::shwi (0, precision);
}

/* Return a wide int of 1 with precision PRECISION.  */
inline wi::hwi_with_prec
wi::one (unsigned int precision)
{
  return wi::shwi (1, precision);
}

/* Return a wide int of 2 with precision PRECISION.  */
inline wi::hwi_with_prec
wi::two (unsigned int precision)
{
  return wi::shwi (2, precision);
}

namespace wi
{
  /* ints_for<T>::zero (X) returns a zero that, when asssigned to a T,
     gives that T the same precision as X.  */
  template<typename T, precision_type = int_traits<T>::precision_type>
  struct ints_for
  {
    static int zero (const T &) { return 0; }
  };

  template<typename T>
  struct ints_for<T, VAR_PRECISION>
  {
    static hwi_with_prec zero (const T &);
  };
}

template<typename T>
inline wi::hwi_with_prec
wi::ints_for<T, wi::VAR_PRECISION>::zero (const T &x)
{
  return wi::zero (wi::get_precision (x));
}

namespace wi
{
  template <>
  struct int_traits <wi::hwi_with_prec>
  {
    static const enum precision_type precision_type = VAR_PRECISION;
    /* hwi_with_prec has an explicitly-given precision, rather than the
       precision of HOST_WIDE_INT.  */
    static const bool host_dependent_precision = false;
    static const bool is_sign_extended = true;
    static unsigned int get_precision (const wi::hwi_with_prec &);
    static wi::storage_ref decompose (HOST_WIDE_INT *, unsigned int,
				      const wi::hwi_with_prec &);
  };
}

inline unsigned int
wi::int_traits <wi::hwi_with_prec>::get_precision (const wi::hwi_with_prec &x)
{
  return x.precision;
}

inline wi::storage_ref
wi::int_traits <wi::hwi_with_prec>::
decompose (HOST_WIDE_INT *scratch, unsigned int precision,
	   const wi::hwi_with_prec &x)
{
  gcc_checking_assert (precision == x.precision);
  scratch[0] = x.val;
  if (x.sgn == SIGNED || x.val >= 0 || precision <= HOST_BITS_PER_WIDE_INT)
    return wi::storage_ref (scratch, 1, precision);
  scratch[1] = 0;
  return wi::storage_ref (scratch, 2, precision);
}

/* Private functions for handling large cases out of line.  They take
   individual length and array parameters because that is cheaper for
   the inline caller than constructing an object on the stack and
   passing a reference to it.  (Although many callers use wide_int_refs,
   we generally want those to be removed by SRA.)  */
namespace wi
{
  bool eq_p_large (const HOST_WIDE_INT *, unsigned int,
		   const HOST_WIDE_INT *, unsigned int, unsigned int);
  bool lts_p_large (const HOST_WIDE_INT *, unsigned int, unsigned int,
		    const HOST_WIDE_INT *, unsigned int);
  bool ltu_p_large (const HOST_WIDE_INT *, unsigned int, unsigned int,
		    const HOST_WIDE_INT *, unsigned int);
  int cmps_large (const HOST_WIDE_INT *, unsigned int, unsigned int,
		  const HOST_WIDE_INT *, unsigned int);
  int cmpu_large (const HOST_WIDE_INT *, unsigned int, unsigned int,
		  const HOST_WIDE_INT *, unsigned int);
  unsigned int sext_large (HOST_WIDE_INT *, const HOST_WIDE_INT *,
			   unsigned int,
			   unsigned int, unsigned int);
  unsigned int zext_large (HOST_WIDE_INT *, const HOST_WIDE_INT *,
			   unsigned int,
			   unsigned int, unsigned int);
  unsigned int set_bit_large (HOST_WIDE_INT *, const HOST_WIDE_INT *,
			      unsigned int, unsigned int, unsigned int);
  unsigned int lshift_large (HOST_WIDE_INT *, const HOST_WIDE_INT *,
			     unsigned int, unsigned int, unsigned int);
  unsigned int lrshift_large (HOST_WIDE_INT *, const HOST_WIDE_INT *,
			      unsigned int, unsigned int, unsigned int,
			      unsigned int);
  unsigned int arshift_large (HOST_WIDE_INT *, const HOST_WIDE_INT *,
			      unsigned int, unsigned int, unsigned int,
			      unsigned int);
  unsigned int and_large (HOST_WIDE_INT *, const HOST_WIDE_INT *, unsigned int,
			  const HOST_WIDE_INT *, unsigned int, unsigned int);
  unsigned int and_not_large (HOST_WIDE_INT *, const HOST_WIDE_INT *,
			      unsigned int, const HOST_WIDE_INT *,
			      unsigned int, unsigned int);
  unsigned int or_large (HOST_WIDE_INT *, const HOST_WIDE_INT *, unsigned int,
			 const HOST_WIDE_INT *, unsigned int, unsigned int);
  unsigned int or_not_large (HOST_WIDE_INT *, const HOST_WIDE_INT *,
			     unsigned int, const HOST_WIDE_INT *,
			     unsigned int, unsigned int);
  unsigned int xor_large (HOST_WIDE_INT *, const HOST_WIDE_INT *, unsigned int,
			  const HOST_WIDE_INT *, unsigned int, unsigned int);
  unsigned int add_large (HOST_WIDE_INT *, const HOST_WIDE_INT *, unsigned int,
			  const HOST_WIDE_INT *, unsigned int, unsigned int,
			  signop, bool *);
  unsigned int sub_large (HOST_WIDE_INT *, const HOST_WIDE_INT *, unsigned int,
			  const HOST_WIDE_INT *, unsigned int, unsigned int,
			  signop, bool *);
  unsigned int mul_internal (HOST_WIDE_INT *, const HOST_WIDE_INT *,
			     unsigned int, const HOST_WIDE_INT *,
			     unsigned int, unsigned int, signop, bool *,
			     bool);
  unsigned int divmod_internal (HOST_WIDE_INT *, unsigned int *,
				HOST_WIDE_INT *, const HOST_WIDE_INT *,
				unsigned int, unsigned int,
				const HOST_WIDE_INT *,
				unsigned int, unsigned int,
				signop, bool *);
}

/* Return the number of bits that integer X can hold.  */
template <typename T>
inline unsigned int
wi::get_precision (const T &x)
{
  return wi::int_traits <T>::get_precision (x);
}

/* Return the number of bits that the result of a binary operation can
   hold when the input operands are X and Y.  */
template <typename T1, typename T2>
inline unsigned int
wi::get_binary_precision (const T1 &x, const T2 &y)
{
  return get_precision (wi::int_traits <WI_BINARY_RESULT (T1, T2)>::
			get_binary_result (x, y));
}

/* Copy the contents of Y to X, but keeping X's current precision.  */
template <typename T1, typename T2>
inline void
wi::copy (T1 &x, const T2 &y)
{
  HOST_WIDE_INT *xval = x.write_val ();
  const HOST_WIDE_INT *yval = y.get_val ();
  unsigned int len = y.get_len ();
  unsigned int i = 0;
  do
    xval[i] = yval[i];
  while (++i < len);
  x.set_len (len, y.is_sign_extended);
}

/* Return true if X fits in a HOST_WIDE_INT with no loss of precision.  */
template <typename T>
inline bool
wi::fits_shwi_p (const T &x)
{
  WIDE_INT_REF_FOR (T) xi (x);
  return xi.len == 1;
}

/* Return true if X fits in an unsigned HOST_WIDE_INT with no loss of
   precision.  */
template <typename T>
inline bool
wi::fits_uhwi_p (const T &x)
{
  WIDE_INT_REF_FOR (T) xi (x);
  if (xi.precision <= HOST_BITS_PER_WIDE_INT)
    return true;
  if (xi.len == 1)
    return xi.slow () >= 0;
  return xi.len == 2 && xi.uhigh () == 0;
}

/* Return true if X is negative based on the interpretation of SGN.
   For UNSIGNED, this is always false.  */
template <typename T>
inline bool
wi::neg_p (const T &x, signop sgn)
{
  WIDE_INT_REF_FOR (T) xi (x);
  if (sgn == UNSIGNED)
    return false;
  return xi.sign_mask () < 0;
}

/* Return -1 if the top bit of X is set and 0 if the top bit is clear.  */
template <typename T>
inline HOST_WIDE_INT
wi::sign_mask (const T &x)
{
  WIDE_INT_REF_FOR (T) xi (x);
  return xi.sign_mask ();
}

/* Return true if X == Y.  X and Y must be binary-compatible.  */
template <typename T1, typename T2>
inline bool
wi::eq_p (const T1 &x, const T2 &y)
{
  unsigned int precision = get_binary_precision (x, y);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y, precision);
  if (xi.is_sign_extended && yi.is_sign_extended)
    {
      /* This case reduces to array equality.  */
      if (xi.len != yi.len)
	return false;
      unsigned int i = 0;
      do
	if (xi.val[i] != yi.val[i])
	  return false;
      while (++i != xi.len);
      return true;
    }
  if (__builtin_expect (yi.len == 1, true))
    {
      /* XI is only equal to YI if it too has a single HWI.  */
      if (xi.len != 1)
	return false;
      /* Excess bits in xi.val[0] will be signs or zeros, so comparisons
	 with 0 are simple.  */
      if (STATIC_CONSTANT_P (yi.val[0] == 0))
	return xi.val[0] == 0;
      /* Otherwise flush out any excess bits first.  */
      unsigned HOST_WIDE_INT diff = xi.val[0] ^ yi.val[0];
      int excess = HOST_BITS_PER_WIDE_INT - precision;
      if (excess > 0)
	diff <<= excess;
      return diff == 0;
    }
  return eq_p_large (xi.val, xi.len, yi.val, yi.len, precision);
}

/* Return true if X != Y.  X and Y must be binary-compatible.  */
template <typename T1, typename T2>
inline bool
wi::ne_p (const T1 &x, const T2 &y)
{
  return !eq_p (x, y);
}

/* Return true if X < Y when both are treated as signed values.  */
template <typename T1, typename T2>
inline bool
wi::lts_p (const T1 &x, const T2 &y)
{
  unsigned int precision = get_binary_precision (x, y);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y, precision);
  /* We optimize x < y, where y is 64 or fewer bits.  */
  if (wi::fits_shwi_p (yi))
    {
      /* Make lts_p (x, 0) as efficient as wi::neg_p (x).  */
      if (STATIC_CONSTANT_P (yi.val[0] == 0))
	return neg_p (xi);
      /* If x fits directly into a shwi, we can compare directly.  */
      if (wi::fits_shwi_p (xi))
	return xi.to_shwi () < yi.to_shwi ();
      /* If x doesn't fit and is negative, then it must be more
	 negative than any value in y, and hence smaller than y.  */
      if (neg_p (xi))
	return true;
      /* If x is positive, then it must be larger than any value in y,
	 and hence greater than y.  */
      return false;
    }
  /* Optimize the opposite case, if it can be detected at compile time.  */
  if (STATIC_CONSTANT_P (xi.len == 1))
    /* If YI is negative it is lower than the least HWI.
       If YI is positive it is greater than the greatest HWI.  */
    return !neg_p (yi);
  return lts_p_large (xi.val, xi.len, precision, yi.val, yi.len);
}

/* Return true if X < Y when both are treated as unsigned values.  */
template <typename T1, typename T2>
inline bool
wi::ltu_p (const T1 &x, const T2 &y)
{
  unsigned int precision = get_binary_precision (x, y);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y, precision);
  /* Optimize comparisons with constants.  */
  if (STATIC_CONSTANT_P (yi.len == 1 && yi.val[0] >= 0))
    return xi.len == 1 && xi.to_uhwi () < (unsigned HOST_WIDE_INT) yi.val[0];
  if (STATIC_CONSTANT_P (xi.len == 1 && xi.val[0] >= 0))
    return yi.len != 1 || yi.to_uhwi () > (unsigned HOST_WIDE_INT) xi.val[0];
  /* Optimize the case of two HWIs.  The HWIs are implicitly sign-extended
     for precisions greater than HOST_BITS_WIDE_INT, but sign-extending both
     values does not change the result.  */
  if (__builtin_expect (xi.len + yi.len == 2, true))
    {
      unsigned HOST_WIDE_INT xl = xi.to_uhwi ();
      unsigned HOST_WIDE_INT yl = yi.to_uhwi ();
      return xl < yl;
    }
  return ltu_p_large (xi.val, xi.len, precision, yi.val, yi.len);
}

/* Return true if X < Y.  Signedness of X and Y is indicated by SGN.  */
template <typename T1, typename T2>
inline bool
wi::lt_p (const T1 &x, const T2 &y, signop sgn)
{
  if (sgn == SIGNED)
    return lts_p (x, y);
  else
    return ltu_p (x, y);
}

/* Return true if X <= Y when both are treated as signed values.  */
template <typename T1, typename T2>
inline bool
wi::les_p (const T1 &x, const T2 &y)
{
  return !lts_p (y, x);
}

/* Return true if X <= Y when both are treated as unsigned values.  */
template <typename T1, typename T2>
inline bool
wi::leu_p (const T1 &x, const T2 &y)
{
  return !ltu_p (y, x);
}

/* Return true if X <= Y.  Signedness of X and Y is indicated by SGN.  */
template <typename T1, typename T2>
inline bool
wi::le_p (const T1 &x, const T2 &y, signop sgn)
{
  if (sgn == SIGNED)
    return les_p (x, y);
  else
    return leu_p (x, y);
}

/* Return true if X > Y when both are treated as signed values.  */
template <typename T1, typename T2>
inline bool
wi::gts_p (const T1 &x, const T2 &y)
{
  return lts_p (y, x);
}

/* Return true if X > Y when both are treated as unsigned values.  */
template <typename T1, typename T2>
inline bool
wi::gtu_p (const T1 &x, const T2 &y)
{
  return ltu_p (y, x);
}

/* Return true if X > Y.  Signedness of X and Y is indicated by SGN.  */
template <typename T1, typename T2>
inline bool
wi::gt_p (const T1 &x, const T2 &y, signop sgn)
{
  if (sgn == SIGNED)
    return gts_p (x, y);
  else
    return gtu_p (x, y);
}

/* Return true if X >= Y when both are treated as signed values.  */
template <typename T1, typename T2>
inline bool
wi::ges_p (const T1 &x, const T2 &y)
{
  return !lts_p (x, y);
}

/* Return true if X >= Y when both are treated as unsigned values.  */
template <typename T1, typename T2>
inline bool
wi::geu_p (const T1 &x, const T2 &y)
{
  return !ltu_p (x, y);
}

/* Return true if X >= Y.  Signedness of X and Y is indicated by SGN.  */
template <typename T1, typename T2>
inline bool
wi::ge_p (const T1 &x, const T2 &y, signop sgn)
{
  if (sgn == SIGNED)
    return ges_p (x, y);
  else
    return geu_p (x, y);
}

/* Return -1 if X < Y, 0 if X == Y and 1 if X > Y.  Treat both X and Y
   as signed values.  */
template <typename T1, typename T2>
inline int
wi::cmps (const T1 &x, const T2 &y)
{
  unsigned int precision = get_binary_precision (x, y);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y, precision);
  if (wi::fits_shwi_p (yi))
    {
      /* Special case for comparisons with 0.  */
      if (STATIC_CONSTANT_P (yi.val[0] == 0))
	return neg_p (xi) ? -1 : !(xi.len == 1 && xi.val[0] == 0);
      /* If x fits into a signed HWI, we can compare directly.  */
      if (wi::fits_shwi_p (xi))
	{
	  HOST_WIDE_INT xl = xi.to_shwi ();
	  HOST_WIDE_INT yl = yi.to_shwi ();
	  return xl < yl ? -1 : xl > yl;
	}
      /* If x doesn't fit and is negative, then it must be more
	 negative than any signed HWI, and hence smaller than y.  */
      if (neg_p (xi))
	return -1;
      /* If x is positive, then it must be larger than any signed HWI,
	 and hence greater than y.  */
      return 1;
    }
  /* Optimize the opposite case, if it can be detected at compile time.  */
  if (STATIC_CONSTANT_P (xi.len == 1))
    /* If YI is negative it is lower than the least HWI.
       If YI is positive it is greater than the greatest HWI.  */
    return neg_p (yi) ? 1 : -1;
  return cmps_large (xi.val, xi.len, precision, yi.val, yi.len);
}

/* Return -1 if X < Y, 0 if X == Y and 1 if X > Y.  Treat both X and Y
   as unsigned values.  */
template <typename T1, typename T2>
inline int
wi::cmpu (const T1 &x, const T2 &y)
{
  unsigned int precision = get_binary_precision (x, y);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y, precision);
  /* Optimize comparisons with constants.  */
  if (STATIC_CONSTANT_P (yi.len == 1 && yi.val[0] >= 0))
    {
      /* If XI doesn't fit in a HWI then it must be larger than YI.  */
      if (xi.len != 1)
	return 1;
      /* Otherwise compare directly.  */
      unsigned HOST_WIDE_INT xl = xi.to_uhwi ();
      unsigned HOST_WIDE_INT yl = yi.val[0];
      return xl < yl ? -1 : xl > yl;
    }
  if (STATIC_CONSTANT_P (xi.len == 1 && xi.val[0] >= 0))
    {
      /* If YI doesn't fit in a HWI then it must be larger than XI.  */
      if (yi.len != 1)
	return -1;
      /* Otherwise compare directly.  */
      unsigned HOST_WIDE_INT xl = xi.val[0];
      unsigned HOST_WIDE_INT yl = yi.to_uhwi ();
      return xl < yl ? -1 : xl > yl;
    }
  /* Optimize the case of two HWIs.  The HWIs are implicitly sign-extended
     for precisions greater than HOST_BITS_WIDE_INT, but sign-extending both
     values does not change the result.  */
  if (__builtin_expect (xi.len + yi.len == 2, true))
    {
      unsigned HOST_WIDE_INT xl = xi.to_uhwi ();
      unsigned HOST_WIDE_INT yl = yi.to_uhwi ();
      return xl < yl ? -1 : xl > yl;
    }
  return cmpu_large (xi.val, xi.len, precision, yi.val, yi.len);
}

/* Return -1 if X < Y, 0 if X == Y and 1 if X > Y.  Signedness of
   X and Y indicated by SGN.  */
template <typename T1, typename T2>
inline int
wi::cmp (const T1 &x, const T2 &y, signop sgn)
{
  if (sgn == SIGNED)
    return cmps (x, y);
  else
    return cmpu (x, y);
}

/* Return ~x.  */
template <typename T>
inline WI_UNARY_RESULT (T)
wi::bit_not (const T &x)
{
  WI_UNARY_RESULT_VAR (result, val, T, x);
  WIDE_INT_REF_FOR (T) xi (x, get_precision (result));
  for (unsigned int i = 0; i < xi.len; ++i)
    val[i] = ~xi.val[i];
  result.set_len (xi.len);
  return result;
}

/* Return -x.  */
template <typename T>
inline WI_UNARY_RESULT (T)
wi::neg (const T &x)
{
  return sub (0, x);
}

/* Return -x.  Indicate in *OVERFLOW if X is the minimum signed value.  */
template <typename T>
inline WI_UNARY_RESULT (T)
wi::neg (const T &x, bool *overflow)
{
  *overflow = only_sign_bit_p (x);
  return sub (0, x);
}

/* Return the absolute value of x.  */
template <typename T>
inline WI_UNARY_RESULT (T)
wi::abs (const T &x)
{
  return neg_p (x) ? neg (x) : WI_UNARY_RESULT (T) (x);
}

/* Return the result of sign-extending the low OFFSET bits of X.  */
template <typename T>
inline WI_UNARY_RESULT (T)
wi::sext (const T &x, unsigned int offset)
{
  WI_UNARY_RESULT_VAR (result, val, T, x);
  unsigned int precision = get_precision (result);
  WIDE_INT_REF_FOR (T) xi (x, precision);

  if (offset <= HOST_BITS_PER_WIDE_INT)
    {
      val[0] = sext_hwi (xi.ulow (), offset);
      result.set_len (1, true);
    }
  else
    result.set_len (sext_large (val, xi.val, xi.len, precision, offset));
  return result;
}

/* Return the result of zero-extending the low OFFSET bits of X.  */
template <typename T>
inline WI_UNARY_RESULT (T)
wi::zext (const T &x, unsigned int offset)
{
  WI_UNARY_RESULT_VAR (result, val, T, x);
  unsigned int precision = get_precision (result);
  WIDE_INT_REF_FOR (T) xi (x, precision);

  /* This is not just an optimization, it is actually required to
     maintain canonization.  */
  if (offset >= precision)
    {
      wi::copy (result, xi);
      return result;
    }

  /* In these cases we know that at least the top bit will be clear,
     so no sign extension is necessary.  */
  if (offset < HOST_BITS_PER_WIDE_INT)
    {
      val[0] = zext_hwi (xi.ulow (), offset);
      result.set_len (1, true);
    }
  else
    result.set_len (zext_large (val, xi.val, xi.len, precision, offset), true);
  return result;
}

/* Return the result of extending the low OFFSET bits of X according to
   signedness SGN.  */
template <typename T>
inline WI_UNARY_RESULT (T)
wi::ext (const T &x, unsigned int offset, signop sgn)
{
  return sgn == SIGNED ? sext (x, offset) : zext (x, offset);
}

/* Return an integer that represents X | (1 << bit).  */
template <typename T>
inline WI_UNARY_RESULT (T)
wi::set_bit (const T &x, unsigned int bit)
{
  WI_UNARY_RESULT_VAR (result, val, T, x);
  unsigned int precision = get_precision (result);
  WIDE_INT_REF_FOR (T) xi (x, precision);
  if (precision <= HOST_BITS_PER_WIDE_INT)
    {
      val[0] = xi.ulow () | (HOST_WIDE_INT_1U << bit);
      result.set_len (1);
    }
  else
    result.set_len (set_bit_large (val, xi.val, xi.len, precision, bit));
  return result;
}

/* Return the mininum of X and Y, treating them both as having
   signedness SGN.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::min (const T1 &x, const T2 &y, signop sgn)
{
  WI_BINARY_RESULT_VAR (result, val ATTRIBUTE_UNUSED, T1, x, T2, y);
  unsigned int precision = get_precision (result);
  if (wi::le_p (x, y, sgn))
    wi::copy (result, WIDE_INT_REF_FOR (T1) (x, precision));
  else
    wi::copy (result, WIDE_INT_REF_FOR (T2) (y, precision));
  return result;
}

/* Return the minimum of X and Y, treating both as signed values.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::smin (const T1 &x, const T2 &y)
{
  return wi::min (x, y, SIGNED);
}

/* Return the minimum of X and Y, treating both as unsigned values.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::umin (const T1 &x, const T2 &y)
{
  return wi::min (x, y, UNSIGNED);
}

/* Return the maxinum of X and Y, treating them both as having
   signedness SGN.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::max (const T1 &x, const T2 &y, signop sgn)
{
  WI_BINARY_RESULT_VAR (result, val ATTRIBUTE_UNUSED, T1, x, T2, y);
  unsigned int precision = get_precision (result);
  if (wi::ge_p (x, y, sgn))
    wi::copy (result, WIDE_INT_REF_FOR (T1) (x, precision));
  else
    wi::copy (result, WIDE_INT_REF_FOR (T2) (y, precision));
  return result;
}

/* Return the maximum of X and Y, treating both as signed values.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::smax (const T1 &x, const T2 &y)
{
  return wi::max (x, y, SIGNED);
}

/* Return the maximum of X and Y, treating both as unsigned values.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::umax (const T1 &x, const T2 &y)
{
  return wi::max (x, y, UNSIGNED);
}

/* Return X & Y.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::bit_and (const T1 &x, const T2 &y)
{
  WI_BINARY_RESULT_VAR (result, val, T1, x, T2, y);
  unsigned int precision = get_precision (result);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y, precision);
  bool is_sign_extended = xi.is_sign_extended && yi.is_sign_extended;
  if (__builtin_expect (xi.len + yi.len == 2, true))
    {
      val[0] = xi.ulow () & yi.ulow ();
      result.set_len (1, is_sign_extended);
    }
  else
    result.set_len (and_large (val, xi.val, xi.len, yi.val, yi.len,
			       precision), is_sign_extended);
  return result;
}

/* Return X & ~Y.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::bit_and_not (const T1 &x, const T2 &y)
{
  WI_BINARY_RESULT_VAR (result, val, T1, x, T2, y);
  unsigned int precision = get_precision (result);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y, precision);
  bool is_sign_extended = xi.is_sign_extended && yi.is_sign_extended;
  if (__builtin_expect (xi.len + yi.len == 2, true))
    {
      val[0] = xi.ulow () & ~yi.ulow ();
      result.set_len (1, is_sign_extended);
    }
  else
    result.set_len (and_not_large (val, xi.val, xi.len, yi.val, yi.len,
				   precision), is_sign_extended);
  return result;
}

/* Return X | Y.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::bit_or (const T1 &x, const T2 &y)
{
  WI_BINARY_RESULT_VAR (result, val, T1, x, T2, y);
  unsigned int precision = get_precision (result);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y, precision);
  bool is_sign_extended = xi.is_sign_extended && yi.is_sign_extended;
  if (__builtin_expect (xi.len + yi.len == 2, true))
    {
      val[0] = xi.ulow () | yi.ulow ();
      result.set_len (1, is_sign_extended);
    }
  else
    result.set_len (or_large (val, xi.val, xi.len,
			      yi.val, yi.len, precision), is_sign_extended);
  return result;
}

/* Return X | ~Y.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::bit_or_not (const T1 &x, const T2 &y)
{
  WI_BINARY_RESULT_VAR (result, val, T1, x, T2, y);
  unsigned int precision = get_precision (result);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y, precision);
  bool is_sign_extended = xi.is_sign_extended && yi.is_sign_extended;
  if (__builtin_expect (xi.len + yi.len == 2, true))
    {
      val[0] = xi.ulow () | ~yi.ulow ();
      result.set_len (1, is_sign_extended);
    }
  else
    result.set_len (or_not_large (val, xi.val, xi.len, yi.val, yi.len,
				  precision), is_sign_extended);
  return result;
}

/* Return X ^ Y.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::bit_xor (const T1 &x, const T2 &y)
{
  WI_BINARY_RESULT_VAR (result, val, T1, x, T2, y);
  unsigned int precision = get_precision (result);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y, precision);
  bool is_sign_extended = xi.is_sign_extended && yi.is_sign_extended;
  if (__builtin_expect (xi.len + yi.len == 2, true))
    {
      val[0] = xi.ulow () ^ yi.ulow ();
      result.set_len (1, is_sign_extended);
    }
  else
    result.set_len (xor_large (val, xi.val, xi.len,
			       yi.val, yi.len, precision), is_sign_extended);
  return result;
}

/* Return X + Y.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::add (const T1 &x, const T2 &y)
{
  WI_BINARY_RESULT_VAR (result, val, T1, x, T2, y);
  unsigned int precision = get_precision (result);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y, precision);
  if (precision <= HOST_BITS_PER_WIDE_INT)
    {
      val[0] = xi.ulow () + yi.ulow ();
      result.set_len (1);
    }
  /* If the precision is known at compile time to be greater than
     HOST_BITS_PER_WIDE_INT, we can optimize the single-HWI case
     knowing that (a) all bits in those HWIs are significant and
     (b) the result has room for at least two HWIs.  This provides
     a fast path for things like offset_int and widest_int.

     The STATIC_CONSTANT_P test prevents this path from being
     used for wide_ints.  wide_ints with precisions greater than
     HOST_BITS_PER_WIDE_INT are relatively rare and there's not much
     point handling them inline.  */
  else if (STATIC_CONSTANT_P (precision > HOST_BITS_PER_WIDE_INT)
	   && __builtin_expect (xi.len + yi.len == 2, true))
    {
      unsigned HOST_WIDE_INT xl = xi.ulow ();
      unsigned HOST_WIDE_INT yl = yi.ulow ();
      unsigned HOST_WIDE_INT resultl = xl + yl;
      val[0] = resultl;
      val[1] = (HOST_WIDE_INT) resultl < 0 ? 0 : -1;
      result.set_len (1 + (((resultl ^ xl) & (resultl ^ yl))
			   >> (HOST_BITS_PER_WIDE_INT - 1)));
    }
  else
    result.set_len (add_large (val, xi.val, xi.len,
			       yi.val, yi.len, precision,
			       UNSIGNED, 0));
  return result;
}

/* Return X + Y.  Treat X and Y as having the signednes given by SGN
   and indicate in *OVERFLOW whether the operation overflowed.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::add (const T1 &x, const T2 &y, signop sgn, bool *overflow)
{
  WI_BINARY_RESULT_VAR (result, val, T1, x, T2, y);
  unsigned int precision = get_precision (result);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y, precision);
  if (precision <= HOST_BITS_PER_WIDE_INT)
    {
      unsigned HOST_WIDE_INT xl = xi.ulow ();
      unsigned HOST_WIDE_INT yl = yi.ulow ();
      unsigned HOST_WIDE_INT resultl = xl + yl;
      if (sgn == SIGNED)
	*overflow = (((resultl ^ xl) & (resultl ^ yl))
		     >> (precision - 1)) & 1;
      else
	*overflow = ((resultl << (HOST_BITS_PER_WIDE_INT - precision))
		     < (xl << (HOST_BITS_PER_WIDE_INT - precision)));
      val[0] = resultl;
      result.set_len (1);
    }
  else
    result.set_len (add_large (val, xi.val, xi.len,
			       yi.val, yi.len, precision,
			       sgn, overflow));
  return result;
}

/* Return X - Y.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::sub (const T1 &x, const T2 &y)
{
  WI_BINARY_RESULT_VAR (result, val, T1, x, T2, y);
  unsigned int precision = get_precision (result);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y, precision);
  if (precision <= HOST_BITS_PER_WIDE_INT)
    {
      val[0] = xi.ulow () - yi.ulow ();
      result.set_len (1);
    }
  /* If the precision is known at compile time to be greater than
     HOST_BITS_PER_WIDE_INT, we can optimize the single-HWI case
     knowing that (a) all bits in those HWIs are significant and
     (b) the result has room for at least two HWIs.  This provides
     a fast path for things like offset_int and widest_int.

     The STATIC_CONSTANT_P test prevents this path from being
     used for wide_ints.  wide_ints with precisions greater than
     HOST_BITS_PER_WIDE_INT are relatively rare and there's not much
     point handling them inline.  */
  else if (STATIC_CONSTANT_P (precision > HOST_BITS_PER_WIDE_INT)
	   && __builtin_expect (xi.len + yi.len == 2, true))
    {
      unsigned HOST_WIDE_INT xl = xi.ulow ();
      unsigned HOST_WIDE_INT yl = yi.ulow ();
      unsigned HOST_WIDE_INT resultl = xl - yl;
      val[0] = resultl;
      val[1] = (HOST_WIDE_INT) resultl < 0 ? 0 : -1;
      result.set_len (1 + (((resultl ^ xl) & (xl ^ yl))
			   >> (HOST_BITS_PER_WIDE_INT - 1)));
    }
  else
    result.set_len (sub_large (val, xi.val, xi.len,
			       yi.val, yi.len, precision,
			       UNSIGNED, 0));
  return result;
}

/* Return X - Y.  Treat X and Y as having the signednes given by SGN
   and indicate in *OVERFLOW whether the operation overflowed.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::sub (const T1 &x, const T2 &y, signop sgn, bool *overflow)
{
  WI_BINARY_RESULT_VAR (result, val, T1, x, T2, y);
  unsigned int precision = get_precision (result);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y, precision);
  if (precision <= HOST_BITS_PER_WIDE_INT)
    {
      unsigned HOST_WIDE_INT xl = xi.ulow ();
      unsigned HOST_WIDE_INT yl = yi.ulow ();
      unsigned HOST_WIDE_INT resultl = xl - yl;
      if (sgn == SIGNED)
	*overflow = (((xl ^ yl) & (resultl ^ xl)) >> (precision - 1)) & 1;
      else
	*overflow = ((resultl << (HOST_BITS_PER_WIDE_INT - precision))
		     > (xl << (HOST_BITS_PER_WIDE_INT - precision)));
      val[0] = resultl;
      result.set_len (1);
    }
  else
    result.set_len (sub_large (val, xi.val, xi.len,
			       yi.val, yi.len, precision,
			       sgn, overflow));
  return result;
}

/* Return X * Y.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::mul (const T1 &x, const T2 &y)
{
  WI_BINARY_RESULT_VAR (result, val, T1, x, T2, y);
  unsigned int precision = get_precision (result);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y, precision);
  if (precision <= HOST_BITS_PER_WIDE_INT)
    {
      val[0] = xi.ulow () * yi.ulow ();
      result.set_len (1);
    }
  else
    result.set_len (mul_internal (val, xi.val, xi.len, yi.val, yi.len,
				  precision, UNSIGNED, 0, false));
  return result;
}

/* Return X * Y.  Treat X and Y as having the signednes given by SGN
   and indicate in *OVERFLOW whether the operation overflowed.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::mul (const T1 &x, const T2 &y, signop sgn, bool *overflow)
{
  WI_BINARY_RESULT_VAR (result, val, T1, x, T2, y);
  unsigned int precision = get_precision (result);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y, precision);
  result.set_len (mul_internal (val, xi.val, xi.len,
				yi.val, yi.len, precision,
				sgn, overflow, false));
  return result;
}

/* Return X * Y, treating both X and Y as signed values.  Indicate in
   *OVERFLOW whether the operation overflowed.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::smul (const T1 &x, const T2 &y, bool *overflow)
{
  return mul (x, y, SIGNED, overflow);
}

/* Return X * Y, treating both X and Y as unsigned values.  Indicate in
   *OVERFLOW whether the operation overflowed.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::umul (const T1 &x, const T2 &y, bool *overflow)
{
  return mul (x, y, UNSIGNED, overflow);
}

/* Perform a widening multiplication of X and Y, extending the values
   according to SGN, and return the high part of the result.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::mul_high (const T1 &x, const T2 &y, signop sgn)
{
  WI_BINARY_RESULT_VAR (result, val, T1, x, T2, y);
  unsigned int precision = get_precision (result);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y, precision);
  result.set_len (mul_internal (val, xi.val, xi.len,
				yi.val, yi.len, precision,
				sgn, 0, true));
  return result;
}

/* Return X / Y, rouding towards 0.  Treat X and Y as having the
   signedness given by SGN.  Indicate in *OVERFLOW if the result
   overflows.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::div_trunc (const T1 &x, const T2 &y, signop sgn, bool *overflow)
{
  WI_BINARY_RESULT_VAR (quotient, quotient_val, T1, x, T2, y);
  unsigned int precision = get_precision (quotient);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y);

  quotient.set_len (divmod_internal (quotient_val, 0, 0, xi.val, xi.len,
				     precision,
				     yi.val, yi.len, yi.precision,
				     sgn, overflow));
  return quotient;
}

/* Return X / Y, rouding towards 0.  Treat X and Y as signed values.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::sdiv_trunc (const T1 &x, const T2 &y)
{
  return div_trunc (x, y, SIGNED);
}

/* Return X / Y, rouding towards 0.  Treat X and Y as unsigned values.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::udiv_trunc (const T1 &x, const T2 &y)
{
  return div_trunc (x, y, UNSIGNED);
}

/* Return X / Y, rouding towards -inf.  Treat X and Y as having the
   signedness given by SGN.  Indicate in *OVERFLOW if the result
   overflows.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::div_floor (const T1 &x, const T2 &y, signop sgn, bool *overflow)
{
  WI_BINARY_RESULT_VAR (quotient, quotient_val, T1, x, T2, y);
  WI_BINARY_RESULT_VAR (remainder, remainder_val, T1, x, T2, y);
  unsigned int precision = get_precision (quotient);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y);

  unsigned int remainder_len;
  quotient.set_len (divmod_internal (quotient_val,
				     &remainder_len, remainder_val,
				     xi.val, xi.len, precision,
				     yi.val, yi.len, yi.precision, sgn,
				     overflow));
  remainder.set_len (remainder_len);
  if (wi::neg_p (x, sgn) != wi::neg_p (y, sgn) && remainder != 0)
    return quotient - 1;
  return quotient;
}

/* Return X / Y, rouding towards -inf.  Treat X and Y as signed values.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::sdiv_floor (const T1 &x, const T2 &y)
{
  return div_floor (x, y, SIGNED);
}

/* Return X / Y, rouding towards -inf.  Treat X and Y as unsigned values.  */
/* ??? Why do we have both this and udiv_trunc.  Aren't they the same?  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::udiv_floor (const T1 &x, const T2 &y)
{
  return div_floor (x, y, UNSIGNED);
}

/* Return X / Y, rouding towards +inf.  Treat X and Y as having the
   signedness given by SGN.  Indicate in *OVERFLOW if the result
   overflows.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::div_ceil (const T1 &x, const T2 &y, signop sgn, bool *overflow)
{
  WI_BINARY_RESULT_VAR (quotient, quotient_val, T1, x, T2, y);
  WI_BINARY_RESULT_VAR (remainder, remainder_val, T1, x, T2, y);
  unsigned int precision = get_precision (quotient);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y);

  unsigned int remainder_len;
  quotient.set_len (divmod_internal (quotient_val,
				     &remainder_len, remainder_val,
				     xi.val, xi.len, precision,
				     yi.val, yi.len, yi.precision, sgn,
				     overflow));
  remainder.set_len (remainder_len);
  if (wi::neg_p (x, sgn) == wi::neg_p (y, sgn) && remainder != 0)
    return quotient + 1;
  return quotient;
}

/* Return X / Y, rouding towards +inf.  Treat X and Y as unsigned values.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::udiv_ceil (const T1 &x, const T2 &y)
{
  return div_ceil (x, y, UNSIGNED);
}

/* Return X / Y, rouding towards nearest with ties away from zero.
   Treat X and Y as having the signedness given by SGN.  Indicate
   in *OVERFLOW if the result overflows.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::div_round (const T1 &x, const T2 &y, signop sgn, bool *overflow)
{
  WI_BINARY_RESULT_VAR (quotient, quotient_val, T1, x, T2, y);
  WI_BINARY_RESULT_VAR (remainder, remainder_val, T1, x, T2, y);
  unsigned int precision = get_precision (quotient);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y);

  unsigned int remainder_len;
  quotient.set_len (divmod_internal (quotient_val,
				     &remainder_len, remainder_val,
				     xi.val, xi.len, precision,
				     yi.val, yi.len, yi.precision, sgn,
				     overflow));
  remainder.set_len (remainder_len);

  if (remainder != 0)
    {
      if (sgn == SIGNED)
	{
	  WI_BINARY_RESULT (T1, T2) abs_remainder = wi::abs (remainder);
	  if (wi::geu_p (abs_remainder, wi::sub (wi::abs (y), abs_remainder)))
	    {
	      if (wi::neg_p (x, sgn) != wi::neg_p (y, sgn))
		return quotient - 1;
	      else
		return quotient + 1;
	    }
	}
      else
	{
	  if (wi::geu_p (remainder, wi::sub (y, remainder)))
	    return quotient + 1;
	}
    }
  return quotient;
}

/* Return X / Y, rouding towards 0.  Treat X and Y as having the
   signedness given by SGN.  Store the remainder in *REMAINDER_PTR.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::divmod_trunc (const T1 &x, const T2 &y, signop sgn,
		  WI_BINARY_RESULT (T1, T2) *remainder_ptr)
{
  WI_BINARY_RESULT_VAR (quotient, quotient_val, T1, x, T2, y);
  WI_BINARY_RESULT_VAR (remainder, remainder_val, T1, x, T2, y);
  unsigned int precision = get_precision (quotient);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y);

  unsigned int remainder_len;
  quotient.set_len (divmod_internal (quotient_val,
				     &remainder_len, remainder_val,
				     xi.val, xi.len, precision,
				     yi.val, yi.len, yi.precision, sgn, 0));
  remainder.set_len (remainder_len);

  *remainder_ptr = remainder;
  return quotient;
}

/* Compute the greatest common divisor of two numbers A and B using
   Euclid's algorithm.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::gcd (const T1 &a, const T2 &b, signop sgn)
{
  T1 x, y, z;

  x = wi::abs (a);
  y = wi::abs (b);

  while (gt_p (x, 0, sgn))
    {
      z = mod_trunc (y, x, sgn);
      y = x;
      x = z;
    }

  return y;
}

/* Compute X / Y, rouding towards 0, and return the remainder.
   Treat X and Y as having the signedness given by SGN.  Indicate
   in *OVERFLOW if the division overflows.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::mod_trunc (const T1 &x, const T2 &y, signop sgn, bool *overflow)
{
  WI_BINARY_RESULT_VAR (remainder, remainder_val, T1, x, T2, y);
  unsigned int precision = get_precision (remainder);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y);

  unsigned int remainder_len;
  divmod_internal (0, &remainder_len, remainder_val,
		   xi.val, xi.len, precision,
		   yi.val, yi.len, yi.precision, sgn, overflow);
  remainder.set_len (remainder_len);

  return remainder;
}

/* Compute X / Y, rouding towards 0, and return the remainder.
   Treat X and Y as signed values.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::smod_trunc (const T1 &x, const T2 &y)
{
  return mod_trunc (x, y, SIGNED);
}

/* Compute X / Y, rouding towards 0, and return the remainder.
   Treat X and Y as unsigned values.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::umod_trunc (const T1 &x, const T2 &y)
{
  return mod_trunc (x, y, UNSIGNED);
}

/* Compute X / Y, rouding towards -inf, and return the remainder.
   Treat X and Y as having the signedness given by SGN.  Indicate
   in *OVERFLOW if the division overflows.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::mod_floor (const T1 &x, const T2 &y, signop sgn, bool *overflow)
{
  WI_BINARY_RESULT_VAR (quotient, quotient_val, T1, x, T2, y);
  WI_BINARY_RESULT_VAR (remainder, remainder_val, T1, x, T2, y);
  unsigned int precision = get_precision (quotient);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y);

  unsigned int remainder_len;
  quotient.set_len (divmod_internal (quotient_val,
				     &remainder_len, remainder_val,
				     xi.val, xi.len, precision,
				     yi.val, yi.len, yi.precision, sgn,
				     overflow));
  remainder.set_len (remainder_len);

  if (wi::neg_p (x, sgn) != wi::neg_p (y, sgn) && remainder != 0)
    return remainder + y;
  return remainder;
}

/* Compute X / Y, rouding towards -inf, and return the remainder.
   Treat X and Y as unsigned values.  */
/* ??? Why do we have both this and umod_trunc.  Aren't they the same?  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::umod_floor (const T1 &x, const T2 &y)
{
  return mod_floor (x, y, UNSIGNED);
}

/* Compute X / Y, rouding towards +inf, and return the remainder.
   Treat X and Y as having the signedness given by SGN.  Indicate
   in *OVERFLOW if the division overflows.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::mod_ceil (const T1 &x, const T2 &y, signop sgn, bool *overflow)
{
  WI_BINARY_RESULT_VAR (quotient, quotient_val, T1, x, T2, y);
  WI_BINARY_RESULT_VAR (remainder, remainder_val, T1, x, T2, y);
  unsigned int precision = get_precision (quotient);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y);

  unsigned int remainder_len;
  quotient.set_len (divmod_internal (quotient_val,
				     &remainder_len, remainder_val,
				     xi.val, xi.len, precision,
				     yi.val, yi.len, yi.precision, sgn,
				     overflow));
  remainder.set_len (remainder_len);

  if (wi::neg_p (x, sgn) == wi::neg_p (y, sgn) && remainder != 0)
    return remainder - y;
  return remainder;
}

/* Compute X / Y, rouding towards nearest with ties away from zero,
   and return the remainder.  Treat X and Y as having the signedness
   given by SGN.  Indicate in *OVERFLOW if the division overflows.  */
template <typename T1, typename T2>
inline WI_BINARY_RESULT (T1, T2)
wi::mod_round (const T1 &x, const T2 &y, signop sgn, bool *overflow)
{
  WI_BINARY_RESULT_VAR (quotient, quotient_val, T1, x, T2, y);
  WI_BINARY_RESULT_VAR (remainder, remainder_val, T1, x, T2, y);
  unsigned int precision = get_precision (quotient);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y);

  unsigned int remainder_len;
  quotient.set_len (divmod_internal (quotient_val,
				     &remainder_len, remainder_val,
				     xi.val, xi.len, precision,
				     yi.val, yi.len, yi.precision, sgn,
				     overflow));
  remainder.set_len (remainder_len);

  if (remainder != 0)
    {
      if (sgn == SIGNED)
	{
	  WI_BINARY_RESULT (T1, T2) abs_remainder = wi::abs (remainder);
	  if (wi::geu_p (abs_remainder, wi::sub (wi::abs (y), abs_remainder)))
	    {
	      if (wi::neg_p (x, sgn) != wi::neg_p (y, sgn))
		return remainder + y;
	      else
		return remainder - y;
	    }
	}
      else
	{
	  if (wi::geu_p (remainder, wi::sub (y, remainder)))
	    return remainder - y;
	}
    }
  return remainder;
}

/* Return true if X is a multiple of Y.  Treat X and Y as having the
   signedness given by SGN.  */
template <typename T1, typename T2>
inline bool
wi::multiple_of_p (const T1 &x, const T2 &y, signop sgn)
{
  return wi::mod_trunc (x, y, sgn) == 0;
}

/* Return true if X is a multiple of Y, storing X / Y in *RES if so.
   Treat X and Y as having the signedness given by SGN.  */
template <typename T1, typename T2>
inline bool
wi::multiple_of_p (const T1 &x, const T2 &y, signop sgn,
		   WI_BINARY_RESULT (T1, T2) *res)
{
  WI_BINARY_RESULT (T1, T2) remainder;
  WI_BINARY_RESULT (T1, T2) quotient
    = divmod_trunc (x, y, sgn, &remainder);
  if (remainder == 0)
    {
      *res = quotient;
      return true;
    }
  return false;
}

/* Return X << Y.  Return 0 if Y is greater than or equal to
   the precision of X.  */
template <typename T1, typename T2>
inline WI_UNARY_RESULT (T1)
wi::lshift (const T1 &x, const T2 &y)
{
  WI_UNARY_RESULT_VAR (result, val, T1, x);
  unsigned int precision = get_precision (result);
  WIDE_INT_REF_FOR (T1) xi (x, precision);
  WIDE_INT_REF_FOR (T2) yi (y);
  /* Handle the simple cases quickly.   */
  if (geu_p (yi, precision))
    {
      val[0] = 0;
      result.set_len (1);
    }
  else
    {
      unsigned int shift = yi.to_uhwi ();
      /* For fixed-precision integers like offset_int and widest_int,
	 handle the case where the shift value is constant and the
	 result is a single nonnegative HWI (meaning that we don't
	 need to worry about val[1]).  This is particularly common
	 for converting a byte count to a bit count.

	 For variable-precision integers like wide_int, handle HWI
	 and sub-HWI integers inline.  */
      if (STATIC_CONSTANT_P (xi.precision > HOST_BITS_PER_WIDE_INT)
	  ? (STATIC_CONSTANT_P (shift < HOST_BITS_PER_WIDE_INT - 1)
	     && xi.len == 1
	     && xi.val[0] <= (HOST_WIDE_INT) ((unsigned HOST_WIDE_INT)
					      HOST_WIDE_INT_MAX >> shift))
	  : precision <= HOST_BITS_PER_WIDE_INT)
	{
	  val[0] = xi.ulow () << shift;
	  result.set_len (1);
	}
      else
	result.set_len (lshift_large (val, xi.val, xi.len,
				      precision, shift));
    }
  return result;
}

/* Return X >> Y, using a logical shift.  Return 0 if Y is greater than
   or equal to the precision of X.  */
template <typename T1, typename T2>
inline WI_UNARY_RESULT (T1)
wi::lrshift (const T1 &x, const T2 &y)
{
  WI_UNARY_RESULT_VAR (result, val, T1, x);
  /* Do things in the precision of the input rather than the output,
     since the result can be no larger than that.  */
  WIDE_INT_REF_FOR (T1) xi (x);
  WIDE_INT_REF_FOR (T2) yi (y);
  /* Handle the simple cases quickly.   */
  if (geu_p (yi, xi.precision))
    {
      val[0] = 0;
      result.set_len (1);
    }
  else
    {
      unsigned int shift = yi.to_uhwi ();
      /* For fixed-precision integers like offset_int and widest_int,
	 handle the case where the shift value is constant and the
	 shifted value is a single nonnegative HWI (meaning that all
	 bits above the HWI are zero).  This is particularly common
	 for converting a bit count to a byte count.

	 For variable-precision integers like wide_int, handle HWI
	 and sub-HWI integers inline.  */
      if (STATIC_CONSTANT_P (xi.precision > HOST_BITS_PER_WIDE_INT)
	  ? (shift < HOST_BITS_PER_WIDE_INT
	     && xi.len == 1
	     && xi.val[0] >= 0)
	  : xi.precision <= HOST_BITS_PER_WIDE_INT)
	{
	  val[0] = xi.to_uhwi () >> shift;
	  result.set_len (1);
	}
      else
	result.set_len (lrshift_large (val, xi.val, xi.len, xi.precision,
				       get_precision (result), shift));
    }
  return result;
}

/* Return X >> Y, using an arithmetic shift.  Return a sign mask if
   Y is greater than or equal to the precision of X.  */
template <typename T1, typename T2>
inline WI_UNARY_RESULT (T1)
wi::arshift (const T1 &x, const T2 &y)
{
  WI_UNARY_RESULT_VAR (result, val, T1, x);
  /* Do things in the precision of the input rather than the output,
     since the result can be no larger than that.  */
  WIDE_INT_REF_FOR (T1) xi (x);
  WIDE_INT_REF_FOR (T2) yi (y);
  /* Handle the simple cases quickly.   */
  if (geu_p (yi, xi.precision))
    {
      val[0] = sign_mask (x);
      result.set_len (1);
    }
  else
    {
      unsigned int shift = yi.to_uhwi ();
      if (xi.precision <= HOST_BITS_PER_WIDE_INT)
	{
	  val[0] = sext_hwi (xi.ulow () >> shift, xi.precision - shift);
	  result.set_len (1, true);
	}
      else
	result.set_len (arshift_large (val, xi.val, xi.len, xi.precision,
				       get_precision (result), shift));
    }
  return result;
}

/* Return X >> Y, using an arithmetic shift if SGN is SIGNED and a
   logical shift otherwise.  */
template <typename T1, typename T2>
inline WI_UNARY_RESULT (T1)
wi::rshift (const T1 &x, const T2 &y, signop sgn)
{
  if (sgn == UNSIGNED)
    return lrshift (x, y);
  else
    return arshift (x, y);
}

/* Return the result of rotating the low WIDTH bits of X left by Y
   bits and zero-extending the result.  Use a full-width rotate if
   WIDTH is zero.  */
template <typename T1, typename T2>
WI_UNARY_RESULT (T1)
wi::lrotate (const T1 &x, const T2 &y, unsigned int width)
{
  unsigned int precision = get_binary_precision (x, x);
  if (width == 0)
    width = precision;
  WI_UNARY_RESULT (T2) ymod = umod_trunc (y, width);
  WI_UNARY_RESULT (T1) left = wi::lshift (x, ymod);
  WI_UNARY_RESULT (T1) right = wi::lrshift (x, wi::sub (width, ymod));
  if (width != precision)
    return wi::zext (left, width) | wi::zext (right, width);
  return left | right;
}

/* Return the result of rotating the low WIDTH bits of X right by Y
   bits and zero-extending the result.  Use a full-width rotate if
   WIDTH is zero.  */
template <typename T1, typename T2>
WI_UNARY_RESULT (T1)
wi::rrotate (const T1 &x, const T2 &y, unsigned int width)
{
  unsigned int precision = get_binary_precision (x, x);
  if (width == 0)
    width = precision;
  WI_UNARY_RESULT (T2) ymod = umod_trunc (y, width);
  WI_UNARY_RESULT (T1) right = wi::lrshift (x, ymod);
  WI_UNARY_RESULT (T1) left = wi::lshift (x, wi::sub (width, ymod));
  if (width != precision)
    return wi::zext (left, width) | wi::zext (right, width);
  return left | right;
}

/* Return 0 if the number of 1s in X is even and 1 if the number of 1s
   is odd.  */
inline int
wi::parity (const wide_int_ref &x)
{
  return popcount (x) & 1;
}

/* Extract WIDTH bits from X, starting at BITPOS.  */
template <typename T>
inline unsigned HOST_WIDE_INT
wi::extract_uhwi (const T &x, unsigned int bitpos, unsigned int width)
{
  unsigned precision = get_precision (x);
  if (precision < bitpos + width)
    precision = bitpos + width;
  WIDE_INT_REF_FOR (T) xi (x, precision);

  /* Handle this rare case after the above, so that we assert about
     bogus BITPOS values.  */
  if (width == 0)
    return 0;

  unsigned int start = bitpos / HOST_BITS_PER_WIDE_INT;
  unsigned int shift = bitpos % HOST_BITS_PER_WIDE_INT;
  unsigned HOST_WIDE_INT res = xi.elt (start);
  res >>= shift;
  if (shift + width > HOST_BITS_PER_WIDE_INT)
    {
      unsigned HOST_WIDE_INT upper = xi.elt (start + 1);
      res |= upper << (-shift % HOST_BITS_PER_WIDE_INT);
    }
  return zext_hwi (res, width);
}

/* Return the minimum precision needed to store X with sign SGN.  */
template <typename T>
inline unsigned int
wi::min_precision (const T &x, signop sgn)
{
  if (sgn == SIGNED)
    return get_precision (x) - clrsb (x);
  else
    return get_precision (x) - clz (x);
}

#define SIGNED_BINARY_PREDICATE(OP, F)			\
  template <typename T1, typename T2>			\
    inline WI_SIGNED_BINARY_PREDICATE_RESULT (T1, T2)	\
    OP (const T1 &x, const T2 &y)			\
    {							\
      return wi::F (x, y);				\
    }

SIGNED_BINARY_PREDICATE (operator <, lts_p)
SIGNED_BINARY_PREDICATE (operator <=, les_p)
SIGNED_BINARY_PREDICATE (operator >, gts_p)
SIGNED_BINARY_PREDICATE (operator >=, ges_p)

#undef SIGNED_BINARY_PREDICATE

#define UNARY_OPERATOR(OP, F) \
  template<typename T> \
  WI_UNARY_RESULT (generic_wide_int<T>) \
  OP (const generic_wide_int<T> &x) \
  { \
    return wi::F (x); \
  }

#define BINARY_PREDICATE(OP, F) \
  template<typename T1, typename T2> \
  WI_BINARY_PREDICATE_RESULT (T1, T2) \
  OP (const T1 &x, const T2 &y) \
  { \
    return wi::F (x, y); \
  }

#define BINARY_OPERATOR(OP, F) \
  template<typename T1, typename T2> \
  WI_BINARY_OPERATOR_RESULT (T1, T2) \
  OP (const T1 &x, const T2 &y) \
  { \
    return wi::F (x, y); \
  }

#define SHIFT_OPERATOR(OP, F) \
  template<typename T1, typename T2> \
  WI_BINARY_OPERATOR_RESULT (T1, T1) \
  OP (const T1 &x, const T2 &y) \
  { \
    return wi::F (x, y); \
  }

UNARY_OPERATOR (operator ~, bit_not)
UNARY_OPERATOR (operator -, neg)
BINARY_PREDICATE (operator ==, eq_p)
BINARY_PREDICATE (operator !=, ne_p)
BINARY_OPERATOR (operator &, bit_and)
BINARY_OPERATOR (operator |, bit_or)
BINARY_OPERATOR (operator ^, bit_xor)
BINARY_OPERATOR (operator +, add)
BINARY_OPERATOR (operator -, sub)
BINARY_OPERATOR (operator *, mul)
SHIFT_OPERATOR (operator <<, lshift)

#undef UNARY_OPERATOR
#undef BINARY_PREDICATE
#undef BINARY_OPERATOR
#undef SHIFT_OPERATOR

template <typename T1, typename T2>
inline WI_SIGNED_SHIFT_RESULT (T1, T2)
operator >> (const T1 &x, const T2 &y)
{
  return wi::arshift (x, y);
}

template <typename T1, typename T2>
inline WI_SIGNED_SHIFT_RESULT (T1, T2)
operator / (const T1 &x, const T2 &y)
{
  return wi::sdiv_trunc (x, y);
}

template <typename T1, typename T2>
inline WI_SIGNED_SHIFT_RESULT (T1, T2)
operator % (const T1 &x, const T2 &y)
{
  return wi::smod_trunc (x, y);
}

template<typename T>
void
gt_ggc_mx (generic_wide_int <T> *)
{
}

template<typename T>
void
gt_pch_nx (generic_wide_int <T> *)
{
}

template<typename T>
void
gt_pch_nx (generic_wide_int <T> *, void (*) (void *, void *), void *)
{
}

template<int N>
void
gt_ggc_mx (trailing_wide_ints <N> *)
{
}

template<int N>
void
gt_pch_nx (trailing_wide_ints <N> *)
{
}

template<int N>
void
gt_pch_nx (trailing_wide_ints <N> *, void (*) (void *, void *), void *)
{
}

namespace wi
{
  /* Used for overloaded functions in which the only other acceptable
     scalar type is a pointer.  It stops a plain 0 from being treated
     as a null pointer.  */
  struct never_used1 {};
  struct never_used2 {};

  wide_int min_value (unsigned int, signop);
  wide_int min_value (never_used1 *);
  wide_int min_value (never_used2 *);
  wide_int max_value (unsigned int, signop);
  wide_int max_value (never_used1 *);
  wide_int max_value (never_used2 *);

  /* FIXME: this is target dependent, so should be elsewhere.
     It also seems to assume that CHAR_BIT == BITS_PER_UNIT.  */
  wide_int from_buffer (const unsigned char *, unsigned int);

#ifndef GENERATOR_FILE
  void to_mpz (const wide_int_ref &, mpz_t, signop);
#endif

  wide_int mask (unsigned int, bool, unsigned int);
  wide_int shifted_mask (unsigned int, unsigned int, bool, unsigned int);
  wide_int set_bit_in_zero (unsigned int, unsigned int);
  wide_int insert (const wide_int &x, const wide_int &y, unsigned int,
		   unsigned int);
  wide_int round_down_for_mask (const wide_int &, const wide_int &);
  wide_int round_up_for_mask (const wide_int &, const wide_int &);

  template <typename T>
  T mask (unsigned int, bool);

  template <typename T>
  T shifted_mask (unsigned int, unsigned int, bool);

  template <typename T>
  T set_bit_in_zero (unsigned int);

  unsigned int mask (HOST_WIDE_INT *, unsigned int, bool, unsigned int);
  unsigned int shifted_mask (HOST_WIDE_INT *, unsigned int, unsigned int,
			     bool, unsigned int);
  unsigned int from_array (HOST_WIDE_INT *, const HOST_WIDE_INT *,
			   unsigned int, unsigned int, bool);
}

/* Return a PRECISION-bit integer in which the low WIDTH bits are set
   and the other bits are clear, or the inverse if NEGATE_P.  */
inline wide_int
wi::mask (unsigned int width, bool negate_p, unsigned int precision)
{
  wide_int result = wide_int::create (precision);
  result.set_len (mask (result.write_val (), width, negate_p, precision));
  return result;
}

/* Return a PRECISION-bit integer in which the low START bits are clear,
   the next WIDTH bits are set, and the other bits are clear,
   or the inverse if NEGATE_P.  */
inline wide_int
wi::shifted_mask (unsigned int start, unsigned int width, bool negate_p,
		  unsigned int precision)
{
  wide_int result = wide_int::create (precision);
  result.set_len (shifted_mask (result.write_val (), start, width, negate_p,
				precision));
  return result;
}

/* Return a PRECISION-bit integer in which bit BIT is set and all the
   others are clear.  */
inline wide_int
wi::set_bit_in_zero (unsigned int bit, unsigned int precision)
{
  return shifted_mask (bit, 1, false, precision);
}

/* Return an integer of type T in which the low WIDTH bits are set
   and the other bits are clear, or the inverse if NEGATE_P.  */
template <typename T>
inline T
wi::mask (unsigned int width, bool negate_p)
{
  STATIC_ASSERT (wi::int_traits<T>::precision);
  T result;
  result.set_len (mask (result.write_val (), width, negate_p,
			wi::int_traits <T>::precision));
  return result;
}

/* Return an integer of type T in which the low START bits are clear,
   the next WIDTH bits are set, and the other bits are clear, or the
   inverse if NEGATE_P.  */
template <typename T>
inline T
wi::shifted_mask (unsigned int start, unsigned int width, bool negate_p)
{
  STATIC_ASSERT (wi::int_traits<T>::precision);
  T result;
  result.set_len (shifted_mask (result.write_val (), start, width,
				negate_p,
				wi::int_traits <T>::precision));
  return result;
}

/* Return an integer of type T in which bit BIT is set and all the
   others are clear.  */
template <typename T>
inline T
wi::set_bit_in_zero (unsigned int bit)
{
  return shifted_mask <T> (bit, 1, false);
}

#endif /* WIDE_INT_H */
