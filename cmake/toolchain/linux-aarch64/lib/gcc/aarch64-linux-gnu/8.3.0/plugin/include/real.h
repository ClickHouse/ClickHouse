/* Definitions of floating-point access for GNU compiler.
   Copyright (C) 1989-2018 Free Software Foundation, Inc.

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

#ifndef GCC_REAL_H
#define GCC_REAL_H

/* An expanded form of the represented number.  */

/* Enumerate the special cases of numbers that we encounter.  */
enum real_value_class {
  rvc_zero,
  rvc_normal,
  rvc_inf,
  rvc_nan
};

#define SIGNIFICAND_BITS	(128 + HOST_BITS_PER_LONG)
#define EXP_BITS		(32 - 6)
#define MAX_EXP			((1 << (EXP_BITS - 1)) - 1)
#define SIGSZ			(SIGNIFICAND_BITS / HOST_BITS_PER_LONG)
#define SIG_MSB			((unsigned long)1 << (HOST_BITS_PER_LONG - 1))

struct GTY(()) real_value {
  /* Use the same underlying type for all bit-fields, so as to make
     sure they're packed together, otherwise REAL_VALUE_TYPE_SIZE will
     be miscomputed.  */
  unsigned int /* ENUM_BITFIELD (real_value_class) */ cl : 2;
  unsigned int decimal : 1;
  unsigned int sign : 1;
  unsigned int signalling : 1;
  unsigned int canonical : 1;
  unsigned int uexp : EXP_BITS;
  unsigned long sig[SIGSZ];
};

#define REAL_EXP(REAL) \
  ((int)((REAL)->uexp ^ (unsigned int)(1 << (EXP_BITS - 1))) \
   - (1 << (EXP_BITS - 1)))
#define SET_REAL_EXP(REAL, EXP) \
  ((REAL)->uexp = ((unsigned int)(EXP) & (unsigned int)((1 << EXP_BITS) - 1)))

/* Various headers condition prototypes on #ifdef REAL_VALUE_TYPE, so it
   needs to be a macro.  We do need to continue to have a structure tag
   so that other headers can forward declare it.  */
#define REAL_VALUE_TYPE struct real_value

/* We store a REAL_VALUE_TYPE into an rtx, and we do this by putting it in
   consecutive "w" slots.  Moreover, we've got to compute the number of "w"
   slots at preprocessor time, which means we can't use sizeof.  Guess.  */

#define REAL_VALUE_TYPE_SIZE (SIGNIFICAND_BITS + 32)
#define REAL_WIDTH \
  (REAL_VALUE_TYPE_SIZE/HOST_BITS_PER_WIDE_INT \
   + (REAL_VALUE_TYPE_SIZE%HOST_BITS_PER_WIDE_INT ? 1 : 0)) /* round up */

/* Verify the guess.  */
extern char test_real_width
  [sizeof (REAL_VALUE_TYPE) <= REAL_WIDTH * sizeof (HOST_WIDE_INT) ? 1 : -1];

/* Calculate the format for CONST_DOUBLE.  We need as many slots as
   are necessary to overlay a REAL_VALUE_TYPE on them.  This could be
   as many as four (32-bit HOST_WIDE_INT, 128-bit REAL_VALUE_TYPE).

   A number of places assume that there are always at least two 'w'
   slots in a CONST_DOUBLE, so we provide them even if one would suffice.  */

#if REAL_WIDTH == 1
# define CONST_DOUBLE_FORMAT	 "ww"
#else
# if REAL_WIDTH == 2
#  define CONST_DOUBLE_FORMAT	 "ww"
# else
#  if REAL_WIDTH == 3
#   define CONST_DOUBLE_FORMAT	 "www"
#  else
#   if REAL_WIDTH == 4
#    define CONST_DOUBLE_FORMAT	 "wwww"
#   else
#    if REAL_WIDTH == 5
#     define CONST_DOUBLE_FORMAT "wwwww"
#    else
#     if REAL_WIDTH == 6
#      define CONST_DOUBLE_FORMAT "wwwwww"
#     else
       #error "REAL_WIDTH > 6 not supported"
#     endif
#    endif
#   endif
#  endif
# endif
#endif


/* Describes the properties of the specific target format in use.  */
struct real_format
{
  /* Move to and from the target bytes.  */
  void (*encode) (const struct real_format *, long *,
		  const REAL_VALUE_TYPE *);
  void (*decode) (const struct real_format *, REAL_VALUE_TYPE *,
		  const long *);

  /* The radix of the exponent and digits of the significand.  */
  int b;

  /* Size of the significand in digits of radix B.  */
  int p;

  /* Size of the significant of a NaN, in digits of radix B.  */
  int pnan;

  /* The minimum negative integer, x, such that b**(x-1) is normalized.  */
  int emin;

  /* The maximum integer, x, such that b**(x-1) is representable.  */
  int emax;

  /* The bit position of the sign bit, for determining whether a value
     is positive/negative, or -1 for a complex encoding.  */
  int signbit_ro;

  /* The bit position of the sign bit, for changing the sign of a number,
     or -1 for a complex encoding.  */
  int signbit_rw;

  /* If this is an IEEE interchange format, the number of bits in the
     format; otherwise, if it is an IEEE extended format, one more
     than the greatest number of bits in an interchange format it
     extends; otherwise 0.  Formats need not follow the IEEE 754-2008
     recommended practice regarding how signaling NaNs are identified,
     and may vary in the choice of default NaN, but must follow other
     IEEE practice regarding having NaNs, infinities and subnormal
     values, and the relation of minimum and maximum exponents, and,
     for interchange formats, the details of the encoding.  */
  int ieee_bits;

  /* Default rounding mode for operations on this format.  */
  bool round_towards_zero;
  bool has_sign_dependent_rounding;

  /* Properties of the format.  */
  bool has_nans;
  bool has_inf;
  bool has_denorm;
  bool has_signed_zero;
  bool qnan_msb_set;
  bool canonical_nan_lsbs_set;
  const char *name;
};


/* The target format used for each floating point mode.
   Float modes are followed by decimal float modes, with entries for
   float modes indexed by (MODE - first float mode), and entries for
   decimal float modes indexed by (MODE - first decimal float mode) +
   the number of float modes.  */
extern const struct real_format *
  real_format_for_mode[MAX_MODE_FLOAT - MIN_MODE_FLOAT + 1
		       + MAX_MODE_DECIMAL_FLOAT - MIN_MODE_DECIMAL_FLOAT + 1];

#define REAL_MODE_FORMAT(MODE)						\
  (real_format_for_mode[DECIMAL_FLOAT_MODE_P (MODE)			\
			? (((MODE) - MIN_MODE_DECIMAL_FLOAT)		\
			   + (MAX_MODE_FLOAT - MIN_MODE_FLOAT + 1))	\
			: GET_MODE_CLASS (MODE) == MODE_FLOAT		\
			? ((MODE) - MIN_MODE_FLOAT)			\
			: (gcc_unreachable (), 0)])

#define FLOAT_MODE_FORMAT(MODE) \
  (REAL_MODE_FORMAT (as_a <scalar_float_mode> (GET_MODE_INNER (MODE))))

/* The following macro determines whether the floating point format is
   composite, i.e. may contain non-consecutive mantissa bits, in which
   case compile-time FP overflow may not model run-time overflow.  */
#define MODE_COMPOSITE_P(MODE) \
  (FLOAT_MODE_P (MODE) \
   && FLOAT_MODE_FORMAT (MODE)->pnan < FLOAT_MODE_FORMAT (MODE)->p)

/* Accessor macros for format properties.  */
#define MODE_HAS_NANS(MODE) \
  (FLOAT_MODE_P (MODE) && FLOAT_MODE_FORMAT (MODE)->has_nans)
#define MODE_HAS_INFINITIES(MODE) \
  (FLOAT_MODE_P (MODE) && FLOAT_MODE_FORMAT (MODE)->has_inf)
#define MODE_HAS_SIGNED_ZEROS(MODE) \
  (FLOAT_MODE_P (MODE) && FLOAT_MODE_FORMAT (MODE)->has_signed_zero)
#define MODE_HAS_SIGN_DEPENDENT_ROUNDING(MODE) \
  (FLOAT_MODE_P (MODE) \
   && FLOAT_MODE_FORMAT (MODE)->has_sign_dependent_rounding)

/* This class allows functions in this file to accept a floating-point
   format as either a mode or an explicit real_format pointer.  In the
   former case the mode must be VOIDmode (which means "no particular
   format") or must satisfy SCALAR_FLOAT_MODE_P.  */
class format_helper
{
public:
  format_helper (const real_format *format) : m_format (format) {}
  template<typename T> format_helper (const T &);
  const real_format *operator-> () const { return m_format; }
  operator const real_format *() const { return m_format; }

  bool decimal_p () const { return m_format && m_format->b == 10; }

private:
  const real_format *m_format;
};

template<typename T>
inline format_helper::format_helper (const T &m)
  : m_format (m == VOIDmode ? 0 : REAL_MODE_FORMAT (m))
{}

/* Declare functions in real.c.  */

/* True if the given mode has a NaN representation and the treatment of
   NaN operands is important.  Certain optimizations, such as folding
   x * 0 into 0, are not correct for NaN operands, and are normally
   disabled for modes with NaNs.  The user can ask for them to be
   done anyway using the -funsafe-math-optimizations switch.  */
extern bool HONOR_NANS (machine_mode);
extern bool HONOR_NANS (const_tree);
extern bool HONOR_NANS (const_rtx);

/* Like HONOR_NANs, but true if we honor signaling NaNs (or sNaNs).  */
extern bool HONOR_SNANS (machine_mode);
extern bool HONOR_SNANS (const_tree);
extern bool HONOR_SNANS (const_rtx);

/* As for HONOR_NANS, but true if the mode can represent infinity and
   the treatment of infinite values is important.  */
extern bool HONOR_INFINITIES (machine_mode);
extern bool HONOR_INFINITIES (const_tree);
extern bool HONOR_INFINITIES (const_rtx);

/* Like HONOR_NANS, but true if the given mode distinguishes between
   positive and negative zero, and the sign of zero is important.  */
extern bool HONOR_SIGNED_ZEROS (machine_mode);
extern bool HONOR_SIGNED_ZEROS (const_tree);
extern bool HONOR_SIGNED_ZEROS (const_rtx);

/* Like HONOR_NANS, but true if given mode supports sign-dependent rounding,
   and the rounding mode is important.  */
extern bool HONOR_SIGN_DEPENDENT_ROUNDING (machine_mode);
extern bool HONOR_SIGN_DEPENDENT_ROUNDING (const_tree);
extern bool HONOR_SIGN_DEPENDENT_ROUNDING (const_rtx);

/* Binary or unary arithmetic on tree_code.  */
extern bool real_arithmetic (REAL_VALUE_TYPE *, int, const REAL_VALUE_TYPE *,
			     const REAL_VALUE_TYPE *);

/* Compare reals by tree_code.  */
extern bool real_compare (int, const REAL_VALUE_TYPE *, const REAL_VALUE_TYPE *);

/* Determine whether a floating-point value X is infinite.  */
extern bool real_isinf (const REAL_VALUE_TYPE *);

/* Determine whether a floating-point value X is a NaN.  */
extern bool real_isnan (const REAL_VALUE_TYPE *);

/* Determine whether a floating-point value X is a signaling NaN.  */
extern bool real_issignaling_nan (const REAL_VALUE_TYPE *);

/* Determine whether a floating-point value X is finite.  */
extern bool real_isfinite (const REAL_VALUE_TYPE *);

/* Determine whether a floating-point value X is negative.  */
extern bool real_isneg (const REAL_VALUE_TYPE *);

/* Determine whether a floating-point value X is minus zero.  */
extern bool real_isnegzero (const REAL_VALUE_TYPE *);

/* Test relationships between reals.  */
extern bool real_identical (const REAL_VALUE_TYPE *, const REAL_VALUE_TYPE *);
extern bool real_equal (const REAL_VALUE_TYPE *, const REAL_VALUE_TYPE *);
extern bool real_less (const REAL_VALUE_TYPE *, const REAL_VALUE_TYPE *);

/* Extend or truncate to a new format.  */
extern void real_convert (REAL_VALUE_TYPE *, format_helper,
			  const REAL_VALUE_TYPE *);

/* Return true if truncating to NEW is exact.  */
extern bool exact_real_truncate (format_helper, const REAL_VALUE_TYPE *);

/* Render R as a decimal floating point constant.  */
extern void real_to_decimal (char *, const REAL_VALUE_TYPE *, size_t,
			     size_t, int);

/* Render R as a decimal floating point constant, rounded so as to be
   parsed back to the same value when interpreted in mode MODE.  */
extern void real_to_decimal_for_mode (char *, const REAL_VALUE_TYPE *, size_t,
				      size_t, int, machine_mode);

/* Render R as a hexadecimal floating point constant.  */
extern void real_to_hexadecimal (char *, const REAL_VALUE_TYPE *,
				 size_t, size_t, int);

/* Render R as an integer.  */
extern HOST_WIDE_INT real_to_integer (const REAL_VALUE_TYPE *);

/* Initialize R from a decimal or hexadecimal string.  Return -1 if
   the value underflows, +1 if overflows, and 0 otherwise.  */
extern int real_from_string (REAL_VALUE_TYPE *, const char *);
/* Wrapper to allow different internal representation for decimal floats. */
extern void real_from_string3 (REAL_VALUE_TYPE *, const char *, format_helper);

extern long real_to_target (long *, const REAL_VALUE_TYPE *, format_helper);

extern void real_from_target (REAL_VALUE_TYPE *, const long *,
			      format_helper);

extern void real_inf (REAL_VALUE_TYPE *);

extern bool real_nan (REAL_VALUE_TYPE *, const char *, int, format_helper);

extern void real_maxval (REAL_VALUE_TYPE *, int, machine_mode);

extern void real_2expN (REAL_VALUE_TYPE *, int, format_helper);

extern unsigned int real_hash (const REAL_VALUE_TYPE *);


/* Target formats defined in real.c.  */
extern const struct real_format ieee_single_format;
extern const struct real_format mips_single_format;
extern const struct real_format motorola_single_format;
extern const struct real_format spu_single_format;
extern const struct real_format ieee_double_format;
extern const struct real_format mips_double_format;
extern const struct real_format motorola_double_format;
extern const struct real_format ieee_extended_motorola_format;
extern const struct real_format ieee_extended_intel_96_format;
extern const struct real_format ieee_extended_intel_96_round_53_format;
extern const struct real_format ieee_extended_intel_128_format;
extern const struct real_format ibm_extended_format;
extern const struct real_format mips_extended_format;
extern const struct real_format ieee_quad_format;
extern const struct real_format mips_quad_format;
extern const struct real_format vax_f_format;
extern const struct real_format vax_d_format;
extern const struct real_format vax_g_format;
extern const struct real_format real_internal_format;
extern const struct real_format decimal_single_format;
extern const struct real_format decimal_double_format;
extern const struct real_format decimal_quad_format;
extern const struct real_format ieee_half_format;
extern const struct real_format arm_half_format;


/* ====================================================================== */
/* Crap.  */

/* Determine whether a floating-point value X is infinite.  */
#define REAL_VALUE_ISINF(x)		real_isinf (&(x))

/* Determine whether a floating-point value X is a NaN.  */
#define REAL_VALUE_ISNAN(x)		real_isnan (&(x))

/* Determine whether a floating-point value X is a signaling NaN.  */ 
#define REAL_VALUE_ISSIGNALING_NAN(x)  real_issignaling_nan (&(x))

/* Determine whether a floating-point value X is negative.  */
#define REAL_VALUE_NEGATIVE(x)		real_isneg (&(x))

/* Determine whether a floating-point value X is minus zero.  */
#define REAL_VALUE_MINUS_ZERO(x)	real_isnegzero (&(x))

/* IN is a REAL_VALUE_TYPE.  OUT is an array of longs.  */
#define REAL_VALUE_TO_TARGET_LONG_DOUBLE(IN, OUT)			\
  real_to_target (OUT, &(IN),						\
		  float_mode_for_size (LONG_DOUBLE_TYPE_SIZE).require ())

#define REAL_VALUE_TO_TARGET_DOUBLE(IN, OUT) \
  real_to_target (OUT, &(IN), float_mode_for_size (64).require ())

/* IN is a REAL_VALUE_TYPE.  OUT is a long.  */
#define REAL_VALUE_TO_TARGET_SINGLE(IN, OUT) \
  ((OUT) = real_to_target (NULL, &(IN), float_mode_for_size (32).require ()))

/* Real values to IEEE 754 decimal floats.  */

/* IN is a REAL_VALUE_TYPE.  OUT is an array of longs.  */
#define REAL_VALUE_TO_TARGET_DECIMAL128(IN, OUT) \
  real_to_target (OUT, &(IN), decimal_float_mode_for_size (128).require ())

#define REAL_VALUE_TO_TARGET_DECIMAL64(IN, OUT) \
  real_to_target (OUT, &(IN), decimal_float_mode_for_size (64).require ())

/* IN is a REAL_VALUE_TYPE.  OUT is a long.  */
#define REAL_VALUE_TO_TARGET_DECIMAL32(IN, OUT) \
  ((OUT) = real_to_target (NULL, &(IN), \
			   decimal_float_mode_for_size (32).require ()))

extern REAL_VALUE_TYPE real_value_truncate (format_helper, REAL_VALUE_TYPE);

extern REAL_VALUE_TYPE real_value_negate (const REAL_VALUE_TYPE *);
extern REAL_VALUE_TYPE real_value_abs (const REAL_VALUE_TYPE *);

extern int significand_size (format_helper);

extern REAL_VALUE_TYPE real_from_string2 (const char *, format_helper);

#define REAL_VALUE_ATOF(s, m) \
  real_from_string2 (s, m)

#define CONST_DOUBLE_ATOF(s, m) \
  const_double_from_real_value (real_from_string2 (s, m), m)

#define REAL_VALUE_FIX(r) \
  real_to_integer (&(r))

/* ??? Not quite right.  */
#define REAL_VALUE_UNSIGNED_FIX(r) \
  real_to_integer (&(r))

/* ??? These were added for Paranoia support.  */

/* Return floor log2(R).  */
extern int real_exponent (const REAL_VALUE_TYPE *);

/* R = A * 2**EXP.  */
extern void real_ldexp (REAL_VALUE_TYPE *, const REAL_VALUE_TYPE *, int);

/* **** End of software floating point emulator interface macros **** */

/* Constant real values 0, 1, 2, -1 and 0.5.  */

extern REAL_VALUE_TYPE dconst0;
extern REAL_VALUE_TYPE dconst1;
extern REAL_VALUE_TYPE dconst2;
extern REAL_VALUE_TYPE dconstm1;
extern REAL_VALUE_TYPE dconsthalf;

#define dconst_e() (*dconst_e_ptr ())
#define dconst_third() (*dconst_third_ptr ())
#define dconst_quarter() (*dconst_quarter_ptr ())
#define dconst_sixth() (*dconst_sixth_ptr ())
#define dconst_ninth() (*dconst_ninth_ptr ())
#define dconst_sqrt2() (*dconst_sqrt2_ptr ())

/* Function to return the real value special constant 'e'.  */
extern const REAL_VALUE_TYPE * dconst_e_ptr (void);

/* Returns a cached REAL_VALUE_TYPE corresponding to 1/n, for various n.  */
extern const REAL_VALUE_TYPE *dconst_third_ptr (void);
extern const REAL_VALUE_TYPE *dconst_quarter_ptr (void);
extern const REAL_VALUE_TYPE *dconst_sixth_ptr (void);
extern const REAL_VALUE_TYPE *dconst_ninth_ptr (void);

/* Returns the special REAL_VALUE_TYPE corresponding to sqrt(2).  */
extern const REAL_VALUE_TYPE * dconst_sqrt2_ptr (void);

/* Function to return a real value (not a tree node)
   from a given integer constant.  */
REAL_VALUE_TYPE real_value_from_int_cst (const_tree, const_tree);

/* Return a CONST_DOUBLE with value R and mode M.  */
extern rtx const_double_from_real_value (REAL_VALUE_TYPE, machine_mode);

/* Replace R by 1/R in the given format, if the result is exact.  */
extern bool exact_real_inverse (format_helper, REAL_VALUE_TYPE *);

/* Return true if arithmetic on values in IMODE that were promoted
   from values in TMODE is equivalent to direct arithmetic on values
   in TMODE.  */
bool real_can_shorten_arithmetic (machine_mode, machine_mode);

/* In tree.c: wrap up a REAL_VALUE_TYPE in a tree node.  */
extern tree build_real (tree, REAL_VALUE_TYPE);

/* Likewise, but first truncate the value to the type.  */
extern tree build_real_truncate (tree, REAL_VALUE_TYPE);

/* Calculate R as X raised to the integer exponent N in format FMT.  */
extern bool real_powi (REAL_VALUE_TYPE *, format_helper,
		       const REAL_VALUE_TYPE *, HOST_WIDE_INT);

/* Standard round to integer value functions.  */
extern void real_trunc (REAL_VALUE_TYPE *, format_helper,
			const REAL_VALUE_TYPE *);
extern void real_floor (REAL_VALUE_TYPE *, format_helper,
			const REAL_VALUE_TYPE *);
extern void real_ceil (REAL_VALUE_TYPE *, format_helper,
		       const REAL_VALUE_TYPE *);
extern void real_round (REAL_VALUE_TYPE *, format_helper,
			const REAL_VALUE_TYPE *);

/* Set the sign of R to the sign of X.  */
extern void real_copysign (REAL_VALUE_TYPE *, const REAL_VALUE_TYPE *);

/* Check whether the real constant value given is an integer.  */
extern bool real_isinteger (const REAL_VALUE_TYPE *, format_helper);
extern bool real_isinteger (const REAL_VALUE_TYPE *, HOST_WIDE_INT *);

/* Write into BUF the maximum representable finite floating-point
   number, (1 - b**-p) * b**emax for a given FP format FMT as a hex
   float string.  BUF must be large enough to contain the result.  */
extern void get_max_float (const struct real_format *, char *, size_t);

#ifndef GENERATOR_FILE
/* real related routines.  */
extern wide_int real_to_integer (const REAL_VALUE_TYPE *, bool *, int);
extern void real_from_integer (REAL_VALUE_TYPE *, format_helper,
			       const wide_int_ref &, signop);
#endif

#endif /* ! GCC_REAL_H */
