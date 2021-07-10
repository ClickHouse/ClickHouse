/* HOST_WIDE_INT definitions for the GNU compiler.
   Copyright (C) 1998-2018 Free Software Foundation, Inc.

   This file is part of GCC.

   Provide definitions for macros which depend on HOST_BITS_PER_INT
   and HOST_BITS_PER_LONG.  */

#ifndef GCC_HWINT_H
#define GCC_HWINT_H

/* This describes the machine the compiler is hosted on.  */
#define HOST_BITS_PER_CHAR  CHAR_BIT
#define HOST_BITS_PER_SHORT (CHAR_BIT * SIZEOF_SHORT)
#define HOST_BITS_PER_INT   (CHAR_BIT * SIZEOF_INT)
#define HOST_BITS_PER_LONG  (CHAR_BIT * SIZEOF_LONG)
#define HOST_BITS_PER_PTR   (CHAR_BIT * SIZEOF_VOID_P)

/* The string that should be inserted into a printf style format to
   indicate a "long" operand.  */
#ifndef HOST_LONG_FORMAT
#define HOST_LONG_FORMAT "l"
#endif

/* The string that should be inserted into a printf style format to
   indicate a "long long" operand.  */
#ifndef HOST_LONG_LONG_FORMAT
#define HOST_LONG_LONG_FORMAT "ll"
#endif

/* If HAVE_LONG_LONG and SIZEOF_LONG_LONG aren't defined, but
   GCC_VERSION >= 3000, assume this is the second or later stage of a
   bootstrap, we do have long long, and it's 64 bits.  (This is
   required by C99; we do have some ports that violate that assumption
   but they're all cross-compile-only.)  Just in case, force a
   constraint violation if that assumption is incorrect.  */
#if !defined HAVE_LONG_LONG
# if GCC_VERSION >= 3000
#  define HAVE_LONG_LONG 1
#  define SIZEOF_LONG_LONG 8
extern char sizeof_long_long_must_be_8[sizeof (long long) == 8 ? 1 : -1];
# endif
#endif

#ifdef HAVE_LONG_LONG
# define HOST_BITS_PER_LONGLONG (CHAR_BIT * SIZEOF_LONG_LONG)
#endif

/* Set HOST_WIDE_INT, this should be always 64 bits.
   The underlying type is matched to that of int64_t and assumed
   to be either long or long long.  */

#define HOST_BITS_PER_WIDE_INT 64
#if INT64_T_IS_LONG   
#   define HOST_WIDE_INT long
#   define HOST_WIDE_INT_C(X) X ## L
#else
# if HOST_BITS_PER_LONGLONG == 64
#   define HOST_WIDE_INT long long
#   define HOST_WIDE_INT_C(X) X ## LL
# else
   #error "Unable to find a suitable type for HOST_WIDE_INT"
# endif
#endif

#define HOST_WIDE_INT_UC(X) HOST_WIDE_INT_C (X ## U)
#define HOST_WIDE_INT_0 HOST_WIDE_INT_C (0)
#define HOST_WIDE_INT_0U HOST_WIDE_INT_UC (0)
#define HOST_WIDE_INT_1 HOST_WIDE_INT_C (1)
#define HOST_WIDE_INT_1U HOST_WIDE_INT_UC (1)
#define HOST_WIDE_INT_M1 HOST_WIDE_INT_C (-1)
#define HOST_WIDE_INT_M1U HOST_WIDE_INT_UC (-1)

/* This is a magic identifier which allows GCC to figure out the type
   of HOST_WIDE_INT for %wd specifier checks.  You must issue this
   typedef before using the __asm_fprintf__ format attribute.  */
typedef HOST_WIDE_INT __gcc_host_wide_int__;

/* Provide C99 <inttypes.h> style format definitions for 64bits.  */
#ifndef HAVE_INTTYPES_H
#if INT64_T_IS_LONG
# define GCC_PRI64 HOST_LONG_FORMAT
#else
# define GCC_PRI64 HOST_LONG_LONG_FORMAT
#endif
#undef PRId64
#define PRId64 GCC_PRI64 "d"
#undef PRIi64
#define PRIi64 GCC_PRI64 "i"
#undef PRIo64
#define PRIo64 GCC_PRI64 "o"
#undef PRIu64
#define PRIu64 GCC_PRI64 "u"
#undef PRIx64
#define PRIx64 GCC_PRI64 "x"
#undef PRIX64
#define PRIX64 GCC_PRI64 "X"
#endif

/* Various printf format strings for HOST_WIDE_INT.  */

#if INT64_T_IS_LONG
# define HOST_WIDE_INT_PRINT HOST_LONG_FORMAT
# define HOST_WIDE_INT_PRINT_C "L"
#else
# define HOST_WIDE_INT_PRINT HOST_LONG_LONG_FORMAT
# define HOST_WIDE_INT_PRINT_C "LL"
#endif

#define HOST_WIDE_INT_PRINT_DEC "%" PRId64
#define HOST_WIDE_INT_PRINT_DEC_C "%" PRId64 HOST_WIDE_INT_PRINT_C
#define HOST_WIDE_INT_PRINT_UNSIGNED "%" PRIu64
#define HOST_WIDE_INT_PRINT_HEX "%#" PRIx64
#define HOST_WIDE_INT_PRINT_HEX_PURE "%" PRIx64
#define HOST_WIDE_INT_PRINT_DOUBLE_HEX "0x%" PRIx64 "%016" PRIx64
#define HOST_WIDE_INT_PRINT_PADDED_HEX "%016" PRIx64

/* Define HOST_WIDEST_FAST_INT to the widest integer type supported
   efficiently in hardware.  (That is, the widest integer type that fits
   in a hardware register.)  Normally this is "long" but on some hosts it
   should be "long long" or "__int64".  This is no convenient way to
   autodetect this, so such systems must set a flag in config.host; see there
   for details.  */

#ifdef USE_LONG_LONG_FOR_WIDEST_FAST_INT
#  ifdef HAVE_LONG_LONG
#    define HOST_WIDEST_FAST_INT long long
#    define HOST_BITS_PER_WIDEST_FAST_INT HOST_BITS_PER_LONGLONG
#  else
#    error "Your host said it wanted to use long long but that does not exist"
#  endif
#else
#  define HOST_WIDEST_FAST_INT long
#  define HOST_BITS_PER_WIDEST_FAST_INT HOST_BITS_PER_LONG
#endif

/* Inline functions operating on HOST_WIDE_INT.  */

/* Return X with all but the lowest bit masked off.  */

static inline unsigned HOST_WIDE_INT
least_bit_hwi (unsigned HOST_WIDE_INT x)
{
  return (x & -x);
}

/* True if X is zero or a power of two.  */

static inline bool
pow2_or_zerop (unsigned HOST_WIDE_INT x)
{
  return least_bit_hwi (x) == x;
}

/* True if X is a power of two.  */

static inline bool
pow2p_hwi (unsigned HOST_WIDE_INT x)
{
  return x && pow2_or_zerop (x);
}

#if GCC_VERSION < 3004

extern int clz_hwi (unsigned HOST_WIDE_INT x);
extern int ctz_hwi (unsigned HOST_WIDE_INT x);
extern int ffs_hwi (unsigned HOST_WIDE_INT x);

/* Return the number of set bits in X.  */
extern int popcount_hwi (unsigned HOST_WIDE_INT x);

/* Return log2, or -1 if not exact.  */
extern int exact_log2                  (unsigned HOST_WIDE_INT);

/* Return floor of log2, with -1 for zero.  */
extern int floor_log2                  (unsigned HOST_WIDE_INT);

/* Return the smallest n such that 2**n >= X.  */
extern int ceil_log2			(unsigned HOST_WIDE_INT);

#else /* GCC_VERSION >= 3004 */

/* For convenience, define 0 -> word_size.  */
static inline int
clz_hwi (unsigned HOST_WIDE_INT x)
{
  if (x == 0)
    return HOST_BITS_PER_WIDE_INT;
# if HOST_BITS_PER_WIDE_INT == HOST_BITS_PER_LONG
  return __builtin_clzl (x);
# elif HOST_BITS_PER_WIDE_INT == HOST_BITS_PER_LONGLONG
  return __builtin_clzll (x);
# else
  return __builtin_clz (x);
# endif
}

static inline int
ctz_hwi (unsigned HOST_WIDE_INT x)
{
  if (x == 0)
    return HOST_BITS_PER_WIDE_INT;
# if HOST_BITS_PER_WIDE_INT == HOST_BITS_PER_LONG
  return __builtin_ctzl (x);
# elif HOST_BITS_PER_WIDE_INT == HOST_BITS_PER_LONGLONG
  return __builtin_ctzll (x);
# else
  return __builtin_ctz (x);
# endif
}

static inline int
ffs_hwi (unsigned HOST_WIDE_INT x)
{
# if HOST_BITS_PER_WIDE_INT == HOST_BITS_PER_LONG
  return __builtin_ffsl (x);
# elif HOST_BITS_PER_WIDE_INT == HOST_BITS_PER_LONGLONG
  return __builtin_ffsll (x);
# else
  return __builtin_ffs (x);
# endif
}

static inline int
popcount_hwi (unsigned HOST_WIDE_INT x)
{
# if HOST_BITS_PER_WIDE_INT == HOST_BITS_PER_LONG
  return __builtin_popcountl (x);
# elif HOST_BITS_PER_WIDE_INT == HOST_BITS_PER_LONGLONG
  return __builtin_popcountll (x);
# else
  return __builtin_popcount (x);
# endif
}

static inline int
floor_log2 (unsigned HOST_WIDE_INT x)
{
  return HOST_BITS_PER_WIDE_INT - 1 - clz_hwi (x);
}

static inline int
ceil_log2 (unsigned HOST_WIDE_INT x)
{
  return floor_log2 (x - 1) + 1;
}

static inline int
exact_log2 (unsigned HOST_WIDE_INT x)
{
  return pow2p_hwi (x) ? ctz_hwi (x) : -1;
}

#endif /* GCC_VERSION >= 3004 */

#define HOST_WIDE_INT_MIN (HOST_WIDE_INT) \
  (HOST_WIDE_INT_1U << (HOST_BITS_PER_WIDE_INT - 1))
#define HOST_WIDE_INT_MAX (~(HOST_WIDE_INT_MIN))

extern HOST_WIDE_INT abs_hwi (HOST_WIDE_INT);
extern unsigned HOST_WIDE_INT absu_hwi (HOST_WIDE_INT);
extern HOST_WIDE_INT gcd (HOST_WIDE_INT, HOST_WIDE_INT);
extern HOST_WIDE_INT pos_mul_hwi (HOST_WIDE_INT, HOST_WIDE_INT);
extern HOST_WIDE_INT mul_hwi (HOST_WIDE_INT, HOST_WIDE_INT);
extern HOST_WIDE_INT least_common_multiple (HOST_WIDE_INT, HOST_WIDE_INT);

/* Like ctz_hwi, except 0 when x == 0.  */

static inline int
ctz_or_zero (unsigned HOST_WIDE_INT x)
{
  return ffs_hwi (x) - 1;
}

/* Sign extend SRC starting from PREC.  */

static inline HOST_WIDE_INT
sext_hwi (HOST_WIDE_INT src, unsigned int prec)
{
  if (prec == HOST_BITS_PER_WIDE_INT)
    return src;
  else
#if defined (__GNUC__)
    {
      /* Take the faster path if the implementation-defined bits it's relying
	 on are implemented the way we expect them to be.  Namely, conversion
	 from unsigned to signed preserves bit pattern, and right shift of
	 a signed value propagates the sign bit.
	 We have to convert from signed to unsigned and back, because when left
	 shifting signed values, any overflow is undefined behavior.  */
      gcc_checking_assert (prec < HOST_BITS_PER_WIDE_INT);
      int shift = HOST_BITS_PER_WIDE_INT - prec;
      return ((HOST_WIDE_INT) ((unsigned HOST_WIDE_INT) src << shift)) >> shift;
    }
#else
    {
      /* Fall back to the slower, well defined path otherwise.  */
      gcc_checking_assert (prec < HOST_BITS_PER_WIDE_INT);
      HOST_WIDE_INT sign_mask = HOST_WIDE_INT_1 << (prec - 1);
      HOST_WIDE_INT value_mask = (HOST_WIDE_INT_1U << prec) - HOST_WIDE_INT_1U;
      return (((src & value_mask) ^ sign_mask) - sign_mask);
    }
#endif
}

/* Zero extend SRC starting from PREC.  */
static inline unsigned HOST_WIDE_INT
zext_hwi (unsigned HOST_WIDE_INT src, unsigned int prec)
{
  if (prec == HOST_BITS_PER_WIDE_INT)
    return src;
  else
    {
      gcc_checking_assert (prec < HOST_BITS_PER_WIDE_INT);
      return src & ((HOST_WIDE_INT_1U << prec) - 1);
    }
}

/* Compute the absolute value of X.  */

inline HOST_WIDE_INT
abs_hwi (HOST_WIDE_INT x)
{
  gcc_checking_assert (x != HOST_WIDE_INT_MIN);
  return x >= 0 ? x : -x;
}

/* Compute the absolute value of X as an unsigned type.  */

inline unsigned HOST_WIDE_INT
absu_hwi (HOST_WIDE_INT x)
{
  return x >= 0 ? (unsigned HOST_WIDE_INT)x : -(unsigned HOST_WIDE_INT)x;
}

#endif /* ! GCC_HWINT_H */
