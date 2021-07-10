/* Fixed-point arithmetic support.
   Copyright (C) 2006-2018 Free Software Foundation, Inc.

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

#ifndef GCC_FIXED_VALUE_H
#define GCC_FIXED_VALUE_H

struct GTY(()) fixed_value
{
  double_int data;       /* Store data up to 2 wide integers.  */
  scalar_mode_pod mode;  /* Use machine mode to know IBIT and FBIT.  */
};

#define FIXED_VALUE_TYPE struct fixed_value

#define MAX_FCONST0	18	/* For storing 18 fixed-point zeros per
				   fract, ufract, accum, and uaccum modes .  */
#define MAX_FCONST1	8	/* For storing 8 fixed-point ones per accum
				   and uaccum modes.  */
/* Constant fixed-point values 0 and 1.  */
extern FIXED_VALUE_TYPE fconst0[MAX_FCONST0];
extern FIXED_VALUE_TYPE fconst1[MAX_FCONST1];

/* Macros to access fconst0 and fconst1 via machine modes.  */
#define FCONST0(mode)	fconst0[mode - QQmode]
#define FCONST1(mode)	fconst1[mode - HAmode]

/* Return a CONST_FIXED with value R and mode M.  */
#define CONST_FIXED_FROM_FIXED_VALUE(r, m) \
  const_fixed_from_fixed_value (r, m)
extern rtx const_fixed_from_fixed_value (FIXED_VALUE_TYPE, machine_mode);

/* Construct a FIXED_VALUE from a bit payload and machine mode MODE.
   The bits in PAYLOAD are sign-extended/zero-extended according to MODE.  */
extern FIXED_VALUE_TYPE fixed_from_double_int (double_int, scalar_mode);

/* Return a CONST_FIXED from a bit payload and machine mode MODE.
   The bits in PAYLOAD are sign-extended/zero-extended according to MODE.  */
static inline rtx
const_fixed_from_double_int (double_int payload,
			     scalar_mode mode)
{
  return
    const_fixed_from_fixed_value (fixed_from_double_int (payload, mode),
                                  mode);
}

/* Initialize from a decimal or hexadecimal string.  */
extern void fixed_from_string (FIXED_VALUE_TYPE *, const char *,
			       scalar_mode);

/* In tree.c: wrap up a FIXED_VALUE_TYPE in a tree node.  */
extern tree build_fixed (tree, FIXED_VALUE_TYPE);

/* Extend or truncate to a new mode.  */
extern bool fixed_convert (FIXED_VALUE_TYPE *, scalar_mode,
			   const FIXED_VALUE_TYPE *, bool);

/* Convert to a fixed-point mode from an integer.  */
extern bool fixed_convert_from_int (FIXED_VALUE_TYPE *, scalar_mode,
				    double_int, bool, bool);

/* Convert to a fixed-point mode from a real.  */
extern bool fixed_convert_from_real (FIXED_VALUE_TYPE *, scalar_mode,
				     const REAL_VALUE_TYPE *, bool);

/* Convert to a real mode from a fixed-point.  */
extern void real_convert_from_fixed (REAL_VALUE_TYPE *, scalar_mode,
				     const FIXED_VALUE_TYPE *);

/* Compare two fixed-point objects for bitwise identity.  */
extern bool fixed_identical (const FIXED_VALUE_TYPE *, const FIXED_VALUE_TYPE *);

/* Calculate a hash value.  */
extern unsigned int fixed_hash (const FIXED_VALUE_TYPE *);

#define FIXED_VALUES_IDENTICAL(x, y)	fixed_identical (&(x), &(y))

/* Determine whether a fixed-point value X is negative.  */
#define FIXED_VALUE_NEGATIVE(x)		fixed_isneg (&(x))

/* Render F as a decimal floating point constant.  */
extern void fixed_to_decimal (char *str, const FIXED_VALUE_TYPE *, size_t);

/* Binary or unary arithmetic on tree_code.  */
extern bool fixed_arithmetic (FIXED_VALUE_TYPE *, int, const FIXED_VALUE_TYPE *,
			      const FIXED_VALUE_TYPE *, bool);

/* Compare fixed-point values by tree_code.  */
extern bool fixed_compare (int, const FIXED_VALUE_TYPE *,
			   const FIXED_VALUE_TYPE *);

/* Determine whether a fixed-point value X is negative.  */
extern bool fixed_isneg (const FIXED_VALUE_TYPE *);

#endif /* GCC_FIXED_VALUE_H */
