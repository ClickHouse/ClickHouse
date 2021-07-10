/* Decimal floating point support functions for GNU compiler.
   Copyright (C) 2005-2018 Free Software Foundation, Inc.

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

#ifndef GCC_DFP_H
#define GCC_DFP_H

/* Encode REAL_VALUE_TYPEs into 32/64/128-bit IEEE 754 encoded values.  */
void encode_decimal32  (const struct real_format *fmt, long *, const REAL_VALUE_TYPE *);
void encode_decimal64  (const struct real_format *fmt, long *, const REAL_VALUE_TYPE *);
void decode_decimal128 (const struct real_format *, REAL_VALUE_TYPE *, const long *);

/* Decode 32/64/128-bit IEEE 754 encoded values into REAL_VALUE_TYPEs.  */
void decode_decimal32  (const struct real_format *, REAL_VALUE_TYPE *, const long *);
void decode_decimal64  (const struct real_format *, REAL_VALUE_TYPE *, const long *);
void encode_decimal128 (const struct real_format *fmt, long *, const REAL_VALUE_TYPE *);

/* Arithmetic and conversion functions.  */
int  decimal_do_compare (const REAL_VALUE_TYPE *, const REAL_VALUE_TYPE *, int);
void decimal_real_from_string (REAL_VALUE_TYPE *, const char *);
void decimal_round_for_format (const struct real_format *, REAL_VALUE_TYPE *);
void decimal_real_convert (REAL_VALUE_TYPE *, const real_format *,
			   const REAL_VALUE_TYPE *);
void decimal_real_to_decimal (char *, const REAL_VALUE_TYPE *, size_t, size_t, int);
void decimal_do_fix_trunc (REAL_VALUE_TYPE *, const REAL_VALUE_TYPE *);
void decimal_real_maxval (REAL_VALUE_TYPE *, int, machine_mode);
wide_int decimal_real_to_integer (const REAL_VALUE_TYPE *, bool *, int);
HOST_WIDE_INT decimal_real_to_integer (const REAL_VALUE_TYPE *);

#ifdef TREE_CODE
bool decimal_real_arithmetic (REAL_VALUE_TYPE *, enum tree_code, const REAL_VALUE_TYPE *,
			      const REAL_VALUE_TYPE *);
#endif

#endif /* GCC_DFP_H */
