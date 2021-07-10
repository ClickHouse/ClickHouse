/* Define __GLIBC_FLT_EVAL_METHOD.
   Copyright (C) 2016-2018 Free Software Foundation, Inc.
   This file is part of the GNU C Library.

   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the GNU C Library; if not, see
   <http://www.gnu.org/licenses/>.  */

#ifndef _MATH_H
# error "Never use <bits/flt-eval-method.h> directly; include <math.h> instead."
#endif

/* __GLIBC_FLT_EVAL_METHOD is the value of FLT_EVAL_METHOD used to
   determine the evaluation method typedefs such as float_t and
   double_t.  It must be a value from C11 or TS 18661-3:2015, and not
   -1.  */

/* In the default version of this header, follow __FLT_EVAL_METHOD__.
   -1 is mapped to 2 (considering evaluation as long double to be a
   conservatively safe assumption), and if __FLT_EVAL_METHOD__ is not
   defined then assume there is no excess precision and use the value
   0.  */

#ifdef __FLT_EVAL_METHOD__
# if __FLT_EVAL_METHOD__ == -1
#  define __GLIBC_FLT_EVAL_METHOD	2
# else
#  define __GLIBC_FLT_EVAL_METHOD	__FLT_EVAL_METHOD__
# endif
#else
# define __GLIBC_FLT_EVAL_METHOD	0
#endif
