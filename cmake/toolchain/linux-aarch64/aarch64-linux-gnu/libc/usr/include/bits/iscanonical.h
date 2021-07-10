/* Define iscanonical macro.
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
# error "Never use <bits/iscanonical.h> directly; include <math.h> instead."
#endif

/* Return nonzero value if X is canonical.  By default, we only have
   IEEE interchange binary formats, in which all values are canonical,
   but the argument must still be converted to its semantic type for
   any exceptions arising from the conversion, before being
   discarded.  */
#define iscanonical(x) ((void) (__typeof (x)) (x), 1)
