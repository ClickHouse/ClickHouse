/* bits/xtitypes.h -- Define some types used by <bits/stropts.h>.  Generic.
   Copyright (C) 2002-2018 Free Software Foundation, Inc.
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

#ifndef _STROPTS_H
# error "Never include <bits/xtitypes.h> directly; use <stropts.h> instead."
#endif

#ifndef _BITS_XTITYPES_H
#define _BITS_XTITYPES_H	1

#include <bits/types.h>

/* This type is used by some structs in <bits/stropts.h>.  */
typedef __SLONGWORD_TYPE __t_scalar_t;
typedef __ULONGWORD_TYPE __t_uscalar_t;


#endif /* bits/xtitypes.h */
