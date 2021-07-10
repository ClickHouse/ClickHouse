/* Definition of __INDIRECT_RETURN.  Generic version.
   Copyright (C) 2018 Free Software Foundation, Inc.
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

#ifndef _UCONTEXT_H
# error "Never include <bits/indirect-return.h> directly; use <ucontext.h> instead."
#endif

/* __INDIRECT_RETURN is used on swapcontext to indicate if it requires
   special compiler treatment.  */
#define __INDIRECT_RETURN
