/* Copyright (C) 2002-2018 Free Software Foundation, Inc.

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

#ifndef _SEMAPHORE_H
# error "Never use <bits/semaphore.h> directly; include <semaphore.h> instead."
#endif


#ifdef __ILP32__
# define __SIZEOF_SEM_T	16
#else
# define __SIZEOF_SEM_T	32
#endif


/* Value returned if `sem_open' failed.  */
#define SEM_FAILED      ((sem_t *) 0)


typedef union
{
  char __size[__SIZEOF_SEM_T];
  long long int __align;
} sem_t;
