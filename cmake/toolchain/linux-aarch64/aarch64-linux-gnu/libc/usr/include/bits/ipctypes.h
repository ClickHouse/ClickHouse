/* bits/ipctypes.h -- Define some types used by SysV IPC/MSG/SHM.  Generic.
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

/*
 * Never include <bits/ipctypes.h> directly.
 */

#ifndef _BITS_IPCTYPES_H
#define _BITS_IPCTYPES_H	1

#include <bits/types.h>

/* Used in `struct shmid_ds'.  */
# if __WORDSIZE == 32
typedef unsigned short int __ipc_pid_t;
# else
typedef int __ipc_pid_t;
# endif


#endif /* bits/ipctypes.h */
