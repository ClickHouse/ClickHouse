/* sigstack, sigaltstack definitions.
   Copyright (C) 1998-2018 Free Software Foundation, Inc.
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

#ifndef _BITS_SIGSTACK_H
#define _BITS_SIGSTACK_H 1

#if !defined _SIGNAL_H && !defined _SYS_UCONTEXT_H
# error "Never include this file directly.  Use <signal.h> instead"
#endif

/* Minimum stack size for a signal handler.  */
#define MINSIGSTKSZ	2048

/* System default stack size.  */
#define SIGSTKSZ	8192

#endif /* bits/sigstack.h */
