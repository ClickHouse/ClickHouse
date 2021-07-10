/* Get common system includes and various definitions and declarations
   based on target macros.
   Copyright (C) 2000-2018 Free Software Foundation, Inc.

This file is part of GCC.

GCC is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free
Software Foundation; either version 3, or (at your option) any later
version.

GCC is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
for more details.

Under Section 7 of GPL version 3, you are granted additional
permissions described in the GCC Runtime Library Exception, version
3.1, as published by the Free Software Foundation.

You should have received a copy of the GNU General Public License and
a copy of the GCC Runtime Library Exception along with this program;
see the files COPYING3 and COPYING.RUNTIME respectively.  If not, see
<http://www.gnu.org/licenses/>.  */

#ifndef GCC_TSYSTEM_H
#define GCC_TSYSTEM_H

/* System headers (e.g. stdio.h, stdlib.h, unistd.h) sometimes
   indirectly include getopt.h.  Our -I flags will cause gcc's gnu
   getopt.h to be included, not the platform's copy.  In the default
   case, gnu getopt.h will provide us with a no-argument prototype
   which will generate -Wstrict-prototypes warnings.  None of the
   target files actually use getopt, so it is safe to tell gnu
   getopt.h we never need this prototype.  */
#ifndef HAVE_DECL_GETOPT
#define HAVE_DECL_GETOPT 1
#endif

/* We want everything from the glibc headers.  */
#define _GNU_SOURCE 1

/* GCC supplies these headers.  */
#include <stddef.h>
#include <float.h>

#ifdef inhibit_libc

#ifndef malloc
extern void *malloc (size_t);
#endif

#ifndef free
extern void free (void *);
#endif

#ifndef atexit
extern int atexit (void (*)(void));
#endif

#ifndef abort
extern void abort (void) __attribute__ ((__noreturn__));
#endif

#ifndef strlen
extern size_t strlen (const char *);
#endif

#ifndef memcpy
extern void *memcpy (void *, const void *, size_t);
#endif

#ifndef memset
extern void *memset (void *, int, size_t);
#endif

#else /* ! inhibit_libc */
/* We disable this when inhibit_libc, so that gcc can still be built without
   needing header files first.  */
/* ??? This is not a good solution, since prototypes may be required in
   some cases for correct code.  */

/* GCC supplies this header.  */
#include <stdarg.h>

/* All systems have this header.  */
#include <stdio.h>

/* All systems have this header.  */
#include <sys/types.h>

/* All systems have this header.  */
#include <errno.h>

#ifndef errno
extern int errno;
#endif

/* If these system headers do not exist, fixincludes must create them.  */
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

/* GCC supplies this header.  */
#include <limits.h>

/* If these system headers do not exist, fixincludes must create them.  */
#include <time.h>

#endif /* inhibit_libc */

/* Define a generic NULL if one hasn't already been defined.  */
#ifndef NULL
#define NULL 0
#endif

/* GCC always provides __builtin_alloca(x).  */
#undef alloca
#define alloca(x) __builtin_alloca(x)

#ifdef ENABLE_RUNTIME_CHECKING
#define gcc_assert(EXPR) ((void)(!(EXPR) ? abort (), 0 : 0))
#else
/* Include EXPR, so that unused variable warnings do not occur.  */
#define gcc_assert(EXPR) ((void)(0 && (EXPR)))
#endif
/* Use gcc_unreachable() to mark unreachable locations (like an
   unreachable default case of a switch.  Do not use gcc_assert(0).  */
#define gcc_unreachable() (abort ())

#define CONST_CAST2(TOTYPE,FROMTYPE,X) ((__extension__(union {FROMTYPE _q; TOTYPE _nq;})(X))._nq)
#define CONST_CAST(TYPE,X) CONST_CAST2 (TYPE, const TYPE, (X))

/* Filename handling macros.  */
#include "filenames.h"

#endif /* ! GCC_TSYSTEM_H */
