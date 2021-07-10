/* Declarations of core diagnostic functionality for code that does
   not need to deal with diagnostic contexts or diagnostic info
   structures.
   Copyright (C) 1998-2018 Free Software Foundation, Inc.

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

#ifndef GCC_DIAGNOSTIC_CORE_H
#define GCC_DIAGNOSTIC_CORE_H

#include "bversion.h"

/* Constants used to discriminate diagnostics.  */
typedef enum
{
#define DEFINE_DIAGNOSTIC_KIND(K, msgid, C) K,
#include "diagnostic.def"
#undef DEFINE_DIAGNOSTIC_KIND
  DK_LAST_DIAGNOSTIC_KIND,
  /* This is used for tagging pragma pops in the diagnostic
     classification history chain.  */
  DK_POP
} diagnostic_t;

extern const char *progname;

extern const char *trim_filename (const char *);

/* If we haven't already defined a front-end-specific diagnostics
   style, use the generic one.  */
#ifndef GCC_DIAG_STYLE
#define GCC_DIAG_STYLE __gcc_tdiag__
#endif
/* None of these functions are suitable for ATTRIBUTE_PRINTF, because
   each language front end can extend them with its own set of format
   specifiers.  We must use custom format checks.  */
#if (CHECKING_P && GCC_VERSION >= 4001) || GCC_VERSION == BUILDING_GCC_VERSION
#define ATTRIBUTE_GCC_DIAG(m, n) __attribute__ ((__format__ (GCC_DIAG_STYLE, m, n))) ATTRIBUTE_NONNULL(m)
#else
#define ATTRIBUTE_GCC_DIAG(m, n) ATTRIBUTE_NONNULL(m)
#endif
extern void internal_error (const char *, ...) ATTRIBUTE_GCC_DIAG(1,2)
     ATTRIBUTE_NORETURN;
extern void internal_error_no_backtrace (const char *, ...)
     ATTRIBUTE_GCC_DIAG(1,2) ATTRIBUTE_NORETURN;
/* Pass one of the OPT_W* from options.h as the first parameter.  */
extern bool warning (int, const char *, ...) ATTRIBUTE_GCC_DIAG(2,3);
extern bool warning_n (location_t, int, unsigned HOST_WIDE_INT,
		       const char *, const char *, ...)
    ATTRIBUTE_GCC_DIAG(4,6) ATTRIBUTE_GCC_DIAG(5,6);
extern bool warning_n (rich_location *, int, unsigned HOST_WIDE_INT,
		       const char *, const char *, ...)
    ATTRIBUTE_GCC_DIAG(4, 6) ATTRIBUTE_GCC_DIAG(5, 6);
extern bool warning_at (location_t, int, const char *, ...)
    ATTRIBUTE_GCC_DIAG(3,4);
extern bool warning_at (rich_location *, int, const char *, ...)
    ATTRIBUTE_GCC_DIAG(3,4);
extern void error (const char *, ...) ATTRIBUTE_GCC_DIAG(1,2);
extern void error_n (location_t, unsigned HOST_WIDE_INT, const char *,
		     const char *, ...)
    ATTRIBUTE_GCC_DIAG(3,5) ATTRIBUTE_GCC_DIAG(4,5);
extern void error_at (location_t, const char *, ...) ATTRIBUTE_GCC_DIAG(2,3);
extern void error_at (rich_location *, const char *, ...)
  ATTRIBUTE_GCC_DIAG(2,3);
extern void fatal_error (location_t, const char *, ...) ATTRIBUTE_GCC_DIAG(2,3)
     ATTRIBUTE_NORETURN;
/* Pass one of the OPT_W* from options.h as the second parameter.  */
extern bool pedwarn (location_t, int, const char *, ...)
     ATTRIBUTE_GCC_DIAG(3,4);
extern bool pedwarn (rich_location *, int, const char *, ...)
     ATTRIBUTE_GCC_DIAG(3,4);
extern bool permerror (location_t, const char *, ...) ATTRIBUTE_GCC_DIAG(2,3);
extern bool permerror (rich_location *, const char *,
				   ...) ATTRIBUTE_GCC_DIAG(2,3);
extern void sorry (const char *, ...) ATTRIBUTE_GCC_DIAG(1,2);
extern void inform (location_t, const char *, ...) ATTRIBUTE_GCC_DIAG(2,3);
extern void inform (rich_location *, const char *, ...) ATTRIBUTE_GCC_DIAG(2,3);
extern void inform_n (location_t, unsigned HOST_WIDE_INT, const char *,
		      const char *, ...)
    ATTRIBUTE_GCC_DIAG(3,5) ATTRIBUTE_GCC_DIAG(4,5);
extern void verbatim (const char *, ...) ATTRIBUTE_GCC_DIAG(1,2);
extern bool emit_diagnostic (diagnostic_t, location_t, int,
			     const char *, ...) ATTRIBUTE_GCC_DIAG(4,5);
extern bool emit_diagnostic_valist (diagnostic_t, location_t, int, const char *,
				    va_list *) ATTRIBUTE_GCC_DIAG (4,0);
extern bool seen_error (void);

#ifdef BUFSIZ
  /* N.B. Unlike all the others, fnotice is just gettext+fprintf, and
     therefore it can have ATTRIBUTE_PRINTF.  */
extern void fnotice			(FILE *, const char *, ...)
     ATTRIBUTE_PRINTF_2;
#endif

#endif /* ! GCC_DIAGNOSTIC_CORE_H */
