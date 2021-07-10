/* Basic error reporting routines.
   Copyright (C) 1999-2018 Free Software Foundation, Inc.

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

/* warning, error, and fatal.  These definitions are suitable for use
   in the generator programs; eventually we would like to use them in
   cc1 too, but that's a longer term project.

   N.B. We cannot presently use ATTRIBUTE_PRINTF with these functions,
   because they can be extended with additional format specifiers which
   GCC does not know about.  */

#ifndef GCC_ERRORS_H
#define GCC_ERRORS_H

extern void warning (const char *, ...) ATTRIBUTE_PRINTF_1 ATTRIBUTE_COLD;
extern void error (const char *, ...) ATTRIBUTE_PRINTF_1 ATTRIBUTE_COLD;
extern void fatal (const char *, ...) ATTRIBUTE_NORETURN ATTRIBUTE_PRINTF_1 ATTRIBUTE_COLD;
extern void internal_error (const char *, ...) ATTRIBUTE_NORETURN ATTRIBUTE_PRINTF_1 ATTRIBUTE_COLD;
extern const char *trim_filename (const char *);

extern int have_error;
extern const char *progname;

#endif /* ! GCC_ERRORS_H */
