/* Declarations and definitons for stringpool.c.
   Copyright (C) 2013-2018 Free Software Foundation, Inc.

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

#ifndef GCC_STRINGPOOL_H
#define GCC_STRINGPOOL_H

/* Return the (unique) IDENTIFIER_NODE node for a given name.
   The name is supplied as a char *.  */
extern tree get_identifier (const char *);

/* If an identifier with the name TEXT (a null-terminated string) has
   previously been referred to, return that node; otherwise return
   NULL_TREE.  */
extern tree maybe_get_identifier (const char *);

/* Identical to get_identifier, except that the length is assumed
   known.  */
extern tree get_identifier_with_length (const char *, size_t);

#if GCC_VERSION >= 3000
#define get_identifier(str) \
  (__builtin_constant_p (str)				\
    ? get_identifier_with_length ((str), strlen (str))  \
    : get_identifier (str))
#endif

#endif  // GCC_STRINGPOOL_H
