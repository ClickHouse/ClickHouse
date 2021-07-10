/* Define builtin-in macros for all front ends that perform preprocessing
   Copyright (C) 2010-2018 Free Software Foundation, Inc.

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

#ifndef GCC_CPPBUILTIN_H
#define GCC_CPPBUILTIN_H

/* Parse a BASEVER version string of the format "major.minor.patchlevel"
   or "major.minor" to extract its components.  */
extern void parse_basever (int *, int *, int *);

/* Define macros builtins common to all language performing CPP
   preprocessing.  */
extern void define_language_independent_builtin_macros (cpp_reader *);


#endif /* ! GCC_CPPBUILTIN_H */

