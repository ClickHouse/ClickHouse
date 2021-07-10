/* CPP Library.
   Copyright (C) 1986-2018 Free Software Foundation, Inc.
   Contributed by Per Bothner, 1994-95.
   Based on CCCP program by Paul Rubin, June 1986
   Adapted to ANSI C, Richard Stallman, Jan 1987

   This program is free software; you can redistribute it and/or modify it
   under the terms of the GNU General Public License as published by the
   Free Software Foundation; either version 3, or (at your option) any
   later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; see the file COPYING3.  If not see
   <http://www.gnu.org/licenses/>.  */

#ifndef GCC_CPPDEFAULT_H
#define GCC_CPPDEFAULT_H

/* This is the default list of directories to search for include files.
   It may be overridden by the various -I and -ixxx options.

   #include "file" looks in the same directory as the current file,
   then this list.
   #include <file> just looks in this list.

   All these directories are treated as `system' include directories
   (they are not subject to pedantic warnings in some cases).  */

struct default_include
{
  const char *const fname;	/* The name of the directory.  */
  const char *const component;	/* The component containing the directory
				   (see update_path in prefix.c) */
  const char cplusplus;		/* Only look here if we're compiling C++.  */
  const char cxx_aware;		/* Includes in this directory don't need to
				   be wrapped in extern "C" when compiling
				   C++.  */
  const char add_sysroot;	/* FNAME should be prefixed by
				   cpp_SYSROOT.  */
  const char multilib;		/* FNAME should have appended
				   - the multilib path specified with -imultilib
				     when set to 1,
				   - the multiarch path specified with
				     -imultiarch, when set to 2.  */
};

extern const struct default_include cpp_include_defaults[];
extern const char cpp_GCC_INCLUDE_DIR[];
extern const size_t cpp_GCC_INCLUDE_DIR_len;

/* The configure-time prefix, i.e., the value supplied as the argument
   to --prefix=.  */
extern const char cpp_PREFIX[];
/* The length of the configure-time prefix.  */
extern const size_t cpp_PREFIX_len;
/* The configure-time execution prefix.  This is typically the lib/gcc
   subdirectory of cpp_PREFIX.  */
extern const char cpp_EXEC_PREFIX[];
/* The run-time execution prefix.  This is typically the lib/gcc
   subdirectory of the actual installation.  */
extern const char *gcc_exec_prefix;

/* Return true if the toolchain is relocated.  */
bool cpp_relocated (void);

#endif /* ! GCC_CPPDEFAULT_H */
