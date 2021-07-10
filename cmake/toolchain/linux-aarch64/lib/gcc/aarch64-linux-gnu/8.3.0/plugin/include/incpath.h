/* Set up combined include path for the preprocessor.
   Copyright (C) 2003-2018 Free Software Foundation, Inc.

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

#ifndef GCC_INCPATH_H
#define GCC_INCPATH_H

/* Various fragments of include path.  */
enum incpath_kind {
  INC_QUOTE = 0, /* include "foo" */
  INC_BRACKET,   /* include <foo> */
  INC_SYSTEM,    /* sysinclude */
  INC_AFTER,	/* post-sysinclude.  */
  INC_MAX
};

extern void split_quote_chain (void);
extern void add_path (char *, incpath_kind, int, bool);
extern void register_include_chains (cpp_reader *, const char *,
				     const char *, const char *,
				     int, int, int);
extern void add_cpp_dir_path (struct cpp_dir *, incpath_kind);
extern struct cpp_dir *get_added_cpp_dirs (incpath_kind);

struct target_c_incpath_s {
  /* Do extra includes processing.  STDINC is false iff -nostdinc was given.  */
  void (*extra_pre_includes) (const char *, const char *, int);
  void (*extra_includes) (const char *, const char *, int);
};

extern struct target_c_incpath_s target_c_incpath;

#endif /* GCC_INCPATH_H */
