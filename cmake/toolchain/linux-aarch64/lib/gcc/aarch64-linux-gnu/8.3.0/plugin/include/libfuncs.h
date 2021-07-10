/* Definitions for code generation pass of GNU compiler.
   Copyright (C) 2001-2018 Free Software Foundation, Inc.

This file is part of GCC.

GCC is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option)
any later version.

GCC is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with GCC; see the file COPYING3.  If not see
<http://www.gnu.org/licenses/>.  */

#ifndef GCC_LIBFUNCS_H
#define GCC_LIBFUNCS_H


/* Enumeration of indexes into libfunc_table.  */
enum libfunc_index
{
  LTI_unwind_sjlj_register,
  LTI_unwind_sjlj_unregister,
  LTI_synchronize,
  LTI_MAX
};

/* Information about an optab-related libfunc.  The op field is logically
   an enum optab_d, and the mode fields are logically machine_mode.
   However, in the absence of forward-declared enums, there's no practical
   benefit of pulling in the defining headers.

   We use the same hashtable for normal optabs and conversion optabs.  In
   the first case mode2 is forced to VOIDmode.  */

struct GTY((for_user)) libfunc_entry {
  int op, mode1, mode2;
  rtx libfunc;
};

/* Descriptor for libfunc_entry.  */

struct libfunc_hasher : ggc_ptr_hash<libfunc_entry>
{
  static hashval_t hash (libfunc_entry *);
  static bool equal (libfunc_entry *, libfunc_entry *);
};

/* Target-dependent globals.  */
struct GTY(()) target_libfuncs {
  /* SYMBOL_REF rtx's for the library functions that are called
     implicitly and not via optabs.  */
  rtx x_libfunc_table[LTI_MAX];

  /* Hash table used to convert declarations into nodes.  */
  hash_table<libfunc_hasher> *GTY(()) x_libfunc_hash;
};

extern GTY(()) struct target_libfuncs default_target_libfuncs;
#if SWITCHABLE_TARGET
extern struct target_libfuncs *this_target_libfuncs;
#else
#define this_target_libfuncs (&default_target_libfuncs)
#endif

#define libfunc_table \
  (this_target_libfuncs->x_libfunc_table)

/* Accessor macros for libfunc_table.  */

#define unwind_sjlj_register_libfunc (libfunc_table[LTI_unwind_sjlj_register])
#define unwind_sjlj_unregister_libfunc \
  (libfunc_table[LTI_unwind_sjlj_unregister])
#define synchronize_libfunc	(libfunc_table[LTI_synchronize])

/* In explow.c */
extern void set_stack_check_libfunc (const char *);

#endif /* GCC_LIBFUNCS_H */
