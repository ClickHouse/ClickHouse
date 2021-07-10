/* Global common subexpression elimination/Partial redundancy elimination
   and global constant/copy propagation for GNU compiler.
   Copyright (C) 1997-2018 Free Software Foundation, Inc.

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

#ifndef GCC_GCSE_H
#define GCC_GCSE_H

/* Target-dependent globals.  */
struct target_gcse {
  /* Nonzero for each mode that supports (set (reg) (reg)).
     This is trivially true for integer and floating point values.
     It may or may not be true for condition codes.  */
  char x_can_copy[(int) NUM_MACHINE_MODES];

  /* True if the previous field has been initialized.  */
  bool x_can_copy_init_p;
};

extern struct target_gcse default_target_gcse;
#if SWITCHABLE_TARGET
extern struct target_gcse *this_target_gcse;
#else
#define this_target_gcse (&default_target_gcse)
#endif

void gcse_c_finalize (void);
extern bool gcse_or_cprop_is_too_expensive (const char *);

#endif
