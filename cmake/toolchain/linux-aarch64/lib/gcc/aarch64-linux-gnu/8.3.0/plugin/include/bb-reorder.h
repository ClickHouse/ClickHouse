/* Basic block reordering routines for the GNU compiler.
   Copyright (C) 2000-2018 Free Software Foundation, Inc.

   This file is part of GCC.

   GCC is free software; you can redistribute it and/or modify it
   under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 3, or (at your option)
   any later version.

   GCC is distributed in the hope that it will be useful, but WITHOUT
   ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
   or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
   License for more details.

   You should have received a copy of the GNU General Public License
   along with GCC; see the file COPYING3.  If not see
   <http://www.gnu.org/licenses/>.  */

#ifndef GCC_BB_REORDER
#define GCC_BB_REORDER

/* Target-specific globals.  */
struct target_bb_reorder {
  /* Length of unconditional jump instruction.  */
  int x_uncond_jump_length;
};

extern struct target_bb_reorder default_target_bb_reorder;
#if SWITCHABLE_TARGET
extern struct target_bb_reorder *this_target_bb_reorder;
#else
#define this_target_bb_reorder (&default_target_bb_reorder)
#endif

extern int get_uncond_jump_length (void);

extern void insert_section_boundary_note (void);

#endif
