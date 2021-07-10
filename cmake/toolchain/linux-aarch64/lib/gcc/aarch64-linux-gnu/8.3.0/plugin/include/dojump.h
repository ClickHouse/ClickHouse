/* Export function prototypes from dojump.c.
   Copyright (C) 2015-2018 Free Software Foundation, Inc.

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

#ifndef GCC_DOJUMP_H
#define GCC_DOJUMP_H

/* At the start of a function, record that we have no previously-pushed
   arguments waiting to be popped.  */
extern void init_pending_stack_adjust (void);

/* Discard any pending stack adjustment.  */
extern void discard_pending_stack_adjust (void);

/* When exiting from function, if safe, clear out any pending stack adjust
   so the adjustment won't get done.  */
extern void clear_pending_stack_adjust (void);

/* Pop any previously-pushed arguments that have not been popped yet.  */
extern void do_pending_stack_adjust (void);

/* Struct for saving/restoring of pending_stack_adjust/stack_pointer_delta
   values.  */

struct saved_pending_stack_adjust
{
  /* Saved value of pending_stack_adjust.  */
  poly_int64 x_pending_stack_adjust;

  /* Saved value of stack_pointer_delta.  */
  poly_int64 x_stack_pointer_delta;
};

/* Remember pending_stack_adjust/stack_pointer_delta.
   To be used around code that may call do_pending_stack_adjust (),
   but the generated code could be discarded e.g. using delete_insns_since.  */

extern void save_pending_stack_adjust (saved_pending_stack_adjust *);

/* Restore the saved pending_stack_adjust/stack_pointer_delta.  */

extern void restore_pending_stack_adjust (saved_pending_stack_adjust *);

/* Generate code to evaluate EXP and jump to LABEL if the value is zero.  */
extern void jumpifnot (tree exp, rtx_code_label *label,
		       profile_probability prob);
extern void jumpifnot_1 (enum tree_code, tree, tree, rtx_code_label *,
			 profile_probability);

/* Generate code to evaluate EXP and jump to LABEL if the value is nonzero.  */
extern void jumpif (tree exp, rtx_code_label *label, profile_probability prob);
extern void jumpif_1 (enum tree_code, tree, tree, rtx_code_label *,
		      profile_probability);

/* Generate code to evaluate EXP and jump to IF_FALSE_LABEL if
   the result is zero, or IF_TRUE_LABEL if the result is one.  */
extern void do_jump (tree exp, rtx_code_label *if_false_label,
		     rtx_code_label *if_true_label, profile_probability prob);
extern void do_jump_1 (enum tree_code, tree, tree, rtx_code_label *,
		       rtx_code_label *, profile_probability);

extern void do_compare_rtx_and_jump (rtx, rtx, enum rtx_code, int,
				     machine_mode, rtx, rtx_code_label *,
				     rtx_code_label *, profile_probability);

extern bool split_comparison (enum rtx_code, machine_mode,
			      enum rtx_code *, enum rtx_code *);

#endif /* GCC_DOJUMP_H */
