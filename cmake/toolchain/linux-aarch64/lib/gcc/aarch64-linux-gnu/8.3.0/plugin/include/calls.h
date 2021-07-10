/* Declarations and data types for RTL call insn generation.
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

#ifndef GCC_CALLS_H
#define GCC_CALLS_H

extern int flags_from_decl_or_type (const_tree);
extern int call_expr_flags (const_tree);
extern int setjmp_call_p (const_tree);
extern bool gimple_maybe_alloca_call_p (const gimple *);
extern bool gimple_alloca_call_p (const gimple *);
extern bool alloca_call_p (const_tree);
extern bool must_pass_in_stack_var_size (machine_mode, const_tree);
extern bool must_pass_in_stack_var_size_or_pad (machine_mode, const_tree);
extern rtx prepare_call_address (tree, rtx, rtx, rtx *, int, int);
extern bool shift_return_value (machine_mode, bool, rtx);
extern rtx expand_call (tree, rtx, int);
extern void fixup_tail_calls (void);

extern bool pass_by_reference (CUMULATIVE_ARGS *, machine_mode,
			       tree, bool);
extern bool reference_callee_copied (CUMULATIVE_ARGS *, machine_mode,
				     tree, bool);
extern void maybe_warn_alloc_args_overflow (tree, tree, tree[2], int[2]);
extern tree get_attr_nonstring_decl (tree, tree * = NULL);
extern void maybe_warn_nonstring_arg (tree, tree);
extern bool get_size_range (tree, tree[2], bool = false);
extern rtx rtx_for_static_chain (const_tree, bool);

#endif // GCC_CALLS_H
