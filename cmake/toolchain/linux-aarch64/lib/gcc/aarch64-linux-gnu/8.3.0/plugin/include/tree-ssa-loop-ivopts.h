/* Header file for Induction variable optimizations.
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

#ifndef GCC_TREE_SSA_LOOP_IVOPTS_H
#define GCC_TREE_SSA_LOOP_IVOPTS_H

extern edge single_dom_exit (struct loop *);
extern void dump_iv (FILE *, struct iv *);
extern void dump_use (FILE *, struct iv_use *);
extern void dump_uses (FILE *, struct ivopts_data *);
extern void dump_cand (FILE *, struct iv_cand *);
extern bool contains_abnormal_ssa_name_p (tree);
extern struct loop *outermost_invariant_loop_for_expr (struct loop *, tree);
extern bool expr_invariant_in_loop_p (struct loop *, tree);
extern tree strip_offset (tree, poly_uint64_pod *);
bool may_be_nonaddressable_p (tree expr);
void tree_ssa_iv_optimize (void);

void create_canonical_iv (struct loop *, edge, tree,
			  tree * = NULL, tree * = NULL);
#endif /* GCC_TREE_SSA_LOOP_IVOPTS_H */
