/* Gimple folding definitions.

   Copyright (C) 2011-2018 Free Software Foundation, Inc.
   Contributed by Richard Guenther <rguenther@suse.de>

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

#ifndef GCC_GIMPLE_FOLD_H
#define GCC_GIMPLE_FOLD_H

extern tree create_tmp_reg_or_ssa_name (tree, gimple *stmt = NULL);
extern tree canonicalize_constructor_val (tree, tree);
extern tree get_symbol_constant_value (tree);
extern bool get_range_strlen (tree, tree[2], bool = false);
extern tree get_maxval_strlen (tree, int);
extern void gimplify_and_update_call_from_tree (gimple_stmt_iterator *, tree);
extern bool fold_stmt (gimple_stmt_iterator *);
extern bool fold_stmt (gimple_stmt_iterator *, tree (*) (tree));
extern bool fold_stmt_inplace (gimple_stmt_iterator *);
extern tree maybe_fold_and_comparisons (enum tree_code, tree, tree, 
					enum tree_code, tree, tree);
extern tree maybe_fold_or_comparisons (enum tree_code, tree, tree,
				       enum tree_code, tree, tree);
extern bool optimize_atomic_compare_exchange_p (gimple *);
extern void fold_builtin_atomic_compare_exchange (gimple_stmt_iterator *);
extern bool arith_overflowed_p (enum tree_code, const_tree, const_tree,
				const_tree);
extern tree no_follow_ssa_edges (tree);
extern tree follow_single_use_edges (tree);
extern tree gimple_fold_stmt_to_constant_1 (gimple *, tree (*) (tree),
					    tree (*) (tree) = no_follow_ssa_edges);
extern tree gimple_fold_stmt_to_constant (gimple *, tree (*) (tree));
extern tree fold_ctor_reference (tree, tree, poly_uint64, poly_uint64, tree);
extern tree fold_const_aggregate_ref_1 (tree, tree (*) (tree));
extern tree fold_const_aggregate_ref (tree);
extern tree gimple_get_virt_method_for_binfo (HOST_WIDE_INT, tree,
					      bool *can_refer = NULL);
extern tree gimple_get_virt_method_for_vtable (HOST_WIDE_INT, tree,
					       unsigned HOST_WIDE_INT,
					       bool *can_refer = NULL);
extern tree gimple_fold_indirect_ref (tree);
extern bool gimple_fold_builtin_sprintf (gimple_stmt_iterator *);
extern bool gimple_fold_builtin_snprintf (gimple_stmt_iterator *);
extern bool arith_code_with_undefined_signed_overflow (tree_code);
extern gimple_seq rewrite_to_defined_overflow (gimple *);
extern void replace_call_with_value (gimple_stmt_iterator *, tree);

/* gimple_build, functionally matching fold_buildN, outputs stmts
   int the provided sequence, matching and simplifying them on-the-fly.
   Supposed to replace force_gimple_operand (fold_buildN (...), ...).  */
extern tree gimple_build (gimple_seq *, location_t,
			  enum tree_code, tree, tree);
inline tree
gimple_build (gimple_seq *seq,
	      enum tree_code code, tree type, tree op0)
{
  return gimple_build (seq, UNKNOWN_LOCATION, code, type, op0);
}
extern tree gimple_build (gimple_seq *, location_t,
			  enum tree_code, tree, tree, tree);
inline tree
gimple_build (gimple_seq *seq,
	      enum tree_code code, tree type, tree op0, tree op1)
{
  return gimple_build (seq, UNKNOWN_LOCATION, code, type, op0, op1);
}
extern tree gimple_build (gimple_seq *, location_t,
			  enum tree_code, tree, tree, tree, tree);
inline tree
gimple_build (gimple_seq *seq,
	      enum tree_code code, tree type, tree op0, tree op1, tree op2)
{
  return gimple_build (seq, UNKNOWN_LOCATION, code, type, op0, op1, op2);
}
extern tree gimple_build (gimple_seq *, location_t,
			  enum built_in_function, tree, tree);
inline tree
gimple_build (gimple_seq *seq,
	      enum built_in_function fn, tree type, tree arg0)
{
  return gimple_build (seq, UNKNOWN_LOCATION, fn, type, arg0);
}
extern tree gimple_build (gimple_seq *, location_t,
			  enum built_in_function, tree, tree, tree);
inline tree
gimple_build (gimple_seq *seq,
	      enum built_in_function fn, tree type, tree arg0, tree arg1)
{
  return gimple_build (seq, UNKNOWN_LOCATION, fn, type, arg0, arg1);
}
extern tree gimple_build (gimple_seq *, location_t,
			  enum built_in_function, tree, tree, tree, tree);
inline tree
gimple_build (gimple_seq *seq,
	      enum built_in_function fn, tree type,
	      tree arg0, tree arg1, tree arg2)
{
  return gimple_build (seq, UNKNOWN_LOCATION, fn, type, arg0, arg1, arg2);
}

extern tree gimple_convert (gimple_seq *, location_t, tree, tree);
inline tree
gimple_convert (gimple_seq *seq, tree type, tree op)
{
  return gimple_convert (seq, UNKNOWN_LOCATION, type, op);
}

extern tree gimple_convert_to_ptrofftype (gimple_seq *, location_t, tree);
inline tree
gimple_convert_to_ptrofftype (gimple_seq *seq, tree op)
{
  return gimple_convert_to_ptrofftype (seq, UNKNOWN_LOCATION, op);
}

extern tree gimple_build_vector_from_val (gimple_seq *, location_t, tree,
					  tree);
inline tree
gimple_build_vector_from_val (gimple_seq *seq, tree type, tree op)
{
  return gimple_build_vector_from_val (seq, UNKNOWN_LOCATION, type, op);
}

class tree_vector_builder;
extern tree gimple_build_vector (gimple_seq *, location_t,
				 tree_vector_builder *);
inline tree
gimple_build_vector (gimple_seq *seq, tree_vector_builder *builder)
{
  return gimple_build_vector (seq, UNKNOWN_LOCATION, builder);
}

extern bool gimple_stmt_nonnegative_warnv_p (gimple *, bool *, int = 0);
extern bool gimple_stmt_integer_valued_real_p (gimple *, int = 0);

/* In gimple-match.c.  */
extern tree gimple_simplify (enum tree_code, tree, tree,
			     gimple_seq *, tree (*)(tree));
extern tree gimple_simplify (enum tree_code, tree, tree, tree,
			     gimple_seq *, tree (*)(tree));
extern tree gimple_simplify (enum tree_code, tree, tree, tree, tree,
			     gimple_seq *, tree (*)(tree));
extern tree gimple_simplify (enum built_in_function, tree, tree,
			     gimple_seq *, tree (*)(tree));
extern tree gimple_simplify (enum built_in_function, tree, tree, tree,
			     gimple_seq *, tree (*)(tree));
extern tree gimple_simplify (enum built_in_function, tree, tree, tree, tree,
			     gimple_seq *, tree (*)(tree));

#endif  /* GCC_GIMPLE_FOLD_H */
