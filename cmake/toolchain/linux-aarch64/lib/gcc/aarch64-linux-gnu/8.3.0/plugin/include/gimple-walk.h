/* Header file for gimple statement walk support.
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

#ifndef GCC_GIMPLE_WALK_H
#define GCC_GIMPLE_WALK_H

/* Convenience routines to walk all statements of a gimple function.
   Note that this is useful exclusively before the code is converted
   into SSA form.  Once the program is in SSA form, the standard
   operand interface should be used to analyze/modify statements.  */
struct walk_stmt_info
{
  /* Points to the current statement being walked.  */
  gimple_stmt_iterator gsi;
  gimple *stmt;

  /* Additional data that the callback functions may want to carry
     through the recursion.  */
  void *info;

  /* Pointer map used to mark visited tree nodes when calling
     walk_tree on each operand.  If set to NULL, duplicate tree nodes
     will be visited more than once.  */
  hash_set<tree> *pset;

  /* Operand returned by the callbacks.  This is set when calling
     walk_gimple_seq.  If the walk_stmt_fn or walk_tree_fn callback
     returns non-NULL, this field will contain the tree returned by
     the last callback.  */
  tree callback_result;

  /* Indicates whether the operand being examined may be replaced
     with something that matches is_gimple_val (if true) or something
     slightly more complicated (if false).  "Something" technically
     means the common subset of is_gimple_lvalue and is_gimple_rhs,
     but we never try to form anything more complicated than that, so
     we don't bother checking.

     Also note that CALLBACK should update this flag while walking the
     sub-expressions of a statement.  For instance, when walking the
     statement 'foo (&var)', the flag VAL_ONLY will initially be set
     to true, however, when walking &var, the operand of that
     ADDR_EXPR does not need to be a GIMPLE value.  */
  BOOL_BITFIELD val_only : 1;

  /* True if we are currently walking the LHS of an assignment.  */
  BOOL_BITFIELD is_lhs : 1;

  /* Optional.  Set to true by the callback functions if they made any
     changes.  */
  BOOL_BITFIELD changed : 1;

  /* True if we're interested in location information.  */
  BOOL_BITFIELD want_locations : 1;

  /* True if we've removed the statement that was processed.  */
  BOOL_BITFIELD removed_stmt : 1;
};

/* Callback for walk_gimple_stmt.  Called for every statement found
   during traversal.  The first argument points to the statement to
   walk.  The second argument is a flag that the callback sets to
   'true' if it the callback handled all the operands and
   sub-statements of the statement (the default value of this flag is
   'false').  The third argument is an anonymous pointer to data
   to be used by the callback.  */
typedef tree (*walk_stmt_fn) (gimple_stmt_iterator *, bool *,
			      struct walk_stmt_info *);

extern gimple *walk_gimple_seq_mod (gimple_seq *, walk_stmt_fn, walk_tree_fn,
				   struct walk_stmt_info *);
extern gimple *walk_gimple_seq (gimple_seq, walk_stmt_fn, walk_tree_fn,
			       struct walk_stmt_info *);
extern tree walk_gimple_op (gimple *, walk_tree_fn, struct walk_stmt_info *);
extern tree walk_gimple_stmt (gimple_stmt_iterator *, walk_stmt_fn,
			      walk_tree_fn, struct walk_stmt_info *);
typedef bool (*walk_stmt_load_store_addr_fn) (gimple *, tree, tree, void *);
extern bool walk_stmt_load_store_addr_ops (gimple *, void *,
					   walk_stmt_load_store_addr_fn,
					   walk_stmt_load_store_addr_fn,
					   walk_stmt_load_store_addr_fn);
extern bool walk_stmt_load_store_ops (gimple *, void *,
				      walk_stmt_load_store_addr_fn,
				      walk_stmt_load_store_addr_fn);
#endif /* GCC_GIMPLE_WALK_H */
