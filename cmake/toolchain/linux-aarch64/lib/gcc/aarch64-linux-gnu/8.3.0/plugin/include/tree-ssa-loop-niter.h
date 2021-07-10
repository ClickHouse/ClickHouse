/* Header file for loop interation estimates.
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

#ifndef GCC_TREE_SSA_LOOP_NITER_H
#define GCC_TREE_SSA_LOOP_NITER_H

extern tree expand_simple_operations (tree, tree = NULL);
extern tree simplify_using_initial_conditions (struct loop *, tree);
extern bool loop_only_exit_p (const struct loop *, const_edge);
extern bool number_of_iterations_exit (struct loop *, edge,
				       struct tree_niter_desc *niter, bool,
				       bool every_iteration = true);
extern bool number_of_iterations_exit_assumptions (struct loop *, edge,
						   struct tree_niter_desc *,
						   gcond **, bool = true);
extern tree find_loop_niter (struct loop *, edge *);
extern bool finite_loop_p (struct loop *);
extern tree loop_niter_by_eval (struct loop *, edge);
extern tree find_loop_niter_by_eval (struct loop *, edge *);
extern bool estimated_loop_iterations (struct loop *, widest_int *);
extern HOST_WIDE_INT estimated_loop_iterations_int (struct loop *);
extern bool max_loop_iterations (struct loop *, widest_int *);
extern HOST_WIDE_INT max_loop_iterations_int (struct loop *);
extern bool likely_max_loop_iterations (struct loop *, widest_int *);
extern HOST_WIDE_INT likely_max_loop_iterations_int (struct loop *);
extern HOST_WIDE_INT max_stmt_executions_int (struct loop *);
extern HOST_WIDE_INT likely_max_stmt_executions_int (struct loop *);
extern HOST_WIDE_INT estimated_stmt_executions_int (struct loop *);
extern bool max_stmt_executions (struct loop *, widest_int *);
extern bool likely_max_stmt_executions (struct loop *, widest_int *);
extern bool estimated_stmt_executions (struct loop *, widest_int *);
extern void estimate_numbers_of_iterations (function *);
extern void estimate_numbers_of_iterations (struct loop *);
extern bool stmt_dominates_stmt_p (gimple *, gimple *);
extern bool nowrap_type_p (tree);
extern bool scev_probably_wraps_p (tree, tree, tree, gimple *,
				   struct loop *, bool);
extern void free_numbers_of_iterations_estimates (struct loop *);
extern void free_numbers_of_iterations_estimates (function *);
extern void substitute_in_loop_info (struct loop *, tree, tree);

#endif /* GCC_TREE_SSA_LOOP_NITER_H */
