/* Tree-based target query functions relating to optabs
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

#ifndef GCC_OPTABS_TREE_H
#define GCC_OPTABS_TREE_H

#include "optabs-query.h"

/* An extra flag to control optab_for_tree_code's behavior.  This is needed to
   distinguish between machines with a vector shift that takes a scalar for the
   shift amount vs. machines that take a vector for the shift amount.  */
enum optab_subtype
{
  optab_default,
  optab_scalar,
  optab_vector
};

/* Return the optab used for computing the given operation on the type given by
   the second argument.  The third argument distinguishes between the types of
   vector shifts and rotates.  */
optab optab_for_tree_code (enum tree_code, const_tree, enum optab_subtype);
bool supportable_convert_operation (enum tree_code, tree, tree, tree *,
				    enum tree_code *);
bool expand_vec_cmp_expr_p (tree, tree, enum tree_code);
bool expand_vec_cond_expr_p (tree, tree, enum tree_code);
void init_tree_optimization_optabs (tree);
bool target_supports_op_p (tree, enum tree_code,
			   enum optab_subtype = optab_default);

#endif
