/* Header file for middle end gimplification.
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

#ifndef GCC_GIMPLIFY_ME_H
#define GCC_GIMPLIFY_ME_H

/* Validation of GIMPLE expressions.  Note that these predicates only check
 *    the basic form of the expression, they don't recurse to make sure that
 *       underlying nodes are also of the right form.  */
typedef bool (*gimple_predicate)(tree);

extern tree force_gimple_operand_1 (tree, gimple_seq *, gimple_predicate, tree);
extern tree force_gimple_operand (tree, gimple_seq *, bool, tree);
extern tree force_gimple_operand_gsi_1 (gimple_stmt_iterator *, tree,
					gimple_predicate, tree,
					bool, enum gsi_iterator_update);
extern tree force_gimple_operand_gsi (gimple_stmt_iterator *, tree, bool, tree,
				      bool, enum gsi_iterator_update);
extern void gimple_regimplify_operands (gimple *, gimple_stmt_iterator *);

#endif /* GCC_GIMPLIFY_ME_H */
