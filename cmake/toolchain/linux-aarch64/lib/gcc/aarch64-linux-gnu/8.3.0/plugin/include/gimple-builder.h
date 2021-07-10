/* Header file for high level statement building routines.
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


#ifndef GCC_GIMPLE_BUILDER_H
#define GCC_GIMPLE_BUILDER_H

gassign *build_assign (enum tree_code, tree, int, tree lhs = NULL_TREE);
gassign *build_assign (enum tree_code, gimple *, int, tree lhs = NULL_TREE);
gassign *build_assign (enum tree_code, tree, tree, tree lhs = NULL_TREE);
gassign *build_assign (enum tree_code, gimple *, tree, tree lhs = NULL_TREE);
gassign *build_assign (enum tree_code, tree, gimple *, tree lhs = NULL_TREE);
gassign *build_assign (enum tree_code, gimple *, gimple *,
		       tree lhs = NULL_TREE);
gassign *build_type_cast (tree, tree, tree lhs = NULL_TREE);
gassign *build_type_cast (tree, gimple *, tree lhs = NULL_TREE);

#endif /* GCC_GIMPLE_BUILDER_H */
