/* Declarations for tree-object-size.c.
   Copyright (C) 2013-2018 Free Software Foundation, Inc.

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

#ifndef GCC_TREE_OBJECT_SIZE_H
#define GCC_TREE_OBJECT_SIZE_H

extern void init_object_sizes (void);
extern void fini_object_sizes (void);
extern bool compute_builtin_object_size (tree, int, unsigned HOST_WIDE_INT *);

#endif  // GCC_TREE_OBJECT_SIZE_H
