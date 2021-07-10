/* Fold calls to built-in and internal functions with constant arguments.
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

#ifndef GCC_FOLD_CONST_CALL_H
#define GCC_FOLD_CONST_CALL_H

tree fold_const_call (combined_fn, tree, tree);
tree fold_const_call (combined_fn, tree, tree, tree);
tree fold_const_call (combined_fn, tree, tree, tree, tree);
tree fold_fma (location_t, tree, tree, tree, tree);
tree build_cmp_result (tree type, int res);

#endif
