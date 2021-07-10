/* Generic simplify definitions.

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

#ifndef GCC_GENERIC_MATCH_H
#define GCC_GENERIC_MATCH_H

/* Note the following functions are supposed to be only used from
   fold_unary_loc, fold_binary_loc and fold_ternary_loc respectively.
   They are not considered a public API.  */

tree generic_simplify (location_t, enum tree_code, tree, tree);
tree generic_simplify (location_t, enum tree_code, tree, tree, tree);
tree generic_simplify (location_t, enum tree_code, tree, tree, tree, tree);

#endif  /* GCC_GENERIC_MATCH_H */
