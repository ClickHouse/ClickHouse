/* Definition of functions in convert.c.
   Copyright (C) 1993-2018 Free Software Foundation, Inc.

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

#ifndef GCC_CONVERT_H
#define GCC_CONVERT_H

extern tree convert_to_integer (tree, tree);
extern tree convert_to_integer_maybe_fold (tree, tree, bool);
extern tree convert_to_pointer (tree, tree);
extern tree convert_to_pointer_maybe_fold (tree, tree, bool);
extern tree convert_to_real (tree, tree);
extern tree convert_to_real_maybe_fold (tree, tree, bool);
extern tree convert_to_fixed (tree, tree);
extern tree convert_to_complex (tree, tree);
extern tree convert_to_complex_maybe_fold (tree, tree, bool);
extern tree convert_to_vector (tree, tree);

extern inline tree convert_to_integer_nofold (tree t, tree x)
{ return convert_to_integer_maybe_fold (t, x, false); }
extern inline tree convert_to_pointer_nofold (tree t, tree x)
{ return convert_to_pointer_maybe_fold (t, x, false); }
extern inline tree convert_to_real_nofold (tree t, tree x)
{ return convert_to_real_maybe_fold (t, x, false); }
extern inline tree convert_to_complex_nofold (tree t, tree x)
{ return convert_to_complex_maybe_fold (t, x, false); }

#endif /* GCC_CONVERT_H */
