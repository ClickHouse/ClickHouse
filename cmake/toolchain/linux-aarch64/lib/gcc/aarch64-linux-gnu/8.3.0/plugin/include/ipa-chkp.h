/* Declaration of interface functions of Pointer Bounds Checker.
   Copyright (C) 2014-2018 Free Software Foundation, Inc.

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

#ifndef GCC_IPA_CHKP_H
#define GCC_IPA_CHKP_H

extern tree chkp_copy_function_type_adding_bounds (tree orig_type);
extern tree chkp_maybe_clone_builtin_fndecl (tree fndecl);
extern cgraph_node *chkp_maybe_create_clone (tree fndecl);
extern bool chkp_instrumentable_p (tree fndecl);
extern bool chkp_wrap_function (tree fndecl);

#endif /* GCC_IPA_CHKP_H */
