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

#ifndef GCC_RTL_CHKP_H
#define GCC_RTL_CHKP_H

#define DECL_BOUNDS_RTL(NODE) (chkp_get_rtl_bounds (DECL_WRTL_CHECK (NODE)))

#define SET_DECL_BOUNDS_RTL(NODE, VAL) \
  (chkp_set_rtl_bounds (DECL_WRTL_CHECK (NODE), VAL))

extern rtx chkp_get_rtl_bounds (tree node);
extern void chkp_set_rtl_bounds (tree node, rtx val);
extern void chkp_reset_rtl_bounds ();
extern void chkp_split_slot (rtx slot, rtx *slot_val, rtx *slot_bnd);
extern rtx chkp_join_splitted_slot (rtx val, rtx bnd);
extern rtx chkp_get_value_with_offs (rtx par, rtx offs);
extern void chkp_copy_bounds_for_stack_parm (rtx slot, rtx value, tree type);
extern void chkp_emit_bounds_store (rtx bounds, rtx value, rtx mem);
extern void chkp_put_regs_to_expr_list (rtx par);

#endif /* GCC_RTL_CHKP_H */
