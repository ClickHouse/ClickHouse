/* Control flow graph building header file.
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

#ifndef GCC_CFGBUILD_H
#define GCC_CFGBUILD_H

extern bool inside_basic_block_p (const rtx_insn *);
extern bool control_flow_insn_p (const rtx_insn *);
extern void rtl_make_eh_edge (sbitmap, basic_block, rtx);
extern void find_many_sub_basic_blocks (sbitmap);

#endif /* GCC_CFGBUILD_H */
