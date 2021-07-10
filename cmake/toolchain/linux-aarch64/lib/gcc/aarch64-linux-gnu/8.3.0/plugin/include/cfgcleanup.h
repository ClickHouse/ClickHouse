/* Control flow optimization header file.
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


#ifndef GCC_CFGCLEANUP_H
#define GCC_CFGCLEANUP_H

enum replace_direction { dir_none, dir_forward, dir_backward, dir_both };

extern int flow_find_cross_jump (basic_block, basic_block, rtx_insn **,
				 rtx_insn **, enum replace_direction*);
extern int flow_find_head_matching_sequence (basic_block, basic_block,
					     rtx_insn **, rtx_insn **, int);
extern bool delete_unreachable_blocks (void);
extern void delete_dead_jumptables (void);
extern bool cleanup_cfg (int);

#endif /* GCC_CFGCLEANUP_H */
