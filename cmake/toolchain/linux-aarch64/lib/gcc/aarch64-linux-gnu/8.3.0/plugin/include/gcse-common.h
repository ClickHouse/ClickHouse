/* Structures and prototypes common across the normal GCSE
   implementation and the post-reload implementation.
   Copyright (C) 1997-2018 Free Software Foundation, Inc.

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

#ifndef GCC_GCSE_COMMON_H
#define GCC_GCSE_COMMON_H

typedef vec<rtx_insn *> vec_rtx_heap;
struct modify_pair
{
  rtx dest;                     /* A MEM.  */
  rtx dest_addr;                /* The canonical address of `dest'.  */
};

typedef vec<modify_pair> vec_modify_pair_heap;

struct gcse_note_stores_info
{
  rtx_insn *insn;
  vec<modify_pair> *canon_mem_list;
};

extern void compute_transp (const_rtx, int, sbitmap *, bitmap,
			    bitmap, vec<modify_pair> *);
extern void record_last_mem_set_info_common (rtx_insn *,
					     vec<rtx_insn *> *,
					     vec<modify_pair> *,
					     bitmap, bitmap);


#endif
