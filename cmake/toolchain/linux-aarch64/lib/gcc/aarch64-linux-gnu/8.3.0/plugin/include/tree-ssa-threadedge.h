/* Header file for SSA jump threading.
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

#ifndef GCC_TREE_SSA_THREADEDGE_H
#define GCC_TREE_SSA_THREADEDGE_H

extern vec<tree> ssa_name_values;
#define SSA_NAME_VALUE(x) \
    (SSA_NAME_VERSION (x) < ssa_name_values.length () \
     ? ssa_name_values[SSA_NAME_VERSION (x)] \
     : NULL_TREE)
extern void set_ssa_name_value (tree, tree);
extern void threadedge_initialize_values (void);
extern void threadedge_finalize_values (void);
extern bool potentially_threadable_block (basic_block);
extern void propagate_threaded_block_debug_into (basic_block, basic_block);
class evrp_range_analyzer;
extern void thread_outgoing_edges (basic_block, gcond *,
				   const_and_copies *,
				   avail_exprs_stack *,
				   evrp_range_analyzer *,
				   tree (*) (gimple *, gimple *,
					     avail_exprs_stack *, basic_block));

#endif /* GCC_TREE_SSA_THREADEDGE_H */
