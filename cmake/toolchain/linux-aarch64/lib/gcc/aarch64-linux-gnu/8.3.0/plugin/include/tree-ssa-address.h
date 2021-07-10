/* Header file for memory address lowering and mode selection.
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

#ifndef GCC_TREE_SSA_ADDRESS_H
#define GCC_TREE_SSA_ADDRESS_H

/* Description of a memory address.  */

struct mem_address
{
  tree symbol, base, index, step, offset;
};

extern rtx addr_for_mem_ref (struct mem_address *, addr_space_t, bool);
extern rtx addr_for_mem_ref (tree exp, addr_space_t as, bool really_expand);
extern void get_address_description (tree, struct mem_address *);
extern tree tree_mem_ref_addr (tree, tree);
extern bool valid_mem_ref_p (machine_mode, addr_space_t, struct mem_address *);
extern void move_fixed_address_to_symbol (struct mem_address *,
					  struct aff_tree *);
tree create_mem_ref (gimple_stmt_iterator *, tree,
		     struct aff_tree *, tree, tree, tree, bool);
extern void copy_ref_info (tree, tree);
tree maybe_fold_tmr (tree);

#endif /* GCC_TREE_SSA_ADDRESS_H */
