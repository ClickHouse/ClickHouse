/* Header file for PHI node routines
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

#ifndef GCC_TREE_PHINODES_H
#define GCC_TREE_PHINODES_H

extern void phinodes_print_statistics (void);
extern void reserve_phi_args_for_new_edge (basic_block);
extern void add_phi_node_to_bb (gphi *phi, basic_block bb);
extern gphi *create_phi_node (tree, basic_block);
extern void add_phi_arg (gphi *, tree, edge, source_location);
extern void remove_phi_args (edge);
extern void remove_phi_node (gimple_stmt_iterator *, bool);
extern void remove_phi_nodes (basic_block);
extern tree degenerate_phi_result (gphi *);
extern void set_phi_nodes (basic_block, gimple_seq);

static inline use_operand_p
gimple_phi_arg_imm_use_ptr (gimple *gs, int i)
{
  return &gimple_phi_arg (gs, i)->imm_use;
}

/* Return the phi argument which contains the specified use.  */

static inline int
phi_arg_index_from_use (use_operand_p use)
{
  struct phi_arg_d *element, *root;
  size_t index;
  gimple *phi;

  /* Since the use is the first thing in a PHI argument element, we can
     calculate its index based on casting it to an argument, and performing
     pointer arithmetic.  */

  phi = USE_STMT (use);

  element = (struct phi_arg_d *)use;
  root = gimple_phi_arg (phi, 0);
  index = element - root;

  /* Make sure the calculation doesn't have any leftover bytes.  If it does,
     then imm_use is likely not the first element in phi_arg_d.  */
  gcc_checking_assert ((((char *)element - (char *)root)
			% sizeof (struct phi_arg_d)) == 0
		       && index < gimple_phi_capacity (phi));

 return index;
}

#endif /* GCC_TREE_PHINODES_H */
