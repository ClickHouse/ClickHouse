/* SSA operand management for trees.
   Copyright (C) 2003-2018 Free Software Foundation, Inc.

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

#ifndef GCC_TREE_SSA_OPERANDS_H
#define GCC_TREE_SSA_OPERANDS_H

/* Interface to SSA operands.  */


/* This represents a pointer to a DEF operand.  */
typedef tree *def_operand_p;

/* This represents a pointer to a USE operand.  */
typedef ssa_use_operand_t *use_operand_p;

/* NULL operand types.  */
#define NULL_USE_OPERAND_P 		((use_operand_p)NULL)
#define NULL_DEF_OPERAND_P 		((def_operand_p)NULL)

/* This represents the USE operands of a stmt.  */
struct use_optype_d
{
  struct use_optype_d *next;
  struct ssa_use_operand_t use_ptr;
};
typedef struct use_optype_d *use_optype_p;

/* This structure represents a variable sized buffer which is allocated by the
   operand memory manager.  Operands are suballocated out of this block.  The
   MEM array varies in size.  */

struct GTY((chain_next("%h.next"))) ssa_operand_memory_d {
  struct ssa_operand_memory_d *next;
  char mem[1];
};

/* Per-function operand caches.  */
struct GTY(()) ssa_operands {
   struct ssa_operand_memory_d *operand_memory;
   unsigned operand_memory_index;
   /* Current size of the operand memory buffer.  */
   unsigned int ssa_operand_mem_size;

   bool ops_active;

   struct use_optype_d * GTY ((skip (""))) free_uses;
};

#define USE_FROM_PTR(PTR)	get_use_from_ptr (PTR)
#define DEF_FROM_PTR(PTR)	get_def_from_ptr (PTR)
#define SET_USE(USE, V)		set_ssa_use_from_ptr (USE, V)
#define SET_DEF(DEF, V)		((*(DEF)) = (V))

#define USE_STMT(USE)		(USE)->loc.stmt

#define USE_OP_PTR(OP)		(&((OP)->use_ptr))
#define USE_OP(OP)		(USE_FROM_PTR (USE_OP_PTR (OP)))

#define PHI_RESULT_PTR(PHI)	gimple_phi_result_ptr (PHI)
#define PHI_RESULT(PHI)		DEF_FROM_PTR (PHI_RESULT_PTR (PHI))
#define SET_PHI_RESULT(PHI, V)	SET_DEF (PHI_RESULT_PTR (PHI), (V))
/*
#define PHI_ARG_DEF(PHI, I)	USE_FROM_PTR (PHI_ARG_DEF_PTR ((PHI), (I)))
*/
#define PHI_ARG_DEF_PTR(PHI, I)	gimple_phi_arg_imm_use_ptr ((PHI), (I))
#define PHI_ARG_DEF(PHI, I)	gimple_phi_arg_def ((PHI), (I))
#define SET_PHI_ARG_DEF(PHI, I, V)					\
				SET_USE (PHI_ARG_DEF_PTR ((PHI), (I)), (V))
#define PHI_ARG_DEF_FROM_EDGE(PHI, E)					\
				PHI_ARG_DEF ((PHI), (E)->dest_idx)
#define PHI_ARG_DEF_PTR_FROM_EDGE(PHI, E)				\
				PHI_ARG_DEF_PTR ((PHI), (E)->dest_idx)
#define PHI_ARG_INDEX_FROM_USE(USE)   phi_arg_index_from_use (USE)


extern bool ssa_operands_active (struct function *);
extern void init_ssa_operands (struct function *fn);
extern void fini_ssa_operands (struct function *);
extern bool verify_ssa_operands (struct function *, gimple *stmt);
extern void free_stmt_operands (struct function *, gimple *);
extern void update_stmt_operands (struct function *, gimple *);
extern void swap_ssa_operands (gimple *, tree *, tree *);
extern bool verify_imm_links (FILE *f, tree var);

extern void dump_immediate_uses_for (FILE *file, tree var);
extern void dump_immediate_uses (FILE *file);
extern void debug_immediate_uses (void);
extern void debug_immediate_uses_for (tree var);

extern void unlink_stmt_vdef (gimple *);

/* Return the tree pointed-to by USE.  */
static inline tree
get_use_from_ptr (use_operand_p use)
{
  return *(use->use);
}

/* Return the tree pointed-to by DEF.  */
static inline tree
get_def_from_ptr (def_operand_p def)
{
  return *def;
}

#endif  /* GCC_TREE_SSA_OPERANDS_H  */
