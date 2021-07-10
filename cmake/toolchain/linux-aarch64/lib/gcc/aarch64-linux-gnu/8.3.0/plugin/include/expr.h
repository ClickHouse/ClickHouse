/* Definitions for code generation pass of GNU compiler.
   Copyright (C) 1987-2018 Free Software Foundation, Inc.

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

#ifndef GCC_EXPR_H
#define GCC_EXPR_H

/* This is the 4th arg to `expand_expr'.
   EXPAND_STACK_PARM means we are possibly expanding a call param onto
   the stack.
   EXPAND_SUM means it is ok to return a PLUS rtx or MULT rtx.
   EXPAND_INITIALIZER is similar but also record any labels on forced_labels.
   EXPAND_CONST_ADDRESS means it is ok to return a MEM whose address
    is a constant that is not a legitimate address.
   EXPAND_WRITE means we are only going to write to the resulting rtx.
   EXPAND_MEMORY means we are interested in a memory result, even if
    the memory is constant and we could have propagated a constant value,
    or the memory is unaligned on a STRICT_ALIGNMENT target.  */
enum expand_modifier {EXPAND_NORMAL = 0, EXPAND_STACK_PARM, EXPAND_SUM,
		      EXPAND_CONST_ADDRESS, EXPAND_INITIALIZER, EXPAND_WRITE,
		      EXPAND_MEMORY};

/* Prevent the compiler from deferring stack pops.  See
   inhibit_defer_pop for more information.  */
#define NO_DEFER_POP (inhibit_defer_pop += 1)

/* Allow the compiler to defer stack pops.  See inhibit_defer_pop for
   more information.  */
#define OK_DEFER_POP (inhibit_defer_pop -= 1)

/* This structure is used to pass around information about exploded
   unary, binary and trinary expressions between expand_expr_real_1 and
   friends.  */
typedef struct separate_ops
{
  enum tree_code code;
  location_t location;
  tree type;
  tree op0, op1, op2;
} *sepops;

/* This is run during target initialization to set up which modes can be
   used directly in memory and to initialize the block move optab.  */
extern void init_expr_target (void);

/* This is run at the start of compiling a function.  */
extern void init_expr (void);

/* Emit some rtl insns to move data between rtx's, converting machine modes.
   Both modes must be floating or both fixed.  */
extern void convert_move (rtx, rtx, int);

/* Convert an rtx to specified machine mode and return the result.  */
extern rtx convert_to_mode (machine_mode, rtx, int);

/* Convert an rtx to MODE from OLDMODE and return the result.  */
extern rtx convert_modes (machine_mode, machine_mode, rtx, int);

/* Expand a call to memcpy or memmove or memcmp, and return the result.  */
extern rtx emit_block_op_via_libcall (enum built_in_function, rtx, rtx, rtx,
				      bool);

static inline rtx
emit_block_copy_via_libcall (rtx dst, rtx src, rtx size, bool tailcall = false)
{
  return emit_block_op_via_libcall (BUILT_IN_MEMCPY, dst, src, size, tailcall);
}

static inline rtx
emit_block_move_via_libcall (rtx dst, rtx src, rtx size, bool tailcall = false)
{
  return emit_block_op_via_libcall (BUILT_IN_MEMMOVE, dst, src, size, tailcall);
}

static inline rtx
emit_block_comp_via_libcall (rtx dst, rtx src, rtx size, bool tailcall = false)
{
  return emit_block_op_via_libcall (BUILT_IN_MEMCMP, dst, src, size, tailcall);
}

/* Emit code to move a block Y to a block X.  */
enum block_op_methods
{
  BLOCK_OP_NORMAL,
  BLOCK_OP_NO_LIBCALL,
  BLOCK_OP_CALL_PARM,
  /* Like BLOCK_OP_NORMAL, but the libcall can be tail call optimized.  */
  BLOCK_OP_TAILCALL,
  /* Like BLOCK_OP_NO_LIBCALL, but instead of emitting a libcall return
     pc_rtx to indicate nothing has been emitted and let the caller handle
     it.  */
  BLOCK_OP_NO_LIBCALL_RET
};

typedef rtx (*by_pieces_constfn) (void *, HOST_WIDE_INT, scalar_int_mode);

extern rtx emit_block_move (rtx, rtx, rtx, enum block_op_methods);
extern rtx emit_block_move_hints (rtx, rtx, rtx, enum block_op_methods,
			          unsigned int, HOST_WIDE_INT,
				  unsigned HOST_WIDE_INT,
				  unsigned HOST_WIDE_INT,
				  unsigned HOST_WIDE_INT);
extern rtx emit_block_cmp_hints (rtx, rtx, rtx, tree, rtx, bool,
				 by_pieces_constfn, void *);
extern bool emit_storent_insn (rtx to, rtx from);

/* Copy all or part of a value X into registers starting at REGNO.
   The number of registers to be filled is NREGS.  */
extern void move_block_to_reg (int, rtx, int, machine_mode);

/* Copy all or part of a BLKmode value X out of registers starting at REGNO.
   The number of registers to be filled is NREGS.  */
extern void move_block_from_reg (int, rtx, int);

/* Generate a non-consecutive group of registers represented by a PARALLEL.  */
extern rtx gen_group_rtx (rtx);

/* Load a BLKmode value into non-consecutive registers represented by a
   PARALLEL.  */
extern void emit_group_load (rtx, rtx, tree, poly_int64);

/* Similarly, but load into new temporaries.  */
extern rtx emit_group_load_into_temps (rtx, rtx, tree, poly_int64);

/* Move a non-consecutive group of registers represented by a PARALLEL into
   a non-consecutive group of registers represented by a PARALLEL.  */
extern void emit_group_move (rtx, rtx);

/* Move a group of registers represented by a PARALLEL into pseudos.  */
extern rtx emit_group_move_into_temps (rtx);

/* Store a BLKmode value from non-consecutive registers represented by a
   PARALLEL.  */
extern void emit_group_store (rtx, rtx, tree, poly_int64);

extern rtx maybe_emit_group_store (rtx, tree);

/* Mark REG as holding a parameter for the next CALL_INSN.
   Mode is TYPE_MODE of the non-promoted parameter, or VOIDmode.  */
extern void use_reg_mode (rtx *, rtx, machine_mode);
extern void clobber_reg_mode (rtx *, rtx, machine_mode);

extern rtx copy_blkmode_to_reg (machine_mode, tree);

/* Mark REG as holding a parameter for the next CALL_INSN.  */
static inline void
use_reg (rtx *fusage, rtx reg)
{
  use_reg_mode (fusage, reg, VOIDmode);
}

/* Mark REG as clobbered by the call with FUSAGE as CALL_INSN_FUNCTION_USAGE.  */
static inline void
clobber_reg (rtx *fusage, rtx reg)
{
  clobber_reg_mode (fusage, reg, VOIDmode);
}

/* Mark NREGS consecutive regs, starting at REGNO, as holding parameters
   for the next CALL_INSN.  */
extern void use_regs (rtx *, int, int);

/* Mark a PARALLEL as holding a parameter for the next CALL_INSN.  */
extern void use_group_regs (rtx *, rtx);

#ifdef GCC_INSN_CODES_H
extern rtx expand_cmpstrn_or_cmpmem (insn_code, rtx, rtx, rtx, tree, rtx,
				     HOST_WIDE_INT);
#endif

/* Write zeros through the storage of OBJECT.
   If OBJECT has BLKmode, SIZE is its length in bytes.  */
extern rtx clear_storage (rtx, rtx, enum block_op_methods);
extern rtx clear_storage_hints (rtx, rtx, enum block_op_methods,
			        unsigned int, HOST_WIDE_INT,
				unsigned HOST_WIDE_INT,
				unsigned HOST_WIDE_INT,
				unsigned HOST_WIDE_INT);
/* The same, but always output an library call.  */
extern rtx set_storage_via_libcall (rtx, rtx, rtx, bool = false);

/* Expand a setmem pattern; return true if successful.  */
extern bool set_storage_via_setmem (rtx, rtx, rtx, unsigned int,
				    unsigned int, HOST_WIDE_INT,
				    unsigned HOST_WIDE_INT,
				    unsigned HOST_WIDE_INT,
				    unsigned HOST_WIDE_INT);

/* Return nonzero if it is desirable to store LEN bytes generated by
   CONSTFUN with several move instructions by store_by_pieces
   function.  CONSTFUNDATA is a pointer which will be passed as argument
   in every CONSTFUN call.
   ALIGN is maximum alignment we can assume.
   MEMSETP is true if this is a real memset/bzero, not a copy
   of a const string.  */
extern int can_store_by_pieces (unsigned HOST_WIDE_INT,
				by_pieces_constfn,
				void *, unsigned int, bool);

/* Generate several move instructions to store LEN bytes generated by
   CONSTFUN to block TO.  (A MEM rtx with BLKmode).  CONSTFUNDATA is a
   pointer which will be passed as argument in every CONSTFUN call.
   ALIGN is maximum alignment we can assume.
   MEMSETP is true if this is a real memset/bzero, not a copy.
   Returns TO + LEN.  */
extern rtx store_by_pieces (rtx, unsigned HOST_WIDE_INT, by_pieces_constfn,
			    void *, unsigned int, bool, int);

/* Emit insns to set X from Y.  */
extern rtx_insn *emit_move_insn (rtx, rtx);
extern rtx_insn *gen_move_insn (rtx, rtx);

/* Emit insns to set X from Y, with no frills.  */
extern rtx_insn *emit_move_insn_1 (rtx, rtx);

extern rtx_insn *emit_move_complex_push (machine_mode, rtx, rtx);
extern rtx_insn *emit_move_complex_parts (rtx, rtx);
extern rtx read_complex_part (rtx, bool);
extern void write_complex_part (rtx, rtx, bool);
extern rtx read_complex_part (rtx, bool);
extern rtx emit_move_resolve_push (machine_mode, rtx);

/* Push a block of length SIZE (perhaps variable)
   and return an rtx to address the beginning of the block.  */
extern rtx push_block (rtx, poly_int64, int);

/* Generate code to push something onto the stack, given its mode and type.  */
extern bool emit_push_insn (rtx, machine_mode, tree, rtx, unsigned int,
			    int, rtx, poly_int64, rtx, rtx, int, rtx, bool);

/* Extract the accessible bit-range from a COMPONENT_REF.  */
extern void get_bit_range (poly_uint64_pod *, poly_uint64_pod *, tree,
			   poly_int64_pod *, tree *);

/* Expand an assignment that stores the value of FROM into TO.  */
extern void expand_assignment (tree, tree, bool);

/* Generate code for computing expression EXP,
   and storing the value into TARGET.
   If SUGGEST_REG is nonzero, copy the value through a register
   and return that register, if that is possible.  */
extern rtx store_expr_with_bounds (tree, rtx, int, bool, bool, tree);
extern rtx store_expr (tree, rtx, int, bool, bool);

/* Given an rtx that may include add and multiply operations,
   generate them as insns and return a pseudo-reg containing the value.
   Useful after calling expand_expr with 1 as sum_ok.  */
extern rtx force_operand (rtx, rtx);

/* Work horses for expand_expr.  */
extern rtx expand_expr_real (tree, rtx, machine_mode,
			     enum expand_modifier, rtx *, bool);
extern rtx expand_expr_real_1 (tree, rtx, machine_mode,
			       enum expand_modifier, rtx *, bool);
extern rtx expand_expr_real_2 (sepops, rtx, machine_mode,
			       enum expand_modifier);

/* Generate code for computing expression EXP.
   An rtx for the computed value is returned.  The value is never null.
   In the case of a void EXP, const0_rtx is returned.  */
static inline rtx
expand_expr (tree exp, rtx target, machine_mode mode,
	     enum expand_modifier modifier)
{
  return expand_expr_real (exp, target, mode, modifier, NULL, false);
}

static inline rtx
expand_normal (tree exp)
{
  return expand_expr_real (exp, NULL_RTX, VOIDmode, EXPAND_NORMAL, NULL, false);
}


/* Return the tree node and offset if a given argument corresponds to
   a string constant.  */
extern tree string_constant (tree, tree *);

/* Two different ways of generating switch statements.  */
extern int try_casesi (tree, tree, tree, tree, rtx, rtx, rtx, profile_probability);
extern int try_tablejump (tree, tree, tree, tree, rtx, rtx, profile_probability);

extern int safe_from_p (const_rtx, tree, int);

/* Get the personality libfunc for a function decl.  */
rtx get_personality_function (tree);

/* Determine whether the LEN bytes can be moved by using several move
   instructions.  Return nonzero if a call to move_by_pieces should
   succeed.  */
extern bool can_move_by_pieces (unsigned HOST_WIDE_INT, unsigned int);

extern unsigned HOST_WIDE_INT highest_pow2_factor (const_tree);

extern bool categorize_ctor_elements (const_tree, HOST_WIDE_INT *,
				      HOST_WIDE_INT *, HOST_WIDE_INT *,
				      bool *);

extern void expand_operands (tree, tree, rtx, rtx*, rtx*,
			     enum expand_modifier);

/* rtl.h and tree.h were included.  */
/* Return an rtx for the size in bytes of the value of an expr.  */
extern rtx expr_size (tree);

#endif /* GCC_EXPR_H */
