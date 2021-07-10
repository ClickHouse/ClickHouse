/* Exported functions from emit-rtl.c
   Copyright (C) 2004-2018 Free Software Foundation, Inc.

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

#ifndef GCC_EMIT_RTL_H
#define GCC_EMIT_RTL_H

struct temp_slot;
typedef struct temp_slot *temp_slot_p;

/* Information mainlined about RTL representation of incoming arguments.  */
struct GTY(()) incoming_args {
  /* Number of bytes of args popped by function being compiled on its return.
     Zero if no bytes are to be popped.
     May affect compilation of return insn or of function epilogue.  */
  poly_int64_pod pops_args;

  /* If function's args have a fixed size, this is that size, in bytes.
     Otherwise, it is -1.
     May affect compilation of return insn or of function epilogue.  */
  poly_int64_pod size;

  /* # bytes the prologue should push and pretend that the caller pushed them.
     The prologue must do this, but only if parms can be passed in
     registers.  */
  int pretend_args_size;

  /* This is the offset from the arg pointer to the place where the first
     anonymous arg can be found, if there is one.  */
  rtx arg_offset_rtx;

  /* Quantities of various kinds of registers
     used for the current function's args.  */
  CUMULATIVE_ARGS info;

  /* The arg pointer hard register, or the pseudo into which it was copied.  */
  rtx internal_arg_pointer;
};


/* Datastructures maintained for currently processed function in RTL form.  */
struct GTY(()) rtl_data {
  void init_stack_alignment ();

  struct expr_status expr;
  struct emit_status emit;
  struct varasm_status varasm;
  struct incoming_args args;
  struct function_subsections subsections;
  struct rtl_eh eh;

  /* For function.c  */

  /* # of bytes of outgoing arguments.  If ACCUMULATE_OUTGOING_ARGS is
     defined, the needed space is pushed by the prologue.  */
  poly_int64_pod outgoing_args_size;

  /* If nonzero, an RTL expression for the location at which the current
     function returns its result.  If the current function returns its
     result in a register, current_function_return_rtx will always be
     the hard register containing the result.  */
  rtx return_rtx;
  /* If nonxero, an RTL expression for the lcoation at which the current
     function returns bounds for its result.  */
  rtx return_bnd;

  /* Vector of initial-value pairs.  Each pair consists of a pseudo
     register of approprite mode that stores the initial value a hard
     register REGNO, and that hard register itself.  */
  /* ??? This could be a VEC but there is currently no way to define an
	 opaque VEC type.  */
  struct initial_value_struct *hard_reg_initial_vals;

  /* A variable living at the top of the frame that holds a known value.
     Used for detecting stack clobbers.  */
  tree stack_protect_guard;

  /* List (chain of INSN_LIST) of labels heading the current handlers for
     nonlocal gotos.  */
  rtx_insn_list *x_nonlocal_goto_handler_labels;

  /* Label that will go on function epilogue.
     Jumping to this label serves as a "return" instruction
     on machines which require execution of the epilogue on all returns.  */
  rtx_code_label *x_return_label;

  /* Label that will go on the end of function epilogue.
     Jumping to this label serves as a "naked return" instruction
     on machines which require execution of the epilogue on all returns.  */
  rtx_code_label *x_naked_return_label;

  /* List (chain of EXPR_LISTs) of all stack slots in this function.
     Made for the sake of unshare_all_rtl.  */
  vec<rtx, va_gc> *x_stack_slot_list;

  /* List of empty areas in the stack frame.  */
  struct frame_space *frame_space_list;

  /* Place after which to insert the tail_recursion_label if we need one.  */
  rtx_note *x_stack_check_probe_note;

  /* Location at which to save the argument pointer if it will need to be
     referenced.  There are two cases where this is done: if nonlocal gotos
     exist, or if vars stored at an offset from the argument pointer will be
     needed by inner routines.  */
  rtx x_arg_pointer_save_area;

  /* Dynamic Realign Argument Pointer used for realigning stack.  */
  rtx drap_reg;

  /* Offset to end of allocated area of stack frame.
     If stack grows down, this is the address of the last stack slot allocated.
     If stack grows up, this is the address for the next slot.  */
  poly_int64_pod x_frame_offset;

  /* Insn after which register parms and SAVE_EXPRs are born, if nonopt.  */
  rtx_insn *x_parm_birth_insn;

  /* List of all used temporaries allocated, by level.  */
  vec<temp_slot_p, va_gc> *x_used_temp_slots;

  /* List of available temp slots.  */
  struct temp_slot *x_avail_temp_slots;

  /* Current nesting level for temporaries.  */
  int x_temp_slot_level;

  /* The largest alignment needed on the stack, including requirement
     for outgoing stack alignment.  */
  unsigned int stack_alignment_needed;

  /* Preferred alignment of the end of stack frame, which is preferred
     to call other functions.  */
  unsigned int preferred_stack_boundary;

  /* The minimum alignment of parameter stack.  */
  unsigned int parm_stack_boundary;

  /* The largest alignment of slot allocated on the stack.  */
  unsigned int max_used_stack_slot_alignment;

  /* The stack alignment estimated before reload, with consideration of
     following factors:
     1. Alignment of local stack variables (max_used_stack_slot_alignment)
     2. Alignment requirement to call other functions
        (preferred_stack_boundary)
     3. Alignment of non-local stack variables but might be spilled in
        local stack.  */
  unsigned int stack_alignment_estimated;

  /* For reorg.  */

  /* Nonzero if function being compiled called builtin_return_addr or
     builtin_frame_address with nonzero count.  */
  bool accesses_prior_frames;

  /* Nonzero if the function calls __builtin_eh_return.  */
  bool calls_eh_return;

  /* Nonzero if function saves all registers, e.g. if it has a nonlocal
     label that can reach the exit block via non-exceptional paths. */
  bool saves_all_registers;

  /* Nonzero if function being compiled has nonlocal gotos to parent
     function.  */
  bool has_nonlocal_goto;

  /* Nonzero if function being compiled has an asm statement.  */
  bool has_asm_statement;

  /* This bit is used by the exception handling logic.  It is set if all
     calls (if any) are sibling calls.  Such functions do not have to
     have EH tables generated, as they cannot throw.  A call to such a
     function, however, should be treated as throwing if any of its callees
     can throw.  */
  bool all_throwers_are_sibcalls;

  /* Nonzero if stack limit checking should be enabled in the current
     function.  */
  bool limit_stack;

  /* Nonzero if profiling code should be generated.  */
  bool profile;

  /* Nonzero if the current function uses the constant pool.  */
  bool uses_const_pool;

  /* Nonzero if the current function uses pic_offset_table_rtx.  */
  bool uses_pic_offset_table;

  /* Nonzero if the current function needs an lsda for exception handling.  */
  bool uses_eh_lsda;

  /* Set when the tail call has been produced.  */
  bool tail_call_emit;

  /* Nonzero if code to initialize arg_pointer_save_area has been emitted.  */
  bool arg_pointer_save_area_init;

  /* Nonzero if current function must be given a frame pointer.
     Set in reload1.c or lra-eliminations.c if anything is allocated
     on the stack there.  */
  bool frame_pointer_needed;

  /* When set, expand should optimize for speed.  */
  bool maybe_hot_insn_p;

  /* Nonzero if function stack realignment is needed.  This flag may be
     set twice: before and after reload.  It is set before reload wrt
     stack alignment estimation before reload.  It will be changed after
     reload if by then criteria of stack realignment is different.
     The value set after reload is the accurate one and is finalized.  */
  bool stack_realign_needed;

  /* Nonzero if function stack realignment is tried.  This flag is set
     only once before reload.  It affects register elimination.  This
     is used to generate DWARF debug info for stack variables.  */
  bool stack_realign_tried;

  /* Nonzero if function being compiled needs dynamic realigned
     argument pointer (drap) if stack needs realigning.  */
  bool need_drap;

  /* Nonzero if function stack realignment estimation is done, namely
     stack_realign_needed flag has been set before reload wrt estimated
     stack alignment info.  */
  bool stack_realign_processed;

  /* Nonzero if function stack realignment has been finalized, namely
     stack_realign_needed flag has been set and finalized after reload.  */
  bool stack_realign_finalized;

  /* True if dbr_schedule has already been called for this function.  */
  bool dbr_scheduled_p;

  /* True if current function can not throw.  Unlike
     TREE_NOTHROW (current_function_decl) it is set even for overwritable
     function where currently compiled version of it is nothrow.  */
  bool nothrow;

  /* True if we performed shrink-wrapping for the current function.  */
  bool shrink_wrapped;

  /* True if we performed shrink-wrapping for separate components for
     the current function.  */
  bool shrink_wrapped_separate;

  /* Nonzero if function being compiled doesn't modify the stack pointer
     (ignoring the prologue and epilogue).  This is only valid after
     pass_stack_ptr_mod has run.  */
  bool sp_is_unchanging;

  /* Nonzero if function being compiled doesn't contain any calls
     (ignoring the prologue and epilogue).  This is set prior to
     register allocation in IRA and is valid for the remaining
     compiler passes.  */
  bool is_leaf;

  /* Nonzero if the function being compiled is a leaf function which only
     uses leaf registers.  This is valid after reload (specifically after
     sched2) and is useful only if the port defines LEAF_REGISTERS.  */
  bool uses_only_leaf_regs;

  /* Nonzero if the function being compiled has undergone hot/cold partitioning
     (under flag_reorder_blocks_and_partition) and has at least one cold
     block.  */
  bool has_bb_partition;

  /* Nonzero if the function being compiled has completed the bb reordering
     pass.  */
  bool bb_reorder_complete;

  /* Like regs_ever_live, but 1 if a reg is set or clobbered from an
     asm.  Unlike regs_ever_live, elements of this array corresponding
     to eliminable regs (like the frame pointer) are set if an asm
     sets them.  */
  HARD_REG_SET asm_clobbers;

  /* The highest address seen during shorten_branches.  */
  int max_insn_address;
};

#define return_label (crtl->x_return_label)
#define naked_return_label (crtl->x_naked_return_label)
#define stack_slot_list (crtl->x_stack_slot_list)
#define parm_birth_insn (crtl->x_parm_birth_insn)
#define frame_offset (crtl->x_frame_offset)
#define stack_check_probe_note (crtl->x_stack_check_probe_note)
#define arg_pointer_save_area (crtl->x_arg_pointer_save_area)
#define used_temp_slots (crtl->x_used_temp_slots)
#define avail_temp_slots (crtl->x_avail_temp_slots)
#define temp_slot_level (crtl->x_temp_slot_level)
#define nonlocal_goto_handler_labels (crtl->x_nonlocal_goto_handler_labels)
#define frame_pointer_needed (crtl->frame_pointer_needed)
#define stack_realign_fp (crtl->stack_realign_needed && !crtl->need_drap)
#define stack_realign_drap (crtl->stack_realign_needed && crtl->need_drap)

extern GTY(()) struct rtl_data x_rtl;

/* Accessor to RTL datastructures.  We keep them statically allocated now since
   we never keep multiple functions.  For threaded compiler we might however
   want to do differently.  */
#define crtl (&x_rtl)

/* Return whether two MEM_ATTRs are equal.  */
bool mem_attrs_eq_p (const struct mem_attrs *, const struct mem_attrs *);

/* Set the alias set of MEM to SET.  */
extern void set_mem_alias_set (rtx, alias_set_type);

/* Set the alignment of MEM to ALIGN bits.  */
extern void set_mem_align (rtx, unsigned int);

/* Set the address space of MEM to ADDRSPACE.  */
extern void set_mem_addr_space (rtx, addr_space_t);

/* Set the expr for MEM to EXPR.  */
extern void set_mem_expr (rtx, tree);

/* Set the offset for MEM to OFFSET.  */
extern void set_mem_offset (rtx, poly_int64);

/* Clear the offset recorded for MEM.  */
extern void clear_mem_offset (rtx);

/* Set the size for MEM to SIZE.  */
extern void set_mem_size (rtx, poly_int64);

/* Clear the size recorded for MEM.  */
extern void clear_mem_size (rtx);

/* Set the attributes for MEM appropriate for a spill slot.  */
extern void set_mem_attrs_for_spill (rtx);
extern tree get_spill_slot_decl (bool);

/* Return a memory reference like MEMREF, but with its address changed to
   ADDR.  The caller is asserting that the actual piece of memory pointed
   to is the same, just the form of the address is being changed, such as
   by putting something into a register.  */
extern rtx replace_equiv_address (rtx, rtx, bool = false);

/* Likewise, but the reference is not required to be valid.  */
extern rtx replace_equiv_address_nv (rtx, rtx, bool = false);

extern rtx gen_blockage (void);
extern rtvec gen_rtvec (int, ...);
extern rtx copy_insn_1 (rtx);
extern rtx copy_insn (rtx);
extern rtx_insn *copy_delay_slot_insn (rtx_insn *);
extern rtx gen_int_mode (poly_int64, machine_mode);
extern rtx_insn *emit_copy_of_insn_after (rtx_insn *, rtx_insn *);
extern void set_reg_attrs_from_value (rtx, rtx);
extern void set_reg_attrs_for_parm (rtx, rtx);
extern void set_reg_attrs_for_decl_rtl (tree t, rtx x);
extern void adjust_reg_mode (rtx, machine_mode);
extern int mem_expr_equal_p (const_tree, const_tree);
extern rtx gen_int_shift_amount (machine_mode, poly_int64);

extern bool need_atomic_barrier_p (enum memmodel, bool);

/* Return the current sequence.  */

static inline struct sequence_stack *
get_current_sequence (void)
{
  return &crtl->emit.seq;
}

/* Return the outermost sequence.  */

static inline struct sequence_stack *
get_topmost_sequence (void)
{
  struct sequence_stack *seq, *top;

  seq = get_current_sequence ();
  do
    {
      top = seq;
      seq = seq->next;
    } while (seq);
  return top;
}

/* Return the first insn of the current sequence or current function.  */

static inline rtx_insn *
get_insns (void)
{
  return get_current_sequence ()->first;
}

/* Specify a new insn as the first in the chain.  */

static inline void
set_first_insn (rtx_insn *insn)
{
  gcc_checking_assert (!insn || !PREV_INSN (insn));
  get_current_sequence ()->first = insn;
}

/* Return the last insn emitted in current sequence or current function.  */

static inline rtx_insn *
get_last_insn (void)
{
  return get_current_sequence ()->last;
}

/* Specify a new insn as the last in the chain.  */

static inline void
set_last_insn (rtx_insn *insn)
{
  gcc_checking_assert (!insn || !NEXT_INSN (insn));
  get_current_sequence ()->last = insn;
}

/* Return a number larger than any instruction's uid in this function.  */

static inline int
get_max_uid (void)
{
  return crtl->emit.x_cur_insn_uid;
}

extern bool valid_for_const_vector_p (machine_mode, rtx);
extern rtx gen_const_vec_duplicate (machine_mode, rtx);
extern rtx gen_vec_duplicate (machine_mode, rtx);

extern rtx gen_const_vec_series (machine_mode, rtx, rtx);
extern rtx gen_vec_series (machine_mode, rtx, rtx);

extern void set_decl_incoming_rtl (tree, rtx, bool);

/* Return a memory reference like MEMREF, but with its mode changed
   to MODE and its address changed to ADDR.
   (VOIDmode means don't change the mode.
   NULL for ADDR means don't change the address.)  */
extern rtx change_address (rtx, machine_mode, rtx);

/* Return a memory reference like MEMREF, but with its mode changed
   to MODE and its address offset by OFFSET bytes.  */
#define adjust_address(MEMREF, MODE, OFFSET) \
  adjust_address_1 (MEMREF, MODE, OFFSET, 1, 1, 0, 0)

/* Likewise, but the reference is not required to be valid.  */
#define adjust_address_nv(MEMREF, MODE, OFFSET) \
  adjust_address_1 (MEMREF, MODE, OFFSET, 0, 1, 0, 0)

/* Return a memory reference like MEMREF, but with its mode changed
   to MODE and its address offset by OFFSET bytes.  Assume that it's
   for a bitfield and conservatively drop the underlying object if we
   cannot be sure to stay within its bounds.  */
#define adjust_bitfield_address(MEMREF, MODE, OFFSET) \
  adjust_address_1 (MEMREF, MODE, OFFSET, 1, 1, 1, 0)

/* As for adjust_bitfield_address, but specify that the width of
   BLKmode accesses is SIZE bytes.  */
#define adjust_bitfield_address_size(MEMREF, MODE, OFFSET, SIZE) \
  adjust_address_1 (MEMREF, MODE, OFFSET, 1, 1, 1, SIZE)

/* Likewise, but the reference is not required to be valid.  */
#define adjust_bitfield_address_nv(MEMREF, MODE, OFFSET) \
  adjust_address_1 (MEMREF, MODE, OFFSET, 0, 1, 1, 0)

/* Return a memory reference like MEMREF, but with its mode changed
   to MODE and its address changed to ADDR, which is assumed to be
   increased by OFFSET bytes from MEMREF.  */
#define adjust_automodify_address(MEMREF, MODE, ADDR, OFFSET) \
  adjust_automodify_address_1 (MEMREF, MODE, ADDR, OFFSET, 1)

/* Likewise, but the reference is not required to be valid.  */
#define adjust_automodify_address_nv(MEMREF, MODE, ADDR, OFFSET) \
  adjust_automodify_address_1 (MEMREF, MODE, ADDR, OFFSET, 0)

extern rtx adjust_address_1 (rtx, machine_mode, poly_int64, int, int,
			     int, poly_int64);
extern rtx adjust_automodify_address_1 (rtx, machine_mode, rtx,
					poly_int64, int);

/* Return a memory reference like MEMREF, but whose address is changed by
   adding OFFSET, an RTX, to it.  POW2 is the highest power of two factor
   known to be in OFFSET (possibly 1).  */
extern rtx offset_address (rtx, rtx, unsigned HOST_WIDE_INT);

/* Given REF, a MEM, and T, either the type of X or the expression
   corresponding to REF, set the memory attributes.  OBJECTP is nonzero
   if we are making a new object of this type.  */
extern void set_mem_attributes (rtx, tree, int);

/* Similar, except that BITPOS has not yet been applied to REF, so if
   we alter MEM_OFFSET according to T then we should subtract BITPOS
   expecting that it'll be added back in later.  */
extern void set_mem_attributes_minus_bitpos (rtx, tree, int, poly_int64);

/* Return OFFSET if XEXP (MEM, 0) - OFFSET is known to be ALIGN
   bits aligned for 0 <= OFFSET < ALIGN / BITS_PER_UNIT, or
   -1 if not known.  */
extern int get_mem_align_offset (rtx, unsigned int);

/* Return a memory reference like MEMREF, but with its mode widened to
   MODE and adjusted by OFFSET.  */
extern rtx widen_memory_access (rtx, machine_mode, poly_int64);

extern void maybe_set_max_label_num (rtx_code_label *x);

#endif /* GCC_EMIT_RTL_H */
