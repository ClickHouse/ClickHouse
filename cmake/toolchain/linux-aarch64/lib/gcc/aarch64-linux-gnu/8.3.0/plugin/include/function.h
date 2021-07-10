/* Structure for saving state for a nested function.
   Copyright (C) 1989-2018 Free Software Foundation, Inc.

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

#ifndef GCC_FUNCTION_H
#define GCC_FUNCTION_H


/* Stack of pending (incomplete) sequences saved by `start_sequence'.
   Each element describes one pending sequence.
   The main insn-chain is saved in the last element of the chain,
   unless the chain is empty.  */

struct GTY(()) sequence_stack {
  /* First and last insns in the chain of the saved sequence.  */
  rtx_insn *first;
  rtx_insn *last;
  struct sequence_stack *next;
};

struct GTY(()) emit_status {
  void ensure_regno_capacity ();

  /* This is reset to LAST_VIRTUAL_REGISTER + 1 at the start of each function.
     After rtl generation, it is 1 plus the largest register number used.  */
  int x_reg_rtx_no;

  /* Lowest label number in current function.  */
  int x_first_label_num;

  /* seq.first and seq.last are the ends of the doubly-linked chain of
     rtl for the current function.  Both are reset to null at the
     start of rtl generation for the function. 

     start_sequence saves both of these on seq.next and then starts
     a new, nested sequence of insns.

     seq.next is a stack of pending (incomplete) sequences saved by
     start_sequence.  Each element describes one pending sequence.
     The main insn-chain is the last element of the chain.  */
  struct sequence_stack seq;

  /* INSN_UID for next insn emitted.
     Reset to 1 for each function compiled.  */
  int x_cur_insn_uid;

  /* INSN_UID for next debug insn emitted.  Only used if
     --param min-nondebug-insn-uid=<value> is given with nonzero value.  */
  int x_cur_debug_insn_uid;

  /* The length of the regno_pointer_align, regno_decl, and x_regno_reg_rtx
     vectors.  Since these vectors are needed during the expansion phase when
     the total number of registers in the function is not yet known, the
     vectors are copied and made bigger when necessary.  */
  int regno_pointer_align_length;

  /* Indexed by pseudo register number, if nonzero gives the known alignment
     for that pseudo (if REG_POINTER is set in x_regno_reg_rtx).
     Allocated in parallel with x_regno_reg_rtx.  */
  unsigned char * GTY((skip)) regno_pointer_align;
};


/* Indexed by register number, gives an rtx for that register (and only
   that register).  For pseudo registers, it is the unique rtx for
   that pseudo.  For hard registers, it is an rtx of the mode specified
   by reg_raw_mode.

   FIXME: We could put it into emit_status struct, but gengtype is not
   able to deal with length attribute nested in top level structures.  */

extern GTY ((length ("crtl->emit.x_reg_rtx_no"))) rtx * regno_reg_rtx;

/* For backward compatibility... eventually these should all go away.  */
#define reg_rtx_no (crtl->emit.x_reg_rtx_no)

#define REGNO_POINTER_ALIGN(REGNO) (crtl->emit.regno_pointer_align[REGNO])

struct GTY(()) expr_status {
  /* Number of units that we should eventually pop off the stack.
     These are the arguments to function calls that have already returned.  */
  poly_int64_pod x_pending_stack_adjust;

  /* Under some ABIs, it is the caller's responsibility to pop arguments
     pushed for function calls.  A naive implementation would simply pop
     the arguments immediately after each call.  However, if several
     function calls are made in a row, it is typically cheaper to pop
     all the arguments after all of the calls are complete since a
     single pop instruction can be used.  Therefore, GCC attempts to
     defer popping the arguments until absolutely necessary.  (For
     example, at the end of a conditional, the arguments must be popped,
     since code outside the conditional won't know whether or not the
     arguments need to be popped.)

     When INHIBIT_DEFER_POP is nonzero, however, the compiler does not
     attempt to defer pops.  Instead, the stack is popped immediately
     after each call.  Rather then setting this variable directly, use
     NO_DEFER_POP and OK_DEFER_POP.  */
  int x_inhibit_defer_pop;

  /* If PREFERRED_STACK_BOUNDARY and PUSH_ROUNDING are defined, the stack
     boundary can be momentarily unaligned while pushing the arguments.
     Record the delta since last aligned boundary here in order to get
     stack alignment in the nested function calls working right.  */
  poly_int64_pod x_stack_pointer_delta;

  /* Nonzero means __builtin_saveregs has already been done in this function.
     The value is the pseudoreg containing the value __builtin_saveregs
     returned.  */
  rtx x_saveregs_value;

  /* Similarly for __builtin_apply_args.  */
  rtx x_apply_args_value;

  /* List of labels that must never be deleted.  */
  vec<rtx_insn *, va_gc> *x_forced_labels;
};

typedef struct call_site_record_d *call_site_record;

/* RTL representation of exception handling.  */
struct GTY(()) rtl_eh {
  rtx ehr_stackadj;
  rtx ehr_handler;
  rtx_code_label *ehr_label;

  rtx sjlj_fc;
  rtx_insn *sjlj_exit_after;

  vec<uchar, va_gc> *action_record_data;

  vec<call_site_record, va_gc> *call_site_record_v[2];
};

#define pending_stack_adjust (crtl->expr.x_pending_stack_adjust)
#define inhibit_defer_pop (crtl->expr.x_inhibit_defer_pop)
#define saveregs_value (crtl->expr.x_saveregs_value)
#define apply_args_value (crtl->expr.x_apply_args_value)
#define forced_labels (crtl->expr.x_forced_labels)
#define stack_pointer_delta (crtl->expr.x_stack_pointer_delta)

struct gimple_df;
struct call_site_record_d;
struct dw_fde_node;

struct GTY(()) varasm_status {
  /* If we're using a per-function constant pool, this is it.  */
  struct rtx_constant_pool *pool;

  /* Number of tree-constants deferred during the expansion of this
     function.  */
  unsigned int deferred_constants;
};


/* Data for function partitioning.  */
struct GTY(()) function_subsections {
  /* Assembly labels for the hot and cold text sections, to
     be used by debugger functions for determining the size of text
     sections.  */

  const char *hot_section_label;
  const char *cold_section_label;
  const char *hot_section_end_label;
  const char *cold_section_end_label;
};

/* Describe an empty area of space in the stack frame.  These can be chained
   into a list; this is used to keep track of space wasted for alignment
   reasons.  */
struct GTY(()) frame_space
{
  struct frame_space *next;

  poly_int64 start;
  poly_int64 length;
};

struct GTY(()) stack_usage
{
  /* # of bytes of static stack space allocated by the function.  */
  HOST_WIDE_INT static_stack_size;

  /* # of bytes of dynamic stack space allocated by the function.  This is
     meaningful only if has_unbounded_dynamic_stack_size is zero.  */
  HOST_WIDE_INT dynamic_stack_size;

  /* Upper bound on the number of bytes pushed onto the stack after the
     prologue.  If !ACCUMULATE_OUTGOING_ARGS, it contains the outgoing
     arguments.  */
  poly_int64 pushed_stack_size;

  /* Nonzero if the amount of stack space allocated dynamically cannot
     be bounded at compile-time.  */
  unsigned int has_unbounded_dynamic_stack_size : 1;
};

#define current_function_static_stack_size (cfun->su->static_stack_size)
#define current_function_dynamic_stack_size (cfun->su->dynamic_stack_size)
#define current_function_pushed_stack_size (cfun->su->pushed_stack_size)
#define current_function_has_unbounded_dynamic_stack_size \
  (cfun->su->has_unbounded_dynamic_stack_size)
#define current_function_allocates_dynamic_stack_space    \
  (current_function_dynamic_stack_size != 0               \
   || current_function_has_unbounded_dynamic_stack_size)

/* This structure can save all the important global and static variables
   describing the status of the current function.  */

struct GTY(()) function {
  struct eh_status *eh;

  /* The control flow graph for this function.  */
  struct control_flow_graph *cfg;

  /* GIMPLE body for this function.  */
  gimple_seq gimple_body;

  /* SSA and dataflow information.  */
  struct gimple_df *gimple_df;

  /* The loops in this function.  */
  struct loops *x_current_loops;

  /* Filled by the GIMPLE and RTL FEs, pass to start compilation with.  */
  char *pass_startwith;

  /* The stack usage of this function.  */
  struct stack_usage *su;

  /* Value histograms attached to particular statements.  */
  htab_t GTY((skip)) value_histograms;

  /* For function.c.  */

  /* Points to the FUNCTION_DECL of this function.  */
  tree decl;

  /* A PARM_DECL that should contain the static chain for this function.
     It will be initialized at the beginning of the function.  */
  tree static_chain_decl;

  /* An expression that contains the non-local goto save area.  The first
     word is the saved frame pointer and the second is the saved stack
     pointer.  */
  tree nonlocal_goto_save_area;

  /* Vector of function local variables, functions, types and constants.  */
  vec<tree, va_gc> *local_decls;

  /* For md files.  */

  /* tm.h can use this to store whatever it likes.  */
  struct machine_function * GTY ((maybe_undef)) machine;

  /* Language-specific code can use this to store whatever it likes.  */
  struct language_function * language;

  /* Used types hash table.  */
  hash_set<tree> *GTY (()) used_types_hash;

  /* Dwarf2 Frame Description Entry, containing the Call Frame Instructions
     used for unwinding.  Only set when either dwarf2 unwinding or dwarf2
     debugging is enabled.  */
  struct dw_fde_node *fde;

  /* Last statement uid.  */
  int last_stmt_uid;

  /* Debug marker counter.  Count begin stmt markers.  We don't have
     to keep it exact, it's more of a rough estimate to enable us to
     decide whether they are too many to copy during inlining, or when
     expanding to RTL.  */
  int debug_marker_count;

  /* Function sequence number for profiling, debugging, etc.  */
  int funcdef_no;

  /* Line number of the start of the function for debugging purposes.  */
  location_t function_start_locus;

  /* Line number of the end of the function.  */
  location_t function_end_locus;

  /* Properties used by the pass manager.  */
  unsigned int curr_properties;
  unsigned int last_verified;

  /* Non-null if the function does something that would prevent it from
     being copied; this applies to both versioning and inlining.  Set to
     a string describing the reason for failure.  */
  const char * GTY((skip)) cannot_be_copied_reason;

  /* Last assigned dependence info clique.  */
  unsigned short last_clique;

  /* Collected bit flags.  */

  /* Number of units of general registers that need saving in stdarg
     function.  What unit is depends on the backend, either it is number
     of bytes, or it can be number of registers.  */
  unsigned int va_list_gpr_size : 8;

  /* Number of units of floating point registers that need saving in stdarg
     function.  */
  unsigned int va_list_fpr_size : 8;

  /* Nonzero if function being compiled can call setjmp.  */
  unsigned int calls_setjmp : 1;

  /* Nonzero if function being compiled can call alloca,
     either as a subroutine or builtin.  */
  unsigned int calls_alloca : 1;

  /* Nonzero if function being compiled receives nonlocal gotos
     from nested functions.  */
  unsigned int has_nonlocal_label : 1;

  /* Nonzero if function being compiled has a forced label
     placed into static storage.  */
  unsigned int has_forced_label_in_static : 1;

  /* Nonzero if we've set cannot_be_copied_reason.  I.e. if
     (cannot_be_copied_set && !cannot_be_copied_reason), the function
     can in fact be copied.  */
  unsigned int cannot_be_copied_set : 1;

  /* Nonzero if current function uses stdarg.h or equivalent.  */
  unsigned int stdarg : 1;

  unsigned int after_inlining : 1;
  unsigned int always_inline_functions_inlined : 1;

  /* Nonzero if function being compiled can throw synchronous non-call
     exceptions.  */
  unsigned int can_throw_non_call_exceptions : 1;

  /* Nonzero if instructions that may throw exceptions but don't otherwise
     contribute to the execution of the program can be deleted.  */
  unsigned int can_delete_dead_exceptions : 1;

  /* Fields below this point are not set for abstract functions; see
     allocate_struct_function.  */

  /* Nonzero if function being compiled needs to be given an address
     where the value should be stored.  */
  unsigned int returns_struct : 1;

  /* Nonzero if function being compiled needs to
     return the address of where it has put a structure value.  */
  unsigned int returns_pcc_struct : 1;

  /* Nonzero if this function has local DECL_HARD_REGISTER variables.
     In this case code motion has to be done more carefully.  */
  unsigned int has_local_explicit_reg_vars : 1;

  /* Nonzero if the current function is a thunk, i.e., a lightweight
     function implemented by the output_mi_thunk hook) that just
     adjusts one of its arguments and forwards to another
     function.  */
  unsigned int is_thunk : 1;

  /* Nonzero if the current function contains any loops with
     loop->force_vectorize set.  */
  unsigned int has_force_vectorize_loops : 1;

  /* Nonzero if the current function contains any loops with
     nonzero value in loop->simduid.  */
  unsigned int has_simduid_loops : 1;

  /* Nonzero when the tail call has been identified.  */
  unsigned int tail_call_marked : 1;

  /* Nonzero if the current function contains a #pragma GCC unroll.  */
  unsigned int has_unroll : 1;

  /* Set when the function was compiled with generation of debug
     (begin stmt, inline entry, ...) markers enabled.  */
  unsigned int debug_nonbind_markers : 1;
};

/* Add the decl D to the local_decls list of FUN.  */

void add_local_decl (struct function *fun, tree d);

#define FOR_EACH_LOCAL_DECL(FUN, I, D)		\
  FOR_EACH_VEC_SAFE_ELT_REVERSE ((FUN)->local_decls, I, D)

/* If va_list_[gf]pr_size is set to this, it means we don't know how
   many units need to be saved.  */
#define VA_LIST_MAX_GPR_SIZE	255
#define VA_LIST_MAX_FPR_SIZE	255

/* The function currently being compiled.  */
extern GTY(()) struct function *cfun;

/* In order to ensure that cfun is not set directly, we redefine it so
   that it is not an lvalue.  Rather than assign to cfun, use
   push_cfun or set_cfun.  */
#define cfun (cfun + 0)

/* Nonzero if we've already converted virtual regs to hard regs.  */
extern int virtuals_instantiated;

/* Nonzero if at least one trampoline has been created.  */
extern int trampolines_created;

struct GTY((for_user)) types_used_by_vars_entry {
  tree type;
  tree var_decl;
};

struct used_type_hasher : ggc_ptr_hash<types_used_by_vars_entry>
{
  static hashval_t hash (types_used_by_vars_entry *);
  static bool equal (types_used_by_vars_entry *, types_used_by_vars_entry *);
};

/* Hash table making the relationship between a global variable
   and the types it references in its initializer. The key of the
   entry is a referenced type, and the value is the DECL of the global
   variable. types_use_by_vars_do_hash and types_used_by_vars_eq below are
   the hash and equality functions to use for this hash table.  */
extern GTY(()) hash_table<used_type_hasher> *types_used_by_vars_hash;

void types_used_by_var_decl_insert (tree type, tree var_decl);

/* During parsing of a global variable, this vector contains the types
   referenced by the global variable.  */
extern GTY(()) vec<tree, va_gc> *types_used_by_cur_var_decl;


/* Return the loop tree of FN.  */

inline struct loops *
loops_for_fn (struct function *fn)
{
  return fn->x_current_loops;
}

/* Set the loop tree of FN to LOOPS.  */

inline void
set_loops_for_fn (struct function *fn, struct loops *loops)
{
  gcc_checking_assert (fn->x_current_loops == NULL || loops == NULL);
  fn->x_current_loops = loops;
}

/* For backward compatibility... eventually these should all go away.  */
#define current_function_funcdef_no (cfun->funcdef_no)

#define current_loops (cfun->x_current_loops)
#define dom_computed (cfun->cfg->x_dom_computed)
#define n_bbs_in_dom_tree (cfun->cfg->x_n_bbs_in_dom_tree)
#define VALUE_HISTOGRAMS(fun) (fun)->value_histograms

/* A pointer to a function to create target specific, per-function
   data structures.  */
extern struct machine_function * (*init_machine_status) (void);

/* Structure to record the size of a sequence of arguments
   as the sum of a tree-expression and a constant.  This structure is
   also used to store offsets from the stack, which might be negative,
   so the variable part must be ssizetype, not sizetype.  */

struct args_size
{
  poly_int64_pod constant;
  tree var;
};

/* Package up various arg related fields of struct args for
   locate_and_pad_parm.  */
struct locate_and_pad_arg_data
{
  /* Size of this argument on the stack, rounded up for any padding it
     gets.  If REG_PARM_STACK_SPACE is defined, then register parms are
     counted here, otherwise they aren't.  */
  struct args_size size;
  /* Offset of this argument from beginning of stack-args.  */
  struct args_size offset;
  /* Offset to the start of the stack slot.  Different from OFFSET
     if this arg pads downward.  */
  struct args_size slot_offset;
  /* The amount that the stack pointer needs to be adjusted to
     force alignment for the next argument.  */
  struct args_size alignment_pad;
  /* Which way we should pad this arg.  */
  pad_direction where_pad;
  /* slot_offset is at least this aligned.  */
  unsigned int boundary;
};

/* Add the value of the tree INC to the `struct args_size' TO.  */

#define ADD_PARM_SIZE(TO, INC)					\
do {								\
  tree inc = (INC);						\
  if (tree_fits_shwi_p (inc))					\
    (TO).constant += tree_to_shwi (inc);			\
  else if ((TO).var == 0)					\
    (TO).var = fold_convert (ssizetype, inc);			\
  else								\
    (TO).var = size_binop (PLUS_EXPR, (TO).var,			\
			   fold_convert (ssizetype, inc));	\
} while (0)

#define SUB_PARM_SIZE(TO, DEC)					\
do {								\
  tree dec = (DEC);						\
  if (tree_fits_shwi_p (dec))					\
    (TO).constant -= tree_to_shwi (dec);			\
  else if ((TO).var == 0)					\
    (TO).var = size_binop (MINUS_EXPR, ssize_int (0),		\
			   fold_convert (ssizetype, dec));	\
  else								\
    (TO).var = size_binop (MINUS_EXPR, (TO).var,		\
			   fold_convert (ssizetype, dec));	\
} while (0)

/* Convert the implicit sum in a `struct args_size' into a tree
   of type ssizetype.  */
#define ARGS_SIZE_TREE(SIZE)					\
((SIZE).var == 0 ? ssize_int ((SIZE).constant)			\
 : size_binop (PLUS_EXPR, fold_convert (ssizetype, (SIZE).var),	\
	       ssize_int ((SIZE).constant)))

/* Convert the implicit sum in a `struct args_size' into an rtx.  */
#define ARGS_SIZE_RTX(SIZE)					\
((SIZE).var == 0 ? gen_int_mode ((SIZE).constant, Pmode)	\
 : expand_normal (ARGS_SIZE_TREE (SIZE)))

#define ASLK_REDUCE_ALIGN 1
#define ASLK_RECORD_PAD 2

/* If pointers to member functions use the least significant bit to
   indicate whether a function is virtual, ensure a pointer
   to this function will have that bit clear.  */
#define MINIMUM_METHOD_BOUNDARY \
  ((TARGET_PTRMEMFUNC_VBIT_LOCATION == ptrmemfunc_vbit_in_pfn)	     \
   ? MAX (FUNCTION_BOUNDARY, 2 * BITS_PER_UNIT) : FUNCTION_BOUNDARY)

enum stack_clash_probes {
  NO_PROBE_NO_FRAME,
  NO_PROBE_SMALL_FRAME,
  PROBE_INLINE,
  PROBE_LOOP
};

extern void dump_stack_clash_frame_info (enum stack_clash_probes, bool);


extern void push_function_context (void);
extern void pop_function_context (void);

/* Save and restore status information for a nested function.  */
extern void free_after_parsing (struct function *);
extern void free_after_compilation (struct function *);

/* Return size needed for stack frame based on slots so far allocated.
   This size counts from zero.  It is not rounded to STACK_BOUNDARY;
   the caller may have to do that.  */
extern poly_int64 get_frame_size (void);

/* Issue an error message and return TRUE if frame OFFSET overflows in
   the signed target pointer arithmetics for function FUNC.  Otherwise
   return FALSE.  */
extern bool frame_offset_overflow (poly_int64, tree);

extern unsigned int spill_slot_alignment (machine_mode);

extern rtx assign_stack_local_1 (machine_mode, poly_int64, int, int);
extern rtx assign_stack_local (machine_mode, poly_int64, int);
extern rtx assign_stack_temp_for_type (machine_mode, poly_int64, tree);
extern rtx assign_stack_temp (machine_mode, poly_int64);
extern rtx assign_temp (tree, int, int);
extern void update_temp_slot_address (rtx, rtx);
extern void preserve_temp_slots (rtx);
extern void free_temp_slots (void);
extern void push_temp_slots (void);
extern void pop_temp_slots (void);
extern void init_temp_slots (void);
extern rtx get_hard_reg_initial_reg (rtx);
extern rtx get_hard_reg_initial_val (machine_mode, unsigned int);
extern rtx has_hard_reg_initial_val (machine_mode, unsigned int);

/* Called from gimple_expand_cfg.  */
extern unsigned int emit_initial_value_sets (void);

extern bool initial_value_entry (int i, rtx *, rtx *);
extern void instantiate_decl_rtl (rtx x);
extern int aggregate_value_p (const_tree, const_tree);
extern bool use_register_for_decl (const_tree);
extern gimple_seq gimplify_parameters (gimple_seq *);
extern void locate_and_pad_parm (machine_mode, tree, int, int, int,
				 tree, struct args_size *,
				 struct locate_and_pad_arg_data *);
extern void generate_setjmp_warnings (void);

/* Identify BLOCKs referenced by more than one NOTE_INSN_BLOCK_{BEG,END},
   and create duplicate blocks.  */
extern void reorder_blocks (void);
extern void clear_block_marks (tree);
extern tree blocks_nreverse (tree);
extern tree block_chainon (tree, tree);

/* Set BLOCK_NUMBER for all the blocks in FN.  */
extern void number_blocks (tree);

/* cfun shouldn't be set directly; use one of these functions instead.  */
extern void set_cfun (struct function *new_cfun, bool force = false);
extern void push_cfun (struct function *new_cfun);
extern void pop_cfun (void);

extern int get_next_funcdef_no (void);
extern int get_last_funcdef_no (void);
extern void allocate_struct_function (tree, bool);
extern void push_struct_function (tree fndecl);
extern void push_dummy_function (bool);
extern void pop_dummy_function (void);
extern void init_dummy_function_start (void);
extern void init_function_start (tree);
extern void stack_protect_epilogue (void);
extern void expand_function_start (tree);
extern void expand_dummy_function_end (void);

extern void thread_prologue_and_epilogue_insns (void);
extern void diddle_return_value (void (*)(rtx, void*), void*);
extern void clobber_return_register (void);
extern void expand_function_end (void);
extern rtx get_arg_pointer_save_area (void);
extern void maybe_copy_prologue_epilogue_insn (rtx, rtx);
extern int prologue_contains (const rtx_insn *);
extern int epilogue_contains (const rtx_insn *);
extern int prologue_epilogue_contains (const rtx_insn *);
extern void record_prologue_seq (rtx_insn *);
extern void record_epilogue_seq (rtx_insn *);
extern void emit_return_into_block (bool simple_p, basic_block bb);
extern void set_return_jump_label (rtx_insn *);
extern bool active_insn_between (rtx_insn *head, rtx_insn *tail);
extern vec<edge> convert_jumps_to_returns (basic_block last_bb, bool simple_p,
					   vec<edge> unconverted);
extern basic_block emit_return_for_exit (edge exit_fallthru_edge,
					 bool simple_p);
extern void reposition_prologue_and_epilogue_notes (void);

/* Returns the name of the current function.  */
extern const char *fndecl_name (tree);
extern const char *function_name (struct function *);
extern const char *current_function_name (void);

extern void used_types_insert (tree);

#endif  /* GCC_FUNCTION_H */
