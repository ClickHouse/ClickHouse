/* Local Register Allocator (LRA) intercommunication header file.
   Copyright (C) 2010-2018 Free Software Foundation, Inc.
   Contributed by Vladimir Makarov <vmakarov@redhat.com>.

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
along with GCC; see the file COPYING3.	If not see
<http://www.gnu.org/licenses/>.	 */

#ifndef GCC_LRA_INT_H
#define GCC_LRA_INT_H

#define lra_assert(c) gcc_checking_assert (c)

/* The parameter used to prevent infinite reloading for an insn.  Each
   insn operands might require a reload and, if it is a memory, its
   base and index registers might require a reload too.	 */
#define LRA_MAX_INSN_RELOADS (MAX_RECOG_OPERANDS * 3)

typedef struct lra_live_range *lra_live_range_t;

/* The structure describes program points where a given pseudo lives.
   The live ranges can be used to find conflicts with other pseudos.
   If the live ranges of two pseudos are intersected, the pseudos are
   in conflict.	 */
struct lra_live_range
{
  /* Pseudo regno whose live range is described by given
     structure.	 */
  int regno;
  /* Program point range.  */
  int start, finish;
  /* Next structure describing program points where the pseudo
     lives.  */
  lra_live_range_t next;
  /* Pointer to structures with the same start.	 */
  lra_live_range_t start_next;
};

typedef struct lra_copy *lra_copy_t;

/* Copy between pseudos which affects assigning hard registers.	 */
struct lra_copy
{
  /* True if regno1 is the destination of the copy.  */
  bool regno1_dest_p;
  /* Execution frequency of the copy.  */
  int freq;
  /* Pseudos connected by the copy.  REGNO1 < REGNO2.  */
  int regno1, regno2;
  /* Next copy with correspondingly REGNO1 and REGNO2.	*/
  lra_copy_t regno1_next, regno2_next;
};

/* Common info about a register (pseudo or hard register).  */
struct lra_reg
{
  /* Bitmap of UIDs of insns (including debug insns) referring the
     reg.  */
  bitmap_head insn_bitmap;
  /* The following fields are defined only for pseudos.	 */
  /* Hard registers with which the pseudo conflicts.  */
  HARD_REG_SET conflict_hard_regs;
  /* Call used registers with which the pseudo conflicts, taking into account
     the registers used by functions called from calls which cross the
     pseudo.  */
  HARD_REG_SET actual_call_used_reg_set;
  /* We assign hard registers to reload pseudos which can occur in few
     places.  So two hard register preferences are enough for them.
     The following fields define the preferred hard registers.	If
     there are no such hard registers the first field value is
     negative.	If there is only one preferred hard register, the 2nd
     field is negative.	 */
  int preferred_hard_regno1, preferred_hard_regno2;
  /* Profits to use the corresponding preferred hard registers.	 If
     the both hard registers defined, the first hard register has not
     less profit than the second one.  */
  int preferred_hard_regno_profit1, preferred_hard_regno_profit2;
#ifdef STACK_REGS
  /* True if the pseudo should not be assigned to a stack register.  */
  bool no_stack_p;
#endif
  /* True if the pseudo crosses a call.	 It is setup in lra-lives.c
     and used to check that the pseudo crossing a call did not get a
     call used hard register.  */
  bool call_p;
  /* Number of references and execution frequencies of the register in
     *non-debug* insns.	 */
  int nrefs, freq;
  int last_reload;
  /* rtx used to undo the inheritance.  It can be non-null only
     between subsequent inheritance and undo inheritance passes.  */
  rtx restore_rtx;
  /* Value holding by register.	 If the pseudos have the same value
     they do not conflict.  */
  int val;
  /* Offset from relative eliminate register to pesudo reg.  */
  poly_int64 offset;
  /* These members are set up in lra-lives.c and updated in
     lra-coalesce.c.  */
  /* The biggest size mode in which each pseudo reg is referred in
     whole function (possibly via subreg).  */
  machine_mode biggest_mode;
  /* Live ranges of the pseudo.	 */
  lra_live_range_t live_ranges;
  /* This member is set up in lra-lives.c for subsequent
     assignments.  */
  lra_copy_t copies;
};

/* References to the common info about each register.  */
extern struct lra_reg *lra_reg_info;

extern HARD_REG_SET hard_regs_spilled_into;

/* Static info about each insn operand (common for all insns with the
   same ICODE).	 Warning: if the structure definition is changed, the
   initializer for debug_operand_data in lra.c should be changed
   too.	 */
struct lra_operand_data
{
  /* The machine description constraint string of the operand.	*/
  const char *constraint;
  /* Alternatives for which early_clobber can be true.  */
  alternative_mask early_clobber_alts;
  /* It is taken only from machine description (which is different
     from recog_data.operand_mode) and can be of VOIDmode.  */
  ENUM_BITFIELD(machine_mode) mode : 16;
  /* The type of the operand (in/out/inout).  */
  ENUM_BITFIELD (op_type) type : 8;
  /* Through if accessed through STRICT_LOW.  */
  unsigned int strict_low : 1;
  /* True if the operand is an operator.  */
  unsigned int is_operator : 1;
  /* True if there is an early clobber alternative for this operand.
     This field is set up every time when corresponding
     operand_alternative in lra_static_insn_data is set up.  */
  unsigned int early_clobber : 1;
  /* True if the operand is an address.  */
  unsigned int is_address : 1;
};

/* Info about register occurrence in an insn.  */
struct lra_insn_reg
{
  /* Alternatives for which early_clobber can be true.  */
  alternative_mask early_clobber_alts;
  /* The biggest mode through which the insn refers to the register
     occurrence (remember the register can be accessed through a
     subreg in the insn).  */
  ENUM_BITFIELD(machine_mode) biggest_mode : 16;
  /* The type of the corresponding operand which is the register.  */
  ENUM_BITFIELD (op_type) type : 8;
  /* True if the reg is accessed through a subreg and the subreg is
     just a part of the register.  */
  unsigned int subreg_p : 1;
  /* True if there is an early clobber alternative for this
     operand.  */
  unsigned int early_clobber : 1;
  /* The corresponding regno of the register.  */
  int regno;
  /* Next reg info of the same insn.  */
  struct lra_insn_reg *next;
};

/* Static part (common info for insns with the same ICODE) of LRA
   internal insn info. It exists in at most one exemplar for each
   non-negative ICODE. There is only one exception. Each asm insn has
   own structure.  Warning: if the structure definition is changed,
   the initializer for debug_insn_static_data in lra.c should be
   changed too.  */
struct lra_static_insn_data
{
  /* Static info about each insn operand.  */
  struct lra_operand_data *operand;
  /* Each duplication refers to the number of the corresponding
     operand which is duplicated.  */
  int *dup_num;
  /* The number of an operand marked as commutative, -1 otherwise.  */
  int commutative;
  /* Number of operands, duplications, and alternatives of the
     insn.  */
  char n_operands;
  char n_dups;
  char n_alternatives;
  /* Insns in machine description (or clobbers in asm) may contain
     explicit hard regs which are not operands.	 The following list
     describes such hard registers.  */
  struct lra_insn_reg *hard_regs;
  /* Array [n_alternatives][n_operand] of static constraint info for
     given operand in given alternative.  This info can be changed if
     the target reg info is changed.  */
  const struct operand_alternative *operand_alternative;
};

/* Negative insn alternative numbers used for special cases.  */
#define LRA_UNKNOWN_ALT -1
#define LRA_NON_CLOBBERED_ALT -2

/* LRA internal info about an insn (LRA internal insn
   representation).  */
struct lra_insn_recog_data
{
  /* The insn code.  */
  int icode;
  /* The alternative should be used for the insn, LRA_UNKNOWN_ALT if
     unknown, or we should assume any alternative, or the insn is a
     debug insn.  LRA_NON_CLOBBERED_ALT means ignoring any earlier
     clobbers for the insn.  */
  int used_insn_alternative;
  /* SP offset before the insn relative to one at the func start.  */
  poly_int64 sp_offset;
  /* The insn itself.  */
  rtx_insn *insn;
  /* Common data for insns with the same ICODE.  Asm insns (their
     ICODE is negative) do not share such structures.  */
  struct lra_static_insn_data *insn_static_data;
  /* Two arrays of size correspondingly equal to the operand and the
     duplication numbers: */
  rtx **operand_loc; /* The operand locations, NULL if no operands.  */
  rtx **dup_loc; /* The dup locations, NULL if no dups.	 */
  /* Number of hard registers implicitly used/clobbered in given call
     insn.  The value can be NULL or points to array of the hard
     register numbers ending with a negative value.  To differ
     clobbered and used hard regs, clobbered hard regs are incremented
     by FIRST_PSEUDO_REGISTER.  */
  int *arg_hard_regs;
  /* Cached value of get_preferred_alternatives.  */
  alternative_mask preferred_alternatives;
  /* The following member value is always NULL for a debug insn.  */
  struct lra_insn_reg *regs;
};

typedef struct lra_insn_recog_data *lra_insn_recog_data_t;

/* Whether the clobber is used temporary in LRA.  */
#define LRA_TEMP_CLOBBER_P(x) \
  (RTL_FLAG_CHECK1 ("TEMP_CLOBBER_P", (x), CLOBBER)->unchanging)

/* Cost factor for each additional reload and maximal cost reject for
   insn reloads.  One might ask about such strange numbers.  Their
   values occurred historically from former reload pass.  */
#define LRA_LOSER_COST_FACTOR 6
#define LRA_MAX_REJECT 600

/* Maximum allowed number of assignment pass iterations after the
   latest spill pass when any former reload pseudo was spilled.  It is
   for preventing LRA cycling in a bug case.  */
#define LRA_MAX_ASSIGNMENT_ITERATION_NUMBER 30

/* The maximal number of inheritance/split passes in LRA.  It should
   be more 1 in order to perform caller saves transformations and much
   less MAX_CONSTRAINT_ITERATION_NUMBER to prevent LRA to do as many
   as permitted constraint passes in some complicated cases.  The
   first inheritance/split pass has a biggest impact on generated code
   quality.  Each subsequent affects generated code in less degree.
   For example, the 3rd pass does not change generated SPEC2000 code
   at all on x86-64.  */
#define LRA_MAX_INHERITANCE_PASSES 2

#if LRA_MAX_INHERITANCE_PASSES <= 0 \
    || LRA_MAX_INHERITANCE_PASSES >= LRA_MAX_ASSIGNMENT_ITERATION_NUMBER - 8
#error wrong LRA_MAX_INHERITANCE_PASSES value
#endif

/* Analogous macro to the above one but for rematerialization.  */
#define LRA_MAX_REMATERIALIZATION_PASSES 2

#if LRA_MAX_REMATERIALIZATION_PASSES <= 0 \
    || LRA_MAX_REMATERIALIZATION_PASSES >= LRA_MAX_ASSIGNMENT_ITERATION_NUMBER - 8
#error wrong LRA_MAX_REMATERIALIZATION_PASSES value
#endif

/* lra.c: */

extern FILE *lra_dump_file;

extern bool lra_reg_spill_p;

extern HARD_REG_SET lra_no_alloc_regs;

extern int lra_insn_recog_data_len;
extern lra_insn_recog_data_t *lra_insn_recog_data;

extern int lra_curr_reload_num;

extern void lra_dump_bitmap_with_title (const char *, bitmap, int);
extern hashval_t lra_rtx_hash (rtx x);
extern void lra_push_insn (rtx_insn *);
extern void lra_push_insn_by_uid (unsigned int);
extern void lra_push_insn_and_update_insn_regno_info (rtx_insn *);
extern rtx_insn *lra_pop_insn (void);
extern unsigned int lra_insn_stack_length (void);

extern rtx lra_create_new_reg_with_unique_value (machine_mode, rtx,
						 enum reg_class, const char *);
extern void lra_set_regno_unique_value (int);
extern void lra_invalidate_insn_data (rtx_insn *);
extern void lra_set_insn_deleted (rtx_insn *);
extern void lra_delete_dead_insn (rtx_insn *);
extern void lra_emit_add (rtx, rtx, rtx);
extern void lra_emit_move (rtx, rtx);
extern void lra_update_dups (lra_insn_recog_data_t, signed char *);

extern void lra_process_new_insns (rtx_insn *, rtx_insn *, rtx_insn *,
				   const char *);

extern bool lra_substitute_pseudo (rtx *, int, rtx, bool, bool);
extern bool lra_substitute_pseudo_within_insn (rtx_insn *, int, rtx, bool);

extern lra_insn_recog_data_t lra_set_insn_recog_data (rtx_insn *);
extern lra_insn_recog_data_t lra_update_insn_recog_data (rtx_insn *);
extern void lra_set_used_insn_alternative (rtx_insn *, int);
extern void lra_set_used_insn_alternative_by_uid (int, int);

extern void lra_invalidate_insn_regno_info (rtx_insn *);
extern void lra_update_insn_regno_info (rtx_insn *);
extern struct lra_insn_reg *lra_get_insn_regs (int);

extern void lra_free_copies (void);
extern void lra_create_copy (int, int, int);
extern lra_copy_t lra_get_copy (int);
extern bool lra_former_scratch_p (int);
extern bool lra_former_scratch_operand_p (rtx_insn *, int);
extern void lra_register_new_scratch_op (rtx_insn *, int);

extern int lra_new_regno_start;
extern int lra_constraint_new_regno_start;
extern int lra_bad_spill_regno_start;
extern bitmap_head lra_inheritance_pseudos;
extern bitmap_head lra_split_regs;
extern bitmap_head lra_subreg_reload_pseudos;
extern bitmap_head lra_optional_reload_pseudos;

/* lra-constraints.c: */

extern void lra_init_equiv (void);
extern int lra_constraint_offset (int, machine_mode);

extern int lra_constraint_iter;
extern bool lra_risky_transformations_p;
extern int lra_inheritance_iter;
extern int lra_undo_inheritance_iter;
extern bool lra_constrain_insn (rtx_insn *);
extern bool lra_constraints (bool);
extern void lra_constraints_init (void);
extern void lra_constraints_finish (void);
extern bool spill_hard_reg_in_range (int, enum reg_class, rtx_insn *, rtx_insn *);
extern void lra_inheritance (void);
extern bool lra_undo_inheritance (void);

/* lra-lives.c: */

extern int lra_live_max_point;
extern int *lra_point_freq;

extern int lra_hard_reg_usage[FIRST_PSEUDO_REGISTER];

extern int lra_live_range_iter;
extern void lra_create_live_ranges (bool, bool);
extern lra_live_range_t lra_copy_live_range_list (lra_live_range_t);
extern lra_live_range_t lra_merge_live_ranges (lra_live_range_t,
					       lra_live_range_t);
extern bool lra_intersected_live_ranges_p (lra_live_range_t,
					   lra_live_range_t);
extern void lra_print_live_range_list (FILE *, lra_live_range_t);
extern void debug (lra_live_range &ref);
extern void debug (lra_live_range *ptr);
extern void lra_debug_live_range_list (lra_live_range_t);
extern void lra_debug_pseudo_live_ranges (int);
extern void lra_debug_live_ranges (void);
extern void lra_clear_live_ranges (void);
extern void lra_live_ranges_init (void);
extern void lra_live_ranges_finish (void);
extern void lra_setup_reload_pseudo_preferenced_hard_reg (int, int, int);

/* lra-assigns.c: */

extern int lra_assignment_iter;
extern int lra_assignment_iter_after_spill;
extern void lra_setup_reg_renumber (int, int, bool);
extern bool lra_assign (bool &);
extern bool lra_split_hard_reg_for (void);

/* lra-coalesce.c: */

extern int lra_coalesce_iter;
extern bool lra_coalesce (void);

/* lra-spills.c:  */

extern bool lra_need_for_spills_p (void);
extern void lra_spill (void);
extern void lra_final_code_change (void);

/* lra-remat.c:  */

extern int lra_rematerialization_iter;
extern bool lra_remat (void);

/* lra-elimination.c: */

extern void lra_debug_elim_table (void);
extern int lra_get_elimination_hard_regno (int);
extern rtx lra_eliminate_regs_1 (rtx_insn *, rtx, machine_mode,
				 bool, bool, poly_int64, bool);
extern void eliminate_regs_in_insn (rtx_insn *insn, bool, bool, poly_int64);
extern void lra_eliminate (bool, bool);

extern void lra_eliminate_reg_if_possible (rtx *);



/* Return the hard register which given pseudo REGNO assigned to.
   Negative value means that the register got memory or we don't know
   allocation yet.  */
static inline int
lra_get_regno_hard_regno (int regno)
{
  resize_reg_info ();
  return reg_renumber[regno];
}

/* Change class of pseudo REGNO to NEW_CLASS.  Print info about it
   using TITLE.  Output a new line if NL_P.  */
static void inline
lra_change_class (int regno, enum reg_class new_class,
		  const char *title, bool nl_p)
{
  lra_assert (regno >= FIRST_PSEUDO_REGISTER);
  if (lra_dump_file != NULL)
    fprintf (lra_dump_file, "%s class %s for r%d",
	     title, reg_class_names[new_class], regno);
  setup_reg_classes (regno, new_class, NO_REGS, new_class);
  if (lra_dump_file != NULL && nl_p)
    fprintf (lra_dump_file, "\n");
}

/* Update insn operands which are duplication of NOP operand.  The
   insn is represented by its LRA internal representation ID.  */
static inline void
lra_update_dup (lra_insn_recog_data_t id, int nop)
{
  int i;
  struct lra_static_insn_data *static_id = id->insn_static_data;

  for (i = 0; i < static_id->n_dups; i++)
    if (static_id->dup_num[i] == nop)
      *id->dup_loc[i] = *id->operand_loc[nop];
}

/* Process operator duplications in insn with ID.  We do it after the
   operands processing.	 Generally speaking, we could do this probably
   simultaneously with operands processing because a common practice
   is to enumerate the operators after their operands.	*/
static inline void
lra_update_operator_dups (lra_insn_recog_data_t id)
{
  int i;
  struct lra_static_insn_data *static_id = id->insn_static_data;

  for (i = 0; i < static_id->n_dups; i++)
    {
      int ndup = static_id->dup_num[i];

      if (static_id->operand[ndup].is_operator)
	*id->dup_loc[i] = *id->operand_loc[ndup];
    }
}

/* Return info about INSN.  Set up the info if it is not done yet.  */
static inline lra_insn_recog_data_t
lra_get_insn_recog_data (rtx_insn *insn)
{
  lra_insn_recog_data_t data;
  unsigned int uid = INSN_UID (insn);

  if (lra_insn_recog_data_len > (int) uid
      && (data = lra_insn_recog_data[uid]) != NULL)
    {
      /* Check that we did not change insn without updating the insn
	 info.	*/
      lra_assert (data->insn == insn
		  && (INSN_CODE (insn) < 0
		      || data->icode == INSN_CODE (insn)));
      return data;
    }
  return lra_set_insn_recog_data (insn);
}

/* Update offset from pseudos with VAL by INCR.  */
static inline void
lra_update_reg_val_offset (int val, poly_int64 incr)
{
  int i;

  for (i = FIRST_PSEUDO_REGISTER; i < max_reg_num (); i++)
    {
      if (lra_reg_info[i].val == val)
        lra_reg_info[i].offset += incr;
    }
}

/* Return true if register content is equal to VAL with OFFSET.  */
static inline bool
lra_reg_val_equal_p (int regno, int val, poly_int64 offset)
{
  if (lra_reg_info[regno].val == val
      && known_eq (lra_reg_info[regno].offset, offset))
    return true;

  return false;
}

/* Assign value of register FROM to TO.  */
static inline void
lra_assign_reg_val (int from, int to)
{
  lra_reg_info[to].val = lra_reg_info[from].val;
  lra_reg_info[to].offset = lra_reg_info[from].offset;
}

#endif /* GCC_LRA_INT_H */
