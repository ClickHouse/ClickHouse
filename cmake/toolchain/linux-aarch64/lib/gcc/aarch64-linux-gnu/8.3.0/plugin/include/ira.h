/* Communication between the Integrated Register Allocator (IRA) and
   the rest of the compiler.
   Copyright (C) 2006-2018 Free Software Foundation, Inc.
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
along with GCC; see the file COPYING3.  If not see
<http://www.gnu.org/licenses/>.  */

#ifndef GCC_IRA_H
#define GCC_IRA_H

#include "emit-rtl.h"

/* True when we use LRA instead of reload pass for the current
   function.  */
extern bool ira_use_lra_p;

/* True if we have allocno conflicts.  It is false for non-optimized
   mode or when the conflict table is too big.  */
extern bool ira_conflicts_p;

struct target_ira
{
  /* Map: hard register number -> allocno class it belongs to.  If the
     corresponding class is NO_REGS, the hard register is not available
     for allocation.  */
  enum reg_class x_ira_hard_regno_allocno_class[FIRST_PSEUDO_REGISTER];

  /* Number of allocno classes.  Allocno classes are register classes
     which can be used for allocations of allocnos.  */
  int x_ira_allocno_classes_num;

  /* The array containing allocno classes.  Only first
     IRA_ALLOCNO_CLASSES_NUM elements are used for this.  */
  enum reg_class x_ira_allocno_classes[N_REG_CLASSES];

  /* Map of all register classes to corresponding allocno classes
     containing the given class.  If given class is not a subset of an
     allocno class, we translate it into the cheapest allocno class.  */
  enum reg_class x_ira_allocno_class_translate[N_REG_CLASSES];

  /* Number of pressure classes.  Pressure classes are register
     classes for which we calculate register pressure.  */
  int x_ira_pressure_classes_num;

  /* The array containing pressure classes.  Only first
     IRA_PRESSURE_CLASSES_NUM elements are used for this.  */
  enum reg_class x_ira_pressure_classes[N_REG_CLASSES];

  /* Map of all register classes to corresponding pressure classes
     containing the given class.  If given class is not a subset of an
     pressure class, we translate it into the cheapest pressure
     class.  */
  enum reg_class x_ira_pressure_class_translate[N_REG_CLASSES];

  /* Biggest pressure register class containing stack registers.
     NO_REGS if there are no stack registers.  */
  enum reg_class x_ira_stack_reg_pressure_class;

  /* Maps: register class x machine mode -> maximal/minimal number of
     hard registers of given class needed to store value of given
     mode.  */
  unsigned char x_ira_reg_class_max_nregs[N_REG_CLASSES][MAX_MACHINE_MODE];
  unsigned char x_ira_reg_class_min_nregs[N_REG_CLASSES][MAX_MACHINE_MODE];

  /* Array analogous to target hook TARGET_MEMORY_MOVE_COST.  */
  short x_ira_memory_move_cost[MAX_MACHINE_MODE][N_REG_CLASSES][2];

  /* Array of number of hard registers of given class which are
     available for the allocation.  The order is defined by the
     allocation order.  */
  short x_ira_class_hard_regs[N_REG_CLASSES][FIRST_PSEUDO_REGISTER];

  /* The number of elements of the above array for given register
     class.  */
  int x_ira_class_hard_regs_num[N_REG_CLASSES];

  /* Register class subset relation: TRUE if the first class is a subset
     of the second one considering only hard registers available for the
     allocation.  */
  int x_ira_class_subset_p[N_REG_CLASSES][N_REG_CLASSES];

  /* The biggest class inside of intersection of the two classes (that
     is calculated taking only hard registers available for allocation
     into account.  If the both classes contain no hard registers
     available for allocation, the value is calculated with taking all
     hard-registers including fixed ones into account.  */
  enum reg_class x_ira_reg_class_subset[N_REG_CLASSES][N_REG_CLASSES];

  /* True if the two classes (that is calculated taking only hard
     registers available for allocation into account; are
     intersected.  */
  bool x_ira_reg_classes_intersect_p[N_REG_CLASSES][N_REG_CLASSES];

  /* If class CL has a single allocatable register of mode M,
     index [CL][M] gives the number of that register, otherwise it is -1.  */
  short x_ira_class_singleton[N_REG_CLASSES][MAX_MACHINE_MODE];

  /* Function specific hard registers can not be used for the register
     allocation.  */
  HARD_REG_SET x_ira_no_alloc_regs;

  /* Array whose values are hard regset of hard registers available for
     the allocation of given register class whose targetm.hard_regno_mode_ok
     values for given mode are false.  */
  HARD_REG_SET x_ira_prohibited_class_mode_regs[N_REG_CLASSES][NUM_MACHINE_MODES];
};

extern struct target_ira default_target_ira;
#if SWITCHABLE_TARGET
extern struct target_ira *this_target_ira;
#else
#define this_target_ira (&default_target_ira)
#endif

#define ira_hard_regno_allocno_class \
  (this_target_ira->x_ira_hard_regno_allocno_class)
#define ira_allocno_classes_num \
  (this_target_ira->x_ira_allocno_classes_num)
#define ira_allocno_classes \
  (this_target_ira->x_ira_allocno_classes)
#define ira_allocno_class_translate \
  (this_target_ira->x_ira_allocno_class_translate)
#define ira_pressure_classes_num \
  (this_target_ira->x_ira_pressure_classes_num)
#define ira_pressure_classes \
  (this_target_ira->x_ira_pressure_classes)
#define ira_pressure_class_translate \
  (this_target_ira->x_ira_pressure_class_translate)
#define ira_stack_reg_pressure_class \
  (this_target_ira->x_ira_stack_reg_pressure_class)
#define ira_reg_class_max_nregs \
  (this_target_ira->x_ira_reg_class_max_nregs)
#define ira_reg_class_min_nregs \
  (this_target_ira->x_ira_reg_class_min_nregs)
#define ira_memory_move_cost \
  (this_target_ira->x_ira_memory_move_cost)
#define ira_class_hard_regs \
  (this_target_ira->x_ira_class_hard_regs)
#define ira_class_hard_regs_num \
  (this_target_ira->x_ira_class_hard_regs_num)
#define ira_class_subset_p \
  (this_target_ira->x_ira_class_subset_p)
#define ira_reg_class_subset \
  (this_target_ira->x_ira_reg_class_subset)
#define ira_reg_classes_intersect_p \
  (this_target_ira->x_ira_reg_classes_intersect_p)
#define ira_class_singleton \
  (this_target_ira->x_ira_class_singleton)
#define ira_no_alloc_regs \
  (this_target_ira->x_ira_no_alloc_regs)
#define ira_prohibited_class_mode_regs \
  (this_target_ira->x_ira_prohibited_class_mode_regs)

/* Major structure describing equivalence info for a pseudo.  */
struct ira_reg_equiv_s
{
  /* True if we can use this equivalence.  */
  bool defined_p;
  /* True if the usage of the equivalence is profitable.  */
  bool profitable_p;
  /* Equiv. memory, constant, invariant, and initializing insns of
     given pseudo-register or NULL_RTX.  */
  rtx memory;
  rtx constant;
  rtx invariant;
  /* Always NULL_RTX if defined_p is false.  */
  rtx_insn_list *init_insns;
};

/* The length of the following array.  */
extern int ira_reg_equiv_len;

/* Info about equiv. info for each register.  */
extern struct ira_reg_equiv_s *ira_reg_equiv;

extern void ira_init_once (void);
extern void ira_init (void);
extern void ira_setup_eliminable_regset (void);
extern rtx ira_eliminate_regs (rtx, machine_mode);
extern void ira_set_pseudo_classes (bool, FILE *);
extern void ira_expand_reg_equiv (void);
extern void ira_update_equiv_info_by_shuffle_insn (int, int, rtx_insn *);

extern void ira_sort_regnos_for_alter_reg (int *, int, machine_mode *);
extern void ira_mark_allocation_change (int);
extern void ira_mark_memory_move_deletion (int, int);
extern bool ira_reassign_pseudos (int *, int, HARD_REG_SET, HARD_REG_SET *,
				  HARD_REG_SET *, bitmap);
extern rtx ira_reuse_stack_slot (int, poly_uint64, poly_uint64);
extern void ira_mark_new_stack_slot (rtx, int, poly_uint64);
extern bool ira_better_spill_reload_regno_p (int *, int *, rtx, rtx, rtx_insn *);
extern bool ira_bad_reload_regno (int, rtx, rtx);

extern void ira_adjust_equiv_reg_cost (unsigned, int);

/* ira-costs.c */
extern void ira_costs_c_finalize (void);

/* Spilling static chain pseudo may result in generation of wrong
   non-local goto code using frame-pointer to address saved stack
   pointer value after restoring old frame pointer value.  The
   function returns TRUE if REGNO is such a static chain pseudo.  */
static inline bool
non_spilled_static_chain_regno_p (int regno)
{
  return (cfun->static_chain_decl && crtl->has_nonlocal_goto
	  && REG_EXPR (regno_reg_rtx[regno]) == cfun->static_chain_decl);
}

#endif /* GCC_IRA_H */
