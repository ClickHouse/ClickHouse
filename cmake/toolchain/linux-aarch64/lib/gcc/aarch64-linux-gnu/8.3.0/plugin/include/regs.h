/* Define per-register tables for data flow info and register allocation.
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

#ifndef GCC_REGS_H
#define GCC_REGS_H

#define REG_BYTES(R) mode_size[(int) GET_MODE (R)]

/* When you only have the mode of a pseudo register before it has a hard
   register chosen for it, this reports the size of each hard register
   a pseudo in such a mode would get allocated to.  A target may
   override this.  */

#ifndef REGMODE_NATURAL_SIZE
#define REGMODE_NATURAL_SIZE(MODE)	UNITS_PER_WORD
#endif

/* Maximum register number used in this function, plus one.  */

extern int max_regno;

/* REG_N_REFS and REG_N_SETS are initialized by a call to
   regstat_init_n_sets_and_refs from the current values of
   DF_REG_DEF_COUNT and DF_REG_USE_COUNT.  REG_N_REFS and REG_N_SETS
   should only be used if a pass need to change these values in some
   magical way or the pass needs to have accurate values for these
   and is not using incremental df scanning.

   At the end of a pass that uses REG_N_REFS and REG_N_SETS, a call
   should be made to regstat_free_n_sets_and_refs.

   Local alloc seems to play pretty loose with these values.
   REG_N_REFS is set to 0 if the register is used in an asm.
   Furthermore, local_alloc calls regclass to hack both REG_N_REFS and
   REG_N_SETS for three address insns.  Other passes seem to have
   other special values.  */



/* Structure to hold values for REG_N_SETS (i) and REG_N_REFS (i). */

struct regstat_n_sets_and_refs_t
{
  int sets;			/* # of times (REG n) is set */
  int refs;			/* # of times (REG n) is used or set */
};

extern struct regstat_n_sets_and_refs_t *regstat_n_sets_and_refs;

/* Indexed by n, gives number of times (REG n) is used or set.  */
static inline int
REG_N_REFS (int regno)
{
  return regstat_n_sets_and_refs[regno].refs;
}

/* Indexed by n, gives number of times (REG n) is used or set.  */
#define SET_REG_N_REFS(N,V) (regstat_n_sets_and_refs[N].refs = V)
#define INC_REG_N_REFS(N,V) (regstat_n_sets_and_refs[N].refs += V)

/* Indexed by n, gives number of times (REG n) is set.  */
static inline int
REG_N_SETS (int regno)
{
  return regstat_n_sets_and_refs[regno].sets;
}

/* Indexed by n, gives number of times (REG n) is set.  */
#define SET_REG_N_SETS(N,V) (regstat_n_sets_and_refs[N].sets = V)
#define INC_REG_N_SETS(N,V) (regstat_n_sets_and_refs[N].sets += V)

/* Given a REG, return TRUE if the reg is a PARM_DECL, FALSE otherwise.  */
extern bool reg_is_parm_p (rtx);

/* Functions defined in regstat.c.  */
extern void regstat_init_n_sets_and_refs (void);
extern void regstat_free_n_sets_and_refs (void);
extern void regstat_compute_ri (void);
extern void regstat_free_ri (void);
extern bitmap regstat_get_setjmp_crosses (void);
extern void regstat_compute_calls_crossed (void);
extern void regstat_free_calls_crossed (void);
extern void dump_reg_info (FILE *);

/* Register information indexed by register number.  This structure is
   initialized by calling regstat_compute_ri and is destroyed by
   calling regstat_free_ri.  */
struct reg_info_t
{
  int freq;			/* # estimated frequency (REG n) is used or set */
  int deaths;			/* # of times (REG n) dies */
  int calls_crossed;		/* # of calls (REG n) is live across */
  int basic_block;		/* # of basic blocks (REG n) is used in */
};

extern struct reg_info_t *reg_info_p;

/* The number allocated elements of reg_info_p.  */
extern size_t reg_info_p_size;

/* Estimate frequency of references to register N.  */

#define REG_FREQ(N) (reg_info_p[N].freq)

/* The weights for each insn varies from 0 to REG_FREQ_BASE.
   This constant does not need to be high, as in infrequently executed
   regions we want to count instructions equivalently to optimize for
   size instead of speed.  */
#define REG_FREQ_MAX 1000

/* Compute register frequency from the BB frequency.  When optimizing for size,
   or profile driven feedback is available and the function is never executed,
   frequency is always equivalent.  Otherwise rescale the basic block
   frequency.  */
#define REG_FREQ_FROM_BB(bb) (optimize_function_for_size_p (cfun)	      \
			      ? REG_FREQ_MAX				      \
			      : ((bb)->count.to_frequency (cfun)	      \
				* REG_FREQ_MAX / BB_FREQ_MAX)		      \
			      ? ((bb)->count.to_frequency (cfun)	      \
				 * REG_FREQ_MAX / BB_FREQ_MAX)		      \
			      : 1)

/* Indexed by N, gives number of insns in which register N dies.
   Note that if register N is live around loops, it can die
   in transitions between basic blocks, and that is not counted here.
   So this is only a reliable indicator of how many regions of life there are
   for registers that are contained in one basic block.  */

#define REG_N_DEATHS(N) (reg_info_p[N].deaths)

/* Get the number of consecutive words required to hold pseudo-reg N.  */

#define PSEUDO_REGNO_SIZE(N) \
  ((GET_MODE_SIZE (PSEUDO_REGNO_MODE (N)) + UNITS_PER_WORD - 1)		\
   / UNITS_PER_WORD)

/* Get the number of bytes required to hold pseudo-reg N.  */

#define PSEUDO_REGNO_BYTES(N) \
  GET_MODE_SIZE (PSEUDO_REGNO_MODE (N))

/* Get the machine mode of pseudo-reg N.  */

#define PSEUDO_REGNO_MODE(N) GET_MODE (regno_reg_rtx[N])

/* Indexed by N, gives number of CALL_INSNS across which (REG n) is live.  */

#define REG_N_CALLS_CROSSED(N)  (reg_info_p[N].calls_crossed)

/* Indexed by n, gives number of basic block that  (REG n) is used in.
   If the value is REG_BLOCK_GLOBAL (-1),
   it means (REG n) is used in more than one basic block.
   REG_BLOCK_UNKNOWN (0) means it hasn't been seen yet so we don't know.
   This information remains valid for the rest of the compilation
   of the current function; it is used to control register allocation.  */

#define REG_BLOCK_UNKNOWN 0
#define REG_BLOCK_GLOBAL -1

#define REG_BASIC_BLOCK(N) (reg_info_p[N].basic_block)

/* Vector of substitutions of register numbers,
   used to map pseudo regs into hardware regs.

   This can't be folded into reg_n_info without changing all of the
   machine dependent directories, since the reload functions
   in the machine dependent files access it.  */

extern short *reg_renumber;

/* Flag set by local-alloc or global-alloc if they decide to allocate
   something in a call-clobbered register.  */

extern int caller_save_needed;

/* Select a register mode required for caller save of hard regno REGNO.  */
#ifndef HARD_REGNO_CALLER_SAVE_MODE
#define HARD_REGNO_CALLER_SAVE_MODE(REGNO, NREGS, MODE) \
  choose_hard_reg_mode (REGNO, NREGS, false)
#endif

/* Target-dependent globals.  */
struct target_regs {
  /* For each starting hard register, the number of consecutive hard
     registers that a given machine mode occupies.  */
  unsigned char x_hard_regno_nregs[FIRST_PSEUDO_REGISTER][MAX_MACHINE_MODE];

  /* For each hard register, the widest mode object that it can contain.
     This will be a MODE_INT mode if the register can hold integers.  Otherwise
     it will be a MODE_FLOAT or a MODE_CC mode, whichever is valid for the
     register.  */
  machine_mode x_reg_raw_mode[FIRST_PSEUDO_REGISTER];

  /* Vector indexed by machine mode saying whether there are regs of
     that mode.  */
  bool x_have_regs_of_mode[MAX_MACHINE_MODE];

  /* 1 if the corresponding class contains a register of the given mode.  */
  char x_contains_reg_of_mode[N_REG_CLASSES][MAX_MACHINE_MODE];

  /* 1 if the corresponding class contains a register of the given mode
     which is not global and can therefore be allocated.  */
  char x_contains_allocatable_reg_of_mode[N_REG_CLASSES][MAX_MACHINE_MODE];

  /* Record for each mode whether we can move a register directly to or
     from an object of that mode in memory.  If we can't, we won't try
     to use that mode directly when accessing a field of that mode.  */
  char x_direct_load[NUM_MACHINE_MODES];
  char x_direct_store[NUM_MACHINE_MODES];

  /* Record for each mode whether we can float-extend from memory.  */
  bool x_float_extend_from_mem[NUM_MACHINE_MODES][NUM_MACHINE_MODES];
};

extern struct target_regs default_target_regs;
#if SWITCHABLE_TARGET
extern struct target_regs *this_target_regs;
#else
#define this_target_regs (&default_target_regs)
#endif
#define reg_raw_mode \
  (this_target_regs->x_reg_raw_mode)
#define have_regs_of_mode \
  (this_target_regs->x_have_regs_of_mode)
#define contains_reg_of_mode \
  (this_target_regs->x_contains_reg_of_mode)
#define contains_allocatable_reg_of_mode \
  (this_target_regs->x_contains_allocatable_reg_of_mode)
#define direct_load \
  (this_target_regs->x_direct_load)
#define direct_store \
  (this_target_regs->x_direct_store)
#define float_extend_from_mem \
  (this_target_regs->x_float_extend_from_mem)

/* Return the number of hard registers in (reg:MODE REGNO).  */

ALWAYS_INLINE unsigned char
hard_regno_nregs (unsigned int regno, machine_mode mode)
{
  return this_target_regs->x_hard_regno_nregs[regno][mode];
}

/* Return an exclusive upper bound on the registers occupied by hard
   register (reg:MODE REGNO).  */

static inline unsigned int
end_hard_regno (machine_mode mode, unsigned int regno)
{
  return regno + hard_regno_nregs (regno, mode);
}

/* Add to REGS all the registers required to store a value of mode MODE
   in register REGNO.  */

static inline void
add_to_hard_reg_set (HARD_REG_SET *regs, machine_mode mode,
		     unsigned int regno)
{
  unsigned int end_regno;

  end_regno = end_hard_regno (mode, regno);
  do
    SET_HARD_REG_BIT (*regs, regno);
  while (++regno < end_regno);
}

/* Likewise, but remove the registers.  */

static inline void
remove_from_hard_reg_set (HARD_REG_SET *regs, machine_mode mode,
			  unsigned int regno)
{
  unsigned int end_regno;

  end_regno = end_hard_regno (mode, regno);
  do
    CLEAR_HARD_REG_BIT (*regs, regno);
  while (++regno < end_regno);
}

/* Return true if REGS contains the whole of (reg:MODE REGNO).  */

static inline bool
in_hard_reg_set_p (const HARD_REG_SET regs, machine_mode mode,
		   unsigned int regno)
{
  unsigned int end_regno;

  gcc_assert (HARD_REGISTER_NUM_P (regno));
  
  if (!TEST_HARD_REG_BIT (regs, regno))
    return false;

  end_regno = end_hard_regno (mode, regno);

  if (!HARD_REGISTER_NUM_P (end_regno - 1))
    return false;

  while (++regno < end_regno)
    if (!TEST_HARD_REG_BIT (regs, regno))
      return false;

  return true;
}

/* Return true if (reg:MODE REGNO) includes an element of REGS.  */

static inline bool
overlaps_hard_reg_set_p (const HARD_REG_SET regs, machine_mode mode,
			 unsigned int regno)
{
  unsigned int end_regno;

  if (TEST_HARD_REG_BIT (regs, regno))
    return true;

  end_regno = end_hard_regno (mode, regno);
  while (++regno < end_regno)
    if (TEST_HARD_REG_BIT (regs, regno))
      return true;

  return false;
}

/* Like add_to_hard_reg_set, but use a REGNO/NREGS range instead of
   REGNO and MODE.  */

static inline void
add_range_to_hard_reg_set (HARD_REG_SET *regs, unsigned int regno,
			   int nregs)
{
  while (nregs-- > 0)
    SET_HARD_REG_BIT (*regs, regno + nregs);
}

/* Likewise, but remove the registers.  */

static inline void
remove_range_from_hard_reg_set (HARD_REG_SET *regs, unsigned int regno,
				int nregs)
{
  while (nregs-- > 0)
    CLEAR_HARD_REG_BIT (*regs, regno + nregs);
}

/* Like overlaps_hard_reg_set_p, but use a REGNO/NREGS range instead of
   REGNO and MODE.  */
static inline bool
range_overlaps_hard_reg_set_p (const HARD_REG_SET set, unsigned regno,
			       int nregs)
{
  while (nregs-- > 0)
    if (TEST_HARD_REG_BIT (set, regno + nregs))
      return true;
  return false;
}

/* Like in_hard_reg_set_p, but use a REGNO/NREGS range instead of
   REGNO and MODE.  */
static inline bool
range_in_hard_reg_set_p (const HARD_REG_SET set, unsigned regno, int nregs)
{
  while (nregs-- > 0)
    if (!TEST_HARD_REG_BIT (set, regno + nregs))
      return false;
  return true;
}

/* Get registers used by given function call instruction.  */
extern bool get_call_reg_set_usage (rtx_insn *insn, HARD_REG_SET *reg_set,
				    HARD_REG_SET default_set);

#endif /* GCC_REGS_H */
