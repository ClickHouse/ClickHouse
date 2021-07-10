/* Define regsets.
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

#ifndef GCC_REGSET_H
#define GCC_REGSET_H

/* TODO: regset is just a bitmap in its implementation.  The compiler does
   not consistently use one or the other, i.e. sometimes variables are
   declared as bitmap but they are actually regsets and regset accessors
   are used, and vice versa, or mixed (see e.g. spilled_regs in IRA).

   This should be cleaned up, either by just dropping the regset type, or
   by changing all bitmaps that are really regsets to the regset type.  For
   the latter option, a good start would be to change everything allocated
   on the reg_obstack to regset.  */


/* Head of register set linked list.  */
typedef bitmap_head regset_head;

/* A pointer to a regset_head.  */
typedef bitmap regset;

/* Allocate a register set with oballoc.  */
#define ALLOC_REG_SET(OBSTACK) BITMAP_ALLOC (OBSTACK)

/* Do any cleanup needed on a regset when it is no longer used.  */
#define FREE_REG_SET(REGSET) BITMAP_FREE (REGSET)

/* Initialize a new regset.  */
#define INIT_REG_SET(HEAD) bitmap_initialize (HEAD, &reg_obstack)

/* Clear a register set by freeing up the linked list.  */
#define CLEAR_REG_SET(HEAD) bitmap_clear (HEAD)

/* Copy a register set to another register set.  */
#define COPY_REG_SET(TO, FROM) bitmap_copy (TO, FROM)

/* Compare two register sets.  */
#define REG_SET_EQUAL_P(A, B) bitmap_equal_p (A, B)

/* `and' a register set with a second register set.  */
#define AND_REG_SET(TO, FROM) bitmap_and_into (TO, FROM)

/* `and' the complement of a register set with a register set.  */
#define AND_COMPL_REG_SET(TO, FROM) bitmap_and_compl_into (TO, FROM)

/* Inclusive or a register set with a second register set.  */
#define IOR_REG_SET(TO, FROM) bitmap_ior_into (TO, FROM)

/* Exclusive or a register set with a second register set.  */
#define XOR_REG_SET(TO, FROM) bitmap_xor_into (TO, FROM)

/* Or into TO the register set FROM1 `and'ed with the complement of FROM2.  */
#define IOR_AND_COMPL_REG_SET(TO, FROM1, FROM2) \
  bitmap_ior_and_compl_into (TO, FROM1, FROM2)

/* Clear a single register in a register set.  */
#define CLEAR_REGNO_REG_SET(HEAD, REG) bitmap_clear_bit (HEAD, REG)

/* Set a single register in a register set.  */
#define SET_REGNO_REG_SET(HEAD, REG) bitmap_set_bit (HEAD, REG)

/* Return true if a register is set in a register set.  */
#define REGNO_REG_SET_P(TO, REG) bitmap_bit_p (TO, REG)

/* Copy the hard registers in a register set to the hard register set.  */
extern void reg_set_to_hard_reg_set (HARD_REG_SET *, const_bitmap);
#define REG_SET_TO_HARD_REG_SET(TO, FROM)				\
do {									\
  CLEAR_HARD_REG_SET (TO);						\
  reg_set_to_hard_reg_set (&TO, FROM);					\
} while (0)

typedef bitmap_iterator reg_set_iterator;

/* Loop over all registers in REGSET, starting with MIN, setting REGNUM to the
   register number and executing CODE for all registers that are set.  */
#define EXECUTE_IF_SET_IN_REG_SET(REGSET, MIN, REGNUM, RSI)	\
  EXECUTE_IF_SET_IN_BITMAP (REGSET, MIN, REGNUM, RSI)

/* Loop over all registers in REGSET1 and REGSET2, starting with MIN, setting
   REGNUM to the register number and executing CODE for all registers that are
   set in the first regset and not set in the second.  */
#define EXECUTE_IF_AND_COMPL_IN_REG_SET(REGSET1, REGSET2, MIN, REGNUM, RSI) \
  EXECUTE_IF_AND_COMPL_IN_BITMAP (REGSET1, REGSET2, MIN, REGNUM, RSI)

/* Loop over all registers in REGSET1 and REGSET2, starting with MIN, setting
   REGNUM to the register number and executing CODE for all registers that are
   set in both regsets.  */
#define EXECUTE_IF_AND_IN_REG_SET(REGSET1, REGSET2, MIN, REGNUM, RSI) \
  EXECUTE_IF_AND_IN_BITMAP (REGSET1, REGSET2, MIN, REGNUM, RSI)	\

/* Same information as REGS_INVALIDATED_BY_CALL but in regset form to be used
   in dataflow more conveniently.  */

extern regset regs_invalidated_by_call_regset;

/* Same information as FIXED_REG_SET but in regset form.  */
extern regset fixed_reg_set_regset;

/* An obstack for regsets.  */
extern bitmap_obstack reg_obstack;

/* In df-core.c (which should use regset consistently instead of bitmap...)  */
extern void dump_regset (regset, FILE *);

#endif /* GCC_REGSET_H */
