/* If-conversion header file.
   Copyright (C) 2014-2018 Free Software Foundation, Inc.

   This file is part of GCC.

   GCC is free software; you can redistribute it and/or modify it
   under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 3, or (at your option)
   any later version.

   GCC is distributed in the hope that it will be useful, but WITHOUT
   ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
   or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
   License for more details.

   You should have received a copy of the GNU General Public License
   along with GCC; see the file COPYING3.  If not see
   <http://www.gnu.org/licenses/>.  */

#ifndef GCC_IFCVT_H
#define GCC_IFCVT_H

/* Structure to group all of the information to process IF-THEN and
   IF-THEN-ELSE blocks for the conditional execution support.  */

struct ce_if_block
{
  basic_block test_bb;			/* First test block.  */
  basic_block then_bb;			/* THEN block.  */
  basic_block else_bb;			/* ELSE block or NULL.  */
  basic_block join_bb;			/* Join THEN/ELSE blocks.  */
  basic_block last_test_bb;		/* Last bb to hold && or || tests.  */
  int num_multiple_test_blocks;		/* # of && and || basic blocks.  */
  int num_and_and_blocks;		/* # of && blocks.  */
  int num_or_or_blocks;			/* # of || blocks.  */
  int num_multiple_test_insns;		/* # of insns in && and || blocks.  */
  int and_and_p;			/* Complex test is &&.  */
  int num_then_insns;			/* # of insns in THEN block.  */
  int num_else_insns;			/* # of insns in ELSE block.  */
  int pass;				/* Pass number.  */
};

/* Used by noce_process_if_block to communicate with its subroutines.

   The subroutines know that A and B may be evaluated freely.  They
   know that X is a register.  They should insert new instructions
   before cond_earliest.  */

struct noce_if_info
{
  /* The basic blocks that make up the IF-THEN-{ELSE-,}JOIN block.  */
  basic_block test_bb, then_bb, else_bb, join_bb;

  /* The jump that ends TEST_BB.  */
  rtx_insn *jump;

  /* The jump condition.  */
  rtx cond;

  /* Reversed jump condition.  */
  rtx rev_cond;

  /* New insns should be inserted before this one.  */
  rtx_insn *cond_earliest;

  /* Insns in the THEN and ELSE block.  There is always just this
     one insns in those blocks.  The insns are single_set insns.
     If there was no ELSE block, INSN_B is the last insn before
     COND_EARLIEST, or NULL_RTX.  In the former case, the insn
     operands are still valid, as if INSN_B was moved down below
     the jump.  */
  rtx_insn *insn_a, *insn_b;

  /* The SET_SRC of INSN_A and INSN_B.  */
  rtx a, b;

  /* The SET_DEST of INSN_A.  */
  rtx x;

  /* The original set destination that the THEN and ELSE basic blocks finally
     write their result to.  */
  rtx orig_x;
  /* True if this if block is not canonical.  In the canonical form of
     if blocks, the THEN_BB is the block reached via the fallthru edge
     from TEST_BB.  For the noce transformations, we allow the symmetric
     form as well.  */
  bool then_else_reversed;

  /* True if the contents of then_bb and else_bb are a
     simple single set instruction.  */
  bool then_simple;
  bool else_simple;

  /* True if we're optimisizing the control block for speed, false if
     we're optimizing for size.  */
  bool speed_p;

  /* An estimate of the original costs.  When optimizing for size, this is the
     combined cost of COND, JUMP and the costs for THEN_BB and ELSE_BB.
     When optimizing for speed, we use the costs of COND plus the minimum of
     the costs for THEN_BB and ELSE_BB, as computed in the next field.  */
  unsigned int original_cost;

  /* Maximum permissible cost for the unconditional sequence we should
     generate to replace this branch.  */
  unsigned int max_seq_cost;

  /* The name of the noce transform that succeeded in if-converting
     this structure.  Used for debugging.  */
  const char *transform_name;
};

#endif /* GCC_IFCVT_H */
