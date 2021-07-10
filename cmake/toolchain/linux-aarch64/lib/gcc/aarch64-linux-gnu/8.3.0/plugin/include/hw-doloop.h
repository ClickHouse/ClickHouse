/* Code to analyze doloop loops in order for targets to perform late
   optimizations converting doloops to other forms of hardware loops.
   Copyright (C) 2011-2018 Free Software Foundation, Inc.

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

#ifndef GCC_HW_DOLOOP_H
#define GCC_HW_DOLOOP_H

/* We need to keep a vector of loops */
typedef struct hwloop_info_d *hwloop_info;

/* Information about a loop we have found (or are in the process of
   finding).  */
struct GTY (()) hwloop_info_d
{
  /* loop number, for dumps */
  int loop_no;

  /* Next loop in the graph. */
  hwloop_info next;

  /* Vector of blocks only within the loop, including those within
     inner loops.  */
  vec<basic_block> blocks;

  /* Same information in a bitmap.  */
  bitmap block_bitmap;

  /* Vector of inner loops within this loop.  Includes loops of every
     nesting level.  */
  vec<hwloop_info> loops;

  /* All edges that jump into the loop.  */
  vec<edge, va_gc> *incoming;

  /* The ports currently using this infrastructure can typically
     handle two cases: all incoming edges have the same destination
     block, or all incoming edges have the same source block.  These
     two members are set to the common source or destination we found,
     or NULL if different blocks were found.  If both are NULL the
     loop can't be optimized.  */
  basic_block incoming_src;
  basic_block incoming_dest;

  /* First block in the loop.  This is the one branched to by the loop_end
     insn.  */
  basic_block head;

  /* Last block in the loop (the one with the loop_end insn).  */
  basic_block tail;

  /* The successor block of the loop.  This is the one the loop_end insn
     falls into.  */
  basic_block successor;

  /* The last instruction in the tail.  */
  rtx_insn *last_insn;

  /* The loop_end insn.  */
  rtx_insn *loop_end;

  /* The iteration register.  */
  rtx iter_reg;

  /* The new label placed at the beginning of the loop. */
  rtx_insn *start_label;

  /* The new label placed at the end of the loop. */
  rtx end_label;

  /* The length of the loop.  */
  int length;

  /* The nesting depth of the loop.  Innermost loops are given a depth
     of 1.  Only successfully optimized doloops are counted; if an inner
     loop was marked as bad, it does not increase the depth of its parent
     loop.
     This value is valid when the target's optimize function is called.  */
  int depth;

  /* True if we can't optimize this loop.  */
  bool bad;

  /* True if we have visited this loop during the optimization phase.  */
  bool visited;

  /* The following values are collected before calling the target's optimize
     function and are not valid earlier.  */
  
  /* Record information about control flow: whether the loop has calls
     or asm statements, whether it has edges that jump out of the loop,
     or edges that jump within the loop.  */
  bool has_call;
  bool has_asm;
  bool jumps_within;
  bool jumps_outof;

  /* True if there is an instruction other than the doloop_end which uses the
     iteration register.  */
  bool iter_reg_used;
  /* True if the iteration register lives past the doloop instruction.  */
  bool iter_reg_used_outside;

  /* Hard registers set at any point in the loop, except for the loop counter
     register's set in the doloop_end instruction.  */
  HARD_REG_SET regs_set_in_loop;
};

/* A set of hooks to be defined by a target that wants to use the reorg_loops
   functionality.

   reorg_loops is intended to handle cases where special hardware loop
   setup instructions are required before the loop, for example to set
   up loop counter registers that are not exposed to the register
   allocator, or to inform the hardware about loop bounds.

   reorg_loops performs analysis to discover loop_end patterns created
   by the earlier loop-doloop pass, and sets up a hwloop_info
   structure for each such insn it finds.  It then tries to discover
   the basic blocks containing the loop by tracking the lifetime of
   the iteration register.

   If a valid loop can't be found, the FAIL function is called;
   otherwise the OPT function is called for each loop, visiting
   innermost loops first and ascending.  */
struct hw_doloop_hooks
{
  /* Examine INSN.  If it is a suitable doloop_end pattern, return the
     iteration register, which should be a single hard register.
     Otherwise, return NULL_RTX.  */
  rtx (*end_pattern_reg) (rtx_insn *insn);
  /* Optimize LOOP.  The target should perform any additional analysis
     (e.g. checking that the loop isn't too long), and then perform
     its transformations.  Return true if successful, false if the
     loop should be marked bad.  If it returns false, the FAIL
     function is called.  */
  bool (*opt) (hwloop_info loop);
  /* Handle a loop that was marked bad for any reason.  This could be
     used to split the doloop_end pattern.  */
  void (*fail) (hwloop_info loop);
};

extern void reorg_loops (bool, struct hw_doloop_hooks *);

#endif /* GCC_HW_DOLOOP_H */
