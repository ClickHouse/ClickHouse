/* This file contains definitions for the register renamer.
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

#ifndef GCC_REGRENAME_H
#define GCC_REGRENAME_H

/* We keep linked lists of DU_HEAD structures, each of which describes
   a chain of occurrences of a reg.  */
struct du_head
{
  /* The next chain.  */
  struct du_head *next_chain;
  /* The first and last elements of this chain.  */
  struct du_chain *first, *last;
  /* The chain that this chain is tied to.  */
  struct du_head *tied_chain;
  /* Describes the register being tracked.  */
  unsigned regno;
  int nregs;

  /* A unique id to be used as an index into the conflicts bitmaps.  */
  unsigned id;
  /* A bitmap to record conflicts with other chains.  */
  bitmap_head conflicts;
  /* Conflicts with untracked hard registers.  */
  HARD_REG_SET hard_conflicts;

  /* Nonzero if the chain crosses a call.  */
  unsigned int need_caller_save_reg:1;
  /* Nonzero if the register is used in a way that prevents renaming,
     such as the SET_DEST of a CALL_INSN or an asm operand that used
     to be a hard register.  */
  unsigned int cannot_rename:1;
  /* Nonzero if the chain has already been renamed.  */
  unsigned int renamed:1;

  /* Fields for use by target code.  */
  unsigned int target_data_1;
  unsigned int target_data_2;
};

typedef struct du_head *du_head_p;

/* This struct describes a single occurrence of a register.  */
struct du_chain
{
  /* Links to the next occurrence of the register.  */
  struct du_chain *next_use;

  /* The insn where the register appears.  */
  rtx_insn *insn;
  /* The location inside the insn.  */
  rtx *loc;
  /* The register class required by the insn at this location.  */
  ENUM_BITFIELD(reg_class) cl : 16;
};

/* This struct describes data gathered during regrename_analyze about
   a single operand of an insn.  */
struct operand_rr_info
{
  /* The number of chains recorded for this operand.  */
  short n_chains;
  bool failed;
  /* Holds either the chain for the operand itself, or for the registers in
     a memory operand.  */
  struct du_chain *chains[MAX_REGS_PER_ADDRESS];
  struct du_head *heads[MAX_REGS_PER_ADDRESS];
};

/* A struct to hold a vector of operand_rr_info structures describing the
   operands of an insn.  */
struct insn_rr_info
{
  operand_rr_info *op_info;
};


extern vec<insn_rr_info> insn_rr;

extern void regrename_init (bool);
extern void regrename_finish (void);
extern void regrename_analyze (bitmap);
extern du_head_p regrename_chain_from_id (unsigned int);
extern int find_rename_reg (du_head_p, enum reg_class, HARD_REG_SET *, int,
			    bool);
extern bool regrename_do_replace (du_head_p, int);
extern reg_class regrename_find_superclass (du_head_p, int *,
					    HARD_REG_SET *);

#endif
