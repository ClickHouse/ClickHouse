/* Instruction scheduling pass.  Log dumping infrastructure.
   Copyright (C) 2006-2018 Free Software Foundation, Inc.

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


#ifndef GCC_SEL_SCHED_DUMP_H
#define GCC_SEL_SCHED_DUMP_H



/* These values control the dumping of control flow graph to the .dot file.  */
enum sel_dump_cfg_def
  {
    /* Dump only current region.  */
    SEL_DUMP_CFG_CURRENT_REGION = 2,

    /* Dump note_list for this bb.  */
    SEL_DUMP_CFG_BB_NOTES_LIST = 4,

    /* Dump availability set from the bb header.  */
    SEL_DUMP_CFG_AV_SET = 8,

    /* Dump liveness set from the bb header.  */
    SEL_DUMP_CFG_LV_SET = 16,

    /* Dump insns of the given block.  */
    SEL_DUMP_CFG_BB_INSNS = 32,

    /* Show current fences when dumping cfg.  */
    SEL_DUMP_CFG_FENCES = 64,

    /* Show insn's seqnos when dumping cfg.  */
    SEL_DUMP_CFG_INSN_SEQNO = 128,

    /* Dump function name when dumping cfg.  */
    SEL_DUMP_CFG_FUNCTION_NAME = 256,

    /* Dump loop father number of the given bb.  */
    SEL_DUMP_CFG_BB_LOOP = 512,

    /* The default flags for cfg dumping.  */
    SEL_DUMP_CFG_FLAGS = (SEL_DUMP_CFG_CURRENT_REGION
                          | SEL_DUMP_CFG_BB_NOTES_LIST
                          | SEL_DUMP_CFG_AV_SET
                          | SEL_DUMP_CFG_LV_SET
                          | SEL_DUMP_CFG_BB_INSNS
                          | SEL_DUMP_CFG_FENCES
                          | SEL_DUMP_CFG_INSN_SEQNO
                          | SEL_DUMP_CFG_BB_LOOP)
  };

/* These values control the dumping of insns containing in expressions.  */
enum dump_insn_rtx_def
  {
    /* Dump insn's UID.  */
    DUMP_INSN_RTX_UID = 2,

    /* Dump insn's pattern.  */
    DUMP_INSN_RTX_PATTERN = 4,

    /* Dump insn's basic block number.  */
    DUMP_INSN_RTX_BBN = 8,

    /* Dump all of the above.  */
    DUMP_INSN_RTX_ALL = (DUMP_INSN_RTX_UID | DUMP_INSN_RTX_PATTERN
			 | DUMP_INSN_RTX_BBN)
  };

extern void dump_insn_rtx_1 (rtx, int);
extern void dump_insn_rtx (rtx);
extern void debug_insn_rtx (rtx);

/* These values control dumping of vinsns.  The meaning of different fields
   of a vinsn is explained in sel-sched-ir.h.  */
enum dump_vinsn_def
  {
    /* Dump the insn behind this vinsn.  */
    DUMP_VINSN_INSN_RTX = 2,

    /* Dump vinsn's type.  */
    DUMP_VINSN_TYPE = 4,

    /* Dump vinsn's count.  */
    DUMP_VINSN_COUNT = 8,

    /* Dump the cost (default latency) of the insn behind this vinsn.  */
    DUMP_VINSN_COST = 16,

    /* Dump all of the above.  */
    DUMP_VINSN_ALL = (DUMP_VINSN_INSN_RTX | DUMP_VINSN_TYPE | DUMP_VINSN_COUNT
		      | DUMP_VINSN_COST)
  };

extern void dump_vinsn_1 (vinsn_t, int);
extern void dump_vinsn (vinsn_t);
extern void debug_vinsn (vinsn_t);

extern void debug (vinsn_def &ref);
extern void debug (vinsn_def *ptr);
extern void debug_verbose (vinsn_def &ref);
extern void debug_verbose (vinsn_def *ptr);


/* These values control dumping of expressions.  The meaning of the fields
   is explained in sel-sched-ir.h.  */
enum dump_expr_def
  {
    /* Dump the vinsn behind this expression.  */
    DUMP_EXPR_VINSN = 2,

    /* Dump expression's SPEC parameter.  */
    DUMP_EXPR_SPEC = 4,

    /* Dump expression's priority.  */
    DUMP_EXPR_PRIORITY = 8,

    /* Dump the number of times this expression was scheduled.  */
    DUMP_EXPR_SCHED_TIMES = 16,

    /* Dump speculative status of the expression.  */
    DUMP_EXPR_SPEC_DONE_DS = 32,

    /* Dump the basic block number which originated this expression.  */
    DUMP_EXPR_ORIG_BB = 64,

    /* Dump expression's usefulness.  */
    DUMP_EXPR_USEFULNESS = 128,

    /* Dump all of the above.  */
    DUMP_EXPR_ALL = (DUMP_EXPR_VINSN | DUMP_EXPR_SPEC | DUMP_EXPR_PRIORITY
		     | DUMP_EXPR_SCHED_TIMES | DUMP_EXPR_SPEC_DONE_DS
		     | DUMP_EXPR_ORIG_BB | DUMP_EXPR_USEFULNESS)
  };

extern void dump_expr_1 (expr_t, int);
extern void dump_expr (expr_t);
extern void debug_expr (expr_t);

extern void debug (expr_def &ref);
extern void debug (expr_def *ptr);
extern void debug_verbose (expr_def &ref);
extern void debug_verbose (expr_def *ptr);


/* A enumeration for dumping flags of an insn.  The difference from
   dump_insn_rtx_def is that these fields are for insns in stream only.  */
enum dump_insn_def
{
  /* Dump expression of this insn.  */
  DUMP_INSN_EXPR = 2,

  /* Dump insn's seqno.  */
  DUMP_INSN_SEQNO = 4,

  /* Dump the cycle on which insn was scheduled.  */
  DUMP_INSN_SCHED_CYCLE = 8,

  /* Dump insn's UID.  */
  DUMP_INSN_UID = 16,

  /* Dump insn's pattern.  */
  DUMP_INSN_PATTERN = 32,

  /* Dump insn's basic block number.  */
  DUMP_INSN_BBN = 64,

  /* Dump all of the above.  */
  DUMP_INSN_ALL = (DUMP_INSN_EXPR | DUMP_INSN_SEQNO | DUMP_INSN_BBN
		   | DUMP_INSN_SCHED_CYCLE | DUMP_INSN_UID | DUMP_INSN_PATTERN)
};

extern void dump_insn_1 (insn_t, int);
extern void dump_insn (insn_t);
extern void debug_insn (insn_t);

/* When this flag is on, we are dumping to the .dot file.
   When it is off, we are dumping to log.  */
extern bool sched_dump_to_dot_p;


/* Functions from sel-sched-dump.c.  */
extern void sel_print (const char *fmt, ...) ATTRIBUTE_PRINTF_1;
extern const char * sel_print_insn (const rtx_insn *, int);
extern void free_sel_dump_data (void);

extern void block_start (void);
extern void block_finish (void);
extern int get_print_blocks_num (void);
extern void line_start (void);
extern void line_finish (void);

extern void sel_print_rtl (rtx x);
extern void dump_insn_1 (insn_t, int);
extern void dump_insn (insn_t);
extern void dump_insn_vector (rtx_vec_t);
extern void dump_expr (expr_t);
extern void dump_used_regs (bitmap);
extern void dump_av_set (av_set_t);
extern void dump_lv_set (regset);
extern void dump_blist (blist_t);
extern void dump_flist (flist_t);
extern void dump_hard_reg_set (const char *, HARD_REG_SET);
extern void sel_debug_cfg_1 (int);
extern void sel_debug_cfg (void);
extern void setup_dump_cfg_params (void);

/* Debug functions.  */
extern void debug_expr (expr_t);
extern void debug_av_set (av_set_t);
extern void debug_lv_set (regset);
extern void debug_ilist (ilist_t);
extern void debug_blist (blist_t);
extern void debug (vec<rtx> &ref);
extern void debug (vec<rtx> *ptr);
extern void debug_insn_vector (rtx_vec_t);
extern void debug_hard_reg_set (HARD_REG_SET);
extern rtx debug_mem_addr_value (rtx);
#endif
