/* Form lists of pseudo register references for autoinc optimization
   for GNU compiler.  This is part of flow optimization.
   Copyright (C) 1999-2018 Free Software Foundation, Inc.
   Originally contributed by Michael P. Hayes
             (m.hayes@elec.canterbury.ac.nz, mhayes@redhat.com)
   Major rewrite contributed by Danny Berlin (dberlin@dberlin.org)
             and Kenneth Zadeck (zadeck@naturalbridge.com).

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

#ifndef GCC_DF_H
#define GCC_DF_H

#include "regset.h"
#include "alloc-pool.h"
#include "timevar.h"

struct dataflow;
struct df_d;
struct df_problem;
struct df_link;
struct df_insn_info;
union df_ref_d;

/* Data flow problems.  All problems must have a unique id here.  */

/* Scanning is not really a dataflow problem, but it is useful to have
   the basic block functions in the vector so that things get done in
   a uniform manner.  The last four problems can be added or deleted
   at any time are always defined (though LIVE is always there at -O2
   or higher); the others are always there.  */
enum df_problem_id
  {
    DF_SCAN,
    DF_LR,                /* Live Registers backward. */
    DF_LIVE,              /* Live Registers & Uninitialized Registers */
    DF_RD,                /* Reaching Defs. */
    DF_CHAIN,             /* Def-Use and/or Use-Def Chains. */
    DF_WORD_LR,           /* Subreg tracking lr.  */
    DF_NOTE,              /* REG_DEAD and REG_UNUSED notes.  */
    DF_MD,                /* Multiple Definitions. */
    DF_MIR,               /* Must-initialized Registers.  */

    DF_LAST_PROBLEM_PLUS1
  };

/* Dataflow direction.  */
enum df_flow_dir
  {
    DF_NONE,
    DF_FORWARD,
    DF_BACKWARD
  };

/* Descriminator for the various df_ref types.  */
enum df_ref_class {DF_REF_BASE, DF_REF_ARTIFICIAL, DF_REF_REGULAR};

/* The first of these us a set of a registers.  The remaining three
   are all uses of a register (the mem_load and mem_store relate to
   how the register as an addressing operand).  */
enum df_ref_type {DF_REF_REG_DEF, DF_REF_REG_USE,
		  DF_REF_REG_MEM_LOAD, DF_REF_REG_MEM_STORE};

enum df_ref_flags
  {
    /* This flag is set if this ref occurs inside of a conditional
       execution instruction.  */
    DF_REF_CONDITIONAL = 1 << 0,

    /* If this flag is set for an artificial use or def, that ref
       logically happens at the top of the block.  If it is not set
       for an artificial use or def, that ref logically happens at the
       bottom of the block.  This is never set for regular refs.  */
    DF_REF_AT_TOP = 1 << 1,

    /* This flag is set if the use is inside a REG_EQUAL or REG_EQUIV
       note.  */
    DF_REF_IN_NOTE = 1 << 2,

    /* This bit is true if this ref can make regs_ever_live true for
       this regno.  */
    DF_HARD_REG_LIVE = 1 << 3,


    /* This flag is set if this ref is a partial use or def of the
       associated register.  */
    DF_REF_PARTIAL = 1 << 4,

    /* Read-modify-write refs generate both a use and a def and
       these are marked with this flag to show that they are not
       independent.  */
    DF_REF_READ_WRITE = 1 << 5,

    /* This flag is set if this ref, generally a def, may clobber the
       referenced register.  This is generally only set for hard
       registers that cross a call site.  With better information
       about calls, some of these could be changed in the future to
       DF_REF_MUST_CLOBBER.  */
    DF_REF_MAY_CLOBBER = 1 << 6,

    /* This flag is set if this ref, generally a def, is a real
       clobber. This is not currently set for registers live across a
       call because that clobbering may or may not happen.

       Most of the uses of this are with sets that have a
       GET_CODE(..)==CLOBBER.  Note that this is set even if the
       clobber is to a subreg.  So in order to tell if the clobber
       wipes out the entire register, it is necessary to also check
       the DF_REF_PARTIAL flag.  */
    DF_REF_MUST_CLOBBER = 1 << 7,


    /* If the ref has one of the following two flags set, then the
       struct df_ref can be cast to struct df_ref_extract to access
       the width and offset fields.  */

    /* This flag is set if the ref contains a SIGN_EXTRACT.  */
    DF_REF_SIGN_EXTRACT = 1 << 8,

    /* This flag is set if the ref contains a ZERO_EXTRACT.  */
    DF_REF_ZERO_EXTRACT = 1 << 9,

    /* This flag is set if the ref contains a STRICT_LOW_PART.  */
    DF_REF_STRICT_LOW_PART = 1 << 10,

    /* This flag is set if the ref contains a SUBREG.  */
    DF_REF_SUBREG = 1 << 11,


    /* This bit is true if this ref is part of a multiword hardreg.  */
    DF_REF_MW_HARDREG = 1 << 12,

    /* This flag is set if this ref is a usage of the stack pointer by
       a function call.  */
    DF_REF_CALL_STACK_USAGE = 1 << 13,

    /* This flag is used for verification of existing refs. */
    DF_REF_REG_MARKER = 1 << 14,

    /* This flag is set if this ref is inside a pre/post modify.  */
    DF_REF_PRE_POST_MODIFY = 1 << 15

  };

/* The possible ordering of refs within the df_ref_info.  */
enum df_ref_order
  {
    /* There is not table.  */
    DF_REF_ORDER_NO_TABLE,

    /* There is a table of refs but it is not (or no longer) organized
       by one of the following methods.  */
    DF_REF_ORDER_UNORDERED,
    DF_REF_ORDER_UNORDERED_WITH_NOTES,

    /* Organize the table by reg order, all of the refs with regno 0
       followed by all of the refs with regno 1 ... .  Within all of
       the regs for a particular regno, the refs are unordered.  */
    DF_REF_ORDER_BY_REG,

    /* For uses, the refs within eq notes may be added for
       DF_REF_ORDER_BY_REG.  */
    DF_REF_ORDER_BY_REG_WITH_NOTES,

    /* Organize the refs in insn order.  The insns are ordered within a
       block, and the blocks are ordered by FOR_ALL_BB_FN.  */
    DF_REF_ORDER_BY_INSN,

    /* For uses, the refs within eq notes may be added for
       DF_REF_ORDER_BY_INSN.  */
    DF_REF_ORDER_BY_INSN_WITH_NOTES
  };

/* Function prototypes added to df_problem instance.  */

/* Allocate the problem specific data.  */
typedef void (*df_alloc_function) (bitmap);

/* This function is called if the problem has global data that needs
   to be cleared when ever the set of blocks changes.  The bitmap
   contains the set of blocks that may require special attention.
   This call is only made if some of the blocks are going to change.
   If everything is to be deleted, the wholesale deletion mechanisms
   apply. */
typedef void (*df_reset_function) (bitmap);

/* Free the basic block info.  Called from the block reordering code
   to get rid of the blocks that have been squished down.   */
typedef void (*df_free_bb_function) (basic_block, void *);

/* Local compute function.  */
typedef void (*df_local_compute_function) (bitmap);

/* Init the solution specific data.  */
typedef void (*df_init_function) (bitmap);

/* Iterative dataflow function.  */
typedef void (*df_dataflow_function) (struct dataflow *, bitmap, int *, int);

/* Confluence operator for blocks with 0 out (or in) edges.  */
typedef void (*df_confluence_function_0) (basic_block);

/* Confluence operator for blocks with 1 or more out (or in) edges.
   Return true if BB input data has changed.  */
typedef bool (*df_confluence_function_n) (edge);

/* Transfer function for blocks. 
   Return true if BB output data has changed.  */
typedef bool (*df_transfer_function) (int);

/* Function to massage the information after the problem solving.  */
typedef void (*df_finalizer_function) (bitmap);

/* Function to free all of the problem specific datastructures.  */
typedef void (*df_free_function) (void);

/* Function to remove this problem from the stack of dataflow problems
   without effecting the other problems in the stack except for those
   that depend on this problem.  */
typedef void (*df_remove_problem_function) (void);

/* Function to dump basic block independent results to FILE.  */
typedef void (*df_dump_problem_function) (FILE *);

/* Function to dump top or bottom of basic block results to FILE.  */
typedef void (*df_dump_bb_problem_function) (basic_block, FILE *);

/* Function to dump before or after an insn to FILE.  */
typedef void (*df_dump_insn_problem_function) (const rtx_insn *, FILE *);

/* Function to dump top or bottom of basic block results to FILE.  */
typedef void (*df_verify_solution_start) (void);

/* Function to dump top or bottom of basic block results to FILE.  */
typedef void (*df_verify_solution_end) (void);

/* The static description of a dataflow problem to solve.  See above
   typedefs for doc for the function fields.  */

struct df_problem {
  /* The unique id of the problem.  This is used it index into
     df->defined_problems to make accessing the problem data easy.  */
  enum df_problem_id id;
  enum df_flow_dir dir;			/* Dataflow direction.  */
  df_alloc_function alloc_fun;
  df_reset_function reset_fun;
  df_free_bb_function free_bb_fun;
  df_local_compute_function local_compute_fun;
  df_init_function init_fun;
  df_dataflow_function dataflow_fun;
  df_confluence_function_0 con_fun_0;
  df_confluence_function_n con_fun_n;
  df_transfer_function trans_fun;
  df_finalizer_function finalize_fun;
  df_free_function free_fun;
  df_remove_problem_function remove_problem_fun;
  df_dump_problem_function dump_start_fun;
  df_dump_bb_problem_function dump_top_fun;
  df_dump_bb_problem_function dump_bottom_fun;
  df_dump_insn_problem_function dump_insn_top_fun;
  df_dump_insn_problem_function dump_insn_bottom_fun;
  df_verify_solution_start verify_start_fun;
  df_verify_solution_end verify_end_fun;
  const struct df_problem *dependent_problem;
  unsigned int block_info_elt_size;

  /* The timevar id associated with this pass.  */
  timevar_id_t tv_id;

  /* True if the df_set_blocks should null out the basic block info if
     this block drops out of df->blocks_to_analyze.  */
  bool free_blocks_on_set_blocks;
};


/* The specific instance of the problem to solve.  */
struct dataflow
{
  const struct df_problem *problem;     /* The problem to be solved.  */

  /* Array indexed by bb->index, that contains basic block problem and
     solution specific information.  */
  void *block_info;
  unsigned int block_info_size;

  /* The pool to allocate the block_info from. */
  object_allocator<df_link> *block_pool;

  /* The lr and live problems have their transfer functions recomputed
     only if necessary.  This is possible for them because, the
     problems are kept active for the entire backend and their
     transfer functions are indexed by the REGNO.  These are not
     defined for any other problem.  */
  bitmap out_of_date_transfer_functions;

  /* Other problem specific data that is not on a per basic block
     basis.  The structure is generally defined privately for the
     problem.  The exception being the scanning problem where it is
     fully public.  */
  void *problem_data;

  /* Local flags for some of the problems. */
  unsigned int local_flags;

  /* True if this problem of this instance has been initialized.  This
     is used by the dumpers to keep garbage out of the dumps if, for
     debugging a dump is produced before the first call to
     df_analyze after a new problem is added.  */
  bool computed;

  /* True if the something has changed which invalidates the dataflow
     solutions.  Note that this bit is always true for all problems except
     lr and live.  */
  bool solutions_dirty;

  /* If true, this pass is deleted by df_finish_pass.  This is never
     true for DF_SCAN and DF_LR.  It is true for DF_LIVE if optimize >
     1.  It is always true for the other problems.  */
  bool optional_p;
};


/* The set of multiword hardregs used as operands to this
   instruction. These are factored into individual uses and defs but
   the aggregate is still needed to service the REG_DEAD and
   REG_UNUSED notes.  */
struct df_mw_hardreg
{
  df_mw_hardreg *next;		/* Next entry for this instruction.  */
  rtx mw_reg;                   /* The multiword hardreg.  */
  /* These two bitfields are intentionally oversized, in the hope that
     accesses to 16-bit fields will usually be quicker.  */
  ENUM_BITFIELD(df_ref_type) type : 16;
				/* Used to see if the ref is read or write.  */
  int flags : 16;		/* Various df_ref_flags.  */
  unsigned int start_regno;     /* First word of the multi word subreg.  */
  unsigned int end_regno;       /* Last word of the multi word subreg.  */
  unsigned int mw_order;        /* Same as df_ref.ref_order.  */
};


/* Define a register reference structure.  One of these is allocated
    for every register reference (use or def).  Note some register
    references (e.g., post_inc, subreg) generate both a def and a use.  */
struct df_base_ref
{
  /* These three bitfields are intentionally oversized, in the hope that
     accesses to 8 and 16-bit fields will usually be quicker.  */
  ENUM_BITFIELD(df_ref_class) cl : 8;

  ENUM_BITFIELD(df_ref_type) type : 8;
				/* Type of ref.  */
  int flags : 16;		/* Various df_ref_flags.  */
  unsigned int regno;		/* The register number referenced.  */
  rtx reg;			/* The register referenced.  */
  union df_ref_d *next_loc;	/* Next ref for same insn or bb.  */
  struct df_link *chain;	/* Head of def-use, use-def.  */
  /* Pointer to the insn info of the containing instruction.  FIXME!
     Currently this is NULL for artificial refs but this will be used
     when FUDs are added.  */
  struct df_insn_info *insn_info;
  /* For each regno, there are three chains of refs, one for the uses,
     the eq_uses and the defs.  These chains go through the refs
     themselves rather than using an external structure.  */
  union df_ref_d *next_reg;     /* Next ref with same regno and type.  */
  union df_ref_d *prev_reg;     /* Prev ref with same regno and type.  */
  /* Location in the ref table.  This is only valid after a call to
     df_maybe_reorganize_[use,def]_refs which is an expensive operation.  */
  int id;
  /* The index at which the operand was scanned in the insn.  This is
     used to totally order the refs in an insn.  */
  unsigned int ref_order;
};


/* The three types of df_refs.  Note that the df_ref_extract is an
   extension of the df_regular_ref, not the df_base_ref.  */
struct df_artificial_ref
{
  struct df_base_ref base;

  /* Artificial refs do not have an insn, so to get the basic block,
     it must be explicitly here.  */
  basic_block bb;
};


struct df_regular_ref
{
  struct df_base_ref base;
  /* The loc is the address in the insn of the reg.  This is not
     defined for special registers, such as clobbers and stack
     pointers that are also associated with call insns and so those
     just use the base.  */
  rtx *loc;
};

/* Union of the different kinds of defs/uses placeholders.  */
union df_ref_d
{
  struct df_base_ref base;
  struct df_regular_ref regular_ref;
  struct df_artificial_ref artificial_ref;
};
typedef union df_ref_d *df_ref;


/* One of these structures is allocated for every insn.  */
struct df_insn_info
{
  rtx_insn *insn;	        /* The insn this info comes from.  */
  df_ref defs;	                /* Head of insn-def chain.  */
  df_ref uses;	                /* Head of insn-use chain.  */
  /* Head of insn-use chain for uses in REG_EQUAL/EQUIV notes.  */
  df_ref eq_uses;
  struct df_mw_hardreg *mw_hardregs;
  /* The logical uid of the insn in the basic block.  This is valid
     after any call to df_analyze but may rot after insns are added,
     deleted or moved. */
  int luid;
};

/* These links are used for ref-ref chains.  Currently only DEF-USE and
   USE-DEF chains can be built by DF.  */
struct df_link
{
  df_ref ref;
  struct df_link *next;
};


enum df_chain_flags
{
  /* Flags that control the building of chains.  */
  DF_DU_CHAIN      =  1, /* Build DU chains.  */
  DF_UD_CHAIN      =  2  /* Build UD chains.  */
};

enum df_scan_flags
{
  /* Flags for the SCAN problem.  */
  DF_SCAN_EMPTY_ENTRY_EXIT = 1  /* Don't define any registers in the entry
				   block; don't use any in the exit block.  */
};

enum df_changeable_flags
{
  /* Scanning flags.  */
  /* Flag to control the running of dce as a side effect of building LR.  */
  DF_LR_RUN_DCE           = 1 << 0, /* Run DCE.  */
  DF_NO_HARD_REGS         = 1 << 1, /* Skip hard registers in RD and CHAIN Building.  */

  DF_EQ_NOTES             = 1 << 2, /* Build chains with uses present in EQUIV/EQUAL notes. */
  DF_NO_REGS_EVER_LIVE    = 1 << 3, /* Do not compute the regs_ever_live.  */

  /* Cause df_insn_rescan df_notes_rescan and df_insn_delete, to
  return immediately.  This is used by passes that know how to update
  the scanning them selves.  */
  DF_NO_INSN_RESCAN       = 1 << 4,

  /* Cause df_insn_rescan df_notes_rescan and df_insn_delete, to
  return after marking the insn for later processing.  This allows all
  rescans to be batched.  */
  DF_DEFER_INSN_RESCAN    = 1 << 5,

  /* Compute the reaching defs problem as "live and reaching defs" (LR&RD).
     A DEF is reaching and live at insn I if DEF reaches I and REGNO(DEF)
     is in LR_IN of the basic block containing I.  */
  DF_RD_PRUNE_DEAD_DEFS   = 1 << 6,

  DF_VERIFY_SCHEDULED     = 1 << 7
};

/* Two of these structures are inline in df, one for the uses and one
   for the defs.  This structure is only contains the refs within the
   boundary of the df_set_blocks if that has been defined.  */
struct df_ref_info
{
  df_ref *refs;                 /* Ref table, indexed by id.  */
  unsigned int *begin;          /* First ref_index for this pseudo.  */
  unsigned int *count;          /* Count of refs for this pseudo.  */
  unsigned int refs_size;       /* Size of currently allocated refs table.  */

  /* Table_size is the number of elements in the refs table.  This
     will also be the width of the bitvectors in the rd and ru
     problems.  Total_size is the number of refs.  These will be the
     same if the focus has not been reduced by df_set_blocks.  If the
     focus has been reduced, table_size will be smaller since it only
     contains the refs in the set blocks.  */
  unsigned int table_size;
  unsigned int total_size;

  enum df_ref_order ref_order;
};

/* Three of these structures are allocated for every pseudo reg. One
   for the uses, one for the eq_uses and one for the defs.  */
struct df_reg_info
{
  /* Head of chain for refs of that type and regno.  */
  df_ref reg_chain;
  /* Number of refs in the chain.  */
  unsigned int n_refs;
};


/*----------------------------------------------------------------------------
   Problem data for the scanning dataflow problem.  Unlike the other
   dataflow problems, the problem data for scanning is fully exposed and
   used by owners of the problem.
----------------------------------------------------------------------------*/

struct df_d
{

  /* The set of problems to be solved is stored in two arrays.  In
     PROBLEMS_IN_ORDER, the problems are stored in the order that they
     are solved.  This is an internally dense array that may have
     nulls at the end of it.  In PROBLEMS_BY_INDEX, the problem is
     stored by the value in df_problem.id.  These are used to access
     the problem local data without having to search the first
     array.  */

  struct dataflow *problems_in_order[DF_LAST_PROBLEM_PLUS1];
  struct dataflow *problems_by_index[DF_LAST_PROBLEM_PLUS1];

  /* If not NULL, this subset of blocks of the program to be
     considered for analysis.  At certain times, this will contain all
     the blocks in the function so it cannot be used as an indicator
     of if we are analyzing a subset.  See analyze_subset.  */
  bitmap blocks_to_analyze;

  /* The following information is really the problem data for the
     scanning instance but it is used too often by the other problems
     to keep getting it from there.  */
  struct df_ref_info def_info;   /* Def info.  */
  struct df_ref_info use_info;   /* Use info.  */

  /* The following three arrays are allocated in parallel.   They contain
     the sets of refs of each type for each reg.  */
  struct df_reg_info **def_regs;       /* Def reg info.  */
  struct df_reg_info **use_regs;       /* Eq_use reg info.  */
  struct df_reg_info **eq_use_regs;    /* Eq_use info.  */
  unsigned int regs_size;       /* Size of currently allocated regs table.  */
  unsigned int regs_inited;     /* Number of regs with reg_infos allocated.  */


  struct df_insn_info **insns;   /* Insn table, indexed by insn UID.  */
  unsigned int insns_size;       /* Size of insn table.  */

  int num_problems_defined;

  bitmap_head hardware_regs_used;     /* The set of hardware registers used.  */
  /* The set of hard regs that are in the artificial uses at the end
     of a regular basic block.  */
  bitmap_head regular_block_artificial_uses;
  /* The set of hard regs that are in the artificial uses at the end
     of a basic block that has an EH pred.  */
  bitmap_head eh_block_artificial_uses;
  /* The set of hardware registers live on entry to the function.  */
  bitmap entry_block_defs;
  bitmap exit_block_uses;        /* The set of hardware registers used in exit block.  */

  /* Insns to delete, rescan or reprocess the notes at next
     df_rescan_all or df_process_deferred_rescans. */
  bitmap_head insns_to_delete;
  bitmap_head insns_to_rescan;
  bitmap_head insns_to_notes_rescan;
  int *postorder;                /* The current set of basic blocks
                                    in reverse postorder.  */
  vec<int> postorder_inverted;       /* The current set of basic blocks
                                    in reverse postorder of inverted CFG.  */
  int n_blocks;                  /* The number of blocks in reverse postorder.  */

  /* An array [FIRST_PSEUDO_REGISTER], indexed by regno, of the number
     of refs that qualify as being real hard regs uses.  Artificial
     uses and defs as well as refs in eq notes are ignored.  If the
     ref is a def, it cannot be a MAY_CLOBBER def.  If the ref is a
     use, it cannot be the emim_reg_set or be the frame or arg pointer
     register.  Uses in debug insns are ignored.

     IT IS NOT ACCEPTABLE TO MANUALLY CHANGE THIS ARRAY.  This array
     always reflects the actual number of refs in the insn stream that
     satisfy the above criteria.  */
  unsigned int *hard_regs_live_count;

  /* This counter provides a way to totally order refs without using
     addresses.  It is incremented whenever a ref is created.  */
  unsigned int ref_order;

  /* Problem specific control information.  This is a combination of
     enum df_changeable_flags values.  */
  int changeable_flags : 8;

  /* If this is true, then only a subset of the blocks of the program
     is considered to compute the solutions of dataflow problems.  */
  bool analyze_subset;

  /* True if someone added or deleted something from regs_ever_live so
     that the entry and exit blocks need be reprocessed.  */
  bool redo_entry_and_exit;
};

#define DF_SCAN_BB_INFO(BB) (df_scan_get_bb_info ((BB)->index))
#define DF_RD_BB_INFO(BB) (df_rd_get_bb_info ((BB)->index))
#define DF_LR_BB_INFO(BB) (df_lr_get_bb_info ((BB)->index))
#define DF_LIVE_BB_INFO(BB) (df_live_get_bb_info ((BB)->index))
#define DF_WORD_LR_BB_INFO(BB) (df_word_lr_get_bb_info ((BB)->index))
#define DF_MD_BB_INFO(BB) (df_md_get_bb_info ((BB)->index))
#define DF_MIR_BB_INFO(BB) (df_mir_get_bb_info ((BB)->index))

/* Most transformations that wish to use live register analysis will
   use these macros.  This info is the and of the lr and live sets.  */
#define DF_LIVE_IN(BB) (&DF_LIVE_BB_INFO (BB)->in)
#define DF_LIVE_OUT(BB) (&DF_LIVE_BB_INFO (BB)->out)

#define DF_MIR_IN(BB) (&DF_MIR_BB_INFO (BB)->in)
#define DF_MIR_OUT(BB) (&DF_MIR_BB_INFO (BB)->out)

/* These macros are used by passes that are not tolerant of
   uninitialized variables.  This intolerance should eventually
   be fixed.  */
#define DF_LR_IN(BB) (&DF_LR_BB_INFO (BB)->in)
#define DF_LR_OUT(BB) (&DF_LR_BB_INFO (BB)->out)

/* These macros are used by passes that are not tolerant of
   uninitialized variables.  This intolerance should eventually
   be fixed.  */
#define DF_WORD_LR_IN(BB) (&DF_WORD_LR_BB_INFO (BB)->in)
#define DF_WORD_LR_OUT(BB) (&DF_WORD_LR_BB_INFO (BB)->out)

/* Macros to access the elements within the ref structure.  */


#define DF_REF_REAL_REG(REF) (GET_CODE ((REF)->base.reg) == SUBREG \
				? SUBREG_REG ((REF)->base.reg) : ((REF)->base.reg))
#define DF_REF_REGNO(REF) ((REF)->base.regno)
#define DF_REF_REAL_LOC(REF) (GET_CODE (*((REF)->regular_ref.loc)) == SUBREG \
                               ? &SUBREG_REG (*((REF)->regular_ref.loc)) : ((REF)->regular_ref.loc))
#define DF_REF_REG(REF) ((REF)->base.reg)
#define DF_REF_LOC(REF) (DF_REF_CLASS (REF) == DF_REF_REGULAR ? \
			 (REF)->regular_ref.loc : NULL)
#define DF_REF_BB(REF) (DF_REF_IS_ARTIFICIAL (REF) \
			? (REF)->artificial_ref.bb \
			: BLOCK_FOR_INSN (DF_REF_INSN (REF)))
#define DF_REF_BBNO(REF) (DF_REF_BB (REF)->index)
#define DF_REF_INSN_INFO(REF) ((REF)->base.insn_info)
#define DF_REF_INSN(REF) ((REF)->base.insn_info->insn)
#define DF_REF_INSN_UID(REF) (INSN_UID (DF_REF_INSN(REF)))
#define DF_REF_CLASS(REF) ((REF)->base.cl)
#define DF_REF_TYPE(REF) ((REF)->base.type)
#define DF_REF_CHAIN(REF) ((REF)->base.chain)
#define DF_REF_ID(REF) ((REF)->base.id)
#define DF_REF_FLAGS(REF) ((REF)->base.flags)
#define DF_REF_FLAGS_IS_SET(REF, v) ((DF_REF_FLAGS (REF) & (v)) != 0)
#define DF_REF_FLAGS_SET(REF, v) (DF_REF_FLAGS (REF) |= (v))
#define DF_REF_FLAGS_CLEAR(REF, v) (DF_REF_FLAGS (REF) &= ~(v))
#define DF_REF_ORDER(REF) ((REF)->base.ref_order)
/* If DF_REF_IS_ARTIFICIAL () is true, this is not a real
   definition/use, but an artificial one created to model always live
   registers, eh uses, etc.  */
#define DF_REF_IS_ARTIFICIAL(REF) (DF_REF_CLASS (REF) == DF_REF_ARTIFICIAL)
#define DF_REF_REG_MARK(REF) (DF_REF_FLAGS_SET ((REF),DF_REF_REG_MARKER))
#define DF_REF_REG_UNMARK(REF) (DF_REF_FLAGS_CLEAR ((REF),DF_REF_REG_MARKER))
#define DF_REF_IS_REG_MARKED(REF) (DF_REF_FLAGS_IS_SET ((REF),DF_REF_REG_MARKER))
#define DF_REF_NEXT_LOC(REF) ((REF)->base.next_loc)
#define DF_REF_NEXT_REG(REF) ((REF)->base.next_reg)
#define DF_REF_PREV_REG(REF) ((REF)->base.prev_reg)
/* The following two macros may only be applied if one of
   DF_REF_SIGN_EXTRACT | DF_REF_ZERO_EXTRACT is true. */
#define DF_REF_EXTRACT_WIDTH(REF) ((REF)->extract_ref.width)
#define DF_REF_EXTRACT_OFFSET(REF) ((REF)->extract_ref.offset)
#define DF_REF_EXTRACT_MODE(REF) ((REF)->extract_ref.mode)

/* Macros to determine the reference type.  */
#define DF_REF_REG_DEF_P(REF) (DF_REF_TYPE (REF) == DF_REF_REG_DEF)
#define DF_REF_REG_USE_P(REF) (!DF_REF_REG_DEF_P (REF))
#define DF_REF_REG_MEM_STORE_P(REF) (DF_REF_TYPE (REF) == DF_REF_REG_MEM_STORE)
#define DF_REF_REG_MEM_LOAD_P(REF) (DF_REF_TYPE (REF) == DF_REF_REG_MEM_LOAD)
#define DF_REF_REG_MEM_P(REF) (DF_REF_REG_MEM_STORE_P (REF) \
                               || DF_REF_REG_MEM_LOAD_P (REF))

#define DF_MWS_REG_DEF_P(MREF) (DF_MWS_TYPE (MREF) == DF_REF_REG_DEF)
#define DF_MWS_REG_USE_P(MREF) (!DF_MWS_REG_DEF_P (MREF))
#define DF_MWS_NEXT(MREF) ((MREF)->next)
#define DF_MWS_TYPE(MREF) ((MREF)->type)

/* Macros to get the refs out of def_info or use_info refs table.  If
   the focus of the dataflow has been set to some subset of blocks
   with df_set_blocks, these macros will only find the uses and defs
   in that subset of blocks.

   These macros should be used with care.  The def macros are only
   usable after a call to df_maybe_reorganize_def_refs and the use
   macros are only usable after a call to
   df_maybe_reorganize_use_refs.  HOWEVER, BUILDING AND USING THESE
   ARRAYS ARE A CACHE LOCALITY KILLER.  */

#define DF_DEFS_TABLE_SIZE() (df->def_info.table_size)
#define DF_DEFS_GET(ID) (df->def_info.refs[(ID)])
#define DF_DEFS_SET(ID,VAL) (df->def_info.refs[(ID)]=(VAL))
#define DF_DEFS_COUNT(ID) (df->def_info.count[(ID)])
#define DF_DEFS_BEGIN(ID) (df->def_info.begin[(ID)])
#define DF_USES_TABLE_SIZE() (df->use_info.table_size)
#define DF_USES_GET(ID) (df->use_info.refs[(ID)])
#define DF_USES_SET(ID,VAL) (df->use_info.refs[(ID)]=(VAL))
#define DF_USES_COUNT(ID) (df->use_info.count[(ID)])
#define DF_USES_BEGIN(ID) (df->use_info.begin[(ID)])

/* Macros to access the register information from scan dataflow record.  */

#define DF_REG_SIZE(DF) (df->regs_inited)
#define DF_REG_DEF_GET(REG) (df->def_regs[(REG)])
#define DF_REG_DEF_CHAIN(REG) (df->def_regs[(REG)]->reg_chain)
#define DF_REG_DEF_COUNT(REG) (df->def_regs[(REG)]->n_refs)
#define DF_REG_USE_GET(REG) (df->use_regs[(REG)])
#define DF_REG_USE_CHAIN(REG) (df->use_regs[(REG)]->reg_chain)
#define DF_REG_USE_COUNT(REG) (df->use_regs[(REG)]->n_refs)
#define DF_REG_EQ_USE_GET(REG) (df->eq_use_regs[(REG)])
#define DF_REG_EQ_USE_CHAIN(REG) (df->eq_use_regs[(REG)]->reg_chain)
#define DF_REG_EQ_USE_COUNT(REG) (df->eq_use_regs[(REG)]->n_refs)

/* Macros to access the elements within the reg_info structure table.  */

#define DF_REGNO_FIRST_DEF(REGNUM) \
(DF_REG_DEF_GET(REGNUM) ? DF_REG_DEF_GET (REGNUM) : 0)
#define DF_REGNO_LAST_USE(REGNUM) \
(DF_REG_USE_GET(REGNUM) ? DF_REG_USE_GET (REGNUM) : 0)

/* Macros to access the elements within the insn_info structure table.  */

#define DF_INSN_SIZE() ((df)->insns_size)
#define DF_INSN_INFO_GET(INSN) (df->insns[(INSN_UID (INSN))])
#define DF_INSN_INFO_SET(INSN,VAL) (df->insns[(INSN_UID (INSN))]=(VAL))
#define DF_INSN_INFO_LUID(II) ((II)->luid)
#define DF_INSN_INFO_DEFS(II) ((II)->defs)
#define DF_INSN_INFO_USES(II) ((II)->uses)
#define DF_INSN_INFO_EQ_USES(II) ((II)->eq_uses)
#define DF_INSN_INFO_MWS(II) ((II)->mw_hardregs)

#define DF_INSN_LUID(INSN) (DF_INSN_INFO_LUID (DF_INSN_INFO_GET (INSN)))
#define DF_INSN_DEFS(INSN) (DF_INSN_INFO_DEFS (DF_INSN_INFO_GET (INSN)))
#define DF_INSN_USES(INSN) (DF_INSN_INFO_USES (DF_INSN_INFO_GET (INSN)))
#define DF_INSN_EQ_USES(INSN) (DF_INSN_INFO_EQ_USES (DF_INSN_INFO_GET (INSN)))

#define DF_INSN_UID_GET(UID) (df->insns[(UID)])
#define DF_INSN_UID_SET(UID,VAL) (df->insns[(UID)]=(VAL))
#define DF_INSN_UID_SAFE_GET(UID) (((unsigned)(UID) < DF_INSN_SIZE ())	\
                                     ? DF_INSN_UID_GET (UID) \
                                     : NULL)
#define DF_INSN_UID_LUID(INSN) (DF_INSN_UID_GET (INSN)->luid)
#define DF_INSN_UID_DEFS(INSN) (DF_INSN_UID_GET (INSN)->defs)
#define DF_INSN_UID_USES(INSN) (DF_INSN_UID_GET (INSN)->uses)
#define DF_INSN_UID_EQ_USES(INSN) (DF_INSN_UID_GET (INSN)->eq_uses)
#define DF_INSN_UID_MWS(INSN) (DF_INSN_UID_GET (INSN)->mw_hardregs)

#define FOR_EACH_INSN_INFO_DEF(ITER, INSN) \
  for (ITER = DF_INSN_INFO_DEFS (INSN); ITER; ITER = DF_REF_NEXT_LOC (ITER))

#define FOR_EACH_INSN_INFO_USE(ITER, INSN) \
  for (ITER = DF_INSN_INFO_USES (INSN); ITER; ITER = DF_REF_NEXT_LOC (ITER))

#define FOR_EACH_INSN_INFO_EQ_USE(ITER, INSN) \
  for (ITER = DF_INSN_INFO_EQ_USES (INSN); ITER; ITER = DF_REF_NEXT_LOC (ITER))

#define FOR_EACH_INSN_INFO_MW(ITER, INSN) \
  for (ITER = DF_INSN_INFO_MWS (INSN); ITER; ITER = DF_MWS_NEXT (ITER))

#define FOR_EACH_INSN_DEF(ITER, INSN) \
  FOR_EACH_INSN_INFO_DEF(ITER, DF_INSN_INFO_GET (INSN))

#define FOR_EACH_INSN_USE(ITER, INSN) \
  FOR_EACH_INSN_INFO_USE(ITER, DF_INSN_INFO_GET (INSN))

#define FOR_EACH_INSN_EQ_USE(ITER, INSN) \
  FOR_EACH_INSN_INFO_EQ_USE(ITER, DF_INSN_INFO_GET (INSN))

#define FOR_EACH_ARTIFICIAL_USE(ITER, BB_INDEX) \
  for (ITER = df_get_artificial_uses (BB_INDEX); ITER; \
       ITER = DF_REF_NEXT_LOC (ITER))

#define FOR_EACH_ARTIFICIAL_DEF(ITER, BB_INDEX) \
  for (ITER = df_get_artificial_defs (BB_INDEX); ITER; \
       ITER = DF_REF_NEXT_LOC (ITER))

/* An obstack for bitmap not related to specific dataflow problems.
   This obstack should e.g. be used for bitmaps with a short life time
   such as temporary bitmaps.  This obstack is declared in df-core.c.  */

extern bitmap_obstack df_bitmap_obstack;


/* One of these structures is allocated for every basic block.  */
struct df_scan_bb_info
{
  /* The entry block has many artificial defs and these are at the
     bottom of the block.

     Blocks that are targets of exception edges may have some
     artificial defs.  These are logically located at the top of the
     block.

     Blocks that are the targets of non-local goto's have the hard
     frame pointer defined at the top of the block.  */
  df_ref artificial_defs;

  /* Blocks that are targets of exception edges may have some
     artificial uses.  These are logically at the top of the block.

     Most blocks have artificial uses at the bottom of the block.  */
  df_ref artificial_uses;
};


/* Reaching definitions.  All bitmaps are indexed by the id field of
   the ref except sparse_kill which is indexed by regno.  For the
   LR&RD problem, the kill set is not complete: It does not contain
   DEFs killed because the set register has died in the LR set.  */
struct df_rd_bb_info
{
  /* Local sets to describe the basic blocks.   */
  bitmap_head kill;
  bitmap_head sparse_kill;
  bitmap_head gen;   /* The set of defs generated in this block.  */

  /* The results of the dataflow problem.  */
  bitmap_head in;    /* At the top of the block.  */
  bitmap_head out;   /* At the bottom of the block.  */
};


/* Multiple reaching definitions.  All bitmaps are referenced by the
   register number.  */

struct df_md_bb_info
{
  /* Local sets to describe the basic blocks.  */
  bitmap_head gen;    /* Partial/conditional definitions live at BB out.  */
  bitmap_head kill;   /* Other definitions that are live at BB out.  */
  bitmap_head init;   /* Definitions coming from dominance frontier edges. */

  /* The results of the dataflow problem.  */
  bitmap_head in;    /* Just before the block itself. */
  bitmap_head out;   /* At the bottom of the block.  */
};


/* Live registers, a backwards dataflow problem.  All bitmaps are
   referenced by the register number.  */

struct df_lr_bb_info
{
  /* Local sets to describe the basic blocks.  */
  bitmap_head def;   /* The set of registers set in this block
                        - except artificial defs at the top.  */
  bitmap_head use;   /* The set of registers used in this block.  */

  /* The results of the dataflow problem.  */
  bitmap_head in;    /* Just before the block itself. */
  bitmap_head out;   /* At the bottom of the block.  */
};


/* Uninitialized registers.  All bitmaps are referenced by the
   register number.  Anded results of the forwards and backward live
   info.  Note that the forwards live information is not available
   separately.  */
struct df_live_bb_info
{
  /* Local sets to describe the basic blocks.  */
  bitmap_head kill;  /* The set of registers unset in this block.  Calls,
		        for instance, unset registers.  */
  bitmap_head gen;   /* The set of registers set in this block.  */

  /* The results of the dataflow problem.  */
  bitmap_head in;    /* At the top of the block.  */
  bitmap_head out;   /* At the bottom of the block.  */
};


/* Live registers, a backwards dataflow problem.  These bitmaps are
   indexed by 2 * regno for each pseudo and have two entries for each
   pseudo.  Only pseudos that have a size of 2 * UNITS_PER_WORD are
   meaningfully tracked.  */

struct df_word_lr_bb_info
{
  /* Local sets to describe the basic blocks.  */
  bitmap_head def;   /* The set of registers set in this block
                        - except artificial defs at the top.  */
  bitmap_head use;   /* The set of registers used in this block.  */

  /* The results of the dataflow problem.  */
  bitmap_head in;    /* Just before the block itself. */
  bitmap_head out;   /* At the bottom of the block.  */
};

/* Must-initialized registers.  All bitmaps are referenced by the
   register number.  */
struct df_mir_bb_info
{
  /* Local sets to describe the basic blocks.  */
  bitmap_head kill;  /* The set of registers unset in this block.  Calls,
		        for instance, unset registers.  */
  bitmap_head gen;   /* The set of registers set in this block, excluding the
			ones killed later on in this block.  */

  /* The results of the dataflow problem.  */
  bitmap_head in;    /* At the top of the block.  */
  bitmap_head out;   /* At the bottom of the block.  */
};


/* This is used for debugging and for the dumpers to find the latest
   instance so that the df info can be added to the dumps.  This
   should not be used by regular code.  */
extern struct df_d *df;
#define df_scan    (df->problems_by_index[DF_SCAN])
#define df_rd      (df->problems_by_index[DF_RD])
#define df_lr      (df->problems_by_index[DF_LR])
#define df_live    (df->problems_by_index[DF_LIVE])
#define df_chain   (df->problems_by_index[DF_CHAIN])
#define df_word_lr (df->problems_by_index[DF_WORD_LR])
#define df_note    (df->problems_by_index[DF_NOTE])
#define df_md      (df->problems_by_index[DF_MD])
#define df_mir     (df->problems_by_index[DF_MIR])

/* This symbol turns on checking that each modification of the cfg has
  been identified to the appropriate df routines.  It is not part of
  verification per se because the check that the final solution has
  not changed covers this.  However, if the solution is not being
  properly recomputed because the cfg is being modified, adding in
  calls to df_check_cfg_clean can be used to find the source of that
  kind of problem.  */
#if 0
#define DF_DEBUG_CFG
#endif


/* Functions defined in df-core.c.  */

extern void df_add_problem (const struct df_problem *);
extern int df_set_flags (int);
extern int df_clear_flags (int);
extern void df_set_blocks (bitmap);
extern void df_remove_problem (struct dataflow *);
extern void df_finish_pass (bool);
extern void df_analyze_problem (struct dataflow *, bitmap, int *, int);
extern void df_analyze ();
extern void df_analyze_loop (struct loop *);
extern int df_get_n_blocks (enum df_flow_dir);
extern int *df_get_postorder (enum df_flow_dir);
extern void df_simple_dataflow (enum df_flow_dir, df_init_function,
				df_confluence_function_0, df_confluence_function_n,
				df_transfer_function, bitmap, int *, int);
extern void df_mark_solutions_dirty (void);
extern bool df_get_bb_dirty (basic_block);
extern void df_set_bb_dirty (basic_block);
extern void df_compact_blocks (void);
extern void df_bb_replace (int, basic_block);
extern void df_bb_delete (int);
extern void df_verify (void);
#ifdef DF_DEBUG_CFG
extern void df_check_cfg_clean (void);
#endif
extern df_ref df_bb_regno_first_def_find (basic_block, unsigned int);
extern df_ref df_bb_regno_last_def_find (basic_block, unsigned int);
extern df_ref df_find_def (rtx_insn *, rtx);
extern bool df_reg_defined (rtx_insn *, rtx);
extern df_ref df_find_use (rtx_insn *, rtx);
extern bool df_reg_used (rtx_insn *, rtx);
extern void df_worklist_dataflow (struct dataflow *,bitmap, int *, int);
extern void df_print_regset (FILE *file, bitmap r);
extern void df_print_word_regset (FILE *file, bitmap r);
extern void df_dump (FILE *);
extern void df_dump_region (FILE *);
extern void df_dump_start (FILE *);
extern void df_dump_top (basic_block, FILE *);
extern void df_dump_bottom (basic_block, FILE *);
extern void df_dump_insn_top (const rtx_insn *, FILE *);
extern void df_dump_insn_bottom (const rtx_insn *, FILE *);
extern void df_refs_chain_dump (df_ref, bool, FILE *);
extern void df_regs_chain_dump (df_ref,  FILE *);
extern void df_insn_debug (rtx_insn *, bool, FILE *);
extern void df_insn_debug_regno (rtx_insn *, FILE *);
extern void df_regno_debug (unsigned int, FILE *);
extern void df_ref_debug (df_ref, FILE *);
extern void debug_df_insn (rtx_insn *);
extern void debug_df_regno (unsigned int);
extern void debug_df_reg (rtx);
extern void debug_df_defno (unsigned int);
extern void debug_df_useno (unsigned int);
extern void debug_df_ref (df_ref);
extern void debug_df_chain (struct df_link *);

/* Functions defined in df-problems.c. */

extern struct df_link *df_chain_create (df_ref, df_ref);
extern void df_chain_unlink (df_ref);
extern void df_chain_copy (df_ref, struct df_link *);
extern void df_grow_bb_info (struct dataflow *);
extern void df_chain_dump (struct df_link *, FILE *);
extern void df_print_bb_index (basic_block bb, FILE *file);
extern void df_rd_add_problem (void);
extern void df_rd_simulate_artificial_defs_at_top (basic_block, bitmap);
extern void df_rd_simulate_one_insn (basic_block, rtx_insn *, bitmap);
extern void df_lr_add_problem (void);
extern void df_lr_verify_transfer_functions (void);
extern void df_live_verify_transfer_functions (void);
extern void df_live_add_problem (void);
extern void df_live_set_all_dirty (void);
extern void df_chain_add_problem (unsigned int);
extern void df_word_lr_add_problem (void);
extern bool df_word_lr_mark_ref (df_ref, bool, bitmap);
extern bool df_word_lr_simulate_defs (rtx_insn *, bitmap);
extern void df_word_lr_simulate_uses (rtx_insn *, bitmap);
extern void df_word_lr_simulate_artificial_refs_at_top (basic_block, bitmap);
extern void df_word_lr_simulate_artificial_refs_at_end (basic_block, bitmap);
extern void df_note_add_problem (void);
extern void df_md_add_problem (void);
extern void df_md_simulate_artificial_defs_at_top (basic_block, bitmap);
extern void df_md_simulate_one_insn (basic_block, rtx_insn *, bitmap);
extern void df_mir_add_problem (void);
extern void df_mir_simulate_one_insn (basic_block, rtx_insn *, bitmap, bitmap);
extern void df_simulate_find_noclobber_defs (rtx_insn *, bitmap);
extern void df_simulate_find_defs (rtx_insn *, bitmap);
extern void df_simulate_defs (rtx_insn *, bitmap);
extern void df_simulate_uses (rtx_insn *, bitmap);
extern void df_simulate_initialize_backwards (basic_block, bitmap);
extern void df_simulate_one_insn_backwards (basic_block, rtx_insn *, bitmap);
extern void df_simulate_finalize_backwards (basic_block, bitmap);
extern void df_simulate_initialize_forwards (basic_block, bitmap);
extern void df_simulate_one_insn_forwards (basic_block, rtx_insn *, bitmap);
extern void simulate_backwards_to_point (basic_block, regset, rtx);
extern bool can_move_insns_across (rtx_insn *, rtx_insn *,
				   rtx_insn *, rtx_insn *,
				   basic_block, regset,
				   regset, rtx_insn **);
/* Functions defined in df-scan.c.  */

extern void df_scan_alloc (bitmap);
extern void df_scan_add_problem (void);
extern void df_grow_reg_info (void);
extern void df_grow_insn_info (void);
extern void df_scan_blocks (void);
extern void df_uses_create (rtx *, rtx_insn *, int);
extern struct df_insn_info * df_insn_create_insn_record (rtx_insn *);
extern void df_insn_delete (rtx_insn *);
extern void df_bb_refs_record (int, bool);
extern bool df_insn_rescan (rtx_insn *);
extern bool df_insn_rescan_debug_internal (rtx_insn *);
extern void df_insn_rescan_all (void);
extern void df_process_deferred_rescans (void);
extern void df_recompute_luids (basic_block);
extern void df_insn_change_bb (rtx_insn *, basic_block);
extern void df_maybe_reorganize_use_refs (enum df_ref_order);
extern void df_maybe_reorganize_def_refs (enum df_ref_order);
extern void df_ref_change_reg_with_loc (rtx, unsigned int);
extern void df_notes_rescan (rtx_insn *);
extern void df_hard_reg_init (void);
extern void df_update_entry_block_defs (void);
extern void df_update_exit_block_uses (void);
extern void df_update_entry_exit_and_calls (void);
extern bool df_hard_reg_used_p (unsigned int);
extern unsigned int df_hard_reg_used_count (unsigned int);
extern bool df_regs_ever_live_p (unsigned int);
extern void df_set_regs_ever_live (unsigned int, bool);
extern void df_compute_regs_ever_live (bool);
extern void df_scan_verify (void);


/*----------------------------------------------------------------------------
   Public functions access functions for the dataflow problems.
----------------------------------------------------------------------------*/

static inline struct df_scan_bb_info *
df_scan_get_bb_info (unsigned int index)
{
  if (index < df_scan->block_info_size)
    return &((struct df_scan_bb_info *) df_scan->block_info)[index];
  else
    return NULL;
}

static inline struct df_rd_bb_info *
df_rd_get_bb_info (unsigned int index)
{
  if (index < df_rd->block_info_size)
    return &((struct df_rd_bb_info *) df_rd->block_info)[index];
  else
    return NULL;
}

static inline struct df_lr_bb_info *
df_lr_get_bb_info (unsigned int index)
{
  if (index < df_lr->block_info_size)
    return &((struct df_lr_bb_info *) df_lr->block_info)[index];
  else
    return NULL;
}

static inline struct df_md_bb_info *
df_md_get_bb_info (unsigned int index)
{
  if (index < df_md->block_info_size)
    return &((struct df_md_bb_info *) df_md->block_info)[index];
  else
    return NULL;
}

static inline struct df_live_bb_info *
df_live_get_bb_info (unsigned int index)
{
  if (index < df_live->block_info_size)
    return &((struct df_live_bb_info *) df_live->block_info)[index];
  else
    return NULL;
}

static inline struct df_word_lr_bb_info *
df_word_lr_get_bb_info (unsigned int index)
{
  if (index < df_word_lr->block_info_size)
    return &((struct df_word_lr_bb_info *) df_word_lr->block_info)[index];
  else
    return NULL;
}

static inline struct df_mir_bb_info *
df_mir_get_bb_info (unsigned int index)
{
  if (index < df_mir->block_info_size)
    return &((struct df_mir_bb_info *) df_mir->block_info)[index];
  else
    return NULL;
}

/* Get the live at out set for BB no matter what problem happens to be
   defined.  This function is used by the register allocators who
   choose different dataflow problems depending on the optimization
   level.  */

static inline bitmap
df_get_live_out (basic_block bb)
{
  gcc_checking_assert (df_lr);

  if (df_live)
    return DF_LIVE_OUT (bb);
  else
    return DF_LR_OUT (bb);
}

/* Get the live at in set for BB no matter what problem happens to be
   defined.  This function is used by the register allocators who
   choose different dataflow problems depending on the optimization
   level.  */

static inline bitmap
df_get_live_in (basic_block bb)
{
  gcc_checking_assert (df_lr);

  if (df_live)
    return DF_LIVE_IN (bb);
  else
    return DF_LR_IN (bb);
}

/* Get basic block info.  */
/* Get the artificial defs for a basic block.  */

static inline df_ref
df_get_artificial_defs (unsigned int bb_index)
{
  return df_scan_get_bb_info (bb_index)->artificial_defs;
}


/* Get the artificial uses for a basic block.  */

static inline df_ref
df_get_artificial_uses (unsigned int bb_index)
{
  return df_scan_get_bb_info (bb_index)->artificial_uses;
}

/* If INSN defines exactly one register, return the associated reference,
   otherwise return null.  */

static inline df_ref
df_single_def (const df_insn_info *info)
{
  df_ref defs = DF_INSN_INFO_DEFS (info);
  return defs && !DF_REF_NEXT_LOC (defs) ? defs : NULL;
}

/* If INSN uses exactly one register, return the associated reference,
   otherwise return null.  */

static inline df_ref
df_single_use (const df_insn_info *info)
{
  df_ref uses = DF_INSN_INFO_USES (info);
  return uses && !DF_REF_NEXT_LOC (uses) ? uses : NULL;
}

/* web */

class web_entry_base
{
 private:
  /* Reference to the parent in the union/find tree.  */
  web_entry_base *pred_pvt;

 public:
  /* Accessors.  */
  web_entry_base *pred () { return pred_pvt; }
  void set_pred (web_entry_base *p) { pred_pvt = p; }

  /* Find representative in union-find tree.  */
  web_entry_base *unionfind_root ();

  /* Union with another set, returning TRUE if they are already unioned.  */
  friend bool unionfind_union (web_entry_base *first, web_entry_base *second);
};

#endif /* GCC_DF_H */
