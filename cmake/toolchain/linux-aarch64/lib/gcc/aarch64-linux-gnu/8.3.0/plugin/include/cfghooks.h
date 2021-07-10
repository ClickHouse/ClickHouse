/* Hooks for cfg representation specific functions.
   Copyright (C) 2003-2018 Free Software Foundation, Inc.
   Contributed by Sebastian Pop <s.pop@laposte.net>

This file is part of GCC.

GCC is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option)
any later version.

GCC is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with GCC; see the file COPYING3.  If not see
<http://www.gnu.org/licenses/>.  */

#ifndef GCC_CFGHOOKS_H
#define GCC_CFGHOOKS_H

#include "predict.h"

/* Structure to gather statistic about profile consistency, per pass.
   An array of this structure, indexed by pass static number, is allocated
   in passes.c.  The structure is defined here so that different CFG modes
   can do their book-keeping via CFG hooks.

   For every field[2], field[0] is the count before the pass runs, and
   field[1] is the post-pass count.  This allows us to monitor the effect
   of each individual pass on the profile consistency.
   
   This structure is not supposed to be used by anything other than passes.c
   and one CFG hook per CFG mode.  */
struct profile_record
{
  /* The number of basic blocks where sum(freq) of the block's predecessors
     doesn't match reasonably well with the incoming frequency.  */
  int num_mismatched_freq_in[2];
  /* Likewise for a basic block's successors.  */
  int num_mismatched_freq_out[2];
  /* The number of basic blocks where sum(count) of the block's predecessors
     doesn't match reasonably well with the incoming frequency.  */
  int num_mismatched_count_in[2];
  /* Likewise for a basic block's successors.  */
  int num_mismatched_count_out[2];
  /* A weighted cost of the run-time of the function body.  */
  gcov_type time[2];
  /* A weighted cost of the size of the function body.  */
  int size[2];
  /* True iff this pass actually was run.  */
  bool run;
};


struct cfg_hooks
{
  /* Name of the corresponding ir.  */
  const char *name;

  /* Debugging.  */
  int (*verify_flow_info) (void);
  void (*dump_bb) (FILE *, basic_block, int, dump_flags_t);
  void (*dump_bb_for_graph) (pretty_printer *, basic_block);

  /* Basic CFG manipulation.  */

  /* Return new basic block.  */
  basic_block (*create_basic_block) (void *head, void *end, basic_block after);

  /* Redirect edge E to the given basic block B and update underlying program
     representation.  Returns edge representing redirected branch (that may not
     be equivalent to E in the case of duplicate edges being removed) or NULL
     if edge is not easily redirectable for whatever reason.  */
  edge (*redirect_edge_and_branch) (edge e, basic_block b);

  /* Same as the above but allows redirecting of fallthru edges.  In that case
     newly created forwarder basic block is returned.  The edge must
     not be abnormal.  */
  basic_block (*redirect_edge_and_branch_force) (edge, basic_block);

  /* Returns true if it is possible to remove the edge by redirecting it
     to the destination of the other edge going from its source.  */
  bool (*can_remove_branch_p) (const_edge);

  /* Remove statements corresponding to a given basic block.  */
  void (*delete_basic_block) (basic_block);

  /* Creates a new basic block just after basic block B by splitting
     everything after specified instruction I.  */
  basic_block (*split_block) (basic_block b, void * i);

  /* Move block B immediately after block A.  */
  bool (*move_block_after) (basic_block b, basic_block a);

  /* Return true when blocks A and B can be merged into single basic block.  */
  bool (*can_merge_blocks_p) (basic_block a, basic_block b);

  /* Merge blocks A and B.  */
  void (*merge_blocks) (basic_block a, basic_block b);

  /* Predict edge E using PREDICTOR to given PROBABILITY.  */
  void (*predict_edge) (edge e, enum br_predictor predictor, int probability);

  /* Return true if the one of outgoing edges is already predicted by
     PREDICTOR.  */
  bool (*predicted_by_p) (const_basic_block bb, enum br_predictor predictor);

  /* Return true when block A can be duplicated.  */
  bool (*can_duplicate_block_p) (const_basic_block a);

  /* Duplicate block A.  */
  basic_block (*duplicate_block) (basic_block a);

  /* Higher level functions representable by primitive operations above if
     we didn't have some oddities in RTL and Tree representations.  */
  basic_block (*split_edge) (edge);
  void (*make_forwarder_block) (edge);

  /* Try to make the edge fallthru.  */
  void (*tidy_fallthru_edge) (edge);

  /* Make the edge non-fallthru.  */
  basic_block (*force_nonfallthru) (edge);

  /* Say whether a block ends with a call, possibly followed by some
     other code that must stay with the call.  */
  bool (*block_ends_with_call_p) (basic_block);

  /* Say whether a block ends with a conditional branch.  Switches
     and unconditional branches do not qualify.  */
  bool (*block_ends_with_condjump_p) (const_basic_block);

  /* Add fake edges to the function exit for any non constant and non noreturn
     calls, volatile inline assembly in the bitmap of blocks specified by
     BLOCKS or to the whole CFG if BLOCKS is zero.  Return the number of blocks
     that were split.

     The goal is to expose cases in which entering a basic block does not imply
     that all subsequent instructions must be executed.  */
  int (*flow_call_edges_add) (sbitmap);

  /* This function is called immediately after edge E is added to the
     edge vector E->dest->preds.  */
  void (*execute_on_growing_pred) (edge);

  /* This function is called immediately before edge E is removed from
     the edge vector E->dest->preds.  */
  void (*execute_on_shrinking_pred) (edge);

  /* A hook for duplicating loop in CFG, currently this is used
     in loop versioning.  */
  bool (*cfg_hook_duplicate_loop_to_header_edge) (struct loop *, edge,
						  unsigned, sbitmap,
						  edge, vec<edge> *,
						  int);

  /* Add condition to new basic block and update CFG used in loop
     versioning.  */
  void (*lv_add_condition_to_bb) (basic_block, basic_block, basic_block,
				  void *);
  /* Update the PHI nodes in case of loop versioning.  */
  void (*lv_adjust_loop_header_phi) (basic_block, basic_block,
				     basic_block, edge);

  /* Given a condition BB extract the true/false taken/not taken edges
     (depending if we are on tree's or RTL). */
  void (*extract_cond_bb_edges) (basic_block, edge *, edge *);


  /* Add PHI arguments queued in PENDINT_STMT list on edge E to edge
     E->dest (only in tree-ssa loop versioning.  */
  void (*flush_pending_stmts) (edge);
  
  /* True if a block contains no executable instructions.  */
  bool (*empty_block_p) (basic_block);

  /* Split a basic block if it ends with a conditional branch and if
     the other part of the block is not empty.  */
  basic_block (*split_block_before_cond_jump) (basic_block);

  /* Do book-keeping of a basic block for the profile consistency checker.  */
  void (*account_profile_record) (basic_block, int, struct profile_record *);
};

extern void verify_flow_info (void);

/* Check control flow invariants, if internal consistency checks are
   enabled.  */

static inline void
checking_verify_flow_info (void)
{
  /* TODO: Add a separate option for -fchecking=cfg.  */
  if (flag_checking)
    verify_flow_info ();
}

extern void dump_bb (FILE *, basic_block, int, dump_flags_t);
extern void dump_bb_for_graph (pretty_printer *, basic_block);
extern void dump_flow_info (FILE *, dump_flags_t);

extern edge redirect_edge_and_branch (edge, basic_block);
extern basic_block redirect_edge_and_branch_force (edge, basic_block);
extern edge redirect_edge_succ_nodup (edge, basic_block);
extern bool can_remove_branch_p (const_edge);
extern void remove_branch (edge);
extern void remove_edge (edge);
extern edge split_block (basic_block, rtx);
extern edge split_block (basic_block, gimple *);
extern edge split_block_after_labels (basic_block);
extern bool move_block_after (basic_block, basic_block);
extern void delete_basic_block (basic_block);
extern basic_block split_edge (edge);
extern basic_block create_basic_block (rtx, rtx, basic_block);
extern basic_block create_basic_block (gimple_seq, basic_block);
extern basic_block create_empty_bb (basic_block);
extern bool can_merge_blocks_p (basic_block, basic_block);
extern void merge_blocks (basic_block, basic_block);
extern edge make_forwarder_block (basic_block, bool (*)(edge),
				  void (*) (basic_block));
extern basic_block force_nonfallthru (edge);
extern void tidy_fallthru_edge (edge);
extern void tidy_fallthru_edges (void);
extern void predict_edge (edge e, enum br_predictor predictor, int probability);
extern bool predicted_by_p (const_basic_block bb, enum br_predictor predictor);
extern bool can_duplicate_block_p (const_basic_block);
extern basic_block duplicate_block (basic_block, edge, basic_block);
extern bool block_ends_with_call_p (basic_block bb);
extern bool empty_block_p (basic_block);
extern basic_block split_block_before_cond_jump (basic_block);
extern bool block_ends_with_condjump_p (const_basic_block bb);
extern int flow_call_edges_add (sbitmap);
extern void execute_on_growing_pred (edge);
extern void execute_on_shrinking_pred (edge);
extern bool cfg_hook_duplicate_loop_to_header_edge (struct loop *loop, edge,
						    unsigned int ndupl,
						    sbitmap wont_exit,
						    edge orig,
						    vec<edge> *to_remove,
						    int flags);

extern void lv_flush_pending_stmts (edge);
extern void extract_cond_bb_edges (basic_block, edge *, edge*);
extern void lv_adjust_loop_header_phi (basic_block, basic_block, basic_block,
				       edge);
extern void lv_add_condition_to_bb (basic_block, basic_block, basic_block,
				    void *);

extern bool can_copy_bbs_p (basic_block *, unsigned);
extern void copy_bbs (basic_block *, unsigned, basic_block *,
		      edge *, unsigned, edge *, struct loop *,
		      basic_block, bool);

void account_profile_record (struct profile_record *, int);

/* Hooks containers.  */
extern struct cfg_hooks gimple_cfg_hooks;
extern struct cfg_hooks rtl_cfg_hooks;
extern struct cfg_hooks cfg_layout_rtl_cfg_hooks;

/* Declarations.  */
extern enum ir_type current_ir_type (void);
extern void rtl_register_cfg_hooks (void);
extern void cfg_layout_rtl_register_cfg_hooks (void);
extern void gimple_register_cfg_hooks (void);
extern struct cfg_hooks get_cfg_hooks (void);
extern void set_cfg_hooks (struct cfg_hooks);

#endif /* GCC_CFGHOOKS_H */
