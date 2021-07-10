/* Define control flow data structures for the CFG.
   Copyright (C) 2014-2018 Free Software Foundation, Inc.

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

#ifndef GCC_CFGRTL_H
#define GCC_CFGRTL_H

extern void delete_insn (rtx_insn *);
extern bool delete_insn_and_edges (rtx_insn *);
extern void delete_insn_chain (rtx, rtx_insn *, bool);
extern basic_block create_basic_block_structure (rtx_insn *, rtx_insn *,
						 rtx_note *, basic_block);
extern void compute_bb_for_insn (void);
extern unsigned int free_bb_for_insn (void);
extern rtx_insn *entry_of_function (void);
extern void update_bb_for_insn (basic_block);
extern bool contains_no_active_insn_p (const_basic_block);
extern bool forwarder_block_p (const_basic_block);
extern bool can_fallthru (basic_block, basic_block);
extern rtx_note *bb_note (basic_block);
extern rtx_code_label *block_label (basic_block);
extern edge try_redirect_by_replacing_jump (edge, basic_block, bool);
extern void emit_barrier_after_bb (basic_block bb);
extern basic_block force_nonfallthru_and_redirect (edge, basic_block, rtx);
extern void insert_insn_on_edge (rtx, edge);
extern void commit_one_edge_insertion (edge e);
extern void commit_edge_insertions (void);
extern void print_rtl_with_bb (FILE *, const rtx_insn *, dump_flags_t);
extern void update_br_prob_note (basic_block);
extern rtx_insn *get_last_bb_insn (basic_block);
extern void fixup_partitions (void);
extern bool purge_dead_edges (basic_block);
extern bool purge_all_dead_edges (void);
extern bool fixup_abnormal_edges (void);
extern rtx_insn *unlink_insn_chain (rtx_insn *, rtx_insn *);
extern void relink_block_chain (bool);
extern rtx_insn *duplicate_insn_chain (rtx_insn *, rtx_insn *);
extern void cfg_layout_initialize (int);
extern void cfg_layout_finalize (void);
extern void break_superblocks (void);
extern void init_rtl_bb_info (basic_block);
extern void find_bbs_reachable_by_hot_paths (hash_set <basic_block> *);

#endif /* GCC_CFGRTL_H */
