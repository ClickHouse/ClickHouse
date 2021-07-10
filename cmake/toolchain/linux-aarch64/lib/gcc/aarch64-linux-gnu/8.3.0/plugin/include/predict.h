/* Definitions for branch prediction routines in the GNU compiler.
   Copyright (C) 2001-2018 Free Software Foundation, Inc.

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

#ifndef GCC_PREDICT_H
#define GCC_PREDICT_H

#include "profile-count.h"

/* Random guesstimation given names.
   PROB_VERY_UNLIKELY should be small enough so basic block predicted
   by it gets below HOT_BB_FREQUENCY_FRACTION.  */
#define PROB_VERY_UNLIKELY	(REG_BR_PROB_BASE / 2000 - 1)
#define PROB_EVEN		(REG_BR_PROB_BASE / 2)
#define PROB_VERY_LIKELY	(REG_BR_PROB_BASE - PROB_VERY_UNLIKELY)
#define PROB_ALWAYS		(REG_BR_PROB_BASE)
#define PROB_UNLIKELY           (REG_BR_PROB_BASE / 5 - 1)
#define PROB_LIKELY             (REG_BR_PROB_BASE - PROB_UNLIKELY)
#define PROB_UNINITIALIZED      (-1)

#define DEF_PREDICTOR(ENUM, NAME, HITRATE, FLAGS) ENUM,
enum br_predictor
{
#include "predict.def"

  /* Upper bound on non-language-specific builtins.  */
  END_PREDICTORS
};
#undef DEF_PREDICTOR
enum prediction
{
   NOT_TAKEN,
   TAKEN
};

/* In emit-rtl.c.  */
extern profile_probability split_branch_probability;

extern gcov_type get_hot_bb_threshold (void);
extern void set_hot_bb_threshold (gcov_type);
extern bool maybe_hot_count_p (struct function *, profile_count);
extern bool maybe_hot_bb_p (struct function *, const_basic_block);
extern bool maybe_hot_edge_p (edge);
extern bool probably_never_executed_bb_p (struct function *, const_basic_block);
extern bool probably_never_executed_edge_p (struct function *, edge);
extern bool optimize_function_for_size_p (struct function *);
extern bool optimize_function_for_speed_p (struct function *);
extern optimization_type function_optimization_type (struct function *);
extern bool optimize_bb_for_size_p (const_basic_block);
extern bool optimize_bb_for_speed_p (const_basic_block);
extern optimization_type bb_optimization_type (const_basic_block);
extern bool optimize_edge_for_size_p (edge);
extern bool optimize_edge_for_speed_p (edge);
extern bool optimize_insn_for_size_p (void);
extern bool optimize_insn_for_speed_p (void);
extern bool optimize_loop_for_size_p (struct loop *);
extern bool optimize_loop_for_speed_p (struct loop *);
extern bool optimize_loop_nest_for_speed_p (struct loop *);
extern bool optimize_loop_nest_for_size_p (struct loop *);
extern bool predictable_edge_p (edge);
extern void rtl_profile_for_bb (basic_block);
extern void rtl_profile_for_edge (edge);
extern void default_rtl_profile (void);
extern bool rtl_predicted_by_p (const_basic_block, enum br_predictor);
extern bool gimple_predicted_by_p (const_basic_block, enum br_predictor);
extern bool edge_probability_reliable_p (const_edge);
extern bool br_prob_note_reliable_p (const_rtx);
extern void predict_insn_def (rtx_insn *, enum br_predictor, enum prediction);
extern void rtl_predict_edge (edge, enum br_predictor, int);
extern void gimple_predict_edge (edge, enum br_predictor, int);
extern void remove_predictions_associated_with_edge (edge);
extern void predict_edge_def (edge, enum br_predictor, enum prediction);
extern void invert_br_probabilities (rtx);
extern void guess_outgoing_edge_probabilities (basic_block);
extern void tree_guess_outgoing_edge_probabilities (basic_block);
extern void tree_estimate_probability (bool);
extern void handle_missing_profiles (void);
extern bool update_max_bb_count (void);
extern bool expensive_function_p (int);
extern void estimate_bb_frequencies (bool);
extern void compute_function_frequency (void);
extern tree build_predict_expr (enum br_predictor, enum prediction);
extern const char *predictor_name (enum br_predictor);
extern void rebuild_frequencies (void);
extern void report_predictor_hitrates (void);
extern void force_edge_cold (edge, bool);
extern void propagate_unlikely_bbs_forward (void);

extern void add_reg_br_prob_note (rtx_insn *, profile_probability);

/* In ipa-pure-const.c   */
extern void warn_function_cold (tree);

#endif  /* GCC_PREDICT_H */
