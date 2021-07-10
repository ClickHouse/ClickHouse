/* Definitions for describing one tree-ssa optimization pass.
   Copyright (C) 2004-2018 Free Software Foundation, Inc.
   Contributed by Richard Henderson <rth@redhat.com>

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


#ifndef GCC_TREE_PASS_H
#define GCC_TREE_PASS_H 1

#include "timevar.h"
#include "dumpfile.h"

struct function;

/* Optimization pass type.  */
enum opt_pass_type
{
  GIMPLE_PASS,
  RTL_PASS,
  SIMPLE_IPA_PASS,
  IPA_PASS
};

/* Metadata for a pass, non-varying across all instances of a pass.  */
struct pass_data
{
  /* Optimization pass type.  */
  enum opt_pass_type type;

  /* Terse name of the pass used as a fragment of the dump file
     name.  If the name starts with a star, no dump happens. */
  const char *name;

  /* The -fopt-info optimization group flags as defined in dumpfile.h. */
  unsigned int optinfo_flags;

  /* The timevar id associated with this pass.  */
  /* ??? Ideally would be dynamically assigned.  */
  timevar_id_t tv_id;

  /* Sets of properties input and output from this pass.  */
  unsigned int properties_required;
  unsigned int properties_provided;
  unsigned int properties_destroyed;

  /* Flags indicating common sets things to do before and after.  */
  unsigned int todo_flags_start;
  unsigned int todo_flags_finish;
};

namespace gcc
{
  class context;
} // namespace gcc

/* An instance of a pass.  This is also "pass_data" to minimize the
   changes in existing code.  */
class opt_pass : public pass_data
{
public:
  virtual ~opt_pass () { }

  /* Create a copy of this pass.

     Passes that can have multiple instances must provide their own
     implementation of this, to ensure that any sharing of state between
     this instance and the copy is "wired up" correctly.

     The default implementation prints an error message and aborts.  */
  virtual opt_pass *clone ();
  virtual void set_pass_param (unsigned int, bool);

  /* This pass and all sub-passes are executed only if the function returns
     true.  The default implementation returns true.  */
  virtual bool gate (function *fun);

  /* This is the code to run.  If this is not overridden, then there should
     be sub-passes otherwise this pass does nothing.
     The return value contains TODOs to execute in addition to those in
     TODO_flags_finish.   */
  virtual unsigned int execute (function *fun);

protected:
  opt_pass (const pass_data&, gcc::context *);

public:
  /* A list of sub-passes to run, dependent on gate predicate.  */
  opt_pass *sub;

  /* Next in the list of passes to run, independent of gate predicate.  */
  opt_pass *next;

  /* Static pass number, used as a fragment of the dump file name.  */
  int static_pass_number;

protected:
  gcc::context *m_ctxt;
};

/* Description of GIMPLE pass.  */
class gimple_opt_pass : public opt_pass
{
protected:
  gimple_opt_pass (const pass_data& data, gcc::context *ctxt)
    : opt_pass (data, ctxt)
  {
  }
};

/* Description of RTL pass.  */
class rtl_opt_pass : public opt_pass
{
protected:
  rtl_opt_pass (const pass_data& data, gcc::context *ctxt)
    : opt_pass (data, ctxt)
  {
  }
};

class varpool_node;
struct cgraph_node;
struct lto_symtab_encoder_d;

/* Description of IPA pass with generate summary, write, execute, read and
   transform stages.  */
class ipa_opt_pass_d : public opt_pass
{
public:
  /* IPA passes can analyze function body and variable initializers
      using this hook and produce summary.  */
  void (*generate_summary) (void);

  /* This hook is used to serialize IPA summaries on disk.  */
  void (*write_summary) (void);

  /* This hook is used to deserialize IPA summaries from disk.  */
  void (*read_summary) (void);

  /* This hook is used to serialize IPA optimization summaries on disk.  */
  void (*write_optimization_summary) (void);

  /* This hook is used to deserialize IPA summaries from disk.  */
  void (*read_optimization_summary) (void);

  /* Hook to convert gimple stmt uids into true gimple statements.  The second
     parameter is an array of statements indexed by their uid. */
  void (*stmt_fixup) (struct cgraph_node *, gimple **);

  /* Results of interprocedural propagation of an IPA pass is applied to
     function body via this hook.  */
  unsigned int function_transform_todo_flags_start;
  unsigned int (*function_transform) (struct cgraph_node *);
  void (*variable_transform) (varpool_node *);

protected:
  ipa_opt_pass_d (const pass_data& data, gcc::context *ctxt,
		  void (*generate_summary) (void),
		  void (*write_summary) (void),
		  void (*read_summary) (void),
		  void (*write_optimization_summary) (void),
		  void (*read_optimization_summary) (void),
		  void (*stmt_fixup) (struct cgraph_node *, gimple **),
		  unsigned int function_transform_todo_flags_start,
		  unsigned int (*function_transform) (struct cgraph_node *),
		  void (*variable_transform) (varpool_node *))
    : opt_pass (data, ctxt),
      generate_summary (generate_summary),
      write_summary (write_summary),
      read_summary (read_summary),
      write_optimization_summary (write_optimization_summary),
      read_optimization_summary (read_optimization_summary),
      stmt_fixup (stmt_fixup),
      function_transform_todo_flags_start (function_transform_todo_flags_start),
      function_transform (function_transform),
      variable_transform (variable_transform)
  {
  }
};

/* Description of simple IPA pass.  Simple IPA passes have just one execute
   hook.  */
class simple_ipa_opt_pass : public opt_pass
{
protected:
  simple_ipa_opt_pass (const pass_data& data, gcc::context *ctxt)
    : opt_pass (data, ctxt)
  {
  }
};

/* Pass properties.  */
#define PROP_gimple_any		(1 << 0)	/* entire gimple grammar */
#define PROP_gimple_lcf		(1 << 1)	/* lowered control flow */
#define PROP_gimple_leh		(1 << 2)	/* lowered eh */
#define PROP_cfg		(1 << 3)
#define PROP_ssa		(1 << 5)
#define PROP_no_crit_edges      (1 << 6)
#define PROP_rtl		(1 << 7)
#define PROP_gimple_lomp	(1 << 8)	/* lowered OpenMP directives */
#define PROP_cfglayout	 	(1 << 9)	/* cfglayout mode on RTL */
#define PROP_gimple_lcx		(1 << 10)       /* lowered complex */
#define PROP_loops		(1 << 11)	/* preserve loop structures */
#define PROP_gimple_lvec	(1 << 12)       /* lowered vector */
#define PROP_gimple_eomp	(1 << 13)       /* no OpenMP directives */
#define PROP_gimple_lva		(1 << 14)       /* No va_arg internal function.  */
#define PROP_gimple_opt_math	(1 << 15)	/* Disable canonicalization
						   of math functions; the
						   current choices have
						   been optimized.  */
#define PROP_gimple_lomp_dev	(1 << 16)	/* done omp_device_lower */
#define PROP_rtl_split_insns	(1 << 17)	/* RTL has insns split.  */

#define PROP_trees \
  (PROP_gimple_any | PROP_gimple_lcf | PROP_gimple_leh | PROP_gimple_lomp)

/* To-do flags.  */
#define TODO_do_not_ggc_collect		(1 << 1)
#define TODO_cleanup_cfg        	(1 << 5)
#define TODO_verify_il			(1 << 6)
#define TODO_dump_symtab		(1 << 7)
#define TODO_remove_functions		(1 << 8)
#define TODO_rebuild_frequencies	(1 << 9)

/* To-do flags for calls to update_ssa.  */

/* Update the SSA form inserting PHI nodes for newly exposed symbols
   and virtual names marked for updating.  When updating real names,
   only insert PHI nodes for a real name O_j in blocks reached by all
   the new and old definitions for O_j.  If the iterated dominance
   frontier for O_j is not pruned, we may end up inserting PHI nodes
   in blocks that have one or more edges with no incoming definition
   for O_j.  This would lead to uninitialized warnings for O_j's
   symbol.  */
#define TODO_update_ssa			(1 << 11)

/* Update the SSA form without inserting any new PHI nodes at all.
   This is used by passes that have either inserted all the PHI nodes
   themselves or passes that need only to patch use-def and def-def
   chains for virtuals (e.g., DCE).  */
#define TODO_update_ssa_no_phi		(1 << 12)

/* Insert PHI nodes everywhere they are needed.  No pruning of the
   IDF is done.  This is used by passes that need the PHI nodes for
   O_j even if it means that some arguments will come from the default
   definition of O_j's symbol.

   WARNING: If you need to use this flag, chances are that your pass
   may be doing something wrong.  Inserting PHI nodes for an old name
   where not all edges carry a new replacement may lead to silent
   codegen errors or spurious uninitialized warnings.  */
#define TODO_update_ssa_full_phi	(1 << 13)

/* Passes that update the SSA form on their own may want to delegate
   the updating of virtual names to the generic updater.  Since FUD
   chains are easier to maintain, this simplifies the work they need
   to do.  NOTE: If this flag is used, any OLD->NEW mappings for real
   names are explicitly destroyed and only the symbols marked for
   renaming are processed.  */
#define TODO_update_ssa_only_virtuals	(1 << 14)

/* Some passes leave unused local variables that can be removed from
   cfun->local_decls.  This reduces the size of dump files
   and the memory footprint for VAR_DECLs.  */
#define TODO_remove_unused_locals	(1 << 15)

/* Call df_finish at the end of the pass.  This is done after all of
   the dumpers have been allowed to run so that they have access to
   the instance before it is destroyed.  */
#define TODO_df_finish                  (1 << 17)

/* Call df_verify at the end of the pass if checking is enabled.  */
#define TODO_df_verify                  (1 << 18)

/* Internally used for the first instance of a pass.  */
#define TODO_mark_first_instance	(1 << 19)

/* Rebuild aliasing info.  */
#define TODO_rebuild_alias              (1 << 20)

/* Rebuild the addressable-vars bitmap and do register promotion.  */
#define TODO_update_address_taken	(1 << 21)

/* Rebuild the callgraph edges.  */
#define TODO_rebuild_cgraph_edges       (1 << 22)

/* Release function body and stop pass manager.  */
#define TODO_discard_function		(1 << 23)

/* Internally used in execute_function_todo().  */
#define TODO_update_ssa_any		\
    (TODO_update_ssa			\
     | TODO_update_ssa_no_phi		\
     | TODO_update_ssa_full_phi		\
     | TODO_update_ssa_only_virtuals)

#define TODO_verify_all TODO_verify_il


/* Register pass info. */

enum pass_positioning_ops
{
  PASS_POS_INSERT_AFTER,  /* Insert after the reference pass.  */
  PASS_POS_INSERT_BEFORE, /* Insert before the reference pass.  */
  PASS_POS_REPLACE        /* Replace the reference pass.  */
};

struct register_pass_info
{
  opt_pass *pass;		    /* New pass to register.  */
  const char *reference_pass_name;  /* Name of the reference pass for hooking
                                       up the new pass.  */
  int ref_pass_instance_number;     /* Insert the pass at the specified
                                       instance number of the reference pass.
                                       Do it for every instance if it is 0.  */
  enum pass_positioning_ops pos_op; /* how to insert the new pass.  */
};

/* Registers a new pass.  Either fill out the register_pass_info or specify
   the individual parameters.  The pass object is expected to have been
   allocated using operator new and the pass manager takes the ownership of
   the pass object.  */
extern void register_pass (register_pass_info *);
extern void register_pass (opt_pass* pass, pass_positioning_ops pos,
			   const char* ref_pass_name, int ref_pass_inst_number);

extern simple_ipa_opt_pass *make_pass_ipa_chkp_versioning (gcc::context *ctxt);
extern simple_ipa_opt_pass *make_pass_ipa_chkp_early_produce_thunks (gcc::context *ctxt);
extern simple_ipa_opt_pass *make_pass_ipa_chkp_produce_thunks (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_chkp (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_chkp_opt (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_asan (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_asan_O0 (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_tsan (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_tsan_O0 (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_sancov (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_sancov_O0 (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_lower_cf (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_refactor_eh (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_lower_eh (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_lower_eh_dispatch (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_lower_resx (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_build_cfg (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_early_tree_profile (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_cleanup_eh (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_sra (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_sra_early (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_early_ipa_sra (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_tail_recursion (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_tail_calls (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_fix_loops (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_tree_loop (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_tree_no_loop (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_tree_loop_init (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_lim (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_linterchange (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_tree_unswitch (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_loop_split (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_loop_jam (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_predcom (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_iv_canon (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_scev_cprop (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_empty_loop (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_graphite (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_graphite_transforms (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_if_conversion (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_loop_distribution (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_vectorize (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_simduid_cleanup (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_slp_vectorize (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_complete_unroll (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_complete_unrolli (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_parallelize_loops (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_loop_prefetch (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_iv_optimize (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_tree_loop_done (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_ch (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_ch_vect (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_ccp (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_split_paths (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_phi_only_cprop (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_build_ssa (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_build_alias (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_build_ealias (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_dominator (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_dce (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_cd_dce (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_call_cdce (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_merge_phi (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_thread_jumps (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_early_thread_jumps (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_split_crit_edges (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_laddress (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_pre (gcc::context *ctxt);
extern unsigned int tail_merge_optimize (unsigned int);
extern gimple_opt_pass *make_pass_profile (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_strip_predict_hints (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_lower_complex_O0 (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_lower_complex (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_lower_switch (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_lower_vector (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_lower_vector_ssa (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_lower_omp (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_diagnose_omp_blocks (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_expand_omp (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_expand_omp_ssa (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_omp_target_link (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_oacc_device_lower (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_omp_device_lower (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_object_sizes (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_strlen (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_fold_builtins (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_post_ipa_warn (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_stdarg (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_early_warn_uninitialized (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_late_warn_uninitialized (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_cse_reciprocals (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_cse_sincos (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_optimize_bswap (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_store_merging (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_optimize_widening_mul (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_warn_function_return (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_warn_function_noreturn (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_cselim (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_phiopt (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_forwprop (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_phiprop (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_tree_ifcombine (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_dse (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_nrv (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_rename_ssa_copies (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_sink_code (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_fre (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_check_data_deps (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_copy_prop (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_isolate_erroneous_paths (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_early_vrp (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_vrp (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_uncprop (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_return_slot (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_reassoc (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_rebuild_cgraph_edges (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_remove_cgraph_callee_edges (gcc::context
							      *ctxt);
extern gimple_opt_pass *make_pass_build_cgraph_edges (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_local_pure_const (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_nothrow (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_tracer (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_warn_restrict (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_warn_unused_result (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_diagnose_tm_blocks (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_lower_tm (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_tm_init (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_tm_mark (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_tm_memopt (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_tm_edges (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_split_functions (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_feedback_split_functions (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_strength_reduction (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_vtable_verify (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_ubsan (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_sanopt (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_oacc_kernels (gcc::context *ctxt);
extern simple_ipa_opt_pass *make_pass_ipa_oacc (gcc::context *ctxt);
extern simple_ipa_opt_pass *make_pass_ipa_oacc_kernels (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_gen_hsail (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_warn_nonnull_compare (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_sprintf_length (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_walloca (gcc::context *ctxt);

/* IPA Passes */
extern simple_ipa_opt_pass *make_pass_ipa_lower_emutls (gcc::context *ctxt);
extern simple_ipa_opt_pass
							      *make_pass_ipa_function_and_variable_visibility (gcc::context *ctxt);
extern simple_ipa_opt_pass *make_pass_ipa_tree_profile (gcc::context *ctxt);
extern simple_ipa_opt_pass *make_pass_ipa_auto_profile (gcc::context *ctxt);

extern simple_ipa_opt_pass *make_pass_build_ssa_passes (gcc::context *ctxt);
extern simple_ipa_opt_pass *make_pass_chkp_instrumentation_passes (gcc::context *ctxt);
extern simple_ipa_opt_pass *make_pass_local_optimization_passes (gcc::context *ctxt);

extern ipa_opt_pass_d *make_pass_ipa_whole_program_visibility (gcc::context
							       *ctxt);
extern simple_ipa_opt_pass *make_pass_ipa_increase_alignment (gcc::context
							      *ctxt);
extern ipa_opt_pass_d *make_pass_ipa_fn_summary (gcc::context *ctxt);
extern ipa_opt_pass_d *make_pass_ipa_inline (gcc::context *ctxt);
extern simple_ipa_opt_pass *make_pass_ipa_free_lang_data (gcc::context *ctxt);
extern simple_ipa_opt_pass *make_pass_ipa_free_fn_summary (gcc::context *ctxt);
extern ipa_opt_pass_d *make_pass_ipa_cp (gcc::context *ctxt);
extern ipa_opt_pass_d *make_pass_ipa_icf (gcc::context *ctxt);
extern ipa_opt_pass_d *make_pass_ipa_devirt (gcc::context *ctxt);
extern ipa_opt_pass_d *make_pass_ipa_reference (gcc::context *ctxt);
extern ipa_opt_pass_d *make_pass_ipa_hsa (gcc::context *ctxt);
extern ipa_opt_pass_d *make_pass_ipa_pure_const (gcc::context *ctxt);
extern simple_ipa_opt_pass *make_pass_ipa_pta (gcc::context *ctxt);
extern simple_ipa_opt_pass *make_pass_ipa_tm (gcc::context *ctxt);
extern simple_ipa_opt_pass *make_pass_target_clone (gcc::context *ctxt);
extern simple_ipa_opt_pass *make_pass_dispatcher_calls (gcc::context *ctxt);
extern simple_ipa_opt_pass *make_pass_omp_simd_clone (gcc::context *ctxt);
extern ipa_opt_pass_d *make_pass_ipa_profile (gcc::context *ctxt);
extern ipa_opt_pass_d *make_pass_ipa_cdtor_merge (gcc::context *ctxt);
extern ipa_opt_pass_d *make_pass_ipa_single_use (gcc::context *ctxt);
extern ipa_opt_pass_d *make_pass_ipa_comdats (gcc::context *ctxt);
extern simple_ipa_opt_pass *make_pass_materialize_all_clones (gcc::context *
							      ctxt);

extern gimple_opt_pass *make_pass_cleanup_cfg_post_optimizing (gcc::context
							       *ctxt);
extern gimple_opt_pass *make_pass_fixup_cfg (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_backprop (gcc::context *ctxt);

extern rtl_opt_pass *make_pass_expand (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_instantiate_virtual_regs (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_rtl_fwprop (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_rtl_fwprop_addr (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_jump (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_jump2 (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_lower_subreg (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_cse (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_fast_rtl_dce (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_ud_rtl_dce (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_rtl_dce (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_rtl_dse1 (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_rtl_dse2 (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_rtl_dse3 (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_rtl_cprop (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_rtl_pre (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_rtl_hoist (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_rtl_store_motion (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_cse_after_global_opts (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_rtl_ifcvt (gcc::context *ctxt);

extern rtl_opt_pass *make_pass_into_cfg_layout_mode (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_outof_cfg_layout_mode (gcc::context *ctxt);

extern rtl_opt_pass *make_pass_loop2 (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_rtl_loop_init (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_rtl_move_loop_invariants (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_rtl_unroll_loops (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_rtl_doloop (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_rtl_loop_done (gcc::context *ctxt);

extern rtl_opt_pass *make_pass_web (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_cse2 (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_df_initialize_opt (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_df_initialize_no_opt (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_reginfo_init (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_inc_dec (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_stack_ptr_mod (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_initialize_regs (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_combine (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_if_after_combine (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_ree (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_partition_blocks (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_match_asm_constraints (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_split_all_insns (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_fast_rtl_byte_dce (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_lower_subreg2 (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_mode_switching (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_sms (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_sched (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_live_range_shrinkage (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_early_remat (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_ira (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_reload (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_clean_state (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_branch_prob (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_value_profile_transformations (gcc::context
							      *ctxt);
extern rtl_opt_pass *make_pass_postreload_cse (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_gcse2 (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_split_after_reload (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_branch_target_load_optimize1 (gcc::context
							     *ctxt);
extern rtl_opt_pass *make_pass_thread_prologue_and_epilogue (gcc::context
							     *ctxt);
extern rtl_opt_pass *make_pass_stack_adjustments (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_sched_fusion (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_peephole2 (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_if_after_reload (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_regrename (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_cprop_hardreg (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_reorder_blocks (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_branch_target_load_optimize2 (gcc::context
							     *ctxt);
extern rtl_opt_pass *make_pass_leaf_regs (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_split_before_sched2 (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_compare_elim_after_reload (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_sched2 (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_stack_regs (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_stack_regs_run (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_df_finish (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_compute_alignments (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_duplicate_computed_gotos (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_variable_tracking (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_free_cfg (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_machine_reorg (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_cleanup_barriers (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_delay_slots (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_split_for_shorten_branches (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_split_before_regstack (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_convert_to_eh_region_ranges (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_shorten_branches (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_set_nothrow_function_flags (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_dwarf2_frame (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_final (gcc::context *ctxt);
extern rtl_opt_pass *make_pass_rtl_seqabstr (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_release_ssa_names (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_early_inline (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_local_fn_summary (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_update_address_taken (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_convert_switch (gcc::context *ctxt);
extern gimple_opt_pass *make_pass_lower_vaarg (gcc::context *ctxt);

/* Current optimization pass.  */
extern opt_pass *current_pass;

extern bool execute_one_pass (opt_pass *);
extern void execute_pass_list (function *, opt_pass *);
extern void execute_ipa_pass_list (opt_pass *);
extern void execute_ipa_summary_passes (ipa_opt_pass_d *);
extern void execute_all_ipa_transforms (void);
extern void execute_all_ipa_stmt_fixups (struct cgraph_node *, gimple **);
extern bool pass_init_dump_file (opt_pass *);
extern void pass_fini_dump_file (opt_pass *);
extern void emergency_dump_function (void);

extern void print_current_pass (FILE *);
extern void debug_pass (void);
extern void ipa_write_summaries (void);
extern void ipa_write_optimization_summaries (struct lto_symtab_encoder_d *);
extern void ipa_read_summaries (void);
extern void ipa_read_optimization_summaries (void);
extern void register_one_dump_file (opt_pass *);
extern bool function_called_by_processed_nodes_p (void);

/* Declare for plugins.  */
extern void do_per_function_toporder (void (*) (function *, void *), void *);

extern void disable_pass (const char *);
extern void enable_pass (const char *);
extern void dump_passes (void);

#endif /* GCC_TREE_PASS_H */
