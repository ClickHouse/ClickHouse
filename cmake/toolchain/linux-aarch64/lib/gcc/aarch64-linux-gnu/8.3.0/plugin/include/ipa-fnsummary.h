/* IPA function body analysis.
   Copyright (C) 2003-2018 Free Software Foundation, Inc.
   Contributed by Jan Hubicka

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

#ifndef GCC_IPA_SUMMARY_H
#define GCC_IPA_SUMMARY_H

#include "sreal.h"
#include "ipa-predicate.h"


/* Hints are reasons why IPA heuristics should preffer specializing given
   function.  They are represtented as bitmap of the following values.  */
enum ipa_hints_vals {
  /* When specialization turns indirect call into a direct call,
     it is good idea to do so.  */
  INLINE_HINT_indirect_call = 1,
  /* Inlining may make loop iterations or loop stride known.  It is good idea
     to do so because it enables loop optimizatoins.  */
  INLINE_HINT_loop_iterations = 2,
  INLINE_HINT_loop_stride = 4,
  /* Inlining within same strongly connected component of callgraph is often
     a loss due to increased stack frame usage and prologue setup costs.  */
  INLINE_HINT_same_scc = 8,
  /* Inlining functions in strongly connected component is not such a great
     win.  */
  INLINE_HINT_in_scc = 16,
  /* If function is declared inline by user, it may be good idea to inline
     it.  Set by simple_edge_hints in ipa-inline-analysis.c.  */
  INLINE_HINT_declared_inline = 32,
  /* Programs are usually still organized for non-LTO compilation and thus
     if functions are in different modules, inlining may not be so important. 
     Set by simple_edge_hints in ipa-inline-analysis.c.   */
  INLINE_HINT_cross_module = 64,
  /* If array indexes of loads/stores become known there may be room for
     further optimization.  */
  INLINE_HINT_array_index = 128,
  /* We know that the callee is hot by profile.  */
  INLINE_HINT_known_hot = 256
};

typedef int ipa_hints;

/* Simple description of whether a memory load or a condition refers to a load
   from an aggregate and if so, how and where from in the aggregate.
   Individual fields have the same meaning like fields with the same name in
   struct condition.  */

struct agg_position_info
{
  HOST_WIDE_INT offset;
  bool agg_contents;
  bool by_ref;
};

/* Representation of function body size and time depending on the call
   context.  We keep simple array of record, every containing of predicate
   and time/size to account.  */
struct GTY(()) size_time_entry
{
  /* Predicate for code to be executed.  */
  predicate exec_predicate;
  /* Predicate for value to be constant and optimized out in a specialized copy.
     When deciding on specialization this makes it possible to see how much
     the executed code paths will simplify.  */
  predicate nonconst_predicate;
  int size;
  sreal GTY((skip)) time;
};

/* Function inlining information.  */
struct GTY(()) ipa_fn_summary
{
  /* Information about the function body itself.  */

  /* Estimated stack frame consumption by the function.  */
  HOST_WIDE_INT estimated_self_stack_size;
  /* Size of the function body.  */
  int self_size;
  /* Minimal size increase after inlining.  */
  int min_size;

  /* False when there something makes inlining impossible (such as va_arg).  */
  unsigned inlinable : 1;
  /* True wen there is only one caller of the function before small function
     inlining.  */
  unsigned int single_caller : 1;
  /* True if function contains any floating point expressions.  */
  unsigned int fp_expressions : 1;

  /* Information about function that will result after applying all the
     inline decisions present in the callgraph.  Generally kept up to
     date only for functions that are not inline clones. */

  /* Estimated stack frame consumption by the function.  */
  HOST_WIDE_INT estimated_stack_size;
  /* Expected offset of the stack frame of function.  */
  HOST_WIDE_INT stack_frame_offset;
  /* Estimated size of the function after inlining.  */
  sreal GTY((skip)) time;
  int size;

  /* Conditional size/time information.  The summaries are being
     merged during inlining.  */
  conditions conds;
  vec<size_time_entry, va_gc> *size_time_table;

  /* Predicate on when some loop in the function becomes to have known
     bounds.   */
  predicate * GTY((skip)) loop_iterations;
  /* Predicate on when some loop in the function becomes to have known
     stride.   */
  predicate * GTY((skip)) loop_stride;
  /* Predicate on when some array indexes become constants.  */
  predicate * GTY((skip)) array_index;
  /* Estimated growth for inlining all copies of the function before start
     of small functions inlining.
     This value will get out of date as the callers are duplicated, but
     using up-to-date value in the badness metric mean a lot of extra
     expenses.  */
  int growth;
  /* Number of SCC on the beginning of inlining process.  */
  int scc_no;

  /* Keep all field empty so summary dumping works during its computation.
     This is useful for debugging.  */
  ipa_fn_summary ()
    : estimated_self_stack_size (0), self_size (0), min_size (0),
      inlinable (false), single_caller (false),
      fp_expressions (false), estimated_stack_size (false),
      stack_frame_offset (false), time (0), size (0), conds (NULL),
      size_time_table (NULL), loop_iterations (NULL), loop_stride (NULL),
      array_index (NULL), growth (0), scc_no (0)
    {
    }

  /* Record time and size under given predicates.  */
  void account_size_time (int, sreal, const predicate &, const predicate &);

  /* Reset summary to empty state.  */
  void reset (struct cgraph_node *node);

  /* We keep values scaled up, so fractional sizes can be accounted.  */
  static const int size_scale = 2;
};

class GTY((user)) ipa_fn_summary_t: public function_summary <ipa_fn_summary *>
{
public:
  ipa_fn_summary_t (symbol_table *symtab, bool ggc):
    function_summary <ipa_fn_summary *> (symtab, ggc) {}

  static ipa_fn_summary_t *create_ggc (symbol_table *symtab)
  {
    struct ipa_fn_summary_t *summary = new (ggc_alloc <ipa_fn_summary_t> ())
      ipa_fn_summary_t(symtab, true);
    summary->disable_insertion_hook ();
    return summary;
  }


  virtual void insert (cgraph_node *, ipa_fn_summary *);
  virtual void remove (cgraph_node *node, ipa_fn_summary *);
  virtual void duplicate (cgraph_node *src, cgraph_node *dst,
			  ipa_fn_summary *src_data, ipa_fn_summary *dst_data);
};

extern GTY(()) function_summary <ipa_fn_summary *> *ipa_fn_summaries;

/* Information kept about callgraph edges.  */
struct ipa_call_summary
{
  class predicate *predicate;
  /* Vector indexed by parameters.  */
  vec<inline_param_summary> param;
  /* Estimated size and time of the call statement.  */
  int call_stmt_size;
  int call_stmt_time;
  /* Depth of loop nest, 0 means no nesting.  */
  unsigned int loop_depth;
  /* Indicates whether the caller returns the value of it's callee.  */
  bool is_return_callee_uncaptured;

  /* Keep all field empty so summary dumping works during its computation.
     This is useful for debugging.  */
  ipa_call_summary ()
    : predicate (NULL), param (vNULL), call_stmt_size (0), call_stmt_time (0),
      loop_depth (0)
    {
    }

  /* Reset inline summary to empty state.  */
  void reset ();
};

class ipa_call_summary_t: public call_summary <ipa_call_summary *>
{
public:
  ipa_call_summary_t (symbol_table *symtab, bool ggc):
    call_summary <ipa_call_summary *> (symtab, ggc) {}

  /* Hook that is called by summary when an edge is duplicated.  */
  virtual void remove (cgraph_edge *cs, ipa_call_summary *);
  /* Hook that is called by summary when an edge is duplicated.  */
  virtual void duplicate (cgraph_edge *src, cgraph_edge *dst,
			  ipa_call_summary *src_data,
			  ipa_call_summary *dst_data);
};

extern call_summary <ipa_call_summary *> *ipa_call_summaries;

/* In ipa-fnsummary.c  */
void ipa_debug_fn_summary (struct cgraph_node *);
void ipa_dump_fn_summaries (FILE *f);
void ipa_dump_fn_summary (FILE *f, struct cgraph_node *node);
void ipa_dump_hints (FILE *f, ipa_hints);
void ipa_free_fn_summary (void);
void inline_analyze_function (struct cgraph_node *node);
void estimate_ipcp_clone_size_and_time (struct cgraph_node *,
					vec<tree>,
					vec<ipa_polymorphic_call_context>,
					vec<ipa_agg_jump_function_p>,
					int *, sreal *, sreal *,
				        ipa_hints *);
void ipa_merge_fn_summary_after_inlining (struct cgraph_edge *edge);
void ipa_update_overall_fn_summary (struct cgraph_node *node);
void compute_fn_summary (struct cgraph_node *, bool);


void evaluate_properties_for_edge (struct cgraph_edge *e, bool inline_p,
				   clause_t *clause_ptr,
				   clause_t *nonspec_clause_ptr,
				   vec<tree> *known_vals_ptr,
				   vec<ipa_polymorphic_call_context>
				   *known_contexts_ptr,
				   vec<ipa_agg_jump_function_p> *);
void estimate_node_size_and_time (struct cgraph_node *node,
				  clause_t possible_truths,
				  clause_t nonspec_possible_truths,
				  vec<tree> known_vals,
				  vec<ipa_polymorphic_call_context>,
				  vec<ipa_agg_jump_function_p> known_aggs,
				  int *ret_size, int *ret_min_size,
				  sreal *ret_time,
				  sreal *ret_nonspecialized_time,
				  ipa_hints *ret_hints,
				  vec<inline_param_summary>
				  inline_param_summary);

void ipa_fnsummary_c_finalize (void);

#endif /* GCC_IPA_FNSUMMARY_H */
