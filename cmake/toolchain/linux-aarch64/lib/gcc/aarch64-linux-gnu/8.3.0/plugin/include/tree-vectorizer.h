/* Vectorizer
   Copyright (C) 2003-2018 Free Software Foundation, Inc.
   Contributed by Dorit Naishlos <dorit@il.ibm.com>

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

#ifndef GCC_TREE_VECTORIZER_H
#define GCC_TREE_VECTORIZER_H

#include "tree-data-ref.h"
#include "tree-hash-traits.h"
#include "target.h"

/* Used for naming of new temporaries.  */
enum vect_var_kind {
  vect_simple_var,
  vect_pointer_var,
  vect_scalar_var,
  vect_mask_var
};

/* Defines type of operation.  */
enum operation_type {
  unary_op = 1,
  binary_op,
  ternary_op
};

/* Define type of available alignment support.  */
enum dr_alignment_support {
  dr_unaligned_unsupported,
  dr_unaligned_supported,
  dr_explicit_realign,
  dr_explicit_realign_optimized,
  dr_aligned
};

/* Define type of def-use cross-iteration cycle.  */
enum vect_def_type {
  vect_uninitialized_def = 0,
  vect_constant_def = 1,
  vect_external_def,
  vect_internal_def,
  vect_induction_def,
  vect_reduction_def,
  vect_double_reduction_def,
  vect_nested_cycle,
  vect_unknown_def_type
};

/* Define type of reduction.  */
enum vect_reduction_type {
  TREE_CODE_REDUCTION,
  COND_REDUCTION,
  INTEGER_INDUC_COND_REDUCTION,
  CONST_COND_REDUCTION,

  /* Retain a scalar phi and use a FOLD_EXTRACT_LAST within the loop
     to implement:

       for (int i = 0; i < VF; ++i)
         res = cond[i] ? val[i] : res;  */
  EXTRACT_LAST_REDUCTION,

  /* Use a folding reduction within the loop to implement:

       for (int i = 0; i < VF; ++i)
	 res = res OP val[i];

     (with no reassocation).  */
  FOLD_LEFT_REDUCTION
};

#define VECTORIZABLE_CYCLE_DEF(D) (((D) == vect_reduction_def)           \
                                   || ((D) == vect_double_reduction_def) \
                                   || ((D) == vect_nested_cycle))

/* Structure to encapsulate information about a group of like
   instructions to be presented to the target cost model.  */
struct stmt_info_for_cost {
  int count;
  enum vect_cost_for_stmt kind;
  gimple *stmt;
  int misalign;
};

typedef vec<stmt_info_for_cost> stmt_vector_for_cost;

/* Maps base addresses to an innermost_loop_behavior that gives the maximum
   known alignment for that base.  */
typedef hash_map<tree_operand_hash,
		 innermost_loop_behavior *> vec_base_alignments;

/************************************************************************
  SLP
 ************************************************************************/
typedef struct _slp_tree *slp_tree;

/* A computation tree of an SLP instance.  Each node corresponds to a group of
   stmts to be packed in a SIMD stmt.  */
struct _slp_tree {
  /* Nodes that contain def-stmts of this node statements operands.  */
  vec<slp_tree> children;
  /* A group of scalar stmts to be vectorized together.  */
  vec<gimple *> stmts;
  /* Load permutation relative to the stores, NULL if there is no
     permutation.  */
  vec<unsigned> load_permutation;
  /* Vectorized stmt/s.  */
  vec<gimple *> vec_stmts;
  /* Number of vector stmts that are created to replace the group of scalar
     stmts. It is calculated during the transformation phase as the number of
     scalar elements in one scalar iteration (GROUP_SIZE) multiplied by VF
     divided by vector size.  */
  unsigned int vec_stmts_size;
  /* Whether the scalar computations use two different operators.  */
  bool two_operators;
  /* The DEF type of this node.  */
  enum vect_def_type def_type;
};


/* SLP instance is a sequence of stmts in a loop that can be packed into
   SIMD stmts.  */
typedef struct _slp_instance {
  /* The root of SLP tree.  */
  slp_tree root;

  /* Size of groups of scalar stmts that will be replaced by SIMD stmt/s.  */
  unsigned int group_size;

  /* The unrolling factor required to vectorized this SLP instance.  */
  poly_uint64 unrolling_factor;

  /* The group of nodes that contain loads of this SLP instance.  */
  vec<slp_tree> loads;

  /* The SLP node containing the reduction PHIs.  */
  slp_tree reduc_phis;
} *slp_instance;


/* Access Functions.  */
#define SLP_INSTANCE_TREE(S)                     (S)->root
#define SLP_INSTANCE_GROUP_SIZE(S)               (S)->group_size
#define SLP_INSTANCE_UNROLLING_FACTOR(S)         (S)->unrolling_factor
#define SLP_INSTANCE_LOADS(S)                    (S)->loads

#define SLP_TREE_CHILDREN(S)                     (S)->children
#define SLP_TREE_SCALAR_STMTS(S)                 (S)->stmts
#define SLP_TREE_VEC_STMTS(S)                    (S)->vec_stmts
#define SLP_TREE_NUMBER_OF_VEC_STMTS(S)          (S)->vec_stmts_size
#define SLP_TREE_LOAD_PERMUTATION(S)             (S)->load_permutation
#define SLP_TREE_TWO_OPERATORS(S)		 (S)->two_operators
#define SLP_TREE_DEF_TYPE(S)			 (S)->def_type



/* Describes two objects whose addresses must be unequal for the vectorized
   loop to be valid.  */
typedef std::pair<tree, tree> vec_object_pair;

/* Records that vectorization is only possible if abs (EXPR) >= MIN_VALUE.
   UNSIGNED_P is true if we can assume that abs (EXPR) == EXPR.  */
struct vec_lower_bound {
  vec_lower_bound () {}
  vec_lower_bound (tree e, bool u, poly_uint64 m)
    : expr (e), unsigned_p (u), min_value (m) {}

  tree expr;
  bool unsigned_p;
  poly_uint64 min_value;
};

/* Vectorizer state common between loop and basic-block vectorization.  */
struct vec_info {
  enum vec_kind { bb, loop };

  vec_info (vec_kind, void *);
  ~vec_info ();

  /* The type of vectorization.  */
  vec_kind kind;

  /* All SLP instances.  */
  auto_vec<slp_instance> slp_instances;

  /* All data references.  Freed by free_data_refs, so not an auto_vec.  */
  vec<data_reference_p> datarefs;

  /* Maps base addresses to an innermost_loop_behavior that gives the maximum
     known alignment for that base.  */
  vec_base_alignments base_alignments;

  /* All data dependences.  Freed by free_dependence_relations, so not
     an auto_vec.  */
  vec<ddr_p> ddrs;

  /* All interleaving chains of stores, represented by the first
     stmt in the chain.  */
  auto_vec<gimple *> grouped_stores;

  /* Cost data used by the target cost model.  */
  void *target_cost_data;
};

struct _loop_vec_info;
struct _bb_vec_info;

template<>
template<>
inline bool
is_a_helper <_loop_vec_info *>::test (vec_info *i)
{
  return i->kind == vec_info::loop;
}

template<>
template<>
inline bool
is_a_helper <_bb_vec_info *>::test (vec_info *i)
{
  return i->kind == vec_info::bb;
}


/* In general, we can divide the vector statements in a vectorized loop
   into related groups ("rgroups") and say that for each rgroup there is
   some nS such that the rgroup operates on nS values from one scalar
   iteration followed by nS values from the next.  That is, if VF is the
   vectorization factor of the loop, the rgroup operates on a sequence:

     (1,1) (1,2) ... (1,nS) (2,1) ... (2,nS) ... (VF,1) ... (VF,nS)

   where (i,j) represents a scalar value with index j in a scalar
   iteration with index i.

   [ We use the term "rgroup" to emphasise that this grouping isn't
     necessarily the same as the grouping of statements used elsewhere.
     For example, if we implement a group of scalar loads using gather
     loads, we'll use a separate gather load for each scalar load, and
     thus each gather load will belong to its own rgroup. ]

   In general this sequence will occupy nV vectors concatenated
   together.  If these vectors have nL lanes each, the total number
   of scalar values N is given by:

       N = nS * VF = nV * nL

   None of nS, VF, nV and nL are required to be a power of 2.  nS and nV
   are compile-time constants but VF and nL can be variable (if the target
   supports variable-length vectors).

   In classical vectorization, each iteration of the vector loop would
   handle exactly VF iterations of the original scalar loop.  However,
   in a fully-masked loop, a particular iteration of the vector loop
   might handle fewer than VF iterations of the scalar loop.  The vector
   lanes that correspond to iterations of the scalar loop are said to be
   "active" and the other lanes are said to be "inactive".

   In a fully-masked loop, many rgroups need to be masked to ensure that
   they have no effect for the inactive lanes.  Each such rgroup needs a
   sequence of booleans in the same order as above, but with each (i,j)
   replaced by a boolean that indicates whether iteration i is active.
   This sequence occupies nV vector masks that again have nL lanes each.
   Thus the mask sequence as a whole consists of VF independent booleans
   that are each repeated nS times.

   We make the simplifying assumption that if a sequence of nV masks is
   suitable for one (nS,nL) pair, we can reuse it for (nS/2,nL/2) by
   VIEW_CONVERTing it.  This holds for all current targets that support
   fully-masked loops.  For example, suppose the scalar loop is:

     float *f;
     double *d;
     for (int i = 0; i < n; ++i)
       {
	 f[i * 2 + 0] += 1.0f;
	 f[i * 2 + 1] += 2.0f;
	 d[i] += 3.0;
       }

   and suppose that vectors have 256 bits.  The vectorized f accesses
   will belong to one rgroup and the vectorized d access to another:

     f rgroup: nS = 2, nV = 1, nL = 8
     d rgroup: nS = 1, nV = 1, nL = 4
	       VF = 4

     [ In this simple example the rgroups do correspond to the normal
       SLP grouping scheme. ]

   If only the first three lanes are active, the masks we need are:

     f rgroup: 1 1 | 1 1 | 1 1 | 0 0
     d rgroup:  1  |  1  |  1  |  0

   Here we can use a mask calculated for f's rgroup for d's, but not
   vice versa.

   Thus for each value of nV, it is enough to provide nV masks, with the
   mask being calculated based on the highest nL (or, equivalently, based
   on the highest nS) required by any rgroup with that nV.  We therefore
   represent the entire collection of masks as a two-level table, with the
   first level being indexed by nV - 1 (since nV == 0 doesn't exist) and
   the second being indexed by the mask index 0 <= i < nV.  */

/* The masks needed by rgroups with nV vectors, according to the
   description above.  */
struct rgroup_masks {
  /* The largest nS for all rgroups that use these masks.  */
  unsigned int max_nscalars_per_iter;

  /* The type of mask to use, based on the highest nS recorded above.  */
  tree mask_type;

  /* A vector of nV masks, in iteration order.  */
  vec<tree> masks;
};

typedef auto_vec<rgroup_masks> vec_loop_masks;

/*-----------------------------------------------------------------*/
/* Info on vectorized loops.                                       */
/*-----------------------------------------------------------------*/
typedef struct _loop_vec_info : public vec_info {
  _loop_vec_info (struct loop *);
  ~_loop_vec_info ();

  /* The loop to which this info struct refers to.  */
  struct loop *loop;

  /* The loop basic blocks.  */
  basic_block *bbs;

  /* Number of latch executions.  */
  tree num_itersm1;
  /* Number of iterations.  */
  tree num_iters;
  /* Number of iterations of the original loop.  */
  tree num_iters_unchanged;
  /* Condition under which this loop is analyzed and versioned.  */
  tree num_iters_assumptions;

  /* Threshold of number of iterations below which vectorzation will not be
     performed. It is calculated from MIN_PROFITABLE_ITERS and
     PARAM_MIN_VECT_LOOP_BOUND.  */
  unsigned int th;

  /* When applying loop versioning, the vector form should only be used
     if the number of scalar iterations is >= this value, on top of all
     the other requirements.  Ignored when loop versioning is not being
     used.  */
  poly_uint64 versioning_threshold;

  /* Unrolling factor  */
  poly_uint64 vectorization_factor;

  /* Maximum runtime vectorization factor, or MAX_VECTORIZATION_FACTOR
     if there is no particular limit.  */
  unsigned HOST_WIDE_INT max_vectorization_factor;

  /* The masks that a fully-masked loop should use to avoid operating
     on inactive scalars.  */
  vec_loop_masks masks;

  /* If we are using a loop mask to align memory addresses, this variable
     contains the number of vector elements that we should skip in the
     first iteration of the vector loop (i.e. the number of leading
     elements that should be false in the first mask).  */
  tree mask_skip_niters;

  /* Type of the variables to use in the WHILE_ULT call for fully-masked
     loops.  */
  tree mask_compare_type;

  /* Unknown DRs according to which loop was peeled.  */
  struct data_reference *unaligned_dr;

  /* peeling_for_alignment indicates whether peeling for alignment will take
     place, and what the peeling factor should be:
     peeling_for_alignment = X means:
        If X=0: Peeling for alignment will not be applied.
        If X>0: Peel first X iterations.
        If X=-1: Generate a runtime test to calculate the number of iterations
                 to be peeled, using the dataref recorded in the field
                 unaligned_dr.  */
  int peeling_for_alignment;

  /* The mask used to check the alignment of pointers or arrays.  */
  int ptr_mask;

  /* The loop nest in which the data dependences are computed.  */
  auto_vec<loop_p> loop_nest;

  /* Data Dependence Relations defining address ranges that are candidates
     for a run-time aliasing check.  */
  auto_vec<ddr_p> may_alias_ddrs;

  /* Data Dependence Relations defining address ranges together with segment
     lengths from which the run-time aliasing check is built.  */
  auto_vec<dr_with_seg_len_pair_t> comp_alias_ddrs;

  /* Check that the addresses of each pair of objects is unequal.  */
  auto_vec<vec_object_pair> check_unequal_addrs;

  /* List of values that are required to be nonzero.  This is used to check
     whether things like "x[i * n] += 1;" are safe and eventually gets added
     to the checks for lower bounds below.  */
  auto_vec<tree> check_nonzero;

  /* List of values that need to be checked for a minimum value.  */
  auto_vec<vec_lower_bound> lower_bounds;

  /* Statements in the loop that have data references that are candidates for a
     runtime (loop versioning) misalignment check.  */
  auto_vec<gimple *> may_misalign_stmts;

  /* Reduction cycles detected in the loop. Used in loop-aware SLP.  */
  auto_vec<gimple *> reductions;

  /* All reduction chains in the loop, represented by the first
     stmt in the chain.  */
  auto_vec<gimple *> reduction_chains;

  /* Cost vector for a single scalar iteration.  */
  auto_vec<stmt_info_for_cost> scalar_cost_vec;

  /* Map of IV base/step expressions to inserted name in the preheader.  */
  hash_map<tree_operand_hash, tree> *ivexpr_map;

  /* The unrolling factor needed to SLP the loop. In case of that pure SLP is
     applied to the loop, i.e., no unrolling is needed, this is 1.  */
  poly_uint64 slp_unrolling_factor;

  /* Cost of a single scalar iteration.  */
  int single_scalar_iteration_cost;

  /* Is the loop vectorizable? */
  bool vectorizable;

  /* Records whether we still have the option of using a fully-masked loop.  */
  bool can_fully_mask_p;

  /* True if have decided to use a fully-masked loop.  */
  bool fully_masked_p;

  /* When we have grouped data accesses with gaps, we may introduce invalid
     memory accesses.  We peel the last iteration of the loop to prevent
     this.  */
  bool peeling_for_gaps;

  /* When the number of iterations is not a multiple of the vector size
     we need to peel off iterations at the end to form an epilogue loop.  */
  bool peeling_for_niter;

  /* Reductions are canonicalized so that the last operand is the reduction
     operand.  If this places a constant into RHS1, this decanonicalizes
     GIMPLE for other phases, so we must track when this has occurred and
     fix it up.  */
  bool operands_swapped;

  /* True if there are no loop carried data dependencies in the loop.
     If loop->safelen <= 1, then this is always true, either the loop
     didn't have any loop carried data dependencies, or the loop is being
     vectorized guarded with some runtime alias checks, or couldn't
     be vectorized at all, but then this field shouldn't be used.
     For loop->safelen >= 2, the user has asserted that there are no
     backward dependencies, but there still could be loop carried forward
     dependencies in such loops.  This flag will be false if normal
     vectorizer data dependency analysis would fail or require versioning
     for alias, but because of loop->safelen >= 2 it has been vectorized
     even without versioning for alias.  E.g. in:
     #pragma omp simd
     for (int i = 0; i < m; i++)
       a[i] = a[i + k] * c;
     (or #pragma simd or #pragma ivdep) we can vectorize this and it will
     DTRT even for k > 0 && k < m, but without safelen we would not
     vectorize this, so this field would be false.  */
  bool no_data_dependencies;

  /* Mark loops having masked stores.  */
  bool has_mask_store;

  /* If if-conversion versioned this loop before conversion, this is the
     loop version without if-conversion.  */
  struct loop *scalar_loop;

  /* For loops being epilogues of already vectorized loops
     this points to the original vectorized loop.  Otherwise NULL.  */
  _loop_vec_info *orig_loop_info;

} *loop_vec_info;

/* Access Functions.  */
#define LOOP_VINFO_LOOP(L)                 (L)->loop
#define LOOP_VINFO_BBS(L)                  (L)->bbs
#define LOOP_VINFO_NITERSM1(L)             (L)->num_itersm1
#define LOOP_VINFO_NITERS(L)               (L)->num_iters
/* Since LOOP_VINFO_NITERS and LOOP_VINFO_NITERSM1 can change after
   prologue peeling retain total unchanged scalar loop iterations for
   cost model.  */
#define LOOP_VINFO_NITERS_UNCHANGED(L)     (L)->num_iters_unchanged
#define LOOP_VINFO_NITERS_ASSUMPTIONS(L)   (L)->num_iters_assumptions
#define LOOP_VINFO_COST_MODEL_THRESHOLD(L) (L)->th
#define LOOP_VINFO_VERSIONING_THRESHOLD(L) (L)->versioning_threshold
#define LOOP_VINFO_VECTORIZABLE_P(L)       (L)->vectorizable
#define LOOP_VINFO_CAN_FULLY_MASK_P(L)     (L)->can_fully_mask_p
#define LOOP_VINFO_FULLY_MASKED_P(L)       (L)->fully_masked_p
#define LOOP_VINFO_VECT_FACTOR(L)          (L)->vectorization_factor
#define LOOP_VINFO_MAX_VECT_FACTOR(L)      (L)->max_vectorization_factor
#define LOOP_VINFO_MASKS(L)                (L)->masks
#define LOOP_VINFO_MASK_SKIP_NITERS(L)     (L)->mask_skip_niters
#define LOOP_VINFO_MASK_COMPARE_TYPE(L)    (L)->mask_compare_type
#define LOOP_VINFO_PTR_MASK(L)             (L)->ptr_mask
#define LOOP_VINFO_LOOP_NEST(L)            (L)->loop_nest
#define LOOP_VINFO_DATAREFS(L)             (L)->datarefs
#define LOOP_VINFO_DDRS(L)                 (L)->ddrs
#define LOOP_VINFO_INT_NITERS(L)           (TREE_INT_CST_LOW ((L)->num_iters))
#define LOOP_VINFO_PEELING_FOR_ALIGNMENT(L) (L)->peeling_for_alignment
#define LOOP_VINFO_UNALIGNED_DR(L)         (L)->unaligned_dr
#define LOOP_VINFO_MAY_MISALIGN_STMTS(L)   (L)->may_misalign_stmts
#define LOOP_VINFO_MAY_ALIAS_DDRS(L)       (L)->may_alias_ddrs
#define LOOP_VINFO_COMP_ALIAS_DDRS(L)      (L)->comp_alias_ddrs
#define LOOP_VINFO_CHECK_UNEQUAL_ADDRS(L)  (L)->check_unequal_addrs
#define LOOP_VINFO_CHECK_NONZERO(L)        (L)->check_nonzero
#define LOOP_VINFO_LOWER_BOUNDS(L)         (L)->lower_bounds
#define LOOP_VINFO_GROUPED_STORES(L)       (L)->grouped_stores
#define LOOP_VINFO_SLP_INSTANCES(L)        (L)->slp_instances
#define LOOP_VINFO_SLP_UNROLLING_FACTOR(L) (L)->slp_unrolling_factor
#define LOOP_VINFO_REDUCTIONS(L)           (L)->reductions
#define LOOP_VINFO_REDUCTION_CHAINS(L)     (L)->reduction_chains
#define LOOP_VINFO_TARGET_COST_DATA(L)     (L)->target_cost_data
#define LOOP_VINFO_PEELING_FOR_GAPS(L)     (L)->peeling_for_gaps
#define LOOP_VINFO_OPERANDS_SWAPPED(L)     (L)->operands_swapped
#define LOOP_VINFO_PEELING_FOR_NITER(L)    (L)->peeling_for_niter
#define LOOP_VINFO_NO_DATA_DEPENDENCIES(L) (L)->no_data_dependencies
#define LOOP_VINFO_SCALAR_LOOP(L)	   (L)->scalar_loop
#define LOOP_VINFO_HAS_MASK_STORE(L)       (L)->has_mask_store
#define LOOP_VINFO_SCALAR_ITERATION_COST(L) (L)->scalar_cost_vec
#define LOOP_VINFO_SINGLE_SCALAR_ITERATION_COST(L) (L)->single_scalar_iteration_cost
#define LOOP_VINFO_ORIG_LOOP_INFO(L)       (L)->orig_loop_info

#define LOOP_REQUIRES_VERSIONING_FOR_ALIGNMENT(L)	\
  ((L)->may_misalign_stmts.length () > 0)
#define LOOP_REQUIRES_VERSIONING_FOR_ALIAS(L)		\
  ((L)->comp_alias_ddrs.length () > 0 \
   || (L)->check_unequal_addrs.length () > 0 \
   || (L)->lower_bounds.length () > 0)
#define LOOP_REQUIRES_VERSIONING_FOR_NITERS(L)		\
  (LOOP_VINFO_NITERS_ASSUMPTIONS (L))
#define LOOP_REQUIRES_VERSIONING(L)			\
  (LOOP_REQUIRES_VERSIONING_FOR_ALIGNMENT (L)		\
   || LOOP_REQUIRES_VERSIONING_FOR_ALIAS (L)		\
   || LOOP_REQUIRES_VERSIONING_FOR_NITERS (L))

#define LOOP_VINFO_NITERS_KNOWN_P(L)          \
  (tree_fits_shwi_p ((L)->num_iters) && tree_to_shwi ((L)->num_iters) > 0)

#define LOOP_VINFO_EPILOGUE_P(L) \
  (LOOP_VINFO_ORIG_LOOP_INFO (L) != NULL)

#define LOOP_VINFO_ORIG_MAX_VECT_FACTOR(L) \
  (LOOP_VINFO_MAX_VECT_FACTOR (LOOP_VINFO_ORIG_LOOP_INFO (L)))

static inline loop_vec_info
loop_vec_info_for_loop (struct loop *loop)
{
  return (loop_vec_info) loop->aux;
}

static inline bool
nested_in_vect_loop_p (struct loop *loop, gimple *stmt)
{
  return (loop->inner
          && (loop->inner == (gimple_bb (stmt))->loop_father));
}

typedef struct _bb_vec_info : public vec_info
{
  _bb_vec_info (gimple_stmt_iterator, gimple_stmt_iterator);
  ~_bb_vec_info ();

  basic_block bb;
  gimple_stmt_iterator region_begin;
  gimple_stmt_iterator region_end;
} *bb_vec_info;

#define BB_VINFO_BB(B)               (B)->bb
#define BB_VINFO_GROUPED_STORES(B)   (B)->grouped_stores
#define BB_VINFO_SLP_INSTANCES(B)    (B)->slp_instances
#define BB_VINFO_DATAREFS(B)         (B)->datarefs
#define BB_VINFO_DDRS(B)             (B)->ddrs
#define BB_VINFO_TARGET_COST_DATA(B) (B)->target_cost_data

static inline bb_vec_info
vec_info_for_bb (basic_block bb)
{
  return (bb_vec_info) bb->aux;
}

/*-----------------------------------------------------------------*/
/* Info on vectorized defs.                                        */
/*-----------------------------------------------------------------*/
enum stmt_vec_info_type {
  undef_vec_info_type = 0,
  load_vec_info_type,
  store_vec_info_type,
  shift_vec_info_type,
  op_vec_info_type,
  call_vec_info_type,
  call_simd_clone_vec_info_type,
  assignment_vec_info_type,
  condition_vec_info_type,
  comparison_vec_info_type,
  reduc_vec_info_type,
  induc_vec_info_type,
  type_promotion_vec_info_type,
  type_demotion_vec_info_type,
  type_conversion_vec_info_type,
  loop_exit_ctrl_vec_info_type
};

/* Indicates whether/how a variable is used in the scope of loop/basic
   block.  */
enum vect_relevant {
  vect_unused_in_scope = 0,

  /* The def is only used outside the loop.  */
  vect_used_only_live,
  /* The def is in the inner loop, and the use is in the outer loop, and the
     use is a reduction stmt.  */
  vect_used_in_outer_by_reduction,
  /* The def is in the inner loop, and the use is in the outer loop (and is
     not part of reduction).  */
  vect_used_in_outer,

  /* defs that feed computations that end up (only) in a reduction. These
     defs may be used by non-reduction stmts, but eventually, any
     computations/values that are affected by these defs are used to compute
     a reduction (i.e. don't get stored to memory, for example). We use this
     to identify computations that we can change the order in which they are
     computed.  */
  vect_used_by_reduction,

  vect_used_in_scope
};

/* The type of vectorization that can be applied to the stmt: regular loop-based
   vectorization; pure SLP - the stmt is a part of SLP instances and does not
   have uses outside SLP instances; or hybrid SLP and loop-based - the stmt is
   a part of SLP instance and also must be loop-based vectorized, since it has
   uses outside SLP sequences.

   In the loop context the meanings of pure and hybrid SLP are slightly
   different. By saying that pure SLP is applied to the loop, we mean that we
   exploit only intra-iteration parallelism in the loop; i.e., the loop can be
   vectorized without doing any conceptual unrolling, cause we don't pack
   together stmts from different iterations, only within a single iteration.
   Loop hybrid SLP means that we exploit both intra-iteration and
   inter-iteration parallelism (e.g., number of elements in the vector is 4
   and the slp-group-size is 2, in which case we don't have enough parallelism
   within an iteration, so we obtain the rest of the parallelism from subsequent
   iterations by unrolling the loop by 2).  */
enum slp_vect_type {
  loop_vect = 0,
  pure_slp,
  hybrid
};

/* Says whether a statement is a load, a store of a vectorized statement
   result, or a store of an invariant value.  */
enum vec_load_store_type {
  VLS_LOAD,
  VLS_STORE,
  VLS_STORE_INVARIANT
};

/* Describes how we're going to vectorize an individual load or store,
   or a group of loads or stores.  */
enum vect_memory_access_type {
  /* An access to an invariant address.  This is used only for loads.  */
  VMAT_INVARIANT,

  /* A simple contiguous access.  */
  VMAT_CONTIGUOUS,

  /* A contiguous access that goes down in memory rather than up,
     with no additional permutation.  This is used only for stores
     of invariants.  */
  VMAT_CONTIGUOUS_DOWN,

  /* A simple contiguous access in which the elements need to be permuted
     after loading or before storing.  Only used for loop vectorization;
     SLP uses separate permutes.  */
  VMAT_CONTIGUOUS_PERMUTE,

  /* A simple contiguous access in which the elements need to be reversed
     after loading or before storing.  */
  VMAT_CONTIGUOUS_REVERSE,

  /* An access that uses IFN_LOAD_LANES or IFN_STORE_LANES.  */
  VMAT_LOAD_STORE_LANES,

  /* An access in which each scalar element is loaded or stored
     individually.  */
  VMAT_ELEMENTWISE,

  /* A hybrid of VMAT_CONTIGUOUS and VMAT_ELEMENTWISE, used for grouped
     SLP accesses.  Each unrolled iteration uses a contiguous load
     or store for the whole group, but the groups from separate iterations
     are combined in the same way as for VMAT_ELEMENTWISE.  */
  VMAT_STRIDED_SLP,

  /* The access uses gather loads or scatter stores.  */
  VMAT_GATHER_SCATTER
};

typedef struct data_reference *dr_p;

typedef struct _stmt_vec_info {

  enum stmt_vec_info_type type;

  /* Indicates whether this stmts is part of a computation whose result is
     used outside the loop.  */
  bool live;

  /* Stmt is part of some pattern (computation idiom)  */
  bool in_pattern_p;

  /* Is this statement vectorizable or should it be skipped in (partial)
     vectorization.  */
  bool vectorizable;

  /* The stmt to which this info struct refers to.  */
  gimple *stmt;

  /* The vec_info with respect to which STMT is vectorized.  */
  vec_info *vinfo;

  /* The vector type to be used for the LHS of this statement.  */
  tree vectype;

  /* The vectorized version of the stmt.  */
  gimple *vectorized_stmt;


  /* The following is relevant only for stmts that contain a non-scalar
     data-ref (array/pointer/struct access). A GIMPLE stmt is expected to have
     at most one such data-ref.  */

  /* Information about the data-ref (access function, etc),
     relative to the inner-most containing loop.  */
  struct data_reference *data_ref_info;

  /* Information about the data-ref relative to this loop
     nest (the loop that is being considered for vectorization).  */
  innermost_loop_behavior dr_wrt_vec_loop;

  /* For loop PHI nodes, the base and evolution part of it.  This makes sure
     this information is still available in vect_update_ivs_after_vectorizer
     where we may not be able to re-analyze the PHI nodes evolution as
     peeling for the prologue loop can make it unanalyzable.  The evolution
     part is still correct after peeling, but the base may have changed from
     the version here.  */
  tree loop_phi_evolution_base_unchanged;
  tree loop_phi_evolution_part;

  /* Used for various bookkeeping purposes, generally holding a pointer to
     some other stmt S that is in some way "related" to this stmt.
     Current use of this field is:
        If this stmt is part of a pattern (i.e. the field 'in_pattern_p' is
        true): S is the "pattern stmt" that represents (and replaces) the
        sequence of stmts that constitutes the pattern.  Similarly, the
        related_stmt of the "pattern stmt" points back to this stmt (which is
        the last stmt in the original sequence of stmts that constitutes the
        pattern).  */
  gimple *related_stmt;

  /* Used to keep a sequence of def stmts of a pattern stmt if such exists.  */
  gimple_seq pattern_def_seq;

  /* List of datarefs that are known to have the same alignment as the dataref
     of this stmt.  */
  vec<dr_p> same_align_refs;

  /* Selected SIMD clone's function info.  First vector element
     is SIMD clone's function decl, followed by a pair of trees (base + step)
     for linear arguments (pair of NULLs for other arguments).  */
  vec<tree> simd_clone_info;

  /* Classify the def of this stmt.  */
  enum vect_def_type def_type;

  /*  Whether the stmt is SLPed, loop-based vectorized, or both.  */
  enum slp_vect_type slp_type;

  /* Interleaving and reduction chains info.  */
  /* First element in the group.  */
  gimple *first_element;
  /* Pointer to the next element in the group.  */
  gimple *next_element;
  /* For data-refs, in case that two or more stmts share data-ref, this is the
     pointer to the previously detected stmt with the same dr.  */
  gimple *same_dr_stmt;
  /* The size of the group.  */
  unsigned int size;
  /* For stores, number of stores from this group seen. We vectorize the last
     one.  */
  unsigned int store_count;
  /* For loads only, the gap from the previous load. For consecutive loads, GAP
     is 1.  */
  unsigned int gap;

  /* The minimum negative dependence distance this stmt participates in
     or zero if none.  */
  unsigned int min_neg_dist;

  /* Not all stmts in the loop need to be vectorized. e.g, the increment
     of the loop induction variable and computation of array indexes. relevant
     indicates whether the stmt needs to be vectorized.  */
  enum vect_relevant relevant;

  /* For loads if this is a gather, for stores if this is a scatter.  */
  bool gather_scatter_p;

  /* True if this is an access with loop-invariant stride.  */
  bool strided_p;

  /* For both loads and stores.  */
  bool simd_lane_access_p;

  /* Classifies how the load or store is going to be implemented
     for loop vectorization.  */
  vect_memory_access_type memory_access_type;

  /* For reduction loops, this is the type of reduction.  */
  enum vect_reduction_type v_reduc_type;

  /* For CONST_COND_REDUCTION, record the reduc code.  */
  enum tree_code const_cond_reduc_code;

  /* On a reduction PHI the reduction type as detected by
     vect_force_simple_reduction.  */
  enum vect_reduction_type reduc_type;

  /* On a reduction PHI the def returned by vect_force_simple_reduction.
     On the def returned by vect_force_simple_reduction the
     corresponding PHI.  */
  gimple *reduc_def;

  /* The number of scalar stmt references from active SLP instances.  */
  unsigned int num_slp_uses;
} *stmt_vec_info;

/* Information about a gather/scatter call.  */
struct gather_scatter_info {
  /* The internal function to use for the gather/scatter operation,
     or IFN_LAST if a built-in function should be used instead.  */
  internal_fn ifn;

  /* The FUNCTION_DECL for the built-in gather/scatter function,
     or null if an internal function should be used instead.  */
  tree decl;

  /* The loop-invariant base value.  */
  tree base;

  /* The original scalar offset, which is a non-loop-invariant SSA_NAME.  */
  tree offset;

  /* Each offset element should be multiplied by this amount before
     being added to the base.  */
  int scale;

  /* The definition type for the vectorized offset.  */
  enum vect_def_type offset_dt;

  /* The type of the vectorized offset.  */
  tree offset_vectype;

  /* The type of the scalar elements after loading or before storing.  */
  tree element_type;

  /* The type of the scalar elements being loaded or stored.  */
  tree memory_type;
};

/* Access Functions.  */
#define STMT_VINFO_TYPE(S)                 (S)->type
#define STMT_VINFO_STMT(S)                 (S)->stmt
inline loop_vec_info
STMT_VINFO_LOOP_VINFO (stmt_vec_info stmt_vinfo)
{
  if (loop_vec_info loop_vinfo = dyn_cast <loop_vec_info> (stmt_vinfo->vinfo))
    return loop_vinfo;
  return NULL;
}
inline bb_vec_info
STMT_VINFO_BB_VINFO (stmt_vec_info stmt_vinfo)
{
  if (bb_vec_info bb_vinfo = dyn_cast <bb_vec_info> (stmt_vinfo->vinfo))
    return bb_vinfo;
  return NULL;
}
#define STMT_VINFO_RELEVANT(S)             (S)->relevant
#define STMT_VINFO_LIVE_P(S)               (S)->live
#define STMT_VINFO_VECTYPE(S)              (S)->vectype
#define STMT_VINFO_VEC_STMT(S)             (S)->vectorized_stmt
#define STMT_VINFO_VECTORIZABLE(S)         (S)->vectorizable
#define STMT_VINFO_DATA_REF(S)             (S)->data_ref_info
#define STMT_VINFO_GATHER_SCATTER_P(S)	   (S)->gather_scatter_p
#define STMT_VINFO_STRIDED_P(S)	   	   (S)->strided_p
#define STMT_VINFO_MEMORY_ACCESS_TYPE(S)   (S)->memory_access_type
#define STMT_VINFO_SIMD_LANE_ACCESS_P(S)   (S)->simd_lane_access_p
#define STMT_VINFO_VEC_REDUCTION_TYPE(S)   (S)->v_reduc_type
#define STMT_VINFO_VEC_CONST_COND_REDUC_CODE(S) (S)->const_cond_reduc_code

#define STMT_VINFO_DR_WRT_VEC_LOOP(S)      (S)->dr_wrt_vec_loop
#define STMT_VINFO_DR_BASE_ADDRESS(S)      (S)->dr_wrt_vec_loop.base_address
#define STMT_VINFO_DR_INIT(S)              (S)->dr_wrt_vec_loop.init
#define STMT_VINFO_DR_OFFSET(S)            (S)->dr_wrt_vec_loop.offset
#define STMT_VINFO_DR_STEP(S)              (S)->dr_wrt_vec_loop.step
#define STMT_VINFO_DR_BASE_ALIGNMENT(S)    (S)->dr_wrt_vec_loop.base_alignment
#define STMT_VINFO_DR_BASE_MISALIGNMENT(S) \
  (S)->dr_wrt_vec_loop.base_misalignment
#define STMT_VINFO_DR_OFFSET_ALIGNMENT(S) \
  (S)->dr_wrt_vec_loop.offset_alignment
#define STMT_VINFO_DR_STEP_ALIGNMENT(S) \
  (S)->dr_wrt_vec_loop.step_alignment

#define STMT_VINFO_IN_PATTERN_P(S)         (S)->in_pattern_p
#define STMT_VINFO_RELATED_STMT(S)         (S)->related_stmt
#define STMT_VINFO_PATTERN_DEF_SEQ(S)      (S)->pattern_def_seq
#define STMT_VINFO_SAME_ALIGN_REFS(S)      (S)->same_align_refs
#define STMT_VINFO_SIMD_CLONE_INFO(S)	   (S)->simd_clone_info
#define STMT_VINFO_DEF_TYPE(S)             (S)->def_type
#define STMT_VINFO_GROUP_FIRST_ELEMENT(S)  (S)->first_element
#define STMT_VINFO_GROUP_NEXT_ELEMENT(S)   (S)->next_element
#define STMT_VINFO_GROUP_SIZE(S)           (S)->size
#define STMT_VINFO_GROUP_STORE_COUNT(S)    (S)->store_count
#define STMT_VINFO_GROUP_GAP(S)            (S)->gap
#define STMT_VINFO_GROUP_SAME_DR_STMT(S)   (S)->same_dr_stmt
#define STMT_VINFO_GROUPED_ACCESS(S)      ((S)->first_element != NULL && (S)->data_ref_info)
#define STMT_VINFO_LOOP_PHI_EVOLUTION_BASE_UNCHANGED(S) (S)->loop_phi_evolution_base_unchanged
#define STMT_VINFO_LOOP_PHI_EVOLUTION_PART(S) (S)->loop_phi_evolution_part
#define STMT_VINFO_MIN_NEG_DIST(S)	(S)->min_neg_dist
#define STMT_VINFO_NUM_SLP_USES(S)	(S)->num_slp_uses
#define STMT_VINFO_REDUC_TYPE(S)	(S)->reduc_type
#define STMT_VINFO_REDUC_DEF(S)		(S)->reduc_def

#define GROUP_FIRST_ELEMENT(S)          (S)->first_element
#define GROUP_NEXT_ELEMENT(S)           (S)->next_element
#define GROUP_SIZE(S)                   (S)->size
#define GROUP_STORE_COUNT(S)            (S)->store_count
#define GROUP_GAP(S)                    (S)->gap
#define GROUP_SAME_DR_STMT(S)           (S)->same_dr_stmt

#define STMT_VINFO_RELEVANT_P(S)          ((S)->relevant != vect_unused_in_scope)

#define HYBRID_SLP_STMT(S)                ((S)->slp_type == hybrid)
#define PURE_SLP_STMT(S)                  ((S)->slp_type == pure_slp)
#define STMT_SLP_TYPE(S)                   (S)->slp_type

struct dataref_aux {
  /* The misalignment in bytes of the reference, or -1 if not known.  */
  int misalignment;
  /* The byte alignment that we'd ideally like the reference to have,
     and the value that misalignment is measured against.  */
  int target_alignment;
  /* If true the alignment of base_decl needs to be increased.  */
  bool base_misaligned;
  tree base_decl;
};

#define DR_VECT_AUX(dr) ((dataref_aux *)(dr)->aux)

#define VECT_MAX_COST 1000

/* The maximum number of intermediate steps required in multi-step type
   conversion.  */
#define MAX_INTERM_CVT_STEPS         3

#define MAX_VECTORIZATION_FACTOR INT_MAX

/* Nonzero if TYPE represents a (scalar) boolean type or type
   in the middle-end compatible with it (unsigned precision 1 integral
   types).  Used to determine which types should be vectorized as
   VECTOR_BOOLEAN_TYPE_P.  */

#define VECT_SCALAR_BOOLEAN_TYPE_P(TYPE) \
  (TREE_CODE (TYPE) == BOOLEAN_TYPE		\
   || ((TREE_CODE (TYPE) == INTEGER_TYPE	\
	|| TREE_CODE (TYPE) == ENUMERAL_TYPE)	\
       && TYPE_PRECISION (TYPE) == 1		\
       && TYPE_UNSIGNED (TYPE)))

extern vec<stmt_vec_info> stmt_vec_info_vec;

void init_stmt_vec_info_vec (void);
void free_stmt_vec_info_vec (void);

/* Return a stmt_vec_info corresponding to STMT.  */

static inline stmt_vec_info
vinfo_for_stmt (gimple *stmt)
{
  int uid = gimple_uid (stmt);
  if (uid <= 0)
    return NULL;

  return stmt_vec_info_vec[uid - 1];
}

/* Set vectorizer information INFO for STMT.  */

static inline void
set_vinfo_for_stmt (gimple *stmt, stmt_vec_info info)
{
  unsigned int uid = gimple_uid (stmt);
  if (uid == 0)
    {
      gcc_checking_assert (info);
      uid = stmt_vec_info_vec.length () + 1;
      gimple_set_uid (stmt, uid);
      stmt_vec_info_vec.safe_push (info);
    }
  else
    {
      gcc_checking_assert (info == NULL);
      stmt_vec_info_vec[uid - 1] = info;
    }
}

/* Return TRUE if a statement represented by STMT_INFO is a part of a
   pattern.  */

static inline bool
is_pattern_stmt_p (stmt_vec_info stmt_info)
{
  gimple *related_stmt;
  stmt_vec_info related_stmt_info;

  related_stmt = STMT_VINFO_RELATED_STMT (stmt_info);
  if (related_stmt
      && (related_stmt_info = vinfo_for_stmt (related_stmt))
      && STMT_VINFO_IN_PATTERN_P (related_stmt_info))
    return true;

  return false;
}

/* Return the later statement between STMT1 and STMT2.  */

static inline gimple *
get_later_stmt (gimple *stmt1, gimple *stmt2)
{
  unsigned int uid1, uid2;

  if (stmt1 == NULL)
    return stmt2;

  if (stmt2 == NULL)
    return stmt1;

  stmt_vec_info stmt_info1 = vinfo_for_stmt (stmt1);
  stmt_vec_info stmt_info2 = vinfo_for_stmt (stmt2);
  uid1 = gimple_uid (is_pattern_stmt_p (stmt_info1)
		     ? STMT_VINFO_RELATED_STMT (stmt_info1) : stmt1);
  uid2 = gimple_uid (is_pattern_stmt_p (stmt_info2)
		     ? STMT_VINFO_RELATED_STMT (stmt_info2) : stmt2);

  if (uid1 == 0 || uid2 == 0)
    return NULL;

  gcc_assert (uid1 <= stmt_vec_info_vec.length ());
  gcc_assert (uid2 <= stmt_vec_info_vec.length ());

  if (uid1 > uid2)
    return stmt1;
  else
    return stmt2;
}

/* Return true if BB is a loop header.  */

static inline bool
is_loop_header_bb_p (basic_block bb)
{
  if (bb == (bb->loop_father)->header)
    return true;
  gcc_checking_assert (EDGE_COUNT (bb->preds) == 1);
  return false;
}

/* Return pow2 (X).  */

static inline int
vect_pow2 (int x)
{
  int i, res = 1;

  for (i = 0; i < x; i++)
    res *= 2;

  return res;
}

/* Alias targetm.vectorize.builtin_vectorization_cost.  */

static inline int
builtin_vectorization_cost (enum vect_cost_for_stmt type_of_cost,
			    tree vectype, int misalign)
{
  return targetm.vectorize.builtin_vectorization_cost (type_of_cost,
						       vectype, misalign);
}

/* Get cost by calling cost target builtin.  */

static inline
int vect_get_stmt_cost (enum vect_cost_for_stmt type_of_cost)
{
  return builtin_vectorization_cost (type_of_cost, NULL, 0);
}

/* Alias targetm.vectorize.init_cost.  */

static inline void *
init_cost (struct loop *loop_info)
{
  return targetm.vectorize.init_cost (loop_info);
}

/* Alias targetm.vectorize.add_stmt_cost.  */

static inline unsigned
add_stmt_cost (void *data, int count, enum vect_cost_for_stmt kind,
	       stmt_vec_info stmt_info, int misalign,
	       enum vect_cost_model_location where)
{
  return targetm.vectorize.add_stmt_cost (data, count, kind,
					  stmt_info, misalign, where);
}

/* Alias targetm.vectorize.finish_cost.  */

static inline void
finish_cost (void *data, unsigned *prologue_cost,
	     unsigned *body_cost, unsigned *epilogue_cost)
{
  targetm.vectorize.finish_cost (data, prologue_cost, body_cost, epilogue_cost);
}

/* Alias targetm.vectorize.destroy_cost_data.  */

static inline void
destroy_cost_data (void *data)
{
  targetm.vectorize.destroy_cost_data (data);
}

/*-----------------------------------------------------------------*/
/* Info on data references alignment.                              */
/*-----------------------------------------------------------------*/
inline void
set_dr_misalignment (struct data_reference *dr, int val)
{
  dataref_aux *data_aux = DR_VECT_AUX (dr);

  if (!data_aux)
    {
      data_aux = XCNEW (dataref_aux);
      dr->aux = data_aux;
    }

  data_aux->misalignment = val;
}

inline int
dr_misalignment (struct data_reference *dr)
{
  return DR_VECT_AUX (dr)->misalignment;
}

/* Reflects actual alignment of first access in the vectorized loop,
   taking into account peeling/versioning if applied.  */
#define DR_MISALIGNMENT(DR) dr_misalignment (DR)
#define SET_DR_MISALIGNMENT(DR, VAL) set_dr_misalignment (DR, VAL)
#define DR_MISALIGNMENT_UNKNOWN (-1)

/* Only defined once DR_MISALIGNMENT is defined.  */
#define DR_TARGET_ALIGNMENT(DR) DR_VECT_AUX (DR)->target_alignment

/* Return true if data access DR is aligned to its target alignment
   (which may be less than a full vector).  */

static inline bool
aligned_access_p (struct data_reference *data_ref_info)
{
  return (DR_MISALIGNMENT (data_ref_info) == 0);
}

/* Return TRUE if the alignment of the data access is known, and FALSE
   otherwise.  */

static inline bool
known_alignment_for_access_p (struct data_reference *data_ref_info)
{
  return (DR_MISALIGNMENT (data_ref_info) != DR_MISALIGNMENT_UNKNOWN);
}

/* Return the minimum alignment in bytes that the vectorized version
   of DR is guaranteed to have.  */

static inline unsigned int
vect_known_alignment_in_bytes (struct data_reference *dr)
{
  if (DR_MISALIGNMENT (dr) == DR_MISALIGNMENT_UNKNOWN)
    return TYPE_ALIGN_UNIT (TREE_TYPE (DR_REF (dr)));
  if (DR_MISALIGNMENT (dr) == 0)
    return DR_TARGET_ALIGNMENT (dr);
  return DR_MISALIGNMENT (dr) & -DR_MISALIGNMENT (dr);
}

/* Return the behavior of DR with respect to the vectorization context
   (which for outer loop vectorization might not be the behavior recorded
   in DR itself).  */

static inline innermost_loop_behavior *
vect_dr_behavior (data_reference *dr)
{
  gimple *stmt = DR_STMT (dr);
  stmt_vec_info stmt_info = vinfo_for_stmt (stmt);
  loop_vec_info loop_vinfo = STMT_VINFO_LOOP_VINFO (stmt_info);
  if (loop_vinfo == NULL
      || !nested_in_vect_loop_p (LOOP_VINFO_LOOP (loop_vinfo), stmt))
    return &DR_INNERMOST (dr);
  else
    return &STMT_VINFO_DR_WRT_VEC_LOOP (stmt_info);
}

/* Return true if the vect cost model is unlimited.  */
static inline bool
unlimited_cost_model (loop_p loop)
{
  if (loop != NULL && loop->force_vectorize
      && flag_simd_cost_model != VECT_COST_MODEL_DEFAULT)
    return flag_simd_cost_model == VECT_COST_MODEL_UNLIMITED;
  return (flag_vect_cost_model == VECT_COST_MODEL_UNLIMITED);
}

/* Return true if the loop described by LOOP_VINFO is fully-masked and
   if the first iteration should use a partial mask in order to achieve
   alignment.  */

static inline bool
vect_use_loop_mask_for_alignment_p (loop_vec_info loop_vinfo)
{
  return (LOOP_VINFO_FULLY_MASKED_P (loop_vinfo)
	  && LOOP_VINFO_PEELING_FOR_ALIGNMENT (loop_vinfo));
}

/* Return the number of vectors of type VECTYPE that are needed to get
   NUNITS elements.  NUNITS should be based on the vectorization factor,
   so it is always a known multiple of the number of elements in VECTYPE.  */

static inline unsigned int
vect_get_num_vectors (poly_uint64 nunits, tree vectype)
{
  return exact_div (nunits, TYPE_VECTOR_SUBPARTS (vectype)).to_constant ();
}

/* Return the number of copies needed for loop vectorization when
   a statement operates on vectors of type VECTYPE.  This is the
   vectorization factor divided by the number of elements in
   VECTYPE and is always known at compile time.  */

static inline unsigned int
vect_get_num_copies (loop_vec_info loop_vinfo, tree vectype)
{
  return vect_get_num_vectors (LOOP_VINFO_VECT_FACTOR (loop_vinfo), vectype);
}

/* Update maximum unit count *MAX_NUNITS so that it accounts for
   the number of units in vector type VECTYPE.  *MAX_NUNITS can be 1
   if we haven't yet recorded any vector types.  */

static inline void
vect_update_max_nunits (poly_uint64 *max_nunits, tree vectype)
{
  /* All unit counts have the form current_vector_size * X for some
     rational X, so two unit sizes must have a common multiple.
     Everything is a multiple of the initial value of 1.  */
  poly_uint64 nunits = TYPE_VECTOR_SUBPARTS (vectype);
  *max_nunits = force_common_multiple (*max_nunits, nunits);
}

/* Return the vectorization factor that should be used for costing
   purposes while vectorizing the loop described by LOOP_VINFO.
   Pick a reasonable estimate if the vectorization factor isn't
   known at compile time.  */

static inline unsigned int
vect_vf_for_cost (loop_vec_info loop_vinfo)
{
  return estimated_poly_value (LOOP_VINFO_VECT_FACTOR (loop_vinfo));
}

/* Estimate the number of elements in VEC_TYPE for costing purposes.
   Pick a reasonable estimate if the exact number isn't known at
   compile time.  */

static inline unsigned int
vect_nunits_for_cost (tree vec_type)
{
  return estimated_poly_value (TYPE_VECTOR_SUBPARTS (vec_type));
}

/* Return the maximum possible vectorization factor for LOOP_VINFO.  */

static inline unsigned HOST_WIDE_INT
vect_max_vf (loop_vec_info loop_vinfo)
{
  unsigned HOST_WIDE_INT vf;
  if (LOOP_VINFO_VECT_FACTOR (loop_vinfo).is_constant (&vf))
    return vf;
  return MAX_VECTORIZATION_FACTOR;
}

/* Return the size of the value accessed by unvectorized data reference DR.
   This is only valid once STMT_VINFO_VECTYPE has been calculated for the
   associated gimple statement, since that guarantees that DR accesses
   either a scalar or a scalar equivalent.  ("Scalar equivalent" here
   includes things like V1SI, which can be vectorized in the same way
   as a plain SI.)  */

inline unsigned int
vect_get_scalar_dr_size (struct data_reference *dr)
{
  return tree_to_uhwi (TYPE_SIZE_UNIT (TREE_TYPE (DR_REF (dr))));
}

/* Source location */
extern source_location vect_location;

/*-----------------------------------------------------------------*/
/* Function prototypes.                                            */
/*-----------------------------------------------------------------*/

/* Simple loop peeling and versioning utilities for vectorizer's purposes -
   in tree-vect-loop-manip.c.  */
extern void vect_set_loop_condition (struct loop *, loop_vec_info,
				     tree, tree, tree, bool);
extern bool slpeel_can_duplicate_loop_p (const struct loop *, const_edge);
struct loop *slpeel_tree_duplicate_loop_to_edge_cfg (struct loop *,
						     struct loop *, edge);
extern void vect_loop_versioning (loop_vec_info, unsigned int, bool,
				  poly_uint64);
extern struct loop *vect_do_peeling (loop_vec_info, tree, tree,
				     tree *, tree *, tree *, int, bool, bool);
extern void vect_prepare_for_masked_peels (loop_vec_info);
extern source_location find_loop_location (struct loop *);
extern bool vect_can_advance_ivs_p (loop_vec_info);

/* In tree-vect-stmts.c.  */
extern poly_uint64 current_vector_size;
extern tree get_vectype_for_scalar_type (tree);
extern tree get_vectype_for_scalar_type_and_size (tree, poly_uint64);
extern tree get_mask_type_for_scalar_type (tree);
extern tree get_same_sized_vectype (tree, tree);
extern bool vect_get_loop_mask_type (loop_vec_info);
extern bool vect_is_simple_use (tree, vec_info *, gimple **,
                                enum vect_def_type *);
extern bool vect_is_simple_use (tree, vec_info *, gimple **,
				enum vect_def_type *, tree *);
extern bool supportable_widening_operation (enum tree_code, gimple *, tree,
					    tree, enum tree_code *,
					    enum tree_code *, int *,
					    vec<tree> *);
extern bool supportable_narrowing_operation (enum tree_code, tree, tree,
					     enum tree_code *,
					     int *, vec<tree> *);
extern stmt_vec_info new_stmt_vec_info (gimple *stmt, vec_info *);
extern void free_stmt_vec_info (gimple *stmt);
extern void vect_model_simple_cost (stmt_vec_info, int, enum vect_def_type *,
				    int, stmt_vector_for_cost *,
				    stmt_vector_for_cost *);
extern void vect_model_store_cost (stmt_vec_info, int, vect_memory_access_type,
				   vec_load_store_type, slp_tree,
				   stmt_vector_for_cost *,
				   stmt_vector_for_cost *);
extern void vect_model_load_cost (stmt_vec_info, int, vect_memory_access_type,
				  slp_tree, stmt_vector_for_cost *,
				  stmt_vector_for_cost *);
extern unsigned record_stmt_cost (stmt_vector_for_cost *, int,
				  enum vect_cost_for_stmt, stmt_vec_info,
				  int, enum vect_cost_model_location);
extern void vect_finish_replace_stmt (gimple *, gimple *);
extern void vect_finish_stmt_generation (gimple *, gimple *,
                                         gimple_stmt_iterator *);
extern bool vect_mark_stmts_to_be_vectorized (loop_vec_info);
extern tree vect_get_store_rhs (gimple *);
extern tree vect_get_vec_def_for_operand_1 (gimple *, enum vect_def_type);
extern tree vect_get_vec_def_for_operand (tree, gimple *, tree = NULL);
extern void vect_get_vec_defs (tree, tree, gimple *, vec<tree> *,
			       vec<tree> *, slp_tree);
extern void vect_get_vec_defs_for_stmt_copy (enum vect_def_type *,
					     vec<tree> *, vec<tree> *);
extern tree vect_init_vector (gimple *, tree, tree,
                              gimple_stmt_iterator *);
extern tree vect_get_vec_def_for_stmt_copy (enum vect_def_type, tree);
extern bool vect_transform_stmt (gimple *, gimple_stmt_iterator *,
                                 bool *, slp_tree, slp_instance);
extern void vect_remove_stores (gimple *);
extern bool vect_analyze_stmt (gimple *, bool *, slp_tree, slp_instance);
extern bool vectorizable_condition (gimple *, gimple_stmt_iterator *,
				    gimple **, tree, int, slp_tree);
extern void vect_get_load_cost (struct data_reference *, int, bool,
				unsigned int *, unsigned int *,
				stmt_vector_for_cost *,
				stmt_vector_for_cost *, bool);
extern void vect_get_store_cost (struct data_reference *, int,
				 unsigned int *, stmt_vector_for_cost *);
extern bool vect_supportable_shift (enum tree_code, tree);
extern tree vect_gen_perm_mask_any (tree, const vec_perm_indices &);
extern tree vect_gen_perm_mask_checked (tree, const vec_perm_indices &);
extern void optimize_mask_stores (struct loop*);
extern gcall *vect_gen_while (tree, tree, tree);
extern tree vect_gen_while_not (gimple_seq *, tree, tree, tree);

/* In tree-vect-data-refs.c.  */
extern bool vect_can_force_dr_alignment_p (const_tree, unsigned int);
extern enum dr_alignment_support vect_supportable_dr_alignment
                                           (struct data_reference *, bool);
extern tree vect_get_smallest_scalar_type (gimple *, HOST_WIDE_INT *,
                                           HOST_WIDE_INT *);
extern bool vect_analyze_data_ref_dependences (loop_vec_info, unsigned int *);
extern bool vect_slp_analyze_instance_dependence (slp_instance);
extern bool vect_enhance_data_refs_alignment (loop_vec_info);
extern bool vect_analyze_data_refs_alignment (loop_vec_info);
extern bool vect_verify_datarefs_alignment (loop_vec_info);
extern bool vect_slp_analyze_and_verify_instance_alignment (slp_instance);
extern bool vect_analyze_data_ref_accesses (vec_info *);
extern bool vect_prune_runtime_alias_test_list (loop_vec_info);
extern bool vect_gather_scatter_fn_p (bool, bool, tree, tree, unsigned int,
				      signop, int, internal_fn *, tree *);
extern bool vect_check_gather_scatter (gimple *, loop_vec_info,
				       gather_scatter_info *);
extern bool vect_analyze_data_refs (vec_info *, poly_uint64 *);
extern void vect_record_base_alignments (vec_info *);
extern tree vect_create_data_ref_ptr (gimple *, tree, struct loop *, tree,
				      tree *, gimple_stmt_iterator *,
				      gimple **, bool, bool *,
				      tree = NULL_TREE, tree = NULL_TREE);
extern tree bump_vector_ptr (tree, gimple *, gimple_stmt_iterator *, gimple *,
			     tree);
extern void vect_copy_ref_info (tree, tree);
extern tree vect_create_destination_var (tree, tree);
extern bool vect_grouped_store_supported (tree, unsigned HOST_WIDE_INT);
extern bool vect_store_lanes_supported (tree, unsigned HOST_WIDE_INT, bool);
extern bool vect_grouped_load_supported (tree, bool, unsigned HOST_WIDE_INT);
extern bool vect_load_lanes_supported (tree, unsigned HOST_WIDE_INT, bool);
extern void vect_permute_store_chain (vec<tree> ,unsigned int, gimple *,
                                    gimple_stmt_iterator *, vec<tree> *);
extern tree vect_setup_realignment (gimple *, gimple_stmt_iterator *, tree *,
                                    enum dr_alignment_support, tree,
                                    struct loop **);
extern void vect_transform_grouped_load (gimple *, vec<tree> , int,
                                         gimple_stmt_iterator *);
extern void vect_record_grouped_load_vectors (gimple *, vec<tree> );
extern tree vect_get_new_vect_var (tree, enum vect_var_kind, const char *);
extern tree vect_get_new_ssa_name (tree, enum vect_var_kind,
				   const char * = NULL);
extern tree vect_create_addr_base_for_vector_ref (gimple *, gimple_seq *,
						  tree, tree = NULL_TREE);

/* In tree-vect-loop.c.  */
/* FORNOW: Used in tree-parloops.c.  */
extern gimple *vect_force_simple_reduction (loop_vec_info, gimple *,
					    bool *, bool);
/* Used in gimple-loop-interchange.c.  */
extern bool check_reduction_path (location_t, loop_p, gphi *, tree,
				  enum tree_code);
/* Drive for loop analysis stage.  */
extern loop_vec_info vect_analyze_loop (struct loop *, loop_vec_info);
extern tree vect_build_loop_niters (loop_vec_info, bool * = NULL);
extern void vect_gen_vector_loop_niters (loop_vec_info, tree, tree *,
					 tree *, bool);
extern tree vect_halve_mask_nunits (tree);
extern tree vect_double_mask_nunits (tree);
extern void vect_record_loop_mask (loop_vec_info, vec_loop_masks *,
				   unsigned int, tree);
extern tree vect_get_loop_mask (gimple_stmt_iterator *, vec_loop_masks *,
				unsigned int, tree, unsigned int);

/* Drive for loop transformation stage.  */
extern struct loop *vect_transform_loop (loop_vec_info);
extern loop_vec_info vect_analyze_loop_form (struct loop *);
extern bool vectorizable_live_operation (gimple *, gimple_stmt_iterator *,
					 slp_tree, int, gimple **);
extern bool vectorizable_reduction (gimple *, gimple_stmt_iterator *,
				    gimple **, slp_tree, slp_instance);
extern bool vectorizable_induction (gimple *, gimple_stmt_iterator *,
				    gimple **, slp_tree);
extern tree get_initial_def_for_reduction (gimple *, tree, tree *);
extern bool vect_worthwhile_without_simd_p (vec_info *, tree_code);
extern int vect_get_known_peeling_cost (loop_vec_info, int, int *,
					stmt_vector_for_cost *,
					stmt_vector_for_cost *,
					stmt_vector_for_cost *);
extern tree cse_and_gimplify_to_preheader (loop_vec_info, tree);

/* In tree-vect-slp.c.  */
extern void vect_free_slp_instance (slp_instance);
extern bool vect_transform_slp_perm_load (slp_tree, vec<tree> ,
					  gimple_stmt_iterator *, poly_uint64,
					  slp_instance, bool, unsigned *);
extern bool vect_slp_analyze_operations (vec_info *);
extern bool vect_schedule_slp (vec_info *);
extern bool vect_analyze_slp (vec_info *, unsigned);
extern bool vect_make_slp_decision (loop_vec_info);
extern void vect_detect_hybrid_slp (loop_vec_info);
extern void vect_get_slp_defs (vec<tree> , slp_tree, vec<vec<tree> > *);
extern bool vect_slp_bb (basic_block);
extern gimple *vect_find_last_scalar_stmt_in_slp (slp_tree);
extern bool is_simple_and_all_uses_invariant (gimple *, loop_vec_info);
extern bool can_duplicate_and_interleave_p (unsigned int, machine_mode,
					    unsigned int * = NULL,
					    tree * = NULL, tree * = NULL);
extern void duplicate_and_interleave (gimple_seq *, tree, vec<tree>,
				      unsigned int, vec<tree> &);
extern int vect_get_place_in_interleaving_chain (gimple *, gimple *);

/* In tree-vect-patterns.c.  */
/* Pattern recognition functions.
   Additional pattern recognition functions can (and will) be added
   in the future.  */
typedef gimple *(* vect_recog_func_ptr) (vec<gimple *> *, tree *, tree *);
#define NUM_PATTERNS 15
void vect_pattern_recog (vec_info *);

/* In tree-vectorizer.c.  */
unsigned vectorize_loops (void);
bool vect_stmt_in_region_p (vec_info *, gimple *);
void vect_free_loop_info_assumptions (struct loop *);

#endif  /* GCC_TREE_VECTORIZER_H  */
