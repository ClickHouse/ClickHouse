/* Define control flow data structures for the CFG.
   Copyright (C) 1987-2018 Free Software Foundation, Inc.

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

#ifndef GCC_BASIC_BLOCK_H
#define GCC_BASIC_BLOCK_H

#include <profile-count.h>

/* Control flow edge information.  */
struct GTY((user)) edge_def {
  /* The two blocks at the ends of the edge.  */
  basic_block src;
  basic_block dest;

  /* Instructions queued on the edge.  */
  union edge_def_insns {
    gimple_seq g;
    rtx_insn *r;
  } insns;

  /* Auxiliary info specific to a pass.  */
  PTR aux;

  /* Location of any goto implicit in the edge.  */
  location_t goto_locus;

  /* The index number corresponding to this edge in the edge vector
     dest->preds.  */
  unsigned int dest_idx;

  int flags;			/* see cfg-flags.def */
  profile_probability probability;

  /* Return count of edge E.  */
  inline profile_count count () const;
};

/* Masks for edge.flags.  */
#define DEF_EDGE_FLAG(NAME,IDX) EDGE_##NAME = 1 << IDX ,
enum cfg_edge_flags {
#include "cfg-flags.def"
  LAST_CFG_EDGE_FLAG		/* this is only used for EDGE_ALL_FLAGS */
};
#undef DEF_EDGE_FLAG

/* Bit mask for all edge flags.  */
#define EDGE_ALL_FLAGS		((LAST_CFG_EDGE_FLAG - 1) * 2 - 1)

/* The following four flags all indicate something special about an edge.
   Test the edge flags on EDGE_COMPLEX to detect all forms of "strange"
   control flow transfers.  */
#define EDGE_COMPLEX \
  (EDGE_ABNORMAL | EDGE_ABNORMAL_CALL | EDGE_EH | EDGE_PRESERVE)

struct GTY(()) rtl_bb_info {
  /* The first insn of the block is embedded into bb->il.x.  */
  /* The last insn of the block.  */
  rtx_insn *end_;

  /* In CFGlayout mode points to insn notes/jumptables to be placed just before
     and after the block.   */
  rtx_insn *header_;
  rtx_insn *footer_;
};

struct GTY(()) gimple_bb_info {
  /* Sequence of statements in this block.  */
  gimple_seq seq;

  /* PHI nodes for this block.  */
  gimple_seq phi_nodes;
};

/* A basic block is a sequence of instructions with only one entry and
   only one exit.  If any one of the instructions are executed, they
   will all be executed, and in sequence from first to last.

   There may be COND_EXEC instructions in the basic block.  The
   COND_EXEC *instructions* will be executed -- but if the condition
   is false the conditionally executed *expressions* will of course
   not be executed.  We don't consider the conditionally executed
   expression (which might have side-effects) to be in a separate
   basic block because the program counter will always be at the same
   location after the COND_EXEC instruction, regardless of whether the
   condition is true or not.

   Basic blocks need not start with a label nor end with a jump insn.
   For example, a previous basic block may just "conditionally fall"
   into the succeeding basic block, and the last basic block need not
   end with a jump insn.  Block 0 is a descendant of the entry block.

   A basic block beginning with two labels cannot have notes between
   the labels.

   Data for jump tables are stored in jump_insns that occur in no
   basic block even though these insns can follow or precede insns in
   basic blocks.  */

/* Basic block information indexed by block number.  */
struct GTY((chain_next ("%h.next_bb"), chain_prev ("%h.prev_bb"))) basic_block_def {
  /* The edges into and out of the block.  */
  vec<edge, va_gc> *preds;
  vec<edge, va_gc> *succs;

  /* Auxiliary info specific to a pass.  */
  PTR GTY ((skip (""))) aux;

  /* Innermost loop containing the block.  */
  struct loop *loop_father;

  /* The dominance and postdominance information node.  */
  struct et_node * GTY ((skip (""))) dom[2];

  /* Previous and next blocks in the chain.  */
  basic_block prev_bb;
  basic_block next_bb;

  union basic_block_il_dependent {
      struct gimple_bb_info GTY ((tag ("0"))) gimple;
      struct {
        rtx_insn *head_;
        struct rtl_bb_info * rtl;
      } GTY ((tag ("1"))) x;
    } GTY ((desc ("((%1.flags & BB_RTL) != 0)"))) il;

  /* Various flags.  See cfg-flags.def.  */
  int flags;

  /* The index of this block.  */
  int index;

  /* Expected number of executions: calculated in profile.c.  */
  profile_count count;

  /* The discriminator for this block.  The discriminator distinguishes
     among several basic blocks that share a common locus, allowing for
     more accurate sample-based profiling.  */
  int discriminator;
};

/* This ensures that struct gimple_bb_info is smaller than
   struct rtl_bb_info, so that inlining the former into basic_block_def
   is the better choice.  */
typedef int __assert_gimple_bb_smaller_rtl_bb
              [(int) sizeof (struct rtl_bb_info)
               - (int) sizeof (struct gimple_bb_info)];


#define BB_FREQ_MAX 10000

/* Masks for basic_block.flags.  */
#define DEF_BASIC_BLOCK_FLAG(NAME,IDX) BB_##NAME = 1 << IDX ,
enum cfg_bb_flags
{
#include "cfg-flags.def"
  LAST_CFG_BB_FLAG		/* this is only used for BB_ALL_FLAGS */
};
#undef DEF_BASIC_BLOCK_FLAG

/* Bit mask for all basic block flags.  */
#define BB_ALL_FLAGS		((LAST_CFG_BB_FLAG - 1) * 2 - 1)

/* Bit mask for all basic block flags that must be preserved.  These are
   the bit masks that are *not* cleared by clear_bb_flags.  */
#define BB_FLAGS_TO_PRESERVE					\
  (BB_DISABLE_SCHEDULE | BB_RTL | BB_NON_LOCAL_GOTO_TARGET	\
   | BB_HOT_PARTITION | BB_COLD_PARTITION)

/* Dummy bitmask for convenience in the hot/cold partitioning code.  */
#define BB_UNPARTITIONED	0

/* Partitions, to be used when partitioning hot and cold basic blocks into
   separate sections.  */
#define BB_PARTITION(bb) ((bb)->flags & (BB_HOT_PARTITION|BB_COLD_PARTITION))
#define BB_SET_PARTITION(bb, part) do {					\
  basic_block bb_ = (bb);						\
  bb_->flags = ((bb_->flags & ~(BB_HOT_PARTITION|BB_COLD_PARTITION))	\
		| (part));						\
} while (0)

#define BB_COPY_PARTITION(dstbb, srcbb) \
  BB_SET_PARTITION (dstbb, BB_PARTITION (srcbb))

/* Defines for accessing the fields of the CFG structure for function FN.  */
#define ENTRY_BLOCK_PTR_FOR_FN(FN)	     ((FN)->cfg->x_entry_block_ptr)
#define EXIT_BLOCK_PTR_FOR_FN(FN)	     ((FN)->cfg->x_exit_block_ptr)
#define basic_block_info_for_fn(FN)	     ((FN)->cfg->x_basic_block_info)
#define n_basic_blocks_for_fn(FN)	     ((FN)->cfg->x_n_basic_blocks)
#define n_edges_for_fn(FN)		     ((FN)->cfg->x_n_edges)
#define last_basic_block_for_fn(FN)	     ((FN)->cfg->x_last_basic_block)
#define label_to_block_map_for_fn(FN)	     ((FN)->cfg->x_label_to_block_map)
#define profile_status_for_fn(FN)	     ((FN)->cfg->x_profile_status)

#define BASIC_BLOCK_FOR_FN(FN,N) \
  ((*basic_block_info_for_fn (FN))[(N)])
#define SET_BASIC_BLOCK_FOR_FN(FN,N,BB) \
  ((*basic_block_info_for_fn (FN))[(N)] = (BB))

/* For iterating over basic blocks.  */
#define FOR_BB_BETWEEN(BB, FROM, TO, DIR) \
  for (BB = FROM; BB != TO; BB = BB->DIR)

#define FOR_EACH_BB_FN(BB, FN) \
  FOR_BB_BETWEEN (BB, (FN)->cfg->x_entry_block_ptr->next_bb, (FN)->cfg->x_exit_block_ptr, next_bb)

#define FOR_EACH_BB_REVERSE_FN(BB, FN) \
  FOR_BB_BETWEEN (BB, (FN)->cfg->x_exit_block_ptr->prev_bb, (FN)->cfg->x_entry_block_ptr, prev_bb)

/* For iterating over insns in basic block.  */
#define FOR_BB_INSNS(BB, INSN)			\
  for ((INSN) = BB_HEAD (BB);			\
       (INSN) && (INSN) != NEXT_INSN (BB_END (BB));	\
       (INSN) = NEXT_INSN (INSN))

/* For iterating over insns in basic block when we might remove the
   current insn.  */
#define FOR_BB_INSNS_SAFE(BB, INSN, CURR)			\
  for ((INSN) = BB_HEAD (BB), (CURR) = (INSN) ? NEXT_INSN ((INSN)): NULL;	\
       (INSN) && (INSN) != NEXT_INSN (BB_END (BB));	\
       (INSN) = (CURR), (CURR) = (INSN) ? NEXT_INSN ((INSN)) : NULL)

#define FOR_BB_INSNS_REVERSE(BB, INSN)		\
  for ((INSN) = BB_END (BB);			\
       (INSN) && (INSN) != PREV_INSN (BB_HEAD (BB));	\
       (INSN) = PREV_INSN (INSN))

#define FOR_BB_INSNS_REVERSE_SAFE(BB, INSN, CURR)	\
  for ((INSN) = BB_END (BB),(CURR) = (INSN) ? PREV_INSN ((INSN)) : NULL;	\
       (INSN) && (INSN) != PREV_INSN (BB_HEAD (BB));	\
       (INSN) = (CURR), (CURR) = (INSN) ? PREV_INSN ((INSN)) : NULL)

/* Cycles through _all_ basic blocks, even the fake ones (entry and
   exit block).  */

#define FOR_ALL_BB_FN(BB, FN) \
  for (BB = ENTRY_BLOCK_PTR_FOR_FN (FN); BB; BB = BB->next_bb)


/* Stuff for recording basic block info.  */

/* For now, these will be functions (so that they can include checked casts
   to rtx_insn.   Once the underlying fields are converted from rtx
   to rtx_insn, these can be converted back to macros.  */

#define BB_HEAD(B)      (B)->il.x.head_
#define BB_END(B)       (B)->il.x.rtl->end_
#define BB_HEADER(B)    (B)->il.x.rtl->header_
#define BB_FOOTER(B)    (B)->il.x.rtl->footer_

/* Special block numbers [markers] for entry and exit.
   Neither of them is supposed to hold actual statements.  */
#define ENTRY_BLOCK (0)
#define EXIT_BLOCK (1)

/* The two blocks that are always in the cfg.  */
#define NUM_FIXED_BLOCKS (2)

/* This is the value which indicates no edge is present.  */
#define EDGE_INDEX_NO_EDGE	-1

/* EDGE_INDEX returns an integer index for an edge, or EDGE_INDEX_NO_EDGE
   if there is no edge between the 2 basic blocks.  */
#define EDGE_INDEX(el, pred, succ) (find_edge_index ((el), (pred), (succ)))

/* INDEX_EDGE_PRED_BB and INDEX_EDGE_SUCC_BB return a pointer to the basic
   block which is either the pred or succ end of the indexed edge.  */
#define INDEX_EDGE_PRED_BB(el, index)	((el)->index_to_edge[(index)]->src)
#define INDEX_EDGE_SUCC_BB(el, index)	((el)->index_to_edge[(index)]->dest)

/* INDEX_EDGE returns a pointer to the edge.  */
#define INDEX_EDGE(el, index)           ((el)->index_to_edge[(index)])

/* Number of edges in the compressed edge list.  */
#define NUM_EDGES(el)			((el)->num_edges)

/* BB is assumed to contain conditional jump.  Return the fallthru edge.  */
#define FALLTHRU_EDGE(bb)		(EDGE_SUCC ((bb), 0)->flags & EDGE_FALLTHRU \
					 ? EDGE_SUCC ((bb), 0) : EDGE_SUCC ((bb), 1))

/* BB is assumed to contain conditional jump.  Return the branch edge.  */
#define BRANCH_EDGE(bb)			(EDGE_SUCC ((bb), 0)->flags & EDGE_FALLTHRU \
					 ? EDGE_SUCC ((bb), 1) : EDGE_SUCC ((bb), 0))

/* Return expected execution frequency of the edge E.  */
#define EDGE_FREQUENCY(e)		e->count ().to_frequency (cfun)

/* Compute a scale factor (or probability) suitable for scaling of
   gcov_type values via apply_probability() and apply_scale().  */
#define GCOV_COMPUTE_SCALE(num,den) \
  ((den) ? RDIV ((num) * REG_BR_PROB_BASE, (den)) : REG_BR_PROB_BASE)

/* Return nonzero if edge is critical.  */
#define EDGE_CRITICAL_P(e)		(EDGE_COUNT ((e)->src->succs) >= 2 \
					 && EDGE_COUNT ((e)->dest->preds) >= 2)

#define EDGE_COUNT(ev)			vec_safe_length (ev)
#define EDGE_I(ev,i)			(*ev)[(i)]
#define EDGE_PRED(bb,i)			(*(bb)->preds)[(i)]
#define EDGE_SUCC(bb,i)			(*(bb)->succs)[(i)]

/* Returns true if BB has precisely one successor.  */

static inline bool
single_succ_p (const_basic_block bb)
{
  return EDGE_COUNT (bb->succs) == 1;
}

/* Returns true if BB has precisely one predecessor.  */

static inline bool
single_pred_p (const_basic_block bb)
{
  return EDGE_COUNT (bb->preds) == 1;
}

/* Returns the single successor edge of basic block BB.  Aborts if
   BB does not have exactly one successor.  */

static inline edge
single_succ_edge (const_basic_block bb)
{
  gcc_checking_assert (single_succ_p (bb));
  return EDGE_SUCC (bb, 0);
}

/* Returns the single predecessor edge of basic block BB.  Aborts
   if BB does not have exactly one predecessor.  */

static inline edge
single_pred_edge (const_basic_block bb)
{
  gcc_checking_assert (single_pred_p (bb));
  return EDGE_PRED (bb, 0);
}

/* Returns the single successor block of basic block BB.  Aborts
   if BB does not have exactly one successor.  */

static inline basic_block
single_succ (const_basic_block bb)
{
  return single_succ_edge (bb)->dest;
}

/* Returns the single predecessor block of basic block BB.  Aborts
   if BB does not have exactly one predecessor.*/

static inline basic_block
single_pred (const_basic_block bb)
{
  return single_pred_edge (bb)->src;
}

/* Iterator object for edges.  */

struct edge_iterator {
  unsigned index;
  vec<edge, va_gc> **container;
};

static inline vec<edge, va_gc> *
ei_container (edge_iterator i)
{
  gcc_checking_assert (i.container);
  return *i.container;
}

#define ei_start(iter) ei_start_1 (&(iter))
#define ei_last(iter) ei_last_1 (&(iter))

/* Return an iterator pointing to the start of an edge vector.  */
static inline edge_iterator
ei_start_1 (vec<edge, va_gc> **ev)
{
  edge_iterator i;

  i.index = 0;
  i.container = ev;

  return i;
}

/* Return an iterator pointing to the last element of an edge
   vector.  */
static inline edge_iterator
ei_last_1 (vec<edge, va_gc> **ev)
{
  edge_iterator i;

  i.index = EDGE_COUNT (*ev) - 1;
  i.container = ev;

  return i;
}

/* Is the iterator `i' at the end of the sequence?  */
static inline bool
ei_end_p (edge_iterator i)
{
  return (i.index == EDGE_COUNT (ei_container (i)));
}

/* Is the iterator `i' at one position before the end of the
   sequence?  */
static inline bool
ei_one_before_end_p (edge_iterator i)
{
  return (i.index + 1 == EDGE_COUNT (ei_container (i)));
}

/* Advance the iterator to the next element.  */
static inline void
ei_next (edge_iterator *i)
{
  gcc_checking_assert (i->index < EDGE_COUNT (ei_container (*i)));
  i->index++;
}

/* Move the iterator to the previous element.  */
static inline void
ei_prev (edge_iterator *i)
{
  gcc_checking_assert (i->index > 0);
  i->index--;
}

/* Return the edge pointed to by the iterator `i'.  */
static inline edge
ei_edge (edge_iterator i)
{
  return EDGE_I (ei_container (i), i.index);
}

/* Return an edge pointed to by the iterator.  Do it safely so that
   NULL is returned when the iterator is pointing at the end of the
   sequence.  */
static inline edge
ei_safe_edge (edge_iterator i)
{
  return !ei_end_p (i) ? ei_edge (i) : NULL;
}

/* Return 1 if we should continue to iterate.  Return 0 otherwise.
   *Edge P is set to the next edge if we are to continue to iterate
   and NULL otherwise.  */

static inline bool
ei_cond (edge_iterator ei, edge *p)
{
  if (!ei_end_p (ei))
    {
      *p = ei_edge (ei);
      return 1;
    }
  else
    {
      *p = NULL;
      return 0;
    }
}

/* This macro serves as a convenient way to iterate each edge in a
   vector of predecessor or successor edges.  It must not be used when
   an element might be removed during the traversal, otherwise
   elements will be missed.  Instead, use a for-loop like that shown
   in the following pseudo-code:

   FOR (ei = ei_start (bb->succs); (e = ei_safe_edge (ei)); )
     {
	IF (e != taken_edge)
	  remove_edge (e);
	ELSE
	  ei_next (&ei);
     }
*/

#define FOR_EACH_EDGE(EDGE,ITER,EDGE_VEC)	\
  for ((ITER) = ei_start ((EDGE_VEC));		\
       ei_cond ((ITER), &(EDGE));		\
       ei_next (&(ITER)))

#define CLEANUP_EXPENSIVE	1	/* Do relatively expensive optimizations
					   except for edge forwarding */
#define CLEANUP_CROSSJUMP	2	/* Do crossjumping.  */
#define CLEANUP_POST_REGSTACK	4	/* We run after reg-stack and need
					   to care REG_DEAD notes.  */
#define CLEANUP_THREADING	8	/* Do jump threading.  */
#define CLEANUP_NO_INSN_DEL	16	/* Do not try to delete trivially dead
					   insns.  */
#define CLEANUP_CFGLAYOUT	32	/* Do cleanup in cfglayout mode.  */
#define CLEANUP_CFG_CHANGED	64      /* The caller changed the CFG.  */
#define CLEANUP_NO_PARTITIONING	128     /* Do not try to fix partitions.  */

/* Return true if BB is in a transaction.  */

static inline bool
bb_in_transaction (basic_block bb)
{
  return bb->flags & BB_IN_TRANSACTION;
}

/* Return true when one of the predecessor edges of BB is marked with EDGE_EH.  */
static inline bool
bb_has_eh_pred (basic_block bb)
{
  edge e;
  edge_iterator ei;

  FOR_EACH_EDGE (e, ei, bb->preds)
    {
      if (e->flags & EDGE_EH)
	return true;
    }
  return false;
}

/* Return true when one of the predecessor edges of BB is marked with EDGE_ABNORMAL.  */
static inline bool
bb_has_abnormal_pred (basic_block bb)
{
  edge e;
  edge_iterator ei;

  FOR_EACH_EDGE (e, ei, bb->preds)
    {
      if (e->flags & EDGE_ABNORMAL)
	return true;
    }
  return false;
}

/* Return the fallthru edge in EDGES if it exists, NULL otherwise.  */
static inline edge
find_fallthru_edge (vec<edge, va_gc> *edges)
{
  edge e;
  edge_iterator ei;

  FOR_EACH_EDGE (e, ei, edges)
    if (e->flags & EDGE_FALLTHRU)
      break;

  return e;
}

/* Check tha probability is sane.  */

static inline void
check_probability (int prob)
{
  gcc_checking_assert (prob >= 0 && prob <= REG_BR_PROB_BASE);
}

/* Given PROB1 and PROB2, return PROB1*PROB2/REG_BR_PROB_BASE. 
   Used to combine BB probabilities.  */

static inline int
combine_probabilities (int prob1, int prob2)
{
  check_probability (prob1);
  check_probability (prob2);
  return RDIV (prob1 * prob2, REG_BR_PROB_BASE);
}

/* Apply scale factor SCALE on frequency or count FREQ. Use this
   interface when potentially scaling up, so that SCALE is not
   constrained to be < REG_BR_PROB_BASE.  */

static inline gcov_type
apply_scale (gcov_type freq, gcov_type scale)
{
  return RDIV (freq * scale, REG_BR_PROB_BASE);
}

/* Apply probability PROB on frequency or count FREQ.  */

static inline gcov_type
apply_probability (gcov_type freq, int prob)
{
  check_probability (prob);
  return apply_scale (freq, prob);
}

/* Return inverse probability for PROB.  */

static inline int
inverse_probability (int prob1)
{
  check_probability (prob1);
  return REG_BR_PROB_BASE - prob1;
}

/* Return true if BB has at least one abnormal outgoing edge.  */

static inline bool
has_abnormal_or_eh_outgoing_edge_p (basic_block bb)
{
  edge e;
  edge_iterator ei;

  FOR_EACH_EDGE (e, ei, bb->succs)
    if (e->flags & (EDGE_ABNORMAL | EDGE_EH))
      return true;

  return false;
}

/* Return true when one of the predecessor edges of BB is marked with
   EDGE_ABNORMAL_CALL or EDGE_EH.  */

static inline bool
has_abnormal_call_or_eh_pred_edge_p (basic_block bb)
{
  edge e;
  edge_iterator ei;

  FOR_EACH_EDGE (e, ei, bb->preds)
    if (e->flags & (EDGE_ABNORMAL_CALL | EDGE_EH))
      return true;

  return false;
}

/* Return count of edge E.  */
inline profile_count edge_def::count () const
{
  return src->count.apply_probability (probability);
}

#endif /* GCC_BASIC_BLOCK_H */
