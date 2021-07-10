/* Inlining decision heuristics.
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

#ifndef GCC_IPA_INLINE_H
#define GCC_IPA_INLINE_H

/* Data we cache about callgraph edges during inlining to avoid expensive
   re-computations during the greedy algorithm.  */
struct edge_growth_cache_entry
{
  sreal time, nonspec_time;
  int size;
  ipa_hints hints;

  edge_growth_cache_entry()
    : size (0), hints (0) {}

  edge_growth_cache_entry(int64_t time, int64_t nonspec_time,
			  int size, ipa_hints hints)
    : time (time), nonspec_time (nonspec_time), size (size),
      hints (hints) {}
};

extern vec<edge_growth_cache_entry> edge_growth_cache;

/* In ipa-inline-analysis.c  */
int estimate_size_after_inlining (struct cgraph_node *, struct cgraph_edge *);
int estimate_growth (struct cgraph_node *);
bool growth_likely_positive (struct cgraph_node *, int);
int do_estimate_edge_size (struct cgraph_edge *edge);
sreal do_estimate_edge_time (struct cgraph_edge *edge);
ipa_hints do_estimate_edge_hints (struct cgraph_edge *edge);
void initialize_growth_caches (void);
void free_growth_caches (void);

/* In ipa-inline.c  */
unsigned int early_inliner (function *fun);
bool inline_account_function_p (struct cgraph_node *node);


/* In ipa-inline-transform.c  */
bool inline_call (struct cgraph_edge *, bool, vec<cgraph_edge *> *, int *, bool,
		  bool *callee_removed = NULL);
unsigned int inline_transform (struct cgraph_node *);
void clone_inlined_nodes (struct cgraph_edge *e, bool, bool, int *);

extern int ncalls_inlined;
extern int nfunctions_inlined;

/* Return estimated size of the inline sequence of EDGE.  */

static inline int
estimate_edge_size (struct cgraph_edge *edge)
{
  int ret;
  if ((int)edge_growth_cache.length () <= edge->uid
      || !(ret = edge_growth_cache[edge->uid].size))
    return do_estimate_edge_size (edge);
  return ret - (ret > 0);
}

/* Return estimated callee growth after inlining EDGE.  */

static inline int
estimate_edge_growth (struct cgraph_edge *edge)
{
  gcc_checking_assert (ipa_call_summaries->get (edge)->call_stmt_size
		       || !edge->callee->analyzed);
  return (estimate_edge_size (edge)
	  - ipa_call_summaries->get (edge)->call_stmt_size);
}

/* Return estimated callee runtime increase after inlining
   EDGE.  */

static inline sreal
estimate_edge_time (struct cgraph_edge *edge, sreal *nonspec_time = NULL)
{
  sreal ret;
  if ((int)edge_growth_cache.length () <= edge->uid
      || !edge_growth_cache[edge->uid].size)
    return do_estimate_edge_time (edge);
  if (nonspec_time)
    *nonspec_time = edge_growth_cache[edge->uid].nonspec_time;
  return edge_growth_cache[edge->uid].time;
}


/* Return estimated callee runtime increase after inlining
   EDGE.  */

static inline ipa_hints
estimate_edge_hints (struct cgraph_edge *edge)
{
  ipa_hints ret;
  if ((int)edge_growth_cache.length () <= edge->uid
      || !(ret = edge_growth_cache[edge->uid].hints))
    return do_estimate_edge_hints (edge);
  return ret - 1;
}

/* Reset cached value for EDGE.  */

static inline void
reset_edge_growth_cache (struct cgraph_edge *edge)
{
  if ((int)edge_growth_cache.length () > edge->uid)
    {
      struct edge_growth_cache_entry zero (0, 0, 0, 0);
      edge_growth_cache[edge->uid] = zero;
    }
}

#endif /* GCC_IPA_INLINE_H */
