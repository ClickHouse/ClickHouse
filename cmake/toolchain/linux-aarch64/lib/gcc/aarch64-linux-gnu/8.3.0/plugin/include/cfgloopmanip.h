/* Loop manipulation header.
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

#ifndef GCC_CFGLOOPMANIP_H
#define GCC_CFGLOOPMANIP_H

enum
{
  CP_SIMPLE_PREHEADERS = 1,
  CP_FALLTHRU_PREHEADERS = 2
};

#define DLTHE_FLAG_UPDATE_FREQ	1	/* Update frequencies in
					   duplicate_loop_to_header_edge.  */
#define DLTHE_RECORD_COPY_NUMBER 2	/* Record copy number in the aux
					   field of newly create BB.  */
#define DLTHE_FLAG_COMPLETTE_PEEL 4	/* Update frequencies expecting
					   a complete peeling.  */
extern edge mfb_kj_edge;

extern bool remove_path (edge, bool * = NULL, bitmap = NULL);
extern void place_new_loop (struct function *, struct loop *);
extern void add_loop (struct loop *, struct loop *);
extern void scale_loop_frequencies (struct loop *, profile_probability);
extern void scale_loop_profile (struct loop *, profile_probability, gcov_type);
extern edge create_empty_if_region_on_edge (edge, tree);
extern struct loop *create_empty_loop_on_edge (edge, tree, tree, tree, tree,
					       tree *, tree *, struct loop *);
extern struct loop *loopify (edge, edge,
			     basic_block, edge, edge, bool,
			     profile_probability, profile_probability);
extern void unloop (struct loop *, bool *, bitmap);
extern void copy_loop_info (struct loop *loop, struct loop *target);
extern struct loop * duplicate_loop (struct loop *, struct loop *,
				     struct loop * = NULL);
extern void duplicate_subloops (struct loop *, struct loop *);
extern bool can_duplicate_loop_p (const struct loop *loop);
extern bool duplicate_loop_to_header_edge (struct loop *, edge,
					   unsigned, sbitmap, edge,
 					   vec<edge> *, int);
extern bool mfb_keep_just (edge);
basic_block create_preheader (struct loop *, int);
extern void create_preheaders (int);
extern void force_single_succ_latches (void);
struct loop * loop_version (struct loop *, void *,
			    basic_block *,
			    profile_probability, profile_probability,
			    profile_probability, profile_probability, bool);

#endif /* GCC_CFGLOOPMANIP_H */
