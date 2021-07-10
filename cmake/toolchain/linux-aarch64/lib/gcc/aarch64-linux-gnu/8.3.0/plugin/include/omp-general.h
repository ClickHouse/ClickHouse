/* General types and functions that are uselful for processing of OpenMP,
   OpenACC and similar directivers at various stages of compilation.

   Copyright (C) 2005-2018 Free Software Foundation, Inc.

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

#ifndef GCC_OMP_GENERAL_H
#define GCC_OMP_GENERAL_H

#include "gomp-constants.h"

/*  Flags for an OpenACC loop.  */

enum oacc_loop_flags {
  OLF_SEQ	= 1u << 0,  /* Explicitly sequential  */
  OLF_AUTO	= 1u << 1,	/* Compiler chooses axes.  */
  OLF_INDEPENDENT = 1u << 2,	/* Iterations are known independent.  */
  OLF_GANG_STATIC = 1u << 3,	/* Gang partitioning is static (has op). */
  OLF_TILE	= 1u << 4,	/* Tiled loop. */
  
  /* Explicitly specified loop axes.  */
  OLF_DIM_BASE = 5,
  OLF_DIM_GANG   = 1u << (OLF_DIM_BASE + GOMP_DIM_GANG),
  OLF_DIM_WORKER = 1u << (OLF_DIM_BASE + GOMP_DIM_WORKER),
  OLF_DIM_VECTOR = 1u << (OLF_DIM_BASE + GOMP_DIM_VECTOR),

  OLF_MAX = OLF_DIM_BASE + GOMP_DIM_MAX
};

/* A structure holding the elements of:
   for (V = N1; V cond N2; V += STEP) [...] */

struct omp_for_data_loop
{
  tree v, n1, n2, step;
  enum tree_code cond_code;
};

/* A structure describing the main elements of a parallel loop.  */

struct omp_for_data
{
  struct omp_for_data_loop loop;
  tree chunk_size;
  gomp_for *for_stmt;
  tree pre, iter_type;
  tree tiling;  /* Tiling values (if non null).  */
  int collapse;  /* Collapsed loops, 1 for a non-collapsed loop.  */
  int ordered;
  bool have_nowait, have_ordered, simd_schedule;
  unsigned char sched_modifiers;
  enum omp_clause_schedule_kind sched_kind;
  struct omp_for_data_loop *loops;
};

#define OACC_FN_ATTRIB "oacc function"

extern tree omp_find_clause (tree clauses, enum omp_clause_code kind);
extern bool omp_is_reference (tree decl);
extern void omp_adjust_for_condition (location_t loc, enum tree_code *cond_code,
				      tree *n2);
extern tree omp_get_for_step_from_incr (location_t loc, tree incr);
extern void omp_extract_for_data (gomp_for *for_stmt, struct omp_for_data *fd,
				  struct omp_for_data_loop *loops);
extern gimple *omp_build_barrier (tree lhs);
extern poly_uint64 omp_max_vf (void);
extern int omp_max_simt_vf (void);
extern tree oacc_launch_pack (unsigned code, tree device, unsigned op);
extern void oacc_replace_fn_attrib (tree fn, tree dims);
extern void oacc_set_fn_attrib (tree fn, tree clauses, vec<tree> *args);
extern tree oacc_build_routine_dims (tree clauses);
extern tree oacc_get_fn_attrib (tree fn);
extern bool offloading_function_p (tree fn);
extern int oacc_get_fn_dim_size (tree fn, int axis);
extern int oacc_get_ifn_dim_arg (const gimple *stmt);

#endif /* GCC_OMP_GENERAL_H */
