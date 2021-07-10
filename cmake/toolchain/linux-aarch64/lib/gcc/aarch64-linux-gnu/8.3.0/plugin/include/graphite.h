/* Graphite polyhedral representation.
   Copyright (C) 2009-2018 Free Software Foundation, Inc.
   Contributed by Sebastian Pop <sebastian.pop@amd.com> and
   Tobias Grosser <grosser@fim.uni-passau.de>.

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

#ifndef GCC_GRAPHITE_POLY_H
#define GCC_GRAPHITE_POLY_H

#include "sese.h"
#include <isl/options.h>
#include <isl/ctx.h>
#include <isl/val.h>
#include <isl/set.h>
#include <isl/union_set.h>
#include <isl/map.h>
#include <isl/union_map.h>
#include <isl/aff.h>
#include <isl/constraint.h>
#include <isl/flow.h>
#include <isl/ilp.h>
#include <isl/schedule.h>
#include <isl/ast_build.h>
#include <isl/schedule_node.h>
#include <isl/id.h>
#include <isl/space.h>

typedef struct poly_dr *poly_dr_p;

typedef struct poly_bb *poly_bb_p;

typedef struct scop *scop_p;

typedef unsigned graphite_dim_t;

static inline graphite_dim_t scop_nb_params (scop_p);

/* A data reference can write or read some memory or we
   just know it may write some memory.  */
enum poly_dr_type
{
  PDR_READ,
  /* PDR_MAY_READs are represented using PDR_READS.  This does not
     limit the expressiveness.  */
  PDR_WRITE,
  PDR_MAY_WRITE
};

struct poly_dr
{
  /* An identifier for this PDR.  */
  int id;

  /* The number of data refs identical to this one in the PBB.  */
  int nb_refs;

  /* A pointer to the gimple stmt containing this reference.  */
  gimple *stmt;

  /* A pointer to the PBB that contains this data reference.  */
  poly_bb_p pbb;

  enum poly_dr_type type;

  /* The access polyhedron contains the polyhedral space this data
     reference will access.

     The polyhedron contains these dimensions:

     - The alias set (a):
     Every memory access is classified in at least one alias set.

     - The subscripts (s_0, ..., s_n):
     The memory is accessed using zero or more subscript dimensions.

     - The iteration domain (variables and parameters)

     Do not hardcode the dimensions.  Use the following accessor functions:
     - pdr_alias_set_dim
     - pdr_subscript_dim
     - pdr_iterator_dim
     - pdr_parameter_dim

     Example:

     | int A[1335][123];
     | int *p = malloc ();
     |
     | k = ...
     | for i
     |   {
     |     if (unknown_function ())
     |       p = A;
     |       ... = p[?][?];
     | 	   for j
     |       A[i][j+k] = m;
     |   }

     The data access A[i][j+k] in alias set "5" is described like this:

     | i   j   k   a  s0  s1   1
     | 0   0   0   1   0   0  -5     =  0
     |-1   0   0   0   1   0   0     =  0
     | 0  -1  -1   0   0   1   0     =  0
     | 0   0   0   0   1   0   0     >= 0  # The last four lines describe the
     | 0   0   0   0   0   1   0     >= 0  # array size.
     | 0   0   0   0  -1   0 1335    >= 0
     | 0   0   0   0   0  -1 123     >= 0

     The pointer "*p" in alias set "5" and "7" is described as a union of
     polyhedron:


     | i   k   a  s0   1
     | 0   0   1   0  -5   =  0
     | 0   0   0   1   0   >= 0

     "or"

     | i   k   a  s0   1
     | 0   0   1   0  -7   =  0
     | 0   0   0   1   0   >= 0

     "*p" accesses all of the object allocated with 'malloc'.

     The scalar data access "m" is represented as an array with zero subscript
     dimensions.

     | i   j   k   a   1
     | 0   0   0  -1   15  = 0

     The difference between the graphite internal format for access data and
     the OpenSop format is in the order of columns.
     Instead of having:

     | i   j   k   a  s0  s1   1
     | 0   0   0   1   0   0  -5     =  0
     |-1   0   0   0   1   0   0     =  0
     | 0  -1  -1   0   0   1   0     =  0
     | 0   0   0   0   1   0   0     >= 0  # The last four lines describe the
     | 0   0   0   0   0   1   0     >= 0  # array size.
     | 0   0   0   0  -1   0 1335    >= 0
     | 0   0   0   0   0  -1 123     >= 0

     In OpenScop we have:

     | a  s0  s1   i   j   k   1
     | 1   0   0   0   0   0  -5     =  0
     | 0   1   0  -1   0   0   0     =  0
     | 0   0   1   0  -1  -1   0     =  0
     | 0   1   0   0   0   0   0     >= 0  # The last four lines describe the
     | 0   0   1   0   0   0   0     >= 0  # array size.
     | 0  -1   0   0   0   0 1335    >= 0
     | 0   0  -1   0   0   0 123     >= 0

     The OpenScop access function is printed as follows:

     | 1  # The number of disjunct components in a union of access functions.
     | R C O I L P  # Described bellow.
     | a  s0  s1   i   j   k   1
     | 1   0   0   0   0   0  -5     =  0
     | 0   1   0  -1   0   0   0     =  0
     | 0   0   1   0  -1  -1   0     =  0
     | 0   1   0   0   0   0   0     >= 0  # The last four lines describe the
     | 0   0   1   0   0   0   0     >= 0  # array size.
     | 0  -1   0   0   0   0 1335    >= 0
     | 0   0  -1   0   0   0 123     >= 0

     Where:
     - R: Number of rows.
     - C: Number of columns.
     - O: Number of output dimensions = alias set + number of subscripts.
     - I: Number of input dimensions (iterators).
     - L: Number of local (existentially quantified) dimensions.
     - P: Number of parameters.

     In the example, the vector "R C O I L P" is "7 7 3 2 0 1".  */
  isl_map *accesses;
  isl_set *subscript_sizes;
};

#define PDR_ID(PDR) (PDR->id)
#define PDR_NB_REFS(PDR) (PDR->nb_refs)
#define PDR_PBB(PDR) (PDR->pbb)
#define PDR_TYPE(PDR) (PDR->type)
#define PDR_ACCESSES(PDR) (NULL)

void new_poly_dr (poly_bb_p, gimple *, enum poly_dr_type,
		  isl_map *, isl_set *);
void debug_pdr (poly_dr_p);
void print_pdr (FILE *, poly_dr_p);

static inline bool
pdr_read_p (poly_dr_p pdr)
{
  return PDR_TYPE (pdr) == PDR_READ;
}

/* Returns true when PDR is a "write".  */

static inline bool
pdr_write_p (poly_dr_p pdr)
{
  return PDR_TYPE (pdr) == PDR_WRITE;
}

/* Returns true when PDR is a "may write".  */

static inline bool
pdr_may_write_p (poly_dr_p pdr)
{
  return PDR_TYPE (pdr) == PDR_MAY_WRITE;
}

/* POLY_BB represents a blackbox in the polyhedral model.  */

struct poly_bb
{
  /* Pointer to a basic block or a statement in the compiler.  */
  gimple_poly_bb_p black_box;

  /* Pointer to the SCOP containing this PBB.  */
  scop_p scop;

  /* The iteration domain of this bb.  The layout of this polyhedron
     is I|G with I the iteration domain, G the context parameters.

     Example:

     for (i = a - 7*b + 8; i <= 3*a + 13*b + 20; i++)
       for (j = 2; j <= 2*i + 5; j++)
         for (k = 0; k <= 5; k++)
           S (i,j,k)

     Loop iterators: i, j, k
     Parameters: a, b

     | i >=  a -  7b +  8
     | i <= 3a + 13b + 20
     | j >= 2
     | j <= 2i + 5
     | k >= 0
     | k <= 5

     The number of variables in the DOMAIN may change and is not
     related to the number of loops in the original code.  */
  isl_set *domain;
  isl_set *iterators;

  /* The data references we access.  */
  vec<poly_dr_p> drs;

  /* The last basic block generated for this pbb.  */
  basic_block new_bb;
};

#define PBB_BLACK_BOX(PBB) ((gimple_poly_bb_p) PBB->black_box)
#define PBB_SCOP(PBB) (PBB->scop)
#define PBB_DRS(PBB) (PBB->drs)

extern poly_bb_p new_poly_bb (scop_p, gimple_poly_bb_p);
extern void print_pbb_domain (FILE *, poly_bb_p);
extern void print_pbb (FILE *, poly_bb_p);
extern void print_scop_context (FILE *, scop_p);
extern void print_scop (FILE *, scop_p);
extern void debug_pbb_domain (poly_bb_p);
extern void debug_pbb (poly_bb_p);
extern void print_pdrs (FILE *, poly_bb_p);
extern void debug_pdrs (poly_bb_p);
extern void debug_scop_context (scop_p);
extern void debug_scop (scop_p);
extern void print_scop_params (FILE *, scop_p);
extern void debug_scop_params (scop_p);
extern void print_iteration_domain (FILE *, poly_bb_p);
extern void print_iteration_domains (FILE *, scop_p);
extern void debug_iteration_domain (poly_bb_p);
extern void debug_iteration_domains (scop_p);
extern void print_isl_set (FILE *, isl_set *);
extern void print_isl_map (FILE *, isl_map *);
extern void print_isl_union_map (FILE *, isl_union_map *);
extern void print_isl_aff (FILE *, isl_aff *);
extern void print_isl_constraint (FILE *, isl_constraint *);
extern void print_isl_schedule (FILE *, isl_schedule *);
extern void debug_isl_schedule (isl_schedule *);
extern void print_isl_ast (FILE *, isl_ast_node *);
extern void debug_isl_ast (isl_ast_node *);
extern void debug_isl_set (isl_set *);
extern void debug_isl_map (isl_map *);
extern void debug_isl_union_map (isl_union_map *);
extern void debug_isl_aff (isl_aff *);
extern void debug_isl_constraint (isl_constraint *);
extern void debug_gmp_value (mpz_t);
extern void debug_scop_pbb (scop_p scop, int i);
extern void print_schedule_ast (FILE *, __isl_keep isl_schedule *, scop_p);
extern void debug_schedule_ast (__isl_keep isl_schedule *, scop_p);

/* The basic block of the PBB.  */

static inline basic_block
pbb_bb (poly_bb_p pbb)
{
  return GBB_BB (PBB_BLACK_BOX (pbb));
}

static inline int
pbb_index (poly_bb_p pbb)
{
  return pbb_bb (pbb)->index;
}

/* The loop of the PBB.  */

static inline loop_p
pbb_loop (poly_bb_p pbb)
{
  return gbb_loop (PBB_BLACK_BOX (pbb));
}

/* The scop that contains the PDR.  */

static inline scop_p
pdr_scop (poly_dr_p pdr)
{
  return PBB_SCOP (PDR_PBB (pdr));
}

/* Set black box of PBB to BLACKBOX.  */

static inline void
pbb_set_black_box (poly_bb_p pbb, gimple_poly_bb_p black_box)
{
  pbb->black_box = black_box;
}

/* A helper structure to keep track of data references, polyhedral BBs, and
   alias sets.  */

struct dr_info
{
  enum {
    invalid_alias_set = -1
  };
  /* The data reference.  */
  data_reference_p dr;

  /* The polyhedral BB containing this DR.  */
  poly_bb_p pbb;

  /* ALIAS_SET is the SCC number assigned by a graph_dfs of the alias graph.
     -1 is an invalid alias set.  */
  int alias_set;

  /* Construct a DR_INFO from a data reference DR, an ALIAS_SET, and a PBB.  */
  dr_info (data_reference_p dr, poly_bb_p pbb,
	   int alias_set = invalid_alias_set)
    : dr (dr), pbb (pbb), alias_set (alias_set) {}
};

/* A SCOP is a Static Control Part of the program, simple enough to be
   represented in polyhedral form.  */
struct scop
{
  /* A SCOP is defined as a SESE region.  */
  sese_info_p scop_info;

  /* Number of parameters in SCoP.  */
  graphite_dim_t nb_params;

  /* The maximum alias set as assigned to drs by build_alias_sets.  */
  unsigned max_alias_set;

  /* All the basic blocks in this scop that contain memory references
     and that will be represented as statements in the polyhedral
     representation.  */
  vec<poly_bb_p> pbbs;

  /* All the data references in this scop.  */
  vec<dr_info> drs;

  /* The context describes known restrictions concerning the parameters
     and relations in between the parameters.

  void f (int8_t a, uint_16_t b) {
    c = 2 a + b;
    ...
  }

  Here we can add these restrictions to the context:

  -128 >= a >= 127
     0 >= b >= 65,535
     c = 2a + b  */
  isl_set *param_context;

  /* The context used internally by isl.  */
  isl_ctx *isl_context;

  /* SCoP original schedule.  */
  isl_schedule *original_schedule;

  /* SCoP transformed schedule.  */
  isl_schedule *transformed_schedule;

  /* The data dependence relation among the data references in this scop.  */
  isl_union_map *dependence;
};

extern scop_p new_scop (edge, edge);
extern void free_scop (scop_p);
extern gimple_poly_bb_p new_gimple_poly_bb (basic_block, vec<data_reference_p>,
					    vec<scalar_use>, vec<tree>);
extern bool apply_poly_transforms (scop_p);

/* Set the region of SCOP to REGION.  */

static inline void
scop_set_region (scop_p scop, sese_info_p region)
{
  scop->scop_info = region;
}

/* Returns the number of parameters for SCOP.  */

static inline graphite_dim_t
scop_nb_params (scop_p scop)
{
  return scop->nb_params;
}

/* Set the number of params of SCOP to NB_PARAMS.  */

static inline void
scop_set_nb_params (scop_p scop, graphite_dim_t nb_params)
{
  scop->nb_params = nb_params;
}

extern void scop_get_dependences (scop_p scop);

bool
carries_deps (__isl_keep isl_union_map *schedule,
	      __isl_keep isl_union_map *deps,
	      int depth);

extern bool build_poly_scop (scop_p);
extern bool graphite_regenerate_ast_isl (scop_p);
extern void build_scops (vec<scop_p> *);
extern void dot_all_sese (FILE *, vec<sese_l> &);
extern void dot_sese (sese_l &);
extern void dot_cfg ();

#endif
