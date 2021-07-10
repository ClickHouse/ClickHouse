/* Data references and dependences detectors.
   Copyright (C) 2003-2018 Free Software Foundation, Inc.
   Contributed by Sebastian Pop <pop@cri.ensmp.fr>

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

#ifndef GCC_TREE_DATA_REF_H
#define GCC_TREE_DATA_REF_H

#include "graphds.h"
#include "tree-chrec.h"

/*
  innermost_loop_behavior describes the evolution of the address of the memory
  reference in the innermost enclosing loop.  The address is expressed as
  BASE + STEP * # of iteration, and base is further decomposed as the base
  pointer (BASE_ADDRESS),  loop invariant offset (OFFSET) and
  constant offset (INIT).  Examples, in loop nest

  for (i = 0; i < 100; i++)
    for (j = 3; j < 100; j++)

                       Example 1                      Example 2
      data-ref         a[j].b[i][j]                   *(p + x + 16B + 4B * j)


  innermost_loop_behavior
      base_address     &a                             p
      offset           i * D_i			      x
      init             3 * D_j + offsetof (b)         28
      step             D_j                            4

  */
struct innermost_loop_behavior
{
  tree base_address;
  tree offset;
  tree init;
  tree step;

  /* BASE_ADDRESS is known to be misaligned by BASE_MISALIGNMENT bytes
     from an alignment boundary of BASE_ALIGNMENT bytes.  For example,
     if we had:

       struct S __attribute__((aligned(16))) { ... };

       char *ptr;
       ... *(struct S *) (ptr - 4) ...;

     the information would be:

       base_address:      ptr
       base_aligment:      16
       base_misalignment:   4
       init:               -4

     where init cancels the base misalignment.  If instead we had a
     reference to a particular field:

       struct S __attribute__((aligned(16))) { ... int f; ... };

       char *ptr;
       ... ((struct S *) (ptr - 4))->f ...;

     the information would be:

       base_address:      ptr
       base_aligment:      16
       base_misalignment:   4
       init:               -4 + offsetof (S, f)

     where base_address + init might also be misaligned, and by a different
     amount from base_address.  */
  unsigned int base_alignment;
  unsigned int base_misalignment;

  /* The largest power of two that divides OFFSET, capped to a suitably
     high value if the offset is zero.  This is a byte rather than a bit
     quantity.  */
  unsigned int offset_alignment;

  /* Likewise for STEP.  */
  unsigned int step_alignment;
};

/* Describes the evolutions of indices of the memory reference.  The indices
   are indices of the ARRAY_REFs, indexes in artificial dimensions
   added for member selection of records and the operands of MEM_REFs.
   BASE_OBJECT is the part of the reference that is loop-invariant
   (note that this reference does not have to cover the whole object
   being accessed, in which case UNCONSTRAINED_BASE is set; hence it is
   not recommended to use BASE_OBJECT in any code generation).
   For the examples above,

   base_object:        a                              *(p + x + 4B * j_0)
   indices:            {j_0, +, 1}_2                  {16, +, 4}_2
		       4
		       {i_0, +, 1}_1
		       {j_0, +, 1}_2
*/

struct indices
{
  /* The object.  */
  tree base_object;

  /* A list of chrecs.  Access functions of the indices.  */
  vec<tree> access_fns;

  /* Whether BASE_OBJECT is an access representing the whole object
     or whether the access could not be constrained.  */
  bool unconstrained_base;
};

struct dr_alias
{
  /* The alias information that should be used for new pointers to this
     location.  */
  struct ptr_info_def *ptr_info;
};

/* An integer vector.  A vector formally consists of an element of a vector
   space. A vector space is a set that is closed under vector addition
   and scalar multiplication.  In this vector space, an element is a list of
   integers.  */
typedef int *lambda_vector;

/* An integer matrix.  A matrix consists of m vectors of length n (IE
   all vectors are the same length).  */
typedef lambda_vector *lambda_matrix;



struct data_reference
{
  /* A pointer to the statement that contains this DR.  */
  gimple *stmt;

  /* A pointer to the memory reference.  */
  tree ref;

  /* Auxiliary info specific to a pass.  */
  void *aux;

  /* True when the data reference is in RHS of a stmt.  */
  bool is_read;

  /* True when the data reference is conditional within STMT,
     i.e. if it might not occur even when the statement is executed
     and runs to completion.  */
  bool is_conditional_in_stmt;

  /* Behavior of the memory reference in the innermost loop.  */
  struct innermost_loop_behavior innermost;

  /* Subscripts of this data reference.  */
  struct indices indices;

  /* Alias information for the data reference.  */
  struct dr_alias alias;
};

#define DR_STMT(DR)                (DR)->stmt
#define DR_REF(DR)                 (DR)->ref
#define DR_BASE_OBJECT(DR)         (DR)->indices.base_object
#define DR_UNCONSTRAINED_BASE(DR)  (DR)->indices.unconstrained_base
#define DR_ACCESS_FNS(DR)	   (DR)->indices.access_fns
#define DR_ACCESS_FN(DR, I)        DR_ACCESS_FNS (DR)[I]
#define DR_NUM_DIMENSIONS(DR)      DR_ACCESS_FNS (DR).length ()
#define DR_IS_READ(DR)             (DR)->is_read
#define DR_IS_WRITE(DR)            (!DR_IS_READ (DR))
#define DR_IS_CONDITIONAL_IN_STMT(DR) (DR)->is_conditional_in_stmt
#define DR_BASE_ADDRESS(DR)        (DR)->innermost.base_address
#define DR_OFFSET(DR)              (DR)->innermost.offset
#define DR_INIT(DR)                (DR)->innermost.init
#define DR_STEP(DR)                (DR)->innermost.step
#define DR_PTR_INFO(DR)            (DR)->alias.ptr_info
#define DR_BASE_ALIGNMENT(DR)      (DR)->innermost.base_alignment
#define DR_BASE_MISALIGNMENT(DR)   (DR)->innermost.base_misalignment
#define DR_OFFSET_ALIGNMENT(DR)    (DR)->innermost.offset_alignment
#define DR_STEP_ALIGNMENT(DR)      (DR)->innermost.step_alignment
#define DR_INNERMOST(DR)           (DR)->innermost

typedef struct data_reference *data_reference_p;

/* This struct is used to store the information of a data reference,
   including the data ref itself and the segment length for aliasing
   checks.  This is used to merge alias checks.  */

struct dr_with_seg_len
{
  dr_with_seg_len (data_reference_p d, tree len, unsigned HOST_WIDE_INT size,
		   unsigned int a)
    : dr (d), seg_len (len), access_size (size), align (a) {}

  data_reference_p dr;
  /* The offset of the last access that needs to be checked minus
     the offset of the first.  */
  tree seg_len;
  /* A value that, when added to abs (SEG_LEN), gives the total number of
     bytes in the segment.  */
  poly_uint64 access_size;
  /* The minimum common alignment of DR's start address, SEG_LEN and
     ACCESS_SIZE.  */
  unsigned int align;
};

/* This struct contains two dr_with_seg_len objects with aliasing data
   refs.  Two comparisons are generated from them.  */

struct dr_with_seg_len_pair_t
{
  dr_with_seg_len_pair_t (const dr_with_seg_len& d1,
			       const dr_with_seg_len& d2)
    : first (d1), second (d2) {}

  dr_with_seg_len first;
  dr_with_seg_len second;
};

enum data_dependence_direction {
  dir_positive,
  dir_negative,
  dir_equal,
  dir_positive_or_negative,
  dir_positive_or_equal,
  dir_negative_or_equal,
  dir_star,
  dir_independent
};

/* The description of the grid of iterations that overlap.  At most
   two loops are considered at the same time just now, hence at most
   two functions are needed.  For each of the functions, we store
   the vector of coefficients, f[0] + x * f[1] + y * f[2] + ...,
   where x, y, ... are variables.  */

#define MAX_DIM 2

/* Special values of N.  */
#define NO_DEPENDENCE 0
#define NOT_KNOWN (MAX_DIM + 1)
#define CF_NONTRIVIAL_P(CF) ((CF)->n != NO_DEPENDENCE && (CF)->n != NOT_KNOWN)
#define CF_NOT_KNOWN_P(CF) ((CF)->n == NOT_KNOWN)
#define CF_NO_DEPENDENCE_P(CF) ((CF)->n == NO_DEPENDENCE)

typedef vec<tree> affine_fn;

struct conflict_function
{
  unsigned n;
  affine_fn fns[MAX_DIM];
};

/* What is a subscript?  Given two array accesses a subscript is the
   tuple composed of the access functions for a given dimension.
   Example: Given A[f1][f2][f3] and B[g1][g2][g3], there are three
   subscripts: (f1, g1), (f2, g2), (f3, g3).  These three subscripts
   are stored in the data_dependence_relation structure under the form
   of an array of subscripts.  */

struct subscript
{
  /* The access functions of the two references.  */
  tree access_fn[2];

  /* A description of the iterations for which the elements are
     accessed twice.  */
  conflict_function *conflicting_iterations_in_a;
  conflict_function *conflicting_iterations_in_b;

  /* This field stores the information about the iteration domain
     validity of the dependence relation.  */
  tree last_conflict;

  /* Distance from the iteration that access a conflicting element in
     A to the iteration that access this same conflicting element in
     B.  The distance is a tree scalar expression, i.e. a constant or a
     symbolic expression, but certainly not a chrec function.  */
  tree distance;
};

typedef struct subscript *subscript_p;

#define SUB_ACCESS_FN(SUB, I) (SUB)->access_fn[I]
#define SUB_CONFLICTS_IN_A(SUB) (SUB)->conflicting_iterations_in_a
#define SUB_CONFLICTS_IN_B(SUB) (SUB)->conflicting_iterations_in_b
#define SUB_LAST_CONFLICT(SUB) (SUB)->last_conflict
#define SUB_DISTANCE(SUB) (SUB)->distance

/* A data_dependence_relation represents a relation between two
   data_references A and B.  */

struct data_dependence_relation
{

  struct data_reference *a;
  struct data_reference *b;

  /* A "yes/no/maybe" field for the dependence relation:

     - when "ARE_DEPENDENT == NULL_TREE", there exist a dependence
       relation between A and B, and the description of this relation
       is given in the SUBSCRIPTS array,

     - when "ARE_DEPENDENT == chrec_known", there is no dependence and
       SUBSCRIPTS is empty,

     - when "ARE_DEPENDENT == chrec_dont_know", there may be a dependence,
       but the analyzer cannot be more specific.  */
  tree are_dependent;

  /* If nonnull, COULD_BE_INDEPENDENT_P is true and the accesses are
     independent when the runtime addresses of OBJECT_A and OBJECT_B
     are different.  The addresses of both objects are invariant in the
     loop nest.  */
  tree object_a;
  tree object_b;

  /* For each subscript in the dependence test, there is an element in
     this array.  This is the attribute that labels the edge A->B of
     the data_dependence_relation.  */
  vec<subscript_p> subscripts;

  /* The analyzed loop nest.  */
  vec<loop_p> loop_nest;

  /* The classic direction vector.  */
  vec<lambda_vector> dir_vects;

  /* The classic distance vector.  */
  vec<lambda_vector> dist_vects;

  /* An index in loop_nest for the innermost loop that varies for
     this data dependence relation.  */
  unsigned inner_loop;

  /* Is the dependence reversed with respect to the lexicographic order?  */
  bool reversed_p;

  /* When the dependence relation is affine, it can be represented by
     a distance vector.  */
  bool affine_p;

  /* Set to true when the dependence relation is on the same data
     access.  */
  bool self_reference_p;

  /* True if the dependence described is conservatively correct rather
     than exact, and if it is still possible for the accesses to be
     conditionally independent.  For example, the a and b references in:

       struct s *a, *b;
       for (int i = 0; i < n; ++i)
         a->f[i] += b->f[i];

     conservatively have a distance vector of (0), for the case in which
     a == b, but the accesses are independent if a != b.  Similarly,
     the a and b references in:

       struct s *a, *b;
       for (int i = 0; i < n; ++i)
         a[0].f[i] += b[i].f[i];

     conservatively have a distance vector of (0), but they are indepenent
     when a != b + i.  In contrast, the references in:

       struct s *a;
       for (int i = 0; i < n; ++i)
         a->f[i] += a->f[i];

     have the same distance vector of (0), but the accesses can never be
     independent.  */
  bool could_be_independent_p;
};

typedef struct data_dependence_relation *ddr_p;

#define DDR_A(DDR) (DDR)->a
#define DDR_B(DDR) (DDR)->b
#define DDR_AFFINE_P(DDR) (DDR)->affine_p
#define DDR_ARE_DEPENDENT(DDR) (DDR)->are_dependent
#define DDR_OBJECT_A(DDR) (DDR)->object_a
#define DDR_OBJECT_B(DDR) (DDR)->object_b
#define DDR_SUBSCRIPTS(DDR) (DDR)->subscripts
#define DDR_SUBSCRIPT(DDR, I) DDR_SUBSCRIPTS (DDR)[I]
#define DDR_NUM_SUBSCRIPTS(DDR) DDR_SUBSCRIPTS (DDR).length ()

#define DDR_LOOP_NEST(DDR) (DDR)->loop_nest
/* The size of the direction/distance vectors: the number of loops in
   the loop nest.  */
#define DDR_NB_LOOPS(DDR) (DDR_LOOP_NEST (DDR).length ())
#define DDR_INNER_LOOP(DDR) (DDR)->inner_loop
#define DDR_SELF_REFERENCE(DDR) (DDR)->self_reference_p

#define DDR_DIST_VECTS(DDR) ((DDR)->dist_vects)
#define DDR_DIR_VECTS(DDR) ((DDR)->dir_vects)
#define DDR_NUM_DIST_VECTS(DDR) \
  (DDR_DIST_VECTS (DDR).length ())
#define DDR_NUM_DIR_VECTS(DDR) \
  (DDR_DIR_VECTS (DDR).length ())
#define DDR_DIR_VECT(DDR, I) \
  DDR_DIR_VECTS (DDR)[I]
#define DDR_DIST_VECT(DDR, I) \
  DDR_DIST_VECTS (DDR)[I]
#define DDR_REVERSED_P(DDR) (DDR)->reversed_p
#define DDR_COULD_BE_INDEPENDENT_P(DDR) (DDR)->could_be_independent_p


bool dr_analyze_innermost (innermost_loop_behavior *, tree, struct loop *);
extern bool compute_data_dependences_for_loop (struct loop *, bool,
					       vec<loop_p> *,
					       vec<data_reference_p> *,
					       vec<ddr_p> *);
extern void debug_ddrs (vec<ddr_p> );
extern void dump_data_reference (FILE *, struct data_reference *);
extern void debug (data_reference &ref);
extern void debug (data_reference *ptr);
extern void debug_data_reference (struct data_reference *);
extern void debug_data_references (vec<data_reference_p> );
extern void debug (vec<data_reference_p> &ref);
extern void debug (vec<data_reference_p> *ptr);
extern void debug_data_dependence_relation (struct data_dependence_relation *);
extern void dump_data_dependence_relations (FILE *, vec<ddr_p> );
extern void debug (vec<ddr_p> &ref);
extern void debug (vec<ddr_p> *ptr);
extern void debug_data_dependence_relations (vec<ddr_p> );
extern void free_dependence_relation (struct data_dependence_relation *);
extern void free_dependence_relations (vec<ddr_p> );
extern void free_data_ref (data_reference_p);
extern void free_data_refs (vec<data_reference_p> );
extern bool find_data_references_in_stmt (struct loop *, gimple *,
					  vec<data_reference_p> *);
extern bool graphite_find_data_references_in_stmt (edge, loop_p, gimple *,
						   vec<data_reference_p> *);
tree find_data_references_in_loop (struct loop *, vec<data_reference_p> *);
bool loop_nest_has_data_refs (loop_p loop);
struct data_reference *create_data_ref (edge, loop_p, tree, gimple *, bool,
					bool);
extern bool find_loop_nest (struct loop *, vec<loop_p> *);
extern struct data_dependence_relation *initialize_data_dependence_relation
     (struct data_reference *, struct data_reference *, vec<loop_p>);
extern void compute_affine_dependence (struct data_dependence_relation *,
				       loop_p);
extern void compute_self_dependence (struct data_dependence_relation *);
extern bool compute_all_dependences (vec<data_reference_p> ,
				     vec<ddr_p> *,
				     vec<loop_p>, bool);
extern tree find_data_references_in_bb (struct loop *, basic_block,
                                        vec<data_reference_p> *);
extern unsigned int dr_alignment (innermost_loop_behavior *);
extern tree get_base_for_alignment (tree, unsigned int *);

/* Return the alignment in bytes that DR is guaranteed to have at all
   times.  */

inline unsigned int
dr_alignment (data_reference *dr)
{
  return dr_alignment (&DR_INNERMOST (dr));
}

extern bool dr_may_alias_p (const struct data_reference *,
			    const struct data_reference *, bool);
extern bool dr_equal_offsets_p (struct data_reference *,
                                struct data_reference *);

extern bool runtime_alias_check_p (ddr_p, struct loop *, bool);
extern int data_ref_compare_tree (tree, tree);
extern void prune_runtime_alias_test_list (vec<dr_with_seg_len_pair_t> *,
					   poly_uint64);
extern void create_runtime_alias_checks (struct loop *,
					 vec<dr_with_seg_len_pair_t> *, tree*);
extern tree dr_direction_indicator (struct data_reference *);
extern tree dr_zero_step_indicator (struct data_reference *);
extern bool dr_known_forward_stride_p (struct data_reference *);

/* Return true when the base objects of data references A and B are
   the same memory object.  */

static inline bool
same_data_refs_base_objects (data_reference_p a, data_reference_p b)
{
  return DR_NUM_DIMENSIONS (a) == DR_NUM_DIMENSIONS (b)
    && operand_equal_p (DR_BASE_OBJECT (a), DR_BASE_OBJECT (b), 0);
}

/* Return true when the data references A and B are accessing the same
   memory object with the same access functions.  */

static inline bool
same_data_refs (data_reference_p a, data_reference_p b)
{
  unsigned int i;

  /* The references are exactly the same.  */
  if (operand_equal_p (DR_REF (a), DR_REF (b), 0))
    return true;

  if (!same_data_refs_base_objects (a, b))
    return false;

  for (i = 0; i < DR_NUM_DIMENSIONS (a); i++)
    if (!eq_evolutions_p (DR_ACCESS_FN (a, i), DR_ACCESS_FN (b, i)))
      return false;

  return true;
}

/* Returns true when all the dependences are computable.  */

inline bool
known_dependences_p (vec<ddr_p> dependence_relations)
{
  ddr_p ddr;
  unsigned int i;

  FOR_EACH_VEC_ELT (dependence_relations, i, ddr)
    if (DDR_ARE_DEPENDENT (ddr) == chrec_dont_know)
      return false;

  return true;
}

/* Returns the dependence level for a vector DIST of size LENGTH.
   LEVEL = 0 means a lexicographic dependence, i.e. a dependence due
   to the sequence of statements, not carried by any loop.  */

static inline unsigned
dependence_level (lambda_vector dist_vect, int length)
{
  int i;

  for (i = 0; i < length; i++)
    if (dist_vect[i] != 0)
      return i + 1;

  return 0;
}

/* Return the dependence level for the DDR relation.  */

static inline unsigned
ddr_dependence_level (ddr_p ddr)
{
  unsigned vector;
  unsigned level = 0;

  if (DDR_DIST_VECTS (ddr).exists ())
    level = dependence_level (DDR_DIST_VECT (ddr, 0), DDR_NB_LOOPS (ddr));

  for (vector = 1; vector < DDR_NUM_DIST_VECTS (ddr); vector++)
    level = MIN (level, dependence_level (DDR_DIST_VECT (ddr, vector),
					  DDR_NB_LOOPS (ddr)));
  return level;
}

/* Return the index of the variable VAR in the LOOP_NEST array.  */

static inline int
index_in_loop_nest (int var, vec<loop_p> loop_nest)
{
  struct loop *loopi;
  int var_index;

  for (var_index = 0; loop_nest.iterate (var_index, &loopi);
       var_index++)
    if (loopi->num == var)
      break;

  return var_index;
}

/* Returns true when the data reference DR the form "A[i] = ..."
   with a stride equal to its unit type size.  */

static inline bool
adjacent_dr_p (struct data_reference *dr)
{
  /* If this is a bitfield store bail out.  */
  if (TREE_CODE (DR_REF (dr)) == COMPONENT_REF
      && DECL_BIT_FIELD (TREE_OPERAND (DR_REF (dr), 1)))
    return false;

  if (!DR_STEP (dr)
      || TREE_CODE (DR_STEP (dr)) != INTEGER_CST)
    return false;

  return tree_int_cst_equal (fold_unary (ABS_EXPR, TREE_TYPE (DR_STEP (dr)),
					 DR_STEP (dr)),
			     TYPE_SIZE_UNIT (TREE_TYPE (DR_REF (dr))));
}

void split_constant_offset (tree , tree *, tree *);

/* Compute the greatest common divisor of a VECTOR of SIZE numbers.  */

static inline int
lambda_vector_gcd (lambda_vector vector, int size)
{
  int i;
  int gcd1 = 0;

  if (size > 0)
    {
      gcd1 = vector[0];
      for (i = 1; i < size; i++)
	gcd1 = gcd (gcd1, vector[i]);
    }
  return gcd1;
}

/* Allocate a new vector of given SIZE.  */

static inline lambda_vector
lambda_vector_new (int size)
{
  /* ???  We shouldn't abuse the GC allocator here.  */
  return ggc_cleared_vec_alloc<int> (size);
}

/* Clear out vector VEC1 of length SIZE.  */

static inline void
lambda_vector_clear (lambda_vector vec1, int size)
{
  memset (vec1, 0, size * sizeof (*vec1));
}

/* Returns true when the vector V is lexicographically positive, in
   other words, when the first nonzero element is positive.  */

static inline bool
lambda_vector_lexico_pos (lambda_vector v,
			  unsigned n)
{
  unsigned i;
  for (i = 0; i < n; i++)
    {
      if (v[i] == 0)
	continue;
      if (v[i] < 0)
	return false;
      if (v[i] > 0)
	return true;
    }
  return true;
}

/* Return true if vector VEC1 of length SIZE is the zero vector.  */

static inline bool
lambda_vector_zerop (lambda_vector vec1, int size)
{
  int i;
  for (i = 0; i < size; i++)
    if (vec1[i] != 0)
      return false;
  return true;
}

/* Allocate a matrix of M rows x  N cols.  */

static inline lambda_matrix
lambda_matrix_new (int m, int n, struct obstack *lambda_obstack)
{
  lambda_matrix mat;
  int i;

  mat = XOBNEWVEC (lambda_obstack, lambda_vector, m);

  for (i = 0; i < m; i++)
    mat[i] = XOBNEWVEC (lambda_obstack, int, n);

  return mat;
}

#endif  /* GCC_TREE_DATA_REF_H  */
