/* Garbage collection for the GNU compiler.

   Copyright (C) 1998-2018 Free Software Foundation, Inc.

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

#ifndef GCC_GGC_H
#define GCC_GGC_H

/* Symbols are marked with `ggc' for `gcc gc' so as not to interfere with
   an external gc library that might be linked in.  */

/* Internal functions and data structures used by the GTY
   machinery, including the generated gt*.[hc] files.  */

#include "gtype-desc.h"

/* One of these applies its third parameter (with cookie in the fourth
   parameter) to each pointer in the object pointed to by the first
   parameter, using the second parameter.  */
typedef void (*gt_note_pointers) (void *, void *, gt_pointer_operator,
				  void *);

/* One of these is called before objects are re-ordered in memory.
   The first parameter is the original object, the second is the
   subobject that has had its pointers reordered, the third parameter
   can compute the new values of a pointer when given the cookie in
   the fourth parameter.  */
typedef void (*gt_handle_reorder) (void *, void *, gt_pointer_operator,
				   void *);

/* Used by the gt_pch_n_* routines.  Register an object in the hash table.  */
extern int gt_pch_note_object (void *, void *, gt_note_pointers);

/* Used by the gt_pch_n_* routines.  Register that an object has a reorder
   function.  */
extern void gt_pch_note_reorder (void *, void *, gt_handle_reorder);

/* generated function to clear caches in gc memory.  */
extern void gt_clear_caches ();

/* Mark the object in the first parameter and anything it points to.  */
typedef void (*gt_pointer_walker) (void *);

/* Structures for the easy way to mark roots.
   In an array, terminated by having base == NULL.  */
struct ggc_root_tab {
  void *base;
  size_t nelt;
  size_t stride;
  gt_pointer_walker cb;
  gt_pointer_walker pchw;
};
#define LAST_GGC_ROOT_TAB { NULL, 0, 0, NULL, NULL }
/* Pointers to arrays of ggc_root_tab, terminated by NULL.  */
extern const struct ggc_root_tab * const gt_ggc_rtab[];
extern const struct ggc_root_tab * const gt_ggc_deletable_rtab[];
extern const struct ggc_root_tab * const gt_pch_scalar_rtab[];

/* If EXPR is not NULL and previously unmarked, mark it and evaluate
   to true.  Otherwise evaluate to false.  */
#define ggc_test_and_set_mark(EXPR) \
  ((EXPR) != NULL && ((void *) (EXPR)) != (void *) 1 && ! ggc_set_mark (EXPR))

#define ggc_mark(EXPR)				\
  do {						\
    const void *const a__ = (EXPR);		\
    if (a__ != NULL && a__ != (void *) 1)	\
      ggc_set_mark (a__);			\
  } while (0)

/* Actually set the mark on a particular region of memory, but don't
   follow pointers.  This function is called by ggc_mark_*.  It
   returns zero if the object was not previously marked; nonzero if
   the object was already marked, or if, for any other reason,
   pointers in this data structure should not be traversed.  */
extern int ggc_set_mark	(const void *);

/* Return 1 if P has been marked, zero otherwise.
   P must have been allocated by the GC allocator; it mustn't point to
   static objects, stack variables, or memory allocated with malloc.  */
extern int ggc_marked_p	(const void *);

/* PCH and GGC handling for strings, mostly trivial.  */
extern void gt_pch_n_S (const void *);
extern void gt_ggc_m_S (const void *);

/* End of GTY machinery API.  */

/* Initialize the string pool.  */
extern void init_stringpool (void);

/* Initialize the garbage collector.  */
extern void init_ggc (void);

/* When true, identifier nodes are considered as GC roots.  When
   false, identifier nodes are treated like any other GC-allocated
   object, and the identifier hash table is treated as a weak
   hash.  */
extern bool ggc_protect_identifiers;

/* Write out all GCed objects to F.  */
extern void gt_pch_save (FILE *f);


/* Allocation.  */

/* The internal primitive.  */
extern void *ggc_internal_alloc (size_t, void (*)(void *), size_t,
				 size_t CXX_MEM_STAT_INFO)
     ATTRIBUTE_MALLOC;

inline void *
ggc_internal_alloc (size_t s CXX_MEM_STAT_INFO)
{
  return ggc_internal_alloc (s, NULL, 0, 1 PASS_MEM_STAT);
}

extern size_t ggc_round_alloc_size (size_t requested_size);

/* Allocates cleared memory.  */
extern void *ggc_internal_cleared_alloc (size_t, void (*)(void *),
					 size_t, size_t
					 CXX_MEM_STAT_INFO) ATTRIBUTE_MALLOC;

inline void *
ggc_internal_cleared_alloc (size_t s CXX_MEM_STAT_INFO)
{
  return ggc_internal_cleared_alloc (s, NULL, 0, 1 PASS_MEM_STAT);
}

/* Resize a block.  */
extern void *ggc_realloc (void *, size_t CXX_MEM_STAT_INFO);

/* Free a block.  To be used when known for certain it's not reachable.  */
extern void ggc_free (void *);

extern void dump_ggc_loc_statistics (bool);

/* Reallocator.  */
#define GGC_RESIZEVEC(T, P, N) \
    ((T *) ggc_realloc ((P), (N) * sizeof (T) MEM_STAT_INFO))

template<typename T>
void
finalize (void *p)
{
  static_cast<T *> (p)->~T ();
}

template<typename T>
inline bool
need_finalization_p ()
{
#if GCC_VERSION >= 4003
  return !__has_trivial_destructor (T);
#else
  return true;
#endif
}

template<typename T>
inline T *
ggc_alloc (ALONE_CXX_MEM_STAT_INFO)
{
  if (need_finalization_p<T> ())
    return static_cast<T *> (ggc_internal_alloc (sizeof (T), finalize<T>, 0, 1
						 PASS_MEM_STAT));
  else
    return static_cast<T *> (ggc_internal_alloc (sizeof (T), NULL, 0, 1
						 PASS_MEM_STAT));
}

template<typename T>
inline T *
ggc_cleared_alloc (ALONE_CXX_MEM_STAT_INFO)
{
  if (need_finalization_p<T> ())
    return static_cast<T *> (ggc_internal_cleared_alloc (sizeof (T),
							 finalize<T>, 0, 1
							 PASS_MEM_STAT));
  else
    return static_cast<T *> (ggc_internal_cleared_alloc (sizeof (T), NULL, 0, 1
							 PASS_MEM_STAT));
}

template<typename T>
inline T *
ggc_vec_alloc (size_t c CXX_MEM_STAT_INFO)
{
  if (need_finalization_p<T> ())
    return static_cast<T *> (ggc_internal_alloc (c * sizeof (T), finalize<T>,
						 sizeof (T), c PASS_MEM_STAT));
  else
    return static_cast<T *> (ggc_internal_alloc (c * sizeof (T), NULL, 0, 0
						 PASS_MEM_STAT));
}

template<typename T>
inline T *
ggc_cleared_vec_alloc (size_t c CXX_MEM_STAT_INFO)
{
  if (need_finalization_p<T> ())
    return static_cast<T *> (ggc_internal_cleared_alloc (c * sizeof (T),
							 finalize<T>,
							 sizeof (T), c
							 PASS_MEM_STAT));
  else
    return static_cast<T *> (ggc_internal_cleared_alloc (c * sizeof (T), NULL,
							 0, 0 PASS_MEM_STAT));
}

inline void *
ggc_alloc_atomic (size_t s CXX_MEM_STAT_INFO)
{
    return ggc_internal_alloc (s PASS_MEM_STAT);
}

/* Allocate a gc-able string, and fill it with LENGTH bytes from CONTENTS.
   If LENGTH is -1, then CONTENTS is assumed to be a
   null-terminated string and the memory sized accordingly.  */
extern const char *ggc_alloc_string (const char *contents, int length
                                     CXX_MEM_STAT_INFO);

/* Make a copy of S, in GC-able memory.  */
#define ggc_strdup(S) ggc_alloc_string ((S), -1 MEM_STAT_INFO)

/* Invoke the collector.  Garbage collection occurs only when this
   function is called, not during allocations.  */
extern void ggc_collect	(void);

/* Assume that all GGC memory is reachable and grow the limits for next collection. */
extern void ggc_grow (void);

/* Register an additional root table.  This can be useful for some
   plugins.  Does nothing if the passed pointer is NULL. */
extern void ggc_register_root_tab (const struct ggc_root_tab *);

/* Read objects previously saved with gt_pch_save from F.  */
extern void gt_pch_restore (FILE *f);

/* Statistics.  */

/* Print allocation statistics.  */
extern void ggc_print_statistics (void);

extern void stringpool_statistics (void);

/* Heuristics.  */
extern void init_ggc_heuristics (void);

#define ggc_alloc_rtvec_sized(NELT)				\
  (rtvec_def *) ggc_internal_alloc (sizeof (struct rtvec_def)		\
		       + ((NELT) - 1) * sizeof (rtx))		\

/* Memory statistics passing versions of some allocators.  Too few of them to
   make gengtype produce them, so just define the needed ones here.  */
inline struct rtx_def *
ggc_alloc_rtx_def_stat (size_t s CXX_MEM_STAT_INFO)
{
  return (struct rtx_def *) ggc_internal_alloc (s PASS_MEM_STAT);
}

inline union tree_node *
ggc_alloc_tree_node_stat (size_t s CXX_MEM_STAT_INFO)
{
  return (union tree_node *) ggc_internal_alloc (s PASS_MEM_STAT);
}

inline union tree_node *
ggc_alloc_cleared_tree_node_stat (size_t s CXX_MEM_STAT_INFO)
{
  return (union tree_node *) ggc_internal_cleared_alloc (s PASS_MEM_STAT);
}

inline gimple *
ggc_alloc_cleared_gimple_statement_stat (size_t s CXX_MEM_STAT_INFO)
{
  return (gimple *) ggc_internal_cleared_alloc (s PASS_MEM_STAT);
}

inline void
gt_ggc_mx (const char *s)
{
  ggc_test_and_set_mark (const_cast<char *> (s));
}

inline void
gt_pch_nx (const char *)
{
}

inline void
gt_ggc_mx (int)
{
}

inline void
gt_pch_nx (int)
{
}

inline void
gt_pch_nx (unsigned int)
{
}

#endif
