/* Simple bitmaps.
   Copyright (C) 1999-2018 Free Software Foundation, Inc.

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

#ifndef GCC_SBITMAP_H
#define GCC_SBITMAP_H

/* Implementation of sets using simple bitmap vectors.

   This set representation is suitable for non-sparse sets with a known
   (a priori) universe.  The set is represented as a simple array of the
   host's fastest unsigned integer.  For a given member I in the set:
     - the element for I will be at sbitmap[I / (bits per element)]
     - the position for I within element is I % (bits per element)

   This representation is very space-efficient for large non-sparse sets
   with random access patterns.

   The following operations can be performed in O(1) time:

     * set_size			: SBITMAP_SIZE
     * member_p			: bitmap_bit_p
     * add_member		: bitmap_set_bit
     * remove_member		: bitmap_clear_bit

   Most other operations on this set representation are O(U) where U is
   the size of the set universe:

     * clear			: bitmap_clear
     * choose_one		: bitmap_first_set_bit /
				  bitmap_last_set_bit
     * forall			: EXECUTE_IF_SET_IN_BITMAP
     * set_copy			: bitmap_copy
     * set_intersection		: bitmap_and
     * set_union		: bitmap_ior
     * set_difference		: bitmap_and_compl
     * set_disjuction		: (not implemented)
     * set_compare		: bitmap_equal_p
     * bit_in_range_p		: bitmap_bit_in_range_p

   Some operations on 3 sets that occur frequently in data flow problems
   are also implemented:

      * A | (B & C)		: bitmap_or_and
      * A | (B & ~C)		: bitmap_ior_and_compl
      * A & (B | C)		: bitmap_and_or

   Most of the set functions have two variants: One that returns non-zero
   if members were added or removed from the target set, and one that just
   performs the operation without feedback.  The former operations are a
   bit more expensive but the result can often be used to avoid iterations
   on other sets.

   Allocating a bitmap is done with sbitmap_alloc, and resizing is
   performed with sbitmap_resize.

   The storage requirements for simple bitmap sets is O(U) where U is the
   size of the set universe (colloquially the number of bits in the bitmap).

   This set representation works well for relatively small data flow problems
   (there are special routines for that, see sbitmap_vector_*).  The set
   operations can be vectorized and there is almost no computating overhead,
   so that even sparse simple bitmap sets outperform dedicated sparse set
   representations like linked-list bitmaps.  For larger problems, the size
   overhead of simple bitmap sets gets too high and other set representations
   have to be used.  */

#define SBITMAP_ELT_BITS (HOST_BITS_PER_WIDEST_FAST_INT * 1u)
#define SBITMAP_ELT_TYPE unsigned HOST_WIDEST_FAST_INT

struct simple_bitmap_def
{
  unsigned int n_bits;		/* Number of bits.  */
  unsigned int size;		/* Size in elements.  */
  SBITMAP_ELT_TYPE elms[1];	/* The elements.  */
};

/* Return the set size needed for N elements.  */
#define SBITMAP_SET_SIZE(N) (((N) + SBITMAP_ELT_BITS - 1) / SBITMAP_ELT_BITS)

/* Return the number of bits in BITMAP.  */
#define SBITMAP_SIZE(BITMAP) ((BITMAP)->n_bits)

/* Verify that access at INDEX in bitmap MAP is valid.  */ 

static inline void
bitmap_check_index (const_sbitmap map, int index)
{
  gcc_checking_assert (index >= 0);
  gcc_checking_assert ((unsigned int)index < map->n_bits);
}

/* Verify that bitmaps A and B have same size.  */ 

static inline void
bitmap_check_sizes (const_sbitmap a, const_sbitmap b)
{
  gcc_checking_assert (a->n_bits == b->n_bits);
}

/* Test if bit number bitno in the bitmap is set.  */
static inline SBITMAP_ELT_TYPE
bitmap_bit_p (const_sbitmap map, int bitno)
{
  bitmap_check_index (map, bitno);

  size_t i = bitno / SBITMAP_ELT_BITS;
  unsigned int s = bitno % SBITMAP_ELT_BITS;
  return (map->elms[i] >> s) & (SBITMAP_ELT_TYPE) 1;
}

/* Set bit number BITNO in the sbitmap MAP.  */

static inline void
bitmap_set_bit (sbitmap map, int bitno)
{
  bitmap_check_index (map, bitno);

  map->elms[bitno / SBITMAP_ELT_BITS]
    |= (SBITMAP_ELT_TYPE) 1 << (bitno) % SBITMAP_ELT_BITS;
}

/* Reset bit number BITNO in the sbitmap MAP.  */

static inline void
bitmap_clear_bit (sbitmap map, int bitno)
{
  bitmap_check_index (map, bitno);

  map->elms[bitno / SBITMAP_ELT_BITS]
    &= ~((SBITMAP_ELT_TYPE) 1 << (bitno) % SBITMAP_ELT_BITS);
}

/* The iterator for sbitmap.  */
struct sbitmap_iterator {
  /* The pointer to the first word of the bitmap.  */
  const SBITMAP_ELT_TYPE *ptr;

  /* The size of the bitmap.  */
  unsigned int size;

  /* The current word index.  */
  unsigned int word_num;

  /* The current bit index (not modulo SBITMAP_ELT_BITS).  */
  unsigned int bit_num;

  /* The words currently visited.  */
  SBITMAP_ELT_TYPE word;
};

/* Initialize the iterator I with sbitmap BMP and the initial index
   MIN.  */

static inline void
bmp_iter_set_init (sbitmap_iterator *i, const_sbitmap bmp,
		   unsigned int min, unsigned *bit_no ATTRIBUTE_UNUSED)
{
  i->word_num = min / (unsigned int) SBITMAP_ELT_BITS;
  i->bit_num = min;
  i->size = bmp->size;
  i->ptr = bmp->elms;

  if (i->word_num >= i->size)
    i->word = 0;
  else
    i->word = (i->ptr[i->word_num]
	       >> (i->bit_num % (unsigned int) SBITMAP_ELT_BITS));
}

/* Return true if we have more bits to visit, in which case *N is set
   to the index of the bit to be visited.  Otherwise, return
   false.  */

static inline bool
bmp_iter_set (sbitmap_iterator *i, unsigned int *n)
{
  /* Skip words that are zeros.  */
  for (; i->word == 0; i->word = i->ptr[i->word_num])
    {
      i->word_num++;

      /* If we have reached the end, break.  */
      if (i->word_num >= i->size)
	return false;

      i->bit_num = i->word_num * SBITMAP_ELT_BITS;
    }

  /* Skip bits that are zero.  */
  for (; (i->word & 1) == 0; i->word >>= 1)
    i->bit_num++;

  *n = i->bit_num;

  return true;
}

/* Advance to the next bit.  */

static inline void
bmp_iter_next (sbitmap_iterator *i, unsigned *bit_no ATTRIBUTE_UNUSED)
{
  i->word >>= 1;
  i->bit_num++;
}

/* Loop over all elements of SBITMAP, starting with MIN.  In each
   iteration, N is set to the index of the bit being visited.  ITER is
   an instance of sbitmap_iterator used to iterate the bitmap.  */

#ifndef EXECUTE_IF_SET_IN_BITMAP
/* See bitmap.h for the other definition of EXECUTE_IF_SET_IN_BITMAP.  */
#define EXECUTE_IF_SET_IN_BITMAP(BITMAP, MIN, BITNUM, ITER)	\
  for (bmp_iter_set_init (&(ITER), (BITMAP), (MIN), &(BITNUM));	\
       bmp_iter_set (&(ITER), &(BITNUM));			\
       bmp_iter_next (&(ITER), &(BITNUM)))
#endif

inline void sbitmap_free (sbitmap map)
{
  free (map);
}

inline void sbitmap_vector_free (sbitmap * vec)
{
  free (vec);
}

extern void dump_bitmap (FILE *, const_sbitmap);
extern void debug_raw (const simple_bitmap_def &ref);
extern void debug_raw (const simple_bitmap_def *ptr);
extern void dump_bitmap_file (FILE *, const_sbitmap);
extern void debug (const simple_bitmap_def &ref);
extern void debug (const simple_bitmap_def *ptr);
extern void dump_bitmap_vector (FILE *, const char *, const char *, sbitmap *,
				 int);
extern sbitmap sbitmap_alloc (unsigned int);
extern sbitmap *sbitmap_vector_alloc (unsigned int, unsigned int);
extern sbitmap sbitmap_resize (sbitmap, unsigned int, int);
extern void bitmap_copy (sbitmap, const_sbitmap);
extern int bitmap_equal_p (const_sbitmap, const_sbitmap);
extern unsigned int bitmap_count_bits (const_sbitmap);
extern bool bitmap_empty_p (const_sbitmap);
extern void bitmap_clear (sbitmap);
extern void bitmap_clear_range (sbitmap, unsigned, unsigned);
extern void bitmap_set_range (sbitmap, unsigned, unsigned);
extern void bitmap_ones (sbitmap);
extern void bitmap_vector_clear (sbitmap *, unsigned int);
extern void bitmap_vector_ones (sbitmap *, unsigned int);

extern bool bitmap_ior_and_compl (sbitmap, const_sbitmap,
				      const_sbitmap, const_sbitmap);
extern void bitmap_and_compl (sbitmap, const_sbitmap, const_sbitmap);
extern void bitmap_not (sbitmap, const_sbitmap);
extern bool bitmap_or_and (sbitmap, const_sbitmap,
				     const_sbitmap, const_sbitmap);
extern bool bitmap_and_or (sbitmap, const_sbitmap,
				     const_sbitmap, const_sbitmap);
extern bool bitmap_intersect_p (const_sbitmap, const_sbitmap);
extern bool bitmap_and (sbitmap, const_sbitmap, const_sbitmap);
extern bool bitmap_ior (sbitmap, const_sbitmap, const_sbitmap);
extern bool bitmap_xor (sbitmap, const_sbitmap, const_sbitmap);
extern bool bitmap_subset_p (const_sbitmap, const_sbitmap);
extern bool bitmap_bit_in_range_p (const_sbitmap, unsigned int, unsigned int);

extern int bitmap_first_set_bit (const_sbitmap);
extern int bitmap_last_set_bit (const_sbitmap);

extern void debug_bitmap (const_sbitmap);
extern sbitmap sbitmap_realloc (sbitmap, unsigned int);

/* a class that ties the lifetime of a sbitmap to its scope.  */
class auto_sbitmap
{
public:
  explicit auto_sbitmap (unsigned int size) :
    m_bitmap (sbitmap_alloc (size)) {}
  ~auto_sbitmap () { sbitmap_free (m_bitmap); }

  /* Allow calling sbitmap functions on our bitmap.  */
  operator sbitmap () { return m_bitmap; }

private:
  /* Prevent making a copy that refers to our sbitmap.  */
  auto_sbitmap (const auto_sbitmap &);
  auto_sbitmap &operator = (const auto_sbitmap &);
#if __cplusplus >= 201103L
  auto_sbitmap (auto_sbitmap &&);
  auto_sbitmap &operator = (auto_sbitmap &&);
#endif

  /* The bitmap we are managing.  */
  sbitmap m_bitmap;
};

#endif /* ! GCC_SBITMAP_H */
