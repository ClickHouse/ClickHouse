/* Functions to support general ended bitmaps.
   Copyright (C) 1997-2018 Free Software Foundation, Inc.

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

#ifndef GCC_BITMAP_H
#define GCC_BITMAP_H

/* Implementation of sparse integer sets as a linked list.

   This sparse set representation is suitable for sparse sets with an
   unknown (a priori) universe.  The set is represented as a double-linked
   list of container nodes (struct bitmap_element).  Each node consists
   of an index for the first member that could be held in the container,
   a small array of integers that represent the members in the container,
   and pointers to the next and previous element in the linked list.  The
   elements in the list are sorted in ascending order, i.e. the head of
   the list holds the element with the smallest member of the set.

   For a given member I in the set:
     - the element for I will have index is I / (bits per element)
     - the position for I within element is I % (bits per element)

   This representation is very space-efficient for large sparse sets, and
   the size of the set can be changed dynamically without much overhead.
   An important parameter is the number of bits per element.  In this
   implementation, there are 128 bits per element.  This results in a
   high storage overhead *per element*, but a small overall overhead if
   the set is very sparse.

   The downside is that many operations are relatively slow because the
   linked list has to be traversed to test membership (i.e. member_p/
   add_member/remove_member).  To improve the performance of this set
   representation, the last accessed element and its index are cached.
   For membership tests on members close to recently accessed members,
   the cached last element improves membership test to a constant-time
   operation.

   The following operations can always be performed in O(1) time:

     * clear			: bitmap_clear
     * choose_one		: (not implemented, but could be
				   implemented in constant time)

   The following operations can be performed in O(E) time worst-case (with
   E the number of elements in the linked list), but in O(1) time with a
   suitable access patterns:

     * member_p			: bitmap_bit_p
     * add_member		: bitmap_set_bit
     * remove_member		: bitmap_clear_bit

   The following operations can be performed in O(E) time:

     * cardinality		: bitmap_count_bits
     * set_size			: bitmap_last_set_bit (but this could
				  in constant time with a pointer to
				  the last element in the chain)

   Additionally, the linked-list sparse set representation supports
   enumeration of the members in O(E) time:

     * forall			: EXECUTE_IF_SET_IN_BITMAP
     * set_copy			: bitmap_copy
     * set_intersection		: bitmap_intersect_p /
				  bitmap_and / bitmap_and_into /
				  EXECUTE_IF_AND_IN_BITMAP
     * set_union		: bitmap_ior / bitmap_ior_into
     * set_difference		: bitmap_intersect_compl_p /
				  bitmap_and_comp / bitmap_and_comp_into /
				  EXECUTE_IF_AND_COMPL_IN_BITMAP
     * set_disjuction		: bitmap_xor_comp / bitmap_xor_comp_into
     * set_compare		: bitmap_equal_p

   Some operations on 3 sets that occur frequently in data flow problems
   are also implemented:

     * A | (B & C)		: bitmap_ior_and_into
     * A | (B & ~C)		: bitmap_ior_and_compl /
				  bitmap_ior_and_compl_into

   The storage requirements for linked-list sparse sets are O(E), with E->N
   in the worst case (a sparse set with large distances between the values
   of the set members).

   The linked-list set representation works well for problems involving very
   sparse sets.  The canonical example in GCC is, of course, the "set of
   sets" for some CFG-based data flow problems (liveness analysis, dominance
   frontiers, etc.).
   
   This representation also works well for data flow problems where the size
   of the set may grow dynamically, but care must be taken that the member_p,
   add_member, and remove_member operations occur with a suitable access
   pattern.
   
   For random-access sets with a known, relatively small universe size, the
   SparseSet or simple bitmap representations may be more efficient than a
   linked-list set.  For random-access sets of unknown universe, a hash table
   or a balanced binary tree representation is likely to be a more suitable
   choice.

   Traversing linked lists is usually cache-unfriendly, even with the last
   accessed element cached.
   
   Cache performance can be improved by keeping the elements in the set
   grouped together in memory, using a dedicated obstack for a set (or group
   of related sets).  Elements allocated on obstacks are released to a
   free-list and taken off the free list.  If multiple sets are allocated on
   the same obstack, elements freed from one set may be re-used for one of
   the other sets.  This usually helps avoid cache misses.

   A single free-list is used for all sets allocated in GGC space.  This is
   bad for persistent sets, so persistent sets should be allocated on an
   obstack whenever possible.  */

#include "obstack.h"

/* Bitmap memory usage.  */
struct bitmap_usage: public mem_usage
{
  /* Default contructor.  */
  bitmap_usage (): m_nsearches (0), m_search_iter (0) {}
  /* Constructor.  */
  bitmap_usage (size_t allocated, size_t times, size_t peak,
	     uint64_t nsearches, uint64_t search_iter)
    : mem_usage (allocated, times, peak),
    m_nsearches (nsearches), m_search_iter (search_iter) {}

  /* Sum the usage with SECOND usage.  */
  bitmap_usage
  operator+ (const bitmap_usage &second)
  {
    return bitmap_usage (m_allocated + second.m_allocated,
			     m_times + second.m_times,
			     m_peak + second.m_peak,
			     m_nsearches + second.m_nsearches,
			     m_search_iter + second.m_search_iter);
  }

  /* Dump usage coupled to LOC location, where TOTAL is sum of all rows.  */
  inline void
  dump (mem_location *loc, mem_usage &total) const
  {
    char *location_string = loc->to_string ();

    fprintf (stderr, "%-48s %10" PRIu64 ":%5.1f%%"
	     "%10" PRIu64 "%10" PRIu64 ":%5.1f%%"
	     "%12" PRIu64 "%12" PRIu64 "%10s\n",
	     location_string, (uint64_t)m_allocated,
	     get_percent (m_allocated, total.m_allocated),
	     (uint64_t)m_peak, (uint64_t)m_times,
	     get_percent (m_times, total.m_times),
	     m_nsearches, m_search_iter,
	     loc->m_ggc ? "ggc" : "heap");

    free (location_string);
  }

  /* Dump header with NAME.  */
  static inline void
  dump_header (const char *name)
  {
    fprintf (stderr, "%-48s %11s%16s%17s%12s%12s%10s\n", name, "Leak", "Peak",
	     "Times", "N searches", "Search iter", "Type");
    print_dash_line ();
  }

  /* Number search operations.  */
  uint64_t m_nsearches;
  /* Number of search iterations.  */
  uint64_t m_search_iter;
};

/* Bitmap memory description.  */
extern mem_alloc_description<bitmap_usage> bitmap_mem_desc;

/* Fundamental storage type for bitmap.  */

typedef unsigned long BITMAP_WORD;
/* BITMAP_WORD_BITS needs to be unsigned, but cannot contain casts as
   it is used in preprocessor directives -- hence the 1u.  */
#define BITMAP_WORD_BITS (CHAR_BIT * SIZEOF_LONG * 1u)

/* Number of words to use for each element in the linked list.  */

#ifndef BITMAP_ELEMENT_WORDS
#define BITMAP_ELEMENT_WORDS ((128 + BITMAP_WORD_BITS - 1) / BITMAP_WORD_BITS)
#endif

/* Number of bits in each actual element of a bitmap.  */

#define BITMAP_ELEMENT_ALL_BITS (BITMAP_ELEMENT_WORDS * BITMAP_WORD_BITS)

/* Obstack for allocating bitmaps and elements from.  */
struct GTY (()) bitmap_obstack {
  struct bitmap_element *elements;
  struct bitmap_head *heads;
  struct obstack GTY ((skip)) obstack;
};

/* Bitmap set element.  We use a linked list to hold only the bits that
   are set.  This allows for use to grow the bitset dynamically without
   having to realloc and copy a giant bit array.

   The free list is implemented as a list of lists.  There is one
   outer list connected together by prev fields.  Each element of that
   outer is an inner list (that may consist only of the outer list
   element) that are connected by the next fields.  The prev pointer
   is undefined for interior elements.  This allows
   bitmap_elt_clear_from to be implemented in unit time rather than
   linear in the number of elements to be freed.  */

struct GTY((chain_next ("%h.next"), chain_prev ("%h.prev"))) bitmap_element {
  struct bitmap_element *next;	/* Next element.  */
  struct bitmap_element *prev;	/* Previous element.  */
  unsigned int indx;			/* regno/BITMAP_ELEMENT_ALL_BITS.  */
  BITMAP_WORD bits[BITMAP_ELEMENT_WORDS]; /* Bits that are set.  */
};

/* Head of bitmap linked list.  The 'current' member points to something
   already pointed to by the chain started by first, so GTY((skip)) it.  */

struct GTY(()) bitmap_head {
  unsigned int indx;			/* Index of last element looked at.  */
  unsigned int descriptor_id;		/* Unique identifier for the allocation
					   site of this bitmap, for detailed
					   statistics gathering.  */
  bitmap_element *first;		/* First element in linked list.  */
  bitmap_element * GTY((skip(""))) current; /* Last element looked at.  */
  bitmap_obstack *obstack;		/* Obstack to allocate elements from.
					   If NULL, then use GGC allocation.  */
};

/* Global data */
extern bitmap_element bitmap_zero_bits;	/* Zero bitmap element */
extern bitmap_obstack bitmap_default_obstack;   /* Default bitmap obstack */

/* Clear a bitmap by freeing up the linked list.  */
extern void bitmap_clear (bitmap);

/* Copy a bitmap to another bitmap.  */
extern void bitmap_copy (bitmap, const_bitmap);

/* Move a bitmap to another bitmap.  */
extern void bitmap_move (bitmap, bitmap);

/* True if two bitmaps are identical.  */
extern bool bitmap_equal_p (const_bitmap, const_bitmap);

/* True if the bitmaps intersect (their AND is non-empty).  */
extern bool bitmap_intersect_p (const_bitmap, const_bitmap);

/* True if the complement of the second intersects the first (their
   AND_COMPL is non-empty).  */
extern bool bitmap_intersect_compl_p (const_bitmap, const_bitmap);

/* True if MAP is an empty bitmap.  */
inline bool bitmap_empty_p (const_bitmap map)
{
  return !map->first;
}

/* True if the bitmap has only a single bit set.  */
extern bool bitmap_single_bit_set_p (const_bitmap);

/* Count the number of bits set in the bitmap.  */
extern unsigned long bitmap_count_bits (const_bitmap);

/* Count the number of unique bits set across the two bitmaps.  */
extern unsigned long bitmap_count_unique_bits (const_bitmap, const_bitmap);

/* Boolean operations on bitmaps.  The _into variants are two operand
   versions that modify the first source operand.  The other variants
   are three operand versions that to not destroy the source bitmaps.
   The operations supported are &, & ~, |, ^.  */
extern void bitmap_and (bitmap, const_bitmap, const_bitmap);
extern bool bitmap_and_into (bitmap, const_bitmap);
extern bool bitmap_and_compl (bitmap, const_bitmap, const_bitmap);
extern bool bitmap_and_compl_into (bitmap, const_bitmap);
#define bitmap_compl_and(DST, A, B) bitmap_and_compl (DST, B, A)
extern void bitmap_compl_and_into (bitmap, const_bitmap);
extern void bitmap_clear_range (bitmap, unsigned int, unsigned int);
extern void bitmap_set_range (bitmap, unsigned int, unsigned int);
extern bool bitmap_ior (bitmap, const_bitmap, const_bitmap);
extern bool bitmap_ior_into (bitmap, const_bitmap);
extern void bitmap_xor (bitmap, const_bitmap, const_bitmap);
extern void bitmap_xor_into (bitmap, const_bitmap);

/* DST = A | (B & C).  Return true if DST changes.  */
extern bool bitmap_ior_and_into (bitmap DST, const_bitmap B, const_bitmap C);
/* DST = A | (B & ~C).  Return true if DST changes.  */
extern bool bitmap_ior_and_compl (bitmap DST, const_bitmap A,
				  const_bitmap B, const_bitmap C);
/* A |= (B & ~C).  Return true if A changes.  */
extern bool bitmap_ior_and_compl_into (bitmap A,
				       const_bitmap B, const_bitmap C);

/* Clear a single bit in a bitmap.  Return true if the bit changed.  */
extern bool bitmap_clear_bit (bitmap, int);

/* Set a single bit in a bitmap.  Return true if the bit changed.  */
extern bool bitmap_set_bit (bitmap, int);

/* Return true if a register is set in a register set.  */
extern int bitmap_bit_p (bitmap, int);

/* Debug functions to print a bitmap linked list.  */
extern void debug_bitmap (const_bitmap);
extern void debug_bitmap_file (FILE *, const_bitmap);

/* Print a bitmap.  */
extern void bitmap_print (FILE *, const_bitmap, const char *, const char *);

/* Initialize and release a bitmap obstack.  */
extern void bitmap_obstack_initialize (bitmap_obstack *);
extern void bitmap_obstack_release (bitmap_obstack *);
extern void bitmap_register (bitmap MEM_STAT_DECL);
extern void dump_bitmap_statistics (void);

/* Initialize a bitmap header.  OBSTACK indicates the bitmap obstack
   to allocate from, NULL for GC'd bitmap.  */

static inline void
bitmap_initialize (bitmap head, bitmap_obstack *obstack CXX_MEM_STAT_INFO)
{
  head->first = head->current = NULL;
  head->obstack = obstack;
  if (GATHER_STATISTICS)
    bitmap_register (head PASS_MEM_STAT);
}

/* Allocate and free bitmaps from obstack, malloc and gc'd memory.  */
extern bitmap bitmap_alloc (bitmap_obstack *obstack CXX_MEM_STAT_INFO);
#define BITMAP_ALLOC bitmap_alloc
extern bitmap bitmap_gc_alloc (ALONE_CXX_MEM_STAT_INFO);
#define BITMAP_GGC_ALLOC bitmap_gc_alloc
extern void bitmap_obstack_free (bitmap);

/* A few compatibility/functions macros for compatibility with sbitmaps */
inline void dump_bitmap (FILE *file, const_bitmap map)
{
  bitmap_print (file, map, "", "\n");
}
extern void debug (const bitmap_head &ref);
extern void debug (const bitmap_head *ptr);

extern unsigned bitmap_first_set_bit (const_bitmap);
extern unsigned bitmap_last_set_bit (const_bitmap);

/* Compute bitmap hash (for purposes of hashing etc.)  */
extern hashval_t bitmap_hash (const_bitmap);

/* Do any cleanup needed on a bitmap when it is no longer used.  */
#define BITMAP_FREE(BITMAP) \
       ((void) (bitmap_obstack_free ((bitmap) BITMAP), (BITMAP) = (bitmap) NULL))

/* Iterator for bitmaps.  */

struct bitmap_iterator
{
  /* Pointer to the current bitmap element.  */
  bitmap_element *elt1;

  /* Pointer to 2nd bitmap element when two are involved.  */
  bitmap_element *elt2;

  /* Word within the current element.  */
  unsigned word_no;

  /* Contents of the actually processed word.  When finding next bit
     it is shifted right, so that the actual bit is always the least
     significant bit of ACTUAL.  */
  BITMAP_WORD bits;
};

/* Initialize a single bitmap iterator.  START_BIT is the first bit to
   iterate from.  */

static inline void
bmp_iter_set_init (bitmap_iterator *bi, const_bitmap map,
		   unsigned start_bit, unsigned *bit_no)
{
  bi->elt1 = map->first;
  bi->elt2 = NULL;

  /* Advance elt1 until it is not before the block containing start_bit.  */
  while (1)
    {
      if (!bi->elt1)
	{
	  bi->elt1 = &bitmap_zero_bits;
	  break;
	}

      if (bi->elt1->indx >= start_bit / BITMAP_ELEMENT_ALL_BITS)
	break;
      bi->elt1 = bi->elt1->next;
    }

  /* We might have gone past the start bit, so reinitialize it.  */
  if (bi->elt1->indx != start_bit / BITMAP_ELEMENT_ALL_BITS)
    start_bit = bi->elt1->indx * BITMAP_ELEMENT_ALL_BITS;

  /* Initialize for what is now start_bit.  */
  bi->word_no = start_bit / BITMAP_WORD_BITS % BITMAP_ELEMENT_WORDS;
  bi->bits = bi->elt1->bits[bi->word_no];
  bi->bits >>= start_bit % BITMAP_WORD_BITS;

  /* If this word is zero, we must make sure we're not pointing at the
     first bit, otherwise our incrementing to the next word boundary
     will fail.  It won't matter if this increment moves us into the
     next word.  */
  start_bit += !bi->bits;

  *bit_no = start_bit;
}

/* Initialize an iterator to iterate over the intersection of two
   bitmaps.  START_BIT is the bit to commence from.  */

static inline void
bmp_iter_and_init (bitmap_iterator *bi, const_bitmap map1, const_bitmap map2,
		   unsigned start_bit, unsigned *bit_no)
{
  bi->elt1 = map1->first;
  bi->elt2 = map2->first;

  /* Advance elt1 until it is not before the block containing
     start_bit.  */
  while (1)
    {
      if (!bi->elt1)
	{
	  bi->elt2 = NULL;
	  break;
	}

      if (bi->elt1->indx >= start_bit / BITMAP_ELEMENT_ALL_BITS)
	break;
      bi->elt1 = bi->elt1->next;
    }

  /* Advance elt2 until it is not before elt1.  */
  while (1)
    {
      if (!bi->elt2)
	{
	  bi->elt1 = bi->elt2 = &bitmap_zero_bits;
	  break;
	}

      if (bi->elt2->indx >= bi->elt1->indx)
	break;
      bi->elt2 = bi->elt2->next;
    }

  /* If we're at the same index, then we have some intersecting bits.  */
  if (bi->elt1->indx == bi->elt2->indx)
    {
      /* We might have advanced beyond the start_bit, so reinitialize
	 for that.  */
      if (bi->elt1->indx != start_bit / BITMAP_ELEMENT_ALL_BITS)
	start_bit = bi->elt1->indx * BITMAP_ELEMENT_ALL_BITS;

      bi->word_no = start_bit / BITMAP_WORD_BITS % BITMAP_ELEMENT_WORDS;
      bi->bits = bi->elt1->bits[bi->word_no] & bi->elt2->bits[bi->word_no];
      bi->bits >>= start_bit % BITMAP_WORD_BITS;
    }
  else
    {
      /* Otherwise we must immediately advance elt1, so initialize for
	 that.  */
      bi->word_no = BITMAP_ELEMENT_WORDS - 1;
      bi->bits = 0;
    }

  /* If this word is zero, we must make sure we're not pointing at the
     first bit, otherwise our incrementing to the next word boundary
     will fail.  It won't matter if this increment moves us into the
     next word.  */
  start_bit += !bi->bits;

  *bit_no = start_bit;
}

/* Initialize an iterator to iterate over the bits in MAP1 & ~MAP2.
   */

static inline void
bmp_iter_and_compl_init (bitmap_iterator *bi,
			 const_bitmap map1, const_bitmap map2,
			 unsigned start_bit, unsigned *bit_no)
{
  bi->elt1 = map1->first;
  bi->elt2 = map2->first;

  /* Advance elt1 until it is not before the block containing start_bit.  */
  while (1)
    {
      if (!bi->elt1)
	{
	  bi->elt1 = &bitmap_zero_bits;
	  break;
	}

      if (bi->elt1->indx >= start_bit / BITMAP_ELEMENT_ALL_BITS)
	break;
      bi->elt1 = bi->elt1->next;
    }

  /* Advance elt2 until it is not before elt1.  */
  while (bi->elt2 && bi->elt2->indx < bi->elt1->indx)
    bi->elt2 = bi->elt2->next;

  /* We might have advanced beyond the start_bit, so reinitialize for
     that.  */
  if (bi->elt1->indx != start_bit / BITMAP_ELEMENT_ALL_BITS)
    start_bit = bi->elt1->indx * BITMAP_ELEMENT_ALL_BITS;

  bi->word_no = start_bit / BITMAP_WORD_BITS % BITMAP_ELEMENT_WORDS;
  bi->bits = bi->elt1->bits[bi->word_no];
  if (bi->elt2 && bi->elt1->indx == bi->elt2->indx)
    bi->bits &= ~bi->elt2->bits[bi->word_no];
  bi->bits >>= start_bit % BITMAP_WORD_BITS;

  /* If this word is zero, we must make sure we're not pointing at the
     first bit, otherwise our incrementing to the next word boundary
     will fail.  It won't matter if this increment moves us into the
     next word.  */
  start_bit += !bi->bits;

  *bit_no = start_bit;
}

/* Advance to the next bit in BI.  We don't advance to the next
   nonzero bit yet.  */

static inline void
bmp_iter_next (bitmap_iterator *bi, unsigned *bit_no)
{
  bi->bits >>= 1;
  *bit_no += 1;
}

/* Advance to first set bit in BI.  */

static inline void
bmp_iter_next_bit (bitmap_iterator * bi, unsigned *bit_no)
{
#if (GCC_VERSION >= 3004)
  {
    unsigned int n = __builtin_ctzl (bi->bits);
    gcc_assert (sizeof (unsigned long) == sizeof (BITMAP_WORD));
    bi->bits >>= n;
    *bit_no += n;
  }
#else
  while (!(bi->bits & 1))
    {
      bi->bits >>= 1;
      *bit_no += 1;
    }
#endif
}

/* Advance to the next nonzero bit of a single bitmap, we will have
   already advanced past the just iterated bit.  Return true if there
   is a bit to iterate.  */

static inline bool
bmp_iter_set (bitmap_iterator *bi, unsigned *bit_no)
{
  /* If our current word is nonzero, it contains the bit we want.  */
  if (bi->bits)
    {
    next_bit:
      bmp_iter_next_bit (bi, bit_no);
      return true;
    }

  /* Round up to the word boundary.  We might have just iterated past
     the end of the last word, hence the -1.  It is not possible for
     bit_no to point at the beginning of the now last word.  */
  *bit_no = ((*bit_no + BITMAP_WORD_BITS - 1)
	     / BITMAP_WORD_BITS * BITMAP_WORD_BITS);
  bi->word_no++;

  while (1)
    {
      /* Find the next nonzero word in this elt.  */
      while (bi->word_no != BITMAP_ELEMENT_WORDS)
	{
	  bi->bits = bi->elt1->bits[bi->word_no];
	  if (bi->bits)
	    goto next_bit;
	  *bit_no += BITMAP_WORD_BITS;
	  bi->word_no++;
	}

      /* Make sure we didn't remove the element while iterating.  */
      gcc_checking_assert (bi->elt1->indx != -1U);

      /* Advance to the next element.  */
      bi->elt1 = bi->elt1->next;
      if (!bi->elt1)
	return false;
      *bit_no = bi->elt1->indx * BITMAP_ELEMENT_ALL_BITS;
      bi->word_no = 0;
    }
}

/* Advance to the next nonzero bit of an intersecting pair of
   bitmaps.  We will have already advanced past the just iterated bit.
   Return true if there is a bit to iterate.  */

static inline bool
bmp_iter_and (bitmap_iterator *bi, unsigned *bit_no)
{
  /* If our current word is nonzero, it contains the bit we want.  */
  if (bi->bits)
    {
    next_bit:
      bmp_iter_next_bit (bi, bit_no);
      return true;
    }

  /* Round up to the word boundary.  We might have just iterated past
     the end of the last word, hence the -1.  It is not possible for
     bit_no to point at the beginning of the now last word.  */
  *bit_no = ((*bit_no + BITMAP_WORD_BITS - 1)
	     / BITMAP_WORD_BITS * BITMAP_WORD_BITS);
  bi->word_no++;

  while (1)
    {
      /* Find the next nonzero word in this elt.  */
      while (bi->word_no != BITMAP_ELEMENT_WORDS)
	{
	  bi->bits = bi->elt1->bits[bi->word_no] & bi->elt2->bits[bi->word_no];
	  if (bi->bits)
	    goto next_bit;
	  *bit_no += BITMAP_WORD_BITS;
	  bi->word_no++;
	}

      /* Advance to the next identical element.  */
      do
	{
	  /* Make sure we didn't remove the element while iterating.  */
	  gcc_checking_assert (bi->elt1->indx != -1U);

	  /* Advance elt1 while it is less than elt2.  We always want
	     to advance one elt.  */
	  do
	    {
	      bi->elt1 = bi->elt1->next;
	      if (!bi->elt1)
		return false;
	    }
	  while (bi->elt1->indx < bi->elt2->indx);

	  /* Make sure we didn't remove the element while iterating.  */
	  gcc_checking_assert (bi->elt2->indx != -1U);

	  /* Advance elt2 to be no less than elt1.  This might not
	     advance.  */
	  while (bi->elt2->indx < bi->elt1->indx)
	    {
	      bi->elt2 = bi->elt2->next;
	      if (!bi->elt2)
		return false;
	    }
	}
      while (bi->elt1->indx != bi->elt2->indx);

      *bit_no = bi->elt1->indx * BITMAP_ELEMENT_ALL_BITS;
      bi->word_no = 0;
    }
}

/* Advance to the next nonzero bit in the intersection of
   complemented bitmaps.  We will have already advanced past the just
   iterated bit.  */

static inline bool
bmp_iter_and_compl (bitmap_iterator *bi, unsigned *bit_no)
{
  /* If our current word is nonzero, it contains the bit we want.  */
  if (bi->bits)
    {
    next_bit:
      bmp_iter_next_bit (bi, bit_no);
      return true;
    }

  /* Round up to the word boundary.  We might have just iterated past
     the end of the last word, hence the -1.  It is not possible for
     bit_no to point at the beginning of the now last word.  */
  *bit_no = ((*bit_no + BITMAP_WORD_BITS - 1)
	     / BITMAP_WORD_BITS * BITMAP_WORD_BITS);
  bi->word_no++;

  while (1)
    {
      /* Find the next nonzero word in this elt.  */
      while (bi->word_no != BITMAP_ELEMENT_WORDS)
	{
	  bi->bits = bi->elt1->bits[bi->word_no];
	  if (bi->elt2 && bi->elt2->indx == bi->elt1->indx)
	    bi->bits &= ~bi->elt2->bits[bi->word_no];
	  if (bi->bits)
	    goto next_bit;
	  *bit_no += BITMAP_WORD_BITS;
	  bi->word_no++;
	}

      /* Make sure we didn't remove the element while iterating.  */
      gcc_checking_assert (bi->elt1->indx != -1U);

      /* Advance to the next element of elt1.  */
      bi->elt1 = bi->elt1->next;
      if (!bi->elt1)
	return false;

      /* Make sure we didn't remove the element while iterating.  */
      gcc_checking_assert (! bi->elt2 || bi->elt2->indx != -1U);

      /* Advance elt2 until it is no less than elt1.  */
      while (bi->elt2 && bi->elt2->indx < bi->elt1->indx)
	bi->elt2 = bi->elt2->next;

      *bit_no = bi->elt1->indx * BITMAP_ELEMENT_ALL_BITS;
      bi->word_no = 0;
    }
}

/* If you are modifying a bitmap you are currently iterating over you
   have to ensure to
     - never remove the current bit;
     - if you set or clear a bit before the current bit this operation
       will not affect the set of bits you are visiting during the iteration;
     - if you set or clear a bit after the current bit it is unspecified
       whether that affects the set of bits you are visiting during the
       iteration.
   If you want to remove the current bit you can delay this to the next
   iteration (and after the iteration in case the last iteration is
   affected).  */

/* Loop over all bits set in BITMAP, starting with MIN and setting
   BITNUM to the bit number.  ITER is a bitmap iterator.  BITNUM
   should be treated as a read-only variable as it contains loop
   state.  */

#ifndef EXECUTE_IF_SET_IN_BITMAP
/* See sbitmap.h for the other definition of EXECUTE_IF_SET_IN_BITMAP.  */
#define EXECUTE_IF_SET_IN_BITMAP(BITMAP, MIN, BITNUM, ITER)		\
  for (bmp_iter_set_init (&(ITER), (BITMAP), (MIN), &(BITNUM));		\
       bmp_iter_set (&(ITER), &(BITNUM));				\
       bmp_iter_next (&(ITER), &(BITNUM)))
#endif

/* Loop over all the bits set in BITMAP1 & BITMAP2, starting with MIN
   and setting BITNUM to the bit number.  ITER is a bitmap iterator.
   BITNUM should be treated as a read-only variable as it contains
   loop state.  */

#define EXECUTE_IF_AND_IN_BITMAP(BITMAP1, BITMAP2, MIN, BITNUM, ITER)	\
  for (bmp_iter_and_init (&(ITER), (BITMAP1), (BITMAP2), (MIN),		\
			  &(BITNUM));					\
       bmp_iter_and (&(ITER), &(BITNUM));				\
       bmp_iter_next (&(ITER), &(BITNUM)))

/* Loop over all the bits set in BITMAP1 & ~BITMAP2, starting with MIN
   and setting BITNUM to the bit number.  ITER is a bitmap iterator.
   BITNUM should be treated as a read-only variable as it contains
   loop state.  */

#define EXECUTE_IF_AND_COMPL_IN_BITMAP(BITMAP1, BITMAP2, MIN, BITNUM, ITER) \
  for (bmp_iter_and_compl_init (&(ITER), (BITMAP1), (BITMAP2), (MIN),	\
				&(BITNUM));				\
       bmp_iter_and_compl (&(ITER), &(BITNUM));				\
       bmp_iter_next (&(ITER), &(BITNUM)))

/* A class that ties the lifetime of a bitmap to its scope.  */
class auto_bitmap
{
 public:
  auto_bitmap () { bitmap_initialize (&m_bits, &bitmap_default_obstack); }
  explicit auto_bitmap (bitmap_obstack *o) { bitmap_initialize (&m_bits, o); }
  ~auto_bitmap () { bitmap_clear (&m_bits); }
  // Allow calling bitmap functions on our bitmap.
  operator bitmap () { return &m_bits; }

 private:
  // Prevent making a copy that references our bitmap.
  auto_bitmap (const auto_bitmap &);
  auto_bitmap &operator = (const auto_bitmap &);
#if __cplusplus >= 201103L
  auto_bitmap (auto_bitmap &&);
  auto_bitmap &operator = (auto_bitmap &&);
#endif

  bitmap_head m_bits;
};

#endif /* GCC_BITMAP_H */
