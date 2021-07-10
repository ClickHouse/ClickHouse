/* A type-safe hash table template.
   Copyright (C) 2012-2018 Free Software Foundation, Inc.
   Contributed by Lawrence Crowl <crowl@google.com>

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


/* This file implements a typed hash table.
   The implementation borrows from libiberty's htab_t in hashtab.h.


   INTRODUCTION TO TYPES

   Users of the hash table generally need to be aware of three types.

      1. The type being placed into the hash table.  This type is called
      the value type.

      2. The type used to describe how to handle the value type within
      the hash table.  This descriptor type provides the hash table with
      several things.

         - A typedef named 'value_type' to the value type (from above).

         - A static member function named 'hash' that takes a value_type
         (or 'const value_type &') and returns a hashval_t value.

         - A typedef named 'compare_type' that is used to test when a value
         is found.  This type is the comparison type.  Usually, it will be the
         same as value_type.  If it is not the same type, you must generally
         explicitly compute hash values and pass them to the hash table.

         - A static member function named 'equal' that takes a value_type
         and a compare_type, and returns a bool.  Both arguments can be
         const references.

         - A static function named 'remove' that takes an value_type pointer
         and frees the memory allocated by it.  This function is used when
         individual elements of the table need to be disposed of (e.g.,
         when deleting a hash table, removing elements from the table, etc).

	 - An optional static function named 'keep_cache_entry'.  This
	 function is provided only for garbage-collected elements that
	 are not marked by the normal gc mark pass.  It describes what
	 what should happen to the element at the end of the gc mark phase.
	 The return value should be:
	   - 0 if the element should be deleted
	   - 1 if the element should be kept and needs to be marked
	   - -1 if the element should be kept and is already marked.
	 Returning -1 rather than 1 is purely an optimization.

      3. The type of the hash table itself.  (More later.)

   In very special circumstances, users may need to know about a fourth type.

      4. The template type used to describe how hash table memory
      is allocated.  This type is called the allocator type.  It is
      parameterized on the value type.  It provides two functions:

         - A static member function named 'data_alloc'.  This function
         allocates the data elements in the table.

         - A static member function named 'data_free'.  This function
         deallocates the data elements in the table.

   Hash table are instantiated with two type arguments.

      * The descriptor type, (2) above.

      * The allocator type, (4) above.  In general, you will not need to
      provide your own allocator type.  By default, hash tables will use
      the class template xcallocator, which uses malloc/free for allocation.


   DEFINING A DESCRIPTOR TYPE

   The first task in using the hash table is to describe the element type.
   We compose this into a few steps.

      1. Decide on a removal policy for values stored in the table.
         hash-traits.h provides class templates for the four most common
         policies:

         * typed_free_remove implements the static 'remove' member function
         by calling free().

         * typed_noop_remove implements the static 'remove' member function
         by doing nothing.

         * ggc_remove implements the static 'remove' member by doing nothing,
         but instead provides routines for gc marking and for PCH streaming.
         Use this for garbage-collected data that needs to be preserved across
         collections.

         * ggc_cache_remove is like ggc_remove, except that it does not
         mark the entries during the normal gc mark phase.  Instead it
         uses 'keep_cache_entry' (described above) to keep elements that
         were not collected and delete those that were.  Use this for
         garbage-collected caches that should not in themselves stop
         the data from being collected.

         You can use these policies by simply deriving the descriptor type
         from one of those class template, with the appropriate argument.

         Otherwise, you need to write the static 'remove' member function
         in the descriptor class.

      2. Choose a hash function.  Write the static 'hash' member function.

      3. Decide whether the lookup function should take as input an object
	 of type value_type or something more restricted.  Define compare_type
	 accordingly.

      4. Choose an equality testing function 'equal' that compares a value_type
	 and a compare_type.

   If your elements are pointers, it is usually easiest to start with one
   of the generic pointer descriptors described below and override the bits
   you need to change.

   AN EXAMPLE DESCRIPTOR TYPE

   Suppose you want to put some_type into the hash table.  You could define
   the descriptor type as follows.

      struct some_type_hasher : nofree_ptr_hash <some_type>
      // Deriving from nofree_ptr_hash means that we get a 'remove' that does
      // nothing.  This choice is good for raw values.
      {
        static inline hashval_t hash (const value_type *);
        static inline bool equal (const value_type *, const compare_type *);
      };

      inline hashval_t
      some_type_hasher::hash (const value_type *e)
      { ... compute and return a hash value for E ... }

      inline bool
      some_type_hasher::equal (const value_type *p1, const compare_type *p2)
      { ... compare P1 vs P2.  Return true if they are the 'same' ... }


   AN EXAMPLE HASH_TABLE DECLARATION

   To instantiate a hash table for some_type:

      hash_table <some_type_hasher> some_type_hash_table;

   There is no need to mention some_type directly, as the hash table will
   obtain it using some_type_hasher::value_type.

   You can then use any of the functions in hash_table's public interface.
   See hash_table for details.  The interface is very similar to libiberty's
   htab_t.


   EASY DESCRIPTORS FOR POINTERS

   There are four descriptors for pointer elements, one for each of
   the removal policies above:

   * nofree_ptr_hash (based on typed_noop_remove)
   * free_ptr_hash (based on typed_free_remove)
   * ggc_ptr_hash (based on ggc_remove)
   * ggc_cache_ptr_hash (based on ggc_cache_remove)

   These descriptors hash and compare elements by their pointer value,
   rather than what they point to.  So, to instantiate a hash table over
   pointers to whatever_type, without freeing the whatever_types, use:

      hash_table <nofree_ptr_hash <whatever_type> > whatever_type_hash_table;


   HASH TABLE ITERATORS

   The hash table provides standard C++ iterators.  For example, consider a
   hash table of some_info.  We wish to consume each element of the table:

      extern void consume (some_info *);

   We define a convenience typedef and the hash table:

      typedef hash_table <some_info_hasher> info_table_type;
      info_table_type info_table;

   Then we write the loop in typical C++ style:

      for (info_table_type::iterator iter = info_table.begin ();
           iter != info_table.end ();
           ++iter)
        if ((*iter).status == INFO_READY)
          consume (&*iter);

   Or with common sub-expression elimination:

      for (info_table_type::iterator iter = info_table.begin ();
           iter != info_table.end ();
           ++iter)
        {
          some_info &elem = *iter;
          if (elem.status == INFO_READY)
            consume (&elem);
        }

   One can also use a more typical GCC style:

      typedef some_info *some_info_p;
      some_info *elem_ptr;
      info_table_type::iterator iter;
      FOR_EACH_HASH_TABLE_ELEMENT (info_table, elem_ptr, some_info_p, iter)
        if (elem_ptr->status == INFO_READY)
          consume (elem_ptr);

*/


#ifndef TYPED_HASHTAB_H
#define TYPED_HASHTAB_H

#include "statistics.h"
#include "ggc.h"
#include "vec.h"
#include "hashtab.h"
#include "inchash.h"
#include "mem-stats-traits.h"
#include "hash-traits.h"
#include "hash-map-traits.h"

template<typename, typename, typename> class hash_map;
template<typename, typename> class hash_set;

/* The ordinary memory allocator.  */
/* FIXME (crowl): This allocator may be extracted for wider sharing later.  */

template <typename Type>
struct xcallocator
{
  static Type *data_alloc (size_t count);
  static void data_free (Type *memory);
};


/* Allocate memory for COUNT data blocks.  */

template <typename Type>
inline Type *
xcallocator <Type>::data_alloc (size_t count)
{
  return static_cast <Type *> (xcalloc (count, sizeof (Type)));
}


/* Free memory for data blocks.  */

template <typename Type>
inline void
xcallocator <Type>::data_free (Type *memory)
{
  return ::free (memory);
}


/* Table of primes and their inversion information.  */

struct prime_ent
{
  hashval_t prime;
  hashval_t inv;
  hashval_t inv_m2;     /* inverse of prime-2 */
  hashval_t shift;
};

extern struct prime_ent const prime_tab[];


/* Functions for computing hash table indexes.  */

extern unsigned int hash_table_higher_prime_index (unsigned long n)
   ATTRIBUTE_PURE;

/* Return X % Y using multiplicative inverse values INV and SHIFT.

   The multiplicative inverses computed above are for 32-bit types,
   and requires that we be able to compute a highpart multiply.

   FIX: I am not at all convinced that
     3 loads, 2 multiplications, 3 shifts, and 3 additions
   will be faster than
     1 load and 1 modulus
   on modern systems running a compiler.  */

inline hashval_t
mul_mod (hashval_t x, hashval_t y, hashval_t inv, int shift)
{
   hashval_t t1, t2, t3, t4, q, r;

   t1 = ((uint64_t)x * inv) >> 32;
   t2 = x - t1;
   t3 = t2 >> 1;
   t4 = t1 + t3;
   q  = t4 >> shift;
   r  = x - (q * y);

   return r;
}

/* Compute the primary table index for HASH given current prime index.  */

inline hashval_t
hash_table_mod1 (hashval_t hash, unsigned int index)
{
  const struct prime_ent *p = &prime_tab[index];
  gcc_checking_assert (sizeof (hashval_t) * CHAR_BIT <= 32);
  return mul_mod (hash, p->prime, p->inv, p->shift);
}

/* Compute the secondary table index for HASH given current prime index.  */

inline hashval_t
hash_table_mod2 (hashval_t hash, unsigned int index)
{
  const struct prime_ent *p = &prime_tab[index];
  gcc_checking_assert (sizeof (hashval_t) * CHAR_BIT <= 32);
  return 1 + mul_mod (hash, p->prime - 2, p->inv_m2, p->shift);
}

class mem_usage;

/* User-facing hash table type.

   The table stores elements of type Descriptor::value_type and uses
   the static descriptor functions described at the top of the file
   to hash, compare and remove elements.

   Specify the template Allocator to allocate and free memory.
     The default is xcallocator.

     Storage is an implementation detail and should not be used outside the
     hash table code.

*/
template <typename Descriptor,
	 template<typename Type> class Allocator = xcallocator>
class hash_table
{
  typedef typename Descriptor::value_type value_type;
  typedef typename Descriptor::compare_type compare_type;

public:
  explicit hash_table (size_t, bool ggc = false,
		       bool gather_mem_stats = GATHER_STATISTICS,
		       mem_alloc_origin origin = HASH_TABLE_ORIGIN
		       CXX_MEM_STAT_INFO);
  explicit hash_table (const hash_table &, bool ggc = false,
		       bool gather_mem_stats = GATHER_STATISTICS,
		       mem_alloc_origin origin = HASH_TABLE_ORIGIN
		       CXX_MEM_STAT_INFO);
  ~hash_table ();

  /* Create a hash_table in gc memory.  */
  static hash_table *
  create_ggc (size_t n CXX_MEM_STAT_INFO)
  {
    hash_table *table = ggc_alloc<hash_table> ();
    new (table) hash_table (n, true, GATHER_STATISTICS,
			    HASH_TABLE_ORIGIN PASS_MEM_STAT);
    return table;
  }

  /* Current size (in entries) of the hash table.  */
  size_t size () const { return m_size; }

  /* Return the current number of elements in this hash table. */
  size_t elements () const { return m_n_elements - m_n_deleted; }

  /* Return the current number of elements in this hash table. */
  size_t elements_with_deleted () const { return m_n_elements; }

  /* This function clears all entries in this hash table.  */
  void empty () { if (elements ()) empty_slow (); }

  /* This function clears a specified SLOT in a hash table.  It is
     useful when you've already done the lookup and don't want to do it
     again. */
  void clear_slot (value_type *);

  /* This function searches for a hash table entry equal to the given
     COMPARABLE element starting with the given HASH value.  It cannot
     be used to insert or delete an element. */
  value_type &find_with_hash (const compare_type &, hashval_t);

  /* Like find_slot_with_hash, but compute the hash value from the element.  */
  value_type &find (const value_type &value)
    {
      return find_with_hash (value, Descriptor::hash (value));
    }

  value_type *find_slot (const value_type &value, insert_option insert)
    {
      return find_slot_with_hash (value, Descriptor::hash (value), insert);
    }

  /* This function searches for a hash table slot containing an entry
     equal to the given COMPARABLE element and starting with the given
     HASH.  To delete an entry, call this with insert=NO_INSERT, then
     call clear_slot on the slot returned (possibly after doing some
     checks).  To insert an entry, call this with insert=INSERT, then
     write the value you want into the returned slot.  When inserting an
     entry, NULL may be returned if memory allocation fails. */
  value_type *find_slot_with_hash (const compare_type &comparable,
				    hashval_t hash, enum insert_option insert);

  /* This function deletes an element with the given COMPARABLE value
     from hash table starting with the given HASH.  If there is no
     matching element in the hash table, this function does nothing. */
  void remove_elt_with_hash (const compare_type &, hashval_t);

  /* Like remove_elt_with_hash, but compute the hash value from the
     element.  */
  void remove_elt (const value_type &value)
    {
      remove_elt_with_hash (value, Descriptor::hash (value));
    }

  /* This function scans over the entire hash table calling CALLBACK for
     each live entry.  If CALLBACK returns false, the iteration stops.
     ARGUMENT is passed as CALLBACK's second argument. */
  template <typename Argument,
	    int (*Callback) (value_type *slot, Argument argument)>
  void traverse_noresize (Argument argument);

  /* Like traverse_noresize, but does resize the table when it is too empty
     to improve effectivity of subsequent calls.  */
  template <typename Argument,
	    int (*Callback) (value_type *slot, Argument argument)>
  void traverse (Argument argument);

  class iterator
  {
  public:
    iterator () : m_slot (NULL), m_limit (NULL) {}

    iterator (value_type *slot, value_type *limit) :
      m_slot (slot), m_limit (limit) {}

    inline value_type &operator * () { return *m_slot; }
    void slide ();
    inline iterator &operator ++ ();
    bool operator != (const iterator &other) const
      {
	return m_slot != other.m_slot || m_limit != other.m_limit;
      }

  private:
    value_type *m_slot;
    value_type *m_limit;
  };

  iterator begin () const
    {
      iterator iter (m_entries, m_entries + m_size);
      iter.slide ();
      return iter;
    }

  iterator end () const { return iterator (); }

  double collisions () const
    {
      return m_searches ? static_cast <double> (m_collisions) / m_searches : 0;
    }

private:
  template<typename T> friend void gt_ggc_mx (hash_table<T> *);
  template<typename T> friend void gt_pch_nx (hash_table<T> *);
  template<typename T> friend void
    hashtab_entry_note_pointers (void *, void *, gt_pointer_operator, void *);
  template<typename T, typename U, typename V> friend void
  gt_pch_nx (hash_map<T, U, V> *, gt_pointer_operator, void *);
  template<typename T, typename U> friend void gt_pch_nx (hash_set<T, U> *,
							  gt_pointer_operator,
							  void *);
  template<typename T> friend void gt_pch_nx (hash_table<T> *,
					      gt_pointer_operator, void *);

  template<typename T> friend void gt_cleare_cache (hash_table<T> *);

  void empty_slow ();

  value_type *alloc_entries (size_t n CXX_MEM_STAT_INFO) const;
  value_type *find_empty_slot_for_expand (hashval_t);
  bool too_empty_p (unsigned int);
  void expand ();
  static bool is_deleted (value_type &v)
  {
    return Descriptor::is_deleted (v);
  }

  static bool is_empty (value_type &v)
  {
    return Descriptor::is_empty (v);
  }

  static void mark_deleted (value_type &v)
  {
    Descriptor::mark_deleted (v);
  }

  static void mark_empty (value_type &v)
  {
    Descriptor::mark_empty (v);
  }

  /* Table itself.  */
  typename Descriptor::value_type *m_entries;

  size_t m_size;

  /* Current number of elements including also deleted elements.  */
  size_t m_n_elements;

  /* Current number of deleted elements in the table.  */
  size_t m_n_deleted;

  /* The following member is used for debugging. Its value is number
     of all calls of `htab_find_slot' for the hash table. */
  unsigned int m_searches;

  /* The following member is used for debugging.  Its value is number
     of collisions fixed for time of work with the hash table. */
  unsigned int m_collisions;

  /* Current size (in entries) of the hash table, as an index into the
     table of primes.  */
  unsigned int m_size_prime_index;

  /* if m_entries is stored in ggc memory.  */
  bool m_ggc;

  /* If we should gather memory statistics for the table.  */
  bool m_gather_mem_stats;
};

/* As mem-stats.h heavily utilizes hash maps (hash tables), we have to include
   mem-stats.h after hash_table declaration.  */

#include "mem-stats.h"
#include "hash-map.h"

extern mem_alloc_description<mem_usage> hash_table_usage;

/* Support function for statistics.  */
extern void dump_hash_table_loc_statistics (void);

template<typename Descriptor, template<typename Type> class Allocator>
hash_table<Descriptor, Allocator>::hash_table (size_t size, bool ggc, bool
					       gather_mem_stats,
					       mem_alloc_origin origin
					       MEM_STAT_DECL) :
  m_n_elements (0), m_n_deleted (0), m_searches (0), m_collisions (0),
  m_ggc (ggc), m_gather_mem_stats (gather_mem_stats)
{
  unsigned int size_prime_index;

  size_prime_index = hash_table_higher_prime_index (size);
  size = prime_tab[size_prime_index].prime;

  if (m_gather_mem_stats)
    hash_table_usage.register_descriptor (this, origin, ggc
					  FINAL_PASS_MEM_STAT);

  m_entries = alloc_entries (size PASS_MEM_STAT);
  m_size = size;
  m_size_prime_index = size_prime_index;
}

template<typename Descriptor, template<typename Type> class Allocator>
hash_table<Descriptor, Allocator>::hash_table (const hash_table &h, bool ggc,
					       bool gather_mem_stats,
					       mem_alloc_origin origin
					       MEM_STAT_DECL) :
  m_n_elements (h.m_n_elements), m_n_deleted (h.m_n_deleted),
  m_searches (0), m_collisions (0), m_ggc (ggc),
  m_gather_mem_stats (gather_mem_stats)
{
  size_t size = h.m_size;

  if (m_gather_mem_stats)
    hash_table_usage.register_descriptor (this, origin, ggc
					  FINAL_PASS_MEM_STAT);

  value_type *nentries = alloc_entries (size PASS_MEM_STAT);
  for (size_t i = 0; i < size; ++i)
    {
      value_type &entry = h.m_entries[i];
      if (is_deleted (entry))
	mark_deleted (nentries[i]);
      else if (!is_empty (entry))
	nentries[i] = entry;
    }
  m_entries = nentries;
  m_size = size;
  m_size_prime_index = h.m_size_prime_index;
}

template<typename Descriptor, template<typename Type> class Allocator>
hash_table<Descriptor, Allocator>::~hash_table ()
{
  for (size_t i = m_size - 1; i < m_size; i--)
    if (!is_empty (m_entries[i]) && !is_deleted (m_entries[i]))
      Descriptor::remove (m_entries[i]);

  if (!m_ggc)
    Allocator <value_type> ::data_free (m_entries);
  else
    ggc_free (m_entries);

  if (m_gather_mem_stats)
    hash_table_usage.release_instance_overhead (this,
						sizeof (value_type) * m_size,
						true);
}

/* This function returns an array of empty hash table elements.  */

template<typename Descriptor, template<typename Type> class Allocator>
inline typename hash_table<Descriptor, Allocator>::value_type *
hash_table<Descriptor, Allocator>::alloc_entries (size_t n MEM_STAT_DECL) const
{
  value_type *nentries;

  if (m_gather_mem_stats)
    hash_table_usage.register_instance_overhead (sizeof (value_type) * n, this);

  if (!m_ggc)
    nentries = Allocator <value_type> ::data_alloc (n);
  else
    nentries = ::ggc_cleared_vec_alloc<value_type> (n PASS_MEM_STAT);

  gcc_assert (nentries != NULL);
  for (size_t i = 0; i < n; i++)
    mark_empty (nentries[i]);

  return nentries;
}

/* Similar to find_slot, but without several unwanted side effects:
    - Does not call equal when it finds an existing entry.
    - Does not change the count of elements/searches/collisions in the
      hash table.
   This function also assumes there are no deleted entries in the table.
   HASH is the hash value for the element to be inserted.  */

template<typename Descriptor, template<typename Type> class Allocator>
typename hash_table<Descriptor, Allocator>::value_type *
hash_table<Descriptor, Allocator>::find_empty_slot_for_expand (hashval_t hash)
{
  hashval_t index = hash_table_mod1 (hash, m_size_prime_index);
  size_t size = m_size;
  value_type *slot = m_entries + index;
  hashval_t hash2;

  if (is_empty (*slot))
    return slot;
  gcc_checking_assert (!is_deleted (*slot));

  hash2 = hash_table_mod2 (hash, m_size_prime_index);
  for (;;)
    {
      index += hash2;
      if (index >= size)
        index -= size;

      slot = m_entries + index;
      if (is_empty (*slot))
        return slot;
      gcc_checking_assert (!is_deleted (*slot));
    }
}

/* Return true if the current table is excessively big for ELTS elements.  */

template<typename Descriptor, template<typename Type> class Allocator>
inline bool
hash_table<Descriptor, Allocator>::too_empty_p (unsigned int elts)
{
  return elts * 8 < m_size && m_size > 32;
}

/* The following function changes size of memory allocated for the
   entries and repeatedly inserts the table elements.  The occupancy
   of the table after the call will be about 50%.  Naturally the hash
   table must already exist.  Remember also that the place of the
   table entries is changed.  If memory allocation fails, this function
   will abort.  */

template<typename Descriptor, template<typename Type> class Allocator>
void
hash_table<Descriptor, Allocator>::expand ()
{
  value_type *oentries = m_entries;
  unsigned int oindex = m_size_prime_index;
  size_t osize = size ();
  value_type *olimit = oentries + osize;
  size_t elts = elements ();

  /* Resize only when table after removal of unused elements is either
     too full or too empty.  */
  unsigned int nindex;
  size_t nsize;
  if (elts * 2 > osize || too_empty_p (elts))
    {
      nindex = hash_table_higher_prime_index (elts * 2);
      nsize = prime_tab[nindex].prime;
    }
  else
    {
      nindex = oindex;
      nsize = osize;
    }

  value_type *nentries = alloc_entries (nsize);

  if (m_gather_mem_stats)
    hash_table_usage.release_instance_overhead (this, sizeof (value_type)
						    * osize);

  m_entries = nentries;
  m_size = nsize;
  m_size_prime_index = nindex;
  m_n_elements -= m_n_deleted;
  m_n_deleted = 0;

  value_type *p = oentries;
  do
    {
      value_type &x = *p;

      if (!is_empty (x) && !is_deleted (x))
        {
          value_type *q = find_empty_slot_for_expand (Descriptor::hash (x));

          *q = x;
        }

      p++;
    }
  while (p < olimit);

  if (!m_ggc)
    Allocator <value_type> ::data_free (oentries);
  else
    ggc_free (oentries);
}

/* Implements empty() in cases where it isn't a no-op.  */

template<typename Descriptor, template<typename Type> class Allocator>
void
hash_table<Descriptor, Allocator>::empty_slow ()
{
  size_t size = m_size;
  size_t nsize = size;
  value_type *entries = m_entries;
  int i;

  for (i = size - 1; i >= 0; i--)
    if (!is_empty (entries[i]) && !is_deleted (entries[i]))
      Descriptor::remove (entries[i]);

  /* Instead of clearing megabyte, downsize the table.  */
  if (size > 1024*1024 / sizeof (value_type))
    nsize = 1024 / sizeof (value_type);
  else if (too_empty_p (m_n_elements))
    nsize = m_n_elements * 2;

  if (nsize != size)
    {
      int nindex = hash_table_higher_prime_index (nsize);
      int nsize = prime_tab[nindex].prime;

      if (!m_ggc)
	Allocator <value_type> ::data_free (m_entries);
      else
	ggc_free (m_entries);

      m_entries = alloc_entries (nsize);
      m_size = nsize;
      m_size_prime_index = nindex;
    }
  else
    {
#ifndef BROKEN_VALUE_INITIALIZATION
      for ( ; size; ++entries, --size)
	*entries = value_type ();
#else
      memset (entries, 0, size * sizeof (value_type));
#endif
    }
  m_n_deleted = 0;
  m_n_elements = 0;
}

/* This function clears a specified SLOT in a hash table.  It is
   useful when you've already done the lookup and don't want to do it
   again. */

template<typename Descriptor, template<typename Type> class Allocator>
void
hash_table<Descriptor, Allocator>::clear_slot (value_type *slot)
{
  gcc_checking_assert (!(slot < m_entries || slot >= m_entries + size ()
		         || is_empty (*slot) || is_deleted (*slot)));

  Descriptor::remove (*slot);

  mark_deleted (*slot);
  m_n_deleted++;
}

/* This function searches for a hash table entry equal to the given
   COMPARABLE element starting with the given HASH value.  It cannot
   be used to insert or delete an element. */

template<typename Descriptor, template<typename Type> class Allocator>
typename hash_table<Descriptor, Allocator>::value_type &
hash_table<Descriptor, Allocator>
::find_with_hash (const compare_type &comparable, hashval_t hash)
{
  m_searches++;
  size_t size = m_size;
  hashval_t index = hash_table_mod1 (hash, m_size_prime_index);

  value_type *entry = &m_entries[index];
  if (is_empty (*entry)
      || (!is_deleted (*entry) && Descriptor::equal (*entry, comparable)))
    return *entry;

  hashval_t hash2 = hash_table_mod2 (hash, m_size_prime_index);
  for (;;)
    {
      m_collisions++;
      index += hash2;
      if (index >= size)
        index -= size;

      entry = &m_entries[index];
      if (is_empty (*entry)
          || (!is_deleted (*entry) && Descriptor::equal (*entry, comparable)))
        return *entry;
    }
}

/* This function searches for a hash table slot containing an entry
   equal to the given COMPARABLE element and starting with the given
   HASH.  To delete an entry, call this with insert=NO_INSERT, then
   call clear_slot on the slot returned (possibly after doing some
   checks).  To insert an entry, call this with insert=INSERT, then
   write the value you want into the returned slot.  When inserting an
   entry, NULL may be returned if memory allocation fails. */

template<typename Descriptor, template<typename Type> class Allocator>
typename hash_table<Descriptor, Allocator>::value_type *
hash_table<Descriptor, Allocator>
::find_slot_with_hash (const compare_type &comparable, hashval_t hash,
		       enum insert_option insert)
{
  if (insert == INSERT && m_size * 3 <= m_n_elements * 4)
    expand ();

  m_searches++;

  value_type *first_deleted_slot = NULL;
  hashval_t index = hash_table_mod1 (hash, m_size_prime_index);
  hashval_t hash2 = hash_table_mod2 (hash, m_size_prime_index);
  value_type *entry = &m_entries[index];
  size_t size = m_size;
  if (is_empty (*entry))
    goto empty_entry;
  else if (is_deleted (*entry))
    first_deleted_slot = &m_entries[index];
  else if (Descriptor::equal (*entry, comparable))
    return &m_entries[index];

  for (;;)
    {
      m_collisions++;
      index += hash2;
      if (index >= size)
	index -= size;

      entry = &m_entries[index];
      if (is_empty (*entry))
	goto empty_entry;
      else if (is_deleted (*entry))
	{
	  if (!first_deleted_slot)
	    first_deleted_slot = &m_entries[index];
	}
      else if (Descriptor::equal (*entry, comparable))
	return &m_entries[index];
    }

 empty_entry:
  if (insert == NO_INSERT)
    return NULL;

  if (first_deleted_slot)
    {
      m_n_deleted--;
      mark_empty (*first_deleted_slot);
      return first_deleted_slot;
    }

  m_n_elements++;
  return &m_entries[index];
}

/* This function deletes an element with the given COMPARABLE value
   from hash table starting with the given HASH.  If there is no
   matching element in the hash table, this function does nothing. */

template<typename Descriptor, template<typename Type> class Allocator>
void
hash_table<Descriptor, Allocator>
::remove_elt_with_hash (const compare_type &comparable, hashval_t hash)
{
  value_type *slot = find_slot_with_hash (comparable, hash, NO_INSERT);
  if (is_empty (*slot))
    return;

  Descriptor::remove (*slot);

  mark_deleted (*slot);
  m_n_deleted++;
}

/* This function scans over the entire hash table calling CALLBACK for
   each live entry.  If CALLBACK returns false, the iteration stops.
   ARGUMENT is passed as CALLBACK's second argument. */

template<typename Descriptor,
	  template<typename Type> class Allocator>
template<typename Argument,
	  int (*Callback)
     (typename hash_table<Descriptor, Allocator>::value_type *slot,
      Argument argument)>
void
hash_table<Descriptor, Allocator>::traverse_noresize (Argument argument)
{
  value_type *slot = m_entries;
  value_type *limit = slot + size ();

  do
    {
      value_type &x = *slot;

      if (!is_empty (x) && !is_deleted (x))
        if (! Callback (slot, argument))
          break;
    }
  while (++slot < limit);
}

/* Like traverse_noresize, but does resize the table when it is too empty
   to improve effectivity of subsequent calls.  */

template <typename Descriptor,
	  template <typename Type> class Allocator>
template <typename Argument,
	  int (*Callback)
     (typename hash_table<Descriptor, Allocator>::value_type *slot,
      Argument argument)>
void
hash_table<Descriptor, Allocator>::traverse (Argument argument)
{
  if (too_empty_p (elements ()))
    expand ();

  traverse_noresize <Argument, Callback> (argument);
}

/* Slide down the iterator slots until an active entry is found.  */

template<typename Descriptor, template<typename Type> class Allocator>
void
hash_table<Descriptor, Allocator>::iterator::slide ()
{
  for ( ; m_slot < m_limit; ++m_slot )
    {
      value_type &x = *m_slot;
      if (!is_empty (x) && !is_deleted (x))
        return;
    }
  m_slot = NULL;
  m_limit = NULL;
}

/* Bump the iterator.  */

template<typename Descriptor, template<typename Type> class Allocator>
inline typename hash_table<Descriptor, Allocator>::iterator &
hash_table<Descriptor, Allocator>::iterator::operator ++ ()
{
  ++m_slot;
  slide ();
  return *this;
}


/* Iterate through the elements of hash_table HTAB,
   using hash_table <....>::iterator ITER,
   storing each element in RESULT, which is of type TYPE.  */

#define FOR_EACH_HASH_TABLE_ELEMENT(HTAB, RESULT, TYPE, ITER) \
  for ((ITER) = (HTAB).begin (); \
       (ITER) != (HTAB).end () ? (RESULT = *(ITER) , true) : false; \
       ++(ITER))

/* ggc walking routines.  */

template<typename E>
static inline void
gt_ggc_mx (hash_table<E> *h)
{
  typedef hash_table<E> table;

  if (!ggc_test_and_set_mark (h->m_entries))
    return;

  for (size_t i = 0; i < h->m_size; i++)
    {
      if (table::is_empty (h->m_entries[i])
	  || table::is_deleted (h->m_entries[i]))
	continue;

      /* Use ggc_maxbe_mx so we don't mark right away for cache tables; we'll
	 mark in gt_cleare_cache if appropriate.  */
      E::ggc_maybe_mx (h->m_entries[i]);
    }
}

template<typename D>
static inline void
hashtab_entry_note_pointers (void *obj, void *h, gt_pointer_operator op,
			     void *cookie)
{
  hash_table<D> *map = static_cast<hash_table<D> *> (h);
  gcc_checking_assert (map->m_entries == obj);
  for (size_t i = 0; i < map->m_size; i++)
    {
      typedef hash_table<D> table;
      if (table::is_empty (map->m_entries[i])
	  || table::is_deleted (map->m_entries[i]))
	continue;

      D::pch_nx (map->m_entries[i], op, cookie);
    }
}

template<typename D>
static void
gt_pch_nx (hash_table<D> *h)
{
  bool success
    = gt_pch_note_object (h->m_entries, h, hashtab_entry_note_pointers<D>);
  gcc_checking_assert (success);
  for (size_t i = 0; i < h->m_size; i++)
    {
      if (hash_table<D>::is_empty (h->m_entries[i])
	  || hash_table<D>::is_deleted (h->m_entries[i]))
	continue;

      D::pch_nx (h->m_entries[i]);
    }
}

template<typename D>
static inline void
gt_pch_nx (hash_table<D> *h, gt_pointer_operator op, void *cookie)
{
  op (&h->m_entries, cookie);
}

template<typename H>
inline void
gt_cleare_cache (hash_table<H> *h)
{
  typedef hash_table<H> table;
  if (!h)
    return;

  for (typename table::iterator iter = h->begin (); iter != h->end (); ++iter)
    if (!table::is_empty (*iter) && !table::is_deleted (*iter))
      {
	int res = H::keep_cache_entry (*iter);
	if (res == 0)
	  h->clear_slot (&*iter);
	else if (res != -1)
	  H::ggc_mx (*iter);
      }
}

#endif /* TYPED_HASHTAB_H */
