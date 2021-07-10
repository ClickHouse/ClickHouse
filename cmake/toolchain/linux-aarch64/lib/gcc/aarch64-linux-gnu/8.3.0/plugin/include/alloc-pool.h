/* Functions to support a pool of allocatable objects
   Copyright (C) 1997-2018 Free Software Foundation, Inc.
   Contributed by Daniel Berlin <dan@cgsoftware.com>

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
#ifndef ALLOC_POOL_H
#define ALLOC_POOL_H

#include "memory-block.h"
#include "options.h"	    // for flag_checking

extern void dump_alloc_pool_statistics (void);

/* Flag indicates whether memory statistics are gathered any longer.  */
extern bool after_memory_report;

typedef unsigned long ALLOC_POOL_ID_TYPE;

/* Last used ID.  */
extern ALLOC_POOL_ID_TYPE last_id;

/* Pool allocator memory usage.  */
struct pool_usage: public mem_usage
{
  /* Default contructor.  */
  pool_usage (): m_element_size (0), m_pool_name ("") {}
  /* Constructor.  */
  pool_usage (size_t allocated, size_t times, size_t peak,
	      size_t instances, size_t element_size,
	      const char *pool_name)
    : mem_usage (allocated, times, peak, instances),
      m_element_size (element_size),
      m_pool_name (pool_name) {}

  /* Sum the usage with SECOND usage.  */
  pool_usage
  operator+ (const pool_usage &second)
  {
    return pool_usage (m_allocated + second.m_allocated,
			     m_times + second.m_times,
			     m_peak + second.m_peak,
			     m_instances + second.m_instances,
			     m_element_size, m_pool_name);
  }

  /* Dump usage coupled to LOC location, where TOTAL is sum of all rows.  */
  inline void
  dump (mem_location *loc, mem_usage &total) const
  {
    char *location_string = loc->to_string ();

    fprintf (stderr, "%-32s%-48s %6li%10li:%5.1f%%%10li%10li:%5.1f%%%12li\n",
	     m_pool_name, location_string, (long)m_instances,
	     (long)m_allocated, get_percent (m_allocated, total.m_allocated),
	     (long)m_peak, (long)m_times,
	     get_percent (m_times, total.m_times),
	     (long)m_element_size);

    free (location_string);
  }

  /* Dump header with NAME.  */
  static inline void
  dump_header (const char *name)
  {
    fprintf (stderr, "%-32s%-48s %6s%11s%16s%17s%12s\n", "Pool name", name,
	     "Pools", "Leak", "Peak", "Times", "Elt size");
    print_dash_line ();
  }

  /* Dump footer.  */
  inline void
  dump_footer ()
  {
    print_dash_line ();
    fprintf (stderr, "%s%82li%10li\n", "Total", (long)m_instances,
	     (long)m_allocated);
    print_dash_line ();
  }

  /* Element size.  */
  size_t m_element_size;
  /* Pool name.  */
  const char *m_pool_name;
};

extern mem_alloc_description<pool_usage> pool_allocator_usage;

#if 0
/* If a pool with custom block size is needed, one might use the following
   template.  An instance of this template can be used as a parameter for
   instantiating base_pool_allocator template:

	typedef custom_block_allocator <128*1024> huge_block_allocator;
	...
	static base_pool_allocator <huge_block_allocator>
						value_pool ("value", 16384);

   Right now it's not used anywhere in the code, and is given here as an
   example).  */

template <size_t BlockSize>
class custom_block_allocator
{
public:
  static const size_t block_size = BlockSize;

  static inline void *
  allocate () ATTRIBUTE_MALLOC
  {
    return XNEWVEC (char, BlockSize);
  }

  static inline void
  release (void *block)
  {
    XDELETEVEC (block);
  }
};
#endif

/* Generic pool allocator.  */

template <typename TBlockAllocator>
class base_pool_allocator
{
public:
  /* Default constructor for pool allocator called NAME.  */
  base_pool_allocator (const char *name, size_t size CXX_MEM_STAT_INFO);
  ~base_pool_allocator ();
  void release ();
  void release_if_empty ();
  void *allocate () ATTRIBUTE_MALLOC;
  void remove (void *object);
  size_t num_elts_current ();

private:
  struct allocation_pool_list
  {
    allocation_pool_list *next;
  };

  /* Initialize a pool allocator.  */
  void initialize ();

  struct allocation_object
  {
#if CHECKING_P
    /* The ID of alloc pool which the object was allocated from.  */
    ALLOC_POOL_ID_TYPE id;
#endif

    union
      {
	/* The data of the object.  */
	char data[1];

	/* Because we want any type of data to be well aligned after the ID,
	   the following elements are here.  They are never accessed so
	   the allocated object may be even smaller than this structure.
	   We do not care about alignment for floating-point types.  */
	char *align_p;
	int64_t align_i;
      } u;

#if CHECKING_P
    static inline allocation_object*
    get_instance (void *data_ptr)
    {
      return (allocation_object *)(((char *)(data_ptr))
				      - offsetof (allocation_object,
						  u.data));
    }
#endif

    static inline void*
    get_data (void *instance_ptr)
    {
      return (void*)(((allocation_object *) instance_ptr)->u.data);
    }
  };

  /* Align X to 8.  */
  static inline size_t
  align_eight (size_t x)
  {
    return (((x+7) >> 3) << 3);
  }

  const char *m_name;
  ALLOC_POOL_ID_TYPE m_id;
  size_t m_elts_per_block;

  /* These are the elements that have been allocated at least once
     and freed.  */
  allocation_pool_list *m_returned_free_list;

  /* These are the elements that have not yet been allocated out of
     the last block obtained from XNEWVEC.  */
  char* m_virgin_free_list;

  /* The number of elements in the virgin_free_list that can be
     allocated before needing another block.  */
  size_t m_virgin_elts_remaining;
  /* The number of elements that are allocated.  */
  size_t m_elts_allocated;
  /* The number of elements that are released.  */
  size_t m_elts_free;
  /* The number of allocated blocks.  */
  size_t m_blocks_allocated;
  /* List of blocks that are used to allocate new objects.  */
  allocation_pool_list *m_block_list;
  /* Size of a pool elements in bytes.  */
  size_t m_elt_size;
  /* Size in bytes that should be allocated for each element.  */
  size_t m_size;
  /* Flag if a pool allocator is initialized.  */
  bool m_initialized;
  /* Memory allocation location.  */
  mem_location m_location;
};

template <typename TBlockAllocator>
inline
base_pool_allocator <TBlockAllocator>::base_pool_allocator (
				const char *name, size_t size MEM_STAT_DECL):
  m_name (name), m_id (0), m_elts_per_block (0), m_returned_free_list (NULL),
  m_virgin_free_list (NULL), m_virgin_elts_remaining (0), m_elts_allocated (0),
  m_elts_free (0), m_blocks_allocated (0), m_block_list (NULL), m_elt_size (0),
  m_size (size), m_initialized (false),
  m_location (ALLOC_POOL_ORIGIN, false PASS_MEM_STAT) {}

/* Initialize a pool allocator.  */

template <typename TBlockAllocator>
inline void
base_pool_allocator <TBlockAllocator>::initialize ()
{
  gcc_checking_assert (!m_initialized);
  m_initialized = true;

  size_t size = m_size;

  gcc_checking_assert (m_name);

  /* Make size large enough to store the list header.  */
  if (size < sizeof (allocation_pool_list*))
    size = sizeof (allocation_pool_list*);

  /* Now align the size to a multiple of 8.  */
  size = align_eight (size);

  /* Add the aligned size of ID.  */
  size += offsetof (allocation_object, u.data);

  m_elt_size = size;

  if (GATHER_STATISTICS)
    {
      pool_usage *u = pool_allocator_usage.register_descriptor
	(this, new mem_location (m_location));

      u->m_element_size = m_elt_size;
      u->m_pool_name = m_name;
    }

  /* List header size should be a multiple of 8.  */
  size_t header_size = align_eight (sizeof (allocation_pool_list));

  m_elts_per_block = (TBlockAllocator::block_size - header_size) / size;
  gcc_checking_assert (m_elts_per_block != 0);

  /* Increase the last used ID and use it for this pool.
     ID == 0 is used for free elements of pool so skip it.  */
  last_id++;
  if (last_id == 0)
    last_id++;

  m_id = last_id;
}

/* Free all memory allocated for the given memory pool.  */
template <typename TBlockAllocator>
inline void
base_pool_allocator <TBlockAllocator>::release ()
{
  if (!m_initialized)
    return;

  allocation_pool_list *block, *next_block;

  /* Free each block allocated to the pool.  */
  for (block = m_block_list; block != NULL; block = next_block)
    {
      next_block = block->next;
      TBlockAllocator::release (block);
    }

  if (GATHER_STATISTICS && !after_memory_report)
    {
      pool_allocator_usage.release_instance_overhead
	(this, (m_elts_allocated - m_elts_free) * m_elt_size);
    }

  m_returned_free_list = NULL;
  m_virgin_free_list = NULL;
  m_virgin_elts_remaining = 0;
  m_elts_allocated = 0;
  m_elts_free = 0;
  m_blocks_allocated = 0;
  m_block_list = NULL;
}

template <typename TBlockAllocator>
inline void
base_pool_allocator <TBlockAllocator>::release_if_empty ()
{
  if (m_elts_free == m_elts_allocated)
    release ();
}

template <typename TBlockAllocator>
inline base_pool_allocator <TBlockAllocator>::~base_pool_allocator ()
{
  release ();
}

/* Allocates one element from the pool specified.  */
template <typename TBlockAllocator>
inline void*
base_pool_allocator <TBlockAllocator>::allocate ()
{
  if (!m_initialized)
    initialize ();

  allocation_pool_list *header;
#ifdef ENABLE_VALGRIND_ANNOTATIONS
  int size;
#endif

  if (GATHER_STATISTICS)
    {
      pool_allocator_usage.register_instance_overhead (m_elt_size, this);
    }

#ifdef ENABLE_VALGRIND_ANNOTATIONS
  size = m_elt_size - offsetof (allocation_object, u.data);
#endif

  /* If there are no more free elements, make some more!.  */
  if (!m_returned_free_list)
    {
      char *block;
      if (!m_virgin_elts_remaining)
	{
	  allocation_pool_list *block_header;

	  /* Make the block.  */
	  block = reinterpret_cast<char *> (TBlockAllocator::allocate ());
	  block_header = new (block) allocation_pool_list;
	  block += align_eight (sizeof (allocation_pool_list));

	  /* Throw it on the block list.  */
	  block_header->next = m_block_list;
	  m_block_list = block_header;

	  /* Make the block available for allocation.  */
	  m_virgin_free_list = block;
	  m_virgin_elts_remaining = m_elts_per_block;

	  /* Also update the number of elements we have free/allocated, and
	     increment the allocated block count.  */
	  m_elts_allocated += m_elts_per_block;
	  m_elts_free += m_elts_per_block;
	  m_blocks_allocated += 1;
	}

      /* We now know that we can take the first elt off the virgin list and
	 put it on the returned list.  */
      block = m_virgin_free_list;
      header = (allocation_pool_list*) allocation_object::get_data (block);
      header->next = NULL;

      /* Mark the element to be free.  */
#if CHECKING_P
      ((allocation_object*) block)->id = 0;
#endif
      VALGRIND_DISCARD (VALGRIND_MAKE_MEM_NOACCESS (header,size));
      m_returned_free_list = header;
      m_virgin_free_list += m_elt_size;
      m_virgin_elts_remaining--;

    }

  /* Pull the first free element from the free list, and return it.  */
  header = m_returned_free_list;
  VALGRIND_DISCARD (VALGRIND_MAKE_MEM_DEFINED (header, sizeof (*header)));
  m_returned_free_list = header->next;
  m_elts_free--;

  /* Set the ID for element.  */
#if CHECKING_P
  allocation_object::get_instance (header)->id = m_id;
#endif
  VALGRIND_DISCARD (VALGRIND_MAKE_MEM_UNDEFINED (header, size));

  return (void *)(header);
}

/* Puts PTR back on POOL's free list.  */
template <typename TBlockAllocator>
inline void
base_pool_allocator <TBlockAllocator>::remove (void *object)
{
  int size = m_elt_size - offsetof (allocation_object, u.data);

  if (flag_checking)
    {
      gcc_assert (m_initialized);
      gcc_assert (object
		  /* Check if we free more than we allocated.  */
		  && m_elts_free < m_elts_allocated);
#if CHECKING_P
      /* Check whether the PTR was allocated from POOL.  */
      gcc_assert (m_id == allocation_object::get_instance (object)->id);
#endif

      memset (object, 0xaf, size);
    }

#if CHECKING_P 
  /* Mark the element to be free.  */
  allocation_object::get_instance (object)->id = 0;
#endif

  allocation_pool_list *header = new (object) allocation_pool_list;
  header->next = m_returned_free_list;
  m_returned_free_list = header;
  VALGRIND_DISCARD (VALGRIND_MAKE_MEM_NOACCESS (object, size));
  m_elts_free++;

  if (GATHER_STATISTICS)
    {
      pool_allocator_usage.release_instance_overhead (this, m_elt_size);
    }
}

/* Number of elements currently active (not returned to pool).  Used for cheap
   consistency checks.  */
template <typename TBlockAllocator>
inline size_t
base_pool_allocator <TBlockAllocator>::num_elts_current ()
{
  return m_elts_allocated - m_elts_free;
}

/* Specialization of base_pool_allocator which should be used in most cases.
   Another specialization may be needed, if object size is greater than
   memory_block_pool::block_size (64 KB).  */
typedef base_pool_allocator <memory_block_pool> pool_allocator;

/* Type based memory pool allocator.  */
template <typename T>
class object_allocator
{
public:
  /* Default constructor for pool allocator called NAME.  */
  object_allocator (const char *name CXX_MEM_STAT_INFO):
    m_allocator (name, sizeof (T) PASS_MEM_STAT) {}

  inline void
  release ()
  {
    m_allocator.release ();
  }

  inline void release_if_empty ()
  {
    m_allocator.release_if_empty ();
  }


  /* Allocate memory for instance of type T and call a default constructor.  */

  inline T *
  allocate () ATTRIBUTE_MALLOC
  {
    return ::new (m_allocator.allocate ()) T;
  }

  /* Allocate memory for instance of type T and return void * that
     could be used in situations where a default constructor is not provided
     by the class T.  */

  inline void *
  allocate_raw () ATTRIBUTE_MALLOC
  {
    return m_allocator.allocate ();
  }

  inline void
  remove (T *object)
  {
    /* Call destructor.  */
    object->~T ();

    m_allocator.remove (object);
  }

  inline size_t
  num_elts_current ()
  {
    return m_allocator.num_elts_current ();
  }

private:
  pool_allocator m_allocator;
};

/* Store information about each particular alloc_pool.  Note that this
   will underestimate the amount the amount of storage used by a small amount:
   1) The overhead in a pool is not accounted for.
   2) The unallocated elements in a block are not accounted for.  Note
   that this can at worst case be one element smaller that the block
   size for that pool.  */
struct alloc_pool_descriptor
{
  /* Number of pools allocated.  */
  unsigned long created;
  /* Gross allocated storage.  */
  unsigned long allocated;
  /* Amount of currently active storage.  */
  unsigned long current;
  /* Peak amount of storage used.  */
  unsigned long peak;
  /* Size of element in the pool.  */
  int elt_size;
};

/* Helper for classes that do not provide default ctor.  */

template <typename T>
inline void *
operator new (size_t, object_allocator<T> &a)
{
  return a.allocate_raw ();
}

/* Hashtable mapping alloc_pool names to descriptors.  */
extern hash_map<const char *, alloc_pool_descriptor> *alloc_pool_hash;


#endif
