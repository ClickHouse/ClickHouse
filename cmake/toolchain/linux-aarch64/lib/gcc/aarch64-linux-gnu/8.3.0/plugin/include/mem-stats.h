/* A memory statistics tracking infrastructure.
   Copyright (C) 2015-2018 Free Software Foundation, Inc.
   Contributed by Martin Liska  <mliska@suse.cz>

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

#ifndef GCC_MEM_STATS_H
#define GCC_MEM_STATS_H

/* Forward declaration.  */
template<typename Key, typename Value,
	 typename Traits = simple_hashmap_traits<default_hash_traits<Key>,
						 Value> >
class hash_map;

#define LOCATION_LINE_EXTRA_SPACE 30
#define LOCATION_LINE_WIDTH	  48

/* Memory allocation location.  */
struct mem_location
{
  /* Default constructor.  */
  inline
  mem_location () {}

  /* Constructor.  */
  inline
  mem_location (mem_alloc_origin origin, bool ggc,
		const char *filename = NULL, int line = 0,
		const char *function = NULL):
    m_filename (filename), m_function (function), m_line (line), m_origin
    (origin), m_ggc (ggc) {}

  /* Copy constructor.  */
  inline
  mem_location (mem_location &other): m_filename (other.m_filename),
    m_function (other.m_function), m_line (other.m_line),
    m_origin (other.m_origin), m_ggc (other.m_ggc) {}

  /* Compute hash value based on file name, function name and line in
     source code. As there is just a single pointer registered for every
     constant that points to e.g. the same file name, we can use hash
     of the pointer.  */
  hashval_t
  hash ()
  {
    inchash::hash hash;

    hash.add_ptr (m_filename);
    hash.add_ptr (m_function);
    hash.add_int (m_line);

    return hash.end ();
  }

  /* Return true if the memory location is equal to OTHER.  */
  int
  equal (mem_location &other)
  {
    return m_filename == other.m_filename && m_function == other.m_function
      && m_line == other.m_line;
  }

  /* Return trimmed filename for the location.  */
  inline const char *
  get_trimmed_filename ()
  {
    const char *s1 = m_filename;
    const char *s2;

    while ((s2 = strstr (s1, "gcc/")))
      s1 = s2 + 4;

    return s1;
  }

  inline char *
  to_string ()
  {
    unsigned l = strlen (get_trimmed_filename ()) + strlen (m_function)
      + LOCATION_LINE_EXTRA_SPACE;

    char *s = XNEWVEC (char, l);
    sprintf (s, "%s:%i (%s)", get_trimmed_filename (),
	     m_line, m_function);

    s[MIN (LOCATION_LINE_WIDTH, l - 1)] = '\0';

    return s;
  }

  /* Return display name associated to ORIGIN type.  */
  static const char *
  get_origin_name (mem_alloc_origin origin)
  {
    return mem_alloc_origin_names[(unsigned) origin];
  }

  /* File name of source code.  */
  const char *m_filename;
  /* Funcation name.  */
  const char *m_function;
  /* Line number in source code.  */
  int m_line;
  /* Origin type.  */
  mem_alloc_origin m_origin;
  /* Flag if used by GGC allocation.  */
  bool m_ggc;
};

/* Memory usage register to a memory location.  */
struct mem_usage
{
  /* Default constructor.  */
  mem_usage (): m_allocated (0), m_times (0), m_peak (0), m_instances (1) {}

  /* Constructor.  */
  mem_usage (size_t allocated, size_t times, size_t peak, size_t instances = 0):
    m_allocated (allocated), m_times (times), m_peak (peak),
    m_instances (instances) {}

  /* Register overhead of SIZE bytes.  */
  inline void
  register_overhead (size_t size)
  {
    m_allocated += size;
    m_times++;

    if (m_peak < m_allocated)
      m_peak = m_allocated;
  }

  /* Release overhead of SIZE bytes.  */
  inline void
  release_overhead (size_t size)
  {
    gcc_assert (size <= m_allocated);

    m_allocated -= size;
  }

  /* Sum the usage with SECOND usage.  */
  mem_usage
  operator+ (const mem_usage &second)
  {
    return mem_usage (m_allocated + second.m_allocated,
		      m_times + second.m_times,
		      m_peak + second.m_peak,
		      m_instances + second.m_instances);
  }

  /* Equality operator.  */
  inline bool
  operator== (const mem_usage &second) const
  {
    return (m_allocated == second.m_allocated
	    && m_peak == second.m_peak
	    && m_allocated == second.m_allocated);
  }

  /* Comparison operator.  */
  inline bool
  operator< (const mem_usage &second) const
  {
    if (*this == second)
      return false;

    return (m_allocated == second.m_allocated ?
	    (m_peak == second.m_peak ? m_times < second.m_times
	     : m_peak < second.m_peak) : m_allocated < second.m_allocated);
  }

  /* Compare wrapper used by qsort method.  */
  static int
  compare (const void *first, const void *second)
  {
    typedef std::pair<mem_location *, mem_usage *> mem_pair_t;

    const mem_pair_t f = *(const mem_pair_t *)first;
    const mem_pair_t s = *(const mem_pair_t *)second;

    if (*f.second == *s.second)
      return 0;

    return *f.second < *s.second ? 1 : -1;
  }

  /* Dump usage coupled to LOC location, where TOTAL is sum of all rows.  */
  inline void
  dump (mem_location *loc, mem_usage &total) const
  {
    char *location_string = loc->to_string ();

    fprintf (stderr, "%-48s %10" PRIu64 ":%5.1f%%"
	     "%10" PRIu64 "%10" PRIu64 ":%5.1f%%%10s\n",
	     location_string, (uint64_t)m_allocated,
	     get_percent (m_allocated, total.m_allocated),
	     (uint64_t)m_peak, (uint64_t)m_times,
	     get_percent (m_times, total.m_times), loc->m_ggc ? "ggc" : "heap");

    free (location_string);
  }

  /* Dump footer.  */
  inline void
  dump_footer () const
  {
    print_dash_line ();
    fprintf (stderr, "%s%54" PRIu64 "%27" PRIu64 "\n", "Total",
	     (uint64_t)m_allocated, (uint64_t)m_times);
    print_dash_line ();
  }

  /* Return fraction of NOMINATOR and DENOMINATOR in percent.  */
  static inline float
  get_percent (size_t nominator, size_t denominator)
  {
    return denominator == 0 ? 0.0f : nominator * 100.0 / denominator;
  }

  /* Print line made of dashes.  */
  static inline void
  print_dash_line (size_t count = 140)
  {
    while (count--)
      fputc ('-', stderr);
    fputc ('\n', stderr);
  }

  /* Dump header with NAME.  */
  static inline void
  dump_header (const char *name)
  {
    fprintf (stderr, "%-48s %11s%16s%10s%17s\n", name, "Leak", "Peak",
	     "Times", "Type");
    print_dash_line ();
  }

  /* Current number of allocated bytes.  */
  size_t m_allocated;
  /* Number of allocations.  */
  size_t m_times;
  /* Peak allocation in bytes.  */
  size_t m_peak;
  /* Number of container instances.  */
  size_t m_instances;
};

/* Memory usage pair that connectes memory usage and number
   of allocated bytes.  */
template <class T>
struct mem_usage_pair
{
  mem_usage_pair (T *usage_, size_t allocated_): usage (usage_),
  allocated (allocated_) {}

  T *usage;
  size_t allocated;
};

/* Memory allocation description.  */
template <class T>
class mem_alloc_description
{
public:
  struct mem_location_hash : nofree_ptr_hash <mem_location>
  {
    static hashval_t
    hash (value_type l)
    {
	inchash::hash hstate;

	hstate.add_ptr ((const void *)l->m_filename);
	hstate.add_ptr (l->m_function);
	hstate.add_int (l->m_line);

	return hstate.end ();
    }

    static bool
    equal (value_type l1, value_type l2)
    {
      return l1->m_filename == l2->m_filename
	&& l1->m_function == l2->m_function
	&& l1->m_line == l2->m_line;
    }
  };

  /* Internal class type definitions.  */
  typedef hash_map <mem_location_hash, T *> mem_map_t;
  typedef hash_map <const void *, mem_usage_pair<T> > reverse_mem_map_t;
  typedef hash_map <const void *, std::pair<T *, size_t> > reverse_object_map_t;
  typedef std::pair <mem_location *, T *> mem_list_t;

  /* Default contructor.  */
  mem_alloc_description ();

  /* Default destructor.  */
  ~mem_alloc_description ();

  /* Returns true if instance PTR is registered by the memory description.  */
  bool
  contains_descriptor_for_instance (const void *ptr);

  /* Return descriptor for instance PTR.  */
  T *
  get_descriptor_for_instance (const void *ptr);

  /* Register memory allocation descriptor for container PTR which is
     described by a memory LOCATION.  */
  T *
  register_descriptor (const void *ptr, mem_location *location);

  /* Register memory allocation descriptor for container PTR.  ORIGIN identifies
     type of container and GGC identifes if the allocation is handled in GGC
     memory.  Each location is identified by file NAME, LINE in source code and
     FUNCTION name.  */
  T *
  register_descriptor (const void *ptr, mem_alloc_origin origin,
			  bool ggc, const char *name, int line,
			  const char *function);

  /* Register instance overhead identified by PTR pointer. Allocation takes
     SIZE bytes.  */
  T *
  register_instance_overhead (size_t size, const void *ptr);

  /* For containers (and GGC) where we want to track every instance object,
     we register allocation of SIZE bytes, identified by PTR pointer, belonging
     to USAGE descriptor.  */
  void
  register_object_overhead (T *usage, size_t size, const void *ptr);

  /* Release PTR pointer of SIZE bytes. If REMOVE_FROM_MAP is set to true,
     remove the instance from reverse map.  */
  void
  release_instance_overhead (void *ptr, size_t size,
				  bool remove_from_map = false);

  /* Release intance object identified by PTR pointer.  */
  void
  release_object_overhead (void *ptr);

  /* Get sum value for ORIGIN type of allocation for the descriptor.  */
  T
  get_sum (mem_alloc_origin origin);

  /* Get all tracked instances registered by the description. Items
     are filtered by ORIGIN type, LENGTH is return value where we register
     the number of elements in the list. If we want to process custom order,
     CMP comparator can be provided.  */
  mem_list_t *
  get_list (mem_alloc_origin origin, unsigned *length,
	    int (*cmp) (const void *first, const void *second) = NULL);

  /* Dump all tracked instances of type ORIGIN. If we want to process custom
     order, CMP comparator can be provided.  */
  void dump (mem_alloc_origin origin,
	     int (*cmp) (const void *first, const void *second) = NULL);

  /* Reverse object map used for every object allocation mapping.  */
  reverse_object_map_t *m_reverse_object_map;

private:
  /* Register overhead of SIZE bytes of ORIGIN type. PTR pointer is allocated
     in NAME source file, at LINE in source code, in FUNCTION.  */
  T *register_overhead (size_t size, mem_alloc_origin origin, const char *name,
			int line, const char *function, const void *ptr);

  /* Allocation location coupled to the description.  */
  mem_location m_location;

  /* Location to usage mapping.  */
  mem_map_t *m_map;

  /* Reverse pointer to usage mapping.  */
  reverse_mem_map_t *m_reverse_map;
};


/* Returns true if instance PTR is registered by the memory description.  */

template <class T>
inline bool
mem_alloc_description<T>::contains_descriptor_for_instance (const void *ptr)
{
  return m_reverse_map->get (ptr);
}

/* Return descriptor for instance PTR.  */

template <class T>
inline T*
mem_alloc_description<T>::get_descriptor_for_instance (const void *ptr)
{
  return m_reverse_map->get (ptr) ? (*m_reverse_map->get (ptr)).usage : NULL;
}


  /* Register memory allocation descriptor for container PTR which is
     described by a memory LOCATION.  */
template <class T>
inline T*
mem_alloc_description<T>::register_descriptor (const void *ptr,
					       mem_location *location)
{
  T *usage = NULL;

  T **slot = m_map->get (location);
  if (slot)
    {
      delete location;
      usage = *slot;
      usage->m_instances++;
    }
  else
    {
      usage = new T ();
      m_map->put (location, usage);
    }

  if (!m_reverse_map->get (ptr))
    m_reverse_map->put (ptr, mem_usage_pair<T> (usage, 0));

  return usage;
}

/* Register memory allocation descriptor for container PTR.  ORIGIN identifies
   type of container and GGC identifes if the allocation is handled in GGC
   memory.  Each location is identified by file NAME, LINE in source code and
   FUNCTION name.  */

template <class T>
inline T*
mem_alloc_description<T>::register_descriptor (const void *ptr,
					       mem_alloc_origin origin,
					       bool ggc,
					       const char *filename,
					       int line,
					       const char *function)
{
  mem_location *l = new mem_location (origin, ggc, filename, line, function);
  return register_descriptor (ptr, l);
}

/* Register instance overhead identified by PTR pointer. Allocation takes
   SIZE bytes.  */

template <class T>
inline T*
mem_alloc_description<T>::register_instance_overhead (size_t size,
						      const void *ptr)
{
  mem_usage_pair <T> *slot = m_reverse_map->get (ptr);
  if (!slot)
    {
      /* Due to PCH, it can really happen.  */
      return NULL;
    }

  T *usage = (*slot).usage;
  usage->register_overhead (size);

  return usage;
}

/* For containers (and GGC) where we want to track every instance object,
   we register allocation of SIZE bytes, identified by PTR pointer, belonging
   to USAGE descriptor.  */

template <class T>
void
mem_alloc_description<T>::register_object_overhead (T *usage, size_t size,
						    const void *ptr)
{
  /* In case of GGC, it is possible to have already occupied the memory
     location.  */
  m_reverse_object_map->put (ptr, std::pair<T *, size_t> (usage, size));
}

/* Register overhead of SIZE bytes of ORIGIN type. PTR pointer is allocated
   in NAME source file, at LINE in source code, in FUNCTION.  */

template <class T>
inline T*
mem_alloc_description<T>::register_overhead (size_t size,
					     mem_alloc_origin origin,
					     const char *filename,
					     int line,
					     const char *function,
					     const void *ptr)
{
  T *usage = register_descriptor (ptr, origin, filename, line, function);
  usage->register_overhead (size);

  return usage;
}

/* Release PTR pointer of SIZE bytes.  */

template <class T>
inline void
mem_alloc_description<T>::release_instance_overhead (void *ptr, size_t size,
						     bool remove_from_map)
{
  mem_usage_pair<T> *slot = m_reverse_map->get (ptr);

  if (!slot)
    {
      /* Due to PCH, it can really happen.  */
      return;
    }

  mem_usage_pair<T> usage_pair = *slot;
  usage_pair.usage->release_overhead (size);

  if (remove_from_map)
    m_reverse_map->remove (ptr);
}

/* Release intance object identified by PTR pointer.  */

template <class T>
inline void
mem_alloc_description<T>::release_object_overhead (void *ptr)
{
  std::pair <T *, size_t> *entry = m_reverse_object_map->get (ptr);
  if (entry)
    {
      entry->first->release_overhead (entry->second);
      m_reverse_object_map->remove (ptr);
    }
}

/* Default contructor.  */

template <class T>
inline
mem_alloc_description<T>::mem_alloc_description ()
{
  m_map = new mem_map_t (13, false, false);
  m_reverse_map = new reverse_mem_map_t (13, false, false);
  m_reverse_object_map = new reverse_object_map_t (13, false, false);
}

/* Default destructor.  */

template <class T>
inline
mem_alloc_description<T>::~mem_alloc_description ()
{
  for (typename mem_map_t::iterator it = m_map->begin (); it != m_map->end ();
       ++it)
    {
      delete (*it).first;
      delete (*it).second;
    }

  delete m_map;
  delete m_reverse_map;
  delete m_reverse_object_map;
}

/* Get all tracked instances registered by the description. Items are filtered
   by ORIGIN type, LENGTH is return value where we register the number of
   elements in the list. If we want to process custom order, CMP comparator
   can be provided.  */

template <class T>
inline
typename mem_alloc_description<T>::mem_list_t *
mem_alloc_description<T>::get_list (mem_alloc_origin origin, unsigned *length,
			int (*cmp) (const void *first, const void *second))
{
  /* vec data structure is not used because all vectors generate memory
     allocation info a it would create a cycle.  */
  size_t element_size = sizeof (mem_list_t);
  mem_list_t *list = XCNEWVEC (mem_list_t, m_map->elements ());
  unsigned i = 0;

  for (typename mem_map_t::iterator it = m_map->begin (); it != m_map->end ();
       ++it)
    if ((*it).first->m_origin == origin)
      list[i++] = std::pair<mem_location*, T*> (*it);

  qsort (list, i, element_size, cmp == NULL ? T::compare : cmp);
  *length = i;

  return list;
}

/* Get sum value for ORIGIN type of allocation for the descriptor.  */

template <class T>
inline T
mem_alloc_description<T>::get_sum (mem_alloc_origin origin)
{
  unsigned length;
  mem_list_t *list = get_list (origin, &length);
  T sum;

  for (unsigned i = 0; i < length; i++)
    sum = sum + *list[i].second;

  XDELETEVEC (list);

  return sum;
}

/* Dump all tracked instances of type ORIGIN. If we want to process custom
   order, CMP comparator can be provided.  */

template <class T>
inline void
mem_alloc_description<T>::dump (mem_alloc_origin origin,
				int (*cmp) (const void *first,
					    const void *second))
{
  unsigned length;

  fprintf (stderr, "\n");

  mem_list_t *list = get_list (origin, &length, cmp);
  T total = get_sum (origin);

  T::dump_header (mem_location::get_origin_name (origin));
  for (int i = length - 1; i >= 0; i--)
    list[i].second->dump (list[i].first, total);

  total.dump_footer ();

  XDELETEVEC (list);

  fprintf (stderr, "\n");
}

#endif // GCC_MEM_STATS_H
