/* A type-safe hash set.
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


#ifndef hash_set_h
#define hash_set_h

template<typename KeyId, typename Traits = default_hash_traits<KeyId> >
class hash_set
{
public:
  typedef typename Traits::value_type Key;
  explicit hash_set (size_t n = 13, bool ggc = false CXX_MEM_STAT_INFO)
    : m_table (n, ggc, GATHER_STATISTICS, HASH_SET_ORIGIN PASS_MEM_STAT) {}

  /* Create a hash_set in gc memory with space for at least n elements.  */

  static hash_set *
    create_ggc (size_t n)
      {
	hash_set *set = ggc_alloc<hash_set> ();
	new (set) hash_set (n, true);
	return set;
      }

  /* If key k isn't already in the map add it to the map, and
     return false.  Otherwise return true.  */

  bool add (const Key &k)
    {
      Key *e = m_table.find_slot_with_hash (k, Traits::hash (k), INSERT);
      bool existed = !Traits::is_empty (*e);
      if (!existed)
	*e = k;

      return existed;
    }

  /* if the passed in key is in the map return its value otherwise NULL.  */

  bool contains (const Key &k)
    {
      Key &e = m_table.find_with_hash (k, Traits::hash (k));
      return !Traits::is_empty (e);
    }

  void remove (const Key &k)
    {
      m_table.remove_elt_with_hash (k, Traits::hash (k));
    }

  /* Call the call back on each pair of key and value with the passed in
     arg.  */

  template<typename Arg, bool (*f)(const typename Traits::value_type &, Arg)>
  void traverse (Arg a) const
    {
      for (typename hash_table<Traits>::iterator iter = m_table.begin ();
	   iter != m_table.end (); ++iter)
	f (*iter, a);
    }

  /* Return the number of elements in the set.  */

  size_t elements () const { return m_table.elements (); }

  /* Clear the hash table.  */

  void empty () { m_table.empty (); }

  class iterator
  {
  public:
    explicit iterator (const typename hash_table<Traits>::iterator &iter) :
      m_iter (iter) {}

    iterator &operator++ ()
      {
	++m_iter;
	return *this;
      }

    Key
    operator* ()
      {
	return *m_iter;
      }

    bool
    operator != (const iterator &other) const
      {
	return m_iter != other.m_iter;
      }

  private:
    typename hash_table<Traits>::iterator m_iter;
  };

  /* Standard iterator retrieval methods.  */

  iterator begin () const { return iterator (m_table.begin ()); }
  iterator end () const { return iterator (m_table.end ()); }


private:

  template<typename T, typename U> friend void gt_ggc_mx (hash_set<T, U> *);
  template<typename T, typename U> friend void gt_pch_nx (hash_set<T, U> *);
      template<typename T, typename U> friend void gt_pch_nx (hash_set<T, U> *, gt_pointer_operator, void *);

  hash_table<Traits> m_table;
};

/* Generic hash_set<TYPE> debug helper.

   This needs to be instantiated for each hash_set<TYPE> used throughout
   the compiler like this:

    DEFINE_DEBUG_HASH_SET (TYPE)

   The reason we have a debug_helper() is because GDB can't
   disambiguate a plain call to debug(some_hash), and it must be called
   like debug<TYPE>(some_hash).  */
template<typename T>
void
debug_helper (hash_set<T> &ref)
{
  for (typename hash_set<T>::iterator it = ref.begin ();
       it != ref.end (); ++it)
    {
      debug_slim (*it);
      fputc ('\n', stderr);
    }
}

#define DEFINE_DEBUG_HASH_SET(T) \
  template void debug_helper (hash_set<T> &);		\
  DEBUG_FUNCTION void					\
  debug (hash_set<T> &ref)				\
  {							\
    debug_helper <T> (ref);				\
  }							\
  DEBUG_FUNCTION void					\
  debug (hash_set<T> *ptr)				\
  {							\
    if (ptr)						\
      debug (*ptr);					\
    else						\
      fprintf (stderr, "<nil>\n");			\
  }

/* ggc marking routines.  */

template<typename K, typename H>
static inline void
gt_ggc_mx (hash_set<K, H> *h)
{
  gt_ggc_mx (&h->m_table);
}

template<typename K, typename H>
static inline void
gt_pch_nx (hash_set<K, H> *h)
{
  gt_pch_nx (&h->m_table);
}

template<typename K, typename H>
static inline void
gt_pch_nx (hash_set<K, H> *h, gt_pointer_operator op, void *cookie)
{
  op (&h->m_table.m_entries, cookie);
}

#endif
