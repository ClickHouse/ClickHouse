/* Callgraph summary data structure.
   Copyright (C) 2014-2018 Free Software Foundation, Inc.
   Contributed by Martin Liska

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

#ifndef GCC_SYMBOL_SUMMARY_H
#define GCC_SYMBOL_SUMMARY_H

/* We want to pass just pointer types as argument for function_summary
   template class.  */

template <class T>
class function_summary
{
private:
  function_summary();
};

template <class T>
class GTY((user)) function_summary <T *>
{
public:
  /* Default construction takes SYMTAB as an argument.  */
  function_summary (symbol_table *symtab, bool ggc = false): m_ggc (ggc),
    m_insertion_enabled (true), m_released (false), m_map (13, ggc),
    m_symtab (symtab)
  {
    m_symtab_insertion_hook =
      symtab->add_cgraph_insertion_hook
      (function_summary::symtab_insertion, this);

    m_symtab_removal_hook =
      symtab->add_cgraph_removal_hook
      (function_summary::symtab_removal, this);
    m_symtab_duplication_hook =
      symtab->add_cgraph_duplication_hook
      (function_summary::symtab_duplication, this);
  }

  /* Destructor.  */
  virtual ~function_summary ()
  {
    release ();
  }

  /* Destruction method that can be called for GGT purpose.  */
  void release ()
  {
    if (m_released)
      return;

    m_symtab->remove_cgraph_insertion_hook (m_symtab_insertion_hook);
    m_symtab->remove_cgraph_removal_hook (m_symtab_removal_hook);
    m_symtab->remove_cgraph_duplication_hook (m_symtab_duplication_hook);

    /* Release all summaries.  */
    typedef typename hash_map <map_hash, T *>::iterator map_iterator;
    for (map_iterator it = m_map.begin (); it != m_map.end (); ++it)
      release ((*it).second);

    m_released = true;
  }

  /* Traverses all summarys with a function F called with
     ARG as argument.  */
  template<typename Arg, bool (*f)(const T &, Arg)>
  void traverse (Arg a) const
  {
    m_map.traverse <f> (a);
  }

  /* Basic implementation of insert operation.  */
  virtual void insert (cgraph_node *, T *) {}

  /* Basic implementation of removal operation.  */
  virtual void remove (cgraph_node *, T *) {}

  /* Basic implementation of duplication operation.  */
  virtual void duplicate (cgraph_node *, cgraph_node *, T *, T *) {}

  /* Allocates new data that are stored within map.  */
  T* allocate_new ()
  {
    /* Call gcc_internal_because we do not want to call finalizer for
       a type T.  We call dtor explicitly.  */
    return m_ggc ? new (ggc_internal_alloc (sizeof (T))) T () : new T () ;
  }

  /* Release an item that is stored within map.  */
  void release (T *item)
  {
    if (m_ggc)
      {
	item->~T ();
	ggc_free (item);
      }
    else
      delete item;
  }

  /* Getter for summary callgraph node pointer.  */
  T* get (cgraph_node *node)
  {
    gcc_checking_assert (node->summary_uid);
    return get (node->summary_uid);
  }

  /* Return number of elements handled by data structure.  */
  size_t elements ()
  {
    return m_map.elements ();
  }

  /* Return true if a summary for the given NODE already exists.  */
  bool exists (cgraph_node *node)
  {
    return m_map.get (node->summary_uid) != NULL;
  }

  /* Enable insertion hook invocation.  */
  void enable_insertion_hook ()
  {
    m_insertion_enabled = true;
  }

  /* Enable insertion hook invocation.  */
  void disable_insertion_hook ()
  {
    m_insertion_enabled = false;
  }

  /* Symbol insertion hook that is registered to symbol table.  */
  static void symtab_insertion (cgraph_node *node, void *data)
  {
    gcc_checking_assert (node->summary_uid);
    function_summary *summary = (function_summary <T *> *) (data);

    if (summary->m_insertion_enabled)
      summary->insert (node, summary->get (node));
  }

  /* Symbol removal hook that is registered to symbol table.  */
  static void symtab_removal (cgraph_node *node, void *data)
  {
    gcc_checking_assert (node->summary_uid);
    function_summary *summary = (function_summary <T *> *) (data);

    int summary_uid = node->summary_uid;
    T **v = summary->m_map.get (summary_uid);

    if (v)
      {
	summary->remove (node, *v);
	summary->release (*v);
	summary->m_map.remove (summary_uid);
      }
  }

  /* Symbol duplication hook that is registered to symbol table.  */
  static void symtab_duplication (cgraph_node *node, cgraph_node *node2,
				  void *data)
  {
    function_summary *summary = (function_summary <T *> *) (data);
    T **v = summary->m_map.get (node->summary_uid);

    gcc_checking_assert (node2->summary_uid > 0);

    if (v)
      {
	/* This load is necessary, because we insert a new value!  */
	T *data = *v;
	T *duplicate = summary->allocate_new ();
	summary->m_map.put (node2->summary_uid, duplicate);
	summary->duplicate (node, node2, data, duplicate);
      }
  }

protected:
  /* Indication if we use ggc summary.  */
  bool m_ggc;

private:
  typedef int_hash <int, 0, -1> map_hash;

  /* Getter for summary callgraph ID.  */
  T* get (int uid)
  {
    bool existed;
    T **v = &m_map.get_or_insert (uid, &existed);
    if (!existed)
      *v = allocate_new ();

    return *v;
  }

  /* Indicates if insertion hook is enabled.  */
  bool m_insertion_enabled;
  /* Indicates if the summary is released.  */
  bool m_released;
  /* Main summary store, where summary ID is used as key.  */
  hash_map <map_hash, T *> m_map;
  /* Internal summary insertion hook pointer.  */
  cgraph_node_hook_list *m_symtab_insertion_hook;
  /* Internal summary removal hook pointer.  */
  cgraph_node_hook_list *m_symtab_removal_hook;
  /* Internal summary duplication hook pointer.  */
  cgraph_2node_hook_list *m_symtab_duplication_hook;
  /* Symbol table the summary is registered to.  */
  symbol_table *m_symtab;

  template <typename U> friend void gt_ggc_mx (function_summary <U *> * const &);
  template <typename U> friend void gt_pch_nx (function_summary <U *> * const &);
  template <typename U> friend void gt_pch_nx (function_summary <U *> * const &,
      gt_pointer_operator, void *);
};

template <typename T>
void
gt_ggc_mx(function_summary<T *>* const &summary)
{
  gcc_checking_assert (summary->m_ggc);
  gt_ggc_mx (&summary->m_map);
}

template <typename T>
void
gt_pch_nx(function_summary<T *>* const &summary)
{
  gcc_checking_assert (summary->m_ggc);
  gt_pch_nx (&summary->m_map);
}

template <typename T>
void
gt_pch_nx(function_summary<T *>* const& summary, gt_pointer_operator op,
	  void *cookie)
{
  gcc_checking_assert (summary->m_ggc);
  gt_pch_nx (&summary->m_map, op, cookie);
}

/* An impossible class templated by non-pointers so, which makes sure that only
   summaries gathering pointers can be created.  */

template <class T>
class call_summary
{
private:
  call_summary();
};

/* Class to store auxiliary information about call graph edges.  */

template <class T>
class GTY((user)) call_summary <T *>
{
public:
  /* Default construction takes SYMTAB as an argument.  */
  call_summary (symbol_table *symtab, bool ggc = false): m_ggc (ggc),
    m_map (13, ggc), m_released (false), m_symtab (symtab)
  {
    m_symtab_removal_hook =
      symtab->add_edge_removal_hook
      (call_summary::symtab_removal, this);
    m_symtab_duplication_hook =
      symtab->add_edge_duplication_hook
      (call_summary::symtab_duplication, this);
  }

  /* Destructor.  */
  virtual ~call_summary ()
  {
    release ();
  }

  /* Destruction method that can be called for GGT purpose.  */
  void release ()
  {
    if (m_released)
      return;

    m_symtab->remove_edge_removal_hook (m_symtab_removal_hook);
    m_symtab->remove_edge_duplication_hook (m_symtab_duplication_hook);

    /* Release all summaries.  */
    typedef typename hash_map <map_hash, T *>::iterator map_iterator;
    for (map_iterator it = m_map.begin (); it != m_map.end (); ++it)
      release ((*it).second);

    m_released = true;
  }

  /* Traverses all summarys with a function F called with
     ARG as argument.  */
  template<typename Arg, bool (*f)(const T &, Arg)>
  void traverse (Arg a) const
  {
    m_map.traverse <f> (a);
  }

  /* Basic implementation of removal operation.  */
  virtual void remove (cgraph_edge *, T *) {}

  /* Basic implementation of duplication operation.  */
  virtual void duplicate (cgraph_edge *, cgraph_edge *, T *, T *) {}

  /* Allocates new data that are stored within map.  */
  T* allocate_new ()
  {
    /* Call gcc_internal_because we do not want to call finalizer for
       a type T.  We call dtor explicitly.  */
    return m_ggc ? new (ggc_internal_alloc (sizeof (T))) T () : new T () ;
  }

  /* Release an item that is stored within map.  */
  void release (T *item)
  {
    if (m_ggc)
      {
	item->~T ();
	ggc_free (item);
      }
    else
      delete item;
  }

  /* Getter for summary callgraph edge pointer.  */
  T* get (cgraph_edge *edge)
  {
    return get (hashable_uid (edge));
  }

  /* Return number of elements handled by data structure.  */
  size_t elements ()
  {
    return m_map.elements ();
  }

  /* Return true if a summary for the given EDGE already exists.  */
  bool exists (cgraph_edge *edge)
  {
    return m_map.get (hashable_uid (edge)) != NULL;
  }

  /* Symbol removal hook that is registered to symbol table.  */
  static void symtab_removal (cgraph_edge *edge, void *data)
  {
    call_summary *summary = (call_summary <T *> *) (data);

    int h_uid = summary->hashable_uid (edge);
    T **v = summary->m_map.get (h_uid);

    if (v)
      {
	summary->remove (edge, *v);
	summary->release (*v);
	summary->m_map.remove (h_uid);
      }
  }

  /* Symbol duplication hook that is registered to symbol table.  */
  static void symtab_duplication (cgraph_edge *edge1, cgraph_edge *edge2,
				  void *data)
  {
    call_summary *summary = (call_summary <T *> *) (data);
    T **v = summary->m_map.get (summary->hashable_uid (edge1));

    if (v)
      {
	/* This load is necessary, because we insert a new value!  */
	T *data = *v;
	T *duplicate = summary->allocate_new ();
	summary->m_map.put (summary->hashable_uid (edge2), duplicate);
	summary->duplicate (edge1, edge2, data, duplicate);
      }
  }

protected:
  /* Indication if we use ggc summary.  */
  bool m_ggc;

private:
  typedef int_hash <int, 0, -1> map_hash;

  /* Getter for summary callgraph ID.  */
  T* get (int uid)
  {
    bool existed;
    T **v = &m_map.get_or_insert (uid, &existed);
    if (!existed)
      *v = allocate_new ();

    return *v;
  }

  /* Get a hashable uid of EDGE.  */
  int hashable_uid (cgraph_edge *edge)
  {
    /* Edge uids start at zero which our hash_map does not like.  */
    return edge->uid + 1;
  }

  /* Main summary store, where summary ID is used as key.  */
  hash_map <map_hash, T *> m_map;
  /* Internal summary removal hook pointer.  */
  cgraph_edge_hook_list *m_symtab_removal_hook;
  /* Internal summary duplication hook pointer.  */
  cgraph_2edge_hook_list *m_symtab_duplication_hook;
  /* Indicates if the summary is released.  */
  bool m_released;
  /* Symbol table the summary is registered to.  */
  symbol_table *m_symtab;

  template <typename U> friend void gt_ggc_mx (call_summary <U *> * const &);
  template <typename U> friend void gt_pch_nx (call_summary <U *> * const &);
  template <typename U> friend void gt_pch_nx (call_summary <U *> * const &,
      gt_pointer_operator, void *);
};

template <typename T>
void
gt_ggc_mx(call_summary<T *>* const &summary)
{
  gcc_checking_assert (summary->m_ggc);
  gt_ggc_mx (&summary->m_map);
}

template <typename T>
void
gt_pch_nx(call_summary<T *>* const &summary)
{
  gcc_checking_assert (summary->m_ggc);
  gt_pch_nx (&summary->m_map);
}

template <typename T>
void
gt_pch_nx(call_summary<T *>* const& summary, gt_pointer_operator op,
	  void *cookie)
{
  gcc_checking_assert (summary->m_ggc);
  gt_pch_nx (&summary->m_map, op, cookie);
}

#endif  /* GCC_SYMBOL_SUMMARY_H  */
