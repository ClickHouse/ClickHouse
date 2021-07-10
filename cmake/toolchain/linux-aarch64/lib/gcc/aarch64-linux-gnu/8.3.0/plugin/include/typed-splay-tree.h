/* A typesafe wrapper around libiberty's splay-tree.h.
   Copyright (C) 2015-2018 Free Software Foundation, Inc.

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

#ifndef GCC_TYPED_SPLAY_TREE_H
#define GCC_TYPED_SPLAY_TREE_H

#include "splay-tree.h"

/* Typesafe wrapper around libiberty's splay-tree.h.  */
template <typename KEY_TYPE, typename VALUE_TYPE>
class typed_splay_tree
{
 public:
  typedef KEY_TYPE key_type;
  typedef VALUE_TYPE value_type;

  typedef int (*compare_fn) (key_type, key_type);
  typedef void (*delete_key_fn) (key_type);
  typedef void (*delete_value_fn) (value_type);
  typedef int (*foreach_fn) (key_type, value_type, void *);

  typed_splay_tree (compare_fn,
		    delete_key_fn,
		    delete_value_fn);
  ~typed_splay_tree ();

  value_type lookup (key_type k);
  value_type predecessor (key_type k);
  value_type successor (key_type k);
  void insert (key_type k, value_type v);
  value_type max ();
  value_type min ();
  int foreach (foreach_fn, void *);

 private:
  /* Helper type for typed_splay_tree::foreach.  */
  struct closure
  {
    closure (foreach_fn outer_cb, void *outer_user_data)
    : m_outer_cb (outer_cb), m_outer_user_data (outer_user_data) {}

    foreach_fn m_outer_cb;
    void *m_outer_user_data;
  };

  static int inner_foreach_fn (splay_tree_node node, void *user_data);

  static value_type node_to_value (splay_tree_node node);

 private:
  ::splay_tree m_inner;
};

/* Constructor for typed_splay_tree <K, V>.  */

template <typename KEY_TYPE, typename VALUE_TYPE>
inline typed_splay_tree<KEY_TYPE, VALUE_TYPE>::
  typed_splay_tree (compare_fn compare_fn,
		    delete_key_fn delete_key_fn,
		    delete_value_fn delete_value_fn)
{
  m_inner = splay_tree_new ((splay_tree_compare_fn)
			    (void (*) (void)) compare_fn,
			    (splay_tree_delete_key_fn)
			    (void (*) (void)) delete_key_fn,
			    (splay_tree_delete_value_fn)
			    (void (*) (void)) delete_value_fn);
}

/* Destructor for typed_splay_tree <K, V>.  */

template <typename KEY_TYPE, typename VALUE_TYPE>
inline typed_splay_tree<KEY_TYPE, VALUE_TYPE>::
  ~typed_splay_tree ()
{
  splay_tree_delete (m_inner);
}

/* Lookup KEY, returning a value if present, and NULL
   otherwise.  */

template <typename KEY_TYPE, typename VALUE_TYPE>
inline VALUE_TYPE
typed_splay_tree<KEY_TYPE, VALUE_TYPE>::lookup (key_type key)
{
  splay_tree_node node = splay_tree_lookup (m_inner, (splay_tree_key)key);
  return node_to_value (node);
}

/* Return the immediate predecessor of KEY, or NULL if there is no
   predecessor.  KEY need not be present in the tree.  */

template <typename KEY_TYPE, typename VALUE_TYPE>
inline VALUE_TYPE
typed_splay_tree<KEY_TYPE, VALUE_TYPE>::predecessor (key_type key)
{
  splay_tree_node node = splay_tree_predecessor (m_inner, (splay_tree_key)key);
  return node_to_value (node);
}

/* Return the immediate successor of KEY, or NULL if there is no
   successor.  KEY need not be present in the tree.  */

template <typename KEY_TYPE, typename VALUE_TYPE>
inline VALUE_TYPE
typed_splay_tree<KEY_TYPE, VALUE_TYPE>::successor (key_type k)
{
  splay_tree_node node = splay_tree_successor (m_inner, (splay_tree_key)k);
  return node_to_value (node);
}

/* Insert a new node (associating KEY with VALUE).  If a
   previous node with the indicated KEY exists, its data is replaced
   with the new value.  */

template <typename KEY_TYPE, typename VALUE_TYPE>
inline void
typed_splay_tree<KEY_TYPE, VALUE_TYPE>::insert (key_type key,
						value_type value)
{
  splay_tree_insert (m_inner,
		     (splay_tree_key)key,
		     (splay_tree_value)value);
}

/* Get the value with maximal key.  */

template <typename KEY_TYPE, typename VALUE_TYPE>
inline VALUE_TYPE
typed_splay_tree<KEY_TYPE, VALUE_TYPE>::max ()
{
  return node_to_value (splay_tree_max (m_inner));
}

/* Get the value with minimal key.  */

template <typename KEY_TYPE, typename VALUE_TYPE>
inline VALUE_TYPE
typed_splay_tree<KEY_TYPE, VALUE_TYPE>::min ()
{
  return node_to_value (splay_tree_min (m_inner));
}

/* Call OUTER_CB, passing it the OUTER_USER_DATA, for every node,
   following an in-order traversal.  If OUTER_CB ever returns a non-zero
   value, the iteration ceases immediately, and the value is returned.
   Otherwise, this function returns 0.  */

template <typename KEY_TYPE, typename VALUE_TYPE>
inline int
typed_splay_tree<KEY_TYPE, VALUE_TYPE>::foreach (foreach_fn outer_cb,
						 void *outer_user_data)
{
  closure c (outer_cb, outer_user_data);

  return splay_tree_foreach (m_inner, inner_foreach_fn, &c);
}

/* Helper function for typed_splay_tree::foreach.  */

template <typename KEY_TYPE, typename VALUE_TYPE>
int
typed_splay_tree<KEY_TYPE, VALUE_TYPE>::inner_foreach_fn (splay_tree_node node,
							  void *user_data)
{
  closure *c = (closure *)user_data;

  return c->m_outer_cb ((KEY_TYPE)node->key, (VALUE_TYPE)node->value,
			c->m_outer_user_data);
}

/* Internal function for converting from splay_tree_node to
   VALUE_TYPE.  */
template <typename KEY_TYPE, typename VALUE_TYPE>
inline VALUE_TYPE
typed_splay_tree<KEY_TYPE, VALUE_TYPE>::node_to_value (splay_tree_node node)
{
  if (node)
    return (value_type)node->value;
  else
    return 0;
}

#endif  /* GCC_TYPED_SPLAY_TREE_H  */
