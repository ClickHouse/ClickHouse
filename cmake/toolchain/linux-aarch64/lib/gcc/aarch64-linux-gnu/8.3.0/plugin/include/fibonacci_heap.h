/* Fibonacci heap for GNU compiler.
   Copyright (C) 1998-2018 Free Software Foundation, Inc.
   Contributed by Daniel Berlin (dan@cgsoftware.com).
   Re-implemented in C++ by Martin Liska <mliska@suse.cz>

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

/* Fibonacci heaps are somewhat complex, but, there's an article in
   DDJ that explains them pretty well:

   http://www.ddj.com/articles/1997/9701/9701o/9701o.htm?topic=algoritms

   Introduction to algorithms by Corman and Rivest also goes over them.

   The original paper that introduced them is "Fibonacci heaps and their
   uses in improved network optimization algorithms" by Tarjan and
   Fredman (JACM 34(3), July 1987).

   Amortized and real worst case time for operations:

   ExtractMin: O(lg n) amortized. O(n) worst case.
   DecreaseKey: O(1) amortized.  O(lg n) worst case.
   Insert: O(1) amortized.
   Union: O(1) amortized.  */

#ifndef GCC_FIBONACCI_HEAP_H
#define GCC_FIBONACCI_HEAP_H

/* Forward definition.  */

template<class K, class V>
class fibonacci_heap;

/* Fibonacci heap node class.  */

template<class K, class V>
class fibonacci_node
{
  typedef fibonacci_node<K,V> fibonacci_node_t;
  friend class fibonacci_heap<K,V>;

public:
  /* Default constructor.  */
  fibonacci_node (): m_parent (NULL), m_child (NULL), m_left (this),
    m_right (this), m_degree (0), m_mark (0)
  {
  }

  /* Constructor for a node with given KEY.  */
  fibonacci_node (K key, V *data = NULL): m_parent (NULL), m_child (NULL),
    m_left (this), m_right (this), m_key (key), m_data (data),
    m_degree (0), m_mark (0)
  {
  }

  /* Compare fibonacci node with OTHER node.  */
  int compare (fibonacci_node_t *other)
  {
    if (m_key < other->m_key)
      return -1;
    if (m_key > other->m_key)
      return 1;
    return 0;
  }

  /* Compare the node with a given KEY.  */
  int compare_data (K key)
  {
    return fibonacci_node_t (key).compare (this);
  }

  /* Remove fibonacci heap node.  */
  fibonacci_node_t *remove ();

  /* Link the node with PARENT.  */
  void link (fibonacci_node_t *parent);

  /* Return key associated with the node.  */
  K get_key ()
  {
    return m_key;
  }

  /* Return data associated with the node.  */
  V *get_data ()
  {
    return m_data;
  }

private:
  /* Put node B after this node.  */
  void insert_after (fibonacci_node_t *b);

  /* Insert fibonacci node B after this node.  */
  void insert_before (fibonacci_node_t *b)
  {
    m_left->insert_after (b);
  }

  /* Parent node.  */
  fibonacci_node *m_parent;
  /* Child node.  */
  fibonacci_node *m_child;
  /* Left sibling.  */
  fibonacci_node *m_left;
  /* Right node.  */
  fibonacci_node *m_right;
  /* Key associated with node.  */
  K m_key;
  /* Data associated with node.  */
  V *m_data;

#if defined (__GNUC__) && (!defined (SIZEOF_INT) || SIZEOF_INT < 4)
  /* Degree of the node.  */
  __extension__ unsigned long int m_degree : 31;
  /* Mark of the node.  */
  __extension__ unsigned long int m_mark : 1;
#else
  /* Degree of the node.  */
  unsigned int m_degree : 31;
  /* Mark of the node.  */
  unsigned int m_mark : 1;
#endif
};

/* Fibonacci heap class. */
template<class K, class V>
class fibonacci_heap
{
  typedef fibonacci_node<K,V> fibonacci_node_t;
  friend class fibonacci_node<K,V>;

public:
  /* Default constructor.  */
  fibonacci_heap (K global_min_key): m_nodes (0), m_min (NULL), m_root (NULL),
    m_global_min_key (global_min_key)
  {
  }

  /* Destructor.  */
  ~fibonacci_heap ()
  {
    while (m_min != NULL)
      delete (extract_minimum_node ());
  }

  /* Insert new node given by KEY and DATA associated with the key.  */
  fibonacci_node_t *insert (K key, V *data);

  /* Return true if no entry is present.  */
  bool empty ()
  {
    return m_nodes == 0;
  }

  /* Return the number of nodes.  */
  size_t nodes ()
  {
    return m_nodes;
  }

  /* Return minimal key presented in the heap.  */
  K min_key ()
  {
    if (m_min == NULL)
      gcc_unreachable ();

    return m_min->m_key;
  }

  /* For given NODE, set new KEY value.  */
  K replace_key (fibonacci_node_t *node, K key)
  {
    K okey = node->m_key;

    replace_key_data (node, key, node->m_data);
    return okey;
  }

  /* For given NODE, decrease value to new KEY.  */
  K decrease_key (fibonacci_node_t *node, K key)
  {
    gcc_assert (key <= node->m_key);
    return replace_key (node, key);
  }

  /* For given NODE, set new KEY and DATA value.  */
  V *replace_key_data (fibonacci_node_t *node, K key, V *data);

  /* Extract minimum node in the heap. If RELEASE is specified,
     memory is released.  */
  V *extract_min (bool release = true);

  /* Return value associated with minimum node in the heap.  */
  V *min ()
  {
    if (m_min == NULL)
      return NULL;

    return m_min->m_data;
  }

  /* Replace data associated with NODE and replace it with DATA.  */
  V *replace_data (fibonacci_node_t *node, V *data)
  {
    return replace_key_data (node, node->m_key, data);
  }

  /* Delete NODE in the heap.  */
  V *delete_node (fibonacci_node_t *node, bool release = true);

  /* Union the heap with HEAPB.  */
  fibonacci_heap *union_with (fibonacci_heap *heapb);

private:
  /* Insert new NODE given by KEY and DATA associated with the key.  */
  fibonacci_node_t *insert (fibonacci_node_t *node, K key, V *data);

  /* Insert new NODE that has already filled key and value.  */
  fibonacci_node_t *insert_node (fibonacci_node_t *node);

  /* Insert it into the root list.  */
  void insert_root (fibonacci_node_t *node);

  /* Remove NODE from PARENT's child list.  */
  void cut (fibonacci_node_t *node, fibonacci_node_t *parent);

  /* Process cut of node Y and do it recursivelly.  */
  void cascading_cut (fibonacci_node_t *y);

  /* Extract minimum node from the heap.  */
  fibonacci_node_t * extract_minimum_node ();

  /* Remove root NODE from the heap.  */
  void remove_root (fibonacci_node_t *node);

  /* Consolidate heap.  */
  void consolidate ();

  /* Number of nodes.  */
  size_t m_nodes;
  /* Minimum node of the heap.  */
  fibonacci_node_t *m_min;
  /* Root node of the heap.  */
  fibonacci_node_t *m_root;
  /* Global minimum given in the heap construction.  */
  K m_global_min_key;
};

/* Remove fibonacci heap node.  */

template<class K, class V>
fibonacci_node<K,V> *
fibonacci_node<K,V>::remove ()
{
  fibonacci_node<K,V> *ret;

  if (this == m_left)
    ret = NULL;
  else
    ret = m_left;

  if (m_parent != NULL && m_parent->m_child == this)
    m_parent->m_child = ret;

  m_right->m_left = m_left;
  m_left->m_right = m_right;

  m_parent = NULL;
  m_left = this;
  m_right = this;

  return ret;
}

/* Link the node with PARENT.  */

template<class K, class V>
void
fibonacci_node<K,V>::link (fibonacci_node<K,V> *parent)
{
  if (parent->m_child == NULL)
    parent->m_child = this;
  else
    parent->m_child->insert_before (this);
  m_parent = parent;
  parent->m_degree++;
  m_mark = 0;
}

/* Put node B after this node.  */

template<class K, class V>
void
fibonacci_node<K,V>::insert_after (fibonacci_node<K,V> *b)
{
  fibonacci_node<K,V> *a = this;

  if (a == a->m_right)
    {
      a->m_right = b;
      a->m_left = b;
      b->m_right = a;
      b->m_left = a;
    }
  else
    {
      b->m_right = a->m_right;
      a->m_right->m_left = b;
      a->m_right = b;
      b->m_left = a;
    }
}

/* Insert new node given by KEY and DATA associated with the key.  */

template<class K, class V>
fibonacci_node<K,V>*
fibonacci_heap<K,V>::insert (K key, V *data)
{
  /* Create the new node.  */
  fibonacci_node<K,V> *node = new fibonacci_node_t (key, data);

  return insert_node (node);
}

/* Insert new NODE given by DATA associated with the key.  */

template<class K, class V>
fibonacci_node<K,V>*
fibonacci_heap<K,V>::insert (fibonacci_node_t *node, K key, V *data)
{
  /* Set the node's data.  */
  node->m_data = data;
  node->m_key = key;

  return insert_node (node);
}

/* Insert new NODE that has already filled key and value.  */

template<class K, class V>
fibonacci_node<K,V>*
fibonacci_heap<K,V>::insert_node (fibonacci_node_t *node)
{
  /* Insert it into the root list.  */
  insert_root (node);

  /* If their was no minimum, or this key is less than the min,
     it's the new min.  */
  if (m_min == NULL || node->m_key < m_min->m_key)
    m_min = node;

  m_nodes++;

  return node;
}

/* For given NODE, set new KEY and DATA value.  */

template<class K, class V>
V*
fibonacci_heap<K,V>::replace_key_data (fibonacci_node<K,V> *node, K key,
				       V *data)
{
  K okey;
  fibonacci_node<K,V> *y;
  V *odata = node->m_data;

  /* If we wanted to, we do a real increase by redeleting and
     inserting.  */
  if (node->compare_data (key) > 0)
    {
      delete_node (node, false);

      node = new (node) fibonacci_node_t ();
      insert (node, key, data);

      return odata;
    }

  okey = node->m_key;
  node->m_data = data;
  node->m_key = key;
  y = node->m_parent;

  /* Short-circuit if the key is the same, as we then don't have to
     do anything.  Except if we're trying to force the new node to
     be the new minimum for delete.  */
  if (okey == key && okey != m_global_min_key)
    return odata;

  /* These two compares are specifically <= 0 to make sure that in the case
     of equality, a node we replaced the data on, becomes the new min.  This
     is needed so that delete's call to extractmin gets the right node.  */
  if (y != NULL && node->compare (y) <= 0)
    {
      cut (node, y);
      cascading_cut (y);
    }

  if (node->compare (m_min) <= 0)
    m_min = node;

  return odata;
}

/* Extract minimum node in the heap.  Delete fibonacci node if RELEASE
   is true.  */

template<class K, class V>
V*
fibonacci_heap<K,V>::extract_min (bool release)
{
  fibonacci_node<K,V> *z;
  V *ret = NULL;

  /* If we don't have a min set, it means we have no nodes.  */
  if (m_min != NULL)
    {
      /* Otherwise, extract the min node, free the node, and return the
       node's data.  */
      z = extract_minimum_node ();
      ret = z->m_data;

      if (release)
        delete (z);
    }

  return ret;
}

/* Delete NODE in the heap, if RELEASE is specified memory is released.  */

template<class K, class V>
V*
fibonacci_heap<K,V>::delete_node (fibonacci_node<K,V> *node, bool release)
{
  V *ret = node->m_data;

  /* To perform delete, we just make it the min key, and extract.  */
  replace_key (node, m_global_min_key);
  if (node != m_min)
    {
      fprintf (stderr, "Can't force minimum on fibheap.\n");
      abort ();
    }
  extract_min (release);

  return ret;
}

/* Union the heap with HEAPB.  One of the heaps is going to be deleted.  */

template<class K, class V>
fibonacci_heap<K,V>*
fibonacci_heap<K,V>::union_with (fibonacci_heap<K,V> *heapb)
{
  fibonacci_heap<K,V> *heapa = this;

  fibonacci_node<K,V> *a_root, *b_root;

  /* If one of the heaps is empty, the union is just the other heap.  */
  if ((a_root = heapa->m_root) == NULL)
    {
      delete (heapa);
      return heapb;
    }
  if ((b_root = heapb->m_root) == NULL)
    {
      delete (heapb);
      return heapa;
    }

  /* Merge them to the next nodes on the opposite chain.  */
  a_root->m_left->m_right = b_root;
  b_root->m_left->m_right = a_root;
  std::swap (a_root->m_left, b_root->m_left);
  heapa->m_nodes += heapb->m_nodes;

  /* And set the new minimum, if it's changed.  */
  if (heapb->m_min->compare (heapa->m_min) < 0)
    heapa->m_min = heapb->m_min;

  /* Set m_min to NULL to not to delete live fibonacci nodes.  */
  heapb->m_min = NULL;
  delete (heapb);

  return heapa;
}

/* Insert it into the root list.  */

template<class K, class V>
void
fibonacci_heap<K,V>::insert_root (fibonacci_node_t *node)
{
  /* If the heap is currently empty, the new node becomes the singleton
     circular root list.  */
  if (m_root == NULL)
    {
      m_root = node;
      node->m_left = node;
      node->m_right = node;
      return;
    }

  /* Otherwise, insert it in the circular root list between the root
     and it's right node.  */
  m_root->insert_after (node);
}

/* Remove NODE from PARENT's child list.  */

template<class K, class V>
void
fibonacci_heap<K,V>::cut (fibonacci_node<K,V> *node,
			  fibonacci_node<K,V> *parent)
{
  node->remove ();
  parent->m_degree--;
  insert_root (node);
  node->m_parent = NULL;
  node->m_mark = 0;
}

/* Process cut of node Y and do it recursivelly.  */

template<class K, class V>
void
fibonacci_heap<K,V>::cascading_cut (fibonacci_node<K,V> *y)
{
  fibonacci_node<K,V> *z;

  while ((z = y->m_parent) != NULL)
    {
      if (y->m_mark == 0)
	{
	  y->m_mark = 1;
	  return;
	}
      else
	{
	  cut (y, z);
	  y = z;
	}
    }
}

/* Extract minimum node from the heap.  */

template<class K, class V>
fibonacci_node<K,V>*
fibonacci_heap<K,V>::extract_minimum_node ()
{
  fibonacci_node<K,V> *ret = m_min;
  fibonacci_node<K,V> *x, *y, *orig;

  /* Attach the child list of the minimum node to the root list of the heap.
     If there is no child list, we don't do squat.  */
  for (x = ret->m_child, orig = NULL; x != orig && x != NULL; x = y)
    {
      if (orig == NULL)
	orig = x;
      y = x->m_right;
      x->m_parent = NULL;
      insert_root (x);
    }

  /* Remove the old root.  */
  remove_root (ret);
  m_nodes--;

  /* If we are left with no nodes, then the min is NULL.  */
  if (m_nodes == 0)
    m_min = NULL;
  else
    {
      /* Otherwise, consolidate to find new minimum, as well as do the reorg
       work that needs to be done.  */
      m_min = ret->m_right;
      consolidate ();
    }

  return ret;
}

/* Remove root NODE from the heap.  */

template<class K, class V>
void
fibonacci_heap<K,V>::remove_root (fibonacci_node<K,V> *node)
{
  if (node->m_left == node)
    m_root = NULL;
  else
    m_root = node->remove ();
}

/* Consolidate heap.  */

template<class K, class V>
void fibonacci_heap<K,V>::consolidate ()
{
  int D = 1 + 8 * sizeof (long);
  auto_vec<fibonacci_node<K,V> *> a (D);
  a.safe_grow_cleared (D);
  fibonacci_node<K,V> *w, *x, *y;
  int i, d;

  while ((w = m_root) != NULL)
    {
      x = w;
      remove_root (w);
      d = x->m_degree;
      while (a[d] != NULL)
	{
	  y = a[d];
	  if (x->compare (y) > 0)
	    std::swap (x, y);
	  y->link (x);
	  a[d] = NULL;
	  d++;
	}
      a[d] = x;
    }
  m_min = NULL;
  for (i = 0; i < D; i++)
    if (a[i] != NULL)
      {
	insert_root (a[i]);
	if (m_min == NULL || a[i]->compare (m_min) < 0)
	  m_min = a[i];
      }
}

#endif  // GCC_FIBONACCI_HEAP_H
