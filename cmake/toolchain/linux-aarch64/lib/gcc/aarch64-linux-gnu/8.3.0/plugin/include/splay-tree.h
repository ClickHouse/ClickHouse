/* A splay-tree datatype.  
   Copyright (C) 1998-2018 Free Software Foundation, Inc.
   Contributed by Mark Mitchell (mark@markmitchell.com).

   This file is part of GCC.
   
   GCC is free software; you can redistribute it and/or modify it
   under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2, or (at your option)
   any later version.

   GCC is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with GCC; see the file COPYING.  If not, write to
   the Free Software Foundation, 51 Franklin Street - Fifth Floor,
   Boston, MA 02110-1301, USA.  */

/* For an easily readable description of splay-trees, see:

     Lewis, Harry R. and Denenberg, Larry.  Data Structures and Their
     Algorithms.  Harper-Collins, Inc.  1991.  

   The major feature of splay trees is that all basic tree operations
   are amortized O(log n) time for a tree with n nodes.  */

#ifndef _SPLAY_TREE_H
#define _SPLAY_TREE_H

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

#include "ansidecl.h"

#ifdef HAVE_STDINT_H
#include <stdint.h>
#endif
#ifdef HAVE_INTTYPES_H
#include <inttypes.h>
#endif

/* Use typedefs for the key and data types to facilitate changing
   these types, if necessary.  These types should be sufficiently wide
   that any pointer or scalar can be cast to these types, and then
   cast back, without loss of precision.  */
typedef uintptr_t splay_tree_key;
typedef uintptr_t splay_tree_value;

/* Forward declaration for a node in the tree.  */
typedef struct splay_tree_node_s *splay_tree_node;

/* The type of a function which compares two splay-tree keys.  The
   function should return values as for qsort.  */
typedef int (*splay_tree_compare_fn) (splay_tree_key, splay_tree_key);

/* The type of a function used to deallocate any resources associated
   with the key.  */
typedef void (*splay_tree_delete_key_fn) (splay_tree_key);

/* The type of a function used to deallocate any resources associated
   with the value.  */
typedef void (*splay_tree_delete_value_fn) (splay_tree_value);

/* The type of a function used to iterate over the tree.  */
typedef int (*splay_tree_foreach_fn) (splay_tree_node, void*);

/* The type of a function used to allocate memory for tree root and
   node structures.  The first argument is the number of bytes needed;
   the second is a data pointer the splay tree functions pass through
   to the allocator.  This function must never return zero.  */
typedef void *(*splay_tree_allocate_fn) (int, void *);

/* The type of a function used to free memory allocated using the
   corresponding splay_tree_allocate_fn.  The first argument is the
   memory to be freed; the latter is a data pointer the splay tree
   functions pass through to the freer.  */
typedef void (*splay_tree_deallocate_fn) (void *, void *);

/* The nodes in the splay tree.  */
struct splay_tree_node_s {
  /* The key.  */
  splay_tree_key key;

  /* The value.  */
  splay_tree_value value;

  /* The left and right children, respectively.  */
  splay_tree_node left;
  splay_tree_node right;
};

/* The splay tree itself.  */
struct splay_tree_s {
  /* The root of the tree.  */
  splay_tree_node root;

  /* The comparision function.  */
  splay_tree_compare_fn comp;

  /* The deallocate-key function.  NULL if no cleanup is necessary.  */
  splay_tree_delete_key_fn delete_key;

  /* The deallocate-value function.  NULL if no cleanup is necessary.  */
  splay_tree_delete_value_fn delete_value;

  /* Node allocate function.  Takes allocate_data as a parameter. */
  splay_tree_allocate_fn allocate;

  /* Free function for nodes and trees.  Takes allocate_data as a parameter.  */
  splay_tree_deallocate_fn deallocate;

  /* Parameter for allocate/free functions.  */
  void *allocate_data;
};

typedef struct splay_tree_s *splay_tree;

extern splay_tree splay_tree_new (splay_tree_compare_fn,
				  splay_tree_delete_key_fn,
				  splay_tree_delete_value_fn);
extern splay_tree splay_tree_new_with_allocator (splay_tree_compare_fn,
						 splay_tree_delete_key_fn,
						 splay_tree_delete_value_fn,
						 splay_tree_allocate_fn,
						 splay_tree_deallocate_fn,
						 void *);
extern splay_tree splay_tree_new_typed_alloc (splay_tree_compare_fn,
					      splay_tree_delete_key_fn,
					      splay_tree_delete_value_fn,
					      splay_tree_allocate_fn,
					      splay_tree_allocate_fn,
					      splay_tree_deallocate_fn,
					      void *);
extern void splay_tree_delete (splay_tree);
extern splay_tree_node splay_tree_insert (splay_tree,
					  splay_tree_key,
					  splay_tree_value);
extern void splay_tree_remove	(splay_tree, splay_tree_key);
extern splay_tree_node splay_tree_lookup (splay_tree, splay_tree_key);
extern splay_tree_node splay_tree_predecessor (splay_tree, splay_tree_key);
extern splay_tree_node splay_tree_successor (splay_tree, splay_tree_key);
extern splay_tree_node splay_tree_max (splay_tree);
extern splay_tree_node splay_tree_min (splay_tree);
extern int splay_tree_foreach (splay_tree, splay_tree_foreach_fn, void*);
extern int splay_tree_compare_ints (splay_tree_key, splay_tree_key);
extern int splay_tree_compare_pointers (splay_tree_key,	splay_tree_key);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* _SPLAY_TREE_H */
