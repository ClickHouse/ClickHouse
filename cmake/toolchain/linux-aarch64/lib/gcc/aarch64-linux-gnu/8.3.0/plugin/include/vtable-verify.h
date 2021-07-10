/* Copyright (C) 2013-2018 Free Software Foundation, Inc.

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

/* Virtual Table Pointer Security.  */

#ifndef VTABLE_VERIFY_H
#define VTABLE_VERIFY_H

#include "sbitmap.h"

/* The function decl used to create calls to __VLTVtableVerify.  It must
   be global because it needs to be initialized in the C++ front end, but
   used in the middle end (in the vtable verification pass).  */

extern tree verify_vtbl_ptr_fndecl;

/* Global variable keeping track of how many vtable map variables we
   have created. */
extern unsigned num_vtable_map_nodes;

/* Keep track of how many virtual calls we are actually verifying.  */
extern int total_num_virtual_calls;
extern int total_num_verified_vcalls;

/* Each vtable map variable corresponds to a virtual class.  Each
   vtable map variable has a hash table associated with it, that keeps
   track of the vtable pointers for which we have generated a call to
   __VLTRegisterPair (with the current vtable map variable).  This is
   the hash table node that is used for each entry in this hash table
   of vtable pointers.

   Sometimes there are multiple valid vtable pointer entries that use
   the same vtable pointer decl with different offsets.  Therefore,
   for each vtable pointer in the hash table, there is also an array
   of offsets used with that vtable. */

struct vtable_registration
{
  tree vtable_decl;            /* The var decl of the vtable.               */
  vec<unsigned> offsets;       /* The offsets array.                        */
};

struct registration_hasher : nofree_ptr_hash <struct vtable_registration>
{
  static inline hashval_t hash (const vtable_registration *);
  static inline bool equal (const vtable_registration *,
			    const vtable_registration *);
};

typedef hash_table<registration_hasher> register_table_type;
typedef register_table_type::iterator registration_iterator_type;

/*  This struct is used to represent the class hierarchy information
    that we need.  Each vtable map variable has an associated class
    hierarchy node (struct vtv_graph_node).  Note: In this struct,
    'children' means immediate descendants in the class hierarchy;
    'descendant' means any descendant however many levels deep. */

struct vtv_graph_node {
  tree class_type;                  /* The record_type of the class.        */
  unsigned class_uid;               /* A unique, monotonically
                                       ascending id for class node.
                                       Each vtable map node also has
                                       an id.  The class uid is the
                                       same as the vtable map node id
                                       for nodes corresponding to the
                                       same class.                          */
  unsigned num_processed_children;  /* # of children for whom we have
                                       computed the class hierarchy
                                       transitive closure.                  */
  vec<struct vtv_graph_node *> parents;  /* Vector of parents in the graph. */
  vec<struct vtv_graph_node *> children; /* Vector of children in the graph.*/
  sbitmap descendants;              /* Bitmap representing all this node's
                                       descendants in the graph.            */
};

/* This is the node used for our hashtable of vtable map variable
   information.  When we create a vtable map variable (var decl) we
   put it into one of these nodes; create a corresponding
   vtv_graph_node for our class hierarchy info and store that in this
   node; generate a unique (monotonically ascending) id for both the
   vtbl_map_node and the vtv_graph_node; and insert the node into two
   data structures (to make it easy to find in several different
   ways): 1). A hash table ("vtbl_map_hash" in vtable-verify.c).
   This gives us an easy way to check to see if we already have a node
   for the vtable map variable or not; and 2). An array (vector) of
   vtbl_map_nodes, where the array index corresponds to the unique id
   of the vtbl_map_node, which gives us an easy way to use bitmaps to
   represent and find the vtable map nodes.  */

struct vtbl_map_node {
  tree vtbl_map_decl;                 /* The var decl for the vtable map
                                         variable.                          */
  tree class_name;                    /* The DECL_ASSEMBLER_NAME of the
                                         class.                             */
  struct vtv_graph_node *class_info;  /* Our class hierarchy info for the
                                         class.                             */
  unsigned uid;                       /* The unique id for the vtable map
                                         variable.                          */
  struct vtbl_map_node *next, *prev;  /* Pointers for the linked list
                                         structure.                         */
  register_table_type *registered;     /* Hashtable of vtable pointers for which
                                         we have generated a _VLTRegisterPair
                                         call with this vtable map variable. */
  bool is_used;          /* Boolean indicating if we used this vtable map
                            variable in a call to __VLTVerifyVtablePointer. */
};

/* Controls debugging for vtable verification.  */
extern bool vtv_debug;

/* The global vector of vtbl_map_nodes.  */
extern vec<struct vtbl_map_node *> vtbl_map_nodes_vec;

/*  The global vectors for mangled class names for anonymous classes.  */
extern GTY(()) vec<tree, va_gc> *vtbl_mangled_name_types;
extern GTY(()) vec<tree, va_gc> *vtbl_mangled_name_ids;

extern void vtbl_register_mangled_name (tree, tree);
extern struct vtbl_map_node *vtbl_map_get_node (tree);
extern struct vtbl_map_node *find_or_create_vtbl_map_node (tree);
extern void vtbl_map_node_class_insert (struct vtbl_map_node *, unsigned);
extern bool vtbl_map_node_registration_find (struct vtbl_map_node *,
                                             tree, unsigned);
extern bool vtbl_map_node_registration_insert (struct vtbl_map_node *,
                                               tree, unsigned);

#endif /* VTABLE_VERIFY_H */
