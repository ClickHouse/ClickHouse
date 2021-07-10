/* Utilities for ipa analysis.
   Copyright (C) 2004-2018 Free Software Foundation, Inc.
   Contributed by Kenneth Zadeck <zadeck@naturalbridge.com>

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

#ifndef GCC_IPA_UTILS_H
#define GCC_IPA_UTILS_H

struct ipa_dfs_info {
  int dfn_number;
  int low_link;
  /* This field will have the samy value for any two nodes in the same strongly
     connected component.  */
  int scc_no;
  bool new_node;
  bool on_stack;
  struct cgraph_node* next_cycle;
  PTR aux;
};


/* In ipa-utils.c  */
void ipa_print_order (FILE*, const char *, struct cgraph_node**, int);
int ipa_reduced_postorder (struct cgraph_node **, bool,
			  bool (*ignore_edge) (struct cgraph_edge *));
void ipa_free_postorder_info (void);
vec<cgraph_node *> ipa_get_nodes_in_cycle (struct cgraph_node *);
bool ipa_edge_within_scc (struct cgraph_edge *);
int ipa_reverse_postorder (struct cgraph_node **);
tree get_base_var (tree);
void ipa_merge_profiles (struct cgraph_node *dst,
			 struct cgraph_node *src, bool preserve_body = false);
bool recursive_call_p (tree, tree);

/* In ipa-profile.c  */
bool ipa_propagate_frequency (struct cgraph_node *node);

/* In ipa-devirt.c  */

struct odr_type_d;
typedef odr_type_d *odr_type;
void build_type_inheritance_graph (void);
void rebuild_type_inheritance_graph (void);
void update_type_inheritance_graph (void);
vec <cgraph_node *>
possible_polymorphic_call_targets (tree, HOST_WIDE_INT,
				   ipa_polymorphic_call_context,
				   bool *copletep = NULL,
				   void **cache_token = NULL,
				   bool speuclative = false);
odr_type get_odr_type (tree, bool insert = false);
bool odr_type_p (const_tree);
bool possible_polymorphic_call_target_p (tree ref, gimple *stmt, struct cgraph_node *n);
void dump_possible_polymorphic_call_targets (FILE *, tree, HOST_WIDE_INT,
					     const ipa_polymorphic_call_context &);
bool possible_polymorphic_call_target_p (tree, HOST_WIDE_INT,
				         const ipa_polymorphic_call_context &,
					 struct cgraph_node *);
tree polymorphic_ctor_dtor_p (tree, bool);
tree inlined_polymorphic_ctor_dtor_block_p (tree, bool);
bool decl_maybe_in_construction_p (tree, tree, gimple *, tree);
tree vtable_pointer_value_to_binfo (const_tree);
bool vtable_pointer_value_to_vtable (const_tree, tree *, unsigned HOST_WIDE_INT *);
tree subbinfo_with_vtable_at_offset (tree, unsigned HOST_WIDE_INT, tree);
void compare_virtual_tables (varpool_node *, varpool_node *);
bool type_all_derivations_known_p (const_tree);
bool type_known_to_have_no_derivations_p (tree);
bool contains_polymorphic_type_p (const_tree);
void register_odr_type (tree);
bool types_must_be_same_for_odr (tree, tree);
bool types_odr_comparable (tree, tree, bool strict = false);
cgraph_node *try_speculative_devirtualization (tree, HOST_WIDE_INT,
					       ipa_polymorphic_call_context);
void warn_types_mismatch (tree t1, tree t2, location_t loc1 = UNKNOWN_LOCATION,
			  location_t loc2 = UNKNOWN_LOCATION);
bool odr_or_derived_type_p (const_tree t);
bool odr_types_equivalent_p (tree type1, tree type2);

/* Return vector containing possible targets of polymorphic call E.
   If COMPLETEP is non-NULL, store true if the list is complete. 
   CACHE_TOKEN (if non-NULL) will get stored to an unique ID of entry
   in the target cache.  If user needs to visit every target list
   just once, it can memoize them.

   Returned vector is placed into cache.  It is NOT caller's responsibility
   to free it.  The vector can be freed on cgraph_remove_node call if
   the particular node is a virtual function present in the cache.  */

inline vec <cgraph_node *>
possible_polymorphic_call_targets (struct cgraph_edge *e,
				   bool *completep = NULL,
				   void **cache_token = NULL,
				   bool speculative = false)
{
  ipa_polymorphic_call_context context(e);

  return possible_polymorphic_call_targets (e->indirect_info->otr_type,
					    e->indirect_info->otr_token,
					    context,
					    completep, cache_token,
					    speculative);
}

/* Same as above but taking OBJ_TYPE_REF as an parameter.  */

inline vec <cgraph_node *>
possible_polymorphic_call_targets (tree ref,
				   gimple *call,
				   bool *completep = NULL,
				   void **cache_token = NULL)
{
  ipa_polymorphic_call_context context (current_function_decl, ref, call);

  return possible_polymorphic_call_targets (obj_type_ref_class (ref),
					    tree_to_uhwi
					      (OBJ_TYPE_REF_TOKEN (ref)),
					    context,
					    completep, cache_token);
}

/* Dump possible targets of a polymorphic call E into F.  */

inline void
dump_possible_polymorphic_call_targets (FILE *f, struct cgraph_edge *e)
{
  ipa_polymorphic_call_context context(e);

  dump_possible_polymorphic_call_targets (f, e->indirect_info->otr_type,
					  e->indirect_info->otr_token,
					  context);
}

/* Return true if N can be possibly target of a polymorphic call of
   E.  */

inline bool
possible_polymorphic_call_target_p (struct cgraph_edge *e,
				    struct cgraph_node *n)
{
  ipa_polymorphic_call_context context(e);

  return possible_polymorphic_call_target_p (e->indirect_info->otr_type,
					     e->indirect_info->otr_token,
					     context, n);
}

/* Return true if BINFO corresponds to a type with virtual methods. 

   Every type has several BINFOs.  One is the BINFO associated by the type
   while other represents bases of derived types.  The BINFOs representing
   bases do not have BINFO_VTABLE pointer set when this is the single
   inheritance (because vtables are shared).  Look up the BINFO of type
   and check presence of its vtable.  */

inline bool
polymorphic_type_binfo_p (const_tree binfo)
{
  return (BINFO_TYPE (binfo) && TYPE_BINFO (BINFO_TYPE (binfo))
	  && BINFO_VTABLE (TYPE_BINFO (BINFO_TYPE (binfo))));
}

/* Return true if T is a type with linkage defined.  */

inline bool
type_with_linkage_p (const_tree t)
{
  if (!TYPE_NAME (t) || TREE_CODE (TYPE_NAME (t)) != TYPE_DECL
      || !TYPE_STUB_DECL (t))
    return false;
  /* In LTO do not get confused by non-C++ produced types or types built
     with -fno-lto-odr-type-merigng.  */
  if (in_lto_p)
    {
      /* To support -fno-lto-odr-type-merigng recognize types with vtables
         to have linkage.  */
      if (RECORD_OR_UNION_TYPE_P (t)
	  && TYPE_BINFO (t) && BINFO_VTABLE (TYPE_BINFO (t)))
        return true;
      /* With -flto-odr-type-merging C++ FE specify mangled names
	 for all types with the linkage.  */
      return DECL_ASSEMBLER_NAME_SET_P (TYPE_NAME (t));
    }

  if (!RECORD_OR_UNION_TYPE_P (t) && TREE_CODE (t) != ENUMERAL_TYPE)
    return false;

  /* Builtin types do not define linkage, their TYPE_CONTEXT is NULL.  */
  if (!TYPE_CONTEXT (t))
    return false;

  return true;
}

/* Return true if T is in anonymous namespace.
   This works only on those C++ types with linkage defined.  */

inline bool
type_in_anonymous_namespace_p (const_tree t)
{
  gcc_checking_assert (type_with_linkage_p (t));

  if (!TREE_PUBLIC (TYPE_STUB_DECL (t)))
    {
      /* C++ FE uses magic <anon> as assembler names of anonymous types.
 	 verify that this match with type_in_anonymous_namespace_p.  */
      gcc_checking_assert (!in_lto_p
			   || !DECL_ASSEMBLER_NAME_SET_P (TYPE_NAME (t))
			   || !strcmp ("<anon>",
				       IDENTIFIER_POINTER
				       (DECL_ASSEMBLER_NAME (TYPE_NAME (t)))));
      return true;
    }
  return false;
}

/* Return true of T is type with One Definition Rule info attached. 
   It means that either it is anonymous type or it has assembler name
   set.  */

inline bool
odr_type_p (const_tree t)
{
  /* We do not have this information when not in LTO, but we do not need
     to care, since it is used only for type merging.  */
  gcc_checking_assert (in_lto_p || flag_lto);

  if (!type_with_linkage_p (t))
    return false;

  /* To support -fno-lto-odr-type-merging consider types with vtables ODR.  */
  if (type_in_anonymous_namespace_p (t))
    return true;

  if (TYPE_NAME (t) && DECL_ASSEMBLER_NAME_SET_P (TYPE_NAME (t)))
    {
      /* C++ FE uses magic <anon> as assembler names of anonymous types.
 	 verify that this match with type_in_anonymous_namespace_p.  */
      gcc_checking_assert (strcmp ("<anon>",
				   IDENTIFIER_POINTER
				   (DECL_ASSEMBLER_NAME (TYPE_NAME (t)))));
      return true;
    }
  return false;
}

#endif  /* GCC_IPA_UTILS_H  */


