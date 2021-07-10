/* Declarations for C++ name lookup routines.
   Copyright (C) 2003-2018 Free Software Foundation, Inc.
   Contributed by Gabriel Dos Reis <gdr@integrable-solutions.net>

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

#ifndef GCC_CP_NAME_LOOKUP_H
#define GCC_CP_NAME_LOOKUP_H

#include "c-family/c-common.h"

/* The type of dictionary used to map names to types declared at
   a given scope.  */
typedef struct binding_table_s *binding_table;
typedef struct binding_entry_s *binding_entry;

/* The type of a routine repeatedly called by binding_table_foreach.  */
typedef void (*bt_foreach_proc) (binding_entry, void *);

struct GTY(()) binding_entry_s {
  binding_entry chain;
  tree name;
  tree type;
};

/* These macros indicate the initial chains count for binding_table.  */
#define SCOPE_DEFAULT_HT_SIZE		(1 << 3)
#define CLASS_SCOPE_HT_SIZE		(1 << 3)
#define NAMESPACE_ORDINARY_HT_SIZE	(1 << 5)
#define NAMESPACE_STD_HT_SIZE		(1 << 8)
#define GLOBAL_SCOPE_HT_SIZE		(1 << 8)

extern void binding_table_foreach (binding_table, bt_foreach_proc, void *);
extern binding_entry binding_table_find (binding_table, tree);

/* Datatype that represents binding established by a declaration between
   a name and a C++ entity.  */
typedef struct cxx_binding cxx_binding;

/* The datatype used to implement C++ scope.  */
typedef struct cp_binding_level cp_binding_level;

/* Nonzero if this binding is for a local scope, as opposed to a class
   or namespace scope.  */
#define LOCAL_BINDING_P(NODE) ((NODE)->is_local)

/* True if NODE->value is from a base class of the class which is
   currently being defined.  */
#define INHERITED_VALUE_BINDING_P(NODE) ((NODE)->value_is_inherited)

struct GTY(()) cxx_binding {
  /* Link to chain together various bindings for this name.  */
  cxx_binding *previous;
  /* The non-type entity this name is bound to.  */
  tree value;
  /* The type entity this name is bound to.  */
  tree type;
  /* The scope at which this binding was made.  */
  cp_binding_level *scope;
  unsigned value_is_inherited : 1;
  unsigned is_local : 1;
};

/* Datatype used to temporarily save C++ bindings (for implicit
   instantiations purposes and like).  Implemented in decl.c.  */
struct GTY(()) cxx_saved_binding {
  /* The name of the current binding.  */
  tree identifier;
  /* The binding we're saving.  */
  cxx_binding *binding;
  tree real_type_value;
};


extern tree identifier_type_value (tree);
extern void set_identifier_type_value (tree, tree);
extern void push_binding (tree, tree, cp_binding_level*);
extern void pop_local_binding (tree, tree);
extern void pop_bindings_and_leave_scope (void);
extern tree constructor_name (tree);
extern bool constructor_name_p (tree, tree);

/* The kinds of scopes we recognize.  */
enum scope_kind {
  sk_block = 0,      /* An ordinary block scope.  This enumerator must
			have the value zero because "cp_binding_level"
			is initialized by using "memset" to set the
			contents to zero, and the default scope kind
			is "sk_block".  */
  sk_cleanup,	     /* A scope for (pseudo-)scope for cleanup.  It is
			pseudo in that it is transparent to name lookup
			activities.  */
  sk_try,	     /* A try-block.  */
  sk_catch,	     /* A catch-block.  */
  sk_for,	     /* The scope of the variable declared in a
			init-statement.  */
  sk_cond,	     /* The scope of the variable declared in the condition
			of an if or switch statement.  */
  sk_function_parms, /* The scope containing function parameters.  */
  sk_class,	     /* The scope containing the members of a class.  */
  sk_scoped_enum,    /* The scope containing the enumertors of a C++0x
                        scoped enumeration.  */
  sk_namespace,	     /* The scope containing the members of a
			namespace, including the global scope.  */
  sk_template_parms, /* A scope for template parameters.  */
  sk_template_spec,  /* Like sk_template_parms, but for an explicit
			specialization.  Since, by definition, an
			explicit specialization is introduced by
			"template <>", this scope is always empty.  */
  sk_transaction,    /* A synchronized or atomic statement.  */
  sk_omp	     /* An OpenMP structured block.  */
};

/* The scope where the class/struct/union/enum tag applies.  */
enum tag_scope {
  ts_current = 0,	/* Current scope only.  This is for the
			     class-key identifier;
			   case mentioned in [basic.lookup.elab]/2,
			   or the class/enum definition
			     class-key identifier { ... };  */
  ts_global = 1,	/* All scopes.  This is the 3.4.1
			   [basic.lookup.unqual] lookup mentioned
			   in [basic.lookup.elab]/2.  */
  ts_within_enclosing_non_class = 2,	/* Search within enclosing non-class
					   only, for friend class lookup
					   according to [namespace.memdef]/3
					   and [class.friend]/9.  */
  ts_lambda = 3			/* Declaring a lambda closure.  */
};

struct GTY(()) cp_class_binding {
  cxx_binding *base;
  /* The bound name.  */
  tree identifier;
};

/* For each binding contour we allocate a binding_level structure
   which records the names defined in that contour.
   Contours include:
    0) the global one
    1) one for each function definition,
       where internal declarations of the parameters appear.
    2) one for each compound statement,
       to record its declarations.

   The current meaning of a name can be found by searching the levels
   from the current one out to the global one.

   Off to the side, may be the class_binding_level.  This exists only
   to catch class-local declarations.  It is otherwise nonexistent.

   Also there may be binding levels that catch cleanups that must be
   run when exceptions occur.  Thus, to see whether a name is bound in
   the current scope, it is not enough to look in the
   CURRENT_BINDING_LEVEL.  You should use lookup_name_current_level
   instead.  */

struct GTY(()) cp_binding_level {
  /* A chain of _DECL nodes for all variables, constants, functions,
      and typedef types.  These are in the reverse of the order
      supplied.  There may be OVERLOADs on this list, too, but they
      are wrapped in TREE_LISTs; the TREE_VALUE is the OVERLOAD.  */
  tree names;

  /* A list of USING_DECL nodes.  */
  tree usings;

  /* Using directives.  */
  vec<tree, va_gc> *using_directives;

  /* For the binding level corresponding to a class, the entities
      declared in the class or its base classes.  */
  vec<cp_class_binding, va_gc> *class_shadowed;

  /* Similar to class_shadowed, but for IDENTIFIER_TYPE_VALUE, and
      is used for all binding levels. The TREE_PURPOSE is the name of
      the entity, the TREE_TYPE is the associated type.  In addition
      the TREE_VALUE is the IDENTIFIER_TYPE_VALUE before we entered
      the class.  */
  tree type_shadowed;

  /* For each level (except not the global one),
      a chain of BLOCK nodes for all the levels
      that were entered and exited one level down.  */
  tree blocks;

  /* The entity (namespace, class, function) the scope of which this
      binding contour corresponds to.  Otherwise NULL.  */
  tree this_entity;

  /* The binding level which this one is contained in (inherits from).  */
  cp_binding_level *level_chain;

  /* List of VAR_DECLS saved from a previous for statement.
      These would be dead in ISO-conforming code, but might
      be referenced in ARM-era code.  */
  vec<tree, va_gc> *dead_vars_from_for;

  /* STATEMENT_LIST for statements in this binding contour.
      Only used at present for SK_CLEANUP temporary bindings.  */
  tree statement_list;

  /* Binding depth at which this level began.  */
  int binding_depth;

  /* The kind of scope that this object represents.  However, a
      SK_TEMPLATE_SPEC scope is represented with KIND set to
      SK_TEMPLATE_PARMS and EXPLICIT_SPEC_P set to true.  */
  ENUM_BITFIELD (scope_kind) kind : 4;

  /* True if this scope is an SK_TEMPLATE_SPEC scope.  This field is
      only valid if KIND == SK_TEMPLATE_PARMS.  */
  BOOL_BITFIELD explicit_spec_p : 1;

  /* true means make a BLOCK for this level regardless of all else.  */
  unsigned keep : 1;

  /* Nonzero if this level can safely have additional
      cleanup-needing variables added to it.  */
  unsigned more_cleanups_ok : 1;
  unsigned have_cleanups : 1;

  /* Transient state set if this scope is of sk_class kind
     and is in the process of defining 'this_entity'.  Reset
     on leaving the class definition to allow for the scope
     to be subsequently re-used as a non-defining scope for
     'this_entity'.  */
  unsigned defining_class_p : 1;

  /* 23 bits left to fill a 32-bit word.  */
};

/* The binding level currently in effect.  */

#define current_binding_level			\
  (*(cfun && cp_function_chain && cp_function_chain->bindings \
   ? &cp_function_chain->bindings		\
   : &scope_chain->bindings))

/* The binding level of the current class, if any.  */

#define class_binding_level scope_chain->class_bindings

/* True if SCOPE designates the global scope binding contour.  */
#define global_scope_p(SCOPE) \
  ((SCOPE) == NAMESPACE_LEVEL (global_namespace))

extern cp_binding_level *leave_scope (void);
extern bool kept_level_p (void);
extern bool global_bindings_p (void);
extern bool toplevel_bindings_p (void);
extern bool namespace_bindings_p (void);
extern bool local_bindings_p (void);
extern bool template_parm_scope_p (void);
extern scope_kind innermost_scope_kind (void);
extern cp_binding_level *begin_scope (scope_kind, tree);
extern void print_binding_stack	(void);
extern void pop_everything (void);
extern void keep_next_level (bool);
extern bool is_ancestor (tree ancestor, tree descendant);
extern bool is_nested_namespace (tree parent, tree descendant,
				 bool inline_only = false);
extern tree push_scope (tree);
extern void pop_scope (tree);
extern tree push_inner_scope (tree);
extern void pop_inner_scope (tree, tree);
extern void push_binding_level (cp_binding_level *);

extern bool handle_namespace_attrs (tree, tree);
extern void pushlevel_class (void);
extern void poplevel_class (void);
extern tree lookup_name_prefer_type (tree, int);
extern tree lookup_name_real (tree, int, int, bool, int, int);
extern tree lookup_type_scope (tree, tag_scope);
extern tree get_namespace_binding (tree ns, tree id);
extern void set_global_binding (tree decl);
inline tree get_global_binding (tree id)
{
  return get_namespace_binding (NULL_TREE, id);
}
extern tree lookup_qualified_name (tree, tree, int, bool, /*hidden*/bool = false);
extern tree lookup_name_nonclass (tree);
extern bool is_local_extern (tree);
extern bool pushdecl_class_level (tree);
extern tree pushdecl_namespace_level (tree, bool);
extern bool push_class_level_binding (tree, tree);
extern tree get_local_decls ();
extern int function_parm_depth (void);
extern tree cp_namespace_decls (tree);
extern void set_decl_namespace (tree, tree, bool);
extern void push_decl_namespace (tree);
extern void pop_decl_namespace (void);
extern void do_namespace_alias (tree, tree);
extern tree do_class_using_decl (tree, tree);
extern tree lookup_arg_dependent (tree, tree, vec<tree, va_gc> *);
extern tree search_anon_aggr (tree, tree, bool = false);
extern tree get_class_binding_direct (tree, tree, int type_or_fns = -1);
extern tree get_class_binding (tree, tree, int type_or_fns = -1);
extern tree *find_member_slot (tree klass, tree name);
extern tree *add_member_slot (tree klass, tree name);
extern void resort_type_member_vec (void *, void *,
				    gt_pointer_operator, void *);
extern void set_class_bindings (tree, unsigned extra = 0);
extern void insert_late_enum_def_bindings (tree, tree);
extern tree innermost_non_namespace_value (tree);
extern cxx_binding *outer_binding (tree, cxx_binding *, bool);
extern void cp_emit_debug_info_for_using (tree, tree);

extern void finish_namespace_using_decl (tree, tree, tree);
extern void finish_local_using_decl (tree, tree, tree);
extern void finish_namespace_using_directive (tree, tree);
extern void finish_local_using_directive (tree, tree);
extern tree pushdecl (tree, bool is_friend = false);
extern tree pushdecl_outermost_localscope (tree);
extern tree pushdecl_top_level (tree, bool is_friend = false);
extern tree pushdecl_top_level_and_finish (tree, tree);
extern tree pushtag (tree, tree, tag_scope);
extern int push_namespace (tree, bool make_inline = false);
extern void pop_namespace (void);
extern void push_nested_namespace (tree);
extern void pop_nested_namespace (tree);
extern void push_to_top_level (void);
extern void pop_from_top_level (void);

#endif /* GCC_CP_NAME_LOOKUP_H */
