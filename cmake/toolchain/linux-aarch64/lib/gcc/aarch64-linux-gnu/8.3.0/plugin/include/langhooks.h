/* The lang_hooks data structure.
   Copyright (C) 2001-2018 Free Software Foundation, Inc.

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

#ifndef GCC_LANG_HOOKS_H
#define GCC_LANG_HOOKS_H

/* FIXME: This file should be #include-d after tree.h (for enum tree_code).  */

struct diagnostic_info;

struct gimplify_omp_ctx;

struct array_descr_info;

/* A print hook for print_tree ().  */
typedef void (*lang_print_tree_hook) (FILE *, tree, int indent);

enum classify_record
  { RECORD_IS_STRUCT, RECORD_IS_CLASS, RECORD_IS_INTERFACE };

class substring_loc;

/* The following hooks are documented in langhooks.c.  Must not be
   NULL.  */

struct lang_hooks_for_tree_inlining
{
  bool (*var_mod_type_p) (tree, tree);
};

/* The following hooks are used by tree-dump.c.  */

struct lang_hooks_for_tree_dump
{
  /* Dump language-specific parts of tree nodes.  Returns nonzero if it
     does not want the usual dumping of the second argument.  */
  bool (*dump_tree) (void *, tree);

  /* Determine type qualifiers in a language-specific way.  */
  int (*type_quals) (const_tree);
};

/* Hooks related to types.  */

struct lang_hooks_for_types
{
  /* Return a new type (with the indicated CODE), doing whatever
     language-specific processing is required.  */
  tree (*make_type) (enum tree_code);

  /* Return what kind of RECORD_TYPE this is, mainly for purposes of
     debug information.  If not defined, record types are assumed to
     be structures.  */
  enum classify_record (*classify_record) (tree);

  /* Given MODE and UNSIGNEDP, return a suitable type-tree with that
     mode.  */
  tree (*type_for_mode) (machine_mode, int);

  /* Given PRECISION and UNSIGNEDP, return a suitable type-tree for an
     integer type with at least that precision.  */
  tree (*type_for_size) (unsigned, int);

  /* True if the type is an instantiation of a generic type,
     e.g. C++ template implicit specializations.  */
  bool (*generic_p) (const_tree);

  /* Returns the TREE_VEC of elements of a given generic argument pack.  */
  tree (*get_argument_pack_elems) (const_tree);

  /* Given a type, apply default promotions to unnamed function
     arguments and return the new type.  Return the same type if no
     change.  Required by any language that supports variadic
     arguments.  The default hook dies.  */
  tree (*type_promotes_to) (tree);

  /* Register TYPE as a builtin type with the indicated NAME.  The
     TYPE is placed in the outermost lexical scope.  The semantics
     should be analogous to:

       typedef TYPE NAME;

     in C.  The default hook ignores the declaration.  */
  void (*register_builtin_type) (tree, const char *);

  /* This routine is called in tree.c to print an error message for
     invalid use of an incomplete type.  VALUE is the expression that
     was used (or 0 if that isn't known) and TYPE is the type that was
     invalid.  LOC is the location of the use.  */
  void (*incomplete_type_error) (location_t loc, const_tree value,
				 const_tree type);

  /* Called from assign_temp to return the maximum size, if there is one,
     for a type.  */
  tree (*max_size) (const_tree);

  /* Register language specific type size variables as potentially OpenMP
     firstprivate variables.  */
  void (*omp_firstprivatize_type_sizes) (struct gimplify_omp_ctx *, tree);

  /* Return true if TYPE is a mappable type.  */
  bool (*omp_mappable_type) (tree type);

  /* Return TRUE if TYPE1 and TYPE2 are identical for type hashing purposes.
     Called only after doing all language independent checks.
     At present, this function is only called when both TYPE1 and TYPE2 are
     FUNCTION_TYPE or METHOD_TYPE.  */
  bool (*type_hash_eq) (const_tree, const_tree);

  /* If non-NULL, return TYPE1 with any language-specific modifiers copied from
     TYPE2.  */
  tree (*copy_lang_qualifiers) (const_tree, const_tree);

  /* Return TRUE if TYPE uses a hidden descriptor and fills in information
     for the debugger about the array bounds, strides, etc.  */
  bool (*get_array_descr_info) (const_tree, struct array_descr_info *);

  /* Fill in information for the debugger about the bounds of TYPE.  */
  void (*get_subrange_bounds) (const_tree, tree *, tree *);

  /* Called on INTEGER_TYPEs.  Return NULL_TREE for non-biased types.  For
     biased types, return as an INTEGER_CST node the value that is represented
     by a physical zero.  */
  tree (*get_type_bias) (const_tree);

  /* A type descriptive of TYPE's complex layout generated to help the
     debugger to decode variable-length or self-referential constructs.
     This is only used for the AT_GNAT_descriptive_type DWARF attribute.  */
  tree (*descriptive_type) (const_tree);

  /* If we requested a pointer to a vector, build up the pointers that
     we stripped off while looking for the inner type.  Similarly for
     return values from functions.  The argument TYPE is the top of the
     chain, and BOTTOM is the new type which we will point to.  */
  tree (*reconstruct_complex_type) (tree, tree);

  /* Returns the tree that represents the underlying data type used to
     implement the enumeration.  The default implementation will just use
     type_for_size.  Used in dwarf2out.c to add a DW_AT_type base type
     reference to a DW_TAG_enumeration.  */
  tree (*enum_underlying_base_type) (const_tree);

  /* Return a type to use in the debug info instead of TYPE, or NULL_TREE to
     keep TYPE.  This is useful to keep a single "source type" when the
     middle-end uses specialized types, for instance constrained discriminated
     types in Ada.  */
  tree (*get_debug_type) (const_tree);

  /* Return TRUE if TYPE implements a fixed point type and fills in information
     for the debugger about scale factor, etc.  */
  bool (*get_fixed_point_type_info) (const_tree,
				     struct fixed_point_type_info *);

  /* Returns -1 if dwarf ATTR shouldn't be added for TYPE, or the attribute
     value otherwise.  */
  int (*type_dwarf_attribute) (const_tree, int);

  /* Returns a tree for the unit size of T excluding tail padding that
     might be used by objects inheriting from T.  */
  tree (*unit_size_without_reusable_padding) (tree);
};

/* Language hooks related to decls and the symbol table.  */

struct lang_hooks_for_decls
{
  /* Return true if we are in the global binding level.  This hook is really
     needed only if the language supports variable-sized types at the global
     level, i.e. declared outside subprograms.  */
  bool (*global_bindings_p) (void);

  /* Function to add a decl to the current scope level.  Takes one
     argument, a decl to add.  Returns that decl, or, if the same
     symbol is already declared, may return a different decl for that
     name.  */
  tree (*pushdecl) (tree);

  /* Returns the chain of decls so far in the current scope level.  */
  tree (*getdecls) (void);

  /* Returns -1 if dwarf ATTR shouldn't be added for DECL, or the attribute
     value otherwise.  */
  int (*decl_dwarf_attribute) (const_tree, int);

  /* Returns True if the parameter is a generic parameter decl
     of a generic type, e.g a template template parameter for the C++ FE.  */
  bool (*generic_generic_parameter_decl_p) (const_tree);

  /* Determine if a function parameter got expanded from a
     function parameter pack.  */
  bool (*function_parm_expanded_from_pack_p) (tree, tree);

  /* Returns the generic declaration of a generic function instantiations.  */
  tree (*get_generic_function_decl) (const_tree);

  /* Returns true when we should warn for an unused global DECL.
     We will already have checked that it has static binding.  */
  bool (*warn_unused_global) (const_tree);

  /* Perform any post compilation-proper parser cleanups and
     processing.  This is currently only needed for the C++ parser,
     which hopefully can be cleaned up so this hook is no longer
     necessary.  */
  void (*post_compilation_parsing_cleanups) (void);

  /* True if this decl may be called via a sibcall.  */
  bool (*ok_for_sibcall) (const_tree);

  /* True if OpenMP should privatize what this DECL points to rather
     than the DECL itself.  */
  bool (*omp_privatize_by_reference) (const_tree);

  /* Return sharing kind if OpenMP sharing attribute of DECL is
     predetermined, OMP_CLAUSE_DEFAULT_UNSPECIFIED otherwise.  */
  enum omp_clause_default_kind (*omp_predetermined_sharing) (tree);

  /* Return decl that should be reported for DEFAULT(NONE) failure
     diagnostics.  Usually the DECL passed in.  */
  tree (*omp_report_decl) (tree);

  /* Return true if DECL's DECL_VALUE_EXPR (if any) should be
     disregarded in OpenMP construct, because it is going to be
     remapped during OpenMP lowering.  SHARED is true if DECL
     is going to be shared, false if it is going to be privatized.  */
  bool (*omp_disregard_value_expr) (tree, bool);

  /* Return true if DECL that is shared iff SHARED is true should
     be put into OMP_CLAUSE_PRIVATE_DEBUG.  */
  bool (*omp_private_debug_clause) (tree, bool);

  /* Return true if DECL in private clause needs
     OMP_CLAUSE_PRIVATE_OUTER_REF on the private clause.  */
  bool (*omp_private_outer_ref) (tree);

  /* Build and return code for a default constructor for DECL in
     response to CLAUSE.  OUTER is corresponding outer region's
     variable if needed.  Return NULL if nothing to be done.  */
  tree (*omp_clause_default_ctor) (tree clause, tree decl, tree outer);

  /* Build and return code for a copy constructor from SRC to DST.  */
  tree (*omp_clause_copy_ctor) (tree clause, tree dst, tree src);

  /* Similarly, except use an assignment operator instead.  */
  tree (*omp_clause_assign_op) (tree clause, tree dst, tree src);

  /* Build and return code for a constructor of DST that sets it to
     SRC + ADD.  */
  tree (*omp_clause_linear_ctor) (tree clause, tree dst, tree src, tree add);

  /* Build and return code destructing DECL.  Return NULL if nothing
     to be done.  */
  tree (*omp_clause_dtor) (tree clause, tree decl);

  /* Do language specific checking on an implicitly determined clause.  */
  void (*omp_finish_clause) (tree clause, gimple_seq *pre_p);

  /* Return true if DECL is a scalar variable (for the purpose of
     implicit firstprivatization).  */
  bool (*omp_scalar_p) (tree decl);
};

/* Language hooks related to LTO serialization.  */

struct lang_hooks_for_lto
{
  /* Begin a new LTO section named NAME.  */
  void (*begin_section) (const char *name);

  /* Write DATA of length LEN to the currently open LTO section.  BLOCK is a
     pointer to the dynamically allocated memory containing DATA.  The
     append_data function is responsible for freeing it when it is no longer
     needed.  */
  void (*append_data) (const void *data, size_t len, void *block);

  /* End the previously begun LTO section.  */
  void (*end_section) (void);
};

/* Language-specific hooks.  See langhooks-def.h for defaults.  */

struct lang_hooks
{
  /* String identifying the front end and optionally language standard
     version, e.g. "GNU C++98".  */
  const char *name;

  /* sizeof (struct lang_identifier), so make_node () creates
     identifier nodes long enough for the language-specific slots.  */
  size_t identifier_size;

  /* Remove any parts of the tree that are used only by the FE. */
  void (*free_lang_data) (tree);

  /* Determines the size of any language-specific tcc_constant,
     tcc_exceptional or tcc_type nodes.  Since it is called from
     make_node, the only information available is the tree code.
     Expected to die on unrecognized codes.  */
  size_t (*tree_size) (enum tree_code);

  /* Return the language mask used for converting argv into a sequence
     of options.  */
  unsigned int (*option_lang_mask) (void);

  /* Initialize variables in an options structure.  */
  void (*init_options_struct) (struct gcc_options *opts);

  /* After the initialize_diagnostics hook is called, do any simple
     initialization needed before any calls to handle_option, other
     than that done by the init_options_struct hook.  */
  void (*init_options) (unsigned int decoded_options_count,
			struct cl_decoded_option *decoded_options);

  /* Callback used to perform language-specific initialization for the
     global diagnostic context structure.  */
  void (*initialize_diagnostics) (diagnostic_context *);

  /* Register language-specific dumps.  */
  void (*register_dumps) (gcc::dump_manager *);

  /* Return true if a warning should be given about option OPTION,
     which is for the wrong language, false if it should be quietly
     ignored.  */
  bool (*complain_wrong_lang_p) (const struct cl_option *option);

  /* Handle the switch CODE, which has real type enum opt_code from
     options.h.  If the switch takes an argument, it is passed in ARG
     which points to permanent storage.  The handler is responsible for
     checking whether ARG is NULL, which indicates that no argument
     was in fact supplied.  For -f and -W switches, VALUE is 1 or 0
     for the positive and negative forms respectively.  HANDLERS should
     be passed to any recursive handle_option calls.  LOC is the
     location of the option.

     Return true if the switch is valid, false if invalid.  */
  bool (*handle_option) (size_t code, const char *arg, int value, int kind,
			 location_t loc,
			 const struct cl_option_handlers *handlers);

  /* Called when all command line options have been parsed to allow
     further processing and initialization

     Should return true to indicate that a compiler back-end is
     not required, such as with the -E option.

     If errorcount is nonzero after this call the compiler exits
     immediately and the finish hook is not called.  */
  bool (*post_options) (const char **);

  /* Called after post_options to initialize the front end.  Return
     false to indicate that no further compilation be performed, in
     which case the finish hook is called immediately.  */
  bool (*init) (void);

  /* Called at the end of compilation, as a finalizer.  */
  void (*finish) (void);

  /* Parses the entire file.  */
  void (*parse_file) (void);

  /* Determines if it's ok for a function to have no noreturn attribute.  */
  bool (*missing_noreturn_ok_p) (tree);

  /* Called to obtain the alias set to be used for an expression or type.
     Returns -1 if the language does nothing special for it.  */
  alias_set_type (*get_alias_set) (tree);

  /* Function to finish handling an incomplete decl at the end of
     compilation.  Default hook is does nothing.  */
  void (*finish_incomplete_decl) (tree);

  /* Replace the DECL_LANG_SPECIFIC data, which may be NULL, of the
     DECL_NODE with a newly GC-allocated copy.  */
  void (*dup_lang_specific_decl) (tree);

  /* Set the DECL_ASSEMBLER_NAME for a node.  If it is the sort of
     thing that the assembler should talk about, set
     DECL_ASSEMBLER_NAME to an appropriate IDENTIFIER_NODE.
     Otherwise, set it to the ERROR_MARK_NODE to ensure that the
     assembler does not talk about it.  */
  void (*set_decl_assembler_name) (tree);

  /* Overwrite the DECL_ASSEMBLER_NAME for a node.  The name is being
     changed (including to or from NULL_TREE).  */
  void (*overwrite_decl_assembler_name) (tree, tree);

  /* The front end can add its own statistics to -fmem-report with
     this hook.  It should output to stderr.  */
  void (*print_statistics) (void);

  /* Called by print_tree when there is a tree of class tcc_exceptional
     that it doesn't know how to display.  */
  lang_print_tree_hook print_xnode;

  /* Called to print language-dependent parts of tcc_decl, tcc_type,
     and IDENTIFIER_NODE nodes.  */
  lang_print_tree_hook print_decl;
  lang_print_tree_hook print_type;
  lang_print_tree_hook print_identifier;

  /* Computes the name to use to print a declaration.  DECL is the
     non-NULL declaration in question.  VERBOSITY determines what
     information will be printed: 0: DECL_NAME, demangled as
     necessary.  1: and scope information.  2: and any other
     information that might be interesting, such as function parameter
     types in C++.  The name is in the internal character set and
     needs to be converted to the locale character set of diagnostics,
     or to the execution character set for strings such as
     __PRETTY_FUNCTION__.  */
  const char *(*decl_printable_name) (tree decl, int verbosity);

  /* Computes the dwarf-2/3 name for a tree.  VERBOSITY determines what
     information will be printed: 0: DECL_NAME, demangled as
     necessary.  1: and scope information.  */
  const char *(*dwarf_name) (tree, int verbosity);

  /* This compares two types for equivalence ("compatible" in C-based languages).
     This routine should only return 1 if it is sure.  It should not be used
     in contexts where erroneously returning 0 causes problems.  */
  int (*types_compatible_p) (tree x, tree y);

  /* Called by report_error_function to print out function name.  */
  void (*print_error_function) (diagnostic_context *, const char *,
				struct diagnostic_info *);

  /* Convert a character from the host's to the target's character
     set.  The character should be in what C calls the "basic source
     character set" (roughly, the set of characters defined by plain
     old ASCII).  The default is to return the character unchanged,
     which is correct in most circumstances.  Note that both argument
     and result should be sign-extended under -fsigned-char,
     zero-extended under -fno-signed-char.  */
  HOST_WIDE_INT (*to_target_charset) (HOST_WIDE_INT);

  /* Pointers to machine-independent attribute tables, for front ends
     using attribs.c.  If one is NULL, it is ignored.  Respectively, a
     table of attributes specific to the language, a table of
     attributes common to two or more languages (to allow easy
     sharing), and a table of attributes for checking formats.  */
  const struct attribute_spec *attribute_table;
  const struct attribute_spec *common_attribute_table;
  const struct attribute_spec *format_attribute_table;

  struct lang_hooks_for_tree_inlining tree_inlining;

  struct lang_hooks_for_tree_dump tree_dump;

  struct lang_hooks_for_decls decls;

  struct lang_hooks_for_types types;
  
  struct lang_hooks_for_lto lto;

  /* Returns a TREE_VEC of the generic parameters of an instantiation of
     a generic type or decl, e.g. C++ template instantiation.  If
     TREE_CHAIN of the return value is set, it is an INTEGER_CST
     indicating how many of the elements are non-default.  */
  tree (*get_innermost_generic_parms) (const_tree);

  /* Returns the TREE_VEC of arguments of an instantiation
     of a generic type of decl, e.g. C++ template instantiation.  */
  tree (*get_innermost_generic_args) (const_tree);

  /* Determine if a tree is a function parameter pack.  */
  bool (*function_parameter_pack_p) (const_tree);

  /* Perform language-specific gimplification on the argument.  Returns an
     enum gimplify_status, though we can't see that type here.  */
  int (*gimplify_expr) (tree *, gimple_seq *, gimple_seq *);

  /* Do language specific processing in the builtin function DECL  */
  tree (*builtin_function) (tree decl);

  /* Like builtin_function, but make sure the scope is the external scope.
     This is used to delay putting in back end builtin functions until the ISA
     that defines the builtin is declared via function specific target options,
     which can save memory for machines like the x86_64 that have multiple
     ISAs.  If this points to the same function as builtin_function, the
     backend must add all of the builtins at program initialization time.  */
  tree (*builtin_function_ext_scope) (tree decl);

  /* Used to set up the tree_contains_structure array for a frontend. */
  void (*init_ts) (void);

  /* Called by recompute_tree_invariant_for_addr_expr to go from EXPR
     to a contained expression or DECL, possibly updating *TC or *SE
     if in the process TREE_CONSTANT or TREE_SIDE_EFFECTS need updating.  */
  tree (*expr_to_decl) (tree expr, bool *tc, bool *se);

  /* The EH personality function decl.  */
  tree (*eh_personality) (void);

  /* Map a type to a runtime object to match type.  */
  tree (*eh_runtime_type) (tree);

  /* If non-NULL, this is a function that returns a function decl to be
     executed if an unhandled exception is propagated out of a cleanup
     region.  For example, in C++, an exception thrown by a destructor
     during stack unwinding is required to result in a call to
     `std::terminate', so the C++ version of this function returns a
     FUNCTION_DECL for `std::terminate'.  */
  tree (*eh_protect_cleanup_actions) (void);

  /* Return true if a stmt can fallthru.  Used by block_may_fallthru
     to possibly handle language trees.  */
  bool (*block_may_fallthru) (const_tree);

  /* True if this language uses __cxa_end_cleanup when the ARM EABI
     is enabled.  */
  bool eh_use_cxa_end_cleanup;

  /* True if this language requires deep unsharing of tree nodes prior to
     gimplification.  */
  bool deep_unsharing;

  /* True if this language may use custom descriptors for nested functions
     instead of trampolines.  */
  bool custom_function_descriptors;

  /* True if this language emits begin stmt notes.  */
  bool emits_begin_stmt;

  /* Run all lang-specific selftests.  */
  void (*run_lang_selftests) (void);

  /* Attempt to determine the source location of the substring.
     If successful, return NULL and write the source location to *OUT_LOC.
     Otherwise return an error message.  Error messages are intended
     for GCC developers (to help debugging) rather than for end-users.  */
  const char *(*get_substring_location) (const substring_loc &,
					 location_t *out_loc);

  /* Whenever you add entries here, make sure you adjust langhooks-def.h
     and langhooks.c accordingly.  */
};

/* Each front end provides its own.  */
extern struct lang_hooks lang_hooks;

extern tree add_builtin_function (const char *name, tree type,
				  int function_code, enum built_in_class cl,
				  const char *library_name,
				  tree attrs);

extern tree add_builtin_function_ext_scope (const char *name, tree type,
					    int function_code,
					    enum built_in_class cl,
					    const char *library_name,
					    tree attrs);
extern tree add_builtin_type (const char *name, tree type);

/* Language helper functions.  */

extern bool lang_GNU_C (void);
extern bool lang_GNU_CXX (void);
extern bool lang_GNU_Fortran (void);
extern bool lang_GNU_OBJC (void);

#endif /* GCC_LANG_HOOKS_H */
