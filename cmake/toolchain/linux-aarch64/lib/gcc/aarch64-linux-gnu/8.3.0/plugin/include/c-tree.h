/* Definitions for C parsing and type checking.
   Copyright (C) 1987-2018 Free Software Foundation, Inc.

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

#ifndef GCC_C_TREE_H
#define GCC_C_TREE_H

#include "c-family/c-common.h"
#include "diagnostic.h"

/* struct lang_identifier is private to c-decl.c, but langhooks.c needs to
   know how big it is.  This is sanity-checked in c-decl.c.  */
#define C_SIZEOF_STRUCT_LANG_IDENTIFIER \
  (sizeof (struct c_common_identifier) + 3 * sizeof (void *))

/* In a RECORD_TYPE or UNION_TYPE, nonzero if any component is read-only.  */
#define C_TYPE_FIELDS_READONLY(TYPE) TREE_LANG_FLAG_1 (TYPE)

/* In a RECORD_TYPE or UNION_TYPE, nonzero if any component is volatile.  */
#define C_TYPE_FIELDS_VOLATILE(TYPE) TREE_LANG_FLAG_2 (TYPE)

/* In a RECORD_TYPE or UNION_TYPE or ENUMERAL_TYPE
   nonzero if the definition of the type has already started.  */
#define C_TYPE_BEING_DEFINED(TYPE) TYPE_LANG_FLAG_0 (TYPE)

/* In an incomplete RECORD_TYPE or UNION_TYPE, a list of variable
   declarations whose type would be completed by completing that type.  */
#define C_TYPE_INCOMPLETE_VARS(TYPE) TYPE_VFIELD (TYPE)

/* In an IDENTIFIER_NODE, nonzero if this identifier is actually a
   keyword.  C_RID_CODE (node) is then the RID_* value of the keyword.  */
#define C_IS_RESERVED_WORD(ID) TREE_LANG_FLAG_0 (ID)

/* Record whether a type or decl was written with nonconstant size.
   Note that TYPE_SIZE may have simplified to a constant.  */
#define C_TYPE_VARIABLE_SIZE(TYPE) TYPE_LANG_FLAG_1 (TYPE)
#define C_DECL_VARIABLE_SIZE(TYPE) DECL_LANG_FLAG_0 (TYPE)

/* Record whether a type is defined inside a struct or union type.
   This is used for -Wc++-compat. */
#define C_TYPE_DEFINED_IN_STRUCT(TYPE) TYPE_LANG_FLAG_2 (TYPE)

/* Record whether an "incomplete type" error was given for the type.  */
#define C_TYPE_ERROR_REPORTED(TYPE) TYPE_LANG_FLAG_3 (TYPE)

/* Record whether a typedef for type `int' was actually `signed int'.  */
#define C_TYPEDEF_EXPLICITLY_SIGNED(EXP) DECL_LANG_FLAG_1 (EXP)

/* For a FUNCTION_DECL, nonzero if it was defined without an explicit
   return type.  */
#define C_FUNCTION_IMPLICIT_INT(EXP) DECL_LANG_FLAG_1 (EXP)

/* For a FUNCTION_DECL, nonzero if it was an implicit declaration.  */
#define C_DECL_IMPLICIT(EXP) DECL_LANG_FLAG_2 (EXP)

/* For a PARM_DECL, nonzero if it was declared as an array.  */
#define C_ARRAY_PARAMETER(NODE) DECL_LANG_FLAG_0 (NODE)

/* For FUNCTION_DECLs, evaluates true if the decl is built-in but has
   been declared.  */
#define C_DECL_DECLARED_BUILTIN(EXP)		\
  DECL_LANG_FLAG_3 (FUNCTION_DECL_CHECK (EXP))

/* For FUNCTION_DECLs, evaluates true if the decl is built-in, has a
   built-in prototype and does not have a non-built-in prototype.  */
#define C_DECL_BUILTIN_PROTOTYPE(EXP)		\
  DECL_LANG_FLAG_6 (FUNCTION_DECL_CHECK (EXP))

/* Record whether a decl was declared register.  This is strictly a
   front-end flag, whereas DECL_REGISTER is used for code generation;
   they may differ for structures with volatile fields.  */
#define C_DECL_REGISTER(EXP) DECL_LANG_FLAG_4 (EXP)

/* Record whether a decl was used in an expression anywhere except an
   unevaluated operand of sizeof / typeof / alignof.  This is only
   used for functions declared static but not defined, though outside
   sizeof and typeof it is set for other function decls as well.  */
#define C_DECL_USED(EXP) DECL_LANG_FLAG_5 (FUNCTION_DECL_CHECK (EXP))

/* Record whether a variable has been declared threadprivate by
   #pragma omp threadprivate.  */
#define C_DECL_THREADPRIVATE_P(DECL) DECL_LANG_FLAG_3 (VAR_DECL_CHECK (DECL))

/* Nonzero for a decl which either doesn't exist or isn't a prototype.
   N.B. Could be simplified if all built-in decls had complete prototypes
   (but this is presently difficult because some of them need FILE*).  */
#define C_DECL_ISNT_PROTOTYPE(EXP)			\
       (EXP == 0					\
	|| (!prototype_p (TREE_TYPE (EXP))	\
	    && !DECL_BUILT_IN (EXP)))

/* For FUNCTION_TYPE, a hidden list of types of arguments.  The same as
   TYPE_ARG_TYPES for functions with prototypes, but created for functions
   without prototypes.  */
#define TYPE_ACTUAL_ARG_TYPES(NODE) TYPE_LANG_SLOT_1 (NODE)

/* For a CONSTRUCTOR, whether some initializer contains a
   subexpression meaning it is not a constant expression.  */
#define CONSTRUCTOR_NON_CONST(EXPR) TREE_LANG_FLAG_1 (CONSTRUCTOR_CHECK (EXPR))

/* For a SAVE_EXPR, nonzero if the operand of the SAVE_EXPR has already
   been folded.  */
#define SAVE_EXPR_FOLDED_P(EXP)	TREE_LANG_FLAG_1 (SAVE_EXPR_CHECK (EXP))

/* Record parser information about an expression that is irrelevant
   for code generation alongside a tree representing its value.  */
struct c_expr
{
  /* The value of the expression.  */
  tree value;
  /* Record the original unary/binary operator of an expression, which may
     have been changed by fold, STRING_CST for unparenthesized string
     constants, C_MAYBE_CONST_EXPR for __builtin_constant_p calls
     (even if parenthesized), for subexpressions, and for non-constant
     initializers, or ERROR_MARK for other expressions (including
     parenthesized expressions).  */
  enum tree_code original_code;
  /* If not NULL, the original type of an expression.  This will
     differ from the type of the value field for an enum constant.
     The type of an enum constant is a plain integer type, but this
     field will be the enum type.  */
  tree original_type;

  /* The source range of this expression.  This is redundant
     for node values that have locations, but not all node kinds
     have locations (e.g. constants, and references to params, locals,
     etc), so we stash a copy here.  */
  source_range src_range;

  /* Access to the first and last locations within the source spelling
     of this expression.  */
  location_t get_start () const { return src_range.m_start; }
  location_t get_finish () const { return src_range.m_finish; }

  location_t get_location () const
  {
    if (EXPR_HAS_LOCATION (value))
      return EXPR_LOCATION (value);
    else
      return make_location (get_start (), get_start (), get_finish ());
  }

  /* Set the value to error_mark_node whilst ensuring that src_range
     is initialized.  */
  void set_error ()
  {
    value = error_mark_node;
    src_range.m_start = UNKNOWN_LOCATION;
    src_range.m_finish = UNKNOWN_LOCATION;
  }
};

/* Type alias for struct c_expr. This allows to use the structure
   inside the VEC types.  */
typedef struct c_expr c_expr_t;

/* A kind of type specifier.  Note that this information is currently
   only used to distinguish tag definitions, tag references and typeof
   uses.  */
enum c_typespec_kind {
  /* No typespec.  This appears only in struct c_declspec.  */
  ctsk_none,
  /* A reserved keyword type specifier.  */
  ctsk_resword,
  /* A reference to a tag, previously declared, such as "struct foo".
     This includes where the previous declaration was as a different
     kind of tag, in which case this is only valid if shadowing that
     tag in an inner scope.  */
  ctsk_tagref,
  /* A reference to a tag, not previously declared in a visible
     scope.  */
  ctsk_tagfirstref,
  /* A definition of a tag such as "struct foo { int a; }".  */
  ctsk_tagdef,
  /* A typedef name.  */
  ctsk_typedef,
  /* An ObjC-specific kind of type specifier.  */
  ctsk_objc,
  /* A typeof specifier, or _Atomic ( type-name ).  */
  ctsk_typeof
};

/* A type specifier: this structure is created in the parser and
   passed to declspecs_add_type only.  */
struct c_typespec {
  /* What kind of type specifier this is.  */
  enum c_typespec_kind kind;
  /* Whether the expression has operands suitable for use in constant
     expressions.  */
  bool expr_const_operands;
  /* The specifier itself.  */
  tree spec;
  /* An expression to be evaluated before the type specifier, in the
     case of typeof specifiers, or NULL otherwise or if no such
     expression is required for a particular typeof specifier.  In
     particular, when typeof is applied to an expression of variably
     modified type, that expression must be evaluated in order to
     determine array sizes that form part of the type, but the
     expression itself (as opposed to the array sizes) forms no part
     of the type and so needs to be recorded separately.  */
  tree expr;
};

/* A storage class specifier.  */
enum c_storage_class {
  csc_none,
  csc_auto,
  csc_extern,
  csc_register,
  csc_static,
  csc_typedef
};

/* A type specifier keyword "void", "_Bool", "char", "int", "float",
   "double", "_Decimal32", "_Decimal64", "_Decimal128", "_Fract", "_Accum",
   or none of these.  */
enum c_typespec_keyword {
  cts_none,
  cts_void,
  cts_bool,
  cts_char,
  cts_int,
  cts_float,
  cts_int_n,
  cts_double,
  cts_dfloat32,
  cts_dfloat64,
  cts_dfloat128,
  cts_floatn_nx,
  cts_fract,
  cts_accum,
  cts_auto_type
};

/* This enum lists all the possible declarator specifiers, storage
   class or attribute that a user can write.  There is at least one
   enumerator per possible declarator specifier in the struct
   c_declspecs below.

   It is used to index the array of declspec locations in struct
   c_declspecs.  */
enum c_declspec_word {
  cdw_typespec /* A catch-all for a typespec.  */,
  cdw_storage_class  /* A catch-all for a storage class */,
  cdw_attributes,
  cdw_typedef,
  cdw_explicit_signed,
  cdw_deprecated,
  cdw_default_int,
  cdw_long,
  cdw_long_long,
  cdw_short,
  cdw_signed,
  cdw_unsigned,
  cdw_complex,
  cdw_inline,
  cdw_noreturn,
  cdw_thread,
  cdw_const,
  cdw_volatile,
  cdw_restrict,
  cdw_atomic,
  cdw_saturating,
  cdw_alignas,
  cdw_address_space,
  cdw_gimple,
  cdw_rtl,
  cdw_number_of_elements /* This one must always be the last
			    enumerator.  */
};

/* A sequence of declaration specifiers in C.  When a new declaration
   specifier is added, please update the enum c_declspec_word above
   accordingly.  */
struct c_declspecs {
  source_location locations[cdw_number_of_elements];
  /* The type specified, if a single type specifier such as a struct,
     union or enum specifier, typedef name or typeof specifies the
     whole type, or NULL_TREE if none or a keyword such as "void" or
     "char" is used.  Does not include qualifiers.  */
  tree type;
  /* Any expression to be evaluated before the type, from a typeof
     specifier.  */
  tree expr;
  /* The attributes from a typedef decl.  */
  tree decl_attr;
  /* When parsing, the attributes.  Outside the parser, this will be
     NULL; attributes (possibly from multiple lists) will be passed
     separately.  */
  tree attrs;
  /* The pass to start compiling a __GIMPLE or __RTL function with.  */
  char *gimple_or_rtl_pass;
  /* The base-2 log of the greatest alignment required by an _Alignas
     specifier, in bytes, or -1 if no such specifiers with nonzero
     alignment.  */
  int align_log;
  /* For the __intN declspec, this stores the index into the int_n_* arrays.  */
  int int_n_idx;
  /* For the _FloatN and _FloatNx declspec, this stores the index into
     the floatn_nx_types array.  */
  int floatn_nx_idx;
  /* The storage class specifier, or csc_none if none.  */
  enum c_storage_class storage_class;
  /* Any type specifier keyword used such as "int", not reflecting
     modifiers such as "short", or cts_none if none.  */
  ENUM_BITFIELD (c_typespec_keyword) typespec_word : 8;
  /* The kind of type specifier if one has been seen, ctsk_none
     otherwise.  */
  ENUM_BITFIELD (c_typespec_kind) typespec_kind : 3;
  /* Whether any expressions in typeof specifiers may appear in
     constant expressions.  */
  BOOL_BITFIELD expr_const_operands : 1;
  /* Whether any declaration specifiers have been seen at all.  */
  BOOL_BITFIELD declspecs_seen_p : 1;
  /* Whether something other than a storage class specifier or
     attribute has been seen.  This is used to warn for the
     obsolescent usage of storage class specifiers other than at the
     start of the list.  (Doing this properly would require function
     specifiers to be handled separately from storage class
     specifiers.)  */
  BOOL_BITFIELD non_sc_seen_p : 1;
  /* Whether the type is specified by a typedef or typeof name.  */
  BOOL_BITFIELD typedef_p : 1;
  /* Whether the type is explicitly "signed" or specified by a typedef
     whose type is explicitly "signed".  */
  BOOL_BITFIELD explicit_signed_p : 1;
  /* Whether the specifiers include a deprecated typedef.  */
  BOOL_BITFIELD deprecated_p : 1;
  /* Whether the type defaulted to "int" because there were no type
     specifiers.  */
  BOOL_BITFIELD default_int_p : 1;
  /* Whether "long" was specified.  */
  BOOL_BITFIELD long_p : 1;
  /* Whether "long" was specified more than once.  */
  BOOL_BITFIELD long_long_p : 1;
  /* Whether "short" was specified.  */
  BOOL_BITFIELD short_p : 1;
  /* Whether "signed" was specified.  */
  BOOL_BITFIELD signed_p : 1;
  /* Whether "unsigned" was specified.  */
  BOOL_BITFIELD unsigned_p : 1;
  /* Whether "complex" was specified.  */
  BOOL_BITFIELD complex_p : 1;
  /* Whether "inline" was specified.  */
  BOOL_BITFIELD inline_p : 1;
  /* Whether "_Noreturn" was speciied.  */
  BOOL_BITFIELD noreturn_p : 1;
  /* Whether "__thread" or "_Thread_local" was specified.  */
  BOOL_BITFIELD thread_p : 1;
  /* Whether "__thread" rather than "_Thread_local" was specified.  */
  BOOL_BITFIELD thread_gnu_p : 1;
  /* Whether "const" was specified.  */
  BOOL_BITFIELD const_p : 1;
  /* Whether "volatile" was specified.  */
  BOOL_BITFIELD volatile_p : 1;
  /* Whether "restrict" was specified.  */
  BOOL_BITFIELD restrict_p : 1;
  /* Whether "_Atomic" was specified.  */
  BOOL_BITFIELD atomic_p : 1;
  /* Whether "_Sat" was specified.  */
  BOOL_BITFIELD saturating_p : 1;
  /* Whether any alignment specifier (even with zero alignment) was
     specified.  */
  BOOL_BITFIELD alignas_p : 1;
  /* Whether any __GIMPLE specifier was specified.  */
  BOOL_BITFIELD gimple_p : 1;
  /* Whether any __RTL specifier was specified.  */
  BOOL_BITFIELD rtl_p : 1;
  /* The address space that the declaration belongs to.  */
  addr_space_t address_space;
};

/* The various kinds of declarators in C.  */
enum c_declarator_kind {
  /* An identifier.  */
  cdk_id,
  /* A function.  */
  cdk_function,
  /* An array.  */
  cdk_array,
  /* A pointer.  */
  cdk_pointer,
  /* Parenthesized declarator with nested attributes.  */
  cdk_attrs
};

struct c_arg_tag {
  /* The argument name.  */
  tree id;
  /* The type of the argument.  */
  tree type;
};


/* Information about the parameters in a function declarator.  */
struct c_arg_info {
  /* A list of parameter decls.  */
  tree parms;
  /* A list of structure, union and enum tags defined.  */
  vec<c_arg_tag, va_gc> *tags;
  /* A list of argument types to go in the FUNCTION_TYPE.  */
  tree types;
  /* A list of non-parameter decls (notably enumeration constants)
     defined with the parameters.  */
  tree others;
  /* A compound expression of VLA sizes from the parameters, or NULL.
     In a function definition, these are used to ensure that
     side-effects in sizes of arrays converted to pointers (such as a
     parameter int i[n++]) take place; otherwise, they are
     ignored.  */
  tree pending_sizes;
  /* True when these arguments had [*].  */
  BOOL_BITFIELD had_vla_unspec : 1;
};

/* A declarator.  */
struct c_declarator {
  /* The kind of declarator.  */
  enum c_declarator_kind kind;
  location_t id_loc; /* Currently only set for cdk_id, cdk_array. */
  /* Except for cdk_id, the contained declarator.  For cdk_id, NULL.  */
  struct c_declarator *declarator;
  union {
    /* For identifiers, an IDENTIFIER_NODE or NULL_TREE if an abstract
       declarator.  */
    tree id;
    /* For functions.  */
    struct c_arg_info *arg_info;
    /* For arrays.  */
    struct {
      /* The array dimension, or NULL for [] and [*].  */
      tree dimen;
      /* The qualifiers inside [].  */
      int quals;
      /* The attributes (currently ignored) inside [].  */
      tree attrs;
      /* Whether [static] was used.  */
      BOOL_BITFIELD static_p : 1;
      /* Whether [*] was used.  */
      BOOL_BITFIELD vla_unspec_p : 1;
    } array;
    /* For pointers, the qualifiers on the pointer type.  */
    int pointer_quals;
    /* For attributes.  */
    tree attrs;
  } u;
};

/* A type name.  */
struct c_type_name {
  /* The declaration specifiers.  */
  struct c_declspecs *specs;
  /* The declarator.  */
  struct c_declarator *declarator;
};

/* A parameter.  */
struct c_parm {
  /* The declaration specifiers, minus any prefix attributes.  */
  struct c_declspecs *specs;
  /* The attributes.  */
  tree attrs;
  /* The declarator.  */
  struct c_declarator *declarator;
  /* The location of the parameter.  */
  location_t loc;
};

/* Used when parsing an enum.  Initialized by start_enum.  */
struct c_enum_contents
{
  /* While defining an enum type, this is 1 plus the last enumerator
     constant value.  */
  tree enum_next_value;

  /* Nonzero means that there was overflow computing enum_next_value.  */
  int enum_overflow;
};

/* A type of reference to a static identifier in an inline
   function.  */
enum c_inline_static_type {
  /* Identifier with internal linkage used in function that may be an
     inline definition (i.e., file-scope static).  */
  csi_internal,
  /* Modifiable object with static storage duration defined in
     function that may be an inline definition (i.e., local
     static).  */
  csi_modifiable
};


/* in c-parser.c */
extern void c_parse_init (void);
extern bool c_keyword_starts_typename (enum rid keyword);

/* in c-aux-info.c */
extern void gen_aux_info_record (tree, int, int, int);

/* in c-decl.c */
struct c_spot_bindings;
struct c_struct_parse_info;
extern struct obstack parser_obstack;
extern tree c_break_label;
extern tree c_cont_label;

extern bool global_bindings_p (void);
extern tree pushdecl (tree);
extern void push_scope (void);
extern tree pop_scope (void);
extern void c_bindings_start_stmt_expr (struct c_spot_bindings *);
extern void c_bindings_end_stmt_expr (struct c_spot_bindings *);

extern void record_inline_static (location_t, tree, tree,
				  enum c_inline_static_type);
extern void c_init_decl_processing (void);
extern void c_print_identifier (FILE *, tree, int);
extern int quals_from_declspecs (const struct c_declspecs *);
extern struct c_declarator *build_array_declarator (location_t, tree,
    						    struct c_declspecs *,
						    bool, bool);
extern tree build_enumerator (location_t, location_t, struct c_enum_contents *,
			      tree, tree);
extern tree check_for_loop_decls (location_t, bool);
extern void mark_forward_parm_decls (void);
extern void declare_parm_level (void);
extern void undeclared_variable (location_t, tree);
extern tree lookup_label_for_goto (location_t, tree);
extern tree declare_label (tree);
extern tree define_label (location_t, tree);
extern struct c_spot_bindings *c_get_switch_bindings (void);
extern void c_release_switch_bindings (struct c_spot_bindings *);
extern bool c_check_switch_jump_warnings (struct c_spot_bindings *,
					  location_t, location_t);
extern void finish_decl (tree, location_t, tree, tree, tree);
extern tree finish_enum (tree, tree, tree);
extern void finish_function (void);
extern tree finish_struct (location_t, tree, tree, tree,
			   struct c_struct_parse_info *);
extern struct c_arg_info *build_arg_info (void);
extern struct c_arg_info *get_parm_info (bool, tree);
extern tree grokfield (location_t, struct c_declarator *,
		       struct c_declspecs *, tree, tree *);
extern tree groktypename (struct c_type_name *, tree *, bool *);
extern tree grokparm (const struct c_parm *, tree *);
extern tree implicitly_declare (location_t, tree);
extern void keep_next_level (void);
extern void pending_xref_error (void);
extern void c_push_function_context (void);
extern void c_pop_function_context (void);
extern void push_parm_decl (const struct c_parm *, tree *);
extern struct c_declarator *set_array_declarator_inner (struct c_declarator *,
							struct c_declarator *);
extern tree c_builtin_function (tree);
extern tree c_builtin_function_ext_scope (tree);
extern void shadow_tag (const struct c_declspecs *);
extern void shadow_tag_warned (const struct c_declspecs *, int);
extern tree start_enum (location_t, struct c_enum_contents *, tree);
extern bool start_function (struct c_declspecs *, struct c_declarator *, tree);
extern tree start_decl (struct c_declarator *, struct c_declspecs *, bool,
			tree);
extern tree start_struct (location_t, enum tree_code, tree,
			  struct c_struct_parse_info **);
extern void store_parm_decls (void);
extern void store_parm_decls_from (struct c_arg_info *);
extern void temp_store_parm_decls (tree, tree);
extern void temp_pop_parm_decls (void);
extern tree xref_tag (enum tree_code, tree);
extern struct c_typespec parser_xref_tag (location_t, enum tree_code, tree);
extern struct c_parm *build_c_parm (struct c_declspecs *, tree,
				    struct c_declarator *, location_t);
extern struct c_declarator *build_attrs_declarator (tree,
						    struct c_declarator *);
extern struct c_declarator *build_function_declarator (struct c_arg_info *,
						       struct c_declarator *);
extern struct c_declarator *build_id_declarator (tree);
extern struct c_declarator *make_pointer_declarator (struct c_declspecs *,
						     struct c_declarator *);
extern struct c_declspecs *build_null_declspecs (void);
extern struct c_declspecs *declspecs_add_qual (source_location,
					       struct c_declspecs *, tree);
extern struct c_declspecs *declspecs_add_type (location_t,
					       struct c_declspecs *,
					       struct c_typespec);
extern struct c_declspecs *declspecs_add_scspec (source_location,
						 struct c_declspecs *, tree);
extern struct c_declspecs *declspecs_add_attrs (source_location,
						struct c_declspecs *, tree);
extern struct c_declspecs *declspecs_add_addrspace (source_location,
						    struct c_declspecs *,
						    addr_space_t);
extern struct c_declspecs *declspecs_add_alignas (source_location,
						  struct c_declspecs *, tree);
extern struct c_declspecs *finish_declspecs (struct c_declspecs *);

/* in c-objc-common.c */
extern bool c_objc_common_init (void);
extern bool c_missing_noreturn_ok_p (tree);
extern bool c_warn_unused_global_decl (const_tree);
extern void c_initialize_diagnostics (diagnostic_context *);
extern bool c_vla_unspec_p (tree x, tree fn);

/* in c-typeck.c */
extern int in_alignof;
extern int in_sizeof;
extern int in_typeof;

extern tree c_last_sizeof_arg;
extern location_t c_last_sizeof_loc;

extern struct c_switch *c_switch_stack;

extern tree c_objc_common_truthvalue_conversion (location_t, tree);
extern tree require_complete_type (location_t, tree);
extern bool same_translation_unit_p (const_tree, const_tree);
extern int comptypes (tree, tree);
extern int comptypes_check_different_types (tree, tree, bool *);
extern bool c_vla_type_p (const_tree);
extern bool c_mark_addressable (tree, bool = false);
extern void c_incomplete_type_error (location_t, const_tree, const_tree);
extern tree c_type_promotes_to (tree);
extern struct c_expr default_function_array_conversion (location_t,
							struct c_expr);
extern struct c_expr default_function_array_read_conversion (location_t,
							     struct c_expr);
extern struct c_expr convert_lvalue_to_rvalue (location_t, struct c_expr,
					       bool, bool);
extern tree decl_constant_value_1 (tree, bool);
extern void mark_exp_read (tree);
extern tree composite_type (tree, tree);
extern tree build_component_ref (location_t, tree, tree, location_t);
extern tree build_array_ref (location_t, tree, tree);
extern tree build_external_ref (location_t, tree, bool, tree *);
extern void pop_maybe_used (bool);
extern struct c_expr c_expr_sizeof_expr (location_t, struct c_expr);
extern struct c_expr c_expr_sizeof_type (location_t, struct c_type_name *);
extern struct c_expr parser_build_unary_op (location_t, enum tree_code,
    					    struct c_expr);
extern struct c_expr parser_build_binary_op (location_t,
    					     enum tree_code, struct c_expr,
					     struct c_expr);
extern tree build_conditional_expr (location_t, tree, bool, tree, tree,
				    location_t, tree, tree, location_t);
extern tree build_compound_expr (location_t, tree, tree);
extern tree c_cast_expr (location_t, struct c_type_name *, tree);
extern tree build_c_cast (location_t, tree, tree);
extern void store_init_value (location_t, tree, tree, tree);
extern void maybe_warn_string_init (location_t, tree, struct c_expr);
extern void start_init (tree, tree, int, rich_location *);
extern void finish_init (void);
extern void really_start_incremental_init (tree);
extern void finish_implicit_inits (location_t, struct obstack *);
extern void push_init_level (location_t, int, struct obstack *);
extern struct c_expr pop_init_level (location_t, int, struct obstack *,
				     location_t);
extern void set_init_index (location_t, tree, tree, struct obstack *);
extern void set_init_label (location_t, tree, location_t, struct obstack *);
extern void process_init_element (location_t, struct c_expr, bool,
				  struct obstack *);
extern tree build_compound_literal (location_t, tree, tree, bool,
				    unsigned int);
extern void check_compound_literal_type (location_t, struct c_type_name *);
extern tree c_start_case (location_t, location_t, tree, bool);
extern void c_finish_case (tree, tree);
extern tree build_asm_expr (location_t, tree, tree, tree, tree, tree, bool,
			    bool);
extern tree build_asm_stmt (bool, tree);
extern int c_types_compatible_p (tree, tree);
extern tree c_begin_compound_stmt (bool);
extern tree c_end_compound_stmt (location_t, tree, bool);
extern void c_finish_if_stmt (location_t, tree, tree, tree);
extern void c_finish_loop (location_t, tree, tree, tree, tree, tree, bool);
extern tree c_begin_stmt_expr (void);
extern tree c_finish_stmt_expr (location_t, tree);
extern tree c_process_expr_stmt (location_t, tree);
extern tree c_finish_expr_stmt (location_t, tree);
extern tree c_finish_return (location_t, tree, tree);
extern tree c_finish_bc_stmt (location_t, tree *, bool);
extern tree c_finish_goto_label (location_t, tree);
extern tree c_finish_goto_ptr (location_t, tree);
extern tree c_expr_to_decl (tree, bool *, bool *);
extern tree c_finish_omp_construct (location_t, enum tree_code, tree, tree);
extern tree c_finish_oacc_data (location_t, tree, tree);
extern tree c_finish_oacc_host_data (location_t, tree, tree);
extern tree c_begin_omp_parallel (void);
extern tree c_finish_omp_parallel (location_t, tree, tree);
extern tree c_begin_omp_task (void);
extern tree c_finish_omp_task (location_t, tree, tree);
extern void c_finish_omp_cancel (location_t, tree);
extern void c_finish_omp_cancellation_point (location_t, tree);
extern tree c_finish_omp_clauses (tree, enum c_omp_region_type);
extern tree c_build_va_arg (location_t, tree, location_t, tree);
extern tree c_finish_transaction (location_t, tree, int);
extern bool c_tree_equal (tree, tree);
extern tree c_build_function_call_vec (location_t, vec<location_t>, tree,
				       vec<tree, va_gc> *, vec<tree, va_gc> *);
extern tree c_omp_clause_copy_ctor (tree, tree, tree);

/* Set to 0 at beginning of a function definition, set to 1 if
   a return statement that specifies a return value is seen.  */

extern int current_function_returns_value;

/* Set to 0 at beginning of a function definition, set to 1 if
   a return statement with no argument is seen.  */

extern int current_function_returns_null;

/* Set to 0 at beginning of a function definition, set to 1 if
   a call to a noreturn function is seen.  */

extern int current_function_returns_abnormally;

/* In c-decl.c */

/* Tell the binding oracle what kind of binding we are looking for.  */

enum c_oracle_request
{
  C_ORACLE_SYMBOL,
  C_ORACLE_TAG,
  C_ORACLE_LABEL
};

/* If this is non-NULL, then it is a "binding oracle" which can lazily
   create bindings when needed by the C compiler.  The oracle is told
   the name and type of the binding to create.  It can call pushdecl
   or the like to ensure the binding is visible; or do nothing,
   leaving the binding untouched.  c-decl.c takes note of when the
   oracle has been called and will not call it again if it fails to
   create a given binding.  */

typedef void c_binding_oracle_function (enum c_oracle_request, tree identifier);

extern c_binding_oracle_function *c_binding_oracle;

extern void c_finish_incomplete_decl (tree);
extern tree c_omp_reduction_id (enum tree_code, tree);
extern tree c_omp_reduction_decl (tree);
extern tree c_omp_reduction_lookup (tree, tree);
extern tree c_check_omp_declare_reduction_r (tree *, int *, void *);
extern void c_pushtag (location_t, tree, tree);
extern void c_bind (location_t, tree, bool);
extern bool tag_exists_p (enum tree_code, tree);

/* In c-errors.c */
extern bool pedwarn_c90 (location_t, int opt, const char *, ...)
    ATTRIBUTE_GCC_DIAG(3,4);
extern bool pedwarn_c99 (location_t, int opt, const char *, ...)
    ATTRIBUTE_GCC_DIAG(3,4);

extern void
set_c_expr_source_range (c_expr *expr,
			 location_t start, location_t finish);

extern void
set_c_expr_source_range (c_expr *expr,
			 source_range src_range);

/* In c-fold.c */
extern vec<tree> incomplete_record_decls;

#if CHECKING_P
namespace selftest {
  extern void run_c_tests (void);
} // namespace selftest
#endif /* #if CHECKING_P */


#endif /* ! GCC_C_TREE_H */
