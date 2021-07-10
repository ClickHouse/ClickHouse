/* Default macros to initialize the lang_hooks data structure.
   Copyright (C) 2001-2018 Free Software Foundation, Inc.
   Contributed by Alexandre Oliva  <aoliva@redhat.com>

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

#ifndef GCC_LANG_HOOKS_DEF_H
#define GCC_LANG_HOOKS_DEF_H

#include "hooks.h"

struct diagnostic_info;
class substring_loc;

/* Note to creators of new hooks:

   The macros in this file should NOT be surrounded by a
   #ifdef...#endif pair, since this file declares the defaults.  Each
   front end overrides any hooks it wishes to, in the file containing
   its struct lang_hooks, AFTER including this file.  */

/* See langhooks.h for the definition and documentation of each hook.  */

extern void lhd_do_nothing (void);
extern void lhd_do_nothing_t (tree);
extern void lhd_do_nothing_f (struct function *);
extern tree lhd_pass_through_t (tree);
extern void lhd_register_dumps (gcc::dump_manager *);
extern bool lhd_post_options (const char **);
extern alias_set_type lhd_get_alias_set (tree);
extern tree lhd_return_null_tree (tree);
extern tree lhd_return_null_const_tree (const_tree);
extern tree lhd_do_nothing_iii_return_null_tree (int, int, int);
extern void lhd_print_tree_nothing (FILE *, tree, int);
extern const char *lhd_decl_printable_name (tree, int);
extern const char *lhd_dwarf_name (tree, int);
extern int lhd_types_compatible_p (tree, tree);
extern void lhd_print_error_function (diagnostic_context *,
				      const char *, struct diagnostic_info *);
extern void lhd_set_decl_assembler_name (tree decl);
extern void lhd_overwrite_decl_assembler_name (tree decl, tree name);
extern bool lhd_warn_unused_global_decl (const_tree);
extern tree lhd_type_for_size (unsigned precision, int unsignedp);
extern void lhd_incomplete_type_error (location_t, const_tree, const_tree);
extern tree lhd_type_promotes_to (tree);
extern void lhd_register_builtin_type (tree, const char *);
extern bool lhd_decl_ok_for_sibcall (const_tree);
extern size_t lhd_tree_size (enum tree_code);
extern HOST_WIDE_INT lhd_to_target_charset (HOST_WIDE_INT);
extern tree lhd_expr_to_decl (tree, bool *, bool *);
extern tree lhd_builtin_function (tree);
extern tree lhd_enum_underlying_base_type (const_tree);

/* Declarations of default tree inlining hooks.  */
extern void lhd_initialize_diagnostics (diagnostic_context *);
extern void lhd_init_options (unsigned int,
			      struct cl_decoded_option *);
extern bool lhd_complain_wrong_lang_p (const struct cl_option *);
extern bool lhd_handle_option (size_t, const char *, int, int, location_t,
			       const struct cl_option_handlers *);


/* Declarations for tree gimplification hooks.  */
extern int lhd_gimplify_expr (tree *, gimple_seq *, gimple_seq *);
extern enum omp_clause_default_kind lhd_omp_predetermined_sharing (tree);
extern tree lhd_omp_assignment (tree, tree, tree);
extern void lhd_omp_finish_clause (tree, gimple_seq *);
struct gimplify_omp_ctx;
extern void lhd_omp_firstprivatize_type_sizes (struct gimplify_omp_ctx *,
					       tree);
extern bool lhd_omp_mappable_type (tree);
extern bool lhd_omp_scalar_p (tree);

extern const char *lhd_get_substring_location (const substring_loc &,
					       location_t *out_loc);
extern int lhd_decl_dwarf_attribute (const_tree, int);
extern int lhd_type_dwarf_attribute (const_tree, int);

#define LANG_HOOKS_NAME			"GNU unknown"
#define LANG_HOOKS_IDENTIFIER_SIZE	sizeof (struct lang_identifier)
#define LANG_HOOKS_INIT			hook_bool_void_false
#define LANG_HOOKS_FINISH		lhd_do_nothing
#define LANG_HOOKS_PARSE_FILE		lhd_do_nothing
#define LANG_HOOKS_OPTION_LANG_MASK	hook_uint_void_0
#define LANG_HOOKS_INIT_OPTIONS_STRUCT	hook_void_gcc_optionsp
#define LANG_HOOKS_INIT_OPTIONS		lhd_init_options
#define LANG_HOOKS_INITIALIZE_DIAGNOSTICS lhd_initialize_diagnostics
#define LANG_HOOKS_REGISTER_DUMPS	lhd_register_dumps
#define LANG_HOOKS_COMPLAIN_WRONG_LANG_P lhd_complain_wrong_lang_p
#define LANG_HOOKS_HANDLE_OPTION	lhd_handle_option
#define LANG_HOOKS_POST_OPTIONS		lhd_post_options
#define LANG_HOOKS_MISSING_NORETURN_OK_P hook_bool_tree_true
#define LANG_HOOKS_GET_ALIAS_SET	lhd_get_alias_set
#define LANG_HOOKS_FINISH_INCOMPLETE_DECL lhd_do_nothing_t
#define LANG_HOOKS_DUP_LANG_SPECIFIC_DECL lhd_do_nothing_t
#define LANG_HOOKS_SET_DECL_ASSEMBLER_NAME lhd_set_decl_assembler_name
#define LANG_HOOKS_OVERWRITE_DECL_ASSEMBLER_NAME lhd_overwrite_decl_assembler_name
#define LANG_HOOKS_PRINT_STATISTICS	lhd_do_nothing
#define LANG_HOOKS_PRINT_XNODE		lhd_print_tree_nothing
#define LANG_HOOKS_PRINT_DECL		lhd_print_tree_nothing
#define LANG_HOOKS_PRINT_TYPE		lhd_print_tree_nothing
#define LANG_HOOKS_PRINT_IDENTIFIER	lhd_print_tree_nothing
#define LANG_HOOKS_PRINT_ERROR_FUNCTION lhd_print_error_function
#define LANG_HOOKS_DECL_PRINTABLE_NAME	lhd_decl_printable_name
#define LANG_HOOKS_DWARF_NAME		lhd_dwarf_name
#define LANG_HOOKS_FREE_LANG_DATA	lhd_do_nothing_t
#define LANG_HOOKS_TREE_SIZE		lhd_tree_size
#define LANG_HOOKS_TYPES_COMPATIBLE_P	lhd_types_compatible_p
#define LANG_HOOKS_BUILTIN_FUNCTION	lhd_builtin_function
#define LANG_HOOKS_BUILTIN_FUNCTION_EXT_SCOPE	LANG_HOOKS_BUILTIN_FUNCTION
#define LANG_HOOKS_EXPR_TO_DECL		lhd_expr_to_decl
#define LANG_HOOKS_TO_TARGET_CHARSET	lhd_to_target_charset
#define LANG_HOOKS_INIT_TS		lhd_do_nothing
#define LANG_HOOKS_EH_PERSONALITY	lhd_gcc_personality
#define LANG_HOOKS_EH_RUNTIME_TYPE	lhd_pass_through_t
#define LANG_HOOKS_EH_PROTECT_CLEANUP_ACTIONS	NULL
#define LANG_HOOKS_BLOCK_MAY_FALLTHRU	hook_bool_const_tree_true
#define LANG_HOOKS_EH_USE_CXA_END_CLEANUP	false
#define LANG_HOOKS_DEEP_UNSHARING	false
#define LANG_HOOKS_CUSTOM_FUNCTION_DESCRIPTORS	false
#define LANG_HOOKS_EMITS_BEGIN_STMT	false
#define LANG_HOOKS_RUN_LANG_SELFTESTS   lhd_do_nothing
#define LANG_HOOKS_GET_SUBSTRING_LOCATION lhd_get_substring_location

/* Attribute hooks.  */
#define LANG_HOOKS_ATTRIBUTE_TABLE		NULL
#define LANG_HOOKS_COMMON_ATTRIBUTE_TABLE	NULL
#define LANG_HOOKS_FORMAT_ATTRIBUTE_TABLE	NULL

/* Tree inlining hooks.  */
#define LANG_HOOKS_TREE_INLINING_VAR_MOD_TYPE_P \
  hook_bool_tree_tree_false

#define LANG_HOOKS_TREE_INLINING_INITIALIZER { \
  LANG_HOOKS_TREE_INLINING_VAR_MOD_TYPE_P, \
}

/* Hooks for tree gimplification.  */
#define LANG_HOOKS_GIMPLIFY_EXPR lhd_gimplify_expr

/* Tree dump hooks.  */
extern bool lhd_tree_dump_dump_tree (void *, tree);
extern int lhd_tree_dump_type_quals (const_tree);
extern tree lhd_make_node (enum tree_code);

#define LANG_HOOKS_TREE_DUMP_DUMP_TREE_FN lhd_tree_dump_dump_tree
#define LANG_HOOKS_TREE_DUMP_TYPE_QUALS_FN lhd_tree_dump_type_quals

#define LANG_HOOKS_TREE_DUMP_INITIALIZER { \
  LANG_HOOKS_TREE_DUMP_DUMP_TREE_FN, \
  LANG_HOOKS_TREE_DUMP_TYPE_QUALS_FN \
}

/* Types hooks.  There are no reasonable defaults for most of them,
   so we create a compile-time error instead.  */
extern tree lhd_unit_size_without_reusable_padding (tree);

#define LANG_HOOKS_MAKE_TYPE lhd_make_node
#define LANG_HOOKS_CLASSIFY_RECORD	NULL
#define LANG_HOOKS_TYPE_FOR_SIZE	lhd_type_for_size
#define LANG_HOOKS_INCOMPLETE_TYPE_ERROR lhd_incomplete_type_error
#define LANG_HOOKS_GENERIC_TYPE_P	hook_bool_const_tree_false
#define LANG_HOOKS_GET_INNERMOST_GENERIC_PARMS hook_tree_const_tree_null
#define LANG_HOOKS_GET_INNERMOST_GENERIC_ARGS hook_tree_const_tree_null
#define LANG_HOOKS_FUNCTION_PARAMETER_PACK_P hook_bool_const_tree_false
#define LANG_HOOKS_GET_ARGUMENT_PACK_ELEMS hook_tree_const_tree_null
#define LANG_HOOKS_GENERIC_GENERIC_PARAMETER_DECL_P hook_bool_const_tree_false
#define LANG_HOOKS_FUNCTION_PARM_EXPANDED_FROM_PACK_P \
					hook_bool_tree_tree_false
#define LANG_HOOKS_GET_GENERIC_FUNCTION_DECL hook_tree_const_tree_null
#define LANG_HOOKS_TYPE_PROMOTES_TO lhd_type_promotes_to
#define LANG_HOOKS_REGISTER_BUILTIN_TYPE lhd_register_builtin_type
#define LANG_HOOKS_TYPE_MAX_SIZE	lhd_return_null_const_tree
#define LANG_HOOKS_OMP_FIRSTPRIVATIZE_TYPE_SIZES \
  lhd_omp_firstprivatize_type_sizes
#define LANG_HOOKS_OMP_MAPPABLE_TYPE	lhd_omp_mappable_type
#define LANG_HOOKS_TYPE_HASH_EQ		NULL
#define LANG_HOOKS_COPY_LANG_QUALIFIERS NULL
#define LANG_HOOKS_GET_ARRAY_DESCR_INFO	NULL
#define LANG_HOOKS_GET_SUBRANGE_BOUNDS	NULL
#define LANG_HOOKS_GET_TYPE_BIAS	NULL
#define LANG_HOOKS_DESCRIPTIVE_TYPE	NULL
#define LANG_HOOKS_RECONSTRUCT_COMPLEX_TYPE reconstruct_complex_type
#define LANG_HOOKS_ENUM_UNDERLYING_BASE_TYPE lhd_enum_underlying_base_type
#define LANG_HOOKS_GET_DEBUG_TYPE	NULL
#define LANG_HOOKS_GET_FIXED_POINT_TYPE_INFO NULL
#define LANG_HOOKS_TYPE_DWARF_ATTRIBUTE	lhd_type_dwarf_attribute
#define LANG_HOOKS_UNIT_SIZE_WITHOUT_REUSABLE_PADDING lhd_unit_size_without_reusable_padding

#define LANG_HOOKS_FOR_TYPES_INITIALIZER { \
  LANG_HOOKS_MAKE_TYPE, \
  LANG_HOOKS_CLASSIFY_RECORD, \
  LANG_HOOKS_TYPE_FOR_MODE, \
  LANG_HOOKS_TYPE_FOR_SIZE, \
  LANG_HOOKS_GENERIC_TYPE_P, \
  LANG_HOOKS_GET_ARGUMENT_PACK_ELEMS, \
  LANG_HOOKS_TYPE_PROMOTES_TO, \
  LANG_HOOKS_REGISTER_BUILTIN_TYPE, \
  LANG_HOOKS_INCOMPLETE_TYPE_ERROR, \
  LANG_HOOKS_TYPE_MAX_SIZE, \
  LANG_HOOKS_OMP_FIRSTPRIVATIZE_TYPE_SIZES, \
  LANG_HOOKS_OMP_MAPPABLE_TYPE, \
  LANG_HOOKS_TYPE_HASH_EQ, \
  LANG_HOOKS_COPY_LANG_QUALIFIERS, \
  LANG_HOOKS_GET_ARRAY_DESCR_INFO, \
  LANG_HOOKS_GET_SUBRANGE_BOUNDS, \
  LANG_HOOKS_GET_TYPE_BIAS, \
  LANG_HOOKS_DESCRIPTIVE_TYPE, \
  LANG_HOOKS_RECONSTRUCT_COMPLEX_TYPE, \
  LANG_HOOKS_ENUM_UNDERLYING_BASE_TYPE, \
  LANG_HOOKS_GET_DEBUG_TYPE, \
  LANG_HOOKS_GET_FIXED_POINT_TYPE_INFO, \
  LANG_HOOKS_TYPE_DWARF_ATTRIBUTE, \
  LANG_HOOKS_UNIT_SIZE_WITHOUT_REUSABLE_PADDING \
}

/* Declaration hooks.  */
#define LANG_HOOKS_GLOBAL_BINDINGS_P global_bindings_p
#define LANG_HOOKS_PUSHDECL	pushdecl
#define LANG_HOOKS_GETDECLS	getdecls
#define LANG_HOOKS_DECL_DWARF_ATTRIBUTE lhd_decl_dwarf_attribute
#define LANG_HOOKS_WARN_UNUSED_GLOBAL_DECL lhd_warn_unused_global_decl
#define LANG_HOOKS_POST_COMPILATION_PARSING_CLEANUPS NULL
#define LANG_HOOKS_DECL_OK_FOR_SIBCALL	lhd_decl_ok_for_sibcall
#define LANG_HOOKS_OMP_PRIVATIZE_BY_REFERENCE hook_bool_const_tree_false
#define LANG_HOOKS_OMP_PREDETERMINED_SHARING lhd_omp_predetermined_sharing
#define LANG_HOOKS_OMP_REPORT_DECL lhd_pass_through_t
#define LANG_HOOKS_OMP_DISREGARD_VALUE_EXPR hook_bool_tree_bool_false
#define LANG_HOOKS_OMP_PRIVATE_DEBUG_CLAUSE hook_bool_tree_bool_false
#define LANG_HOOKS_OMP_PRIVATE_OUTER_REF hook_bool_tree_false
#define LANG_HOOKS_OMP_CLAUSE_DEFAULT_CTOR hook_tree_tree_tree_tree_null
#define LANG_HOOKS_OMP_CLAUSE_COPY_CTOR lhd_omp_assignment
#define LANG_HOOKS_OMP_CLAUSE_ASSIGN_OP lhd_omp_assignment
#define LANG_HOOKS_OMP_CLAUSE_LINEAR_CTOR NULL
#define LANG_HOOKS_OMP_CLAUSE_DTOR hook_tree_tree_tree_null
#define LANG_HOOKS_OMP_FINISH_CLAUSE lhd_omp_finish_clause
#define LANG_HOOKS_OMP_SCALAR_P lhd_omp_scalar_p

#define LANG_HOOKS_DECLS { \
  LANG_HOOKS_GLOBAL_BINDINGS_P, \
  LANG_HOOKS_PUSHDECL, \
  LANG_HOOKS_GETDECLS, \
  LANG_HOOKS_DECL_DWARF_ATTRIBUTE, \
  LANG_HOOKS_GENERIC_GENERIC_PARAMETER_DECL_P, \
  LANG_HOOKS_FUNCTION_PARM_EXPANDED_FROM_PACK_P, \
  LANG_HOOKS_GET_GENERIC_FUNCTION_DECL, \
  LANG_HOOKS_WARN_UNUSED_GLOBAL_DECL, \
  LANG_HOOKS_POST_COMPILATION_PARSING_CLEANUPS, \
  LANG_HOOKS_DECL_OK_FOR_SIBCALL, \
  LANG_HOOKS_OMP_PRIVATIZE_BY_REFERENCE, \
  LANG_HOOKS_OMP_PREDETERMINED_SHARING, \
  LANG_HOOKS_OMP_REPORT_DECL, \
  LANG_HOOKS_OMP_DISREGARD_VALUE_EXPR, \
  LANG_HOOKS_OMP_PRIVATE_DEBUG_CLAUSE, \
  LANG_HOOKS_OMP_PRIVATE_OUTER_REF, \
  LANG_HOOKS_OMP_CLAUSE_DEFAULT_CTOR, \
  LANG_HOOKS_OMP_CLAUSE_COPY_CTOR, \
  LANG_HOOKS_OMP_CLAUSE_ASSIGN_OP, \
  LANG_HOOKS_OMP_CLAUSE_LINEAR_CTOR, \
  LANG_HOOKS_OMP_CLAUSE_DTOR, \
  LANG_HOOKS_OMP_FINISH_CLAUSE, \
  LANG_HOOKS_OMP_SCALAR_P \
}

/* LTO hooks.  */
extern void lhd_begin_section (const char *);
extern void lhd_append_data (const void *, size_t, void *);
extern void lhd_end_section (void);

#define LANG_HOOKS_BEGIN_SECTION lhd_begin_section
#define LANG_HOOKS_APPEND_DATA lhd_append_data
#define LANG_HOOKS_END_SECTION lhd_end_section

#define LANG_HOOKS_LTO { \
  LANG_HOOKS_BEGIN_SECTION, \
  LANG_HOOKS_APPEND_DATA, \
  LANG_HOOKS_END_SECTION \
}

/* The whole thing.  The structure is defined in langhooks.h.  */
#define LANG_HOOKS_INITIALIZER { \
  LANG_HOOKS_NAME, \
  LANG_HOOKS_IDENTIFIER_SIZE, \
  LANG_HOOKS_FREE_LANG_DATA, \
  LANG_HOOKS_TREE_SIZE, \
  LANG_HOOKS_OPTION_LANG_MASK, \
  LANG_HOOKS_INIT_OPTIONS_STRUCT, \
  LANG_HOOKS_INIT_OPTIONS, \
  LANG_HOOKS_INITIALIZE_DIAGNOSTICS, \
  LANG_HOOKS_REGISTER_DUMPS, \
  LANG_HOOKS_COMPLAIN_WRONG_LANG_P, \
  LANG_HOOKS_HANDLE_OPTION, \
  LANG_HOOKS_POST_OPTIONS, \
  LANG_HOOKS_INIT, \
  LANG_HOOKS_FINISH, \
  LANG_HOOKS_PARSE_FILE, \
  LANG_HOOKS_MISSING_NORETURN_OK_P, \
  LANG_HOOKS_GET_ALIAS_SET, \
  LANG_HOOKS_FINISH_INCOMPLETE_DECL, \
  LANG_HOOKS_DUP_LANG_SPECIFIC_DECL, \
  LANG_HOOKS_SET_DECL_ASSEMBLER_NAME, \
  LANG_HOOKS_OVERWRITE_DECL_ASSEMBLER_NAME, \
  LANG_HOOKS_PRINT_STATISTICS, \
  LANG_HOOKS_PRINT_XNODE, \
  LANG_HOOKS_PRINT_DECL, \
  LANG_HOOKS_PRINT_TYPE, \
  LANG_HOOKS_PRINT_IDENTIFIER, \
  LANG_HOOKS_DECL_PRINTABLE_NAME, \
  LANG_HOOKS_DWARF_NAME, \
  LANG_HOOKS_TYPES_COMPATIBLE_P, \
  LANG_HOOKS_PRINT_ERROR_FUNCTION, \
  LANG_HOOKS_TO_TARGET_CHARSET, \
  LANG_HOOKS_ATTRIBUTE_TABLE, \
  LANG_HOOKS_COMMON_ATTRIBUTE_TABLE, \
  LANG_HOOKS_FORMAT_ATTRIBUTE_TABLE, \
  LANG_HOOKS_TREE_INLINING_INITIALIZER, \
  LANG_HOOKS_TREE_DUMP_INITIALIZER, \
  LANG_HOOKS_DECLS, \
  LANG_HOOKS_FOR_TYPES_INITIALIZER, \
  LANG_HOOKS_LTO, \
  LANG_HOOKS_GET_INNERMOST_GENERIC_PARMS, \
  LANG_HOOKS_GET_INNERMOST_GENERIC_ARGS, \
  LANG_HOOKS_FUNCTION_PARAMETER_PACK_P, \
  LANG_HOOKS_GIMPLIFY_EXPR, \
  LANG_HOOKS_BUILTIN_FUNCTION, \
  LANG_HOOKS_BUILTIN_FUNCTION_EXT_SCOPE, \
  LANG_HOOKS_INIT_TS,          \
  LANG_HOOKS_EXPR_TO_DECL, \
  LANG_HOOKS_EH_PERSONALITY, \
  LANG_HOOKS_EH_RUNTIME_TYPE, \
  LANG_HOOKS_EH_PROTECT_CLEANUP_ACTIONS, \
  LANG_HOOKS_BLOCK_MAY_FALLTHRU, \
  LANG_HOOKS_EH_USE_CXA_END_CLEANUP, \
  LANG_HOOKS_DEEP_UNSHARING, \
  LANG_HOOKS_CUSTOM_FUNCTION_DESCRIPTORS, \
  LANG_HOOKS_EMITS_BEGIN_STMT, \
  LANG_HOOKS_RUN_LANG_SELFTESTS, \
  LANG_HOOKS_GET_SUBSTRING_LOCATION \
}

#endif /* GCC_LANG_HOOKS_DEF_H */
