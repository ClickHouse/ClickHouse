/* Various declarations for language-independent pretty-print
   subroutines that are only for use in the compilers proper and not
   the driver or other programs.
   Copyright (C) 2002-2018 Free Software Foundation, Inc.

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

#ifndef GCC_TREE_PRETTY_PRINT_H
#define GCC_TREE_PRETTY_PRINT_H

#include "pretty-print.h"

#define pp_unsupported_tree(PP, T)                         \
  pp_verbatim (PP, "#%qs not supported by %s#", \
	       get_tree_code_name (TREE_CODE (T)), __FUNCTION__)

#define pp_ti_abstract_origin(TI) ((tree *) (TI)->x_data)


extern void debug_generic_expr (tree);
extern void debug_generic_stmt (tree);
extern void debug_tree_chain (tree);
extern void print_generic_decl (FILE *, tree, dump_flags_t);
extern void print_generic_stmt (FILE *, tree, dump_flags_t = TDF_NONE);
extern void print_generic_stmt_indented (FILE *, tree, dump_flags_t, int);
extern void print_generic_expr (FILE *, tree, dump_flags_t = TDF_NONE);
extern void dump_omp_clauses (pretty_printer *, tree, int, dump_flags_t);
extern int dump_generic_node (pretty_printer *, tree, int, dump_flags_t, bool);
extern void print_declaration (pretty_printer *, tree, int, dump_flags_t);
extern int op_code_prio (enum tree_code);
extern int op_prio (const_tree);
extern const char *op_symbol_code (enum tree_code);
extern void print_call_name (pretty_printer *, tree, dump_flags_t);
extern void percent_K_format (text_info *, tree);
extern void pp_tree_identifier (pretty_printer *, tree);
extern void dump_function_header (FILE *, tree, dump_flags_t);
extern void pp_double_int (pretty_printer *pp, double_int d, bool uns);
extern void dump_location (pretty_printer *buffer, location_t loc);

#endif /* ! GCC_TREE_PRETTY_PRINT_H */
