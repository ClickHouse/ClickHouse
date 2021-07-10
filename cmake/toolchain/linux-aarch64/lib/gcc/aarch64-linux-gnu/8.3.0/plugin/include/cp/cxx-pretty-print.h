/* Interface for the GNU C++ pretty-printer.
   Copyright (C) 2003-2018 Free Software Foundation, Inc.
   Contributed by Gabriel Dos Reis <gdr@integrable-solutions.net>

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

#ifndef GCC_CXX_PRETTY_PRINT_H
#define GCC_CXX_PRETTY_PRINT_H

#include "c-family/c-pretty-print.h"

enum cxx_pretty_printer_flags
{
  /* Ask for a qualified-id.  */
  pp_cxx_flag_default_argument = 1 << pp_c_flag_last_bit
};

struct cxx_pretty_printer : c_pretty_printer
{
  cxx_pretty_printer ();

  void constant (tree);
  void id_expression (tree);
  void primary_expression (tree);
  void postfix_expression (tree);
  void unary_expression (tree);
  void multiplicative_expression (tree);
  void conditional_expression (tree);
  void assignment_expression (tree);
  void expression (tree);
  void type_id (tree);
  void statement (tree);
  void declaration (tree);
  void declaration_specifiers (tree);
  void simple_type_specifier (tree);
  void function_specifier (tree);
  void declarator (tree);
  void direct_declarator (tree);
  void abstract_declarator (tree);
  void direct_abstract_declarator (tree);

  /* This is the enclosing scope of the entity being pretty-printed.  */
  tree enclosing_scope;
};

#define pp_cxx_cv_qualifier_seq(PP, T)   \
   pp_c_type_qualifier_list (PP, T)
#define pp_cxx_cv_qualifiers(PP, CV, FT) \
   pp_c_cv_qualifiers (PP, CV, FT)

#define pp_cxx_whitespace(PP)		pp_c_whitespace (PP)
#define pp_cxx_left_paren(PP)		pp_c_left_paren (PP)
#define pp_cxx_right_paren(PP)		pp_c_right_paren (PP)
#define pp_cxx_left_brace(PP)		pp_c_left_brace (PP)
#define pp_cxx_right_brace(PP)		pp_c_right_brace (PP)
#define pp_cxx_left_bracket(PP)		pp_c_left_bracket (PP)
#define pp_cxx_right_bracket(PP)	pp_c_right_bracket (PP)
#define pp_cxx_dot(PP)			pp_c_dot (PP)
#define pp_cxx_ampersand(PP)		pp_c_ampersand (PP)
#define pp_cxx_star(PP)			pp_c_star (PP)
#define pp_cxx_arrow(PP)		pp_c_arrow (PP)
#define pp_cxx_semicolon(PP)		pp_c_semicolon (PP)
#define pp_cxx_complement(PP)		pp_c_complement (PP)

#define pp_cxx_ws_string(PP, I)		pp_c_ws_string (PP, I)
#define pp_cxx_identifier(PP, I)	pp_c_identifier (PP, I)
#define pp_cxx_tree_identifier(PP, T) \
  pp_c_tree_identifier (PP, T)

void pp_cxx_begin_template_argument_list (cxx_pretty_printer *);
void pp_cxx_end_template_argument_list (cxx_pretty_printer *);
void pp_cxx_colon_colon (cxx_pretty_printer *);
void pp_cxx_separate_with (cxx_pretty_printer *, int);

void pp_cxx_canonical_template_parameter (cxx_pretty_printer *, tree);
void pp_cxx_trait_expression (cxx_pretty_printer *, tree);
void pp_cxx_va_arg_expression (cxx_pretty_printer *, tree);
void pp_cxx_offsetof_expression (cxx_pretty_printer *, tree);
void pp_cxx_addressof_expression (cxx_pretty_printer *, tree);
void pp_cxx_userdef_literal (cxx_pretty_printer *, tree);
void pp_cxx_requires_clause (cxx_pretty_printer *, tree);
void pp_cxx_requires_expr (cxx_pretty_printer *, tree);
void pp_cxx_simple_requirement (cxx_pretty_printer *, tree);
void pp_cxx_type_requirement (cxx_pretty_printer *, tree);
void pp_cxx_compound_requirement (cxx_pretty_printer *, tree);
void pp_cxx_nested_requirement (cxx_pretty_printer *, tree);
void pp_cxx_predicate_constraint (cxx_pretty_printer *, tree);
void pp_cxx_expression_constraint (cxx_pretty_printer *, tree);
void pp_cxx_type_constraint (cxx_pretty_printer *, tree);
void pp_cxx_implicit_conversion_constraint (cxx_pretty_printer *, tree);
void pp_cxx_argument_deduction_constraint (cxx_pretty_printer *, tree);
void pp_cxx_exception_constraint (cxx_pretty_printer *, tree);
void pp_cxx_parameterized_constraint (cxx_pretty_printer *, tree);
void pp_cxx_conjunction (cxx_pretty_printer *, tree);
void pp_cxx_disjunction (cxx_pretty_printer *, tree);
void pp_cxx_constraint (cxx_pretty_printer *, tree);
void pp_cxx_constrained_type_spec (cxx_pretty_printer *, tree);

#endif /* GCC_CXX_PRETTY_PRINT_H */
