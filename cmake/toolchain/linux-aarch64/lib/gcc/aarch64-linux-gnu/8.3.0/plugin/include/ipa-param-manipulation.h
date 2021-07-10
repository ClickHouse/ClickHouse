/* Manipulation of formal and actual parameters of functions and function
   calls.
   Copyright (C) 2017-2018 Free Software Foundation, Inc.

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

#ifndef IPA_PARAM_MANIPULATION_H
#define IPA_PARAM_MANIPULATION_H

/* Operation to be performed for the parameter in ipa_parm_adjustment
   below.  */
enum ipa_parm_op {
  IPA_PARM_OP_NONE,

  /* This describes a brand new parameter.

     The field `type' should be set to the new type, `arg_prefix'
     should be set to the string prefix for the new DECL_NAME, and
     `new_decl' will ultimately hold the newly created argument.  */
  IPA_PARM_OP_NEW,

  /* This new parameter is an unmodified parameter at index base_index. */
  IPA_PARM_OP_COPY,

  /* This adjustment describes a parameter that is about to be removed
     completely.  Most users will probably need to book keep those so that they
     don't leave behinfd any non default def ssa names belonging to them.  */
  IPA_PARM_OP_REMOVE
};

/* Structure to describe transformations of formal parameters and actual
   arguments.  Each instance describes one new parameter and they are meant to
   be stored in a vector.  Additionally, most users will probably want to store
   adjustments about parameters that are being removed altogether so that SSA
   names belonging to them can be replaced by SSA names of an artificial
   variable.  */
struct ipa_parm_adjustment
{
  /* The original PARM_DECL itself, helpful for processing of the body of the
     function itself.  Intended for traversing function bodies.
     ipa_modify_formal_parameters, ipa_modify_call_arguments and
     ipa_combine_adjustments ignore this and use base_index.
     ipa_modify_formal_parameters actually sets this.  */
  tree base;

  /* Type of the new parameter.  However, if by_ref is true, the real type will
     be a pointer to this type.  */
  tree type;

  /* Alias refrerence type to be used in MEM_REFs when adjusting caller
     arguments.  */
  tree alias_ptr_type;

  /* The new declaration when creating/replacing a parameter.  Created
     by ipa_modify_formal_parameters, useful for functions modifying
     the body accordingly.  For brand new arguments, this is the newly
     created argument.  */
  tree new_decl;

  /* New declaration of a substitute variable that we may use to replace all
     non-default-def ssa names when a parm decl is going away.  */
  tree new_ssa_base;

  /* If non-NULL and the original parameter is to be removed (copy_param below
     is NULL), this is going to be its nonlocalized vars value.  */
  tree nonlocal_value;

  /* This holds the prefix to be used for the new DECL_NAME.  */
  const char *arg_prefix;

  /* Offset into the original parameter (for the cases when the new parameter
     is a component of an original one).  */
  poly_int64_pod offset;

  /* Zero based index of the original parameter this one is based on.  */
  int base_index;

  /* Whether this parameter is a new parameter, a copy of an old one,
     or one about to be removed.  */
  enum ipa_parm_op op;

  /* Storage order of the original parameter (for the cases when the new
     parameter is a component of an original one).  */
  unsigned reverse : 1;

  /* The parameter is to be passed by reference.  */
  unsigned by_ref : 1;
};

typedef vec<ipa_parm_adjustment> ipa_parm_adjustment_vec;

vec<tree> ipa_get_vector_of_formal_parms (tree fndecl);
vec<tree> ipa_get_vector_of_formal_parm_types (tree fntype);
void ipa_modify_formal_parameters (tree fndecl, ipa_parm_adjustment_vec);
void ipa_modify_call_arguments (struct cgraph_edge *, gcall *,
				ipa_parm_adjustment_vec);
ipa_parm_adjustment_vec ipa_combine_adjustments (ipa_parm_adjustment_vec,
						 ipa_parm_adjustment_vec);
void ipa_dump_param_adjustments (FILE *, ipa_parm_adjustment_vec, tree);

bool ipa_modify_expr (tree *, bool, ipa_parm_adjustment_vec);
ipa_parm_adjustment *ipa_get_adjustment_candidate (tree **, bool *,
						   ipa_parm_adjustment_vec,
						   bool);

#endif	/* IPA_PARAM_MANIPULATION_H */
