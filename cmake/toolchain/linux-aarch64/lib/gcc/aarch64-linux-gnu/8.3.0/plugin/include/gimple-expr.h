/* Header file for gimple decl, type and expressions.
   Copyright (C) 2013-2018 Free Software Foundation, Inc.

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

#ifndef GCC_GIMPLE_EXPR_H
#define GCC_GIMPLE_EXPR_H

extern bool useless_type_conversion_p (tree, tree);


extern void gimple_set_body (tree, gimple_seq);
extern gimple_seq gimple_body (tree);
extern bool gimple_has_body_p (tree);
extern const char *gimple_decl_printable_name (tree, int);
extern tree copy_var_decl (tree, tree, tree);
extern tree create_tmp_var_name (const char *);
extern tree create_tmp_var_raw (tree, const char * = NULL);
extern tree create_tmp_var (tree, const char * = NULL);
extern tree create_tmp_reg (tree, const char * = NULL);
extern tree create_tmp_reg_fn (struct function *, tree, const char *);


extern void extract_ops_from_tree (tree, enum tree_code *, tree *, tree *,
				   tree *);
extern void gimple_cond_get_ops_from_tree (tree, enum tree_code *, tree *,
					   tree *);
extern bool is_gimple_lvalue (tree);
extern bool is_gimple_condexpr (tree);
extern bool is_gimple_address (const_tree);
extern bool is_gimple_invariant_address (const_tree);
extern bool is_gimple_ip_invariant_address (const_tree);
extern bool is_gimple_min_invariant (const_tree);
extern bool is_gimple_ip_invariant (const_tree);
extern bool is_gimple_reg (tree);
extern bool is_gimple_val (tree);
extern bool is_gimple_asm_val (tree);
extern bool is_gimple_min_lval (tree);
extern bool is_gimple_call_addr (tree);
extern bool is_gimple_mem_ref_addr (tree);
extern void flush_mark_addressable_queue (void);
extern void mark_addressable (tree);
extern bool is_gimple_reg_rhs (tree);

/* Return true if a conversion from either type of TYPE1 and TYPE2
   to the other is not required.  Otherwise return false.  */

static inline bool
types_compatible_p (tree type1, tree type2)
{
  return (type1 == type2
	  || (useless_type_conversion_p (type1, type2)
	      && useless_type_conversion_p (type2, type1)));
}

/* Return true if TYPE is a suitable type for a scalar register variable.  */

static inline bool
is_gimple_reg_type (tree type)
{
  return !AGGREGATE_TYPE_P (type);
}

/* Return true if T is a variable.  */

static inline bool
is_gimple_variable (tree t)
{
  return (TREE_CODE (t) == VAR_DECL
	  || TREE_CODE (t) == PARM_DECL
	  || TREE_CODE (t) == RESULT_DECL
	  || TREE_CODE (t) == SSA_NAME);
}

/*  Return true if T is a GIMPLE identifier (something with an address).  */

static inline bool
is_gimple_id (tree t)
{
  return (is_gimple_variable (t)
	  || TREE_CODE (t) == FUNCTION_DECL
	  || TREE_CODE (t) == LABEL_DECL
	  || TREE_CODE (t) == CONST_DECL
	  /* Allow string constants, since they are addressable.  */
	  || TREE_CODE (t) == STRING_CST);
}

/* Return true if OP, an SSA name or a DECL is a virtual operand.  */

static inline bool
virtual_operand_p (tree op)
{
  if (TREE_CODE (op) == SSA_NAME)
    return SSA_NAME_IS_VIRTUAL_OPERAND (op);

  if (TREE_CODE (op) == VAR_DECL)
    return VAR_DECL_IS_VIRTUAL_OPERAND (op);

  return false;
}

/*  Return true if T is something whose address can be taken.  */

static inline bool
is_gimple_addressable (tree t)
{
  return (is_gimple_id (t) || handled_component_p (t)
	  || TREE_CODE (t) == TARGET_MEM_REF
	  || TREE_CODE (t) == MEM_REF);
}

/* Return true if T is a valid gimple constant.  */

static inline bool
is_gimple_constant (const_tree t)
{
  switch (TREE_CODE (t))
    {
    case INTEGER_CST:
    case POLY_INT_CST:
    case REAL_CST:
    case FIXED_CST:
    case COMPLEX_CST:
    case VECTOR_CST:
    case STRING_CST:
      return true;

    default:
      return false;
    }
}

/* A wrapper around extract_ops_from_tree with 3 ops, for callers which
   expect to see only a maximum of two operands.  */

static inline void
extract_ops_from_tree (tree expr, enum tree_code *code, tree *op0,
		       tree *op1)
{
  tree op2;
  extract_ops_from_tree (expr, code, op0, op1, &op2);
  gcc_assert (op2 == NULL_TREE);
}

/* Given a valid GIMPLE_CALL function address return the FUNCTION_DECL
   associated with the callee if known.  Otherwise return NULL_TREE.  */

static inline tree
gimple_call_addr_fndecl (const_tree fn)
{
  if (fn && TREE_CODE (fn) == ADDR_EXPR)
    {
      tree fndecl = TREE_OPERAND (fn, 0);
      if (TREE_CODE (fndecl) == MEM_REF
	  && TREE_CODE (TREE_OPERAND (fndecl, 0)) == ADDR_EXPR
	  && integer_zerop (TREE_OPERAND (fndecl, 1)))
	fndecl = TREE_OPERAND (TREE_OPERAND (fndecl, 0), 0);
      if (TREE_CODE (fndecl) == FUNCTION_DECL)
	return fndecl;
    }
  return NULL_TREE;
}

#endif /* GCC_GIMPLE_EXPR_H */
