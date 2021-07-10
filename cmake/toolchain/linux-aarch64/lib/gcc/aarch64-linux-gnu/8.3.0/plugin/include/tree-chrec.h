/* Chains of recurrences.
   Copyright (C) 2003-2018 Free Software Foundation, Inc.
   Contributed by Sebastian Pop <pop@cri.ensmp.fr>

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

#ifndef GCC_TREE_CHREC_H
#define GCC_TREE_CHREC_H

/* The following trees are unique elements.  Thus the comparison of another
   element to these elements should be done on the pointer to these trees,
   and not on their value.  */

extern tree chrec_not_analyzed_yet;
extern GTY(()) tree chrec_dont_know;
extern GTY(()) tree chrec_known;

/* After having added an automatically generated element, please
   include it in the following function.  */

static inline bool
automatically_generated_chrec_p (const_tree chrec)
{
  return (chrec == chrec_dont_know
	  || chrec == chrec_known);
}

/* The tree nodes aka. CHRECs.  */

static inline bool
tree_is_chrec (const_tree expr)
{
  if (TREE_CODE (expr) == POLYNOMIAL_CHREC
      || automatically_generated_chrec_p (expr))
    return true;
  else
    return false;
}


enum ev_direction {EV_DIR_GROWS, EV_DIR_DECREASES, EV_DIR_UNKNOWN};
enum ev_direction scev_direction (const_tree);

/* Chrec folding functions.  */
extern tree chrec_fold_plus (tree, tree, tree);
extern tree chrec_fold_minus (tree, tree, tree);
extern tree chrec_fold_multiply (tree, tree, tree);
extern tree chrec_convert (tree, tree, gimple *, bool = true, tree = NULL);
extern tree chrec_convert_rhs (tree, tree, gimple *);
extern tree chrec_convert_aggressive (tree, tree, bool *);

/* Operations.  */
extern tree chrec_apply (unsigned, tree, tree);
extern tree chrec_apply_map (tree, vec<tree> );
extern tree chrec_replace_initial_condition (tree, tree);
extern tree initial_condition (tree);
extern tree initial_condition_in_loop_num (tree, unsigned);
extern tree evolution_part_in_loop_num (tree, unsigned);
extern tree hide_evolution_in_other_loops_than_loop (tree, unsigned);
extern tree reset_evolution_in_loop (unsigned, tree, tree);
extern tree chrec_merge (tree, tree);
extern void for_each_scev_op (tree *, bool (*) (tree *, void *), void *);
extern bool convert_affine_scev (struct loop *, tree, tree *, tree *, gimple *,
				 bool, tree = NULL);

/* Observers.  */
extern bool eq_evolutions_p (const_tree, const_tree);
extern bool is_multivariate_chrec (const_tree);
extern bool chrec_contains_symbols (const_tree);
extern bool chrec_contains_symbols_defined_in_loop (const_tree, unsigned);
extern bool chrec_contains_undetermined (const_tree);
extern bool tree_contains_chrecs (const_tree, int *);
extern bool evolution_function_is_affine_multivariate_p (const_tree, int);
extern bool evolution_function_is_univariate_p (const_tree);
extern unsigned nb_vars_in_chrec (tree);
extern bool evolution_function_is_invariant_p (tree, int);
extern bool scev_is_linear_expression (tree);
extern bool evolution_function_right_is_integer_cst (const_tree);

/* Determines whether CHREC is equal to zero.  */

static inline bool
chrec_zerop (const_tree chrec)
{
  if (chrec == NULL_TREE)
    return false;

  if (TREE_CODE (chrec) == INTEGER_CST)
    return integer_zerop (chrec);

  return false;
}

/* Determines whether CHREC is a loop invariant with respect to LOOP_NUM.
   Set the result in RES and return true when the property can be computed.  */

static inline bool
no_evolution_in_loop_p (tree chrec, unsigned loop_num, bool *res)
{
  tree scev;

  if (chrec == chrec_not_analyzed_yet
      || chrec == chrec_dont_know
      || chrec_contains_symbols_defined_in_loop (chrec, loop_num))
    return false;

  STRIP_NOPS (chrec);
  scev = hide_evolution_in_other_loops_than_loop (chrec, loop_num);
  *res = !tree_contains_chrecs (scev, NULL);
  return true;
}

/* Build a polynomial chain of recurrence.  */

static inline tree
build_polynomial_chrec (unsigned loop_num,
			tree left,
			tree right)
{
  bool val;

  if (left == chrec_dont_know
      || right == chrec_dont_know)
    return chrec_dont_know;

  if (!no_evolution_in_loop_p (left, loop_num, &val)
      || !val)
    return chrec_dont_know;

  /* Types of left and right sides of a chrec should be compatible, but
     pointer CHRECs are special in that the evolution is of ptroff type.  */
  if (POINTER_TYPE_P (TREE_TYPE (left)))
    gcc_checking_assert (ptrofftype_p (TREE_TYPE (right)));
  else
    {
      /* Pointer types should occur only on the left hand side, i.e. in
	 the base of the chrec, and not in the step.  */
      gcc_checking_assert (!POINTER_TYPE_P (TREE_TYPE (right))
			   && types_compatible_p (TREE_TYPE (left),
						  TREE_TYPE (right)));
    }

  if (chrec_zerop (right))
    return left;

  tree chrec = build2 (POLYNOMIAL_CHREC, TREE_TYPE (left), left, right);
  CHREC_VARIABLE (chrec) = loop_num;
  return chrec;
}

/* Determines whether the expression CHREC is a constant.  */

static inline bool
evolution_function_is_constant_p (const_tree chrec)
{
  if (chrec == NULL_TREE)
    return false;

  if (CONSTANT_CLASS_P (chrec))
    return true;
  return is_gimple_min_invariant (chrec);
}

/* Determine whether CHREC is an affine evolution function in LOOPNUM.  */

static inline bool
evolution_function_is_affine_in_loop (const_tree chrec, int loopnum)
{
  if (chrec == NULL_TREE)
    return false;

  switch (TREE_CODE (chrec))
    {
    case POLYNOMIAL_CHREC:
      if (evolution_function_is_invariant_p (CHREC_LEFT (chrec), loopnum)
	  && evolution_function_is_invariant_p (CHREC_RIGHT (chrec), loopnum))
	return true;
      else
	return false;

    default:
      return false;
    }
}

/* Determine whether CHREC is an affine evolution function or not.  */

static inline bool
evolution_function_is_affine_p (const_tree chrec)
{
  return chrec
    && TREE_CODE (chrec) == POLYNOMIAL_CHREC
    && evolution_function_is_invariant_p (CHREC_RIGHT (chrec),
					  CHREC_VARIABLE (chrec))
    && (TREE_CODE (CHREC_RIGHT (chrec)) != POLYNOMIAL_CHREC
	|| evolution_function_is_affine_p (CHREC_RIGHT (chrec)));
}

/* Determines whether EXPR does not contains chrec expressions.  */

static inline bool
tree_does_not_contain_chrecs (const_tree expr)
{
  return !tree_contains_chrecs (expr, NULL);
}

/* Returns the type of the chrec.  */

static inline tree
chrec_type (const_tree chrec)
{
  if (automatically_generated_chrec_p (chrec))
    return NULL_TREE;

  return TREE_TYPE (chrec);
}

static inline tree
chrec_fold_op (enum tree_code code, tree type, tree op0, tree op1)
{
  switch (code)
    {
    case PLUS_EXPR:
      return chrec_fold_plus (type, op0, op1);

    case MINUS_EXPR:
      return chrec_fold_minus (type, op0, op1);

    case MULT_EXPR:
      return chrec_fold_multiply (type, op0, op1);

    default:
      gcc_unreachable ();
    }

}

#endif  /* GCC_TREE_CHREC_H  */
