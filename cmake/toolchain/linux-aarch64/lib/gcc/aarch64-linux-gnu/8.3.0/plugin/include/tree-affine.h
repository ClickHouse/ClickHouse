/* Operations with affine combinations of trees.
   Copyright (C) 2005-2018 Free Software Foundation, Inc.

This file is part of GCC.

GCC is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation; either version 3, or (at your option) any
later version.

GCC is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
for more details.

You should have received a copy of the GNU General Public License
along with GCC; see the file COPYING3.  If not see
<http://www.gnu.org/licenses/>.  */

/* Affine combination of trees.  We keep track of at most MAX_AFF_ELTS elements
   to make things simpler; this is sufficient in most cases.  */

#ifndef GCC_TREE_AFFINE_H
#define GCC_TREE_AFFINE_H


#define MAX_AFF_ELTS 8

/* Element of an affine combination.  */

struct aff_comb_elt
{
  /* The value of the element.  */
  tree val;

  /* Its coefficient in the combination.  */
  widest_int coef;
};

struct aff_tree
{
  /* Type of the result of the combination.  */
  tree type;

  /* Constant offset.  */
  poly_widest_int offset;

  /* Number of elements of the combination.  */
  unsigned n;

  /* Elements and their coefficients.  Type of elements may be different from
     TYPE, but their sizes must be the same (STRIP_NOPS is applied to the
     elements).

     The coefficients are always sign extended from the precision of TYPE
     (regardless of signedness of TYPE).  */
  struct aff_comb_elt elts[MAX_AFF_ELTS];

  /* Remainder of the expression.  Usually NULL, used only if there are more
     than MAX_AFF_ELTS elements.  Type of REST will be either sizetype for
     TYPE of POINTER_TYPEs or TYPE.  */
  tree rest;
};

struct name_expansion;

void aff_combination_const (aff_tree *, tree, const poly_widest_int &);
void aff_combination_elt (aff_tree *, tree, tree);
void aff_combination_scale (aff_tree *, const widest_int &);
void aff_combination_mult (aff_tree *, aff_tree *, aff_tree *);
void aff_combination_add (aff_tree *, aff_tree *);
void aff_combination_add_elt (aff_tree *, tree, const widest_int &);
void aff_combination_remove_elt (aff_tree *, unsigned);
void aff_combination_convert (aff_tree *, tree);
void tree_to_aff_combination (tree, tree, aff_tree *);
tree aff_combination_to_tree (aff_tree *);
void unshare_aff_combination (aff_tree *);
bool aff_combination_constant_multiple_p (aff_tree *, aff_tree *,
					  poly_widest_int *);
void aff_combination_expand (aff_tree *, hash_map<tree, name_expansion *> **);
void tree_to_aff_combination_expand (tree, tree, aff_tree *,
				     hash_map<tree, name_expansion *> **);
tree get_inner_reference_aff (tree, aff_tree *, poly_widest_int *);
void free_affine_expand_cache (hash_map<tree, name_expansion *> **);
bool aff_comb_cannot_overlap_p (aff_tree *, const poly_widest_int &,
				const poly_widest_int &);

/* Debugging functions.  */
void debug_aff (aff_tree *);

/* Return AFF's type.  */
inline tree
aff_combination_type (aff_tree *aff)
{
  return aff->type;
}

/* Return true if AFF is actually ZERO.  */
inline bool
aff_combination_zero_p (aff_tree *aff)
{
  if (!aff)
    return true;

  if (aff->n == 0 && known_eq (aff->offset, 0))
    return true;

  return false;
}

/* Return true if AFF is actually const.  */
inline bool
aff_combination_const_p (aff_tree *aff)
{
  return (aff == NULL || aff->n == 0);
}

/* Return true iff AFF contains one (negated) singleton variable.  Users need
   to make sure AFF points to a valid combination.  */
inline bool
aff_combination_singleton_var_p (aff_tree *aff)
{
  return (aff->n == 1
	  && known_eq (aff->offset, 0)
	  && (aff->elts[0].coef == 1 || aff->elts[0].coef == -1));
}
#endif /* GCC_TREE_AFFINE_H */
