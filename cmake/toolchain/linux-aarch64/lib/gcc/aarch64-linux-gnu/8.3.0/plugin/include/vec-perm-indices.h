/* A representation of vector permutation indices.
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

#ifndef GCC_VEC_PERN_INDICES_H
#define GCC_VEC_PERN_INDICES_H 1

#include "int-vector-builder.h"

/* A vector_builder for building constant permutation vectors.
   The elements do not need to be clamped to a particular range
   of input elements.  */
typedef int_vector_builder<poly_int64> vec_perm_builder;

/* This class represents a constant permutation vector, such as that used
   as the final operand to a VEC_PERM_EXPR.

   Permutation vectors select indices modulo the number of input elements,
   and the class canonicalizes each permutation vector for a particular
   number of input vectors and for a particular number of elements per
   input.  For example, the gimple statements:

    _1 = VEC_PERM_EXPR <a, a, { 0, 2, 4, 6, 0, 2, 4, 6 }>;
    _2 = VEC_PERM_EXPR <a, a, { 0, 2, 4, 6, 8, 10, 12, 14 }>;
    _3 = VEC_PERM_EXPR <a, a, { 0, 2, 20, 22, 24, 2, 4, 14 }>;

   effectively have only a single vector input "a".  If "a" has 8
   elements, the indices select elements modulo 8, which makes all three
   VEC_PERM_EXPRs equivalent.  The canonical form is for the indices to be
   in the range [0, number of input elements - 1], so the class treats the
   second and third permutation vectors as though they had been the first.

   The class copes with cases in which the input and output vectors have
   different numbers of elements.  */
class vec_perm_indices
{
  typedef poly_int64 element_type;

public:
  vec_perm_indices ();
  vec_perm_indices (const vec_perm_builder &, unsigned int, poly_uint64);

  void new_vector (const vec_perm_builder &, unsigned int, poly_uint64);
  void new_expanded_vector (const vec_perm_indices &, unsigned int);
  void rotate_inputs (int delta);

  /* Return the underlying vector encoding.  */
  const vec_perm_builder &encoding () const { return m_encoding; }

  /* Return the number of output elements.  This is called length ()
     so that we present a more vec-like interface.  */
  poly_uint64 length () const { return m_encoding.full_nelts (); }

  /* Return the number of input vectors being permuted.  */
  unsigned int ninputs () const { return m_ninputs; }

  /* Return the number of elements in each input vector.  */
  poly_uint64 nelts_per_input () const { return m_nelts_per_input; }

  /* Return the total number of input elements.  */
  poly_uint64 input_nelts () const { return m_ninputs * m_nelts_per_input; }

  element_type clamp (element_type) const;
  element_type operator[] (unsigned int i) const;
  bool series_p (unsigned int, unsigned int, element_type, element_type) const;
  bool all_in_range_p (element_type, element_type) const;
  bool all_from_input_p (unsigned int) const;

private:
  vec_perm_indices (const vec_perm_indices &);

  vec_perm_builder m_encoding;
  unsigned int m_ninputs;
  poly_uint64 m_nelts_per_input;
};

bool tree_to_vec_perm_builder (vec_perm_builder *, tree);
tree vec_perm_indices_to_tree (tree, const vec_perm_indices &);
rtx vec_perm_indices_to_rtx (machine_mode, const vec_perm_indices &);

inline
vec_perm_indices::vec_perm_indices ()
  : m_ninputs (0),
    m_nelts_per_input (0)
{
}

/* Construct a permutation vector that selects between NINPUTS vector
   inputs that have NELTS_PER_INPUT elements each.  Take the elements of
   the new vector from ELEMENTS, clamping each one to be in range.  */

inline
vec_perm_indices::vec_perm_indices (const vec_perm_builder &elements,
				    unsigned int ninputs,
				    poly_uint64 nelts_per_input)
{
  new_vector (elements, ninputs, nelts_per_input);
}

/* Return the canonical value for permutation vector element ELT,
   taking into account the current number of input elements.  */

inline vec_perm_indices::element_type
vec_perm_indices::clamp (element_type elt) const
{
  element_type limit = input_nelts (), elem_within_input;
  HOST_WIDE_INT input;
  if (!can_div_trunc_p (elt, limit, &input, &elem_within_input))
    return elt;

  /* Treat negative elements as counting from the end.  This only matters
     if the vector size is not a power of 2.  */
  if (known_lt (elem_within_input, 0))
    return elem_within_input + limit;

  return elem_within_input;
}

/* Return the value of vector element I, which might or might not be
   explicitly encoded.  */

inline vec_perm_indices::element_type
vec_perm_indices::operator[] (unsigned int i) const
{
  return clamp (m_encoding.elt (i));
}

/* Return true if the permutation vector only selects elements from
   input I.  */

inline bool
vec_perm_indices::all_from_input_p (unsigned int i) const
{
  return all_in_range_p (i * m_nelts_per_input, m_nelts_per_input);
}

#endif
