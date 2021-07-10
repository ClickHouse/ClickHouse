/* A class for building vector tree constants.
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

#ifndef GCC_TREE_VECTOR_BUILDER_H
#define GCC_TREE_VECTOR_BUILDER_H

#include "vector-builder.h"

/* This class is used to build VECTOR_CSTs from a sequence of elements.
   See vector_builder for more details.  */
class tree_vector_builder : public vector_builder<tree, tree_vector_builder>
{
  typedef vector_builder<tree, tree_vector_builder> parent;
  friend class vector_builder<tree, tree_vector_builder>;

public:
  tree_vector_builder () : m_type (0) {}
  tree_vector_builder (tree, unsigned int, unsigned int);
  tree build ();

  tree type () const { return m_type; }

  void new_vector (tree, unsigned int, unsigned int);
  bool new_unary_operation (tree, tree, bool);
  bool new_binary_operation (tree, tree, tree, bool);

  static unsigned int binary_encoded_nelts (tree, tree);

private:
  bool equal_p (const_tree, const_tree) const;
  bool allow_steps_p () const;
  bool integral_p (const_tree) const;
  wide_int step (const_tree, const_tree) const;
  tree apply_step (tree, unsigned int, const wide_int &) const;
  bool can_elide_p (const_tree) const;
  void note_representative (tree *, tree);

  tree m_type;
};

/* Create a new builder for a vector of type TYPE.  Initially encode the
   value as NPATTERNS interleaved patterns with NELTS_PER_PATTERN elements
   each.  */

inline
tree_vector_builder::tree_vector_builder (tree type, unsigned int npatterns,
					  unsigned int nelts_per_pattern)
{
  new_vector (type, npatterns, nelts_per_pattern);
}

/* Start building a new vector of type TYPE.  Initially encode the value
   as NPATTERNS interleaved patterns with NELTS_PER_PATTERN elements each.  */

inline void
tree_vector_builder::new_vector (tree type, unsigned int npatterns,
				 unsigned int nelts_per_pattern)
{
  m_type = type;
  parent::new_vector (TYPE_VECTOR_SUBPARTS (type), npatterns,
		      nelts_per_pattern);
}

/* Return true if elements I1 and I2 are equal.  */

inline bool
tree_vector_builder::equal_p (const_tree elt1, const_tree elt2) const
{
  return operand_equal_p (elt1, elt2, 0);
}

/* Return true if a stepped representation is OK.  We don't allow
   linear series for anything other than integers, to avoid problems
   with rounding.  */

inline bool
tree_vector_builder::allow_steps_p () const
{
  return INTEGRAL_TYPE_P (TREE_TYPE (m_type));
}

/* Return true if ELT can be interpreted as an integer.  */

inline bool
tree_vector_builder::integral_p (const_tree elt) const
{
  return TREE_CODE (elt) == INTEGER_CST;
}

/* Return the value of element ELT2 minus the value of element ELT1.
   Both elements are known to be INTEGER_CSTs.  */

inline wide_int
tree_vector_builder::step (const_tree elt1, const_tree elt2) const
{
  return wi::to_wide (elt2) - wi::to_wide (elt1);
}

/* Return true if we can drop element ELT, even if the retained elements
   are different.  Return false if this would mean losing overflow
   information.  */

inline bool
tree_vector_builder::can_elide_p (const_tree elt) const
{
  return !CONSTANT_CLASS_P (elt) || !TREE_OVERFLOW (elt);
}

/* Record that ELT2 is being elided, given that ELT1_PTR points to the last
   encoded element for the containing pattern.  */

inline void
tree_vector_builder::note_representative (tree *elt1_ptr, tree elt2)
{
  if (CONSTANT_CLASS_P (elt2) && TREE_OVERFLOW (elt2))
    {
      gcc_assert (operand_equal_p (*elt1_ptr, elt2, 0));
      if (!TREE_OVERFLOW (elt2))
	*elt1_ptr = elt2;
    }
}

#endif
