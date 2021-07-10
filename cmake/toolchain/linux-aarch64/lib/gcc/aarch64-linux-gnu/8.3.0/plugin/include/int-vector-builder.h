/* A class for building vector integer constants.
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

#ifndef GCC_INT_VECTOR_BUILDER_H
#define GCC_INT_VECTOR_BUILDER_H 1

#include "vector-builder.h"

/* This class is used to build vectors of integer type T using the same
   encoding as tree and rtx constants.  See vector_builder for more
   details.  */
template<typename T>
class int_vector_builder : public vector_builder<T, int_vector_builder<T> >
{
  typedef vector_builder<T, int_vector_builder> parent;
  friend class vector_builder<T, int_vector_builder>;

public:
  int_vector_builder () {}
  int_vector_builder (poly_uint64, unsigned int, unsigned int);

  using parent::new_vector;

private:
  bool equal_p (T, T) const;
  bool allow_steps_p () const { return true; }
  bool integral_p (T) const { return true; }
  T step (T, T) const;
  T apply_step (T, unsigned int, T) const;
  bool can_elide_p (T) const { return true; }
  void note_representative (T *, T) {}
};

/* Create a new builder for a vector with FULL_NELTS elements.
   Initially encode the value as NPATTERNS interleaved patterns with
   NELTS_PER_PATTERN elements each.  */

template<typename T>
inline
int_vector_builder<T>::int_vector_builder (poly_uint64 full_nelts,
					   unsigned int npatterns,
					   unsigned int nelts_per_pattern)
{
  new_vector (full_nelts, npatterns, nelts_per_pattern);
}

/* Return true if elements ELT1 and ELT2 are equal.  */

template<typename T>
inline bool
int_vector_builder<T>::equal_p (T elt1, T elt2) const
{
  return known_eq (elt1, elt2);
}

/* Return the value of element ELT2 minus the value of element ELT1.  */

template<typename T>
inline T
int_vector_builder<T>::step (T elt1, T elt2) const
{
  return elt2 - elt1;
}

/* Return a vector element with the value BASE + FACTOR * STEP.  */

template<typename T>
inline T
int_vector_builder<T>::apply_step (T base, unsigned int factor, T step) const
{
  return base + factor * step;
}

#endif
