/* A class for building vector rtx constants.
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

#ifndef GCC_RTX_VECTOR_BUILDER_H
#define GCC_RTX_VECTOR_BUILDER_H

#include "vector-builder.h"

/* This class is used to build VECTOR_CSTs from a sequence of elements.
   See vector_builder for more details.  */
class rtx_vector_builder : public vector_builder<rtx, rtx_vector_builder>
{
  typedef vector_builder<rtx, rtx_vector_builder> parent;
  friend class vector_builder<rtx, rtx_vector_builder>;

public:
  rtx_vector_builder () : m_mode (VOIDmode) {}
  rtx_vector_builder (machine_mode, unsigned int, unsigned int);
  rtx build (rtvec);
  rtx build ();

  machine_mode mode () const { return m_mode; }

  void new_vector (machine_mode, unsigned int, unsigned int);

private:
  bool equal_p (rtx, rtx) const;
  bool allow_steps_p () const;
  bool integral_p (rtx) const;
  wide_int step (rtx, rtx) const;
  rtx apply_step (rtx, unsigned int, const wide_int &) const;
  bool can_elide_p (rtx) const { return true; }
  void note_representative (rtx *, rtx) {}

  rtx find_cached_value ();

  machine_mode m_mode;
};

/* Create a new builder for a vector of mode MODE.  Initially encode the
   value as NPATTERNS interleaved patterns with NELTS_PER_PATTERN elements
   each.  */

inline
rtx_vector_builder::rtx_vector_builder (machine_mode mode,
					unsigned int npatterns,
					unsigned int nelts_per_pattern)
{
  new_vector (mode, npatterns, nelts_per_pattern);
}

/* Start building a new vector of mode MODE.  Initially encode the value
   as NPATTERNS interleaved patterns with NELTS_PER_PATTERN elements each.  */

inline void
rtx_vector_builder::new_vector (machine_mode mode, unsigned int npatterns,
				unsigned int nelts_per_pattern)
{
  m_mode = mode;
  parent::new_vector (GET_MODE_NUNITS (mode), npatterns, nelts_per_pattern);
}

/* Return true if elements ELT1 and ELT2 are equal.  */

inline bool
rtx_vector_builder::equal_p (rtx elt1, rtx elt2) const
{
  return rtx_equal_p (elt1, elt2);
}

/* Return true if a stepped representation is OK.  We don't allow
   linear series for anything other than integers, to avoid problems
   with rounding.  */

inline bool
rtx_vector_builder::allow_steps_p () const
{
  return is_a <scalar_int_mode> (GET_MODE_INNER (m_mode));
}

/* Return true if element ELT can be interpreted as an integer.  */

inline bool
rtx_vector_builder::integral_p (rtx elt) const
{
  return CONST_SCALAR_INT_P (elt);
}

/* Return the value of element ELT2 minus the value of element ELT1.
   Both elements are known to be CONST_SCALAR_INT_Ps.  */

inline wide_int
rtx_vector_builder::step (rtx elt1, rtx elt2) const
{
  return wi::sub (rtx_mode_t (elt2, GET_MODE_INNER (m_mode)),
		  rtx_mode_t (elt1, GET_MODE_INNER (m_mode)));
}

#endif
