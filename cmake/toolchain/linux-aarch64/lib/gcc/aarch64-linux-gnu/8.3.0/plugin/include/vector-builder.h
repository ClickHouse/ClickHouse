/* A class for building vector constant patterns.
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

#ifndef GCC_VECTOR_BUILDER_H
#define GCC_VECTOR_BUILDER_H

/* This class is a wrapper around auto_vec<T> for building vectors of T.
   It aims to encode each vector as npatterns interleaved patterns,
   where each pattern represents a sequence:

     { BASE0, BASE1, BASE1 + STEP, BASE1 + STEP*2, BASE1 + STEP*3, ... }

   The first three elements in each pattern provide enough information
   to derive the other elements.  If all patterns have a STEP of zero,
   we only need to encode the first two elements in each pattern.
   If BASE1 is also equal to BASE0 for all patterns, we only need to
   encode the first element in each pattern.  The number of encoded
   elements per pattern is given by nelts_per_pattern.

   The class can be used in two ways:

   1. It can be used to build a full image of the vector, which is then
      canonicalized by finalize ().  In this case npatterns is initially
      the number of elements in the vector and nelts_per_pattern is
      initially 1.

   2. It can be used to build a vector that already has a known encoding.
      This is preferred since it is more efficient and copes with
      variable-length vectors.  finalize () then canonicalizes the encoding
      to a simpler form if possible.

   The derived class Derived provides this functionality for specific Ts.
   Derived needs to provide the following interface:

      bool equal_p (T elt1, T elt2) const;

	  Return true if elements ELT1 and ELT2 are equal.

      bool allow_steps_p () const;

	  Return true if a stepped representation is OK.  We don't allow
	  linear series for anything other than integers, to avoid problems
	  with rounding.

      bool integral_p (T elt) const;

	  Return true if element ELT can be interpreted as an integer.

      StepType step (T elt1, T elt2) const;

	  Return the value of element ELT2 minus the value of element ELT1,
	  given integral_p (ELT1) && integral_p (ELT2).  There is no fixed
	  choice of StepType.

      T apply_step (T base, unsigned int factor, StepType step) const;

	  Return a vector element with the value BASE + FACTOR * STEP.

      bool can_elide_p (T elt) const;

	  Return true if we can drop element ELT, even if the retained
	  elements are different.  This is provided for TREE_OVERFLOW
	  handling.

      void note_representative (T *elt1_ptr, T elt2);

	  Record that ELT2 is being elided, given that ELT1_PTR points to
	  the last encoded element for the containing pattern.  This is
	  again provided for TREE_OVERFLOW handling.  */

template<typename T, typename Derived>
class vector_builder : public auto_vec<T, 32>
{
public:
  vector_builder ();

  poly_uint64 full_nelts () const { return m_full_nelts; }
  unsigned int npatterns () const { return m_npatterns; }
  unsigned int nelts_per_pattern () const { return m_nelts_per_pattern; }
  unsigned int encoded_nelts () const;
  bool encoded_full_vector_p () const;
  T elt (unsigned int) const;

  bool operator == (const Derived &) const;
  bool operator != (const Derived &x) const { return !operator == (x); }

  void finalize ();

protected:
  void new_vector (poly_uint64, unsigned int, unsigned int);
  void reshape (unsigned int, unsigned int);
  bool repeating_sequence_p (unsigned int, unsigned int, unsigned int);
  bool stepped_sequence_p (unsigned int, unsigned int, unsigned int);
  bool try_npatterns (unsigned int);

private:
  vector_builder (const vector_builder &);
  vector_builder &operator= (const vector_builder &);
  Derived *derived () { return static_cast<Derived *> (this); }
  const Derived *derived () const;

  poly_uint64 m_full_nelts;
  unsigned int m_npatterns;
  unsigned int m_nelts_per_pattern;
};

template<typename T, typename Derived>
inline const Derived *
vector_builder<T, Derived>::derived () const
{
  return static_cast<const Derived *> (this);
}

template<typename T, typename Derived>
inline
vector_builder<T, Derived>::vector_builder ()
  : m_full_nelts (0),
    m_npatterns (0),
    m_nelts_per_pattern (0)
{}

/* Return the number of elements that are explicitly encoded.  The vec
   starts with these explicitly-encoded elements and may contain additional
   elided elements.  */

template<typename T, typename Derived>
inline unsigned int
vector_builder<T, Derived>::encoded_nelts () const
{
  return m_npatterns * m_nelts_per_pattern;
}

/* Return true if every element of the vector is explicitly encoded.  */

template<typename T, typename Derived>
inline bool
vector_builder<T, Derived>::encoded_full_vector_p () const
{
  return known_eq (m_npatterns * m_nelts_per_pattern, m_full_nelts);
}

/* Start building a vector that has FULL_NELTS elements.  Initially
   encode it using NPATTERNS patterns with NELTS_PER_PATTERN each.  */

template<typename T, typename Derived>
void
vector_builder<T, Derived>::new_vector (poly_uint64 full_nelts,
					unsigned int npatterns,
					unsigned int nelts_per_pattern)
{
  m_full_nelts = full_nelts;
  m_npatterns = npatterns;
  m_nelts_per_pattern = nelts_per_pattern;
  this->reserve (encoded_nelts ());
  this->truncate (0);
}

/* Return true if this vector and OTHER have the same elements and
   are encoded in the same way.  */

template<typename T, typename Derived>
bool
vector_builder<T, Derived>::operator == (const Derived &other) const
{
  if (maybe_ne (m_full_nelts, other.m_full_nelts)
      || m_npatterns != other.m_npatterns
      || m_nelts_per_pattern != other.m_nelts_per_pattern)
    return false;

  unsigned int nelts = encoded_nelts ();
  for (unsigned int i = 0; i < nelts; ++i)
    if (!derived ()->equal_p ((*this)[i], other[i]))
      return false;

  return true;
}

/* Return the value of vector element I, which might or might not be
   encoded explicitly.  */

template<typename T, typename Derived>
T
vector_builder<T, Derived>::elt (unsigned int i) const
{
  /* This only makes sense if the encoding has been fully populated.  */
  gcc_checking_assert (encoded_nelts () <= this->length ());

  /* First handle elements that are already present in the underlying
     vector, regardless of whether they're part of the encoding or not.  */
  if (i < this->length ())
    return (*this)[i];

  /* Identify the pattern that contains element I and work out the index of
     the last encoded element for that pattern.  */
  unsigned int pattern = i % m_npatterns;
  unsigned int count = i / m_npatterns;
  unsigned int final_i = encoded_nelts () - m_npatterns + pattern;
  T final = (*this)[final_i];

  /* If there are no steps, the final encoded value is the right one.  */
  if (m_nelts_per_pattern <= 2)
    return final;

  /* Otherwise work out the value from the last two encoded elements.  */
  T prev = (*this)[final_i - m_npatterns];
  return derived ()->apply_step (final, count - 2,
				 derived ()->step (prev, final));
}

/* Change the encoding to NPATTERNS patterns of NELTS_PER_PATTERN each,
   but without changing the underlying vector.  */

template<typename T, typename Derived>
void
vector_builder<T, Derived>::reshape (unsigned int npatterns,
				     unsigned int nelts_per_pattern)
{
  unsigned int old_encoded_nelts = encoded_nelts ();
  unsigned int new_encoded_nelts = npatterns * nelts_per_pattern;
  gcc_checking_assert (new_encoded_nelts <= old_encoded_nelts);
  unsigned int next = new_encoded_nelts - npatterns;
  for (unsigned int i = new_encoded_nelts; i < old_encoded_nelts; ++i)
    {
      derived ()->note_representative (&(*this)[next], (*this)[i]);
      next += 1;
      if (next == new_encoded_nelts)
	next -= npatterns;
    }
  m_npatterns = npatterns;
  m_nelts_per_pattern = nelts_per_pattern;
}

/* Return true if elements [START, END) contain a repeating sequence of
   STEP elements.  */

template<typename T, typename Derived>
bool
vector_builder<T, Derived>::repeating_sequence_p (unsigned int start,
						  unsigned int end,
						  unsigned int step)
{
  for (unsigned int i = start; i < end - step; ++i)
    if (!derived ()->equal_p ((*this)[i], (*this)[i + step]))
      return false;
  return true;
}

/* Return true if elements [START, END) contain STEP interleaved linear
   series.  */

template<typename T, typename Derived>
bool
vector_builder<T, Derived>::stepped_sequence_p (unsigned int start,
						unsigned int end,
						unsigned int step)
{
  if (!derived ()->allow_steps_p ())
    return false;

  for (unsigned int i = start + step * 2; i < end; ++i)
    {
      T elt1 = (*this)[i - step * 2];
      T elt2 = (*this)[i - step];
      T elt3 = (*this)[i];

      if (!derived ()->integral_p (elt1)
	  || !derived ()->integral_p (elt2)
	  || !derived ()->integral_p (elt3))
	return false;

      if (maybe_ne (derived ()->step (elt1, elt2),
		    derived ()->step (elt2, elt3)))
	return false;

      if (!derived ()->can_elide_p (elt3))
	return false;
    }
  return true;
}

/* Try to change the number of encoded patterns to NPATTERNS, returning
   true on success.  */

template<typename T, typename Derived>
bool
vector_builder<T, Derived>::try_npatterns (unsigned int npatterns)
{
  if (m_nelts_per_pattern == 1)
    {
      /* See whether NPATTERNS is valid with the current 1-element-per-pattern
	 encoding.  */
      if (repeating_sequence_p (0, encoded_nelts (), npatterns))
	{
	  reshape (npatterns, 1);
	  return true;
	}

      /* We can only increase the number of elements per pattern if all
	 elements are still encoded explicitly.  */
      if (!encoded_full_vector_p ())
	return false;
    }

  if (m_nelts_per_pattern <= 2)
    {
      /* See whether NPATTERNS is valid with a 2-element-per-pattern
	 encoding.  */
      if (repeating_sequence_p (npatterns, encoded_nelts (), npatterns))
	{
	  reshape (npatterns, 2);
	  return true;
	}

      /* We can only increase the number of elements per pattern if all
	 elements are still encoded explicitly.  */
      if (!encoded_full_vector_p ())
	return false;
    }

  if (m_nelts_per_pattern <= 3)
    {
      /* See whether we have NPATTERNS interleaved linear series,
	 giving a 3-element-per-pattern encoding.  */
      if (stepped_sequence_p (npatterns, encoded_nelts (), npatterns))
	{
	  reshape (npatterns, 3);
	  return true;
	}
      return false;
    }

  gcc_unreachable ();
}

/* Replace the current encoding with the canonical form.  */

template<typename T, typename Derived>
void
vector_builder<T, Derived>::finalize ()
{
  /* The encoding requires the same number of elements to come from each
     pattern.  */
  gcc_assert (multiple_p (m_full_nelts, m_npatterns));

  /* Allow the caller to build more elements than necessary.  For example,
     it's often convenient to build a stepped vector from the natural
     encoding of three elements even if the vector itself only has two.  */
  unsigned HOST_WIDE_INT const_full_nelts;
  if (m_full_nelts.is_constant (&const_full_nelts)
      && const_full_nelts <= encoded_nelts ())
    {
      m_npatterns = const_full_nelts;
      m_nelts_per_pattern = 1;
    }

  /* Try to whittle down the number of elements per pattern.  That is:

     1. If we have stepped patterns whose steps are all 0, reduce the
        number of elements per pattern from 3 to 2.

     2. If we have background fill values that are the same as the
        foreground values, reduce the number of elements per pattern
        from 2 to 1.  */
  while (m_nelts_per_pattern > 1
	 && repeating_sequence_p (encoded_nelts () - m_npatterns * 2,
				  encoded_nelts (), m_npatterns))
    /* The last two sequences of M_NPATTERNS elements are equal,
       so remove the last one.  */
    reshape (m_npatterns, m_nelts_per_pattern - 1);

  if (pow2p_hwi (m_npatterns))
    {
      /* Try to halve the number of patterns while doing so gives a
	 valid pattern.  This approach is linear in the number of
	 elements, whereas searcing from 1 up would be O(n*log(n)).

	 Each halving step tries to keep the number of elements per pattern
	 the same.  If that isn't possible, and if all elements are still
	 explicitly encoded, the halving step can instead increase the number
	 of elements per pattern.

	 E.g. for:

	     { 0, 2, 3, 4, 5, 6, 7, 8 }  npatterns == 8  full_nelts == 8

	 we first realize that the second half of the sequence is not
	 equal to the first, so we cannot maintain 1 element per pattern
	 for npatterns == 4.  Instead we halve the number of patterns
	 and double the number of elements per pattern, treating this
	 as a "foreground" { 0, 2, 3, 4 } against a "background" of
	 { 5, 6, 7, 8 | 5, 6, 7, 8 ... }:

	     { 0, 2, 3, 4 | 5, 6, 7, 8 }  npatterns == 4

	 Next we realize that this is *not* a foreround of { 0, 2 }
	 against a background of { 3, 4 | 3, 4 ... }, so the only
	 remaining option for reducing the number of patterns is
	 to use a foreground of { 0, 2 } against a stepped background
	 of { 1, 2 | 3, 4 | 5, 6 ... }.  This is valid because we still
	 haven't elided any elements:

	     { 0, 2 | 3, 4 | 5, 6 }  npatterns == 2

	 This in turn can be reduced to a foreground of { 0 } against a
	 stepped background of { 1 | 2 | 3 ... }:

	     { 0 | 2 | 3 }  npatterns == 1

	 This last step would not have been possible for:

	     { 0, 0 | 3, 4 | 5, 6 }  npatterns == 2.  */
      while ((m_npatterns & 1) == 0 && try_npatterns (m_npatterns / 2))
	continue;

      /* Builders of arbitrary fixed-length vectors can use:

	     new_vector (x, x, 1)

	 so that every element is specified explicitly.  Handle cases
	 that are actually wrapping series, like { 0, 1, 2, 3, 0, 1, 2, 3 }
	 would be for 2-bit elements.  We'll have treated them as
	 duplicates in the loop above.  */
      if (m_nelts_per_pattern == 1
	  && m_full_nelts.is_constant (&const_full_nelts)
	  && this->length () >= const_full_nelts
	  && (m_npatterns & 3) == 0
	  && stepped_sequence_p (m_npatterns / 4, const_full_nelts,
				 m_npatterns / 4))
	{
	  reshape (m_npatterns / 4, 3);
	  while ((m_npatterns & 1) == 0 && try_npatterns (m_npatterns / 2))
	    continue;
	}
    }
  else
    /* For the non-power-of-2 case, do a simple search up from 1.  */
    for (unsigned int i = 1; i <= m_npatterns / 2; ++i)
      if (m_npatterns % i == 0 && try_npatterns (i))
	break;
}

#endif
