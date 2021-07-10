/* Find near-matches for strings and identifiers.
   Copyright (C) 2015-2018 Free Software Foundation, Inc.

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

#ifndef GCC_SPELLCHECK_H
#define GCC_SPELLCHECK_H

typedef unsigned int edit_distance_t;
const edit_distance_t MAX_EDIT_DISTANCE = UINT_MAX;

/* spellcheck.c  */
extern edit_distance_t
levenshtein_distance (const char *s, int len_s,
		      const char *t, int len_t);

extern edit_distance_t
levenshtein_distance (const char *s, const char *t);

extern const char *
find_closest_string (const char *target,
		     const auto_vec<const char *> *candidates);

/* A traits class for describing a string-like type usable by
   class best_match.
   Specializations should provide the implementations of the following:

     static size_t get_length (TYPE);
     static const char *get_string (TYPE);

   get_string should return a non-NULL ptr, which does not need to be
   0-terminated.  */

template <typename TYPE>
struct edit_distance_traits {};

/* Specialization of edit_distance_traits for C-style strings.  */

template <>
struct edit_distance_traits<const char *>
{
  static size_t get_length (const char *str)
  {
    gcc_assert (str);
    return strlen (str);
  }

  static const char *get_string (const char *str)
  {
    gcc_assert (str);
    return str;
  }
};

/* A type for use when determining the best match against a string,
   expressed as a template so that we can match against various
   string-like types (const char *, frontend identifiers, and preprocessor
   macros).

   This type accumulates the best possible match against GOAL_TYPE for
   a sequence of elements of CANDIDATE_TYPE, whilst minimizing the
   number of calls to levenshtein_distance and to
   edit_distance_traits<T>::get_length.  */

template <typename GOAL_TYPE, typename CANDIDATE_TYPE>
class best_match
{
 public:
  typedef GOAL_TYPE goal_t;
  typedef CANDIDATE_TYPE candidate_t;
  typedef edit_distance_traits<goal_t> goal_traits;
  typedef edit_distance_traits<candidate_t> candidate_traits;

  /* Constructor.  */

  best_match (GOAL_TYPE goal,
	      edit_distance_t best_distance_so_far = MAX_EDIT_DISTANCE)
  : m_goal (goal_traits::get_string (goal)),
    m_goal_len (goal_traits::get_length (goal)),
    m_best_candidate (NULL),
    m_best_distance (best_distance_so_far)
  {}

  /* Compare the edit distance between CANDIDATE and m_goal,
     and if it's the best so far, record it.  */

  void consider (candidate_t candidate)
  {
    size_t candidate_len = candidate_traits::get_length (candidate);

    /* Calculate a lower bound on the candidate's distance to the goal,
       based on the difference in lengths; it will require at least
       this many insertions/deletions.  */
    edit_distance_t min_candidate_distance
      = abs ((ssize_t)candidate_len - (ssize_t)m_goal_len);

    /* If the candidate's length is sufficiently different to that
       of the goal string, then the number of insertions/deletions
       may be >= the best distance so far.  If so, we can reject
       the candidate immediately without needing to compute
       the exact distance, since it won't be an improvement.  */
    if (min_candidate_distance >= m_best_distance)
      return;

    /* If the candidate will be unable to beat the criterion in
       get_best_meaningful_candidate, reject it without computing
       the exact distance.  */
    unsigned int cutoff = MAX (m_goal_len, candidate_len) / 2;
    if (min_candidate_distance > cutoff)
      return;

    /* Otherwise, compute the distance and see if the candidate
       has beaten the previous best value.  */
    edit_distance_t dist
      = levenshtein_distance (m_goal, m_goal_len,
			      candidate_traits::get_string (candidate),
			      candidate_len);
    if (dist < m_best_distance)
      {
	m_best_distance = dist;
	m_best_candidate = candidate;
	m_best_candidate_len = candidate_len;
      }
  }

  /* Assuming that BEST_CANDIDATE is known to be better than
     m_best_candidate, update (without recomputing the edit distance to
     the goal).  */

  void set_best_so_far (CANDIDATE_TYPE best_candidate,
			edit_distance_t best_distance,
			size_t best_candidate_len)
  {
    gcc_assert (best_distance < m_best_distance);
    m_best_candidate = best_candidate;
    m_best_distance = best_distance;
    m_best_candidate_len = best_candidate_len;
  }

  /* Get the best candidate so far, but applying a filter to ensure
     that we return NULL if none of the candidates are close to the goal,
     to avoid offering nonsensical suggestions to the user.  */

  candidate_t get_best_meaningful_candidate () const
  {
    /* If more than half of the letters were misspelled, the suggestion is
       likely to be meaningless.  */
    if (m_best_candidate)
      {
	unsigned int cutoff = MAX (m_goal_len, m_best_candidate_len) / 2;
	if (m_best_distance > cutoff)
	  return NULL;
    }

    /* If the goal string somehow makes it into the candidate list, offering
       it as a suggestion will be nonsensical e.g.
         'constexpr' does not name a type; did you mean 'constexpr'?
       Ultimately such suggestions are due to bugs in constructing the
       candidate list, but as a band-aid, do not offer suggestions for
       distance == 0 (where candidate == goal).  */
    if (m_best_distance == 0)
      return NULL;

    return m_best_candidate;
  }

  /* Get the closest candidate so far, without applying any filtering.  */

  candidate_t blithely_get_best_candidate () const
  {
    return m_best_candidate;
  }

  edit_distance_t get_best_distance () const { return m_best_distance; }
  size_t get_best_candidate_length () const { return m_best_candidate_len; }

 private:
  const char *m_goal;
  size_t m_goal_len;
  candidate_t m_best_candidate;
  edit_distance_t m_best_distance;
  size_t m_best_candidate_len;
};

#endif  /* GCC_SPELLCHECK_H  */
