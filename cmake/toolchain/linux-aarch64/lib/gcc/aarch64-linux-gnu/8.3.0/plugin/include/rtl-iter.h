/* RTL iterators
   Copyright (C) 2014-2018 Free Software Foundation, Inc.

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

/* This structure describes the subrtxes of an rtx as follows:

   - if the rtx has no subrtxes, START and COUNT are both 0.

   - if all the subrtxes of an rtx are stored in a contiguous block
     of XEXPs ("e"s), START is the index of the first XEXP and COUNT
     is the number of them.

   - otherwise START is arbitrary and COUNT is UCHAR_MAX.

   rtx_all_subrtx_bounds applies to all codes.  rtx_nonconst_subrtx_bounds
   is like rtx_all_subrtx_bounds except that all constant rtxes are treated
   as having no subrtxes.  */
struct rtx_subrtx_bound_info {
  unsigned char start;
  unsigned char count;
};
extern rtx_subrtx_bound_info rtx_all_subrtx_bounds[];
extern rtx_subrtx_bound_info rtx_nonconst_subrtx_bounds[];

/* Return true if CODE has no subrtxes.  */

static inline bool
leaf_code_p (enum rtx_code code)
{
  return rtx_all_subrtx_bounds[code].count == 0;
}

/* Used to iterate over subrtxes of an rtx.  T abstracts the type of
   access.  */
template <typename T>
class generic_subrtx_iterator
{
  static const size_t LOCAL_ELEMS = 16;
  typedef typename T::value_type value_type;
  typedef typename T::rtx_type rtx_type;
  typedef typename T::rtunion_type rtunion_type;

public:
  struct array_type
  {
    array_type ();
    ~array_type ();
    value_type stack[LOCAL_ELEMS];
    vec <value_type, va_heap, vl_embed> *heap;
  };
  generic_subrtx_iterator (array_type &, value_type,
			   const rtx_subrtx_bound_info *);

  value_type operator * () const;
  bool at_end () const;
  void next ();
  void skip_subrtxes ();
  void substitute (value_type);

private:
  /* The bounds to use for iterating over subrtxes.  */
  const rtx_subrtx_bound_info *m_bounds;

  /* The storage used for the worklist.  */
  array_type &m_array;

  /* The current rtx.  */
  value_type m_current;

  /* The base of the current worklist.  */
  value_type *m_base;

  /* The number of subrtxes in M_BASE.  */
  size_t m_end;

  /* The following booleans shouldn't end up in registers or memory
     but just direct control flow.  */

  /* True if the iteration is over.  */
  bool m_done;

  /* True if we should skip the subrtxes of M_CURRENT.  */
  bool m_skip;

  /* True if M_CURRENT has been replaced with a different rtx.  */
  bool m_substitute;

  static void free_array (array_type &);
  static size_t add_subrtxes_to_queue (array_type &, value_type *, size_t,
				       rtx_type);
  static value_type *add_single_to_queue (array_type &, value_type *, size_t,
					  value_type);
};

template <typename T>
inline generic_subrtx_iterator <T>::array_type::array_type () : heap (0) {}

template <typename T>
inline generic_subrtx_iterator <T>::array_type::~array_type ()
{
  if (__builtin_expect (heap != 0, false))
    free_array (*this);
}

/* Iterate over X and its subrtxes, in arbitrary order.  Use ARRAY to
   store the worklist.  We use an external array in order to avoid
   capturing the fields of this structure when taking the address of
   the array.  Use BOUNDS to find the bounds of simple "e"-string codes.  */

template <typename T>
inline generic_subrtx_iterator <T>::
generic_subrtx_iterator (array_type &array, value_type x,
			 const rtx_subrtx_bound_info *bounds)
  : m_bounds (bounds),
    m_array (array),
    m_current (x),
    m_base (m_array.stack),
    m_end (0),
    m_done (false),
    m_skip (false),
    m_substitute (false)
{
}

/* Return the current subrtx.  */

template <typename T>
inline typename T::value_type
generic_subrtx_iterator <T>::operator * () const
{
  return m_current;
}

/* Return true if the iteration has finished.  */

template <typename T>
inline bool
generic_subrtx_iterator <T>::at_end () const
{
  return m_done;
}

/* Move on to the next subrtx.  */

template <typename T>
inline void
generic_subrtx_iterator <T>::next ()
{
  if (m_substitute)
    {
      m_substitute = false;
      m_skip = false;
      return;
    }
  if (!m_skip)
    {
      /* Add the subrtxes of M_CURRENT.  */
      rtx_type x = T::get_rtx (m_current);
      if (__builtin_expect (x != 0, true))
	{
	  enum rtx_code code = GET_CODE (x);
	  ssize_t count = m_bounds[code].count;
	  if (count > 0)
	    {
	      /* Handle the simple case of a single "e" block that is known
		 to fit into the current array.  */
	      if (__builtin_expect (m_end + count <= LOCAL_ELEMS + 1, true))
		{
		  /* Set M_CURRENT to the first subrtx and queue the rest.  */
		  ssize_t start = m_bounds[code].start;
		  rtunion_type *src = &x->u.fld[start];
		  if (__builtin_expect (count > 2, false))
		    m_base[m_end++] = T::get_value (src[2].rt_rtx);
		  if (count > 1)
		    m_base[m_end++] = T::get_value (src[1].rt_rtx);
		  m_current = T::get_value (src[0].rt_rtx);
		  return;
		}
	      /* Handle cases which aren't simple "e" sequences or where
		 the sequence might overrun M_BASE.  */
	      count = add_subrtxes_to_queue (m_array, m_base, m_end, x);
	      if (count > 0)
		{
		  m_end += count;
		  if (m_end > LOCAL_ELEMS)
		    m_base = m_array.heap->address ();
		  m_current = m_base[--m_end];
		  return;
		}
	    }
	}
    }
  else
    m_skip = false;
  if (m_end == 0)
    m_done = true;
  else
    m_current = m_base[--m_end];
}

/* Skip the subrtxes of the current rtx.  */

template <typename T>
inline void
generic_subrtx_iterator <T>::skip_subrtxes ()
{
  m_skip = true;
}

/* Ignore the subrtxes of the current rtx and look at X instead.  */

template <typename T>
inline void
generic_subrtx_iterator <T>::substitute (value_type x)
{
  m_substitute = true;
  m_current = x;
}

/* Iterators for const_rtx.  */
struct const_rtx_accessor
{
  typedef const_rtx value_type;
  typedef const_rtx rtx_type;
  typedef const rtunion rtunion_type;
  static rtx_type get_rtx (value_type x) { return x; }
  static value_type get_value (rtx_type x) { return x; }
};
typedef generic_subrtx_iterator <const_rtx_accessor> subrtx_iterator;

/* Iterators for non-constant rtx.  */
struct rtx_var_accessor
{
  typedef rtx value_type;
  typedef rtx rtx_type;
  typedef rtunion rtunion_type;
  static rtx_type get_rtx (value_type x) { return x; }
  static value_type get_value (rtx_type x) { return x; }
};
typedef generic_subrtx_iterator <rtx_var_accessor> subrtx_var_iterator;

/* Iterators for rtx *.  */
struct rtx_ptr_accessor
{
  typedef rtx *value_type;
  typedef rtx rtx_type;
  typedef rtunion rtunion_type;
  static rtx_type get_rtx (value_type ptr) { return *ptr; }
  static value_type get_value (rtx_type &x) { return &x; }
};
typedef generic_subrtx_iterator <rtx_ptr_accessor> subrtx_ptr_iterator;

#define ALL_BOUNDS rtx_all_subrtx_bounds
#define NONCONST_BOUNDS rtx_nonconst_subrtx_bounds

/* Use ITER to iterate over const_rtx X and its recursive subrtxes,
   using subrtx_iterator::array ARRAY as the storage for the worklist.
   ARRAY can be reused for multiple consecutive iterations but shouldn't
   be shared by two concurrent iterations.  TYPE is ALL if all subrtxes
   are of interest or NONCONST if it is safe to ignore subrtxes of
   constants.  */
#define FOR_EACH_SUBRTX(ITER, ARRAY, X, TYPE) \
  for (subrtx_iterator ITER (ARRAY, X, TYPE##_BOUNDS); !ITER.at_end (); \
       ITER.next ())

/* Like FOR_EACH_SUBRTX, but iterate over subrtxes of an rtx X.  */
#define FOR_EACH_SUBRTX_VAR(ITER, ARRAY, X, TYPE) \
  for (subrtx_var_iterator ITER (ARRAY, X, TYPE##_BOUNDS); !ITER.at_end (); \
       ITER.next ())

/* Like FOR_EACH_SUBRTX, but iterate over subrtx pointers of rtx pointer X.
   For example, if X is &PATTERN (insn) and the pattern is a SET, iterate
   over &PATTERN (insn), &SET_DEST (PATTERN (insn)), etc.  */
#define FOR_EACH_SUBRTX_PTR(ITER, ARRAY, X, TYPE) \
  for (subrtx_ptr_iterator ITER (ARRAY, X, TYPE##_BOUNDS); !ITER.at_end (); \
       ITER.next ())
