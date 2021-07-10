/* An incremental hash abstract data type.
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

#ifndef INCHASH_H
#define INCHASH_H 1


/* This file implements an incremential hash function ADT, to be used
   by code that incrementially hashes a lot of unrelated data
   (not in a single memory block) into a single value. The goal
   is to make it easy to plug in efficient hash algorithms.
   Currently it just implements the plain old jhash based
   incremental hash from gcc's tree.c.  */

hashval_t iterative_hash_host_wide_int (HOST_WIDE_INT, hashval_t);
hashval_t iterative_hash_hashval_t (hashval_t, hashval_t);

namespace inchash
{

class hash
{
 public:

  /* Start incremential hashing, optionally with SEED.  */
  hash (hashval_t seed = 0)
  {
    val = seed;
    bits = 0;
  }

  /* End incremential hashing and provide the final value.  */
  hashval_t end ()
  {
    return val;
  }

  /* Add unsigned value V.  */
  void add_int (unsigned v)
  {
    val = iterative_hash_hashval_t (v, val);
  }

  /* Add polynomial value V, treating each element as an unsigned int.  */
  template<unsigned int N, typename T>
  void add_poly_int (const poly_int_pod<N, T> &v)
  {
    for (unsigned int i = 0; i < N; ++i)
      add_int (v.coeffs[i]);
  }

  /* Add HOST_WIDE_INT value V.  */
  void add_hwi (HOST_WIDE_INT v)
  {
    val = iterative_hash_host_wide_int (v, val);
  }

  /* Add polynomial value V, treating each element as a HOST_WIDE_INT.  */
  template<unsigned int N, typename T>
  void add_poly_hwi (const poly_int_pod<N, T> &v)
  {
    for (unsigned int i = 0; i < N; ++i)
      add_hwi (v.coeffs[i]);
  }

  /* Add wide_int-based value V.  */
  template<typename T>
  void add_wide_int (const generic_wide_int<T> &x)
  {
    add_int (x.get_len ());
    for (unsigned i = 0; i < x.get_len (); i++)
      add_hwi (x.elt (i));
  }

  /* Hash in pointer PTR.  */
  void add_ptr (const void *ptr)
  {
    add (&ptr, sizeof (ptr));
  }

  /* Add a memory block DATA with size LEN.  */
  void add (const void *data, size_t len)
  {
    val = iterative_hash (data, len, val);
  }

  /* Merge hash value OTHER.  */
  void merge_hash (hashval_t other)
  {
    val = iterative_hash_hashval_t (other, val);
  }

  /* Hash in state from other inchash OTHER.  */
  void merge (hash &other)
  {
    merge_hash (other.val);
  }

  template<class T> void add_object(T &obj)
  {
    add (&obj, sizeof(T));
  }

  /* Support for accumulating boolean flags */

  void add_flag (bool flag)
  {
    bits = (bits << 1) | flag;
  }

  void commit_flag ()
  {
    add_int (bits);
    bits = 0;
  }

  /* Support for commutative hashing. Add A and B in a defined order
     based on their value. This is useful for hashing commutative
     expressions, so that A+B and B+A get the same hash.  */

  void add_commutative (hash &a, hash &b)
  {
    if (a.end() > b.end())
      {
	merge (b);
	merge (a);
      }
    else
      {
	merge (a);
	merge (b);
      }
  }

 private:
  hashval_t val;
  unsigned bits;
};

}

/* Borrowed from hashtab.c iterative_hash implementation.  */
#define mix(a,b,c) \
{ \
  a -= b; a -= c; a ^= (c>>13); \
  b -= c; b -= a; b ^= (a<< 8); \
  c -= a; c -= b; c ^= ((b&0xffffffff)>>13); \
  a -= b; a -= c; a ^= ((c&0xffffffff)>>12); \
  b -= c; b -= a; b = (b ^ (a<<16)) & 0xffffffff; \
  c -= a; c -= b; c = (c ^ (b>> 5)) & 0xffffffff; \
  a -= b; a -= c; a = (a ^ (c>> 3)) & 0xffffffff; \
  b -= c; b -= a; b = (b ^ (a<<10)) & 0xffffffff; \
  c -= a; c -= b; c = (c ^ (b>>15)) & 0xffffffff; \
}


/* Produce good hash value combining VAL and VAL2.  */
inline
hashval_t
iterative_hash_hashval_t (hashval_t val, hashval_t val2)
{
  /* the golden ratio; an arbitrary value.  */
  hashval_t a = 0x9e3779b9;

  mix (a, val, val2);
  return val2;
}

/* Produce good hash value combining VAL and VAL2.  */

inline
hashval_t
iterative_hash_host_wide_int (HOST_WIDE_INT val, hashval_t val2)
{
  if (sizeof (HOST_WIDE_INT) == sizeof (hashval_t))
    return iterative_hash_hashval_t (val, val2);
  else
    {
      hashval_t a = (hashval_t) val;
      /* Avoid warnings about shifting of more than the width of the type on
         hosts that won't execute this path.  */
      int zero = 0;
      hashval_t b = (hashval_t) (val >> (sizeof (hashval_t) * 8 + zero));
      mix (a, b, val2);
      if (sizeof (HOST_WIDE_INT) > 2 * sizeof (hashval_t))
	{
	  hashval_t a = (hashval_t) (val >> (sizeof (hashval_t) * 16 + zero));
	  hashval_t b = (hashval_t) (val >> (sizeof (hashval_t) * 24 + zero));
	  mix (a, b, val2);
	}
      return val2;
    }
}

#endif
