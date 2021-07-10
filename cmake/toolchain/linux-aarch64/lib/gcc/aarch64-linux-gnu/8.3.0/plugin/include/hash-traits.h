/* Traits for hashable types.
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

#ifndef hash_traits_h
#define hash_traits_h

/* Helpful type for removing with free.  */

template <typename Type>
struct typed_free_remove
{
  static inline void remove (Type *p);
};


/* Remove with free.  */

template <typename Type>
inline void
typed_free_remove <Type>::remove (Type *p)
{
  free (p);
}

/* Helpful type for removing with delete.  */

template <typename Type>
struct typed_delete_remove
{
  static inline void remove (Type *p);
};


/* Remove with delete.  */

template <typename Type>
inline void
typed_delete_remove <Type>::remove (Type *p)
{
  delete p;
}

/* Helpful type for a no-op remove.  */

template <typename Type>
struct typed_noop_remove
{
  static inline void remove (Type &);
};


/* Remove doing nothing.  */

template <typename Type>
inline void
typed_noop_remove <Type>::remove (Type &)
{
}


/* Hasher for integer type Type in which Empty is a spare value that can be
   used to mark empty slots.  If Deleted != Empty then Deleted is another
   spare value that can be used for deleted slots; if Deleted == Empty then
   hash table entries cannot be deleted.  */

template <typename Type, Type Empty, Type Deleted = Empty>
struct int_hash : typed_noop_remove <Type>
{
  typedef Type value_type;
  typedef Type compare_type;

  static inline hashval_t hash (value_type);
  static inline bool equal (value_type existing, value_type candidate);
  static inline void mark_deleted (Type &);
  static inline void mark_empty (Type &);
  static inline bool is_deleted (Type);
  static inline bool is_empty (Type);
};

template <typename Type, Type Empty, Type Deleted>
inline hashval_t
int_hash <Type, Empty, Deleted>::hash (value_type x)
{
  return x;
}

template <typename Type, Type Empty, Type Deleted>
inline bool
int_hash <Type, Empty, Deleted>::equal (value_type x, value_type y)
{
  return x == y;
}

template <typename Type, Type Empty, Type Deleted>
inline void
int_hash <Type, Empty, Deleted>::mark_deleted (Type &x)
{
  gcc_assert (Empty != Deleted);
  x = Deleted;
}

template <typename Type, Type Empty, Type Deleted>
inline void
int_hash <Type, Empty, Deleted>::mark_empty (Type &x)
{
  x = Empty;
}

template <typename Type, Type Empty, Type Deleted>
inline bool
int_hash <Type, Empty, Deleted>::is_deleted (Type x)
{
  return Empty != Deleted && x == Deleted;
}

template <typename Type, Type Empty, Type Deleted>
inline bool
int_hash <Type, Empty, Deleted>::is_empty (Type x)
{
  return x == Empty;
}

/* Pointer hasher based on pointer equality.  Other types of pointer hash
   can inherit this and override the hash and equal functions with some
   other form of equality (such as string equality).  */

template <typename Type>
struct pointer_hash
{
  typedef Type *value_type;
  typedef Type *compare_type;

  static inline hashval_t hash (const value_type &);
  static inline bool equal (const value_type &existing,
			    const compare_type &candidate);
  static inline void mark_deleted (Type *&);
  static inline void mark_empty (Type *&);
  static inline bool is_deleted (Type *);
  static inline bool is_empty (Type *);
};

template <typename Type>
inline hashval_t
pointer_hash <Type>::hash (const value_type &candidate)
{
  /* This is a really poor hash function, but it is what the current code uses,
     so I am reusing it to avoid an additional axis in testing.  */
  return (hashval_t) ((intptr_t)candidate >> 3);
}

template <typename Type>
inline bool
pointer_hash <Type>::equal (const value_type &existing,
			   const compare_type &candidate)
{
  return existing == candidate;
}

template <typename Type>
inline void
pointer_hash <Type>::mark_deleted (Type *&e)
{
  e = reinterpret_cast<Type *> (1);
}

template <typename Type>
inline void
pointer_hash <Type>::mark_empty (Type *&e)
{
  e = NULL;
}

template <typename Type>
inline bool
pointer_hash <Type>::is_deleted (Type *e)
{
  return e == reinterpret_cast<Type *> (1);
}

template <typename Type>
inline bool
pointer_hash <Type>::is_empty (Type *e)
{
  return e == NULL;
}

/* Hasher for "const char *" strings, using string rather than pointer
   equality.  */

struct string_hash : pointer_hash <const char>
{
  static inline hashval_t hash (const char *);
  static inline bool equal (const char *, const char *);
};

inline hashval_t
string_hash::hash (const char *id)
{
  return htab_hash_string (id);
}

inline bool
string_hash::equal (const char *id1, const char *id2)
{
  return strcmp (id1, id2) == 0;
}

/* Remover and marker for entries in gc memory.  */

template<typename T>
struct ggc_remove
{
  static void remove (T &) {}

  static void
  ggc_mx (T &p)
  {
    extern void gt_ggc_mx (T &);
    gt_ggc_mx (p);
  }

  /* Overridden in ggc_cache_remove.  */
  static void
  ggc_maybe_mx (T &p)
  {
    ggc_mx (p);
  }

  static void
  pch_nx (T &p)
  {
    extern void gt_pch_nx (T &);
    gt_pch_nx (p);
  }

  static void
  pch_nx (T &p, gt_pointer_operator op, void *cookie)
  {
    op (&p, cookie);
  }
};

/* Remover and marker for "cache" entries in gc memory.  These entries can
   be deleted if there are no non-cache references to the data.  */

template<typename T>
struct ggc_cache_remove : ggc_remove<T>
{
  /* Entries are weakly held because this is for caches.  */
  static void ggc_maybe_mx (T &) {}

  static int
  keep_cache_entry (T &e)
  {
    return ggc_marked_p (e) ? -1 : 0;
  }
};

/* Traits for pointer elements that should not be freed when an element
   is deleted.  */

template <typename T>
struct nofree_ptr_hash : pointer_hash <T>, typed_noop_remove <T *> {};

/* Traits for pointer elements that should be freed via free() when an
   element is deleted.  */

template <typename T>
struct free_ptr_hash : pointer_hash <T>, typed_free_remove <T> {};

/* Traits for pointer elements that should be freed via delete operand when an
   element is deleted.  */

template <typename T>
struct delete_ptr_hash : pointer_hash <T>, typed_delete_remove <T> {};

/* Traits for elements that point to gc memory.  The pointed-to data
   must be kept across collections.  */

template <typename T>
struct ggc_ptr_hash : pointer_hash <T>, ggc_remove <T *> {};

/* Traits for elements that point to gc memory.  The elements don't
   in themselves keep the pointed-to data alive and they can be deleted
   if the pointed-to data is going to be collected.  */

template <typename T>
struct ggc_cache_ptr_hash : pointer_hash <T>, ggc_cache_remove <T *> {};

/* Traits for string elements that should not be freed when an element
   is deleted.  */

struct nofree_string_hash : string_hash, typed_noop_remove <const char *> {};

/* Traits for pairs of values, using the first to record empty and
   deleted slots.  */

template <typename T1, typename T2>
struct pair_hash
{
  typedef std::pair <typename T1::value_type,
		     typename T2::value_type> value_type;
  typedef std::pair <typename T1::compare_type,
		     typename T2::compare_type> compare_type;

  static inline hashval_t hash (const value_type &);
  static inline bool equal (const value_type &, const compare_type &);
  static inline void remove (value_type &);
  static inline void mark_deleted (value_type &);
  static inline void mark_empty (value_type &);
  static inline bool is_deleted (const value_type &);
  static inline bool is_empty (const value_type &);
};

template <typename T1, typename T2>
inline hashval_t
pair_hash <T1, T2>::hash (const value_type &x)
{
  return iterative_hash_hashval_t (T1::hash (x.first), T2::hash (x.second));
}

template <typename T1, typename T2>
inline bool
pair_hash <T1, T2>::equal (const value_type &x, const compare_type &y)
{
  return T1::equal (x.first, y.first) && T2::equal (x.second, y.second);
}

template <typename T1, typename T2>
inline void
pair_hash <T1, T2>::remove (value_type &x)
{
  T1::remove (x.first);
  T2::remove (x.second);
}

template <typename T1, typename T2>
inline void
pair_hash <T1, T2>::mark_deleted (value_type &x)
{
  T1::mark_deleted (x.first);
}

template <typename T1, typename T2>
inline void
pair_hash <T1, T2>::mark_empty (value_type &x)
{
  T1::mark_empty (x.first);
}

template <typename T1, typename T2>
inline bool
pair_hash <T1, T2>::is_deleted (const value_type &x)
{
  return T1::is_deleted (x.first);
}

template <typename T1, typename T2>
inline bool
pair_hash <T1, T2>::is_empty (const value_type &x)
{
  return T1::is_empty (x.first);
}

template <typename T> struct default_hash_traits : T {};

template <typename T>
struct default_hash_traits <T *> : ggc_ptr_hash <T> {};

#endif
