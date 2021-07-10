/* Hash Table Helper for Trees
   Copyright (C) 2012-2018 Free Software Foundation, Inc.
   Contributed by Lawrence Crowl <crowl@google.com>

This file is part of GCC.

GCC is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option)
any later version.

GCC is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with GCC; see the file COPYING3.  If not see
<http://www.gnu.org/licenses/>.  */

#ifndef GCC_TREE_HASHER_H
#define GCC_TREE_HASHER_H 1

struct int_tree_map {
  unsigned int uid;
  tree to;
};

/* Hashtable helpers.  */

struct int_tree_hasher
{
  typedef int_tree_map value_type;
  typedef int_tree_map compare_type;
  static inline hashval_t hash (const value_type &);
  static inline bool equal (const value_type &, const compare_type &);
  static bool is_deleted (const value_type &v)
    {
      return v.to == reinterpret_cast<tree> (1);
    }
  static void mark_deleted (value_type &v) { v.to = reinterpret_cast<tree> (0x1); }
  static bool is_empty (const value_type &v) { return v.to == NULL; }
  static void mark_empty (value_type &v) { v.to = NULL; }
  static void remove (value_type &) {}
};

/* Hash a UID in a int_tree_map.  */

inline hashval_t
int_tree_hasher::hash (const value_type &item)
{
  return item.uid;
}

/* Return true if the uid in both int tree maps are equal.  */

inline bool
int_tree_hasher::equal (const value_type &a, const compare_type &b)
{
  return (a.uid == b.uid);
}

typedef hash_table <int_tree_hasher> int_tree_htab_type;

#endif /* GCC_TREE_HASHER_H  */
