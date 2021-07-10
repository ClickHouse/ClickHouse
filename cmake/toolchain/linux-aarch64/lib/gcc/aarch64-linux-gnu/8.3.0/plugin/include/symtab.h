/* Hash tables.
   Copyright (C) 2000-2018 Free Software Foundation, Inc.

This program is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation; either version 3, or (at your option) any
later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; see the file COPYING3.  If not see
<http://www.gnu.org/licenses/>.  */

#ifndef LIBCPP_SYMTAB_H
#define LIBCPP_SYMTAB_H

#include "obstack.h"

#ifndef GTY
#define GTY(x) /* nothing */
#endif

/* This is what each hash table entry points to.  It may be embedded
   deeply within another object.  */
typedef struct ht_identifier ht_identifier;
typedef struct ht_identifier *ht_identifier_ptr;
struct GTY(()) ht_identifier {
  const unsigned char *str;
  unsigned int len;
  unsigned int hash_value;
};

#define HT_LEN(NODE) ((NODE)->len)
#define HT_STR(NODE) ((NODE)->str)

typedef struct ht cpp_hash_table;
typedef struct ht_identifier *hashnode;

enum ht_lookup_option {HT_NO_INSERT = 0, HT_ALLOC};

/* An identifier hash table for cpplib and the front ends.  */
struct ht
{
  /* Identifiers are allocated from here.  */
  struct obstack stack;

  hashnode *entries;
  /* Call back, allocate a node.  */
  hashnode (*alloc_node) (cpp_hash_table *);
  /* Call back, allocate something that hangs off a node like a cpp_macro.  
     NULL means use the usual allocator.  */
  void * (*alloc_subobject) (size_t);

  unsigned int nslots;		/* Total slots in the entries array.  */
  unsigned int nelements;	/* Number of live elements.  */

  /* Link to reader, if any.  For the benefit of cpplib.  */
  struct cpp_reader *pfile;

  /* Table usage statistics.  */
  unsigned int searches;
  unsigned int collisions;

  /* Should 'entries' be freed when it is no longer needed?  */
  bool entries_owned;
};

/* Initialize the hashtable with 2 ^ order entries.  */
extern cpp_hash_table *ht_create (unsigned int order);

/* Frees all memory associated with a hash table.  */
extern void ht_destroy (cpp_hash_table *);

extern hashnode ht_lookup (cpp_hash_table *, const unsigned char *,
			   size_t, enum ht_lookup_option);
extern hashnode ht_lookup_with_hash (cpp_hash_table *, const unsigned char *,
                                     size_t, unsigned int,
                                     enum ht_lookup_option);
#define HT_HASHSTEP(r, c) ((r) * 67 + ((c) - 113));
#define HT_HASHFINISH(r, len) ((r) + (len))

/* For all nodes in TABLE, make a callback.  The callback takes
   TABLE->PFILE, the node, and a PTR, and the callback sequence stops
   if the callback returns zero.  */
typedef int (*ht_cb) (struct cpp_reader *, hashnode, const void *);
extern void ht_forall (cpp_hash_table *, ht_cb, const void *);

/* For all nodes in TABLE, call the callback.  If the callback returns
   a nonzero value, the node is removed from the table.  */
extern void ht_purge (cpp_hash_table *, ht_cb, const void *);

/* Restore the hash table.  */
extern void ht_load (cpp_hash_table *ht, hashnode *entries,
		     unsigned int nslots, unsigned int nelements, bool own);

/* Dump allocation statistics to stderr.  */
extern void ht_dump_statistics (cpp_hash_table *);

#endif /* LIBCPP_SYMTAB_H */
