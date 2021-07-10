/* Data structures and functions for streaming trees.

   Copyright (C) 2011-2018 Free Software Foundation, Inc.
   Contributed by Diego Novillo <dnovillo@google.com>

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

#ifndef GCC_TREE_STREAMER_H
#define GCC_TREE_STREAMER_H

#include "streamer-hooks.h"
#include "data-streamer.h"

/* Cache of pickled nodes.  Used to avoid writing the same node more
   than once.  The first time a tree node is streamed out, it is
   entered in this cache.  Subsequent references to the same node are
   resolved by looking it up in this cache.

   This is used in two ways:

   - On the writing side, the first time T is added to STREAMER_CACHE,
     a new reference index is created for T and T is emitted on the
     stream.  If T needs to be emitted again to the stream, instead of
     pickling it again, the reference index is emitted.

   - On the reading side, the first time T is read from the stream, it
     is reconstructed in memory and a new reference index created for
     T.  The reconstructed T is inserted in some array so that when
     the reference index for T is found in the input stream, it can be
     used to look up into the array to get the reconstructed T.  */

struct streamer_tree_cache_d
{
  /* The mapping between tree nodes and slots into the nodes array.  */
  hash_map<tree, unsigned> *node_map;

  /* The nodes pickled so far.  */
  vec<tree> nodes;
  /* The node hashes (if available).  */
  vec<hashval_t> hashes;

  /* Next index to assign.  */
  unsigned next_idx;
};

/* In tree-streamer-in.c.  */
tree streamer_read_string_cst (struct data_in *, struct lto_input_block *);
tree streamer_read_chain (struct lto_input_block *, struct data_in *);
tree streamer_alloc_tree (struct lto_input_block *, struct data_in *,
		          enum LTO_tags);
void streamer_read_tree_body (struct lto_input_block *, struct data_in *, tree);
tree streamer_get_pickled_tree (struct lto_input_block *, struct data_in *);
void streamer_read_tree_bitfields (struct lto_input_block *,
				   struct data_in *, tree);

/* In tree-streamer-out.c.  */
void streamer_write_string_cst (struct output_block *,
				struct lto_output_stream *, tree);
void streamer_write_chain (struct output_block *, tree, bool);
void streamer_write_tree_header (struct output_block *, tree);
void streamer_write_tree_bitfields (struct output_block *, tree);
void streamer_write_tree_body (struct output_block *, tree, bool);
void streamer_write_integer_cst (struct output_block *, tree, bool);

/* In tree-streamer.c.  */
extern unsigned char streamer_mode_table[1 << 8];
void streamer_check_handled_ts_structures (void);
bool streamer_tree_cache_insert (struct streamer_tree_cache_d *, tree,
				 hashval_t, unsigned *);
void streamer_tree_cache_replace_tree (struct streamer_tree_cache_d *, tree,
				       unsigned);
void streamer_tree_cache_append (struct streamer_tree_cache_d *, tree,
				 hashval_t);
bool streamer_tree_cache_lookup (struct streamer_tree_cache_d *, tree,
				 unsigned *);
struct streamer_tree_cache_d *streamer_tree_cache_create (bool, bool, bool);
void streamer_tree_cache_delete (struct streamer_tree_cache_d *);

/* Return the tree node at slot IX in CACHE.  */

static inline tree
streamer_tree_cache_get_tree (struct streamer_tree_cache_d *cache, unsigned ix)
{
  return cache->nodes[ix];
}

/* Return the tree hash value at slot IX in CACHE.  */

static inline hashval_t
streamer_tree_cache_get_hash (struct streamer_tree_cache_d *cache, unsigned ix)
{
  return cache->hashes[ix];
}

static inline void
bp_pack_machine_mode (struct bitpack_d *bp, machine_mode mode)
{
  streamer_mode_table[mode] = 1;
  bp_pack_enum (bp, machine_mode, 1 << 8, mode);
}

static inline machine_mode
bp_unpack_machine_mode (struct bitpack_d *bp)
{
  return (machine_mode)
	   ((struct lto_input_block *)
	    bp->stream)->mode_table[bp_unpack_enum (bp, machine_mode, 1 << 8)];
}

#endif  /* GCC_TREE_STREAMER_H  */
