/* Routines for liveness in SSA trees.
   Copyright (C) 2003-2018 Free Software Foundation, Inc.
   Contributed by Andrew MacLeod  <amacleod@redhat.com>

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


#ifndef _TREE_SSA_LIVE_H
#define _TREE_SSA_LIVE_H 1

#include "partition.h"

/* Used to create the variable mapping when we go out of SSA form.

   Mapping from an ssa_name to a partition number is maintained, as well as
   partition number back to ssa_name.

   This data structure also supports "views", which work on a subset of all
   partitions.  This allows the coalescer to decide what partitions are
   interesting to it, and only work with those partitions.  Whenever the view
   is changed, the partition numbers change, but none of the partition groupings
   change. (ie, it is truly a view since it doesn't change anything)

   The final component of the data structure is the basevar map.  This provides
   a list of all the different base variables which occur in a partition view,
   and a unique index for each one. Routines are provided to quickly produce
   the base variable of a partition.

   Note that members of a partition MUST all have the same base variable.  */

typedef struct _var_map
{
  /* The partition manager of all variables.  */
  partition var_partition;

  /* Vector for managing partitions views.  */
  int *partition_to_view;
  int *view_to_partition;

  /* Current number of partitions in var_map based on the current view.  */
  unsigned int num_partitions;

  /* Original full partition size.  */
  unsigned int partition_size;

  /* Number of base variables in the base var list.  */
  int num_basevars;

  /* Map of partitions numbers to base variable table indexes.  */
  int *partition_to_base_index;
} *var_map;


/* Value used to represent no partition number.  */
#define NO_PARTITION		-1

extern var_map init_var_map (int);
extern void delete_var_map (var_map);
extern int var_union (var_map, tree, tree);
extern void partition_view_normal (var_map);
extern void partition_view_bitmap (var_map, bitmap);
extern void dump_scope_blocks (FILE *, dump_flags_t);
extern void debug_scope_block (tree, dump_flags_t);
extern void debug_scope_blocks (dump_flags_t);
extern void remove_unused_locals (void);
extern void dump_var_map (FILE *, var_map);
extern void debug (_var_map &ref);
extern void debug (_var_map *ptr);


/* Return number of partitions in MAP.  */

static inline unsigned
num_var_partitions (var_map map)
{
  return map->num_partitions;
}


/* Given partition index I from MAP, return the variable which represents that
   partition.  */

static inline tree
partition_to_var (var_map map, int i)
{
  tree name;
  if (map->view_to_partition)
    i = map->view_to_partition[i];
  i = partition_find (map->var_partition, i);
  name = ssa_name (i);
  return name;
}


/* Given ssa_name VERSION, if it has a partition in MAP,  return the var it
   is associated with.  Otherwise return NULL.  */

static inline tree
version_to_var (var_map map, int version)
{
  int part;
  part = partition_find (map->var_partition, version);
  if (map->partition_to_view)
    part = map->partition_to_view[part];
  if (part == NO_PARTITION)
    return NULL_TREE;

  return partition_to_var (map, part);
}


/* Given VAR, return the partition number in MAP which contains it.
   NO_PARTITION is returned if it's not in any partition.  */

static inline int
var_to_partition (var_map map, tree var)
{
  int part;

  part = partition_find (map->var_partition, SSA_NAME_VERSION (var));
  if (map->partition_to_view)
    part = map->partition_to_view[part];
  return part;
}


/* Given VAR, return the variable which represents the entire partition
   it is a member of in MAP.  NULL is returned if it is not in a partition.  */

static inline tree
var_to_partition_to_var (var_map map, tree var)
{
  int part;

  part = var_to_partition (map, var);
  if (part == NO_PARTITION)
    return NULL_TREE;
  return partition_to_var (map, part);
}


/* Return the index into the basevar table for PARTITION's base in MAP.  */

static inline int
basevar_index (var_map map, int partition)
{
  gcc_checking_assert (partition >= 0
	      	       && partition <= (int) num_var_partitions (map));
  return map->partition_to_base_index[partition];
}


/* Return the number of different base variables in MAP.  */

static inline int
num_basevars (var_map map)
{
  return map->num_basevars;
}


/*  ---------------- live on entry/exit info ------------------------------

    This structure is used to represent live range information on SSA based
    trees. A partition map must be provided, and based on the active partitions,
    live-on-entry information and live-on-exit information can be calculated.
    As well, partitions are marked as to whether they are global (live
    outside the basic block they are defined in).

    The live-on-entry information is per block.  It provide a bitmap for
    each block which has a bit set for each partition that is live on entry to
    that block.

    The live-on-exit information is per block.  It provides a bitmap for each
    block indicating which partitions are live on exit from the block.

    For the purposes of this implementation, we treat the elements of a PHI
    as follows:

       Uses in a PHI are considered LIVE-ON-EXIT to the block from which they
       originate. They are *NOT* considered live on entry to the block
       containing the PHI node.

       The Def of a PHI node is *not* considered live on entry to the block.
       It is considered to be "define early" in the block. Picture it as each
       block having a stmt (or block-preheader) before the first real stmt in
       the block which defines all the variables that are defined by PHIs.

    -----------------------------------------------------------------------  */


typedef struct tree_live_info_d
{
  /* Var map this relates to.  */
  var_map map;

  /* Bitmap indicating which partitions are global.  */
  bitmap global;

  /* Bitmaps of live on entry blocks for partition elements.  */
  bitmap_head *livein;

  /* Bitmaps of what variables are live on exit for a basic blocks.  */
  bitmap_head *liveout;

  /* Number of basic blocks when live on exit calculated.  */
  int num_blocks;

  /* Vector used when creating live ranges as a visited stack.  */
  int *work_stack;

  /* Top of workstack.  */
  int *stack_top;

  /* Obstacks to allocate the bitmaps on.  */
  bitmap_obstack livein_obstack;
  bitmap_obstack liveout_obstack;
} *tree_live_info_p;


#define LIVEDUMP_ENTRY	0x01
#define LIVEDUMP_EXIT	0x02
#define LIVEDUMP_ALL	(LIVEDUMP_ENTRY | LIVEDUMP_EXIT)
extern void delete_tree_live_info (tree_live_info_p);
extern tree_live_info_p calculate_live_ranges (var_map, bool);
extern void debug (tree_live_info_d &ref);
extern void debug (tree_live_info_d *ptr);
extern void dump_live_info (FILE *, tree_live_info_p, int);


/*  Return TRUE if P is marked as a global in LIVE.  */

static inline int
partition_is_global (tree_live_info_p live, int p)
{
  gcc_checking_assert (live->global);
  return bitmap_bit_p (live->global, p);
}


/* Return the bitmap from LIVE representing the live on entry blocks for
   partition P.  */

static inline bitmap
live_on_entry (tree_live_info_p live, basic_block bb)
{
  gcc_checking_assert (live->livein
		       && bb != ENTRY_BLOCK_PTR_FOR_FN (cfun)
		       && bb != EXIT_BLOCK_PTR_FOR_FN (cfun));

  return &live->livein[bb->index];
}


/* Return the bitmap from LIVE representing the live on exit partitions from
   block BB.  */

static inline bitmap
live_on_exit (tree_live_info_p live, basic_block bb)
{
  gcc_checking_assert (live->liveout
		       && bb != ENTRY_BLOCK_PTR_FOR_FN (cfun)
		       && bb != EXIT_BLOCK_PTR_FOR_FN (cfun));

  return &live->liveout[bb->index];
}


/* Return the partition map which the information in LIVE utilizes.  */

static inline var_map
live_var_map (tree_live_info_p live)
{
  return live->map;
}


/* Merge the live on entry information in LIVE for partitions P1 and P2. Place
   the result into P1.  Clear P2.  */

static inline void
live_merge_and_clear (tree_live_info_p live, int p1, int p2)
{
  gcc_checking_assert (&live->livein[p1] && &live->livein[p2]);
  bitmap_ior_into (&live->livein[p1], &live->livein[p2]);
  bitmap_clear (&live->livein[p2]);
}


/* Mark partition P as live on entry to basic block BB in LIVE.  */

static inline void
make_live_on_entry (tree_live_info_p live, basic_block bb , int p)
{
  bitmap_set_bit (&live->livein[bb->index], p);
  bitmap_set_bit (live->global, p);
}

#endif /* _TREE_SSA_LIVE_H  */
