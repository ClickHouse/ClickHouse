/* Routines for expanding from SSA form to RTL.
   Copyright (C) 2009-2018 Free Software Foundation, Inc.

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


#ifndef GCC_TREE_OUTOF_SSA_H
#define GCC_TREE_OUTOF_SSA_H


/* This structure (of which only a singleton SA exists) is used to
   pass around information between the outof-SSA functions, cfgexpand
   and expand itself.  */
struct ssaexpand
{
  /* The computed partitions of SSA names are stored here.  */
  var_map map;

  /* For an SSA name version V bit V is set iff TER decided that
     its definition should be forwarded.  */
  bitmap values;

  /* For a partition number I partition_to_pseudo[I] contains the
     RTL expression of the allocated space of it (either a MEM or
     a pseudos REG).  */
  rtx *partition_to_pseudo;

  /* If partition I contains an SSA name that has a default def for a
     parameter, bit I will be set in this bitmap.  */
  bitmap partitions_for_parm_default_defs;

  /* If partition I contains an SSA name that has an undefined value,
     bit I will be set in this bitmap.  */
  bitmap partitions_for_undefined_values;
};

/* This is the singleton described above.  */
extern struct ssaexpand SA;

/* Returns the RTX expression representing the storage of the outof-SSA
   partition that the SSA name EXP is a member of.  */
static inline rtx
get_rtx_for_ssa_name (tree exp)
{
  int p = partition_find (SA.map->var_partition, SSA_NAME_VERSION (exp));
  if (SA.map->partition_to_view)
    p = SA.map->partition_to_view[p];
  gcc_assert (p != NO_PARTITION);
  return SA.partition_to_pseudo[p];
}

/* If TER decided to forward the definition of SSA name EXP this function
   returns the defining statement, otherwise NULL.  */
static inline gimple *
get_gimple_for_ssa_name (tree exp)
{
  int v = SSA_NAME_VERSION (exp);
  if (SA.values && bitmap_bit_p (SA.values, v))
    return SSA_NAME_DEF_STMT (exp);
  return NULL;
}

extern bool ssa_is_replaceable_p (gimple *stmt);
extern void finish_out_of_ssa (struct ssaexpand *sa);
extern unsigned int rewrite_out_of_ssa (struct ssaexpand *sa);
extern void expand_phi_nodes (struct ssaexpand *sa);

#endif /* GCC_TREE_OUTOF_SSA_H */
