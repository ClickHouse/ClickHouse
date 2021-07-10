/* Exported functions from alias.c
   Copyright (C) 2004-2018 Free Software Foundation, Inc.

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

#ifndef GCC_ALIAS_H
#define GCC_ALIAS_H

extern alias_set_type new_alias_set (void);
extern alias_set_type get_alias_set (tree);
extern alias_set_type get_deref_alias_set (tree);
extern alias_set_type get_varargs_alias_set (void);
extern alias_set_type get_frame_alias_set (void);
extern tree component_uses_parent_alias_set_from (const_tree);
extern bool alias_set_subset_of (alias_set_type, alias_set_type);
extern void record_alias_subset (alias_set_type, alias_set_type);
extern void record_component_aliases (tree);
extern int alias_sets_conflict_p (alias_set_type, alias_set_type);
extern int alias_sets_must_conflict_p (alias_set_type, alias_set_type);
extern int objects_must_conflict_p (tree, tree);
extern int nonoverlapping_memrefs_p (const_rtx, const_rtx, bool);
extern void dump_alias_stats_in_alias_c (FILE *s);
tree reference_alias_ptr_type (tree);
bool alias_ptr_types_compatible_p (tree, tree);
int compare_base_decls (tree, tree);

/* This alias set can be used to force a memory to conflict with all
   other memories, creating a barrier across which no memory reference
   can move.  Note that there are other legacy ways to create such
   memory barriers, including an address of SCRATCH.  */
#define ALIAS_SET_MEMORY_BARRIER	((alias_set_type) -1)

#endif /* GCC_ALIAS_H */
