/* A memory statistics traits.
   Copyright (C) 2015-2018 Free Software Foundation, Inc.
   Contributed by Martin Liska  <mliska@suse.cz>

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

#ifndef GCC_MEM_STATS_TRAITS_H
#define GCC_MEM_STATS_TRAITS_H

/* Memory allocation origin.  */
enum mem_alloc_origin
{
  HASH_TABLE_ORIGIN,
  HASH_MAP_ORIGIN,
  HASH_SET_ORIGIN,
  VEC_ORIGIN,
  BITMAP_ORIGIN,
  GGC_ORIGIN,
  ALLOC_POOL_ORIGIN,
  MEM_ALLOC_ORIGIN_LENGTH
};

/* Verbose names of the memory allocation origin.  */
static const char * mem_alloc_origin_names[] = { "Hash tables", "Hash maps",
  "Hash sets", "Heap vectors", "Bitmaps", "GGC memory", "Allocation pool" };

#endif // GCC_MEM_STATS_TRAITS_H
