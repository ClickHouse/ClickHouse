/* Garbage collection for the GNU compiler.  Internal definitions
   for ggc-*.c and stringpool.c.

   Copyright (C) 2009-2018 Free Software Foundation, Inc.

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

#ifndef GCC_GGC_INTERNAL_H
#define GCC_GGC_INTERNAL_H


/* Call ggc_set_mark on all the roots.  */
extern void ggc_mark_roots (void);

/* Stringpool.  */

/* Mark the entries in the string pool.  */
extern void ggc_mark_stringpool	(void);

/* Purge the entries in the string pool.  */
extern void ggc_purge_stringpool (void);

/* Save and restore the string pool entries for PCH.  */

extern void gt_pch_save_stringpool (void);
extern void gt_pch_fixup_stringpool (void);
extern void gt_pch_restore_stringpool (void);

/* PCH and GGC handling for strings, mostly trivial.  */
extern void gt_pch_p_S (void *, void *, gt_pointer_operator, void *);

/* PCH.  */

struct ggc_pch_data;

/* Return a new ggc_pch_data structure.  */
extern struct ggc_pch_data *init_ggc_pch (void);

/* The second parameter and third parameters give the address and size
   of an object.  Update the ggc_pch_data structure with as much of
   that information as is necessary. The bool argument should be true
   if the object is a string.  */
extern void ggc_pch_count_object (struct ggc_pch_data *, void *, size_t, bool);

/* Return the total size of the data to be written to hold all
   the objects previously passed to ggc_pch_count_object.  */
extern size_t ggc_pch_total_size (struct ggc_pch_data *);

/* The objects, when read, will most likely be at the address
   in the second parameter.  */
extern void ggc_pch_this_base (struct ggc_pch_data *, void *);

/* Assuming that the objects really do end up at the address
   passed to ggc_pch_this_base, return the address of this object.
   The bool argument should be true if the object is a string.  */
extern char *ggc_pch_alloc_object (struct ggc_pch_data *, void *, size_t, bool);

/* Write out any initial information required.  */
extern void ggc_pch_prepare_write (struct ggc_pch_data *, FILE *);

/* Write out this object, including any padding.  The last argument should be
   true if the object is a string.  */
extern void ggc_pch_write_object (struct ggc_pch_data *, FILE *, void *,
				  void *, size_t, bool);

/* All objects have been written, write out any final information
   required.  */
extern void ggc_pch_finish (struct ggc_pch_data *, FILE *);

/* A PCH file has just been read in at the address specified second
   parameter.  Set up the GC implementation for the new objects.  */
extern void ggc_pch_read (FILE *, void *);


/* Allocation and collection.  */

/* When set, ggc_collect will do collection.  */
extern bool ggc_force_collect;

extern void ggc_record_overhead (size_t, size_t, void * FINAL_MEM_STAT_DECL);

extern void ggc_free_overhead (void *);

extern void ggc_prune_overhead_list (void);

/* Return the number of bytes allocated at the indicated address.  */
extern size_t ggc_get_size (const void *);


/* Statistics.  */

/* This structure contains the statistics common to all collectors.
   Particular collectors can extend this structure.  */
struct ggc_statistics
{
  /* At present, we don't really gather any interesting statistics.  */
  int unused;
};

/* Used by the various collectors to gather and print statistics that
   do not depend on the collector in use.  */
extern void ggc_print_common_statistics (FILE *, ggc_statistics *);

#endif
