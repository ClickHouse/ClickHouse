/* coverage.h - Defines data exported from coverage.c
   Copyright (C) 1998-2018 Free Software Foundation, Inc.

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

#ifndef GCC_COVERAGE_H
#define GCC_COVERAGE_H

#include "gcov-io.h"

extern void coverage_init (const char *);
extern void coverage_finish (void);
extern void coverage_remove_note_file (void);

/* Start outputting coverage information for the current
   function.  */
extern int coverage_begin_function (unsigned, unsigned);

/* Complete the coverage information for the current function.  */
extern void coverage_end_function (unsigned, unsigned);

/* Compute the control flow checksum for the function FN given as argument.  */
extern unsigned coverage_compute_cfg_checksum (struct function *fn);

/* Compute the profile id of function N.  */
extern unsigned coverage_compute_profile_id (struct cgraph_node *n);

/* Compute the line number checksum for the current function.  */
extern unsigned coverage_compute_lineno_checksum (void);

/* Allocate some counters. Repeatable per function.  */
extern int coverage_counter_alloc (unsigned /*counter*/, unsigned/*num*/);
/* Use a counter from the most recent allocation.  */
extern tree tree_coverage_counter_ref (unsigned /*counter*/, unsigned/*num*/);
/* Use a counter address from the most recent allocation.  */
extern tree tree_coverage_counter_addr (unsigned /*counter*/, unsigned/*num*/);

/* Get all the counters for the current function.  */
extern gcov_type *get_coverage_counts (unsigned /*counter*/,
				       unsigned /*expected*/,
				       unsigned /*cfg_checksum*/,
				       unsigned /*lineno_checksum*/,
				       const struct gcov_ctr_summary **);

extern tree get_gcov_type (void);
extern bool coverage_node_map_initialized_p (void);

#endif
