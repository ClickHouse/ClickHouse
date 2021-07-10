/* Expansion pass for OMP directives.  Outlines regions of certain OMP
   directives to separate functions, converts others into explicit calls to the
   runtime library (libgomp) and so forth

Copyright (C) 2005-2018 Free Software Foundation, Inc.

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

#ifndef GCC_OMP_EXPAND_H
#define GCC_OMP_EXPAND_H

struct omp_region;
extern void omp_expand_local (basic_block head);
extern void omp_free_regions (void);
extern bool omp_make_gimple_edges (basic_block bb, struct omp_region **region,
				   int *region_idx);

#endif /* GCC_OMP_EXPAND_H */
