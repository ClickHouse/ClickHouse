/* Lowering and expansion of OpenMP directives for HSA GPU agents.

   Copyright (C) 2013-2018 Free Software Foundation, Inc.

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

#ifndef GCC_OMP_GRID_H
#define GCC_OMP_GRID_H

extern tree omp_grid_lastprivate_predicate (struct omp_for_data *fd);
extern void omp_grid_gridify_all_targets (gimple_seq *body_p);

#endif /* GCC_OMP_GRID_H */
