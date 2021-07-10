/* Bits of OpenMP and OpenACC handling that is specific to device offloading
   and a lowering pass for OpenACC device directives.

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

#ifndef GCC_OMP_DEVICE_H
#define GCC_OMP_DEVICE_H

extern GTY(()) vec<tree, va_gc> *offload_funcs;
extern GTY(()) vec<tree, va_gc> *offload_vars;

extern void omp_finish_file (void);

#endif /* GCC_OMP_DEVICE_H */
