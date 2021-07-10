/* Instruction scheduling pass.
   Copyright (C) 2006-2018 Free Software Foundation, Inc.

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

#ifndef GCC_SEL_SCHED_H
#define GCC_SEL_SCHED_H

/* The main entry point.  */
extern void run_selective_scheduling (void);
extern bool maybe_skip_selective_scheduling (void);

#endif /* GCC_SEL_SCHED_H */
