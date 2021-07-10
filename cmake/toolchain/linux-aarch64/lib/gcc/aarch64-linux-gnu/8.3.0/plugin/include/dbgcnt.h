/* Debug counter for debugging support
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
<http://www.gnu.org/licenses/>.

See dbgcnt.def for usage information.  */

#ifndef GCC_DBGCNT_H
#define GCC_DBGCNT_H

#define DEBUG_COUNTER(a) a,

enum debug_counter {
#include "dbgcnt.def"
   debug_counter_number_of_counters
};

#undef DEBUG_COUNTER

extern bool dbg_cnt_is_enabled (enum debug_counter index);
extern bool dbg_cnt (enum debug_counter index);
extern void dbg_cnt_process_opt (const char *arg);
extern void dbg_cnt_list_all_counters (void);

#endif /* GCC_DBGCNT_H */
