/* Generic partial redundancy elimination with lazy code motion header file.
   Copyright (C) 2014-2018 Free Software Foundation, Inc.

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

#ifndef GCC_LCM_H
#define GCC_LCM_H

extern struct edge_list *pre_edge_lcm_avs (int, sbitmap *, sbitmap *,
					   sbitmap *, sbitmap *, sbitmap *,
					   sbitmap *, sbitmap **, sbitmap **);
extern struct edge_list *pre_edge_lcm (int, sbitmap *, sbitmap *,
				       sbitmap *, sbitmap *, sbitmap **,
				       sbitmap **);
extern void compute_available (sbitmap *, sbitmap *, sbitmap *, sbitmap *);
extern struct edge_list *pre_edge_rev_lcm (int, sbitmap *,
					   sbitmap *, sbitmap *,
					   sbitmap *, sbitmap **,
					   sbitmap **);
#endif /* GCC_LCM_H */
