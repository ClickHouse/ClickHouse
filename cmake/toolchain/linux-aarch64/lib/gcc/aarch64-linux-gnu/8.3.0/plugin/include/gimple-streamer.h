/* Data structures and functions for streaming GIMPLE.

   Copyright (C) 2011-2018 Free Software Foundation, Inc.
   Contributed by Diego Novillo <dnovillo@google.com>

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

#ifndef GCC_GIMPLE_STREAMER_H
#define GCC_GIMPLE_STREAMER_H

#include "tree-streamer.h"

/* In gimple-streamer-in.c  */
void input_bb (struct lto_input_block *, enum LTO_tags, struct data_in *,
	       struct function *, int);

/* In gimple-streamer-out.c  */
void output_bb (struct output_block *, basic_block, struct function *);

#endif  /* GCC_GIMPLE_STREAMER_H  */
