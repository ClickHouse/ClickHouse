/* Default macros to initialize the lang_hooks data structure.
   Copyright (C) 2003-2018 Free Software Foundation, Inc.

This file is part of GCC.

GCC is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option)
any later version.

GCC is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with GCC; see the file COPYING3.  If not see
<http://www.gnu.org/licenses/>.  */

#ifndef GCC_HOST_HOOKS_DEF_H
#define GCC_HOST_HOOKS_DEF_H

#include "hooks.h"

#define HOST_HOOKS_EXTRA_SIGNALS hook_void_void
#if HAVE_MMAP_FILE
#define HOST_HOOKS_GT_PCH_GET_ADDRESS mmap_gt_pch_get_address
#define HOST_HOOKS_GT_PCH_USE_ADDRESS mmap_gt_pch_use_address
#else
#define HOST_HOOKS_GT_PCH_GET_ADDRESS default_gt_pch_get_address
#define HOST_HOOKS_GT_PCH_USE_ADDRESS default_gt_pch_use_address
#endif

#define HOST_HOOKS_GT_PCH_ALLOC_GRANULARITY \
  default_gt_pch_alloc_granularity

extern void* default_gt_pch_get_address (size_t, int);
extern int default_gt_pch_use_address (void *, size_t, int, size_t);
extern size_t default_gt_pch_alloc_granularity (void);
extern void* mmap_gt_pch_get_address (size_t, int);
extern int mmap_gt_pch_use_address (void *, size_t, int, size_t);

/* The structure is defined in hosthooks.h.  */
#define HOST_HOOKS_INITIALIZER {		\
  HOST_HOOKS_EXTRA_SIGNALS,			\
  HOST_HOOKS_GT_PCH_GET_ADDRESS,		\
  HOST_HOOKS_GT_PCH_USE_ADDRESS,		\
  HOST_HOOKS_GT_PCH_ALLOC_GRANULARITY		\
}

#endif /* GCC_HOST_HOOKS_DEF_H */
