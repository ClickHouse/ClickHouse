/* Header file for exception handling.
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

#ifndef GCC_TREE_EH_H
#define GCC_TREE_EH_H


typedef struct eh_region_d *eh_region;

extern void using_eh_for_cleanups (void);
extern void add_stmt_to_eh_lp (gimple *, int);
extern bool remove_stmt_from_eh_lp_fn (struct function *, gimple *);
extern bool remove_stmt_from_eh_lp (gimple *);
extern int lookup_stmt_eh_lp_fn (struct function *, gimple *);
extern int lookup_stmt_eh_lp (gimple *);
extern bool make_eh_dispatch_edges (geh_dispatch *);
extern void make_eh_edges (gimple *);
extern edge redirect_eh_edge (edge, basic_block);
extern void redirect_eh_dispatch_edge (geh_dispatch *, edge, basic_block);
extern bool operation_could_trap_helper_p (enum tree_code, bool, bool, bool,
					   bool, tree, bool *);
extern bool operation_could_trap_p (enum tree_code, bool, bool, tree);
extern bool tree_could_trap_p (tree);
extern tree rewrite_to_non_trapping_overflow (tree);
extern bool stmt_could_throw_p (gimple *);
extern bool tree_could_throw_p (tree);
extern bool stmt_can_throw_external (gimple *);
extern bool stmt_can_throw_internal (gimple *);
extern bool maybe_clean_eh_stmt_fn (struct function *, gimple *);
extern bool maybe_clean_eh_stmt (gimple *);
extern bool maybe_clean_or_replace_eh_stmt (gimple *, gimple *);
extern bool maybe_duplicate_eh_stmt_fn (struct function *, gimple *,
					struct function *, gimple *,
					hash_map<void *, void *> *, int);
extern bool maybe_duplicate_eh_stmt (gimple *, gimple *);
extern void maybe_remove_unreachable_handlers (void);
extern bool verify_eh_edges (gimple *);
extern bool verify_eh_dispatch_edge (geh_dispatch *);

#endif /* GCC_TREE_EH_H */
