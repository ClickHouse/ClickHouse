/* Declarations for printing trees in human readable form
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

#ifndef GCC_PRINT_TREE_H
#define GCC_PRINT_TREE_H

extern void debug_tree (tree);
extern void debug_raw (const tree_node &ref);
extern void debug_raw (const tree_node *ptr);
extern void debug (const tree_node &ref);
extern void debug (const tree_node *ptr);
extern void debug_verbose (const tree_node &ref);
extern void debug_verbose (const tree_node *ptr);
extern void debug_head (const tree_node &ref);
extern void debug_head (const tree_node *ptr);
extern void debug_body (const tree_node &ref);
extern void debug_body (const tree_node *ptr);
extern void debug (vec<tree, va_gc> &ref);
extern void debug (vec<tree, va_gc> *ptr);
extern void debug_raw (vec<tree, va_gc> &ref);
extern void debug_raw (vec<tree, va_gc> *ptr);
#ifdef BUFSIZ
extern void dump_addr (FILE*, const char *, const void *);
extern void print_node (FILE *, const char *, tree, int,
			bool brief_for_visited = true);
extern void print_node_brief (FILE *, const char *, const_tree, int);
extern void indent_to (FILE *, int);
#endif

#endif  // GCC_PRINT_TREE_H
