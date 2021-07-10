/* UndefinedBehaviorSanitizer, undefined behavior detector.
   Copyright (C) 2013-2018 Free Software Foundation, Inc.
   Contributed by Marek Polacek <polacek@redhat.com>

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

#ifndef GCC_UBSAN_H
#define GCC_UBSAN_H

/* The various kinds of NULL pointer checks.  */
enum ubsan_null_ckind {
  UBSAN_LOAD_OF,
  UBSAN_STORE_OF,
  UBSAN_REF_BINDING,
  UBSAN_MEMBER_ACCESS,
  UBSAN_MEMBER_CALL,
  UBSAN_CTOR_CALL,
  UBSAN_DOWNCAST_POINTER,
  UBSAN_DOWNCAST_REFERENCE,
  UBSAN_UPCAST,
  UBSAN_CAST_TO_VBASE
};

/* This controls how ubsan prints types.  Used in ubsan_type_descriptor.  */
enum ubsan_print_style {
  UBSAN_PRINT_NORMAL,
  UBSAN_PRINT_POINTER,
  UBSAN_PRINT_ARRAY
};

/* This controls ubsan_encode_value behavior.  */
enum ubsan_encode_value_phase {
  UBSAN_ENCODE_VALUE_GENERIC,
  UBSAN_ENCODE_VALUE_GIMPLE,
  UBSAN_ENCODE_VALUE_RTL
};

extern bool ubsan_expand_bounds_ifn (gimple_stmt_iterator *);
extern bool ubsan_expand_null_ifn (gimple_stmt_iterator *);
extern bool ubsan_expand_objsize_ifn (gimple_stmt_iterator *);
extern bool ubsan_expand_ptr_ifn (gimple_stmt_iterator *);
extern bool ubsan_expand_vptr_ifn (gimple_stmt_iterator *);
extern bool ubsan_instrument_unreachable (gimple_stmt_iterator *);
extern tree ubsan_create_data (const char *, int, const location_t *, ...);
extern tree ubsan_type_descriptor (tree, ubsan_print_style
					 = UBSAN_PRINT_NORMAL);
extern tree ubsan_encode_value (tree, ubsan_encode_value_phase
				      = UBSAN_ENCODE_VALUE_GENERIC);
extern bool is_ubsan_builtin_p (tree);
extern tree ubsan_build_overflow_builtin (tree_code, location_t, tree, tree,
					  tree, tree *);
extern tree ubsan_instrument_float_cast (location_t, tree, tree);
extern tree ubsan_get_source_location_type (void);

#endif  /* GCC_UBSAN_H  */
