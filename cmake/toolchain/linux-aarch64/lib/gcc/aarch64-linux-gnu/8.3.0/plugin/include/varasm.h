/* Declarations for varasm.h.
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

#ifndef GCC_VARASM_H
#define GCC_VARASM_H

/* The following global holds the "function name" for the code in the
   cold section of a function, if hot/cold function splitting is enabled
   and there was actually code that went into the cold section.  A
   pseudo function name is needed for the cold section of code for some
   debugging tools that perform symbolization. */
extern tree cold_function_name;

extern tree tree_output_constant_def (tree);
extern void make_decl_rtl (tree);
extern rtx make_decl_rtl_for_debug (tree);
extern void make_decl_one_only (tree, tree);
extern int supports_one_only (void);
extern void resolve_unique_section (tree, int, int);
extern void mark_referenced (tree);
extern void mark_decl_referenced (tree);
extern void notice_global_symbol (tree);
extern void set_user_assembler_name (tree, const char *);
extern void process_pending_assemble_externals (void);
extern bool decl_replaceable_p (tree);
extern bool decl_binds_to_current_def_p (const_tree);
extern enum tls_model decl_default_tls_model (const_tree);

/* Declare DECL to be a weak symbol.  */
extern void declare_weak (tree);

/* Merge weak status.  */
extern void merge_weak (tree, tree);

/* Make one symbol an alias for another.  */
extern void assemble_alias (tree, tree);

/* Return nonzero if VALUE is a valid constant-valued expression
   for use in initializing a static variable; one that can be an
   element of a "constant" initializer.

   Return null_pointer_node if the value is absolute;
   if it is relocatable, return the variable that determines the relocation.
   We assume that VALUE has been folded as much as possible;
   therefore, we do not need to check for such things as
   arithmetic-combinations of integers.  */
extern tree initializer_constant_valid_p (tree, tree, bool = false);

/* Return true if VALUE is a valid constant-valued expression
   for use in initializing a static bit-field; one that can be
   an element of a "constant" initializer.  */
extern bool initializer_constant_valid_for_bitfield_p (tree);

/* Whether a constructor CTOR is a valid static constant initializer if all
   its elements are.  This used to be internal to initializer_constant_valid_p
   and has been exposed to let other functions like categorize_ctor_elements
   evaluate the property while walking a constructor for other purposes.  */
extern bool constructor_static_from_elts_p (const_tree);

extern void init_varasm_status (void);

extern rtx assemble_static_space (unsigned HOST_WIDE_INT);

extern rtx assemble_trampoline_template (void);

#endif  // GCC_VARASM_H
