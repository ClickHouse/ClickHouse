/* Declarations and data structures for stmt.c.
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

#ifndef GCC_STMT_H
#define GCC_STMT_H

extern void expand_label (tree);
extern bool parse_output_constraint (const char **, int, int, int,
				     bool *, bool *, bool *);
extern bool parse_input_constraint (const char **, int, int, int, int,
				    const char * const *, bool *, bool *);
extern tree resolve_asm_operand_names (tree, tree, tree, tree);
#ifdef HARD_CONST
/* Silly ifdef to avoid having all includers depend on hard-reg-set.h.  */
extern tree tree_overlaps_hard_reg_set (tree, HARD_REG_SET *);
#endif

/* Return the CODE_LABEL rtx for a LABEL_DECL, creating it if necessary.
   If label was deleted, the corresponding note
   (NOTE_INSN_DELETED{_DEBUG,}_LABEL) insn will be returned.  */
extern rtx_insn *label_rtx (tree);

/* As label_rtx, but additionally the label is placed on the forced label
   list of its containing function (i.e. it is treated as reachable even
   if how is not obvious).  */
extern rtx_insn *force_label_rtx (tree);

/* As label_rtx, but checks that label was not deleted.  */
extern rtx_code_label *jump_target_rtx (tree);

/* Expand a GIMPLE_SWITCH statement.  */
extern void expand_case (gswitch *);

/* Like expand_case but special-case for SJLJ exception dispatching.  */
extern void expand_sjlj_dispatch_table (rtx, vec<tree> );

#endif  // GCC_STMT_H
