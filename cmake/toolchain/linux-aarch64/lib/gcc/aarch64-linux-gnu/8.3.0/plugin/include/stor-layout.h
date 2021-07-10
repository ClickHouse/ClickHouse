/* Definitions and declarations for stor-layout.c.
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

#ifndef GCC_STOR_LAYOUT_H
#define GCC_STOR_LAYOUT_H

extern void set_min_and_max_values_for_integral_type (tree, int, signop);
extern void fixup_signed_type (tree);
extern unsigned int update_alignment_for_field (record_layout_info, tree,
                                                unsigned int);
extern record_layout_info start_record_layout (tree);
extern tree bit_from_pos (tree, tree);
extern tree byte_from_pos (tree, tree);
extern void pos_from_bit (tree *, tree *, unsigned int, tree);
extern void normalize_offset (tree *, tree *, unsigned int);
extern tree rli_size_unit_so_far (record_layout_info);
extern tree rli_size_so_far (record_layout_info);
extern void normalize_rli (record_layout_info);
extern void place_field (record_layout_info, tree);
extern void compute_record_mode (tree);
extern void finish_bitfield_layout (tree);
extern void finish_record_layout (record_layout_info, int);
extern unsigned int element_precision (const_tree);
extern void finalize_size_functions (void);
extern void fixup_unsigned_type (tree);
extern void initialize_sizetypes (void);

/* Finish up a builtin RECORD_TYPE. Give it a name and provide its
   fields. Optionally specify an alignment, and then lay it out.  */
extern void finish_builtin_struct (tree, const char *, tree, tree);

/* Given a VAR_DECL, PARM_DECL, RESULT_DECL or FIELD_DECL node,
   calculates the DECL_SIZE, DECL_SIZE_UNIT, DECL_ALIGN and DECL_MODE
   fields.  Call this only once for any given decl node.

   Second argument is the boundary that this field can be assumed to
   be starting at (in bits).  Zero means it can be assumed aligned
   on any boundary that may be needed.  */
extern void layout_decl (tree, unsigned);

/* Given a ..._TYPE node, calculate the TYPE_SIZE, TYPE_SIZE_UNIT,
   TYPE_ALIGN and TYPE_MODE fields.  If called more than once on one
   node, does nothing except for the first time.  */
extern void layout_type (tree);

/* Return the least alignment in bytes required for type TYPE.  */
extern unsigned int min_align_of_type (tree);

/* Construct various nodes representing fract or accum data types.  */
extern tree make_fract_type (int, int, int);
extern tree make_accum_type (int, int, int);

#define make_signed_fract_type(P) make_fract_type (P, 0, 0)
#define make_unsigned_fract_type(P) make_fract_type (P, 1, 0)
#define make_sat_signed_fract_type(P) make_fract_type (P, 0, 1)
#define make_sat_unsigned_fract_type(P) make_fract_type (P, 1, 1)
#define make_signed_accum_type(P) make_accum_type (P, 0, 0)
#define make_unsigned_accum_type(P) make_accum_type (P, 1, 0)
#define make_sat_signed_accum_type(P) make_accum_type (P, 0, 1)
#define make_sat_unsigned_accum_type(P) make_accum_type (P, 1, 1)

#define make_or_reuse_signed_fract_type(P) \
		make_or_reuse_fract_type (P, 0, 0)
#define make_or_reuse_unsigned_fract_type(P) \
		make_or_reuse_fract_type (P, 1, 0)
#define make_or_reuse_sat_signed_fract_type(P) \
		make_or_reuse_fract_type (P, 0, 1)
#define make_or_reuse_sat_unsigned_fract_type(P) \
		make_or_reuse_fract_type (P, 1, 1)
#define make_or_reuse_signed_accum_type(P) \
		make_or_reuse_accum_type (P, 0, 0)
#define make_or_reuse_unsigned_accum_type(P) \
		make_or_reuse_accum_type (P, 1, 0)
#define make_or_reuse_sat_signed_accum_type(P) \
		make_or_reuse_accum_type (P, 0, 1)
#define make_or_reuse_sat_unsigned_accum_type(P) \
		make_or_reuse_accum_type (P, 1, 1)

extern tree make_signed_type (int);
extern tree make_unsigned_type (int);

/* Return the mode for data of a given size SIZE and mode class CLASS.
   If LIMIT is nonzero, then don't use modes bigger than MAX_FIXED_MODE_SIZE.
   The value is BLKmode if no other mode is found.  This is like
   mode_for_size, but is passed a tree.  */
extern opt_machine_mode mode_for_size_tree (const_tree, enum mode_class, int);

extern tree bitwise_type_for_mode (machine_mode);

/* Given a VAR_DECL, PARM_DECL or RESULT_DECL, clears the results of
   a previous call to layout_decl and calls it again.  */
extern void relayout_decl (tree);

/* variable_size (EXP) is like save_expr (EXP) except that it
   is for the special case of something that is part of a
   variable size for a data type.  It makes special arrangements
   to compute the value at the right time when the data type
   belongs to a function parameter.  */
extern tree variable_size (tree);

#endif  // GCC_STOR_LAYOUT_H
