/* IR-agnostic target query functions relating to optabs
   Copyright (C) 2001-2018 Free Software Foundation, Inc.

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

#ifndef GCC_OPTABS_QUERY_H
#define GCC_OPTABS_QUERY_H

#include "insn-opinit.h"
#include "target.h"

/* Return true if OP is a conversion optab.  */

inline bool
convert_optab_p (optab op)
{
  return op > unknown_optab && op <= LAST_CONV_OPTAB;
}

/* Return the insn used to implement mode MODE of OP, or CODE_FOR_nothing
   if the target does not have such an insn.  */

inline enum insn_code
optab_handler (optab op, machine_mode mode)
{
  unsigned scode = (op << 16) | mode;
  gcc_assert (op > LAST_CONV_OPTAB);
  return raw_optab_handler (scode);
}

/* Return the insn used to perform conversion OP from mode FROM_MODE
   to mode TO_MODE; return CODE_FOR_nothing if the target does not have
   such an insn.  */

inline enum insn_code
convert_optab_handler (convert_optab op, machine_mode to_mode,
		       machine_mode from_mode)
{
  unsigned scode = (op << 16) | (from_mode << 8) | to_mode;
  gcc_assert (convert_optab_p (op));
  return raw_optab_handler (scode);
}

enum insn_code convert_optab_handler (convert_optab, machine_mode,
				      machine_mode, optimization_type);

/* Return the insn used to implement mode MODE of OP, or CODE_FOR_nothing
   if the target does not have such an insn.  */

inline enum insn_code
direct_optab_handler (direct_optab op, machine_mode mode)
{
  return optab_handler (op, mode);
}

enum insn_code direct_optab_handler (convert_optab, machine_mode,
				     optimization_type);

/* Return true if UNOPTAB is for a trapping-on-overflow operation.  */

inline bool
trapv_unoptab_p (optab unoptab)
{
  return (unoptab == negv_optab
	  || unoptab == absv_optab);
}

/* Return true if BINOPTAB is for a trapping-on-overflow operation.  */

inline bool
trapv_binoptab_p (optab binoptab)
{
  return (binoptab == addv_optab
	  || binoptab == subv_optab
	  || binoptab == smulv_optab);
}

/* Return insn code for a comparison operator with VMODE
   resultin MASK_MODE, unsigned if UNS is true.  */

static inline enum insn_code
get_vec_cmp_icode (machine_mode vmode, machine_mode mask_mode, bool uns)
{
  optab tab = uns ? vec_cmpu_optab : vec_cmp_optab;
  return convert_optab_handler (tab, vmode, mask_mode);
}

/* Return insn code for a comparison operator with VMODE
   resultin MASK_MODE (only for EQ/NE).  */

static inline enum insn_code
get_vec_cmp_eq_icode (machine_mode vmode, machine_mode mask_mode)
{
  return convert_optab_handler (vec_cmpeq_optab, vmode, mask_mode);
}

/* Return insn code for a conditional operator with a comparison in
   mode CMODE, unsigned if UNS is true, resulting in a value of mode VMODE.  */

inline enum insn_code
get_vcond_icode (machine_mode vmode, machine_mode cmode, bool uns)
{
  enum insn_code icode = CODE_FOR_nothing;
  if (uns)
    icode = convert_optab_handler (vcondu_optab, vmode, cmode);
  else
    icode = convert_optab_handler (vcond_optab, vmode, cmode);
  return icode;
}

/* Return insn code for a conditional operator with a mask mode
   MMODE resulting in a value of mode VMODE.  */

static inline enum insn_code
get_vcond_mask_icode (machine_mode vmode, machine_mode mmode)
{
  return convert_optab_handler (vcond_mask_optab, vmode, mmode);
}

/* Return insn code for a conditional operator with a comparison in
   mode CMODE (only EQ/NE), resulting in a value of mode VMODE.  */

static inline enum insn_code
get_vcond_eq_icode (machine_mode vmode, machine_mode cmode)
{
  return convert_optab_handler (vcondeq_optab, vmode, cmode);
}

/* Enumerates the possible extraction_insn operations.  */
enum extraction_pattern { EP_insv, EP_extv, EP_extzv };

/* Describes an instruction that inserts or extracts a bitfield.  */
struct extraction_insn
{
  /* The code of the instruction.  */
  enum insn_code icode;

  /* The mode that the structure operand should have.  This is byte_mode
     when using the legacy insv, extv and extzv patterns to access memory.
     If no mode is given, the structure is a BLKmode memory.  */
  opt_scalar_int_mode struct_mode;

  /* The mode of the field to be inserted or extracted, and by extension
     the mode of the insertion or extraction itself.  */
  scalar_int_mode field_mode;

  /* The mode of the field's bit position.  This is only important
     when the position is variable rather than constant.  */
  scalar_int_mode pos_mode;
};

bool get_best_reg_extraction_insn (extraction_insn *,
				   enum extraction_pattern,
				   unsigned HOST_WIDE_INT, machine_mode);
bool get_best_mem_extraction_insn (extraction_insn *,
				   enum extraction_pattern,
				   HOST_WIDE_INT, HOST_WIDE_INT, machine_mode);

enum insn_code can_extend_p (machine_mode, machine_mode, int);
enum insn_code can_float_p (machine_mode, machine_mode, int);
enum insn_code can_fix_p (machine_mode, machine_mode, int, bool *);
bool can_conditionally_move_p (machine_mode mode);
opt_machine_mode qimode_for_vec_perm (machine_mode);
bool selector_fits_mode_p (machine_mode, const vec_perm_indices &);
bool can_vec_perm_var_p (machine_mode);
bool can_vec_perm_const_p (machine_mode, const vec_perm_indices &,
			   bool = true);
/* Find a widening optab even if it doesn't widen as much as we want.  */
#define find_widening_optab_handler(A, B, C) \
  find_widening_optab_handler_and_mode (A, B, C, NULL)
enum insn_code find_widening_optab_handler_and_mode (optab, machine_mode,
						     machine_mode,
						     machine_mode *);
int can_mult_highpart_p (machine_mode, bool);
bool can_vec_mask_load_store_p (machine_mode, machine_mode, bool);
bool can_compare_and_swap_p (machine_mode, bool);
bool can_atomic_exchange_p (machine_mode, bool);
bool can_atomic_load_p (machine_mode);
bool lshift_cheap_p (bool);
bool supports_vec_gather_load_p ();
bool supports_vec_scatter_store_p ();

/* Version of find_widening_optab_handler_and_mode that operates on
   specific mode types.  */

template<typename T>
inline enum insn_code
find_widening_optab_handler_and_mode (optab op, const T &to_mode,
				      const T &from_mode, T *found_mode)
{
  machine_mode tmp;
  enum insn_code icode = find_widening_optab_handler_and_mode
    (op, machine_mode (to_mode), machine_mode (from_mode), &tmp);
  if (icode != CODE_FOR_nothing && found_mode)
    *found_mode = as_a <T> (tmp);
  return icode;
}

#endif
