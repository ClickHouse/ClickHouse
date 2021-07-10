/* Common subexpression elimination for GNU compiler.
   Copyright (C) 1987-2018 Free Software Foundation, Inc.

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

#ifndef GCC_CSELIB_H
#define GCC_CSELIB_H

/* Describe a value.  */
struct cselib_val
{
  /* The hash value.  */
  unsigned int hash;

  /* A unique id assigned to values.  */
  int uid;

  /* A VALUE rtx that points back to this structure.  */
  rtx val_rtx;

  /* All rtl expressions that hold this value at the current time during a
     scan.  */
  struct elt_loc_list *locs;

  /* If this value is used as an address, points to a list of values that
     use it as an address in a MEM.  */
  struct elt_list *addr_list;

  struct cselib_val *next_containing_mem;
};

/* A list of rtl expressions that hold the same value.  */
struct elt_loc_list {
  /* Next element in the list.  */
  struct elt_loc_list *next;
  /* An rtl expression that holds the value.  */
  rtx loc;
  /* The insn that made the equivalence.  */
  rtx_insn *setting_insn;
};

/* Describe a single set that is part of an insn.  */
struct cselib_set
{
  rtx src;
  rtx dest;
  cselib_val *src_elt;
  cselib_val *dest_addr_elt;
};

enum cselib_record_what
{
  CSELIB_RECORD_MEMORY = 1,
  CSELIB_PRESERVE_CONSTANTS = 2
};

extern void (*cselib_discard_hook) (cselib_val *);
extern void (*cselib_record_sets_hook) (rtx_insn *insn, struct cselib_set *sets,
					int n_sets);

extern cselib_val *cselib_lookup (rtx, machine_mode,
				  int, machine_mode);
extern cselib_val *cselib_lookup_from_insn (rtx, machine_mode,
					    int, machine_mode, rtx_insn *);
extern void cselib_init (int);
extern void cselib_clear_table (void);
extern void cselib_finish (void);
extern void cselib_process_insn (rtx_insn *);
extern bool fp_setter_insn (rtx_insn *);
extern machine_mode cselib_reg_set_mode (const_rtx);
extern int rtx_equal_for_cselib_1 (rtx, rtx, machine_mode, int);
extern int references_value_p (const_rtx, int);
extern rtx cselib_expand_value_rtx (rtx, bitmap, int);
typedef rtx (*cselib_expand_callback)(rtx, bitmap, int, void *);
extern rtx cselib_expand_value_rtx_cb (rtx, bitmap, int,
				       cselib_expand_callback, void *);
extern bool cselib_dummy_expand_value_rtx_cb (rtx, bitmap, int,
					      cselib_expand_callback, void *);
extern rtx cselib_subst_to_values (rtx, machine_mode);
extern rtx cselib_subst_to_values_from_insn (rtx, machine_mode, rtx_insn *);
extern void cselib_invalidate_rtx (rtx);

extern void cselib_reset_table (unsigned int);
extern unsigned int cselib_get_next_uid (void);
extern void cselib_preserve_value (cselib_val *);
extern bool cselib_preserved_value_p (cselib_val *);
extern void cselib_preserve_only_values (void);
extern void cselib_preserve_cfa_base_value (cselib_val *, unsigned int);
extern void cselib_add_permanent_equiv (cselib_val *, rtx, rtx_insn *);
extern bool cselib_have_permanent_equivalences (void);
extern void cselib_set_value_sp_based (cselib_val *);
extern bool cselib_sp_based_value_p (cselib_val *);

extern void dump_cselib_table (FILE *);

/* Return the canonical value for VAL, following the equivalence chain
   towards the earliest (== lowest uid) equivalent value.  */

static inline cselib_val *
canonical_cselib_val (cselib_val *val)
{
  cselib_val *canon;

  if (!val->locs || val->locs->next
      || !val->locs->loc || GET_CODE (val->locs->loc) != VALUE
      || val->uid < CSELIB_VAL_PTR (val->locs->loc)->uid)
    return val;

  canon = CSELIB_VAL_PTR (val->locs->loc);
  gcc_checking_assert (canonical_cselib_val (canon) == canon);
  return canon;
}

/* Return nonzero if we can prove that X and Y contain the same value, taking
   our gathered information into account.  */

static inline int
rtx_equal_for_cselib_p (rtx x, rtx y)
{
  if (x == y)
    return 1;

  return rtx_equal_for_cselib_1 (x, y, VOIDmode, 0);
}

#endif /* GCC_CSELIB_H */
