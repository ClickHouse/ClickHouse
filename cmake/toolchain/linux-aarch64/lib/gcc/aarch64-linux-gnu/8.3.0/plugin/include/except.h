/* Exception Handling interface routines.
   Copyright (C) 1996-2018 Free Software Foundation, Inc.
   Contributed by Mike Stump <mrs@cygnus.com>.

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

/* No include guards here, but define an include file marker anyway, so
   that the compiler can keep track of where this file is included.  This
   is e.g. used to avoid including this file in front-end specific files.  */
#ifndef GCC_EXCEPT_H
#define GCC_EXCEPT_H


struct function;
struct eh_region_d;

/* The type of an exception region.  */
enum eh_region_type
{
  /* CLEANUP regions implement e.g. destructors run when exiting a block.
     They can be generated from both GIMPLE_TRY_FINALLY and GIMPLE_TRY_CATCH
     nodes.  It is expected by the runtime that cleanup regions will *not*
     resume normal program flow, but will continue propagation of the
     exception.  */
  ERT_CLEANUP,

  /* TRY regions implement catching an exception.  The list of types associated
     with the attached catch handlers is examined in order by the runtime and
     control is transferred to the appropriate handler.  Note that a NULL type
     list is a catch-all handler, and that it will catch *all* exceptions
     including those originating from a different language.  */
  ERT_TRY,

  /* ALLOWED_EXCEPTIONS regions implement exception filtering, e.g. the
     throw(type-list) specification that can be added to C++ functions.
     The runtime examines the thrown exception vs the type list, and if
     the exception does not match, transfers control to the handler.  The
     normal handler for C++ calls __cxa_call_unexpected.  */
  ERT_ALLOWED_EXCEPTIONS,

  /* MUST_NOT_THROW regions prevent all exceptions from propagating.  This
     region type is used in C++ to surround destructors being run inside a
     CLEANUP region.  This differs from an ALLOWED_EXCEPTIONS region with
     an empty type list in that the runtime is prepared to terminate the
     program directly.  We only generate code for MUST_NOT_THROW regions
     along control paths that are already handling an exception within the
     current function.  */
  ERT_MUST_NOT_THROW
};


/* A landing pad for a given exception region.  Any transfer of control
   from the EH runtime to the function happens at a landing pad.  */

struct GTY(()) eh_landing_pad_d
{
  /* The linked list of all landing pads associated with the region.  */
  struct eh_landing_pad_d *next_lp;

  /* The region with which this landing pad is associated.  */
  struct eh_region_d *region;

  /* At the gimple level, the location to which control will be transferred
     for this landing pad.  There can be both EH and normal edges into the
     block containing the post-landing-pad label.  */
  tree post_landing_pad;

  /* At the rtl level, the location to which the runtime will transfer
     control.  This differs from the post-landing-pad in that the target's
     EXCEPTION_RECEIVER pattern will be expanded here, as well as other
     bookkeeping specific to exceptions.  There must not be normal edges
     into the block containing the landing-pad label.  */
  rtx_code_label *landing_pad;

  /* The index of this landing pad within fun->eh->lp_array.  */
  int index;
};

/* A catch handler associated with an ERT_TRY region.  */

struct GTY(()) eh_catch_d
{
  /* The double-linked list of all catch handlers for the region.  */
  struct eh_catch_d *next_catch;
  struct eh_catch_d *prev_catch;

  /* A TREE_LIST of runtime type objects that this catch handler
     will catch, or NULL if all exceptions are caught.  */
  tree type_list;

  /* A TREE_LIST of INTEGER_CSTs that correspond to the type_list entries,
     having been mapped by assign_filter_values.  These integers are to be
     compared against the __builtin_eh_filter value.  */
  tree filter_list;

  /* The code that should be executed if this catch handler matches the
     thrown exception.  This label is only maintained until
     pass_lower_eh_dispatch, at which point it is cleared.  */
  tree label;
};

/* Describes one exception region.  */

struct GTY(()) eh_region_d
{
  /* The immediately surrounding region.  */
  struct eh_region_d *outer;

  /* The list of immediately contained regions.  */
  struct eh_region_d *inner;
  struct eh_region_d *next_peer;

  /* The index of this region within fun->eh->region_array.  */
  int index;

  /* Each region does exactly one thing.  */
  enum eh_region_type type;

  /* Holds the action to perform based on the preceding type.  */
  union eh_region_u {
    struct eh_region_u_try {
      /* The double-linked list of all catch handlers for this region.  */
      struct eh_catch_d *first_catch;
      struct eh_catch_d *last_catch;
    } GTY ((tag ("ERT_TRY"))) eh_try;

    struct eh_region_u_allowed {
      /* A TREE_LIST of runtime type objects allowed to pass.  */
      tree type_list;
      /* The code that should be executed if the thrown exception does
	 not match the type list.  This label is only maintained until
	 pass_lower_eh_dispatch, at which point it is cleared.  */
      tree label;
      /* The integer that will be passed by the runtime to signal that
	 we should execute the code at LABEL.  This integer is assigned
	 by assign_filter_values and is to be compared against the
	 __builtin_eh_filter value.  */
      int filter;
    } GTY ((tag ("ERT_ALLOWED_EXCEPTIONS"))) allowed;

    struct eh_region_u_must_not_throw {
      /* A function decl to be invoked if this region is actually reachable
	 from within the function, rather than implementable from the runtime.
	 The normal way for this to happen is for there to be a CLEANUP region
	 contained within this MUST_NOT_THROW region.  Note that if the
	 runtime handles the MUST_NOT_THROW region, we have no control over
	 what termination function is called; it will be decided by the
	 personality function in effect for this CIE.  */
      tree failure_decl;
      /* The location assigned to the call of FAILURE_DECL, if expanded.  */
      location_t failure_loc;
    } GTY ((tag ("ERT_MUST_NOT_THROW"))) must_not_throw;
  } GTY ((desc ("%0.type"))) u;

  /* The list of landing pads associated with this region.  */
  struct eh_landing_pad_d *landing_pads;

  /* EXC_PTR and FILTER values copied from the runtime for this region.
     Each region gets its own psuedos so that if there are nested exceptions
     we do not overwrite the values of the first exception.  */
  rtx exc_ptr_reg, filter_reg;

  /* True if this region should use __cxa_end_cleanup instead
     of _Unwind_Resume.  */
  bool use_cxa_end_cleanup;
};

typedef struct eh_landing_pad_d *eh_landing_pad;
typedef struct eh_catch_d *eh_catch;
typedef struct eh_region_d *eh_region;




/* The exception status for each function.  */

struct GTY(()) eh_status
{
  /* The tree of all regions for this function.  */
  eh_region region_tree;

  /* The same information as an indexable array.  */
  vec<eh_region, va_gc> *region_array;

  /* The landing pads as an indexable array.  */
  vec<eh_landing_pad, va_gc> *lp_array;

  /* At the gimple level, a mapping from gimple statement to landing pad
     or must-not-throw region.  See record_stmt_eh_region.  */
  hash_map<gimple *, int> *GTY(()) throw_stmt_table;

  /* All of the runtime type data used by the function.  These objects
     are emitted to the lang-specific-data-area for the function.  */
  vec<tree, va_gc> *ttype_data;

  /* The table of all action chains.  These encode the eh_region tree in
     a compact form for use by the runtime, and is also emitted to the
     lang-specific-data-area.  Note that the ARM EABI uses a different
     format for the encoding than all other ports.  */
  union eh_status_u {
    vec<tree, va_gc> *GTY((tag ("1"))) arm_eabi;
    vec<uchar, va_gc> *GTY((tag ("0"))) other;
  } GTY ((desc ("targetm.arm_eabi_unwinder"))) ehspec_data;
};


/* Invokes CALLBACK for every exception handler label.  Only used by old
   loop hackery; should not be used by new code.  */
extern void for_each_eh_label (void (*) (rtx));

extern void init_eh_for_function (void);

extern void remove_eh_landing_pad (eh_landing_pad);
extern void remove_eh_handler (eh_region);
extern void remove_unreachable_eh_regions (sbitmap);

extern bool current_function_has_exception_handlers (void);
extern void output_function_exception_table (int);

extern rtx expand_builtin_eh_pointer (tree);
extern rtx expand_builtin_eh_filter (tree);
extern rtx expand_builtin_eh_copy_values (tree);
extern void expand_builtin_unwind_init (void);
extern rtx expand_builtin_eh_return_data_regno (tree);
extern rtx expand_builtin_extract_return_addr (tree);
extern void expand_builtin_init_dwarf_reg_sizes (tree);
extern rtx expand_builtin_frob_return_addr (tree);
extern rtx expand_builtin_dwarf_sp_column (void);
extern void expand_builtin_eh_return (tree, tree);
extern void expand_eh_return (void);
extern rtx expand_builtin_extend_pointer (tree);

typedef tree (*duplicate_eh_regions_map) (tree, void *);
extern hash_map<void *, void *> *duplicate_eh_regions
  (struct function *, eh_region, int, duplicate_eh_regions_map, void *);

extern void sjlj_emit_function_exit_after (rtx_insn *);
extern void update_sjlj_context (void);

extern eh_region gen_eh_region_cleanup (eh_region);
extern eh_region gen_eh_region_try (eh_region);
extern eh_region gen_eh_region_allowed (eh_region, tree);
extern eh_region gen_eh_region_must_not_throw (eh_region);

extern eh_catch gen_eh_region_catch (eh_region, tree);
extern eh_landing_pad gen_eh_landing_pad (eh_region);

extern eh_region get_eh_region_from_number_fn (struct function *, int);
extern eh_region get_eh_region_from_number (int);
extern eh_landing_pad get_eh_landing_pad_from_number_fn (struct function*,int);
extern eh_landing_pad get_eh_landing_pad_from_number (int);
extern eh_region get_eh_region_from_lp_number_fn (struct function *, int);
extern eh_region get_eh_region_from_lp_number (int);

extern eh_region eh_region_outermost (struct function *, eh_region, eh_region);

extern void make_reg_eh_region_note (rtx_insn *insn, int ecf_flags, int lp_nr);
extern void make_reg_eh_region_note_nothrow_nononlocal (rtx_insn *);

extern void verify_eh_tree (struct function *);
extern void dump_eh_tree (FILE *, struct function *);
void debug_eh_tree (struct function *);
extern void add_type_for_runtime (tree);
extern tree lookup_type_for_runtime (tree);
extern void assign_filter_values (void);

extern eh_region get_eh_region_from_rtx (const_rtx);
extern eh_landing_pad get_eh_landing_pad_from_rtx (const_rtx);

extern void finish_eh_generation (void);

struct GTY(()) throw_stmt_node {
  gimple *stmt;
  int lp_nr;
};

extern hash_map<gimple *, int> *get_eh_throw_stmt_table (struct function *);
extern void set_eh_throw_stmt_table (function *, hash_map<gimple *, int> *);

enum eh_personality_kind {
  eh_personality_none,
  eh_personality_any,
  eh_personality_lang
};

extern enum eh_personality_kind
function_needs_eh_personality (struct function *);

/* Pre-order iteration within the eh_region tree.  */

static inline eh_region
ehr_next (eh_region r, eh_region start)
{
  if (r->inner)
    r = r->inner;
  else if (r->next_peer && r != start)
    r = r->next_peer;
  else
    {
      do
	{
	  r = r->outer;
	  if (r == start)
	    return NULL;
	}
      while (r->next_peer == NULL);
      r = r->next_peer;
    }
  return r;
}

#define FOR_ALL_EH_REGION_AT(R, START) \
  for ((R) = (START); (R) != NULL; (R) = ehr_next (R, START))

#define FOR_ALL_EH_REGION_FN(R, FN) \
  for ((R) = (FN)->eh->region_tree; (R) != NULL; (R) = ehr_next (R, NULL))

#define FOR_ALL_EH_REGION(R) FOR_ALL_EH_REGION_FN (R, cfun)

#endif
