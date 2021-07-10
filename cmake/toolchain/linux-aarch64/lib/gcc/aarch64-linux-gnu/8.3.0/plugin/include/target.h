/* Data structure definitions for a generic GCC target.
   Copyright (C) 2001-2018 Free Software Foundation, Inc.

   This program is free software; you can redistribute it and/or modify it
   under the terms of the GNU General Public License as published by the
   Free Software Foundation; either version 3, or (at your option) any
   later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; see the file COPYING3.  If not see
   <http://www.gnu.org/licenses/>.

   In other words, you are welcome to use, share and improve this program.
   You are forbidden to forbid anyone else to use, share and improve
   what you give them.   Help stamp out software-hoarding!  */


/* This file contains a data structure that describes a GCC target.
   At present it is incomplete, but in future it should grow to
   contain most or all target machine and target O/S specific
   information.

   This structure has its initializer declared in target-def.h in the
   form of large macro TARGET_INITIALIZER that expands to many smaller
   macros.

   The smaller macros each initialize one component of the structure,
   and each has a default.  Each target should have a file that
   includes target.h and target-def.h, and overrides any inappropriate
   defaults by undefining the relevant macro and defining a suitable
   replacement.  That file should then contain the definition of
   "targetm" like so:

   struct gcc_target targetm = TARGET_INITIALIZER;

   Doing things this way allows us to bring together everything that
   defines a GCC target.  By supplying a default that is appropriate
   to most targets, we can easily add new items without needing to
   edit dozens of target configuration files.  It should also allow us
   to gradually reduce the amount of conditional compilation that is
   scattered throughout GCC.  */

#ifndef GCC_TARGET_H
#define GCC_TARGET_H

#include "insn-codes.h"
#include "tm.h"
#include "hard-reg-set.h"

#if CHECKING_P

struct cumulative_args_t { void *magic; void *p; };

#else /* !CHECKING_P */

/* When using a GCC build compiler, we could use
   __attribute__((transparent_union)) to get cumulative_args_t function
   arguments passed like scalars where the ABI would mandate a less
   efficient way of argument passing otherwise.  However, that would come
   at the cost of less type-safe !CHECKING_P compilation.  */

union cumulative_args_t { void *p; };

#endif /* !CHECKING_P */

/* Types used by the record_gcc_switches() target function.  */
enum print_switch_type
{
  SWITCH_TYPE_PASSED,		/* A switch passed on the command line.  */
  SWITCH_TYPE_ENABLED,		/* An option that is currently enabled.  */
  SWITCH_TYPE_DESCRIPTIVE,	/* Descriptive text, not a switch or option.  */
  SWITCH_TYPE_LINE_START,	/* Please emit any necessary text at the start of a line.  */
  SWITCH_TYPE_LINE_END		/* Please emit a line terminator.  */
};

/* Types of memory operation understood by the "by_pieces" infrastructure.
   Used by the TARGET_USE_BY_PIECES_INFRASTRUCTURE_P target hook and
   internally by the functions in expr.c.  */

enum by_pieces_operation
{
  CLEAR_BY_PIECES,
  MOVE_BY_PIECES,
  SET_BY_PIECES,
  STORE_BY_PIECES,
  COMPARE_BY_PIECES
};

extern unsigned HOST_WIDE_INT by_pieces_ninsns (unsigned HOST_WIDE_INT,
						unsigned int,
						unsigned int,
						by_pieces_operation);

typedef int (* print_switch_fn_type) (print_switch_type, const char *);

/* An example implementation for ELF targets.  Defined in varasm.c  */
extern int elf_record_gcc_switches (print_switch_type type, const char *);

/* Some places still assume that all pointer or address modes are the
   standard Pmode and ptr_mode.  These optimizations become invalid if
   the target actually supports multiple different modes.  For now,
   we disable such optimizations on such targets, using this function.  */
extern bool target_default_pointer_address_modes_p (void);

/* For hooks which use the MOVE_RATIO macro, this gives the legacy default
   behavior.  */
extern unsigned int get_move_ratio (bool);

struct stdarg_info;
struct spec_info_def;
struct hard_reg_set_container;
struct cgraph_node;
struct cgraph_simd_clone;

/* The struct used by the secondary_reload target hook.  */
struct secondary_reload_info
{
  /* icode is actually an enum insn_code, but we don't want to force every
     file that includes target.h to include optabs.h .  */
  int icode;
  int extra_cost; /* Cost for using (a) scratch register(s) to be taken
		     into account by copy_cost.  */
  /* The next two members are for the use of the backward
     compatibility hook.  */
  struct secondary_reload_info *prev_sri;
  int t_icode; /* Actually an enum insn_code - see above.  */
};

/* This is defined in sched-int.h .  */
struct _dep;

/* This is defined in ddg.h .  */
struct ddg;

/* This is defined in cfgloop.h .  */
struct loop;

/* This is defined in ifcvt.h.  */
struct noce_if_info;

/* This is defined in tree-ssa-alias.h.  */
struct ao_ref;

/* This is defined in tree-vectorizer.h.  */
struct _stmt_vec_info;

/* These are defined in tree-vect-stmts.c.  */
extern tree stmt_vectype (struct _stmt_vec_info *);
extern bool stmt_in_inner_loop_p (struct _stmt_vec_info *);

/* Assembler instructions for creating various kinds of integer object.  */

struct asm_int_op
{
  const char *hi;
  const char *si;
  const char *di;
  const char *ti;
};

/* Types of costs for vectorizer cost model.  */
enum vect_cost_for_stmt
{
  scalar_stmt,
  scalar_load,
  scalar_store,
  vector_stmt,
  vector_load,
  vector_gather_load,
  unaligned_load,
  unaligned_store,
  vector_store,
  vector_scatter_store,
  vec_to_scalar,
  scalar_to_vec,
  cond_branch_not_taken,
  cond_branch_taken,
  vec_perm,
  vec_promote_demote,
  vec_construct
};

/* Separate locations for which the vectorizer cost model should
   track costs.  */
enum vect_cost_model_location {
  vect_prologue = 0,
  vect_body = 1,
  vect_epilogue = 2
};

class vec_perm_indices;

/* The type to use for lists of vector sizes.  */
typedef vec<poly_uint64> vector_sizes;

/* Same, but can be used to construct local lists that are
   automatically freed.  */
typedef auto_vec<poly_uint64, 8> auto_vector_sizes;

/* The target structure.  This holds all the backend hooks.  */
#define DEFHOOKPOD(NAME, DOC, TYPE, INIT) TYPE NAME;
#define DEFHOOK(NAME, DOC, TYPE, PARAMS, INIT) TYPE (* NAME) PARAMS;
#define DEFHOOK_UNDOC DEFHOOK
#define HOOKSTRUCT(FRAGMENT) FRAGMENT

#include "target.def"

extern struct gcc_target targetm;

/* Return an estimate of the runtime value of X, for use in things
   like cost calculations or profiling frequencies.  Note that this
   function should never be used in situations where the actual
   runtime value is needed for correctness, since the function only
   provides a rough guess.  */

static inline HOST_WIDE_INT
estimated_poly_value (poly_int64 x)
{
  if (NUM_POLY_INT_COEFFS == 1)
    return x.coeffs[0];
  else
    return targetm.estimated_poly_value (x);
}

#ifdef GCC_TM_H

#ifndef CUMULATIVE_ARGS_MAGIC
#define CUMULATIVE_ARGS_MAGIC ((void *) &targetm.calls)
#endif

static inline CUMULATIVE_ARGS *
get_cumulative_args (cumulative_args_t arg)
{
#if CHECKING_P
  gcc_assert (arg.magic == CUMULATIVE_ARGS_MAGIC);
#endif /* CHECKING_P */
  return (CUMULATIVE_ARGS *) arg.p;
}

static inline cumulative_args_t
pack_cumulative_args (CUMULATIVE_ARGS *arg)
{
  cumulative_args_t ret;

#if CHECKING_P
  ret.magic = CUMULATIVE_ARGS_MAGIC;
#endif /* CHECKING_P */
  ret.p = (void *) arg;
  return ret;
}
#endif /* GCC_TM_H */

#endif /* GCC_TARGET_H */
