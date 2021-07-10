/* Gimple IR definitions.

   Copyright (C) 2007-2018 Free Software Foundation, Inc.
   Contributed by Aldy Hernandez <aldyh@redhat.com>

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

#ifndef GCC_GIMPLE_H
#define GCC_GIMPLE_H

#include "tree-ssa-alias.h"
#include "gimple-expr.h"

typedef gimple *gimple_seq_node;

enum gimple_code {
#define DEFGSCODE(SYM, STRING, STRUCT)	SYM,
#include "gimple.def"
#undef DEFGSCODE
    LAST_AND_UNUSED_GIMPLE_CODE
};

extern const char *const gimple_code_name[];
extern const unsigned char gimple_rhs_class_table[];

/* Strip the outermost pointer, from tr1/type_traits.  */
template<typename T> struct remove_pointer { typedef T type; };
template<typename T> struct remove_pointer<T *> { typedef T type; };

/* Error out if a gimple tuple is addressed incorrectly.  */
#if defined ENABLE_GIMPLE_CHECKING
#define gcc_gimple_checking_assert(EXPR) gcc_assert (EXPR)
extern void gimple_check_failed (const gimple *, const char *, int,        \
                                 const char *, enum gimple_code,           \
				 enum tree_code) ATTRIBUTE_NORETURN 	   \
						 ATTRIBUTE_COLD;

#define GIMPLE_CHECK(GS, CODE)						\
  do {									\
    const gimple *__gs = (GS);						\
    if (gimple_code (__gs) != (CODE))					\
      gimple_check_failed (__gs, __FILE__, __LINE__, __FUNCTION__,	\
	  		   (CODE), ERROR_MARK);				\
  } while (0)
template <typename T>
static inline T
GIMPLE_CHECK2(const gimple *gs,
#if __GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 8)
	      const char *file = __builtin_FILE (),
	      int line = __builtin_LINE (),
	      const char *fun = __builtin_FUNCTION ())
#else
	      const char *file = __FILE__,
	      int line = __LINE__,
	      const char *fun = NULL)
#endif
{
  T ret = dyn_cast <T> (gs);
  if (!ret)
    gimple_check_failed (gs, file, line, fun,
			 remove_pointer<T>::type::code_, ERROR_MARK);
  return ret;
}
template <typename T>
static inline T
GIMPLE_CHECK2(gimple *gs,
#if __GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 8)
	      const char *file = __builtin_FILE (),
	      int line = __builtin_LINE (),
	      const char *fun = __builtin_FUNCTION ())
#else
	      const char *file = __FILE__,
	      int line = __LINE__,
	      const char *fun = NULL)
#endif
{
  T ret = dyn_cast <T> (gs);
  if (!ret)
    gimple_check_failed (gs, file, line, fun,
			 remove_pointer<T>::type::code_, ERROR_MARK);
  return ret;
}
#else  /* not ENABLE_GIMPLE_CHECKING  */
#define gcc_gimple_checking_assert(EXPR) ((void)(0 && (EXPR)))
#define GIMPLE_CHECK(GS, CODE)			(void)0
template <typename T>
static inline T
GIMPLE_CHECK2(gimple *gs)
{
  return as_a <T> (gs);
}
template <typename T>
static inline T
GIMPLE_CHECK2(const gimple *gs)
{
  return as_a <T> (gs);
}
#endif

/* Class of GIMPLE expressions suitable for the RHS of assignments.  See
   get_gimple_rhs_class.  */
enum gimple_rhs_class
{
  GIMPLE_INVALID_RHS,	/* The expression cannot be used on the RHS.  */
  GIMPLE_TERNARY_RHS,	/* The expression is a ternary operation.  */
  GIMPLE_BINARY_RHS,	/* The expression is a binary operation.  */
  GIMPLE_UNARY_RHS,	/* The expression is a unary operation.  */
  GIMPLE_SINGLE_RHS	/* The expression is a single object (an SSA
			   name, a _DECL, a _REF, etc.  */
};

/* Specific flags for individual GIMPLE statements.  These flags are
   always stored in gimple.subcode and they may only be
   defined for statement codes that do not use subcodes.

   Values for the masks can overlap as long as the overlapping values
   are never used in the same statement class.

   The maximum mask value that can be defined is 1 << 15 (i.e., each
   statement code can hold up to 16 bitflags).

   Keep this list sorted.  */
enum gf_mask {
    GF_ASM_INPUT		= 1 << 0,
    GF_ASM_VOLATILE		= 1 << 1,
    GF_ASM_INLINE		= 1 << 2,
    GF_CALL_FROM_THUNK		= 1 << 0,
    GF_CALL_RETURN_SLOT_OPT	= 1 << 1,
    GF_CALL_TAILCALL		= 1 << 2,
    GF_CALL_VA_ARG_PACK		= 1 << 3,
    GF_CALL_NOTHROW		= 1 << 4,
    GF_CALL_ALLOCA_FOR_VAR	= 1 << 5,
    GF_CALL_INTERNAL		= 1 << 6,
    GF_CALL_CTRL_ALTERING       = 1 << 7,
    GF_CALL_WITH_BOUNDS 	= 1 << 8,
    GF_CALL_MUST_TAIL_CALL	= 1 << 9,
    GF_CALL_BY_DESCRIPTOR	= 1 << 10,
    GF_CALL_NOCF_CHECK		= 1 << 11,
    GF_OMP_PARALLEL_COMBINED	= 1 << 0,
    GF_OMP_PARALLEL_GRID_PHONY = 1 << 1,
    GF_OMP_TASK_TASKLOOP	= 1 << 0,
    GF_OMP_FOR_KIND_MASK	= (1 << 4) - 1,
    GF_OMP_FOR_KIND_FOR		= 0,
    GF_OMP_FOR_KIND_DISTRIBUTE	= 1,
    GF_OMP_FOR_KIND_TASKLOOP	= 2,
    GF_OMP_FOR_KIND_OACC_LOOP	= 4,
    GF_OMP_FOR_KIND_GRID_LOOP = 5,
    /* Flag for SIMD variants of OMP_FOR kinds.  */
    GF_OMP_FOR_SIMD		= 1 << 3,
    GF_OMP_FOR_KIND_SIMD	= GF_OMP_FOR_SIMD | 0,
    GF_OMP_FOR_COMBINED		= 1 << 4,
    GF_OMP_FOR_COMBINED_INTO	= 1 << 5,
    /* The following flag must not be used on GF_OMP_FOR_KIND_GRID_LOOP loop
       statements.  */
    GF_OMP_FOR_GRID_PHONY	= 1 << 6,
    /* The following two flags should only be set on GF_OMP_FOR_KIND_GRID_LOOP
       loop statements.  */
    GF_OMP_FOR_GRID_INTRA_GROUP	= 1 << 6,
    GF_OMP_FOR_GRID_GROUP_ITER  = 1 << 7,
    GF_OMP_TARGET_KIND_MASK	= (1 << 4) - 1,
    GF_OMP_TARGET_KIND_REGION	= 0,
    GF_OMP_TARGET_KIND_DATA	= 1,
    GF_OMP_TARGET_KIND_UPDATE	= 2,
    GF_OMP_TARGET_KIND_ENTER_DATA = 3,
    GF_OMP_TARGET_KIND_EXIT_DATA = 4,
    GF_OMP_TARGET_KIND_OACC_PARALLEL = 5,
    GF_OMP_TARGET_KIND_OACC_KERNELS = 6,
    GF_OMP_TARGET_KIND_OACC_DATA = 7,
    GF_OMP_TARGET_KIND_OACC_UPDATE = 8,
    GF_OMP_TARGET_KIND_OACC_ENTER_EXIT_DATA = 9,
    GF_OMP_TARGET_KIND_OACC_DECLARE = 10,
    GF_OMP_TARGET_KIND_OACC_HOST_DATA = 11,
    GF_OMP_TEAMS_GRID_PHONY	= 1 << 0,

    /* True on an GIMPLE_OMP_RETURN statement if the return does not require
       a thread synchronization via some sort of barrier.  The exact barrier
       that would otherwise be emitted is dependent on the OMP statement with
       which this return is associated.  */
    GF_OMP_RETURN_NOWAIT	= 1 << 0,

    GF_OMP_SECTION_LAST		= 1 << 0,
    GF_OMP_ATOMIC_NEED_VALUE	= 1 << 0,
    GF_OMP_ATOMIC_SEQ_CST	= 1 << 1,
    GF_PREDICT_TAKEN		= 1 << 15
};

/* This subcode tells apart different kinds of stmts that are not used
   for codegen, but rather to retain debug information.  */
enum gimple_debug_subcode {
  GIMPLE_DEBUG_BIND = 0,
  GIMPLE_DEBUG_SOURCE_BIND = 1,
  GIMPLE_DEBUG_BEGIN_STMT = 2,
  GIMPLE_DEBUG_INLINE_ENTRY = 3
};

/* Masks for selecting a pass local flag (PLF) to work on.  These
   masks are used by gimple_set_plf and gimple_plf.  */
enum plf_mask {
    GF_PLF_1	= 1 << 0,
    GF_PLF_2	= 1 << 1
};

/* Data structure definitions for GIMPLE tuples.  NOTE: word markers
   are for 64 bit hosts.  */

struct GTY((desc ("gimple_statement_structure (&%h)"), tag ("GSS_BASE"),
	    chain_next ("%h.next"), variable_size))
  gimple
{
  /* [ WORD 1 ]
     Main identifying code for a tuple.  */
  ENUM_BITFIELD(gimple_code) code : 8;

  /* Nonzero if a warning should not be emitted on this tuple.  */
  unsigned int no_warning	: 1;

  /* Nonzero if this tuple has been visited.  Passes are responsible
     for clearing this bit before using it.  */
  unsigned int visited		: 1;

  /* Nonzero if this tuple represents a non-temporal move.  */
  unsigned int nontemporal_move	: 1;

  /* Pass local flags.  These flags are free for any pass to use as
     they see fit.  Passes should not assume that these flags contain
     any useful value when the pass starts.  Any initial state that
     the pass requires should be set on entry to the pass.  See
     gimple_set_plf and gimple_plf for usage.  */
  unsigned int plf		: 2;

  /* Nonzero if this statement has been modified and needs to have its
     operands rescanned.  */
  unsigned modified 		: 1;

  /* Nonzero if this statement contains volatile operands.  */
  unsigned has_volatile_ops 	: 1;

  /* Padding to get subcode to 16 bit alignment.  */
  unsigned pad			: 1;

  /* The SUBCODE field can be used for tuple-specific flags for tuples
     that do not require subcodes.  Note that SUBCODE should be at
     least as wide as tree codes, as several tuples store tree codes
     in there.  */
  unsigned int subcode		: 16;

  /* UID of this statement.  This is used by passes that want to
     assign IDs to statements.  It must be assigned and used by each
     pass.  By default it should be assumed to contain garbage.  */
  unsigned uid;

  /* [ WORD 2 ]
     Locus information for debug info.  */
  location_t location;

  /* Number of operands in this tuple.  */
  unsigned num_ops;

  /* [ WORD 3 ]
     Basic block holding this statement.  */
  basic_block bb;

  /* [ WORD 4-5 ]
     Linked lists of gimple statements.  The next pointers form
     a NULL terminated list, the prev pointers are a cyclic list.
     A gimple statement is hence also a double-ended list of
     statements, with the pointer itself being the first element,
     and the prev pointer being the last.  */
  gimple *next;
  gimple *GTY((skip)) prev;
};


/* Base structure for tuples with operands.  */

/* This gimple subclass has no tag value.  */
struct GTY(())
  gimple_statement_with_ops_base : public gimple
{
  /* [ WORD 1-6 ] : base class */

  /* [ WORD 7 ]
     SSA operand vectors.  NOTE: It should be possible to
     amalgamate these vectors with the operand vector OP.  However,
     the SSA operand vectors are organized differently and contain
     more information (like immediate use chaining).  */
  struct use_optype_d GTY((skip (""))) *use_ops;
};


/* Statements that take register operands.  */

struct GTY((tag("GSS_WITH_OPS")))
  gimple_statement_with_ops : public gimple_statement_with_ops_base
{
  /* [ WORD 1-7 ] : base class */

  /* [ WORD 8 ]
     Operand vector.  NOTE!  This must always be the last field
     of this structure.  In particular, this means that this
     structure cannot be embedded inside another one.  */
  tree GTY((length ("%h.num_ops"))) op[1];
};


/* Base for statements that take both memory and register operands.  */

struct GTY((tag("GSS_WITH_MEM_OPS_BASE")))
  gimple_statement_with_memory_ops_base : public gimple_statement_with_ops_base
{
  /* [ WORD 1-7 ] : base class */

  /* [ WORD 8-9 ]
     Virtual operands for this statement.  The GC will pick them
     up via the ssa_names array.  */
  tree GTY((skip (""))) vdef;
  tree GTY((skip (""))) vuse;
};


/* Statements that take both memory and register operands.  */

struct GTY((tag("GSS_WITH_MEM_OPS")))
  gimple_statement_with_memory_ops :
    public gimple_statement_with_memory_ops_base
{
  /* [ WORD 1-9 ] : base class */

  /* [ WORD 10 ]
     Operand vector.  NOTE!  This must always be the last field
     of this structure.  In particular, this means that this
     structure cannot be embedded inside another one.  */
  tree GTY((length ("%h.num_ops"))) op[1];
};


/* Call statements that take both memory and register operands.  */

struct GTY((tag("GSS_CALL")))
  gcall : public gimple_statement_with_memory_ops_base
{
  /* [ WORD 1-9 ] : base class */

  /* [ WORD 10-13 ]  */
  struct pt_solution call_used;
  struct pt_solution call_clobbered;

  /* [ WORD 14 ]  */
  union GTY ((desc ("%1.subcode & GF_CALL_INTERNAL"))) {
    tree GTY ((tag ("0"))) fntype;
    enum internal_fn GTY ((tag ("GF_CALL_INTERNAL"))) internal_fn;
  } u;

  /* [ WORD 15 ]
     Operand vector.  NOTE!  This must always be the last field
     of this structure.  In particular, this means that this
     structure cannot be embedded inside another one.  */
  tree GTY((length ("%h.num_ops"))) op[1];

  static const enum gimple_code code_ = GIMPLE_CALL;
};


/* OMP statements.  */

struct GTY((tag("GSS_OMP")))
  gimple_statement_omp : public gimple
{
  /* [ WORD 1-6 ] : base class */

  /* [ WORD 7 ]  */
  gimple_seq body;
};


/* GIMPLE_BIND */

struct GTY((tag("GSS_BIND")))
  gbind : public gimple
{
  /* [ WORD 1-6 ] : base class */

  /* [ WORD 7 ]
     Variables declared in this scope.  */
  tree vars;

  /* [ WORD 8 ]
     This is different than the BLOCK field in gimple,
     which is analogous to TREE_BLOCK (i.e., the lexical block holding
     this statement).  This field is the equivalent of BIND_EXPR_BLOCK
     in tree land (i.e., the lexical scope defined by this bind).  See
     gimple-low.c.  */
  tree block;

  /* [ WORD 9 ]  */
  gimple_seq body;
};


/* GIMPLE_CATCH */

struct GTY((tag("GSS_CATCH")))
  gcatch : public gimple
{
  /* [ WORD 1-6 ] : base class */

  /* [ WORD 7 ]  */
  tree types;

  /* [ WORD 8 ]  */
  gimple_seq handler;
};


/* GIMPLE_EH_FILTER */

struct GTY((tag("GSS_EH_FILTER")))
  geh_filter : public gimple
{
  /* [ WORD 1-6 ] : base class */

  /* [ WORD 7 ]
     Filter types.  */
  tree types;

  /* [ WORD 8 ]
     Failure actions.  */
  gimple_seq failure;
};

/* GIMPLE_EH_ELSE */

struct GTY((tag("GSS_EH_ELSE")))
  geh_else : public gimple
{
  /* [ WORD 1-6 ] : base class */

  /* [ WORD 7,8 ] */
  gimple_seq n_body, e_body;
};

/* GIMPLE_EH_MUST_NOT_THROW */

struct GTY((tag("GSS_EH_MNT")))
  geh_mnt : public gimple
{
  /* [ WORD 1-6 ] : base class */

  /* [ WORD 7 ] Abort function decl.  */
  tree fndecl;
};

/* GIMPLE_PHI */

struct GTY((tag("GSS_PHI")))
  gphi : public gimple
{
  /* [ WORD 1-6 ] : base class */

  /* [ WORD 7 ]  */
  unsigned capacity;
  unsigned nargs;

  /* [ WORD 8 ]  */
  tree result;

  /* [ WORD 9 ]  */
  struct phi_arg_d GTY ((length ("%h.nargs"))) args[1];
};


/* GIMPLE_RESX, GIMPLE_EH_DISPATCH */

struct GTY((tag("GSS_EH_CTRL")))
  gimple_statement_eh_ctrl : public gimple
{
  /* [ WORD 1-6 ] : base class */

  /* [ WORD 7 ]
     Exception region number.  */
  int region;
};

struct GTY((tag("GSS_EH_CTRL")))
  gresx : public gimple_statement_eh_ctrl
{
  /* No extra fields; adds invariant:
       stmt->code == GIMPLE_RESX.  */
};

struct GTY((tag("GSS_EH_CTRL")))
  geh_dispatch : public gimple_statement_eh_ctrl
{
  /* No extra fields; adds invariant:
       stmt->code == GIMPLE_EH_DISPATH.  */
};


/* GIMPLE_TRY */

struct GTY((tag("GSS_TRY")))
  gtry : public gimple
{
  /* [ WORD 1-6 ] : base class */

  /* [ WORD 7 ]
     Expression to evaluate.  */
  gimple_seq eval;

  /* [ WORD 8 ]
     Cleanup expression.  */
  gimple_seq cleanup;
};

/* Kind of GIMPLE_TRY statements.  */
enum gimple_try_flags
{
  /* A try/catch.  */
  GIMPLE_TRY_CATCH = 1 << 0,

  /* A try/finally.  */
  GIMPLE_TRY_FINALLY = 1 << 1,
  GIMPLE_TRY_KIND = GIMPLE_TRY_CATCH | GIMPLE_TRY_FINALLY,

  /* Analogous to TRY_CATCH_IS_CLEANUP.  */
  GIMPLE_TRY_CATCH_IS_CLEANUP = 1 << 2
};

/* GIMPLE_WITH_CLEANUP_EXPR */

struct GTY((tag("GSS_WCE")))
  gimple_statement_wce : public gimple
{
  /* [ WORD 1-6 ] : base class */

  /* Subcode: CLEANUP_EH_ONLY.  True if the cleanup should only be
	      executed if an exception is thrown, not on normal exit of its
	      scope.  This flag is analogous to the CLEANUP_EH_ONLY flag
	      in TARGET_EXPRs.  */

  /* [ WORD 7 ]
     Cleanup expression.  */
  gimple_seq cleanup;
};


/* GIMPLE_ASM  */

struct GTY((tag("GSS_ASM")))
  gasm : public gimple_statement_with_memory_ops_base
{
  /* [ WORD 1-9 ] : base class */

  /* [ WORD 10 ]
     __asm__ statement.  */
  const char *string;

  /* [ WORD 11 ]
       Number of inputs, outputs, clobbers, labels.  */
  unsigned char ni;
  unsigned char no;
  unsigned char nc;
  unsigned char nl;

  /* [ WORD 12 ]
     Operand vector.  NOTE!  This must always be the last field
     of this structure.  In particular, this means that this
     structure cannot be embedded inside another one.  */
  tree GTY((length ("%h.num_ops"))) op[1];
};

/* GIMPLE_OMP_CRITICAL */

struct GTY((tag("GSS_OMP_CRITICAL")))
  gomp_critical : public gimple_statement_omp
{
  /* [ WORD 1-7 ] : base class */

  /* [ WORD 8 ]  */
  tree clauses;

  /* [ WORD 9 ]
     Critical section name.  */
  tree name;
};


struct GTY(()) gimple_omp_for_iter {
  /* Condition code.  */
  enum tree_code cond;

  /* Index variable.  */
  tree index;

  /* Initial value.  */
  tree initial;

  /* Final value.  */
  tree final;

  /* Increment.  */
  tree incr;
};

/* GIMPLE_OMP_FOR */

struct GTY((tag("GSS_OMP_FOR")))
  gomp_for : public gimple_statement_omp
{
  /* [ WORD 1-7 ] : base class */

  /* [ WORD 8 ]  */
  tree clauses;

  /* [ WORD 9 ]
     Number of elements in iter array.  */
  size_t collapse;

  /* [ WORD 10 ]  */
  struct gimple_omp_for_iter * GTY((length ("%h.collapse"))) iter;

  /* [ WORD 11 ]
     Pre-body evaluated before the loop body begins.  */
  gimple_seq pre_body;
};


/* GIMPLE_OMP_PARALLEL, GIMPLE_OMP_TARGET, GIMPLE_OMP_TASK */

struct GTY((tag("GSS_OMP_PARALLEL_LAYOUT")))
  gimple_statement_omp_parallel_layout : public gimple_statement_omp
{
  /* [ WORD 1-7 ] : base class */

  /* [ WORD 8 ]
     Clauses.  */
  tree clauses;

  /* [ WORD 9 ]
     Child function holding the body of the parallel region.  */
  tree child_fn;

  /* [ WORD 10 ]
     Shared data argument.  */
  tree data_arg;
};

/* GIMPLE_OMP_PARALLEL or GIMPLE_TASK */
struct GTY((tag("GSS_OMP_PARALLEL_LAYOUT")))
  gimple_statement_omp_taskreg : public gimple_statement_omp_parallel_layout
{
    /* No extra fields; adds invariant:
         stmt->code == GIMPLE_OMP_PARALLEL
	 || stmt->code == GIMPLE_OMP_TASK.  */
};

/* GIMPLE_OMP_PARALLEL */
struct GTY((tag("GSS_OMP_PARALLEL_LAYOUT")))
  gomp_parallel : public gimple_statement_omp_taskreg
{
    /* No extra fields; adds invariant:
         stmt->code == GIMPLE_OMP_PARALLEL.  */
};

/* GIMPLE_OMP_TARGET */
struct GTY((tag("GSS_OMP_PARALLEL_LAYOUT")))
  gomp_target : public gimple_statement_omp_parallel_layout
{
    /* No extra fields; adds invariant:
         stmt->code == GIMPLE_OMP_TARGET.  */
};

/* GIMPLE_OMP_TASK */

struct GTY((tag("GSS_OMP_TASK")))
  gomp_task : public gimple_statement_omp_taskreg
{
  /* [ WORD 1-10 ] : base class */

  /* [ WORD 11 ]
     Child function holding firstprivate initialization if needed.  */
  tree copy_fn;

  /* [ WORD 12-13 ]
     Size and alignment in bytes of the argument data block.  */
  tree arg_size;
  tree arg_align;
};


/* GIMPLE_OMP_SECTION */
/* Uses struct gimple_statement_omp.  */


/* GIMPLE_OMP_SECTIONS */

struct GTY((tag("GSS_OMP_SECTIONS")))
  gomp_sections : public gimple_statement_omp
{
  /* [ WORD 1-7 ] : base class */

  /* [ WORD 8 ]  */
  tree clauses;

  /* [ WORD 9 ]
     The control variable used for deciding which of the sections to
     execute.  */
  tree control;
};

/* GIMPLE_OMP_CONTINUE.

   Note: This does not inherit from gimple_statement_omp, because we
         do not need the body field.  */

struct GTY((tag("GSS_OMP_CONTINUE")))
  gomp_continue : public gimple
{
  /* [ WORD 1-6 ] : base class */

  /* [ WORD 7 ]  */
  tree control_def;

  /* [ WORD 8 ]  */
  tree control_use;
};

/* GIMPLE_OMP_SINGLE, GIMPLE_OMP_TEAMS, GIMPLE_OMP_ORDERED */

struct GTY((tag("GSS_OMP_SINGLE_LAYOUT")))
  gimple_statement_omp_single_layout : public gimple_statement_omp
{
  /* [ WORD 1-7 ] : base class */

  /* [ WORD 8 ]  */
  tree clauses;
};

struct GTY((tag("GSS_OMP_SINGLE_LAYOUT")))
  gomp_single : public gimple_statement_omp_single_layout
{
    /* No extra fields; adds invariant:
         stmt->code == GIMPLE_OMP_SINGLE.  */
};

struct GTY((tag("GSS_OMP_SINGLE_LAYOUT")))
  gomp_teams : public gimple_statement_omp_single_layout
{
    /* No extra fields; adds invariant:
         stmt->code == GIMPLE_OMP_TEAMS.  */
};

struct GTY((tag("GSS_OMP_SINGLE_LAYOUT")))
  gomp_ordered : public gimple_statement_omp_single_layout
{
    /* No extra fields; adds invariant:
	 stmt->code == GIMPLE_OMP_ORDERED.  */
};


/* GIMPLE_OMP_ATOMIC_LOAD.
   Note: This is based on gimple, not g_s_omp, because g_s_omp
   contains a sequence, which we don't need here.  */

struct GTY((tag("GSS_OMP_ATOMIC_LOAD")))
  gomp_atomic_load : public gimple
{
  /* [ WORD 1-6 ] : base class */

  /* [ WORD 7-8 ]  */
  tree rhs, lhs;
};

/* GIMPLE_OMP_ATOMIC_STORE.
   See note on GIMPLE_OMP_ATOMIC_LOAD.  */

struct GTY((tag("GSS_OMP_ATOMIC_STORE_LAYOUT")))
  gimple_statement_omp_atomic_store_layout : public gimple
{
  /* [ WORD 1-6 ] : base class */

  /* [ WORD 7 ]  */
  tree val;
};

struct GTY((tag("GSS_OMP_ATOMIC_STORE_LAYOUT")))
  gomp_atomic_store :
    public gimple_statement_omp_atomic_store_layout
{
    /* No extra fields; adds invariant:
         stmt->code == GIMPLE_OMP_ATOMIC_STORE.  */
};

struct GTY((tag("GSS_OMP_ATOMIC_STORE_LAYOUT")))
  gimple_statement_omp_return :
    public gimple_statement_omp_atomic_store_layout
{
    /* No extra fields; adds invariant:
         stmt->code == GIMPLE_OMP_RETURN.  */
};

/* GIMPLE_TRANSACTION.  */

/* Bits to be stored in the GIMPLE_TRANSACTION subcode.  */

/* The __transaction_atomic was declared [[outer]] or it is
   __transaction_relaxed.  */
#define GTMA_IS_OUTER			(1u << 0)
#define GTMA_IS_RELAXED			(1u << 1)
#define GTMA_DECLARATION_MASK		(GTMA_IS_OUTER | GTMA_IS_RELAXED)

/* The transaction is seen to not have an abort.  */
#define GTMA_HAVE_ABORT			(1u << 2)
/* The transaction is seen to have loads or stores.  */
#define GTMA_HAVE_LOAD			(1u << 3)
#define GTMA_HAVE_STORE			(1u << 4)
/* The transaction MAY enter serial irrevocable mode in its dynamic scope.  */
#define GTMA_MAY_ENTER_IRREVOCABLE	(1u << 5)
/* The transaction WILL enter serial irrevocable mode.
   An irrevocable block post-dominates the entire transaction, such
   that all invocations of the transaction will go serial-irrevocable.
   In such case, we don't bother instrumenting the transaction, and
   tell the runtime that it should begin the transaction in
   serial-irrevocable mode.  */
#define GTMA_DOES_GO_IRREVOCABLE	(1u << 6)
/* The transaction contains no instrumentation code whatsover, most
   likely because it is guaranteed to go irrevocable upon entry.  */
#define GTMA_HAS_NO_INSTRUMENTATION	(1u << 7)

struct GTY((tag("GSS_TRANSACTION")))
  gtransaction : public gimple_statement_with_memory_ops_base
{
  /* [ WORD 1-9 ] : base class */

  /* [ WORD 10 ] */
  gimple_seq body;

  /* [ WORD 11-13 ] */
  tree label_norm;
  tree label_uninst;
  tree label_over;
};

#define DEFGSSTRUCT(SYM, STRUCT, HAS_TREE_OP)	SYM,
enum gimple_statement_structure_enum {
#include "gsstruct.def"
    LAST_GSS_ENUM
};
#undef DEFGSSTRUCT

/* A statement with the invariant that
      stmt->code == GIMPLE_COND
   i.e. a conditional jump statement.  */

struct GTY((tag("GSS_WITH_OPS")))
  gcond : public gimple_statement_with_ops
{
  /* no additional fields; this uses the layout for GSS_WITH_OPS. */
  static const enum gimple_code code_ = GIMPLE_COND;
};

/* A statement with the invariant that
      stmt->code == GIMPLE_DEBUG
   i.e. a debug statement.  */

struct GTY((tag("GSS_WITH_OPS")))
  gdebug : public gimple_statement_with_ops
{
  /* no additional fields; this uses the layout for GSS_WITH_OPS. */
};

/* A statement with the invariant that
      stmt->code == GIMPLE_GOTO
   i.e. a goto statement.  */

struct GTY((tag("GSS_WITH_OPS")))
  ggoto : public gimple_statement_with_ops
{
  /* no additional fields; this uses the layout for GSS_WITH_OPS. */
};

/* A statement with the invariant that
      stmt->code == GIMPLE_LABEL
   i.e. a label statement.  */

struct GTY((tag("GSS_WITH_OPS")))
  glabel : public gimple_statement_with_ops
{
  /* no additional fields; this uses the layout for GSS_WITH_OPS. */
};

/* A statement with the invariant that
      stmt->code == GIMPLE_SWITCH
   i.e. a switch statement.  */

struct GTY((tag("GSS_WITH_OPS")))
  gswitch : public gimple_statement_with_ops
{
  /* no additional fields; this uses the layout for GSS_WITH_OPS. */
};

/* A statement with the invariant that
      stmt->code == GIMPLE_ASSIGN
   i.e. an assignment statement.  */

struct GTY((tag("GSS_WITH_MEM_OPS")))
  gassign : public gimple_statement_with_memory_ops
{
  static const enum gimple_code code_ = GIMPLE_ASSIGN;
  /* no additional fields; this uses the layout for GSS_WITH_MEM_OPS. */
};

/* A statement with the invariant that
      stmt->code == GIMPLE_RETURN
   i.e. a return statement.  */

struct GTY((tag("GSS_WITH_MEM_OPS")))
  greturn : public gimple_statement_with_memory_ops
{
  /* no additional fields; this uses the layout for GSS_WITH_MEM_OPS. */
};

template <>
template <>
inline bool
is_a_helper <gasm *>::test (gimple *gs)
{
  return gs->code == GIMPLE_ASM;
}

template <>
template <>
inline bool
is_a_helper <gassign *>::test (gimple *gs)
{
  return gs->code == GIMPLE_ASSIGN;
}

template <>
template <>
inline bool
is_a_helper <const gassign *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_ASSIGN;
}

template <>
template <>
inline bool
is_a_helper <gbind *>::test (gimple *gs)
{
  return gs->code == GIMPLE_BIND;
}

template <>
template <>
inline bool
is_a_helper <gcall *>::test (gimple *gs)
{
  return gs->code == GIMPLE_CALL;
}

template <>
template <>
inline bool
is_a_helper <gcatch *>::test (gimple *gs)
{
  return gs->code == GIMPLE_CATCH;
}

template <>
template <>
inline bool
is_a_helper <gcond *>::test (gimple *gs)
{
  return gs->code == GIMPLE_COND;
}

template <>
template <>
inline bool
is_a_helper <const gcond *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_COND;
}

template <>
template <>
inline bool
is_a_helper <gdebug *>::test (gimple *gs)
{
  return gs->code == GIMPLE_DEBUG;
}

template <>
template <>
inline bool
is_a_helper <ggoto *>::test (gimple *gs)
{
  return gs->code == GIMPLE_GOTO;
}

template <>
template <>
inline bool
is_a_helper <glabel *>::test (gimple *gs)
{
  return gs->code == GIMPLE_LABEL;
}

template <>
template <>
inline bool
is_a_helper <gresx *>::test (gimple *gs)
{
  return gs->code == GIMPLE_RESX;
}

template <>
template <>
inline bool
is_a_helper <geh_dispatch *>::test (gimple *gs)
{
  return gs->code == GIMPLE_EH_DISPATCH;
}

template <>
template <>
inline bool
is_a_helper <geh_else *>::test (gimple *gs)
{
  return gs->code == GIMPLE_EH_ELSE;
}

template <>
template <>
inline bool
is_a_helper <geh_filter *>::test (gimple *gs)
{
  return gs->code == GIMPLE_EH_FILTER;
}

template <>
template <>
inline bool
is_a_helper <geh_mnt *>::test (gimple *gs)
{
  return gs->code == GIMPLE_EH_MUST_NOT_THROW;
}

template <>
template <>
inline bool
is_a_helper <gomp_atomic_load *>::test (gimple *gs)
{
  return gs->code == GIMPLE_OMP_ATOMIC_LOAD;
}

template <>
template <>
inline bool
is_a_helper <gomp_atomic_store *>::test (gimple *gs)
{
  return gs->code == GIMPLE_OMP_ATOMIC_STORE;
}

template <>
template <>
inline bool
is_a_helper <gimple_statement_omp_return *>::test (gimple *gs)
{
  return gs->code == GIMPLE_OMP_RETURN;
}

template <>
template <>
inline bool
is_a_helper <gomp_continue *>::test (gimple *gs)
{
  return gs->code == GIMPLE_OMP_CONTINUE;
}

template <>
template <>
inline bool
is_a_helper <gomp_critical *>::test (gimple *gs)
{
  return gs->code == GIMPLE_OMP_CRITICAL;
}

template <>
template <>
inline bool
is_a_helper <gomp_ordered *>::test (gimple *gs)
{
  return gs->code == GIMPLE_OMP_ORDERED;
}

template <>
template <>
inline bool
is_a_helper <gomp_for *>::test (gimple *gs)
{
  return gs->code == GIMPLE_OMP_FOR;
}

template <>
template <>
inline bool
is_a_helper <gimple_statement_omp_taskreg *>::test (gimple *gs)
{
  return gs->code == GIMPLE_OMP_PARALLEL || gs->code == GIMPLE_OMP_TASK;
}

template <>
template <>
inline bool
is_a_helper <gomp_parallel *>::test (gimple *gs)
{
  return gs->code == GIMPLE_OMP_PARALLEL;
}

template <>
template <>
inline bool
is_a_helper <gomp_target *>::test (gimple *gs)
{
  return gs->code == GIMPLE_OMP_TARGET;
}

template <>
template <>
inline bool
is_a_helper <gomp_sections *>::test (gimple *gs)
{
  return gs->code == GIMPLE_OMP_SECTIONS;
}

template <>
template <>
inline bool
is_a_helper <gomp_single *>::test (gimple *gs)
{
  return gs->code == GIMPLE_OMP_SINGLE;
}

template <>
template <>
inline bool
is_a_helper <gomp_teams *>::test (gimple *gs)
{
  return gs->code == GIMPLE_OMP_TEAMS;
}

template <>
template <>
inline bool
is_a_helper <gomp_task *>::test (gimple *gs)
{
  return gs->code == GIMPLE_OMP_TASK;
}

template <>
template <>
inline bool
is_a_helper <gphi *>::test (gimple *gs)
{
  return gs->code == GIMPLE_PHI;
}

template <>
template <>
inline bool
is_a_helper <greturn *>::test (gimple *gs)
{
  return gs->code == GIMPLE_RETURN;
}

template <>
template <>
inline bool
is_a_helper <gswitch *>::test (gimple *gs)
{
  return gs->code == GIMPLE_SWITCH;
}

template <>
template <>
inline bool
is_a_helper <gtransaction *>::test (gimple *gs)
{
  return gs->code == GIMPLE_TRANSACTION;
}

template <>
template <>
inline bool
is_a_helper <gtry *>::test (gimple *gs)
{
  return gs->code == GIMPLE_TRY;
}

template <>
template <>
inline bool
is_a_helper <gimple_statement_wce *>::test (gimple *gs)
{
  return gs->code == GIMPLE_WITH_CLEANUP_EXPR;
}

template <>
template <>
inline bool
is_a_helper <const gasm *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_ASM;
}

template <>
template <>
inline bool
is_a_helper <const gbind *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_BIND;
}

template <>
template <>
inline bool
is_a_helper <const gcall *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_CALL;
}

template <>
template <>
inline bool
is_a_helper <const gcatch *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_CATCH;
}

template <>
template <>
inline bool
is_a_helper <const gresx *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_RESX;
}

template <>
template <>
inline bool
is_a_helper <const geh_dispatch *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_EH_DISPATCH;
}

template <>
template <>
inline bool
is_a_helper <const geh_filter *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_EH_FILTER;
}

template <>
template <>
inline bool
is_a_helper <const gomp_atomic_load *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_OMP_ATOMIC_LOAD;
}

template <>
template <>
inline bool
is_a_helper <const gomp_atomic_store *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_OMP_ATOMIC_STORE;
}

template <>
template <>
inline bool
is_a_helper <const gimple_statement_omp_return *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_OMP_RETURN;
}

template <>
template <>
inline bool
is_a_helper <const gomp_continue *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_OMP_CONTINUE;
}

template <>
template <>
inline bool
is_a_helper <const gomp_critical *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_OMP_CRITICAL;
}

template <>
template <>
inline bool
is_a_helper <const gomp_ordered *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_OMP_ORDERED;
}

template <>
template <>
inline bool
is_a_helper <const gomp_for *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_OMP_FOR;
}

template <>
template <>
inline bool
is_a_helper <const gimple_statement_omp_taskreg *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_OMP_PARALLEL || gs->code == GIMPLE_OMP_TASK;
}

template <>
template <>
inline bool
is_a_helper <const gomp_parallel *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_OMP_PARALLEL;
}

template <>
template <>
inline bool
is_a_helper <const gomp_target *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_OMP_TARGET;
}

template <>
template <>
inline bool
is_a_helper <const gomp_sections *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_OMP_SECTIONS;
}

template <>
template <>
inline bool
is_a_helper <const gomp_single *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_OMP_SINGLE;
}

template <>
template <>
inline bool
is_a_helper <const gomp_teams *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_OMP_TEAMS;
}

template <>
template <>
inline bool
is_a_helper <const gomp_task *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_OMP_TASK;
}

template <>
template <>
inline bool
is_a_helper <const gphi *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_PHI;
}

template <>
template <>
inline bool
is_a_helper <const gtransaction *>::test (const gimple *gs)
{
  return gs->code == GIMPLE_TRANSACTION;
}

/* Offset in bytes to the location of the operand vector.
   Zero if there is no operand vector for this tuple structure.  */
extern size_t const gimple_ops_offset_[];

/* Map GIMPLE codes to GSS codes.  */
extern enum gimple_statement_structure_enum const gss_for_code_[];

/* This variable holds the currently expanded gimple statement for purposes
   of comminucating the profile info to the builtin expanders.  */
extern gimple *currently_expanding_gimple_stmt;

gimple *gimple_alloc (enum gimple_code, unsigned CXX_MEM_STAT_INFO);
greturn *gimple_build_return (tree);
void gimple_call_reset_alias_info (gcall *);
gcall *gimple_build_call_vec (tree, vec<tree> );
gcall *gimple_build_call (tree, unsigned, ...);
gcall *gimple_build_call_valist (tree, unsigned, va_list);
gcall *gimple_build_call_internal (enum internal_fn, unsigned, ...);
gcall *gimple_build_call_internal_vec (enum internal_fn, vec<tree> );
gcall *gimple_build_call_from_tree (tree, tree);
gassign *gimple_build_assign (tree, tree CXX_MEM_STAT_INFO);
gassign *gimple_build_assign (tree, enum tree_code,
			      tree, tree, tree CXX_MEM_STAT_INFO);
gassign *gimple_build_assign (tree, enum tree_code,
			      tree, tree CXX_MEM_STAT_INFO);
gassign *gimple_build_assign (tree, enum tree_code, tree CXX_MEM_STAT_INFO);
gcond *gimple_build_cond (enum tree_code, tree, tree, tree, tree);
gcond *gimple_build_cond_from_tree (tree, tree, tree);
void gimple_cond_set_condition_from_tree (gcond *, tree);
glabel *gimple_build_label (tree label);
ggoto *gimple_build_goto (tree dest);
gimple *gimple_build_nop (void);
gbind *gimple_build_bind (tree, gimple_seq, tree);
gasm *gimple_build_asm_vec (const char *, vec<tree, va_gc> *,
				 vec<tree, va_gc> *, vec<tree, va_gc> *,
				 vec<tree, va_gc> *);
gcatch *gimple_build_catch (tree, gimple_seq);
geh_filter *gimple_build_eh_filter (tree, gimple_seq);
geh_mnt *gimple_build_eh_must_not_throw (tree);
geh_else *gimple_build_eh_else (gimple_seq, gimple_seq);
gtry *gimple_build_try (gimple_seq, gimple_seq,
					enum gimple_try_flags);
gimple *gimple_build_wce (gimple_seq);
gresx *gimple_build_resx (int);
gswitch *gimple_build_switch_nlabels (unsigned, tree, tree);
gswitch *gimple_build_switch (tree, tree, vec<tree> );
geh_dispatch *gimple_build_eh_dispatch (int);
gdebug *gimple_build_debug_bind (tree, tree, gimple * CXX_MEM_STAT_INFO);
gdebug *gimple_build_debug_source_bind (tree, tree, gimple * CXX_MEM_STAT_INFO);
gdebug *gimple_build_debug_begin_stmt (tree, location_t CXX_MEM_STAT_INFO);
gdebug *gimple_build_debug_inline_entry (tree, location_t CXX_MEM_STAT_INFO);
gomp_critical *gimple_build_omp_critical (gimple_seq, tree, tree);
gomp_for *gimple_build_omp_for (gimple_seq, int, tree, size_t, gimple_seq);
gomp_parallel *gimple_build_omp_parallel (gimple_seq, tree, tree, tree);
gomp_task *gimple_build_omp_task (gimple_seq, tree, tree, tree, tree,
				       tree, tree);
gimple *gimple_build_omp_section (gimple_seq);
gimple *gimple_build_omp_master (gimple_seq);
gimple *gimple_build_omp_grid_body (gimple_seq);
gimple *gimple_build_omp_taskgroup (gimple_seq);
gomp_continue *gimple_build_omp_continue (tree, tree);
gomp_ordered *gimple_build_omp_ordered (gimple_seq, tree);
gimple *gimple_build_omp_return (bool);
gomp_sections *gimple_build_omp_sections (gimple_seq, tree);
gimple *gimple_build_omp_sections_switch (void);
gomp_single *gimple_build_omp_single (gimple_seq, tree);
gomp_target *gimple_build_omp_target (gimple_seq, int, tree);
gomp_teams *gimple_build_omp_teams (gimple_seq, tree);
gomp_atomic_load *gimple_build_omp_atomic_load (tree, tree);
gomp_atomic_store *gimple_build_omp_atomic_store (tree);
gtransaction *gimple_build_transaction (gimple_seq);
extern void gimple_seq_add_stmt (gimple_seq *, gimple *);
extern void gimple_seq_add_stmt_without_update (gimple_seq *, gimple *);
void gimple_seq_add_seq (gimple_seq *, gimple_seq);
void gimple_seq_add_seq_without_update (gimple_seq *, gimple_seq);
extern void annotate_all_with_location_after (gimple_seq, gimple_stmt_iterator,
					      location_t);
extern void annotate_all_with_location (gimple_seq, location_t);
bool empty_body_p (gimple_seq);
gimple_seq gimple_seq_copy (gimple_seq);
bool gimple_call_same_target_p (const gimple *, const gimple *);
int gimple_call_flags (const gimple *);
int gimple_call_arg_flags (const gcall *, unsigned);
int gimple_call_return_flags (const gcall *);
bool gimple_assign_copy_p (gimple *);
bool gimple_assign_ssa_name_copy_p (gimple *);
bool gimple_assign_unary_nop_p (gimple *);
void gimple_set_bb (gimple *, basic_block);
void gimple_assign_set_rhs_from_tree (gimple_stmt_iterator *, tree);
void gimple_assign_set_rhs_with_ops (gimple_stmt_iterator *, enum tree_code,
				     tree, tree, tree);
tree gimple_get_lhs (const gimple *);
void gimple_set_lhs (gimple *, tree);
gimple *gimple_copy (gimple *);
bool gimple_has_side_effects (const gimple *);
bool gimple_could_trap_p_1 (gimple *, bool, bool);
bool gimple_could_trap_p (gimple *);
bool gimple_assign_rhs_could_trap_p (gimple *);
extern void dump_gimple_statistics (void);
unsigned get_gimple_rhs_num_ops (enum tree_code);
extern tree canonicalize_cond_expr_cond (tree);
gcall *gimple_call_copy_skip_args (gcall *, bitmap);
extern bool gimple_compare_field_offset (tree, tree);
extern tree gimple_unsigned_type (tree);
extern tree gimple_signed_type (tree);
extern alias_set_type gimple_get_alias_set (tree);
extern bool gimple_ior_addresses_taken (bitmap, gimple *);
extern bool gimple_builtin_call_types_compatible_p (const gimple *, tree);
extern combined_fn gimple_call_combined_fn (const gimple *);
extern bool gimple_call_builtin_p (const gimple *);
extern bool gimple_call_builtin_p (const gimple *, enum built_in_class);
extern bool gimple_call_builtin_p (const gimple *, enum built_in_function);
extern bool gimple_asm_clobbers_memory_p (const gasm *);
extern void dump_decl_set (FILE *, bitmap);
extern bool nonfreeing_call_p (gimple *);
extern bool nonbarrier_call_p (gimple *);
extern bool infer_nonnull_range (gimple *, tree);
extern bool infer_nonnull_range_by_dereference (gimple *, tree);
extern bool infer_nonnull_range_by_attribute (gimple *, tree);
extern void sort_case_labels (vec<tree>);
extern void preprocess_case_label_vec_for_gimple (vec<tree>, tree, tree *);
extern void gimple_seq_set_location (gimple_seq, location_t);
extern void gimple_seq_discard (gimple_seq);
extern void maybe_remove_unused_call_args (struct function *, gimple *);
extern bool gimple_inexpensive_call_p (gcall *);
extern bool stmt_can_terminate_bb_p (gimple *);

/* Formal (expression) temporary table handling: multiple occurrences of
   the same scalar expression are evaluated into the same temporary.  */

typedef struct gimple_temp_hash_elt
{
  tree val;   /* Key */
  tree temp;  /* Value */
} elt_t;

/* Get the number of the next statement uid to be allocated.  */
static inline unsigned int
gimple_stmt_max_uid (struct function *fn)
{
  return fn->last_stmt_uid;
}

/* Set the number of the next statement uid to be allocated.  */
static inline void
set_gimple_stmt_max_uid (struct function *fn, unsigned int maxid)
{
  fn->last_stmt_uid = maxid;
}

/* Set the number of the next statement uid to be allocated.  */
static inline unsigned int
inc_gimple_stmt_max_uid (struct function *fn)
{
  return fn->last_stmt_uid++;
}

/* Return the first node in GIMPLE sequence S.  */

static inline gimple_seq_node
gimple_seq_first (gimple_seq s)
{
  return s;
}


/* Return the first statement in GIMPLE sequence S.  */

static inline gimple *
gimple_seq_first_stmt (gimple_seq s)
{
  gimple_seq_node n = gimple_seq_first (s);
  return n;
}

/* Return the first statement in GIMPLE sequence S as a gbind *,
   verifying that it has code GIMPLE_BIND in a checked build.  */

static inline gbind *
gimple_seq_first_stmt_as_a_bind (gimple_seq s)
{
  gimple_seq_node n = gimple_seq_first (s);
  return as_a <gbind *> (n);
}


/* Return the last node in GIMPLE sequence S.  */

static inline gimple_seq_node
gimple_seq_last (gimple_seq s)
{
  return s ? s->prev : NULL;
}


/* Return the last statement in GIMPLE sequence S.  */

static inline gimple *
gimple_seq_last_stmt (gimple_seq s)
{
  gimple_seq_node n = gimple_seq_last (s);
  return n;
}


/* Set the last node in GIMPLE sequence *PS to LAST.  */

static inline void
gimple_seq_set_last (gimple_seq *ps, gimple_seq_node last)
{
  (*ps)->prev = last;
}


/* Set the first node in GIMPLE sequence *PS to FIRST.  */

static inline void
gimple_seq_set_first (gimple_seq *ps, gimple_seq_node first)
{
  *ps = first;
}


/* Return true if GIMPLE sequence S is empty.  */

static inline bool
gimple_seq_empty_p (gimple_seq s)
{
  return s == NULL;
}

/* Allocate a new sequence and initialize its first element with STMT.  */

static inline gimple_seq
gimple_seq_alloc_with_stmt (gimple *stmt)
{
  gimple_seq seq = NULL;
  gimple_seq_add_stmt (&seq, stmt);
  return seq;
}


/* Returns the sequence of statements in BB.  */

static inline gimple_seq
bb_seq (const_basic_block bb)
{
  return (!(bb->flags & BB_RTL)) ? bb->il.gimple.seq : NULL;
}

static inline gimple_seq *
bb_seq_addr (basic_block bb)
{
  return (!(bb->flags & BB_RTL)) ? &bb->il.gimple.seq : NULL;
}

/* Sets the sequence of statements in BB to SEQ.  */

static inline void
set_bb_seq (basic_block bb, gimple_seq seq)
{
  gcc_checking_assert (!(bb->flags & BB_RTL));
  bb->il.gimple.seq = seq;
}


/* Return the code for GIMPLE statement G.  */

static inline enum gimple_code
gimple_code (const gimple *g)
{
  return g->code;
}


/* Return the GSS code used by a GIMPLE code.  */

static inline enum gimple_statement_structure_enum
gss_for_code (enum gimple_code code)
{
  gcc_gimple_checking_assert ((unsigned int)code < LAST_AND_UNUSED_GIMPLE_CODE);
  return gss_for_code_[code];
}


/* Return which GSS code is used by GS.  */

static inline enum gimple_statement_structure_enum
gimple_statement_structure (gimple *gs)
{
  return gss_for_code (gimple_code (gs));
}


/* Return true if statement G has sub-statements.  This is only true for
   High GIMPLE statements.  */

static inline bool
gimple_has_substatements (gimple *g)
{
  switch (gimple_code (g))
    {
    case GIMPLE_BIND:
    case GIMPLE_CATCH:
    case GIMPLE_EH_FILTER:
    case GIMPLE_EH_ELSE:
    case GIMPLE_TRY:
    case GIMPLE_OMP_FOR:
    case GIMPLE_OMP_MASTER:
    case GIMPLE_OMP_TASKGROUP:
    case GIMPLE_OMP_ORDERED:
    case GIMPLE_OMP_SECTION:
    case GIMPLE_OMP_PARALLEL:
    case GIMPLE_OMP_TASK:
    case GIMPLE_OMP_SECTIONS:
    case GIMPLE_OMP_SINGLE:
    case GIMPLE_OMP_TARGET:
    case GIMPLE_OMP_TEAMS:
    case GIMPLE_OMP_CRITICAL:
    case GIMPLE_WITH_CLEANUP_EXPR:
    case GIMPLE_TRANSACTION:
    case GIMPLE_OMP_GRID_BODY:
      return true;

    default:
      return false;
    }
}


/* Return the basic block holding statement G.  */

static inline basic_block
gimple_bb (const gimple *g)
{
  return g->bb;
}


/* Return the lexical scope block holding statement G.  */

static inline tree
gimple_block (const gimple *g)
{
  return LOCATION_BLOCK (g->location);
}


/* Set BLOCK to be the lexical scope block holding statement G.  */

static inline void
gimple_set_block (gimple *g, tree block)
{
  g->location = set_block (g->location, block);
}


/* Return location information for statement G.  */

static inline location_t
gimple_location (const gimple *g)
{
  return g->location;
}

/* Return location information for statement G if g is not NULL.
   Otherwise, UNKNOWN_LOCATION is returned.  */

static inline location_t
gimple_location_safe (const gimple *g)
{
  return g ? gimple_location (g) : UNKNOWN_LOCATION;
}

/* Set location information for statement G.  */

static inline void
gimple_set_location (gimple *g, location_t location)
{
  g->location = location;
}


/* Return true if G contains location information.  */

static inline bool
gimple_has_location (const gimple *g)
{
  return LOCATION_LOCUS (gimple_location (g)) != UNKNOWN_LOCATION;
}


/* Return the file name of the location of STMT.  */

static inline const char *
gimple_filename (const gimple *stmt)
{
  return LOCATION_FILE (gimple_location (stmt));
}


/* Return the line number of the location of STMT.  */

static inline int
gimple_lineno (const gimple *stmt)
{
  return LOCATION_LINE (gimple_location (stmt));
}


/* Determine whether SEQ is a singleton. */

static inline bool
gimple_seq_singleton_p (gimple_seq seq)
{
  return ((gimple_seq_first (seq) != NULL)
	  && (gimple_seq_first (seq) == gimple_seq_last (seq)));
}

/* Return true if no warnings should be emitted for statement STMT.  */

static inline bool
gimple_no_warning_p (const gimple *stmt)
{
  return stmt->no_warning;
}

/* Set the no_warning flag of STMT to NO_WARNING.  */

static inline void
gimple_set_no_warning (gimple *stmt, bool no_warning)
{
  stmt->no_warning = (unsigned) no_warning;
}

/* Set the visited status on statement STMT to VISITED_P.

   Please note that this 'visited' property of the gimple statement is
   supposed to be undefined at pass boundaries.  This means that a
   given pass should not assume it contains any useful value when the
   pass starts and thus can set it to any value it sees fit.

   You can learn more about the visited property of the gimple
   statement by reading the comments of the 'visited' data member of
   struct gimple.
 */

static inline void
gimple_set_visited (gimple *stmt, bool visited_p)
{
  stmt->visited = (unsigned) visited_p;
}


/* Return the visited status for statement STMT.

   Please note that this 'visited' property of the gimple statement is
   supposed to be undefined at pass boundaries.  This means that a
   given pass should not assume it contains any useful value when the
   pass starts and thus can set it to any value it sees fit.

   You can learn more about the visited property of the gimple
   statement by reading the comments of the 'visited' data member of
   struct gimple.  */

static inline bool
gimple_visited_p (gimple *stmt)
{
  return stmt->visited;
}


/* Set pass local flag PLF on statement STMT to VAL_P.

   Please note that this PLF property of the gimple statement is
   supposed to be undefined at pass boundaries.  This means that a
   given pass should not assume it contains any useful value when the
   pass starts and thus can set it to any value it sees fit.

   You can learn more about the PLF property by reading the comment of
   the 'plf' data member of struct gimple_statement_structure.  */

static inline void
gimple_set_plf (gimple *stmt, enum plf_mask plf, bool val_p)
{
  if (val_p)
    stmt->plf |= (unsigned int) plf;
  else
    stmt->plf &= ~((unsigned int) plf);
}


/* Return the value of pass local flag PLF on statement STMT.

   Please note that this 'plf' property of the gimple statement is
   supposed to be undefined at pass boundaries.  This means that a
   given pass should not assume it contains any useful value when the
   pass starts and thus can set it to any value it sees fit.

   You can learn more about the plf property by reading the comment of
   the 'plf' data member of struct gimple_statement_structure.  */

static inline unsigned int
gimple_plf (gimple *stmt, enum plf_mask plf)
{
  return stmt->plf & ((unsigned int) plf);
}


/* Set the UID of statement.

   Please note that this UID property is supposed to be undefined at
   pass boundaries.  This means that a given pass should not assume it
   contains any useful value when the pass starts and thus can set it
   to any value it sees fit.  */

static inline void
gimple_set_uid (gimple *g, unsigned uid)
{
  g->uid = uid;
}


/* Return the UID of statement.

   Please note that this UID property is supposed to be undefined at
   pass boundaries.  This means that a given pass should not assume it
   contains any useful value when the pass starts and thus can set it
   to any value it sees fit.  */

static inline unsigned
gimple_uid (const gimple *g)
{
  return g->uid;
}


/* Make statement G a singleton sequence.  */

static inline void
gimple_init_singleton (gimple *g)
{
  g->next = NULL;
  g->prev = g;
}


/* Return true if GIMPLE statement G has register or memory operands.  */

static inline bool
gimple_has_ops (const gimple *g)
{
  return gimple_code (g) >= GIMPLE_COND && gimple_code (g) <= GIMPLE_RETURN;
}

template <>
template <>
inline bool
is_a_helper <const gimple_statement_with_ops *>::test (const gimple *gs)
{
  return gimple_has_ops (gs);
}

template <>
template <>
inline bool
is_a_helper <gimple_statement_with_ops *>::test (gimple *gs)
{
  return gimple_has_ops (gs);
}

/* Return true if GIMPLE statement G has memory operands.  */

static inline bool
gimple_has_mem_ops (const gimple *g)
{
  return gimple_code (g) >= GIMPLE_ASSIGN && gimple_code (g) <= GIMPLE_RETURN;
}

template <>
template <>
inline bool
is_a_helper <const gimple_statement_with_memory_ops *>::test (const gimple *gs)
{
  return gimple_has_mem_ops (gs);
}

template <>
template <>
inline bool
is_a_helper <gimple_statement_with_memory_ops *>::test (gimple *gs)
{
  return gimple_has_mem_ops (gs);
}

/* Return the set of USE operands for statement G.  */

static inline struct use_optype_d *
gimple_use_ops (const gimple *g)
{
  const gimple_statement_with_ops *ops_stmt =
    dyn_cast <const gimple_statement_with_ops *> (g);
  if (!ops_stmt)
    return NULL;
  return ops_stmt->use_ops;
}


/* Set USE to be the set of USE operands for statement G.  */

static inline void
gimple_set_use_ops (gimple *g, struct use_optype_d *use)
{
  gimple_statement_with_ops *ops_stmt =
    as_a <gimple_statement_with_ops *> (g);
  ops_stmt->use_ops = use;
}


/* Return the single VUSE operand of the statement G.  */

static inline tree
gimple_vuse (const gimple *g)
{
  const gimple_statement_with_memory_ops *mem_ops_stmt =
     dyn_cast <const gimple_statement_with_memory_ops *> (g);
  if (!mem_ops_stmt)
    return NULL_TREE;
  return mem_ops_stmt->vuse;
}

/* Return the single VDEF operand of the statement G.  */

static inline tree
gimple_vdef (const gimple *g)
{
  const gimple_statement_with_memory_ops *mem_ops_stmt =
     dyn_cast <const gimple_statement_with_memory_ops *> (g);
  if (!mem_ops_stmt)
    return NULL_TREE;
  return mem_ops_stmt->vdef;
}

/* Return the single VUSE operand of the statement G.  */

static inline tree *
gimple_vuse_ptr (gimple *g)
{
  gimple_statement_with_memory_ops *mem_ops_stmt =
     dyn_cast <gimple_statement_with_memory_ops *> (g);
  if (!mem_ops_stmt)
    return NULL;
  return &mem_ops_stmt->vuse;
}

/* Return the single VDEF operand of the statement G.  */

static inline tree *
gimple_vdef_ptr (gimple *g)
{
  gimple_statement_with_memory_ops *mem_ops_stmt =
     dyn_cast <gimple_statement_with_memory_ops *> (g);
  if (!mem_ops_stmt)
    return NULL;
  return &mem_ops_stmt->vdef;
}

/* Set the single VUSE operand of the statement G.  */

static inline void
gimple_set_vuse (gimple *g, tree vuse)
{
  gimple_statement_with_memory_ops *mem_ops_stmt =
    as_a <gimple_statement_with_memory_ops *> (g);
  mem_ops_stmt->vuse = vuse;
}

/* Set the single VDEF operand of the statement G.  */

static inline void
gimple_set_vdef (gimple *g, tree vdef)
{
  gimple_statement_with_memory_ops *mem_ops_stmt =
    as_a <gimple_statement_with_memory_ops *> (g);
  mem_ops_stmt->vdef = vdef;
}


/* Return true if statement G has operands and the modified field has
   been set.  */

static inline bool
gimple_modified_p (const gimple *g)
{
  return (gimple_has_ops (g)) ? (bool) g->modified : false;
}


/* Set the MODIFIED flag to MODIFIEDP, iff the gimple statement G has
   a MODIFIED field.  */

static inline void
gimple_set_modified (gimple *s, bool modifiedp)
{
  if (gimple_has_ops (s))
    s->modified = (unsigned) modifiedp;
}


/* Return the tree code for the expression computed by STMT.  This is
   only valid for GIMPLE_COND, GIMPLE_CALL and GIMPLE_ASSIGN.  For
   GIMPLE_CALL, return CALL_EXPR as the expression code for
   consistency.  This is useful when the caller needs to deal with the
   three kinds of computation that GIMPLE supports.  */

static inline enum tree_code
gimple_expr_code (const gimple *stmt)
{
  enum gimple_code code = gimple_code (stmt);
  if (code == GIMPLE_ASSIGN || code == GIMPLE_COND)
    return (enum tree_code) stmt->subcode;
  else
    {
      gcc_gimple_checking_assert (code == GIMPLE_CALL);
      return CALL_EXPR;
    }
}


/* Return true if statement STMT contains volatile operands.  */

static inline bool
gimple_has_volatile_ops (const gimple *stmt)
{
  if (gimple_has_mem_ops (stmt))
    return stmt->has_volatile_ops;
  else
    return false;
}


/* Set the HAS_VOLATILE_OPS flag to VOLATILEP.  */

static inline void
gimple_set_has_volatile_ops (gimple *stmt, bool volatilep)
{
  if (gimple_has_mem_ops (stmt))
    stmt->has_volatile_ops = (unsigned) volatilep;
}

/* Return true if STMT is in a transaction.  */

static inline bool
gimple_in_transaction (const gimple *stmt)
{
  return bb_in_transaction (gimple_bb (stmt));
}

/* Return true if statement STMT may access memory.  */

static inline bool
gimple_references_memory_p (gimple *stmt)
{
  return gimple_has_mem_ops (stmt) && gimple_vuse (stmt);
}


/* Return the subcode for OMP statement S.  */

static inline unsigned
gimple_omp_subcode (const gimple *s)
{
  gcc_gimple_checking_assert (gimple_code (s) >= GIMPLE_OMP_ATOMIC_LOAD
	      && gimple_code (s) <= GIMPLE_OMP_TEAMS);
  return s->subcode;
}

/* Set the subcode for OMP statement S to SUBCODE.  */

static inline void
gimple_omp_set_subcode (gimple *s, unsigned int subcode)
{
  /* We only have 16 bits for the subcode.  Assert that we are not
     overflowing it.  */
  gcc_gimple_checking_assert (subcode < (1 << 16));
  s->subcode = subcode;
}

/* Set the nowait flag on OMP_RETURN statement S.  */

static inline void
gimple_omp_return_set_nowait (gimple *s)
{
  GIMPLE_CHECK (s, GIMPLE_OMP_RETURN);
  s->subcode |= GF_OMP_RETURN_NOWAIT;
}


/* Return true if OMP return statement G has the GF_OMP_RETURN_NOWAIT
   flag set.  */

static inline bool
gimple_omp_return_nowait_p (const gimple *g)
{
  GIMPLE_CHECK (g, GIMPLE_OMP_RETURN);
  return (gimple_omp_subcode (g) & GF_OMP_RETURN_NOWAIT) != 0;
}


/* Set the LHS of OMP return.  */

static inline void
gimple_omp_return_set_lhs (gimple *g, tree lhs)
{
  gimple_statement_omp_return *omp_return_stmt =
    as_a <gimple_statement_omp_return *> (g);
  omp_return_stmt->val = lhs;
}


/* Get the LHS of OMP return.  */

static inline tree
gimple_omp_return_lhs (const gimple *g)
{
  const gimple_statement_omp_return *omp_return_stmt =
    as_a <const gimple_statement_omp_return *> (g);
  return omp_return_stmt->val;
}


/* Return a pointer to the LHS of OMP return.  */

static inline tree *
gimple_omp_return_lhs_ptr (gimple *g)
{
  gimple_statement_omp_return *omp_return_stmt =
    as_a <gimple_statement_omp_return *> (g);
  return &omp_return_stmt->val;
}


/* Return true if OMP section statement G has the GF_OMP_SECTION_LAST
   flag set.  */

static inline bool
gimple_omp_section_last_p (const gimple *g)
{
  GIMPLE_CHECK (g, GIMPLE_OMP_SECTION);
  return (gimple_omp_subcode (g) & GF_OMP_SECTION_LAST) != 0;
}


/* Set the GF_OMP_SECTION_LAST flag on G.  */

static inline void
gimple_omp_section_set_last (gimple *g)
{
  GIMPLE_CHECK (g, GIMPLE_OMP_SECTION);
  g->subcode |= GF_OMP_SECTION_LAST;
}


/* Return true if OMP parallel statement G has the
   GF_OMP_PARALLEL_COMBINED flag set.  */

static inline bool
gimple_omp_parallel_combined_p (const gimple *g)
{
  GIMPLE_CHECK (g, GIMPLE_OMP_PARALLEL);
  return (gimple_omp_subcode (g) & GF_OMP_PARALLEL_COMBINED) != 0;
}


/* Set the GF_OMP_PARALLEL_COMBINED field in G depending on the boolean
   value of COMBINED_P.  */

static inline void
gimple_omp_parallel_set_combined_p (gimple *g, bool combined_p)
{
  GIMPLE_CHECK (g, GIMPLE_OMP_PARALLEL);
  if (combined_p)
    g->subcode |= GF_OMP_PARALLEL_COMBINED;
  else
    g->subcode &= ~GF_OMP_PARALLEL_COMBINED;
}


/* Return true if OMP atomic load/store statement G has the
   GF_OMP_ATOMIC_NEED_VALUE flag set.  */

static inline bool
gimple_omp_atomic_need_value_p (const gimple *g)
{
  if (gimple_code (g) != GIMPLE_OMP_ATOMIC_LOAD)
    GIMPLE_CHECK (g, GIMPLE_OMP_ATOMIC_STORE);
  return (gimple_omp_subcode (g) & GF_OMP_ATOMIC_NEED_VALUE) != 0;
}


/* Set the GF_OMP_ATOMIC_NEED_VALUE flag on G.  */

static inline void
gimple_omp_atomic_set_need_value (gimple *g)
{
  if (gimple_code (g) != GIMPLE_OMP_ATOMIC_LOAD)
    GIMPLE_CHECK (g, GIMPLE_OMP_ATOMIC_STORE);
  g->subcode |= GF_OMP_ATOMIC_NEED_VALUE;
}


/* Return true if OMP atomic load/store statement G has the
   GF_OMP_ATOMIC_SEQ_CST flag set.  */

static inline bool
gimple_omp_atomic_seq_cst_p (const gimple *g)
{
  if (gimple_code (g) != GIMPLE_OMP_ATOMIC_LOAD)
    GIMPLE_CHECK (g, GIMPLE_OMP_ATOMIC_STORE);
  return (gimple_omp_subcode (g) & GF_OMP_ATOMIC_SEQ_CST) != 0;
}


/* Set the GF_OMP_ATOMIC_SEQ_CST flag on G.  */

static inline void
gimple_omp_atomic_set_seq_cst (gimple *g)
{
  if (gimple_code (g) != GIMPLE_OMP_ATOMIC_LOAD)
    GIMPLE_CHECK (g, GIMPLE_OMP_ATOMIC_STORE);
  g->subcode |= GF_OMP_ATOMIC_SEQ_CST;
}


/* Return the number of operands for statement GS.  */

static inline unsigned
gimple_num_ops (const gimple *gs)
{
  return gs->num_ops;
}


/* Set the number of operands for statement GS.  */

static inline void
gimple_set_num_ops (gimple *gs, unsigned num_ops)
{
  gs->num_ops = num_ops;
}


/* Return the array of operands for statement GS.  */

static inline tree *
gimple_ops (gimple *gs)
{
  size_t off;

  /* All the tuples have their operand vector at the very bottom
     of the structure.  Note that those structures that do not
     have an operand vector have a zero offset.  */
  off = gimple_ops_offset_[gimple_statement_structure (gs)];
  gcc_gimple_checking_assert (off != 0);

  return (tree *) ((char *) gs + off);
}


/* Return operand I for statement GS.  */

static inline tree
gimple_op (const gimple *gs, unsigned i)
{
  if (gimple_has_ops (gs))
    {
      gcc_gimple_checking_assert (i < gimple_num_ops (gs));
      return gimple_ops (CONST_CAST_GIMPLE (gs))[i];
    }
  else
    return NULL_TREE;
}

/* Return a pointer to operand I for statement GS.  */

static inline tree *
gimple_op_ptr (gimple *gs, unsigned i)
{
  if (gimple_has_ops (gs))
    {
      gcc_gimple_checking_assert (i < gimple_num_ops (gs));
      return gimple_ops (gs) + i;
    }
  else
    return NULL;
}

/* Set operand I of statement GS to OP.  */

static inline void
gimple_set_op (gimple *gs, unsigned i, tree op)
{
  gcc_gimple_checking_assert (gimple_has_ops (gs) && i < gimple_num_ops (gs));

  /* Note.  It may be tempting to assert that OP matches
     is_gimple_operand, but that would be wrong.  Different tuples
     accept slightly different sets of tree operands.  Each caller
     should perform its own validation.  */
  gimple_ops (gs)[i] = op;
}

/* Return true if GS is a GIMPLE_ASSIGN.  */

static inline bool
is_gimple_assign (const gimple *gs)
{
  return gimple_code (gs) == GIMPLE_ASSIGN;
}

/* Determine if expression CODE is one of the valid expressions that can
   be used on the RHS of GIMPLE assignments.  */

static inline enum gimple_rhs_class
get_gimple_rhs_class (enum tree_code code)
{
  return (enum gimple_rhs_class) gimple_rhs_class_table[(int) code];
}

/* Return the LHS of assignment statement GS.  */

static inline tree
gimple_assign_lhs (const gassign *gs)
{
  return gs->op[0];
}

static inline tree
gimple_assign_lhs (const gimple *gs)
{
  const gassign *ass = GIMPLE_CHECK2<const gassign *> (gs);
  return gimple_assign_lhs (ass);
}


/* Return a pointer to the LHS of assignment statement GS.  */

static inline tree *
gimple_assign_lhs_ptr (gassign *gs)
{
  return &gs->op[0];
}

static inline tree *
gimple_assign_lhs_ptr (gimple *gs)
{
  gassign *ass = GIMPLE_CHECK2<gassign *> (gs);
  return gimple_assign_lhs_ptr (ass);
}


/* Set LHS to be the LHS operand of assignment statement GS.  */

static inline void
gimple_assign_set_lhs (gassign *gs, tree lhs)
{
  gs->op[0] = lhs;

  if (lhs && TREE_CODE (lhs) == SSA_NAME)
    SSA_NAME_DEF_STMT (lhs) = gs;
}

static inline void
gimple_assign_set_lhs (gimple *gs, tree lhs)
{
  gassign *ass = GIMPLE_CHECK2<gassign *> (gs);
  gimple_assign_set_lhs (ass, lhs);
}


/* Return the first operand on the RHS of assignment statement GS.  */

static inline tree
gimple_assign_rhs1 (const gassign *gs)
{
  return gs->op[1];
}

static inline tree
gimple_assign_rhs1 (const gimple *gs)
{
  const gassign *ass = GIMPLE_CHECK2<const gassign *> (gs);
  return gimple_assign_rhs1 (ass);
}


/* Return a pointer to the first operand on the RHS of assignment
   statement GS.  */

static inline tree *
gimple_assign_rhs1_ptr (gassign *gs)
{
  return &gs->op[1];
}

static inline tree *
gimple_assign_rhs1_ptr (gimple *gs)
{
  gassign *ass = GIMPLE_CHECK2<gassign *> (gs);
  return gimple_assign_rhs1_ptr (ass);
}

/* Set RHS to be the first operand on the RHS of assignment statement GS.  */

static inline void
gimple_assign_set_rhs1 (gassign *gs, tree rhs)
{
  gs->op[1] = rhs;
}

static inline void
gimple_assign_set_rhs1 (gimple *gs, tree rhs)
{
  gassign *ass = GIMPLE_CHECK2<gassign *> (gs);
  gimple_assign_set_rhs1 (ass, rhs);
}


/* Return the second operand on the RHS of assignment statement GS.
   If GS does not have two operands, NULL is returned instead.  */

static inline tree
gimple_assign_rhs2 (const gassign *gs)
{
  if (gimple_num_ops (gs) >= 3)
    return gs->op[2];
  else
    return NULL_TREE;
}

static inline tree
gimple_assign_rhs2 (const gimple *gs)
{
  const gassign *ass = GIMPLE_CHECK2<const gassign *> (gs);
  return gimple_assign_rhs2 (ass);
}


/* Return a pointer to the second operand on the RHS of assignment
   statement GS.  */

static inline tree *
gimple_assign_rhs2_ptr (gassign *gs)
{
  gcc_gimple_checking_assert (gimple_num_ops (gs) >= 3);
  return &gs->op[2];
}

static inline tree *
gimple_assign_rhs2_ptr (gimple *gs)
{
  gassign *ass = GIMPLE_CHECK2<gassign *> (gs);
  return gimple_assign_rhs2_ptr (ass);
}


/* Set RHS to be the second operand on the RHS of assignment statement GS.  */

static inline void
gimple_assign_set_rhs2 (gassign *gs, tree rhs)
{
  gcc_gimple_checking_assert (gimple_num_ops (gs) >= 3);
  gs->op[2] = rhs;
}

static inline void
gimple_assign_set_rhs2 (gimple *gs, tree rhs)
{
  gassign *ass = GIMPLE_CHECK2<gassign *> (gs);
  return gimple_assign_set_rhs2 (ass, rhs);
}

/* Return the third operand on the RHS of assignment statement GS.
   If GS does not have two operands, NULL is returned instead.  */

static inline tree
gimple_assign_rhs3 (const gassign *gs)
{
  if (gimple_num_ops (gs) >= 4)
    return gs->op[3];
  else
    return NULL_TREE;
}

static inline tree
gimple_assign_rhs3 (const gimple *gs)
{
  const gassign *ass = GIMPLE_CHECK2<const gassign *> (gs);
  return gimple_assign_rhs3 (ass);
}

/* Return a pointer to the third operand on the RHS of assignment
   statement GS.  */

static inline tree *
gimple_assign_rhs3_ptr (gimple *gs)
{
  gassign *ass = GIMPLE_CHECK2<gassign *> (gs);
  gcc_gimple_checking_assert (gimple_num_ops (gs) >= 4);
  return &ass->op[3];
}


/* Set RHS to be the third operand on the RHS of assignment statement GS.  */

static inline void
gimple_assign_set_rhs3 (gassign *gs, tree rhs)
{
  gcc_gimple_checking_assert (gimple_num_ops (gs) >= 4);
  gs->op[3] = rhs;
}

static inline void
gimple_assign_set_rhs3 (gimple *gs, tree rhs)
{
  gassign *ass = GIMPLE_CHECK2<gassign *> (gs);
  gimple_assign_set_rhs3 (ass, rhs);
}


/* A wrapper around 3 operand gimple_assign_set_rhs_with_ops, for callers
   which expect to see only two operands.  */

static inline void
gimple_assign_set_rhs_with_ops (gimple_stmt_iterator *gsi, enum tree_code code,
				tree op1, tree op2)
{
  gimple_assign_set_rhs_with_ops (gsi, code, op1, op2, NULL);
}

/* A wrapper around 3 operand gimple_assign_set_rhs_with_ops, for callers
   which expect to see only one operands.  */

static inline void
gimple_assign_set_rhs_with_ops (gimple_stmt_iterator *gsi, enum tree_code code,
				tree op1)
{
  gimple_assign_set_rhs_with_ops (gsi, code, op1, NULL, NULL);
}

/* Returns true if GS is a nontemporal move.  */

static inline bool
gimple_assign_nontemporal_move_p (const gassign *gs)
{
  return gs->nontemporal_move;
}

/* Sets nontemporal move flag of GS to NONTEMPORAL.  */

static inline void
gimple_assign_set_nontemporal_move (gimple *gs, bool nontemporal)
{
  GIMPLE_CHECK (gs, GIMPLE_ASSIGN);
  gs->nontemporal_move = nontemporal;
}


/* Return the code of the expression computed on the rhs of assignment
   statement GS.  In case that the RHS is a single object, returns the
   tree code of the object.  */

static inline enum tree_code
gimple_assign_rhs_code (const gassign *gs)
{
  enum tree_code code = (enum tree_code) gs->subcode;
  /* While we initially set subcode to the TREE_CODE of the rhs for
     GIMPLE_SINGLE_RHS assigns we do not update that subcode to stay
     in sync when we rewrite stmts into SSA form or do SSA propagations.  */
  if (get_gimple_rhs_class (code) == GIMPLE_SINGLE_RHS)
    code = TREE_CODE (gs->op[1]);

  return code;
}

static inline enum tree_code
gimple_assign_rhs_code (const gimple *gs)
{
  const gassign *ass = GIMPLE_CHECK2<const gassign *> (gs);
  return gimple_assign_rhs_code (ass);
}


/* Set CODE to be the code for the expression computed on the RHS of
   assignment S.  */

static inline void
gimple_assign_set_rhs_code (gimple *s, enum tree_code code)
{
  GIMPLE_CHECK (s, GIMPLE_ASSIGN);
  s->subcode = code;
}


/* Return the gimple rhs class of the code of the expression computed on
   the rhs of assignment statement GS.
   This will never return GIMPLE_INVALID_RHS.  */

static inline enum gimple_rhs_class
gimple_assign_rhs_class (const gimple *gs)
{
  return get_gimple_rhs_class (gimple_assign_rhs_code (gs));
}

/* Return true if GS is an assignment with a singleton RHS, i.e.,
   there is no operator associated with the assignment itself.
   Unlike gimple_assign_copy_p, this predicate returns true for
   any RHS operand, including those that perform an operation
   and do not have the semantics of a copy, such as COND_EXPR.  */

static inline bool
gimple_assign_single_p (const gimple *gs)
{
  return (is_gimple_assign (gs)
          && gimple_assign_rhs_class (gs) == GIMPLE_SINGLE_RHS);
}

/* Return true if GS performs a store to its lhs.  */

static inline bool
gimple_store_p (const gimple *gs)
{
  tree lhs = gimple_get_lhs (gs);
  return lhs && !is_gimple_reg (lhs);
}

/* Return true if GS is an assignment that loads from its rhs1.  */

static inline bool
gimple_assign_load_p (const gimple *gs)
{
  tree rhs;
  if (!gimple_assign_single_p (gs))
    return false;
  rhs = gimple_assign_rhs1 (gs);
  if (TREE_CODE (rhs) == WITH_SIZE_EXPR)
    return true;
  rhs = get_base_address (rhs);
  return (DECL_P (rhs)
	  || TREE_CODE (rhs) == MEM_REF || TREE_CODE (rhs) == TARGET_MEM_REF);
}


/* Return true if S is a type-cast assignment.  */

static inline bool
gimple_assign_cast_p (const gimple *s)
{
  if (is_gimple_assign (s))
    {
      enum tree_code sc = gimple_assign_rhs_code (s);
      return CONVERT_EXPR_CODE_P (sc)
	     || sc == VIEW_CONVERT_EXPR
	     || sc == FIX_TRUNC_EXPR;
    }

  return false;
}

/* Return true if S is a clobber statement.  */

static inline bool
gimple_clobber_p (const gimple *s)
{
  return gimple_assign_single_p (s)
         && TREE_CLOBBER_P (gimple_assign_rhs1 (s));
}

/* Return true if GS is a GIMPLE_CALL.  */

static inline bool
is_gimple_call (const gimple *gs)
{
  return gimple_code (gs) == GIMPLE_CALL;
}

/* Return the LHS of call statement GS.  */

static inline tree
gimple_call_lhs (const gcall *gs)
{
  return gs->op[0];
}

static inline tree
gimple_call_lhs (const gimple *gs)
{
  const gcall *gc = GIMPLE_CHECK2<const gcall *> (gs);
  return gimple_call_lhs (gc);
}


/* Return a pointer to the LHS of call statement GS.  */

static inline tree *
gimple_call_lhs_ptr (gcall *gs)
{
  return &gs->op[0];
}

static inline tree *
gimple_call_lhs_ptr (gimple *gs)
{
  gcall *gc = GIMPLE_CHECK2<gcall *> (gs);
  return gimple_call_lhs_ptr (gc);
}


/* Set LHS to be the LHS operand of call statement GS.  */

static inline void
gimple_call_set_lhs (gcall *gs, tree lhs)
{
  gs->op[0] = lhs;
  if (lhs && TREE_CODE (lhs) == SSA_NAME)
    SSA_NAME_DEF_STMT (lhs) = gs;
}

static inline void
gimple_call_set_lhs (gimple *gs, tree lhs)
{
  gcall *gc = GIMPLE_CHECK2<gcall *> (gs);
  gimple_call_set_lhs (gc, lhs);
}


/* Return true if call GS calls an internal-only function, as enumerated
   by internal_fn.  */

static inline bool
gimple_call_internal_p (const gcall *gs)
{
  return (gs->subcode & GF_CALL_INTERNAL) != 0;
}

static inline bool
gimple_call_internal_p (const gimple *gs)
{
  const gcall *gc = GIMPLE_CHECK2<const gcall *> (gs);
  return gimple_call_internal_p (gc);
}


/* Return true if call GS is marked as instrumented by
   Pointer Bounds Checker.  */

static inline bool
gimple_call_with_bounds_p (const gcall *gs)
{
  return (gs->subcode & GF_CALL_WITH_BOUNDS) != 0;
}

static inline bool
gimple_call_with_bounds_p (const gimple *gs)
{
  const gcall *gc = GIMPLE_CHECK2<const gcall *> (gs);
  return gimple_call_with_bounds_p (gc);
}


/* If INSTRUMENTED_P is true, marm statement GS as instrumented by
   Pointer Bounds Checker.  */

static inline void
gimple_call_set_with_bounds (gcall *gs, bool with_bounds)
{
  if (with_bounds)
    gs->subcode |= GF_CALL_WITH_BOUNDS;
  else
    gs->subcode &= ~GF_CALL_WITH_BOUNDS;
}

static inline void
gimple_call_set_with_bounds (gimple *gs, bool with_bounds)
{
  gcall *gc = GIMPLE_CHECK2<gcall *> (gs);
  gimple_call_set_with_bounds (gc, with_bounds);
}


/* Return true if call GS is marked as nocf_check.  */

static inline bool
gimple_call_nocf_check_p (const gcall *gs)
{
  return (gs->subcode & GF_CALL_NOCF_CHECK) != 0;
}

/* Mark statement GS as nocf_check call.  */

static inline void
gimple_call_set_nocf_check (gcall *gs, bool nocf_check)
{
  if (nocf_check)
    gs->subcode |= GF_CALL_NOCF_CHECK;
  else
    gs->subcode &= ~GF_CALL_NOCF_CHECK;
}

/* Return the target of internal call GS.  */

static inline enum internal_fn
gimple_call_internal_fn (const gcall *gs)
{
  gcc_gimple_checking_assert (gimple_call_internal_p (gs));
  return gs->u.internal_fn;
}

static inline enum internal_fn
gimple_call_internal_fn (const gimple *gs)
{
  const gcall *gc = GIMPLE_CHECK2<const gcall *> (gs);
  return gimple_call_internal_fn (gc);
}

/* Return true, if this internal gimple call is unique.  */

static inline bool
gimple_call_internal_unique_p (const gcall *gs)
{
  return gimple_call_internal_fn (gs) == IFN_UNIQUE;
}

static inline bool
gimple_call_internal_unique_p (const gimple *gs)
{
  const gcall *gc = GIMPLE_CHECK2<const gcall *> (gs);
  return gimple_call_internal_unique_p (gc);
}

/* Return true if GS is an internal function FN.  */

static inline bool
gimple_call_internal_p (const gimple *gs, internal_fn fn)
{
  return (is_gimple_call (gs)
	  && gimple_call_internal_p (gs)
	  && gimple_call_internal_fn (gs) == fn);
}

/* If CTRL_ALTERING_P is true, mark GIMPLE_CALL S to be a stmt
   that could alter control flow.  */

static inline void
gimple_call_set_ctrl_altering (gcall *s, bool ctrl_altering_p)
{
  if (ctrl_altering_p)
    s->subcode |= GF_CALL_CTRL_ALTERING;
  else
    s->subcode &= ~GF_CALL_CTRL_ALTERING;
}

static inline void
gimple_call_set_ctrl_altering (gimple *s, bool ctrl_altering_p)
{
  gcall *gc = GIMPLE_CHECK2<gcall *> (s);
  gimple_call_set_ctrl_altering (gc, ctrl_altering_p);
}

/* Return true if call GS calls an func whose GF_CALL_CTRL_ALTERING
   flag is set. Such call could not be a stmt in the middle of a bb.  */

static inline bool
gimple_call_ctrl_altering_p (const gcall *gs)
{
  return (gs->subcode & GF_CALL_CTRL_ALTERING) != 0;
}

static inline bool
gimple_call_ctrl_altering_p (const gimple *gs)
{
  const gcall *gc = GIMPLE_CHECK2<const gcall *> (gs);
  return gimple_call_ctrl_altering_p (gc);
}


/* Return the function type of the function called by GS.  */

static inline tree
gimple_call_fntype (const gcall *gs)
{
  if (gimple_call_internal_p (gs))
    return NULL_TREE;
  return gs->u.fntype;
}

static inline tree
gimple_call_fntype (const gimple *gs)
{
  const gcall *call_stmt = GIMPLE_CHECK2<const gcall *> (gs);
  return gimple_call_fntype (call_stmt);
}

/* Set the type of the function called by CALL_STMT to FNTYPE.  */

static inline void
gimple_call_set_fntype (gcall *call_stmt, tree fntype)
{
  gcc_gimple_checking_assert (!gimple_call_internal_p (call_stmt));
  call_stmt->u.fntype = fntype;
}


/* Return the tree node representing the function called by call
   statement GS.  */

static inline tree
gimple_call_fn (const gcall *gs)
{
  return gs->op[1];
}

static inline tree
gimple_call_fn (const gimple *gs)
{
  const gcall *gc = GIMPLE_CHECK2<const gcall *> (gs);
  return gimple_call_fn (gc);
}

/* Return a pointer to the tree node representing the function called by call
   statement GS.  */

static inline tree *
gimple_call_fn_ptr (gcall *gs)
{
  return &gs->op[1];
}

static inline tree *
gimple_call_fn_ptr (gimple *gs)
{
  gcall *gc = GIMPLE_CHECK2<gcall *> (gs);
  return gimple_call_fn_ptr (gc);
}


/* Set FN to be the function called by call statement GS.  */

static inline void
gimple_call_set_fn (gcall *gs, tree fn)
{
  gcc_gimple_checking_assert (!gimple_call_internal_p (gs));
  gs->op[1] = fn;
}


/* Set FNDECL to be the function called by call statement GS.  */

static inline void
gimple_call_set_fndecl (gcall *gs, tree decl)
{
  gcc_gimple_checking_assert (!gimple_call_internal_p (gs));
  gs->op[1] = build1_loc (gimple_location (gs), ADDR_EXPR,
			  build_pointer_type (TREE_TYPE (decl)), decl);
}

static inline void
gimple_call_set_fndecl (gimple *gs, tree decl)
{
  gcall *gc = GIMPLE_CHECK2<gcall *> (gs);
  gimple_call_set_fndecl (gc, decl);
}


/* Set internal function FN to be the function called by call statement CALL_STMT.  */

static inline void
gimple_call_set_internal_fn (gcall *call_stmt, enum internal_fn fn)
{
  gcc_gimple_checking_assert (gimple_call_internal_p (call_stmt));
  call_stmt->u.internal_fn = fn;
}


/* If a given GIMPLE_CALL's callee is a FUNCTION_DECL, return it.
   Otherwise return NULL.  This function is analogous to
   get_callee_fndecl in tree land.  */

static inline tree
gimple_call_fndecl (const gcall *gs)
{
  return gimple_call_addr_fndecl (gimple_call_fn (gs));
}

static inline tree
gimple_call_fndecl (const gimple *gs)
{
  const gcall *gc = GIMPLE_CHECK2<const gcall *> (gs);
  return gimple_call_fndecl (gc);
}


/* Return the type returned by call statement GS.  */

static inline tree
gimple_call_return_type (const gcall *gs)
{
  tree type = gimple_call_fntype (gs);

  if (type == NULL_TREE)
    return TREE_TYPE (gimple_call_lhs (gs));

  /* The type returned by a function is the type of its
     function type.  */
  return TREE_TYPE (type);
}


/* Return the static chain for call statement GS.  */

static inline tree
gimple_call_chain (const gcall *gs)
{
  return gs->op[2];
}

static inline tree
gimple_call_chain (const gimple *gs)
{
  const gcall *gc = GIMPLE_CHECK2<const gcall *> (gs);
  return gimple_call_chain (gc);
}


/* Return a pointer to the static chain for call statement CALL_STMT.  */

static inline tree *
gimple_call_chain_ptr (gcall *call_stmt)
{
  return &call_stmt->op[2];
}

/* Set CHAIN to be the static chain for call statement CALL_STMT.  */

static inline void
gimple_call_set_chain (gcall *call_stmt, tree chain)
{
  call_stmt->op[2] = chain;
}


/* Return the number of arguments used by call statement GS.  */

static inline unsigned
gimple_call_num_args (const gcall *gs)
{
  return gimple_num_ops (gs) - 3;
}

static inline unsigned
gimple_call_num_args (const gimple *gs)
{
  const gcall *gc = GIMPLE_CHECK2<const gcall *> (gs);
  return gimple_call_num_args (gc);
}


/* Return the argument at position INDEX for call statement GS.  */

static inline tree
gimple_call_arg (const gcall *gs, unsigned index)
{
  gcc_gimple_checking_assert (gimple_num_ops (gs) > index + 3);
  return gs->op[index + 3];
}

static inline tree
gimple_call_arg (const gimple *gs, unsigned index)
{
  const gcall *gc = GIMPLE_CHECK2<const gcall *> (gs);
  return gimple_call_arg (gc, index);
}


/* Return a pointer to the argument at position INDEX for call
   statement GS.  */

static inline tree *
gimple_call_arg_ptr (gcall *gs, unsigned index)
{
  gcc_gimple_checking_assert (gimple_num_ops (gs) > index + 3);
  return &gs->op[index + 3];
}

static inline tree *
gimple_call_arg_ptr (gimple *gs, unsigned index)
{
  gcall *gc = GIMPLE_CHECK2<gcall *> (gs);
  return gimple_call_arg_ptr (gc, index);
}


/* Set ARG to be the argument at position INDEX for call statement GS.  */

static inline void
gimple_call_set_arg (gcall *gs, unsigned index, tree arg)
{
  gcc_gimple_checking_assert (gimple_num_ops (gs) > index + 3);
  gs->op[index + 3] = arg;
}

static inline void
gimple_call_set_arg (gimple *gs, unsigned index, tree arg)
{
  gcall *gc = GIMPLE_CHECK2<gcall *> (gs);
  gimple_call_set_arg (gc, index, arg);
}


/* If TAIL_P is true, mark call statement S as being a tail call
   (i.e., a call just before the exit of a function).  These calls are
   candidate for tail call optimization.  */

static inline void
gimple_call_set_tail (gcall *s, bool tail_p)
{
  if (tail_p)
    s->subcode |= GF_CALL_TAILCALL;
  else
    s->subcode &= ~GF_CALL_TAILCALL;
}


/* Return true if GIMPLE_CALL S is marked as a tail call.  */

static inline bool
gimple_call_tail_p (gcall *s)
{
  return (s->subcode & GF_CALL_TAILCALL) != 0;
}

/* Mark (or clear) call statement S as requiring tail call optimization.  */

static inline void
gimple_call_set_must_tail (gcall *s, bool must_tail_p)
{
  if (must_tail_p)
    s->subcode |= GF_CALL_MUST_TAIL_CALL;
  else
    s->subcode &= ~GF_CALL_MUST_TAIL_CALL;
}

/* Return true if call statement has been marked as requiring
   tail call optimization.  */

static inline bool
gimple_call_must_tail_p (const gcall *s)
{
  return (s->subcode & GF_CALL_MUST_TAIL_CALL) != 0;
}

/* If RETURN_SLOT_OPT_P is true mark GIMPLE_CALL S as valid for return
   slot optimization.  This transformation uses the target of the call
   expansion as the return slot for calls that return in memory.  */

static inline void
gimple_call_set_return_slot_opt (gcall *s, bool return_slot_opt_p)
{
  if (return_slot_opt_p)
    s->subcode |= GF_CALL_RETURN_SLOT_OPT;
  else
    s->subcode &= ~GF_CALL_RETURN_SLOT_OPT;
}


/* Return true if S is marked for return slot optimization.  */

static inline bool
gimple_call_return_slot_opt_p (gcall *s)
{
  return (s->subcode & GF_CALL_RETURN_SLOT_OPT) != 0;
}


/* If FROM_THUNK_P is true, mark GIMPLE_CALL S as being the jump from a
   thunk to the thunked-to function.  */

static inline void
gimple_call_set_from_thunk (gcall *s, bool from_thunk_p)
{
  if (from_thunk_p)
    s->subcode |= GF_CALL_FROM_THUNK;
  else
    s->subcode &= ~GF_CALL_FROM_THUNK;
}


/* Return true if GIMPLE_CALL S is a jump from a thunk.  */

static inline bool
gimple_call_from_thunk_p (gcall *s)
{
  return (s->subcode & GF_CALL_FROM_THUNK) != 0;
}


/* If PASS_ARG_PACK_P is true, GIMPLE_CALL S is a stdarg call that needs the
   argument pack in its argument list.  */

static inline void
gimple_call_set_va_arg_pack (gcall *s, bool pass_arg_pack_p)
{
  if (pass_arg_pack_p)
    s->subcode |= GF_CALL_VA_ARG_PACK;
  else
    s->subcode &= ~GF_CALL_VA_ARG_PACK;
}


/* Return true if GIMPLE_CALL S is a stdarg call that needs the
   argument pack in its argument list.  */

static inline bool
gimple_call_va_arg_pack_p (gcall *s)
{
  return (s->subcode & GF_CALL_VA_ARG_PACK) != 0;
}


/* Return true if S is a noreturn call.  */

static inline bool
gimple_call_noreturn_p (const gcall *s)
{
  return (gimple_call_flags (s) & ECF_NORETURN) != 0;
}

static inline bool
gimple_call_noreturn_p (const gimple *s)
{
  const gcall *gc = GIMPLE_CHECK2<const gcall *> (s);
  return gimple_call_noreturn_p (gc);
}


/* If NOTHROW_P is true, GIMPLE_CALL S is a call that is known to not throw
   even if the called function can throw in other cases.  */

static inline void
gimple_call_set_nothrow (gcall *s, bool nothrow_p)
{
  if (nothrow_p)
    s->subcode |= GF_CALL_NOTHROW;
  else
    s->subcode &= ~GF_CALL_NOTHROW;
}

/* Return true if S is a nothrow call.  */

static inline bool
gimple_call_nothrow_p (gcall *s)
{
  return (gimple_call_flags (s) & ECF_NOTHROW) != 0;
}

/* If FOR_VAR is true, GIMPLE_CALL S is a call to builtin_alloca that
   is known to be emitted for VLA objects.  Those are wrapped by
   stack_save/stack_restore calls and hence can't lead to unbounded
   stack growth even when they occur in loops.  */

static inline void
gimple_call_set_alloca_for_var (gcall *s, bool for_var)
{
  if (for_var)
    s->subcode |= GF_CALL_ALLOCA_FOR_VAR;
  else
    s->subcode &= ~GF_CALL_ALLOCA_FOR_VAR;
}

/* Return true of S is a call to builtin_alloca emitted for VLA objects.  */

static inline bool
gimple_call_alloca_for_var_p (gcall *s)
{
  return (s->subcode & GF_CALL_ALLOCA_FOR_VAR) != 0;
}

/* If BY_DESCRIPTOR_P is true, GIMPLE_CALL S is an indirect call for which
   pointers to nested function are descriptors instead of trampolines.  */

static inline void
gimple_call_set_by_descriptor (gcall  *s, bool by_descriptor_p)
{
  if (by_descriptor_p)
    s->subcode |= GF_CALL_BY_DESCRIPTOR;
  else
    s->subcode &= ~GF_CALL_BY_DESCRIPTOR;
}

/* Return true if S is a by-descriptor call.  */

static inline bool
gimple_call_by_descriptor_p (gcall *s)
{
  return (s->subcode & GF_CALL_BY_DESCRIPTOR) != 0;
}

/* Copy all the GF_CALL_* flags from ORIG_CALL to DEST_CALL.  */

static inline void
gimple_call_copy_flags (gcall *dest_call, gcall *orig_call)
{
  dest_call->subcode = orig_call->subcode;
}


/* Return a pointer to the points-to solution for the set of call-used
   variables of the call CALL_STMT.  */

static inline struct pt_solution *
gimple_call_use_set (gcall *call_stmt)
{
  return &call_stmt->call_used;
}


/* Return a pointer to the points-to solution for the set of call-used
   variables of the call CALL_STMT.  */

static inline struct pt_solution *
gimple_call_clobber_set (gcall *call_stmt)
{
  return &call_stmt->call_clobbered;
}


/* Returns true if this is a GIMPLE_ASSIGN or a GIMPLE_CALL with a
   non-NULL lhs.  */

static inline bool
gimple_has_lhs (gimple *stmt)
{
  if (is_gimple_assign (stmt))
    return true;
  if (gcall *call = dyn_cast <gcall *> (stmt))
    return gimple_call_lhs (call) != NULL_TREE;
  return false;
}


/* Return the code of the predicate computed by conditional statement GS.  */

static inline enum tree_code
gimple_cond_code (const gcond *gs)
{
  return (enum tree_code) gs->subcode;
}

static inline enum tree_code
gimple_cond_code (const gimple *gs)
{
  const gcond *gc = GIMPLE_CHECK2<const gcond *> (gs);
  return gimple_cond_code (gc);
}


/* Set CODE to be the predicate code for the conditional statement GS.  */

static inline void
gimple_cond_set_code (gcond *gs, enum tree_code code)
{
  gs->subcode = code;
}


/* Return the LHS of the predicate computed by conditional statement GS.  */

static inline tree
gimple_cond_lhs (const gcond *gs)
{
  return gs->op[0];
}

static inline tree
gimple_cond_lhs (const gimple *gs)
{
  const gcond *gc = GIMPLE_CHECK2<const gcond *> (gs);
  return gimple_cond_lhs (gc);
}

/* Return the pointer to the LHS of the predicate computed by conditional
   statement GS.  */

static inline tree *
gimple_cond_lhs_ptr (gcond *gs)
{
  return &gs->op[0];
}

/* Set LHS to be the LHS operand of the predicate computed by
   conditional statement GS.  */

static inline void
gimple_cond_set_lhs (gcond *gs, tree lhs)
{
  gs->op[0] = lhs;
}


/* Return the RHS operand of the predicate computed by conditional GS.  */

static inline tree
gimple_cond_rhs (const gcond *gs)
{
  return gs->op[1];
}

static inline tree
gimple_cond_rhs (const gimple *gs)
{
  const gcond *gc = GIMPLE_CHECK2<const gcond *> (gs);
  return gimple_cond_rhs (gc);
}

/* Return the pointer to the RHS operand of the predicate computed by
   conditional GS.  */

static inline tree *
gimple_cond_rhs_ptr (gcond *gs)
{
  return &gs->op[1];
}


/* Set RHS to be the RHS operand of the predicate computed by
   conditional statement GS.  */

static inline void
gimple_cond_set_rhs (gcond *gs, tree rhs)
{
  gs->op[1] = rhs;
}


/* Return the label used by conditional statement GS when its
   predicate evaluates to true.  */

static inline tree
gimple_cond_true_label (const gcond *gs)
{
  return gs->op[2];
}


/* Set LABEL to be the label used by conditional statement GS when its
   predicate evaluates to true.  */

static inline void
gimple_cond_set_true_label (gcond *gs, tree label)
{
  gs->op[2] = label;
}


/* Set LABEL to be the label used by conditional statement GS when its
   predicate evaluates to false.  */

static inline void
gimple_cond_set_false_label (gcond *gs, tree label)
{
  gs->op[3] = label;
}


/* Return the label used by conditional statement GS when its
   predicate evaluates to false.  */

static inline tree
gimple_cond_false_label (const gcond *gs)
{
  return gs->op[3];
}


/* Set the conditional COND_STMT to be of the form 'if (1 == 0)'.  */

static inline void
gimple_cond_make_false (gcond *gs)
{
  gimple_cond_set_lhs (gs, boolean_false_node);
  gimple_cond_set_rhs (gs, boolean_false_node);
  gs->subcode = NE_EXPR;
}


/* Set the conditional COND_STMT to be of the form 'if (1 == 1)'.  */

static inline void
gimple_cond_make_true (gcond *gs)
{
  gimple_cond_set_lhs (gs, boolean_true_node);
  gimple_cond_set_rhs (gs, boolean_false_node);
  gs->subcode = NE_EXPR;
}

/* Check if conditional statemente GS is of the form 'if (1 == 1)',
  'if (0 == 0)', 'if (1 != 0)' or 'if (0 != 1)' */

static inline bool
gimple_cond_true_p (const gcond *gs)
{
  tree lhs = gimple_cond_lhs (gs);
  tree rhs = gimple_cond_rhs (gs);
  enum tree_code code = gimple_cond_code (gs);

  if (lhs != boolean_true_node && lhs != boolean_false_node)
    return false;

  if (rhs != boolean_true_node && rhs != boolean_false_node)
    return false;

  if (code == NE_EXPR && lhs != rhs)
    return true;

  if (code == EQ_EXPR && lhs == rhs)
      return true;

  return false;
}

/* Check if conditional statement GS is of the form 'if (1 != 1)',
   'if (0 != 0)', 'if (1 == 0)' or 'if (0 == 1)' */

static inline bool
gimple_cond_false_p (const gcond *gs)
{
  tree lhs = gimple_cond_lhs (gs);
  tree rhs = gimple_cond_rhs (gs);
  enum tree_code code = gimple_cond_code (gs);

  if (lhs != boolean_true_node && lhs != boolean_false_node)
    return false;

  if (rhs != boolean_true_node && rhs != boolean_false_node)
    return false;

  if (code == NE_EXPR && lhs == rhs)
    return true;

  if (code == EQ_EXPR && lhs != rhs)
      return true;

  return false;
}

/* Set the code, LHS and RHS of GIMPLE_COND STMT from CODE, LHS and RHS.  */

static inline void
gimple_cond_set_condition (gcond *stmt, enum tree_code code, tree lhs,
			   tree rhs)
{
  gimple_cond_set_code (stmt, code);
  gimple_cond_set_lhs (stmt, lhs);
  gimple_cond_set_rhs (stmt, rhs);
}

/* Return the LABEL_DECL node used by GIMPLE_LABEL statement GS.  */

static inline tree
gimple_label_label (const glabel *gs)
{
  return gs->op[0];
}


/* Set LABEL to be the LABEL_DECL node used by GIMPLE_LABEL statement
   GS.  */

static inline void
gimple_label_set_label (glabel *gs, tree label)
{
  gs->op[0] = label;
}


/* Return the destination of the unconditional jump GS.  */

static inline tree
gimple_goto_dest (const gimple *gs)
{
  GIMPLE_CHECK (gs, GIMPLE_GOTO);
  return gimple_op (gs, 0);
}


/* Set DEST to be the destination of the unconditonal jump GS.  */

static inline void
gimple_goto_set_dest (ggoto *gs, tree dest)
{
  gs->op[0] = dest;
}


/* Return the variables declared in the GIMPLE_BIND statement GS.  */

static inline tree
gimple_bind_vars (const gbind *bind_stmt)
{
  return bind_stmt->vars;
}


/* Set VARS to be the set of variables declared in the GIMPLE_BIND
   statement GS.  */

static inline void
gimple_bind_set_vars (gbind *bind_stmt, tree vars)
{
  bind_stmt->vars = vars;
}


/* Append VARS to the set of variables declared in the GIMPLE_BIND
   statement GS.  */

static inline void
gimple_bind_append_vars (gbind *bind_stmt, tree vars)
{
  bind_stmt->vars = chainon (bind_stmt->vars, vars);
}


static inline gimple_seq *
gimple_bind_body_ptr (gbind *bind_stmt)
{
  return &bind_stmt->body;
}

/* Return the GIMPLE sequence contained in the GIMPLE_BIND statement GS.  */

static inline gimple_seq
gimple_bind_body (gbind *gs)
{
  return *gimple_bind_body_ptr (gs);
}


/* Set SEQ to be the GIMPLE sequence contained in the GIMPLE_BIND
   statement GS.  */

static inline void
gimple_bind_set_body (gbind *bind_stmt, gimple_seq seq)
{
  bind_stmt->body = seq;
}


/* Append a statement to the end of a GIMPLE_BIND's body.  */

static inline void
gimple_bind_add_stmt (gbind *bind_stmt, gimple *stmt)
{
  gimple_seq_add_stmt (&bind_stmt->body, stmt);
}


/* Append a sequence of statements to the end of a GIMPLE_BIND's body.  */

static inline void
gimple_bind_add_seq (gbind *bind_stmt, gimple_seq seq)
{
  gimple_seq_add_seq (&bind_stmt->body, seq);
}


/* Return the TREE_BLOCK node associated with GIMPLE_BIND statement
   GS.  This is analogous to the BIND_EXPR_BLOCK field in trees.  */

static inline tree
gimple_bind_block (const gbind *bind_stmt)
{
  return bind_stmt->block;
}


/* Set BLOCK to be the TREE_BLOCK node associated with GIMPLE_BIND
   statement GS.  */

static inline void
gimple_bind_set_block (gbind *bind_stmt, tree block)
{
  gcc_gimple_checking_assert (block == NULL_TREE
			      || TREE_CODE (block) == BLOCK);
  bind_stmt->block = block;
}


/* Return the number of input operands for GIMPLE_ASM ASM_STMT.  */

static inline unsigned
gimple_asm_ninputs (const gasm *asm_stmt)
{
  return asm_stmt->ni;
}


/* Return the number of output operands for GIMPLE_ASM ASM_STMT.  */

static inline unsigned
gimple_asm_noutputs (const gasm *asm_stmt)
{
  return asm_stmt->no;
}


/* Return the number of clobber operands for GIMPLE_ASM ASM_STMT.  */

static inline unsigned
gimple_asm_nclobbers (const gasm *asm_stmt)
{
  return asm_stmt->nc;
}

/* Return the number of label operands for GIMPLE_ASM ASM_STMT.  */

static inline unsigned
gimple_asm_nlabels (const gasm *asm_stmt)
{
  return asm_stmt->nl;
}

/* Return input operand INDEX of GIMPLE_ASM ASM_STMT.  */

static inline tree
gimple_asm_input_op (const gasm *asm_stmt, unsigned index)
{
  gcc_gimple_checking_assert (index < asm_stmt->ni);
  return asm_stmt->op[index + asm_stmt->no];
}

/* Set IN_OP to be input operand INDEX in GIMPLE_ASM ASM_STMT.  */

static inline void
gimple_asm_set_input_op (gasm *asm_stmt, unsigned index, tree in_op)
{
  gcc_gimple_checking_assert (index < asm_stmt->ni
			      && TREE_CODE (in_op) == TREE_LIST);
  asm_stmt->op[index + asm_stmt->no] = in_op;
}


/* Return output operand INDEX of GIMPLE_ASM ASM_STMT.  */

static inline tree
gimple_asm_output_op (const gasm *asm_stmt, unsigned index)
{
  gcc_gimple_checking_assert (index < asm_stmt->no);
  return asm_stmt->op[index];
}

/* Set OUT_OP to be output operand INDEX in GIMPLE_ASM ASM_STMT.  */

static inline void
gimple_asm_set_output_op (gasm *asm_stmt, unsigned index, tree out_op)
{
  gcc_gimple_checking_assert (index < asm_stmt->no
			      && TREE_CODE (out_op) == TREE_LIST);
  asm_stmt->op[index] = out_op;
}


/* Return clobber operand INDEX of GIMPLE_ASM ASM_STMT.  */

static inline tree
gimple_asm_clobber_op (const gasm *asm_stmt, unsigned index)
{
  gcc_gimple_checking_assert (index < asm_stmt->nc);
  return asm_stmt->op[index + asm_stmt->ni + asm_stmt->no];
}


/* Set CLOBBER_OP to be clobber operand INDEX in GIMPLE_ASM ASM_STMT.  */

static inline void
gimple_asm_set_clobber_op (gasm *asm_stmt, unsigned index, tree clobber_op)
{
  gcc_gimple_checking_assert (index < asm_stmt->nc
			      && TREE_CODE (clobber_op) == TREE_LIST);
  asm_stmt->op[index + asm_stmt->ni + asm_stmt->no] = clobber_op;
}

/* Return label operand INDEX of GIMPLE_ASM ASM_STMT.  */

static inline tree
gimple_asm_label_op (const gasm *asm_stmt, unsigned index)
{
  gcc_gimple_checking_assert (index < asm_stmt->nl);
  return asm_stmt->op[index + asm_stmt->ni + asm_stmt->nc];
}

/* Set LABEL_OP to be label operand INDEX in GIMPLE_ASM ASM_STMT.  */

static inline void
gimple_asm_set_label_op (gasm *asm_stmt, unsigned index, tree label_op)
{
  gcc_gimple_checking_assert (index < asm_stmt->nl
			      && TREE_CODE (label_op) == TREE_LIST);
  asm_stmt->op[index + asm_stmt->ni + asm_stmt->nc] = label_op;
}

/* Return the string representing the assembly instruction in
   GIMPLE_ASM ASM_STMT.  */

static inline const char *
gimple_asm_string (const gasm *asm_stmt)
{
  return asm_stmt->string;
}


/* Return true if ASM_STMT is marked volatile.  */

static inline bool
gimple_asm_volatile_p (const gasm *asm_stmt)
{
  return (asm_stmt->subcode & GF_ASM_VOLATILE) != 0;
}


/* If VOLATILE_P is true, mark asm statement ASM_STMT as volatile.  */

static inline void
gimple_asm_set_volatile (gasm *asm_stmt, bool volatile_p)
{
  if (volatile_p)
    asm_stmt->subcode |= GF_ASM_VOLATILE;
  else
    asm_stmt->subcode &= ~GF_ASM_VOLATILE;
}


/* Return true if ASM_STMT is marked inline.  */

static inline bool
gimple_asm_inline_p (const gasm *asm_stmt)
{
  return (asm_stmt->subcode & GF_ASM_INLINE) != 0;
}


/* If INLINE_P is true, mark asm statement ASM_STMT as inline.  */

static inline void
gimple_asm_set_inline (gasm *asm_stmt, bool inline_p)
{
  if (inline_p)
    asm_stmt->subcode |= GF_ASM_INLINE;
  else
    asm_stmt->subcode &= ~GF_ASM_INLINE;
}


/* If INPUT_P is true, mark asm ASM_STMT as an ASM_INPUT.  */

static inline void
gimple_asm_set_input (gasm *asm_stmt, bool input_p)
{
  if (input_p)
    asm_stmt->subcode |= GF_ASM_INPUT;
  else
    asm_stmt->subcode &= ~GF_ASM_INPUT;
}


/* Return true if asm ASM_STMT is an ASM_INPUT.  */

static inline bool
gimple_asm_input_p (const gasm *asm_stmt)
{
  return (asm_stmt->subcode & GF_ASM_INPUT) != 0;
}


/* Return the types handled by GIMPLE_CATCH statement CATCH_STMT.  */

static inline tree
gimple_catch_types (const gcatch *catch_stmt)
{
  return catch_stmt->types;
}


/* Return a pointer to the types handled by GIMPLE_CATCH statement CATCH_STMT.  */

static inline tree *
gimple_catch_types_ptr (gcatch *catch_stmt)
{
  return &catch_stmt->types;
}


/* Return a pointer to the GIMPLE sequence representing the body of
   the handler of GIMPLE_CATCH statement CATCH_STMT.  */

static inline gimple_seq *
gimple_catch_handler_ptr (gcatch *catch_stmt)
{
  return &catch_stmt->handler;
}


/* Return the GIMPLE sequence representing the body of the handler of
   GIMPLE_CATCH statement CATCH_STMT.  */

static inline gimple_seq
gimple_catch_handler (gcatch *catch_stmt)
{
  return *gimple_catch_handler_ptr (catch_stmt);
}


/* Set T to be the set of types handled by GIMPLE_CATCH CATCH_STMT.  */

static inline void
gimple_catch_set_types (gcatch *catch_stmt, tree t)
{
  catch_stmt->types = t;
}


/* Set HANDLER to be the body of GIMPLE_CATCH CATCH_STMT.  */

static inline void
gimple_catch_set_handler (gcatch *catch_stmt, gimple_seq handler)
{
  catch_stmt->handler = handler;
}


/* Return the types handled by GIMPLE_EH_FILTER statement GS.  */

static inline tree
gimple_eh_filter_types (const gimple *gs)
{
  const geh_filter *eh_filter_stmt = as_a <const geh_filter *> (gs);
  return eh_filter_stmt->types;
}


/* Return a pointer to the types handled by GIMPLE_EH_FILTER statement
   GS.  */

static inline tree *
gimple_eh_filter_types_ptr (gimple *gs)
{
  geh_filter *eh_filter_stmt = as_a <geh_filter *> (gs);
  return &eh_filter_stmt->types;
}


/* Return a pointer to the sequence of statement to execute when
   GIMPLE_EH_FILTER statement fails.  */

static inline gimple_seq *
gimple_eh_filter_failure_ptr (gimple *gs)
{
  geh_filter *eh_filter_stmt = as_a <geh_filter *> (gs);
  return &eh_filter_stmt->failure;
}


/* Return the sequence of statement to execute when GIMPLE_EH_FILTER
   statement fails.  */

static inline gimple_seq
gimple_eh_filter_failure (gimple *gs)
{
  return *gimple_eh_filter_failure_ptr (gs);
}


/* Set TYPES to be the set of types handled by GIMPLE_EH_FILTER
   EH_FILTER_STMT.  */

static inline void
gimple_eh_filter_set_types (geh_filter *eh_filter_stmt, tree types)
{
  eh_filter_stmt->types = types;
}


/* Set FAILURE to be the sequence of statements to execute on failure
   for GIMPLE_EH_FILTER EH_FILTER_STMT.  */

static inline void
gimple_eh_filter_set_failure (geh_filter *eh_filter_stmt,
			      gimple_seq failure)
{
  eh_filter_stmt->failure = failure;
}

/* Get the function decl to be called by the MUST_NOT_THROW region.  */

static inline tree
gimple_eh_must_not_throw_fndecl (geh_mnt *eh_mnt_stmt)
{
  return eh_mnt_stmt->fndecl;
}

/* Set the function decl to be called by GS to DECL.  */

static inline void
gimple_eh_must_not_throw_set_fndecl (geh_mnt *eh_mnt_stmt,
				     tree decl)
{
  eh_mnt_stmt->fndecl = decl;
}

/* GIMPLE_EH_ELSE accessors.  */

static inline gimple_seq *
gimple_eh_else_n_body_ptr (geh_else *eh_else_stmt)
{
  return &eh_else_stmt->n_body;
}

static inline gimple_seq
gimple_eh_else_n_body (geh_else *eh_else_stmt)
{
  return *gimple_eh_else_n_body_ptr (eh_else_stmt);
}

static inline gimple_seq *
gimple_eh_else_e_body_ptr (geh_else *eh_else_stmt)
{
  return &eh_else_stmt->e_body;
}

static inline gimple_seq
gimple_eh_else_e_body (geh_else *eh_else_stmt)
{
  return *gimple_eh_else_e_body_ptr (eh_else_stmt);
}

static inline void
gimple_eh_else_set_n_body (geh_else *eh_else_stmt, gimple_seq seq)
{
  eh_else_stmt->n_body = seq;
}

static inline void
gimple_eh_else_set_e_body (geh_else *eh_else_stmt, gimple_seq seq)
{
  eh_else_stmt->e_body = seq;
}

/* GIMPLE_TRY accessors. */

/* Return the kind of try block represented by GIMPLE_TRY GS.  This is
   either GIMPLE_TRY_CATCH or GIMPLE_TRY_FINALLY.  */

static inline enum gimple_try_flags
gimple_try_kind (const gimple *gs)
{
  GIMPLE_CHECK (gs, GIMPLE_TRY);
  return (enum gimple_try_flags) (gs->subcode & GIMPLE_TRY_KIND);
}


/* Set the kind of try block represented by GIMPLE_TRY GS.  */

static inline void
gimple_try_set_kind (gtry *gs, enum gimple_try_flags kind)
{
  gcc_gimple_checking_assert (kind == GIMPLE_TRY_CATCH
			      || kind == GIMPLE_TRY_FINALLY);
  if (gimple_try_kind (gs) != kind)
    gs->subcode = (unsigned int) kind;
}


/* Return the GIMPLE_TRY_CATCH_IS_CLEANUP flag.  */

static inline bool
gimple_try_catch_is_cleanup (const gimple *gs)
{
  gcc_gimple_checking_assert (gimple_try_kind (gs) == GIMPLE_TRY_CATCH);
  return (gs->subcode & GIMPLE_TRY_CATCH_IS_CLEANUP) != 0;
}


/* Return a pointer to the sequence of statements used as the
   body for GIMPLE_TRY GS.  */

static inline gimple_seq *
gimple_try_eval_ptr (gimple *gs)
{
  gtry *try_stmt = as_a <gtry *> (gs);
  return &try_stmt->eval;
}


/* Return the sequence of statements used as the body for GIMPLE_TRY GS.  */

static inline gimple_seq
gimple_try_eval (gimple *gs)
{
  return *gimple_try_eval_ptr (gs);
}


/* Return a pointer to the sequence of statements used as the cleanup body for
   GIMPLE_TRY GS.  */

static inline gimple_seq *
gimple_try_cleanup_ptr (gimple *gs)
{
  gtry *try_stmt = as_a <gtry *> (gs);
  return &try_stmt->cleanup;
}


/* Return the sequence of statements used as the cleanup body for
   GIMPLE_TRY GS.  */

static inline gimple_seq
gimple_try_cleanup (gimple *gs)
{
  return *gimple_try_cleanup_ptr (gs);
}


/* Set the GIMPLE_TRY_CATCH_IS_CLEANUP flag.  */

static inline void
gimple_try_set_catch_is_cleanup (gtry *g, bool catch_is_cleanup)
{
  gcc_gimple_checking_assert (gimple_try_kind (g) == GIMPLE_TRY_CATCH);
  if (catch_is_cleanup)
    g->subcode |= GIMPLE_TRY_CATCH_IS_CLEANUP;
  else
    g->subcode &= ~GIMPLE_TRY_CATCH_IS_CLEANUP;
}


/* Set EVAL to be the sequence of statements to use as the body for
   GIMPLE_TRY TRY_STMT.  */

static inline void
gimple_try_set_eval (gtry *try_stmt, gimple_seq eval)
{
  try_stmt->eval = eval;
}


/* Set CLEANUP to be the sequence of statements to use as the cleanup
   body for GIMPLE_TRY TRY_STMT.  */

static inline void
gimple_try_set_cleanup (gtry *try_stmt, gimple_seq cleanup)
{
  try_stmt->cleanup = cleanup;
}


/* Return a pointer to the cleanup sequence for cleanup statement GS.  */

static inline gimple_seq *
gimple_wce_cleanup_ptr (gimple *gs)
{
  gimple_statement_wce *wce_stmt = as_a <gimple_statement_wce *> (gs);
  return &wce_stmt->cleanup;
}


/* Return the cleanup sequence for cleanup statement GS.  */

static inline gimple_seq
gimple_wce_cleanup (gimple *gs)
{
  return *gimple_wce_cleanup_ptr (gs);
}


/* Set CLEANUP to be the cleanup sequence for GS.  */

static inline void
gimple_wce_set_cleanup (gimple *gs, gimple_seq cleanup)
{
  gimple_statement_wce *wce_stmt = as_a <gimple_statement_wce *> (gs);
  wce_stmt->cleanup = cleanup;
}


/* Return the CLEANUP_EH_ONLY flag for a WCE tuple.  */

static inline bool
gimple_wce_cleanup_eh_only (const gimple *gs)
{
  GIMPLE_CHECK (gs, GIMPLE_WITH_CLEANUP_EXPR);
  return gs->subcode != 0;
}


/* Set the CLEANUP_EH_ONLY flag for a WCE tuple.  */

static inline void
gimple_wce_set_cleanup_eh_only (gimple *gs, bool eh_only_p)
{
  GIMPLE_CHECK (gs, GIMPLE_WITH_CLEANUP_EXPR);
  gs->subcode = (unsigned int) eh_only_p;
}


/* Return the maximum number of arguments supported by GIMPLE_PHI GS.  */

static inline unsigned
gimple_phi_capacity (const gimple *gs)
{
  const gphi *phi_stmt = as_a <const gphi *> (gs);
  return phi_stmt->capacity;
}


/* Return the number of arguments in GIMPLE_PHI GS.  This must always
   be exactly the number of incoming edges for the basic block holding
   GS.  */

static inline unsigned
gimple_phi_num_args (const gimple *gs)
{
  const gphi *phi_stmt = as_a <const gphi *> (gs);
  return phi_stmt->nargs;
}


/* Return the SSA name created by GIMPLE_PHI GS.  */

static inline tree
gimple_phi_result (const gphi *gs)
{
  return gs->result;
}

static inline tree
gimple_phi_result (const gimple *gs)
{
  const gphi *phi_stmt = as_a <const gphi *> (gs);
  return gimple_phi_result (phi_stmt);
}

/* Return a pointer to the SSA name created by GIMPLE_PHI GS.  */

static inline tree *
gimple_phi_result_ptr (gphi *gs)
{
  return &gs->result;
}

static inline tree *
gimple_phi_result_ptr (gimple *gs)
{
  gphi *phi_stmt = as_a <gphi *> (gs);
  return gimple_phi_result_ptr (phi_stmt);
}

/* Set RESULT to be the SSA name created by GIMPLE_PHI PHI.  */

static inline void
gimple_phi_set_result (gphi *phi, tree result)
{
  phi->result = result;
  if (result && TREE_CODE (result) == SSA_NAME)
    SSA_NAME_DEF_STMT (result) = phi;
}


/* Return the PHI argument corresponding to incoming edge INDEX for
   GIMPLE_PHI GS.  */

static inline struct phi_arg_d *
gimple_phi_arg (gphi *gs, unsigned index)
{
  gcc_gimple_checking_assert (index < gs->nargs);
  return &(gs->args[index]);
}

static inline struct phi_arg_d *
gimple_phi_arg (gimple *gs, unsigned index)
{
  gphi *phi_stmt = as_a <gphi *> (gs);
  return gimple_phi_arg (phi_stmt, index);
}

/* Set PHIARG to be the argument corresponding to incoming edge INDEX
   for GIMPLE_PHI PHI.  */

static inline void
gimple_phi_set_arg (gphi *phi, unsigned index, struct phi_arg_d * phiarg)
{
  gcc_gimple_checking_assert (index < phi->nargs);
  phi->args[index] = *phiarg;
}

/* Return the PHI nodes for basic block BB, or NULL if there are no
   PHI nodes.  */

static inline gimple_seq
phi_nodes (const_basic_block bb)
{
  gcc_checking_assert (!(bb->flags & BB_RTL));
  return bb->il.gimple.phi_nodes;
}

/* Return a pointer to the PHI nodes for basic block BB.  */

static inline gimple_seq *
phi_nodes_ptr (basic_block bb)
{
  gcc_checking_assert (!(bb->flags & BB_RTL));
  return &bb->il.gimple.phi_nodes;
}

/* Return the tree operand for argument I of PHI node GS.  */

static inline tree
gimple_phi_arg_def (gphi *gs, size_t index)
{
  return gimple_phi_arg (gs, index)->def;
}

static inline tree
gimple_phi_arg_def (gimple *gs, size_t index)
{
  return gimple_phi_arg (gs, index)->def;
}


/* Return a pointer to the tree operand for argument I of phi node PHI.  */

static inline tree *
gimple_phi_arg_def_ptr (gphi *phi, size_t index)
{
  return &gimple_phi_arg (phi, index)->def;
}

/* Return the edge associated with argument I of phi node PHI.  */

static inline edge
gimple_phi_arg_edge (gphi *phi, size_t i)
{
  return EDGE_PRED (gimple_bb (phi), i);
}

/* Return the source location of gimple argument I of phi node PHI.  */

static inline source_location
gimple_phi_arg_location (gphi *phi, size_t i)
{
  return gimple_phi_arg (phi, i)->locus;
}

/* Return the source location of the argument on edge E of phi node PHI.  */

static inline source_location
gimple_phi_arg_location_from_edge (gphi *phi, edge e)
{
  return gimple_phi_arg (phi, e->dest_idx)->locus;
}

/* Set the source location of gimple argument I of phi node PHI to LOC.  */

static inline void
gimple_phi_arg_set_location (gphi *phi, size_t i, source_location loc)
{
  gimple_phi_arg (phi, i)->locus = loc;
}

/* Return TRUE if argument I of phi node PHI has a location record.  */

static inline bool
gimple_phi_arg_has_location (gphi *phi, size_t i)
{
  return gimple_phi_arg_location (phi, i) != UNKNOWN_LOCATION;
}


/* Return the region number for GIMPLE_RESX RESX_STMT.  */

static inline int
gimple_resx_region (const gresx *resx_stmt)
{
  return resx_stmt->region;
}

/* Set REGION to be the region number for GIMPLE_RESX RESX_STMT.  */

static inline void
gimple_resx_set_region (gresx *resx_stmt, int region)
{
  resx_stmt->region = region;
}

/* Return the region number for GIMPLE_EH_DISPATCH EH_DISPATCH_STMT.  */

static inline int
gimple_eh_dispatch_region (const geh_dispatch *eh_dispatch_stmt)
{
  return eh_dispatch_stmt->region;
}

/* Set REGION to be the region number for GIMPLE_EH_DISPATCH
   EH_DISPATCH_STMT.  */

static inline void
gimple_eh_dispatch_set_region (geh_dispatch *eh_dispatch_stmt, int region)
{
  eh_dispatch_stmt->region = region;
}

/* Return the number of labels associated with the switch statement GS.  */

static inline unsigned
gimple_switch_num_labels (const gswitch *gs)
{
  unsigned num_ops;
  GIMPLE_CHECK (gs, GIMPLE_SWITCH);
  num_ops = gimple_num_ops (gs);
  gcc_gimple_checking_assert (num_ops > 1);
  return num_ops - 1;
}


/* Set NLABELS to be the number of labels for the switch statement GS.  */

static inline void
gimple_switch_set_num_labels (gswitch *g, unsigned nlabels)
{
  GIMPLE_CHECK (g, GIMPLE_SWITCH);
  gimple_set_num_ops (g, nlabels + 1);
}


/* Return the index variable used by the switch statement GS.  */

static inline tree
gimple_switch_index (const gswitch *gs)
{
  return gs->op[0];
}


/* Return a pointer to the index variable for the switch statement GS.  */

static inline tree *
gimple_switch_index_ptr (gswitch *gs)
{
  return &gs->op[0];
}


/* Set INDEX to be the index variable for switch statement GS.  */

static inline void
gimple_switch_set_index (gswitch *gs, tree index)
{
  gcc_gimple_checking_assert (SSA_VAR_P (index) || CONSTANT_CLASS_P (index));
  gs->op[0] = index;
}


/* Return the label numbered INDEX.  The default label is 0, followed by any
   labels in a switch statement.  */

static inline tree
gimple_switch_label (const gswitch *gs, unsigned index)
{
  gcc_gimple_checking_assert (gimple_num_ops (gs) > index + 1);
  return gs->op[index + 1];
}

/* Set the label number INDEX to LABEL.  0 is always the default label.  */

static inline void
gimple_switch_set_label (gswitch *gs, unsigned index, tree label)
{
  gcc_gimple_checking_assert (gimple_num_ops (gs) > index + 1
			      && (label == NULL_TREE
			          || TREE_CODE (label) == CASE_LABEL_EXPR));
  gs->op[index + 1] = label;
}

/* Return the default label for a switch statement.  */

static inline tree
gimple_switch_default_label (const gswitch *gs)
{
  tree label = gimple_switch_label (gs, 0);
  gcc_checking_assert (!CASE_LOW (label) && !CASE_HIGH (label));
  return label;
}

/* Set the default label for a switch statement.  */

static inline void
gimple_switch_set_default_label (gswitch *gs, tree label)
{
  gcc_checking_assert (!CASE_LOW (label) && !CASE_HIGH (label));
  gimple_switch_set_label (gs, 0, label);
}

/* Return true if GS is a GIMPLE_DEBUG statement.  */

static inline bool
is_gimple_debug (const gimple *gs)
{
  return gimple_code (gs) == GIMPLE_DEBUG;
}


/* Return the last nondebug statement in GIMPLE sequence S.  */

static inline gimple *
gimple_seq_last_nondebug_stmt (gimple_seq s)
{
  gimple_seq_node n;
  for (n = gimple_seq_last (s);
       n && is_gimple_debug (n);
       n = n->prev)
    if (n->prev == s)
      return NULL;
  return n;
}


/* Return true if S is a GIMPLE_DEBUG BIND statement.  */

static inline bool
gimple_debug_bind_p (const gimple *s)
{
  if (is_gimple_debug (s))
    return s->subcode == GIMPLE_DEBUG_BIND;

  return false;
}

/* Return the variable bound in a GIMPLE_DEBUG bind statement.  */

static inline tree
gimple_debug_bind_get_var (gimple *dbg)
{
  GIMPLE_CHECK (dbg, GIMPLE_DEBUG);
  gcc_gimple_checking_assert (gimple_debug_bind_p (dbg));
  return gimple_op (dbg, 0);
}

/* Return the value bound to the variable in a GIMPLE_DEBUG bind
   statement.  */

static inline tree
gimple_debug_bind_get_value (gimple *dbg)
{
  GIMPLE_CHECK (dbg, GIMPLE_DEBUG);
  gcc_gimple_checking_assert (gimple_debug_bind_p (dbg));
  return gimple_op (dbg, 1);
}

/* Return a pointer to the value bound to the variable in a
   GIMPLE_DEBUG bind statement.  */

static inline tree *
gimple_debug_bind_get_value_ptr (gimple *dbg)
{
  GIMPLE_CHECK (dbg, GIMPLE_DEBUG);
  gcc_gimple_checking_assert (gimple_debug_bind_p (dbg));
  return gimple_op_ptr (dbg, 1);
}

/* Set the variable bound in a GIMPLE_DEBUG bind statement.  */

static inline void
gimple_debug_bind_set_var (gimple *dbg, tree var)
{
  GIMPLE_CHECK (dbg, GIMPLE_DEBUG);
  gcc_gimple_checking_assert (gimple_debug_bind_p (dbg));
  gimple_set_op (dbg, 0, var);
}

/* Set the value bound to the variable in a GIMPLE_DEBUG bind
   statement.  */

static inline void
gimple_debug_bind_set_value (gimple *dbg, tree value)
{
  GIMPLE_CHECK (dbg, GIMPLE_DEBUG);
  gcc_gimple_checking_assert (gimple_debug_bind_p (dbg));
  gimple_set_op (dbg, 1, value);
}

/* The second operand of a GIMPLE_DEBUG_BIND, when the value was
   optimized away.  */
#define GIMPLE_DEBUG_BIND_NOVALUE NULL_TREE /* error_mark_node */

/* Remove the value bound to the variable in a GIMPLE_DEBUG bind
   statement.  */

static inline void
gimple_debug_bind_reset_value (gimple *dbg)
{
  GIMPLE_CHECK (dbg, GIMPLE_DEBUG);
  gcc_gimple_checking_assert (gimple_debug_bind_p (dbg));
  gimple_set_op (dbg, 1, GIMPLE_DEBUG_BIND_NOVALUE);
}

/* Return true if the GIMPLE_DEBUG bind statement is bound to a
   value.  */

static inline bool
gimple_debug_bind_has_value_p (gimple *dbg)
{
  GIMPLE_CHECK (dbg, GIMPLE_DEBUG);
  gcc_gimple_checking_assert (gimple_debug_bind_p (dbg));
  return gimple_op (dbg, 1) != GIMPLE_DEBUG_BIND_NOVALUE;
}

#undef GIMPLE_DEBUG_BIND_NOVALUE

/* Return true if S is a GIMPLE_DEBUG SOURCE BIND statement.  */

static inline bool
gimple_debug_source_bind_p (const gimple *s)
{
  if (is_gimple_debug (s))
    return s->subcode == GIMPLE_DEBUG_SOURCE_BIND;

  return false;
}

/* Return the variable bound in a GIMPLE_DEBUG source bind statement.  */

static inline tree
gimple_debug_source_bind_get_var (gimple *dbg)
{
  GIMPLE_CHECK (dbg, GIMPLE_DEBUG);
  gcc_gimple_checking_assert (gimple_debug_source_bind_p (dbg));
  return gimple_op (dbg, 0);
}

/* Return the value bound to the variable in a GIMPLE_DEBUG source bind
   statement.  */

static inline tree
gimple_debug_source_bind_get_value (gimple *dbg)
{
  GIMPLE_CHECK (dbg, GIMPLE_DEBUG);
  gcc_gimple_checking_assert (gimple_debug_source_bind_p (dbg));
  return gimple_op (dbg, 1);
}

/* Return a pointer to the value bound to the variable in a
   GIMPLE_DEBUG source bind statement.  */

static inline tree *
gimple_debug_source_bind_get_value_ptr (gimple *dbg)
{
  GIMPLE_CHECK (dbg, GIMPLE_DEBUG);
  gcc_gimple_checking_assert (gimple_debug_source_bind_p (dbg));
  return gimple_op_ptr (dbg, 1);
}

/* Set the variable bound in a GIMPLE_DEBUG source bind statement.  */

static inline void
gimple_debug_source_bind_set_var (gimple *dbg, tree var)
{
  GIMPLE_CHECK (dbg, GIMPLE_DEBUG);
  gcc_gimple_checking_assert (gimple_debug_source_bind_p (dbg));
  gimple_set_op (dbg, 0, var);
}

/* Set the value bound to the variable in a GIMPLE_DEBUG source bind
   statement.  */

static inline void
gimple_debug_source_bind_set_value (gimple *dbg, tree value)
{
  GIMPLE_CHECK (dbg, GIMPLE_DEBUG);
  gcc_gimple_checking_assert (gimple_debug_source_bind_p (dbg));
  gimple_set_op (dbg, 1, value);
}

/* Return true if S is a GIMPLE_DEBUG BEGIN_STMT statement.  */

static inline bool
gimple_debug_begin_stmt_p (const gimple *s)
{
  if (is_gimple_debug (s))
    return s->subcode == GIMPLE_DEBUG_BEGIN_STMT;

  return false;
}

/* Return true if S is a GIMPLE_DEBUG INLINE_ENTRY statement.  */

static inline bool
gimple_debug_inline_entry_p (const gimple *s)
{
  if (is_gimple_debug (s))
    return s->subcode == GIMPLE_DEBUG_INLINE_ENTRY;

  return false;
}

/* Return true if S is a GIMPLE_DEBUG non-binding marker statement.  */

static inline bool
gimple_debug_nonbind_marker_p (const gimple *s)
{
  if (is_gimple_debug (s))
    return s->subcode == GIMPLE_DEBUG_BEGIN_STMT
      || s->subcode == GIMPLE_DEBUG_INLINE_ENTRY;

  return false;
}

/* Return the line number for EXPR, or return -1 if we have no line
   number information for it.  */
static inline int
get_lineno (const gimple *stmt)
{
  location_t loc;

  if (!stmt)
    return -1;

  loc = gimple_location (stmt);
  if (loc == UNKNOWN_LOCATION)
    return -1;

  return LOCATION_LINE (loc);
}

/* Return a pointer to the body for the OMP statement GS.  */

static inline gimple_seq *
gimple_omp_body_ptr (gimple *gs)
{
  return &static_cast <gimple_statement_omp *> (gs)->body;
}

/* Return the body for the OMP statement GS.  */

static inline gimple_seq
gimple_omp_body (gimple *gs)
{
  return *gimple_omp_body_ptr (gs);
}

/* Set BODY to be the body for the OMP statement GS.  */

static inline void
gimple_omp_set_body (gimple *gs, gimple_seq body)
{
  static_cast <gimple_statement_omp *> (gs)->body = body;
}


/* Return the name associated with OMP_CRITICAL statement CRIT_STMT.  */

static inline tree
gimple_omp_critical_name (const gomp_critical *crit_stmt)
{
  return crit_stmt->name;
}


/* Return a pointer to the name associated with OMP critical statement
   CRIT_STMT.  */

static inline tree *
gimple_omp_critical_name_ptr (gomp_critical *crit_stmt)
{
  return &crit_stmt->name;
}


/* Set NAME to be the name associated with OMP critical statement
   CRIT_STMT.  */

static inline void
gimple_omp_critical_set_name (gomp_critical *crit_stmt, tree name)
{
  crit_stmt->name = name;
}


/* Return the clauses associated with OMP_CRITICAL statement CRIT_STMT.  */

static inline tree
gimple_omp_critical_clauses (const gomp_critical *crit_stmt)
{
  return crit_stmt->clauses;
}


/* Return a pointer to the clauses associated with OMP critical statement
   CRIT_STMT.  */

static inline tree *
gimple_omp_critical_clauses_ptr (gomp_critical *crit_stmt)
{
  return &crit_stmt->clauses;
}


/* Set CLAUSES to be the clauses associated with OMP critical statement
   CRIT_STMT.  */

static inline void
gimple_omp_critical_set_clauses (gomp_critical *crit_stmt, tree clauses)
{
  crit_stmt->clauses = clauses;
}


/* Return the clauses associated with OMP_ORDERED statement ORD_STMT.  */

static inline tree
gimple_omp_ordered_clauses (const gomp_ordered *ord_stmt)
{
  return ord_stmt->clauses;
}


/* Return a pointer to the clauses associated with OMP ordered statement
   ORD_STMT.  */

static inline tree *
gimple_omp_ordered_clauses_ptr (gomp_ordered *ord_stmt)
{
  return &ord_stmt->clauses;
}


/* Set CLAUSES to be the clauses associated with OMP ordered statement
   ORD_STMT.  */

static inline void
gimple_omp_ordered_set_clauses (gomp_ordered *ord_stmt, tree clauses)
{
  ord_stmt->clauses = clauses;
}


/* Return the kind of the OMP_FOR statemement G.  */

static inline int
gimple_omp_for_kind (const gimple *g)
{
  GIMPLE_CHECK (g, GIMPLE_OMP_FOR);
  return (gimple_omp_subcode (g) & GF_OMP_FOR_KIND_MASK);
}


/* Set the kind of the OMP_FOR statement G.  */

static inline void
gimple_omp_for_set_kind (gomp_for *g, int kind)
{
  g->subcode = (g->subcode & ~GF_OMP_FOR_KIND_MASK)
		      | (kind & GF_OMP_FOR_KIND_MASK);
}


/* Return true if OMP_FOR statement G has the
   GF_OMP_FOR_COMBINED flag set.  */

static inline bool
gimple_omp_for_combined_p (const gimple *g)
{
  GIMPLE_CHECK (g, GIMPLE_OMP_FOR);
  return (gimple_omp_subcode (g) & GF_OMP_FOR_COMBINED) != 0;
}


/* Set the GF_OMP_FOR_COMBINED field in the OMP_FOR statement G depending on
   the boolean value of COMBINED_P.  */

static inline void
gimple_omp_for_set_combined_p (gomp_for *g, bool combined_p)
{
  if (combined_p)
    g->subcode |= GF_OMP_FOR_COMBINED;
  else
    g->subcode &= ~GF_OMP_FOR_COMBINED;
}


/* Return true if the OMP_FOR statement G has the
   GF_OMP_FOR_COMBINED_INTO flag set.  */

static inline bool
gimple_omp_for_combined_into_p (const gimple *g)
{
  GIMPLE_CHECK (g, GIMPLE_OMP_FOR);
  return (gimple_omp_subcode (g) & GF_OMP_FOR_COMBINED_INTO) != 0;
}


/* Set the GF_OMP_FOR_COMBINED_INTO field in the OMP_FOR statement G depending
   on the boolean value of COMBINED_P.  */

static inline void
gimple_omp_for_set_combined_into_p (gomp_for *g, bool combined_p)
{
  if (combined_p)
    g->subcode |= GF_OMP_FOR_COMBINED_INTO;
  else
    g->subcode &= ~GF_OMP_FOR_COMBINED_INTO;
}


/* Return the clauses associated with the OMP_FOR statement GS.  */

static inline tree
gimple_omp_for_clauses (const gimple *gs)
{
  const gomp_for *omp_for_stmt = as_a <const gomp_for *> (gs);
  return omp_for_stmt->clauses;
}


/* Return a pointer to the clauses associated with the OMP_FOR statement
   GS.  */

static inline tree *
gimple_omp_for_clauses_ptr (gimple *gs)
{
  gomp_for *omp_for_stmt = as_a <gomp_for *> (gs);
  return &omp_for_stmt->clauses;
}


/* Set CLAUSES to be the list of clauses associated with the OMP_FOR statement
   GS.  */

static inline void
gimple_omp_for_set_clauses (gimple *gs, tree clauses)
{
  gomp_for *omp_for_stmt = as_a <gomp_for *> (gs);
  omp_for_stmt->clauses = clauses;
}


/* Get the collapse count of the OMP_FOR statement GS.  */

static inline size_t
gimple_omp_for_collapse (gimple *gs)
{
  gomp_for *omp_for_stmt = as_a <gomp_for *> (gs);
  return omp_for_stmt->collapse;
}


/* Return the condition code associated with the OMP_FOR statement GS.  */

static inline enum tree_code
gimple_omp_for_cond (const gimple *gs, size_t i)
{
  const gomp_for *omp_for_stmt = as_a <const gomp_for *> (gs);
  gcc_gimple_checking_assert (i < omp_for_stmt->collapse);
  return omp_for_stmt->iter[i].cond;
}


/* Set COND to be the condition code for the OMP_FOR statement GS.  */

static inline void
gimple_omp_for_set_cond (gimple *gs, size_t i, enum tree_code cond)
{
  gomp_for *omp_for_stmt = as_a <gomp_for *> (gs);
  gcc_gimple_checking_assert (TREE_CODE_CLASS (cond) == tcc_comparison
			      && i < omp_for_stmt->collapse);
  omp_for_stmt->iter[i].cond = cond;
}


/* Return the index variable for the OMP_FOR statement GS.  */

static inline tree
gimple_omp_for_index (const gimple *gs, size_t i)
{
  const gomp_for *omp_for_stmt = as_a <const gomp_for *> (gs);
  gcc_gimple_checking_assert (i < omp_for_stmt->collapse);
  return omp_for_stmt->iter[i].index;
}


/* Return a pointer to the index variable for the OMP_FOR statement GS.  */

static inline tree *
gimple_omp_for_index_ptr (gimple *gs, size_t i)
{
  gomp_for *omp_for_stmt = as_a <gomp_for *> (gs);
  gcc_gimple_checking_assert (i < omp_for_stmt->collapse);
  return &omp_for_stmt->iter[i].index;
}


/* Set INDEX to be the index variable for the OMP_FOR statement GS.  */

static inline void
gimple_omp_for_set_index (gimple *gs, size_t i, tree index)
{
  gomp_for *omp_for_stmt = as_a <gomp_for *> (gs);
  gcc_gimple_checking_assert (i < omp_for_stmt->collapse);
  omp_for_stmt->iter[i].index = index;
}


/* Return the initial value for the OMP_FOR statement GS.  */

static inline tree
gimple_omp_for_initial (const gimple *gs, size_t i)
{
  const gomp_for *omp_for_stmt = as_a <const gomp_for *> (gs);
  gcc_gimple_checking_assert (i < omp_for_stmt->collapse);
  return omp_for_stmt->iter[i].initial;
}


/* Return a pointer to the initial value for the OMP_FOR statement GS.  */

static inline tree *
gimple_omp_for_initial_ptr (gimple *gs, size_t i)
{
  gomp_for *omp_for_stmt = as_a <gomp_for *> (gs);
  gcc_gimple_checking_assert (i < omp_for_stmt->collapse);
  return &omp_for_stmt->iter[i].initial;
}


/* Set INITIAL to be the initial value for the OMP_FOR statement GS.  */

static inline void
gimple_omp_for_set_initial (gimple *gs, size_t i, tree initial)
{
  gomp_for *omp_for_stmt = as_a <gomp_for *> (gs);
  gcc_gimple_checking_assert (i < omp_for_stmt->collapse);
  omp_for_stmt->iter[i].initial = initial;
}


/* Return the final value for the OMP_FOR statement GS.  */

static inline tree
gimple_omp_for_final (const gimple *gs, size_t i)
{
  const gomp_for *omp_for_stmt = as_a <const gomp_for *> (gs);
  gcc_gimple_checking_assert (i < omp_for_stmt->collapse);
  return omp_for_stmt->iter[i].final;
}


/* Return a pointer to the final value for the OMP_FOR statement GS.  */

static inline tree *
gimple_omp_for_final_ptr (gimple *gs, size_t i)
{
  gomp_for *omp_for_stmt = as_a <gomp_for *> (gs);
  gcc_gimple_checking_assert (i < omp_for_stmt->collapse);
  return &omp_for_stmt->iter[i].final;
}


/* Set FINAL to be the final value for the OMP_FOR statement GS.  */

static inline void
gimple_omp_for_set_final (gimple *gs, size_t i, tree final)
{
  gomp_for *omp_for_stmt = as_a <gomp_for *> (gs);
  gcc_gimple_checking_assert (i < omp_for_stmt->collapse);
  omp_for_stmt->iter[i].final = final;
}


/* Return the increment value for the OMP_FOR statement GS.  */

static inline tree
gimple_omp_for_incr (const gimple *gs, size_t i)
{
  const gomp_for *omp_for_stmt = as_a <const gomp_for *> (gs);
  gcc_gimple_checking_assert (i < omp_for_stmt->collapse);
  return omp_for_stmt->iter[i].incr;
}


/* Return a pointer to the increment value for the OMP_FOR statement GS.  */

static inline tree *
gimple_omp_for_incr_ptr (gimple *gs, size_t i)
{
  gomp_for *omp_for_stmt = as_a <gomp_for *> (gs);
  gcc_gimple_checking_assert (i < omp_for_stmt->collapse);
  return &omp_for_stmt->iter[i].incr;
}


/* Set INCR to be the increment value for the OMP_FOR statement GS.  */

static inline void
gimple_omp_for_set_incr (gimple *gs, size_t i, tree incr)
{
  gomp_for *omp_for_stmt = as_a <gomp_for *> (gs);
  gcc_gimple_checking_assert (i < omp_for_stmt->collapse);
  omp_for_stmt->iter[i].incr = incr;
}


/* Return a pointer to the sequence of statements to execute before the OMP_FOR
   statement GS starts.  */

static inline gimple_seq *
gimple_omp_for_pre_body_ptr (gimple *gs)
{
  gomp_for *omp_for_stmt = as_a <gomp_for *> (gs);
  return &omp_for_stmt->pre_body;
}


/* Return the sequence of statements to execute before the OMP_FOR
   statement GS starts.  */

static inline gimple_seq
gimple_omp_for_pre_body (gimple *gs)
{
  return *gimple_omp_for_pre_body_ptr (gs);
}


/* Set PRE_BODY to be the sequence of statements to execute before the
   OMP_FOR statement GS starts.  */

static inline void
gimple_omp_for_set_pre_body (gimple *gs, gimple_seq pre_body)
{
  gomp_for *omp_for_stmt = as_a <gomp_for *> (gs);
  omp_for_stmt->pre_body = pre_body;
}

/* Return the kernel_phony of OMP_FOR statement.  */

static inline bool
gimple_omp_for_grid_phony (const gomp_for *omp_for)
{
  gcc_checking_assert (gimple_omp_for_kind (omp_for)
		       != GF_OMP_FOR_KIND_GRID_LOOP);
  return (gimple_omp_subcode (omp_for) & GF_OMP_FOR_GRID_PHONY) != 0;
}

/* Set kernel_phony flag of OMP_FOR to VALUE.  */

static inline void
gimple_omp_for_set_grid_phony (gomp_for *omp_for, bool value)
{
  gcc_checking_assert (gimple_omp_for_kind (omp_for)
		       != GF_OMP_FOR_KIND_GRID_LOOP);
  if (value)
    omp_for->subcode |= GF_OMP_FOR_GRID_PHONY;
  else
    omp_for->subcode &= ~GF_OMP_FOR_GRID_PHONY;
}

/* Return the kernel_intra_group of a GRID_LOOP OMP_FOR statement.  */

static inline bool
gimple_omp_for_grid_intra_group (const gomp_for *omp_for)
{
  gcc_checking_assert (gimple_omp_for_kind (omp_for)
		       == GF_OMP_FOR_KIND_GRID_LOOP);
  return (gimple_omp_subcode (omp_for) & GF_OMP_FOR_GRID_INTRA_GROUP) != 0;
}

/* Set kernel_intra_group flag of OMP_FOR to VALUE.  */

static inline void
gimple_omp_for_set_grid_intra_group (gomp_for *omp_for, bool value)
{
  gcc_checking_assert (gimple_omp_for_kind (omp_for)
		       == GF_OMP_FOR_KIND_GRID_LOOP);
  if (value)
    omp_for->subcode |= GF_OMP_FOR_GRID_INTRA_GROUP;
  else
    omp_for->subcode &= ~GF_OMP_FOR_GRID_INTRA_GROUP;
}

/* Return true if iterations of a grid OMP_FOR statement correspond to HSA
   groups.  */

static inline bool
gimple_omp_for_grid_group_iter (const gomp_for *omp_for)
{
  gcc_checking_assert (gimple_omp_for_kind (omp_for)
		       == GF_OMP_FOR_KIND_GRID_LOOP);
  return (gimple_omp_subcode (omp_for) & GF_OMP_FOR_GRID_GROUP_ITER) != 0;
}

/* Set group_iter flag of OMP_FOR to VALUE.  */

static inline void
gimple_omp_for_set_grid_group_iter (gomp_for *omp_for, bool value)
{
  gcc_checking_assert (gimple_omp_for_kind (omp_for)
		       == GF_OMP_FOR_KIND_GRID_LOOP);
  if (value)
    omp_for->subcode |= GF_OMP_FOR_GRID_GROUP_ITER;
  else
    omp_for->subcode &= ~GF_OMP_FOR_GRID_GROUP_ITER;
}

/* Return the clauses associated with OMP_PARALLEL GS.  */

static inline tree
gimple_omp_parallel_clauses (const gimple *gs)
{
  const gomp_parallel *omp_parallel_stmt = as_a <const gomp_parallel *> (gs);
  return omp_parallel_stmt->clauses;
}


/* Return a pointer to the clauses associated with OMP_PARALLEL_STMT.  */

static inline tree *
gimple_omp_parallel_clauses_ptr (gomp_parallel *omp_parallel_stmt)
{
  return &omp_parallel_stmt->clauses;
}


/* Set CLAUSES to be the list of clauses associated with OMP_PARALLEL_STMT.  */

static inline void
gimple_omp_parallel_set_clauses (gomp_parallel *omp_parallel_stmt,
				 tree clauses)
{
  omp_parallel_stmt->clauses = clauses;
}


/* Return the child function used to hold the body of OMP_PARALLEL_STMT.  */

static inline tree
gimple_omp_parallel_child_fn (const gomp_parallel *omp_parallel_stmt)
{
  return omp_parallel_stmt->child_fn;
}

/* Return a pointer to the child function used to hold the body of
   OMP_PARALLEL_STMT.  */

static inline tree *
gimple_omp_parallel_child_fn_ptr (gomp_parallel *omp_parallel_stmt)
{
  return &omp_parallel_stmt->child_fn;
}


/* Set CHILD_FN to be the child function for OMP_PARALLEL_STMT.  */

static inline void
gimple_omp_parallel_set_child_fn (gomp_parallel *omp_parallel_stmt,
				  tree child_fn)
{
  omp_parallel_stmt->child_fn = child_fn;
}


/* Return the artificial argument used to send variables and values
   from the parent to the children threads in OMP_PARALLEL_STMT.  */

static inline tree
gimple_omp_parallel_data_arg (const gomp_parallel *omp_parallel_stmt)
{
  return omp_parallel_stmt->data_arg;
}


/* Return a pointer to the data argument for OMP_PARALLEL_STMT.  */

static inline tree *
gimple_omp_parallel_data_arg_ptr (gomp_parallel *omp_parallel_stmt)
{
  return &omp_parallel_stmt->data_arg;
}


/* Set DATA_ARG to be the data argument for OMP_PARALLEL_STMT.  */

static inline void
gimple_omp_parallel_set_data_arg (gomp_parallel *omp_parallel_stmt,
				  tree data_arg)
{
  omp_parallel_stmt->data_arg = data_arg;
}

/* Return the kernel_phony flag of OMP_PARALLEL_STMT.  */

static inline bool
gimple_omp_parallel_grid_phony (const gomp_parallel *stmt)
{
  return (gimple_omp_subcode (stmt) & GF_OMP_PARALLEL_GRID_PHONY) != 0;
}

/* Set kernel_phony flag of OMP_PARALLEL_STMT to VALUE.  */

static inline void
gimple_omp_parallel_set_grid_phony (gomp_parallel *stmt, bool value)
{
  if (value)
    stmt->subcode |= GF_OMP_PARALLEL_GRID_PHONY;
  else
    stmt->subcode &= ~GF_OMP_PARALLEL_GRID_PHONY;
}

/* Return the clauses associated with OMP_TASK GS.  */

static inline tree
gimple_omp_task_clauses (const gimple *gs)
{
  const gomp_task *omp_task_stmt = as_a <const gomp_task *> (gs);
  return omp_task_stmt->clauses;
}


/* Return a pointer to the clauses associated with OMP_TASK GS.  */

static inline tree *
gimple_omp_task_clauses_ptr (gimple *gs)
{
  gomp_task *omp_task_stmt = as_a <gomp_task *> (gs);
  return &omp_task_stmt->clauses;
}


/* Set CLAUSES to be the list of clauses associated with OMP_TASK
   GS.  */

static inline void
gimple_omp_task_set_clauses (gimple *gs, tree clauses)
{
  gomp_task *omp_task_stmt = as_a <gomp_task *> (gs);
  omp_task_stmt->clauses = clauses;
}


/* Return true if OMP task statement G has the
   GF_OMP_TASK_TASKLOOP flag set.  */

static inline bool
gimple_omp_task_taskloop_p (const gimple *g)
{
  GIMPLE_CHECK (g, GIMPLE_OMP_TASK);
  return (gimple_omp_subcode (g) & GF_OMP_TASK_TASKLOOP) != 0;
}


/* Set the GF_OMP_TASK_TASKLOOP field in G depending on the boolean
   value of TASKLOOP_P.  */

static inline void
gimple_omp_task_set_taskloop_p (gimple *g, bool taskloop_p)
{
  GIMPLE_CHECK (g, GIMPLE_OMP_TASK);
  if (taskloop_p)
    g->subcode |= GF_OMP_TASK_TASKLOOP;
  else
    g->subcode &= ~GF_OMP_TASK_TASKLOOP;
}


/* Return the child function used to hold the body of OMP_TASK GS.  */

static inline tree
gimple_omp_task_child_fn (const gimple *gs)
{
  const gomp_task *omp_task_stmt = as_a <const gomp_task *> (gs);
  return omp_task_stmt->child_fn;
}

/* Return a pointer to the child function used to hold the body of
   OMP_TASK GS.  */

static inline tree *
gimple_omp_task_child_fn_ptr (gimple *gs)
{
  gomp_task *omp_task_stmt = as_a <gomp_task *> (gs);
  return &omp_task_stmt->child_fn;
}


/* Set CHILD_FN to be the child function for OMP_TASK GS.  */

static inline void
gimple_omp_task_set_child_fn (gimple *gs, tree child_fn)
{
  gomp_task *omp_task_stmt = as_a <gomp_task *> (gs);
  omp_task_stmt->child_fn = child_fn;
}


/* Return the artificial argument used to send variables and values
   from the parent to the children threads in OMP_TASK GS.  */

static inline tree
gimple_omp_task_data_arg (const gimple *gs)
{
  const gomp_task *omp_task_stmt = as_a <const gomp_task *> (gs);
  return omp_task_stmt->data_arg;
}


/* Return a pointer to the data argument for OMP_TASK GS.  */

static inline tree *
gimple_omp_task_data_arg_ptr (gimple *gs)
{
  gomp_task *omp_task_stmt = as_a <gomp_task *> (gs);
  return &omp_task_stmt->data_arg;
}


/* Set DATA_ARG to be the data argument for OMP_TASK GS.  */

static inline void
gimple_omp_task_set_data_arg (gimple *gs, tree data_arg)
{
  gomp_task *omp_task_stmt = as_a <gomp_task *> (gs);
  omp_task_stmt->data_arg = data_arg;
}


/* Return the clauses associated with OMP_TASK GS.  */

static inline tree
gimple_omp_taskreg_clauses (const gimple *gs)
{
  const gimple_statement_omp_taskreg *omp_taskreg_stmt
    = as_a <const gimple_statement_omp_taskreg *> (gs);
  return omp_taskreg_stmt->clauses;
}


/* Return a pointer to the clauses associated with OMP_TASK GS.  */

static inline tree *
gimple_omp_taskreg_clauses_ptr (gimple *gs)
{
  gimple_statement_omp_taskreg *omp_taskreg_stmt
    = as_a <gimple_statement_omp_taskreg *> (gs);
  return &omp_taskreg_stmt->clauses;
}


/* Set CLAUSES to be the list of clauses associated with OMP_TASK
   GS.  */

static inline void
gimple_omp_taskreg_set_clauses (gimple *gs, tree clauses)
{
  gimple_statement_omp_taskreg *omp_taskreg_stmt
    = as_a <gimple_statement_omp_taskreg *> (gs);
  omp_taskreg_stmt->clauses = clauses;
}


/* Return the child function used to hold the body of OMP_TASK GS.  */

static inline tree
gimple_omp_taskreg_child_fn (const gimple *gs)
{
  const gimple_statement_omp_taskreg *omp_taskreg_stmt
    = as_a <const gimple_statement_omp_taskreg *> (gs);
  return omp_taskreg_stmt->child_fn;
}

/* Return a pointer to the child function used to hold the body of
   OMP_TASK GS.  */

static inline tree *
gimple_omp_taskreg_child_fn_ptr (gimple *gs)
{
  gimple_statement_omp_taskreg *omp_taskreg_stmt
    = as_a <gimple_statement_omp_taskreg *> (gs);
  return &omp_taskreg_stmt->child_fn;
}


/* Set CHILD_FN to be the child function for OMP_TASK GS.  */

static inline void
gimple_omp_taskreg_set_child_fn (gimple *gs, tree child_fn)
{
  gimple_statement_omp_taskreg *omp_taskreg_stmt
    = as_a <gimple_statement_omp_taskreg *> (gs);
  omp_taskreg_stmt->child_fn = child_fn;
}


/* Return the artificial argument used to send variables and values
   from the parent to the children threads in OMP_TASK GS.  */

static inline tree
gimple_omp_taskreg_data_arg (const gimple *gs)
{
  const gimple_statement_omp_taskreg *omp_taskreg_stmt
    = as_a <const gimple_statement_omp_taskreg *> (gs);
  return omp_taskreg_stmt->data_arg;
}


/* Return a pointer to the data argument for OMP_TASK GS.  */

static inline tree *
gimple_omp_taskreg_data_arg_ptr (gimple *gs)
{
  gimple_statement_omp_taskreg *omp_taskreg_stmt
    = as_a <gimple_statement_omp_taskreg *> (gs);
  return &omp_taskreg_stmt->data_arg;
}


/* Set DATA_ARG to be the data argument for OMP_TASK GS.  */

static inline void
gimple_omp_taskreg_set_data_arg (gimple *gs, tree data_arg)
{
  gimple_statement_omp_taskreg *omp_taskreg_stmt
    = as_a <gimple_statement_omp_taskreg *> (gs);
  omp_taskreg_stmt->data_arg = data_arg;
}


/* Return the copy function used to hold the body of OMP_TASK GS.  */

static inline tree
gimple_omp_task_copy_fn (const gimple *gs)
{
  const gomp_task *omp_task_stmt = as_a <const gomp_task *> (gs);
  return omp_task_stmt->copy_fn;
}

/* Return a pointer to the copy function used to hold the body of
   OMP_TASK GS.  */

static inline tree *
gimple_omp_task_copy_fn_ptr (gimple *gs)
{
  gomp_task *omp_task_stmt = as_a <gomp_task *> (gs);
  return &omp_task_stmt->copy_fn;
}


/* Set CHILD_FN to be the copy function for OMP_TASK GS.  */

static inline void
gimple_omp_task_set_copy_fn (gimple *gs, tree copy_fn)
{
  gomp_task *omp_task_stmt = as_a <gomp_task *> (gs);
  omp_task_stmt->copy_fn = copy_fn;
}


/* Return size of the data block in bytes in OMP_TASK GS.  */

static inline tree
gimple_omp_task_arg_size (const gimple *gs)
{
  const gomp_task *omp_task_stmt = as_a <const gomp_task *> (gs);
  return omp_task_stmt->arg_size;
}


/* Return a pointer to the data block size for OMP_TASK GS.  */

static inline tree *
gimple_omp_task_arg_size_ptr (gimple *gs)
{
  gomp_task *omp_task_stmt = as_a <gomp_task *> (gs);
  return &omp_task_stmt->arg_size;
}


/* Set ARG_SIZE to be the data block size for OMP_TASK GS.  */

static inline void
gimple_omp_task_set_arg_size (gimple *gs, tree arg_size)
{
  gomp_task *omp_task_stmt = as_a <gomp_task *> (gs);
  omp_task_stmt->arg_size = arg_size;
}


/* Return align of the data block in bytes in OMP_TASK GS.  */

static inline tree
gimple_omp_task_arg_align (const gimple *gs)
{
  const gomp_task *omp_task_stmt = as_a <const gomp_task *> (gs);
  return omp_task_stmt->arg_align;
}


/* Return a pointer to the data block align for OMP_TASK GS.  */

static inline tree *
gimple_omp_task_arg_align_ptr (gimple *gs)
{
  gomp_task *omp_task_stmt = as_a <gomp_task *> (gs);
  return &omp_task_stmt->arg_align;
}


/* Set ARG_SIZE to be the data block align for OMP_TASK GS.  */

static inline void
gimple_omp_task_set_arg_align (gimple *gs, tree arg_align)
{
  gomp_task *omp_task_stmt = as_a <gomp_task *> (gs);
  omp_task_stmt->arg_align = arg_align;
}


/* Return the clauses associated with OMP_SINGLE GS.  */

static inline tree
gimple_omp_single_clauses (const gimple *gs)
{
  const gomp_single *omp_single_stmt = as_a <const gomp_single *> (gs);
  return omp_single_stmt->clauses;
}


/* Return a pointer to the clauses associated with OMP_SINGLE GS.  */

static inline tree *
gimple_omp_single_clauses_ptr (gimple *gs)
{
  gomp_single *omp_single_stmt = as_a <gomp_single *> (gs);
  return &omp_single_stmt->clauses;
}


/* Set CLAUSES to be the clauses associated with OMP_SINGLE_STMT.  */

static inline void
gimple_omp_single_set_clauses (gomp_single *omp_single_stmt, tree clauses)
{
  omp_single_stmt->clauses = clauses;
}


/* Return the clauses associated with OMP_TARGET GS.  */

static inline tree
gimple_omp_target_clauses (const gimple *gs)
{
  const gomp_target *omp_target_stmt = as_a <const gomp_target *> (gs);
  return omp_target_stmt->clauses;
}


/* Return a pointer to the clauses associated with OMP_TARGET GS.  */

static inline tree *
gimple_omp_target_clauses_ptr (gimple *gs)
{
  gomp_target *omp_target_stmt = as_a <gomp_target *> (gs);
  return &omp_target_stmt->clauses;
}


/* Set CLAUSES to be the clauses associated with OMP_TARGET_STMT.  */

static inline void
gimple_omp_target_set_clauses (gomp_target *omp_target_stmt,
			       tree clauses)
{
  omp_target_stmt->clauses = clauses;
}


/* Return the kind of the OMP_TARGET G.  */

static inline int
gimple_omp_target_kind (const gimple *g)
{
  GIMPLE_CHECK (g, GIMPLE_OMP_TARGET);
  return (gimple_omp_subcode (g) & GF_OMP_TARGET_KIND_MASK);
}


/* Set the kind of the OMP_TARGET G.  */

static inline void
gimple_omp_target_set_kind (gomp_target *g, int kind)
{
  g->subcode = (g->subcode & ~GF_OMP_TARGET_KIND_MASK)
		      | (kind & GF_OMP_TARGET_KIND_MASK);
}


/* Return the child function used to hold the body of OMP_TARGET_STMT.  */

static inline tree
gimple_omp_target_child_fn (const gomp_target *omp_target_stmt)
{
  return omp_target_stmt->child_fn;
}

/* Return a pointer to the child function used to hold the body of
   OMP_TARGET_STMT.  */

static inline tree *
gimple_omp_target_child_fn_ptr (gomp_target *omp_target_stmt)
{
  return &omp_target_stmt->child_fn;
}


/* Set CHILD_FN to be the child function for OMP_TARGET_STMT.  */

static inline void
gimple_omp_target_set_child_fn (gomp_target *omp_target_stmt,
				tree child_fn)
{
  omp_target_stmt->child_fn = child_fn;
}


/* Return the artificial argument used to send variables and values
   from the parent to the children threads in OMP_TARGET_STMT.  */

static inline tree
gimple_omp_target_data_arg (const gomp_target *omp_target_stmt)
{
  return omp_target_stmt->data_arg;
}


/* Return a pointer to the data argument for OMP_TARGET GS.  */

static inline tree *
gimple_omp_target_data_arg_ptr (gomp_target *omp_target_stmt)
{
  return &omp_target_stmt->data_arg;
}


/* Set DATA_ARG to be the data argument for OMP_TARGET_STMT.  */

static inline void
gimple_omp_target_set_data_arg (gomp_target *omp_target_stmt,
				tree data_arg)
{
  omp_target_stmt->data_arg = data_arg;
}


/* Return the clauses associated with OMP_TEAMS GS.  */

static inline tree
gimple_omp_teams_clauses (const gimple *gs)
{
  const gomp_teams *omp_teams_stmt = as_a <const gomp_teams *> (gs);
  return omp_teams_stmt->clauses;
}


/* Return a pointer to the clauses associated with OMP_TEAMS GS.  */

static inline tree *
gimple_omp_teams_clauses_ptr (gimple *gs)
{
  gomp_teams *omp_teams_stmt = as_a <gomp_teams *> (gs);
  return &omp_teams_stmt->clauses;
}


/* Set CLAUSES to be the clauses associated with OMP_TEAMS_STMT.  */

static inline void
gimple_omp_teams_set_clauses (gomp_teams *omp_teams_stmt, tree clauses)
{
  omp_teams_stmt->clauses = clauses;
}

/* Return the kernel_phony flag of an OMP_TEAMS_STMT.  */

static inline bool
gimple_omp_teams_grid_phony (const gomp_teams *omp_teams_stmt)
{
  return (gimple_omp_subcode (omp_teams_stmt) & GF_OMP_TEAMS_GRID_PHONY) != 0;
}

/* Set kernel_phony flag of an OMP_TEAMS_STMT to VALUE.  */

static inline void
gimple_omp_teams_set_grid_phony (gomp_teams *omp_teams_stmt, bool value)
{
  if (value)
    omp_teams_stmt->subcode |= GF_OMP_TEAMS_GRID_PHONY;
  else
    omp_teams_stmt->subcode &= ~GF_OMP_TEAMS_GRID_PHONY;
}

/* Return the clauses associated with OMP_SECTIONS GS.  */

static inline tree
gimple_omp_sections_clauses (const gimple *gs)
{
  const gomp_sections *omp_sections_stmt = as_a <const gomp_sections *> (gs);
  return omp_sections_stmt->clauses;
}


/* Return a pointer to the clauses associated with OMP_SECTIONS GS.  */

static inline tree *
gimple_omp_sections_clauses_ptr (gimple *gs)
{
  gomp_sections *omp_sections_stmt = as_a <gomp_sections *> (gs);
  return &omp_sections_stmt->clauses;
}


/* Set CLAUSES to be the set of clauses associated with OMP_SECTIONS
   GS.  */

static inline void
gimple_omp_sections_set_clauses (gimple *gs, tree clauses)
{
  gomp_sections *omp_sections_stmt = as_a <gomp_sections *> (gs);
  omp_sections_stmt->clauses = clauses;
}


/* Return the control variable associated with the GIMPLE_OMP_SECTIONS
   in GS.  */

static inline tree
gimple_omp_sections_control (const gimple *gs)
{
  const gomp_sections *omp_sections_stmt = as_a <const gomp_sections *> (gs);
  return omp_sections_stmt->control;
}


/* Return a pointer to the clauses associated with the GIMPLE_OMP_SECTIONS
   GS.  */

static inline tree *
gimple_omp_sections_control_ptr (gimple *gs)
{
  gomp_sections *omp_sections_stmt = as_a <gomp_sections *> (gs);
  return &omp_sections_stmt->control;
}


/* Set CONTROL to be the set of clauses associated with the
   GIMPLE_OMP_SECTIONS in GS.  */

static inline void
gimple_omp_sections_set_control (gimple *gs, tree control)
{
  gomp_sections *omp_sections_stmt = as_a <gomp_sections *> (gs);
  omp_sections_stmt->control = control;
}


/* Set the value being stored in an atomic store.  */

static inline void
gimple_omp_atomic_store_set_val (gomp_atomic_store *store_stmt, tree val)
{
  store_stmt->val = val;
}


/* Return the value being stored in an atomic store.  */

static inline tree
gimple_omp_atomic_store_val (const gomp_atomic_store *store_stmt)
{
  return store_stmt->val;
}


/* Return a pointer to the value being stored in an atomic store.  */

static inline tree *
gimple_omp_atomic_store_val_ptr (gomp_atomic_store *store_stmt)
{
  return &store_stmt->val;
}


/* Set the LHS of an atomic load.  */

static inline void
gimple_omp_atomic_load_set_lhs (gomp_atomic_load *load_stmt, tree lhs)
{
  load_stmt->lhs = lhs;
}


/* Get the LHS of an atomic load.  */

static inline tree
gimple_omp_atomic_load_lhs (const gomp_atomic_load *load_stmt)
{
  return load_stmt->lhs;
}


/* Return a pointer to the LHS of an atomic load.  */

static inline tree *
gimple_omp_atomic_load_lhs_ptr (gomp_atomic_load *load_stmt)
{
  return &load_stmt->lhs;
}


/* Set the RHS of an atomic load.  */

static inline void
gimple_omp_atomic_load_set_rhs (gomp_atomic_load *load_stmt, tree rhs)
{
  load_stmt->rhs = rhs;
}


/* Get the RHS of an atomic load.  */

static inline tree
gimple_omp_atomic_load_rhs (const gomp_atomic_load *load_stmt)
{
  return load_stmt->rhs;
}


/* Return a pointer to the RHS of an atomic load.  */

static inline tree *
gimple_omp_atomic_load_rhs_ptr (gomp_atomic_load *load_stmt)
{
  return &load_stmt->rhs;
}


/* Get the definition of the control variable in a GIMPLE_OMP_CONTINUE.  */

static inline tree
gimple_omp_continue_control_def (const gomp_continue *cont_stmt)
{
  return cont_stmt->control_def;
}

/* The same as above, but return the address.  */

static inline tree *
gimple_omp_continue_control_def_ptr (gomp_continue *cont_stmt)
{
  return &cont_stmt->control_def;
}

/* Set the definition of the control variable in a GIMPLE_OMP_CONTINUE.  */

static inline void
gimple_omp_continue_set_control_def (gomp_continue *cont_stmt, tree def)
{
  cont_stmt->control_def = def;
}


/* Get the use of the control variable in a GIMPLE_OMP_CONTINUE.  */

static inline tree
gimple_omp_continue_control_use (const gomp_continue *cont_stmt)
{
  return cont_stmt->control_use;
}


/* The same as above, but return the address.  */

static inline tree *
gimple_omp_continue_control_use_ptr (gomp_continue *cont_stmt)
{
  return &cont_stmt->control_use;
}


/* Set the use of the control variable in a GIMPLE_OMP_CONTINUE.  */

static inline void
gimple_omp_continue_set_control_use (gomp_continue *cont_stmt, tree use)
{
  cont_stmt->control_use = use;
}

/* Return a pointer to the body for the GIMPLE_TRANSACTION statement
   TRANSACTION_STMT.  */

static inline gimple_seq *
gimple_transaction_body_ptr (gtransaction *transaction_stmt)
{
  return &transaction_stmt->body;
}

/* Return the body for the GIMPLE_TRANSACTION statement TRANSACTION_STMT.  */

static inline gimple_seq
gimple_transaction_body (gtransaction *transaction_stmt)
{
  return transaction_stmt->body;
}

/* Return the label associated with a GIMPLE_TRANSACTION.  */

static inline tree
gimple_transaction_label_norm (const gtransaction *transaction_stmt)
{
  return transaction_stmt->label_norm;
}

static inline tree *
gimple_transaction_label_norm_ptr (gtransaction *transaction_stmt)
{
  return &transaction_stmt->label_norm;
}

static inline tree
gimple_transaction_label_uninst (const gtransaction *transaction_stmt)
{
  return transaction_stmt->label_uninst;
}

static inline tree *
gimple_transaction_label_uninst_ptr (gtransaction *transaction_stmt)
{
  return &transaction_stmt->label_uninst;
}

static inline tree
gimple_transaction_label_over (const gtransaction *transaction_stmt)
{
  return transaction_stmt->label_over;
}

static inline tree *
gimple_transaction_label_over_ptr (gtransaction *transaction_stmt)
{
  return &transaction_stmt->label_over;
}

/* Return the subcode associated with a GIMPLE_TRANSACTION.  */

static inline unsigned int
gimple_transaction_subcode (const gtransaction *transaction_stmt)
{
  return transaction_stmt->subcode;
}

/* Set BODY to be the body for the GIMPLE_TRANSACTION statement
   TRANSACTION_STMT.  */

static inline void
gimple_transaction_set_body (gtransaction *transaction_stmt,
			     gimple_seq body)
{
  transaction_stmt->body = body;
}

/* Set the label associated with a GIMPLE_TRANSACTION.  */

static inline void
gimple_transaction_set_label_norm (gtransaction *transaction_stmt, tree label)
{
  transaction_stmt->label_norm = label;
}

static inline void
gimple_transaction_set_label_uninst (gtransaction *transaction_stmt, tree label)
{
  transaction_stmt->label_uninst = label;
}

static inline void
gimple_transaction_set_label_over (gtransaction *transaction_stmt, tree label)
{
  transaction_stmt->label_over = label;
}

/* Set the subcode associated with a GIMPLE_TRANSACTION.  */

static inline void
gimple_transaction_set_subcode (gtransaction *transaction_stmt,
				unsigned int subcode)
{
  transaction_stmt->subcode = subcode;
}

/* Return a pointer to the return value for GIMPLE_RETURN GS.  */

static inline tree *
gimple_return_retval_ptr (greturn *gs)
{
  return &gs->op[0];
}

/* Return the return value for GIMPLE_RETURN GS.  */

static inline tree
gimple_return_retval (const greturn *gs)
{
  return gs->op[0];
}


/* Set RETVAL to be the return value for GIMPLE_RETURN GS.  */

static inline void
gimple_return_set_retval (greturn *gs, tree retval)
{
  gs->op[0] = retval;
}


/* Return the return bounds for GIMPLE_RETURN GS.  */

static inline tree
gimple_return_retbnd (const gimple *gs)
{
  GIMPLE_CHECK (gs, GIMPLE_RETURN);
  return gimple_op (gs, 1);
}


/* Set RETVAL to be the return bounds for GIMPLE_RETURN GS.  */

static inline void
gimple_return_set_retbnd (gimple *gs, tree retval)
{
  GIMPLE_CHECK (gs, GIMPLE_RETURN);
  gimple_set_op (gs, 1, retval);
}


/* Returns true when the gimple statement STMT is any of the OMP types.  */

#define CASE_GIMPLE_OMP				\
    case GIMPLE_OMP_PARALLEL:			\
    case GIMPLE_OMP_TASK:			\
    case GIMPLE_OMP_FOR:			\
    case GIMPLE_OMP_SECTIONS:			\
    case GIMPLE_OMP_SECTIONS_SWITCH:		\
    case GIMPLE_OMP_SINGLE:			\
    case GIMPLE_OMP_TARGET:			\
    case GIMPLE_OMP_TEAMS:			\
    case GIMPLE_OMP_SECTION:			\
    case GIMPLE_OMP_MASTER:			\
    case GIMPLE_OMP_TASKGROUP:			\
    case GIMPLE_OMP_ORDERED:			\
    case GIMPLE_OMP_CRITICAL:			\
    case GIMPLE_OMP_RETURN:			\
    case GIMPLE_OMP_ATOMIC_LOAD:		\
    case GIMPLE_OMP_ATOMIC_STORE:		\
    case GIMPLE_OMP_CONTINUE:			\
    case GIMPLE_OMP_GRID_BODY

static inline bool
is_gimple_omp (const gimple *stmt)
{
  switch (gimple_code (stmt))
    {
    CASE_GIMPLE_OMP:
      return true;
    default:
      return false;
    }
}

/* Return true if the OMP gimple statement STMT is any of the OpenACC types
   specifically.  */

static inline bool
is_gimple_omp_oacc (const gimple *stmt)
{
  gcc_assert (is_gimple_omp (stmt));
  switch (gimple_code (stmt))
    {
    case GIMPLE_OMP_FOR:
      switch (gimple_omp_for_kind (stmt))
	{
	case GF_OMP_FOR_KIND_OACC_LOOP:
	  return true;
	default:
	  return false;
	}
    case GIMPLE_OMP_TARGET:
      switch (gimple_omp_target_kind (stmt))
	{
	case GF_OMP_TARGET_KIND_OACC_PARALLEL:
	case GF_OMP_TARGET_KIND_OACC_KERNELS:
	case GF_OMP_TARGET_KIND_OACC_DATA:
	case GF_OMP_TARGET_KIND_OACC_UPDATE:
	case GF_OMP_TARGET_KIND_OACC_ENTER_EXIT_DATA:
	case GF_OMP_TARGET_KIND_OACC_DECLARE:
	case GF_OMP_TARGET_KIND_OACC_HOST_DATA:
	  return true;
	default:
	  return false;
	}
    default:
      return false;
    }
}


/* Return true if the OMP gimple statement STMT is offloaded.  */

static inline bool
is_gimple_omp_offloaded (const gimple *stmt)
{
  gcc_assert (is_gimple_omp (stmt));
  switch (gimple_code (stmt))
    {
    case GIMPLE_OMP_TARGET:
      switch (gimple_omp_target_kind (stmt))
	{
	case GF_OMP_TARGET_KIND_REGION:
	case GF_OMP_TARGET_KIND_OACC_PARALLEL:
	case GF_OMP_TARGET_KIND_OACC_KERNELS:
	  return true;
	default:
	  return false;
	}
    default:
      return false;
    }
}


/* Returns TRUE if statement G is a GIMPLE_NOP.  */

static inline bool
gimple_nop_p (const gimple *g)
{
  return gimple_code (g) == GIMPLE_NOP;
}


/* Return true if GS is a GIMPLE_RESX.  */

static inline bool
is_gimple_resx (const gimple *gs)
{
  return gimple_code (gs) == GIMPLE_RESX;
}

/* Return the type of the main expression computed by STMT.  Return
   void_type_node if the statement computes nothing.  */

static inline tree
gimple_expr_type (const gimple *stmt)
{
  enum gimple_code code = gimple_code (stmt);
  /* In general we want to pass out a type that can be substituted
     for both the RHS and the LHS types if there is a possibly
     useless conversion involved.  That means returning the
     original RHS type as far as we can reconstruct it.  */
  if (code == GIMPLE_CALL)
    {
      const gcall *call_stmt = as_a <const gcall *> (stmt);
      if (gimple_call_internal_p (call_stmt))
	switch (gimple_call_internal_fn (call_stmt))
	  {
	  case IFN_MASK_STORE:
	  case IFN_SCATTER_STORE:
	    return TREE_TYPE (gimple_call_arg (call_stmt, 3));
	  case IFN_MASK_SCATTER_STORE:
	    return TREE_TYPE (gimple_call_arg (call_stmt, 4));
	  default:
	    break;
	  }
      return gimple_call_return_type (call_stmt);
    }
  else if (code == GIMPLE_ASSIGN)
    {
      if (gimple_assign_rhs_code (stmt) == POINTER_PLUS_EXPR)
        return TREE_TYPE (gimple_assign_rhs1 (stmt));
      else
        /* As fallback use the type of the LHS.  */
        return TREE_TYPE (gimple_get_lhs (stmt));
    }
  else if (code == GIMPLE_COND)
    return boolean_type_node;
  else
    return void_type_node;
}

/* Enum and arrays used for allocation stats.  Keep in sync with
   gimple.c:gimple_alloc_kind_names.  */
enum gimple_alloc_kind
{
  gimple_alloc_kind_assign,	/* Assignments.  */
  gimple_alloc_kind_phi,	/* PHI nodes.  */
  gimple_alloc_kind_cond,	/* Conditionals.  */
  gimple_alloc_kind_rest,	/* Everything else.  */
  gimple_alloc_kind_all
};

extern uint64_t gimple_alloc_counts[];
extern uint64_t gimple_alloc_sizes[];

/* Return the allocation kind for a given stmt CODE.  */
static inline enum gimple_alloc_kind
gimple_alloc_kind (enum gimple_code code)
{
  switch (code)
    {
      case GIMPLE_ASSIGN:
	return gimple_alloc_kind_assign;
      case GIMPLE_PHI:
	return gimple_alloc_kind_phi;
      case GIMPLE_COND:
	return gimple_alloc_kind_cond;
      default:
	return gimple_alloc_kind_rest;
    }
}

/* Return true if a location should not be emitted for this statement
   by annotate_all_with_location.  */

static inline bool
gimple_do_not_emit_location_p (gimple *g)
{
  return gimple_plf (g, GF_PLF_1);
}

/* Mark statement G so a location will not be emitted by
   annotate_one_with_location.  */

static inline void
gimple_set_do_not_emit_location (gimple *g)
{
  /* The PLF flags are initialized to 0 when a new tuple is created,
     so no need to initialize it anywhere.  */
  gimple_set_plf (g, GF_PLF_1, true);
}


/* Macros for showing usage statistics.  */
#define SCALE(x) ((unsigned long) ((x) < 1024*10	\
		  ? (x)					\
		  : ((x) < 1024*1024*10			\
		     ? (x) / 1024			\
		     : (x) / (1024*1024))))

#define LABEL(x) ((x) < 1024*10 ? 'b' : ((x) < 1024*1024*10 ? 'k' : 'M'))

#endif  /* GCC_GIMPLE_H */
