/* Declarations for rtx-reader support for gen* routines.
   Copyright (C) 2000-2018 Free Software Foundation, Inc.

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

#ifndef GCC_GENSUPPORT_H
#define GCC_GENSUPPORT_H

#include "read-md.h"

struct obstack;
extern struct obstack *rtl_obstack;

/* Information about an .md define_* rtx.  */
struct md_rtx_info {
  /* The rtx itself.  */
  rtx def;

  /* The location of the first line of the rtx.  */
  file_location loc;

  /* The unique number attached to the rtx.  Currently all define_insns,
     define_expands, define_splits, define_peepholes and define_peephole2s
     share the same insn_code index space.  */
  int index;
};

#define OPTAB_CL(name, pat, c, b, l)		name,
#define OPTAB_CX(name, pat)
#define OPTAB_CD(name, pat)			name,
#define OPTAB_NL(name, pat, c, b, s, l)		name,
#define OPTAB_NC(name, pat, c)			name,
#define OPTAB_NX(name, pat)
#define OPTAB_VL(name, pat, c, b, s, l)		name,
#define OPTAB_VC(name, pat, c)			name,
#define OPTAB_VX(name, pat)
#define OPTAB_DC(name, pat, c)			name,
#define OPTAB_D(name, pat)			name,

/* Enumerates all optabs.  */
typedef enum optab_tag {
  unknown_optab,
#include "optabs.def"
  NUM_OPTABS
} optab;

#undef OPTAB_CL
#undef OPTAB_CX
#undef OPTAB_CD
#undef OPTAB_NL
#undef OPTAB_NC
#undef OPTAB_NX
#undef OPTAB_VL
#undef OPTAB_VC
#undef OPTAB_VX
#undef OPTAB_DC
#undef OPTAB_D

/* Describes one entry in optabs.def.  */
struct optab_def
{
  /* The name of the optab (e.g. "add_optab").  */
  const char *name;

  /* The pattern that matching define_expands and define_insns have.
     See the comment at the head of optabs.def for details.  */
  const char *pattern;

  /* The initializers (in the form of C code) for the libcall_basename,
     libcall_suffix and libcall_gen fields of (convert_)optab_libcall_d.  */
  const char *base;
  const char *suffix;
  const char *libcall;

  /* The optab's enum value.  */
  unsigned int op;

  /* The value returned by optab_to_code (OP).  */
  enum rtx_code fcode;

  /* CODE if code_to_optab (CODE) should return OP, otherwise UNKNOWN.  */
  enum rtx_code rcode;

  /* 1: conversion optabs with libcall data,
     2: conversion optabs without libcall data,
     3: non-conversion optabs with libcall data ("normal" and "overflow"
        optabs in the optabs.def comment)
     4: non-conversion optabs without libcall data ("direct" optabs).  */
  unsigned int kind;
};

extern optab_def optabs[];
extern unsigned int num_optabs;

/* Information about an instruction name that matches an optab pattern.  */
struct optab_pattern
{
  /* The name of the instruction.  */
  const char *name;

  /* The matching optab.  */
  unsigned int op;

  /* The optab modes.  M2 is only significant for conversion optabs;
     it is zero otherwise.  */
  unsigned int m1, m2;

  /* An index that provides a lexicographical sort of (OP, M2, M1).
     Used by genopinit.c.  */
  unsigned int sort_num;
};

extern rtx add_implicit_parallel (rtvec);
extern rtx_reader *init_rtx_reader_args_cb (int, const char **,
					    bool (*)(const char *));
extern rtx_reader *init_rtx_reader_args (int, const char **);
extern bool read_md_rtx (md_rtx_info *);
extern unsigned int get_num_insn_codes ();

/* Set this to 0 to disable automatic elision of insn patterns which
   can never be used in this configuration.  See genconditions.c.
   Must be set before calling init_md_reader.  */
extern int insn_elision;

/* Return the C test that says whether a definition rtx can be used,
   or "" if it can be used unconditionally.  */
extern const char *get_c_test (rtx);

/* If the C test passed as the argument can be evaluated at compile
   time, return its truth value; else return -1.  The test must have
   appeared somewhere in the machine description when genconditions
   was run.  */
extern int maybe_eval_c_test (const char *);

/* Add an entry to the table of conditions.  Used by genconditions and
   by read-rtl.c.  */
extern void add_c_test (const char *, int);

/* This structure is used internally by gensupport.c and genconditions.c.  */
struct c_test
{
  const char *expr;
  int value;
};

#ifdef __HASHTAB_H__
extern hashval_t hash_c_test (const void *);
extern int cmp_c_test (const void *, const void *);
extern void traverse_c_tests (htab_trav, void *);
#endif

/* Predicate handling: helper functions and data structures.  */

struct pred_data
{
  struct pred_data *next;	/* for iterating over the set of all preds */
  const char *name;		/* predicate name */
  bool special;			/* special handling of modes? */

  /* data used primarily by genpreds.c */
  const char *c_block;		/* C test block */
  rtx exp;			/* RTL test expression */

  /* data used primarily by genrecog.c */
  enum rtx_code singleton;	/* if pred takes only one code, that code */
  int num_codes;		/* number of codes accepted */
  bool allows_non_lvalue;	/* if pred allows non-lvalue expressions */
  bool allows_non_const;	/* if pred allows non-const expressions */
  bool codes[NUM_RTX_CODE];	/* set of codes accepted */
};

extern struct pred_data *first_predicate;
extern struct pred_data *lookup_predicate (const char *);
extern void add_predicate_code (struct pred_data *, enum rtx_code);
extern void add_predicate (struct pred_data *);

#define FOR_ALL_PREDICATES(p) for (p = first_predicate; p; p = p->next)

struct pattern_stats
{
  /* The largest match_operand, match_operator or match_parallel
     number found.  */
  int max_opno;

  /* The largest match_dup, match_op_dup or match_par_dup number found.  */
  int max_dup_opno;

  /* The smallest and largest match_scratch number found.  */
  int min_scratch_opno;
  int max_scratch_opno;

  /* The number of times match_dup, match_op_dup or match_par_dup appears
     in the pattern.  */
  int num_dups;

  /* The number of rtx arguments to the generator function.  */
  int num_generator_args;

  /* The number of rtx operands in an insn.  */
  int num_insn_operands;

  /* The number of operand variables that are needed.  */
  int num_operand_vars;
};

extern void get_pattern_stats (struct pattern_stats *ranges, rtvec vec);
extern void compute_test_codes (rtx, file_location, char *);
extern file_location get_file_location (rtx);
extern const char *get_emit_function (rtx);
extern bool needs_barrier_p (rtx);
extern bool find_optab (optab_pattern *, const char *);

#endif /* GCC_GENSUPPORT_H */
