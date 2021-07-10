/* Declarations for interface to insn recognizer and insn-output.c.
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

#ifndef GCC_RECOG_H
#define GCC_RECOG_H

/* Random number that should be large enough for all purposes.  Also define
   a type that has at least MAX_RECOG_ALTERNATIVES + 1 bits, with the extra
   bit giving an invalid value that can be used to mean "uninitialized".  */
#define MAX_RECOG_ALTERNATIVES 35
typedef uint64_t alternative_mask;

/* A mask of all alternatives.  */
#define ALL_ALTERNATIVES ((alternative_mask) -1)

/* A mask containing just alternative X.  */
#define ALTERNATIVE_BIT(X) ((alternative_mask) 1 << (X))

/* Types of operands.  */
enum op_type {
  OP_IN,
  OP_OUT,
  OP_INOUT
};

struct operand_alternative
{
  /* Pointer to the beginning of the constraint string for this alternative,
     for easier access by alternative number.  */
  const char *constraint;

  /* The register class valid for this alternative (possibly NO_REGS).  */
  ENUM_BITFIELD (reg_class) cl : 16;

  /* "Badness" of this alternative, computed from number of '?' and '!'
     characters in the constraint string.  */
  unsigned int reject : 16;

  /* -1 if no matching constraint was found, or an operand number.  */
  int matches : 8;
  /* The same information, but reversed: -1 if this operand is not
     matched by any other, or the operand number of the operand that
     matches this one.  */
  int matched : 8;

  /* Nonzero if '&' was found in the constraint string.  */
  unsigned int earlyclobber : 1;
  /* Nonzero if TARGET_MEM_CONSTRAINT was found in the constraint
     string.  */
  unsigned int memory_ok : 1;
  /* Nonzero if 'p' was found in the constraint string.  */
  unsigned int is_address : 1;
  /* Nonzero if 'X' was found in the constraint string, or if the constraint
     string for this alternative was empty.  */
  unsigned int anything_ok : 1;

  unsigned int unused : 12;
};

/* Return the class for operand I of alternative ALT, taking matching
   constraints into account.  */

static inline enum reg_class
alternative_class (const operand_alternative *alt, int i)
{
  return alt[i].matches >= 0 ? alt[alt[i].matches].cl : alt[i].cl;
}

extern void init_recog (void);
extern void init_recog_no_volatile (void);
extern int check_asm_operands (rtx);
extern int asm_operand_ok (rtx, const char *, const char **);
extern bool validate_change (rtx, rtx *, rtx, bool);
extern bool validate_unshare_change (rtx, rtx *, rtx, bool);
extern bool canonicalize_change_group (rtx_insn *insn, rtx x);
extern int insn_invalid_p (rtx_insn *, bool);
extern int verify_changes (int);
extern void confirm_change_group (void);
extern int apply_change_group (void);
extern int num_validated_changes (void);
extern void cancel_changes (int);
extern int constrain_operands (int, alternative_mask);
extern int constrain_operands_cached (rtx_insn *, int);
extern int memory_address_addr_space_p (machine_mode, rtx, addr_space_t);
#define memory_address_p(mode,addr) \
	memory_address_addr_space_p ((mode), (addr), ADDR_SPACE_GENERIC)
extern int strict_memory_address_addr_space_p (machine_mode, rtx,
					       addr_space_t);
#define strict_memory_address_p(mode,addr) \
	strict_memory_address_addr_space_p ((mode), (addr), ADDR_SPACE_GENERIC)
extern int validate_replace_rtx_subexp (rtx, rtx, rtx_insn *, rtx *);
extern int validate_replace_rtx (rtx, rtx, rtx_insn *);
extern int validate_replace_rtx_part (rtx, rtx, rtx *, rtx_insn *);
extern int validate_replace_rtx_part_nosimplify (rtx, rtx, rtx *, rtx_insn *);
extern void validate_replace_rtx_group (rtx, rtx, rtx_insn *);
extern void validate_replace_src_group (rtx, rtx, rtx_insn *);
extern bool validate_simplify_insn (rtx_insn *insn);
extern int num_changes_pending (void);
extern int next_insn_tests_no_inequality (rtx_insn *);
extern bool reg_fits_class_p (const_rtx, reg_class_t, int, machine_mode);

extern int offsettable_memref_p (rtx);
extern int offsettable_nonstrict_memref_p (rtx);
extern int offsettable_address_addr_space_p (int, machine_mode, rtx,
					     addr_space_t);
#define offsettable_address_p(strict,mode,addr) \
	offsettable_address_addr_space_p ((strict), (mode), (addr), \
					  ADDR_SPACE_GENERIC)
extern bool mode_dependent_address_p (rtx, addr_space_t);

extern int recog (rtx, rtx_insn *, int *);
#ifndef GENERATOR_FILE
static inline int recog_memoized (rtx_insn *insn);
#endif
extern void add_clobbers (rtx, int);
extern int added_clobbers_hard_reg_p (int);
extern void insn_extract (rtx_insn *);
extern void extract_insn (rtx_insn *);
extern void extract_constrain_insn (rtx_insn *insn);
extern void extract_constrain_insn_cached (rtx_insn *);
extern void extract_insn_cached (rtx_insn *);
extern void preprocess_constraints (int, int, const char **,
				    operand_alternative *, rtx **);
extern const operand_alternative *preprocess_insn_constraints (unsigned int);
extern void preprocess_constraints (rtx_insn *);
extern rtx_insn *peep2_next_insn (int);
extern int peep2_regno_dead_p (int, int);
extern int peep2_reg_dead_p (int, rtx);
#ifdef CLEAR_HARD_REG_SET
extern rtx peep2_find_free_register (int, int, const char *,
				     machine_mode, HARD_REG_SET *);
#endif
extern rtx_insn *peephole2_insns (rtx, rtx_insn *, int *);

extern int store_data_bypass_p (rtx_insn *, rtx_insn *);
extern int if_test_bypass_p (rtx_insn *, rtx_insn *);

#ifndef GENERATOR_FILE
/* Try recognizing the instruction INSN,
   and return the code number that results.
   Remember the code so that repeated calls do not
   need to spend the time for actual rerecognition.

   This function is the normal interface to instruction recognition.
   The automatically-generated function `recog' is normally called
   through this one.  */

static inline int
recog_memoized (rtx_insn *insn)
{
  if (INSN_CODE (insn) < 0)
    INSN_CODE (insn) = recog (PATTERN (insn), insn, 0);
  return INSN_CODE (insn);
}
#endif

/* Skip chars until the next ',' or the end of the string.  This is
   useful to skip alternatives in a constraint string.  */
static inline const char *
skip_alternative (const char *p)
{
  const char *r = p;
  while (*r != '\0' && *r != ',')
    r++;
  if (*r == ',')
    r++;
  return r;
}

/* Nonzero means volatile operands are recognized.  */
extern int volatile_ok;

/* Set by constrain_operands to the number of the alternative that
   matched.  */
extern int which_alternative;

/* The following vectors hold the results from insn_extract.  */

struct recog_data_d
{
  /* It is very tempting to make the 5 operand related arrays into a
     structure and index on that.  However, to be source compatible
     with all of the existing md file insn constraints and output
     templates, we need `operand' as a flat array.  Without that
     member, making an array for the rest seems pointless.  */

  /* Gives value of operand N.  */
  rtx operand[MAX_RECOG_OPERANDS];

  /* Gives location where operand N was found.  */
  rtx *operand_loc[MAX_RECOG_OPERANDS];

  /* Gives the constraint string for operand N.  */
  const char *constraints[MAX_RECOG_OPERANDS];

  /* Nonzero if operand N is a match_operator or a match_parallel.  */
  char is_operator[MAX_RECOG_OPERANDS];

  /* Gives the mode of operand N.  */
  machine_mode operand_mode[MAX_RECOG_OPERANDS];

  /* Gives the type (in, out, inout) for operand N.  */
  enum op_type operand_type[MAX_RECOG_OPERANDS];

  /* Gives location where the Nth duplicate-appearance of an operand
     was found.  This is something that matched MATCH_DUP.  */
  rtx *dup_loc[MAX_DUP_OPERANDS];

  /* Gives the operand number that was duplicated in the Nth
     duplicate-appearance of an operand.  */
  char dup_num[MAX_DUP_OPERANDS];

  /* ??? Note that these are `char' instead of `unsigned char' to (try to)
     avoid certain lossage from K&R C, wherein `unsigned char' default
     promotes to `unsigned int' instead of `int' as in ISO C.  As of 1999,
     the most common places to bootstrap from K&R C are SunOS and HPUX,
     both of which have signed characters by default.  The only other
     supported natives that have both K&R C and unsigned characters are
     ROMP and Irix 3, and neither have been seen for a while, but do
     continue to consider unsignedness when performing arithmetic inside
     a comparison.  */

  /* The number of operands of the insn.  */
  char n_operands;

  /* The number of MATCH_DUPs in the insn.  */
  char n_dups;

  /* The number of alternatives in the constraints for the insn.  */
  char n_alternatives;

  /* True if insn is ASM_OPERANDS.  */
  bool is_asm;

  /* In case we are caching, hold insn data was generated for.  */
  rtx_insn *insn;
};

extern struct recog_data_d recog_data;

extern const operand_alternative *recog_op_alt;

/* Return a pointer to an array in which index OP describes the constraints
   on operand OP of the current instruction alternative (which_alternative).
   Only valid after calling preprocess_constraints and constrain_operands.  */

inline static const operand_alternative *
which_op_alt ()
{
  gcc_checking_assert (IN_RANGE (which_alternative, 0,
				 recog_data.n_alternatives - 1));
  return &recog_op_alt[which_alternative * recog_data.n_operands];
}

/* A table defined in insn-output.c that give information about
   each insn-code value.  */

typedef int (*insn_operand_predicate_fn) (rtx, machine_mode);
typedef const char * (*insn_output_fn) (rtx *, rtx_insn *);

struct insn_gen_fn
{
  typedef rtx_insn * (*f0) (void);
  typedef rtx_insn * (*f1) (rtx);
  typedef rtx_insn * (*f2) (rtx, rtx);
  typedef rtx_insn * (*f3) (rtx, rtx, rtx);
  typedef rtx_insn * (*f4) (rtx, rtx, rtx, rtx);
  typedef rtx_insn * (*f5) (rtx, rtx, rtx, rtx, rtx);
  typedef rtx_insn * (*f6) (rtx, rtx, rtx, rtx, rtx, rtx);
  typedef rtx_insn * (*f7) (rtx, rtx, rtx, rtx, rtx, rtx, rtx);
  typedef rtx_insn * (*f8) (rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx);
  typedef rtx_insn * (*f9) (rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx);
  typedef rtx_insn * (*f10) (rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx);
  typedef rtx_insn * (*f11) (rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx);
  typedef rtx_insn * (*f12) (rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx);
  typedef rtx_insn * (*f13) (rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx);
  typedef rtx_insn * (*f14) (rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx);
  typedef rtx_insn * (*f15) (rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx);
  typedef rtx_insn * (*f16) (rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx);

  typedef void (*stored_funcptr) (void);

  rtx_insn * operator () (void) const { return ((f0)func) (); }
  rtx_insn * operator () (rtx a0) const { return ((f1)func) (a0); }
  rtx_insn * operator () (rtx a0, rtx a1) const { return ((f2)func) (a0, a1); }
  rtx_insn * operator () (rtx a0, rtx a1, rtx a2) const { return ((f3)func) (a0, a1, a2); }
  rtx_insn * operator () (rtx a0, rtx a1, rtx a2, rtx a3) const { return ((f4)func) (a0, a1, a2, a3); }
  rtx_insn * operator () (rtx a0, rtx a1, rtx a2, rtx a3, rtx a4) const { return ((f5)func) (a0, a1, a2, a3, a4); }
  rtx_insn * operator () (rtx a0, rtx a1, rtx a2, rtx a3, rtx a4, rtx a5) const { return ((f6)func) (a0, a1, a2, a3, a4, a5); }
  rtx_insn * operator () (rtx a0, rtx a1, rtx a2, rtx a3, rtx a4, rtx a5, rtx a6) const { return ((f7)func) (a0, a1, a2, a3, a4, a5, a6); }
  rtx_insn * operator () (rtx a0, rtx a1, rtx a2, rtx a3, rtx a4, rtx a5, rtx a6, rtx a7) const { return ((f8)func) (a0, a1, a2, a3, a4, a5, a6, a7); }
  rtx_insn * operator () (rtx a0, rtx a1, rtx a2, rtx a3, rtx a4, rtx a5, rtx a6, rtx a7, rtx a8) const { return ((f9)func) (a0, a1, a2, a3, a4, a5, a6, a7, a8); }
  rtx_insn * operator () (rtx a0, rtx a1, rtx a2, rtx a3, rtx a4, rtx a5, rtx a6, rtx a7, rtx a8, rtx a9) const { return ((f10)func) (a0, a1, a2, a3, a4, a5, a6, a7, a8, a9); }
  rtx_insn * operator () (rtx a0, rtx a1, rtx a2, rtx a3, rtx a4, rtx a5, rtx a6, rtx a7, rtx a8, rtx a9, rtx a10) const { return ((f11)func) (a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10); }
  rtx_insn * operator () (rtx a0, rtx a1, rtx a2, rtx a3, rtx a4, rtx a5, rtx a6, rtx a7, rtx a8, rtx a9, rtx a10, rtx a11) const { return ((f12)func) (a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11); }
  rtx_insn * operator () (rtx a0, rtx a1, rtx a2, rtx a3, rtx a4, rtx a5, rtx a6, rtx a7, rtx a8, rtx a9, rtx a10, rtx a11, rtx a12) const { return ((f13)func) (a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12); }
  rtx_insn * operator () (rtx a0, rtx a1, rtx a2, rtx a3, rtx a4, rtx a5, rtx a6, rtx a7, rtx a8, rtx a9, rtx a10, rtx a11, rtx a12, rtx a13) const { return ((f14)func) (a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13); }
  rtx_insn * operator () (rtx a0, rtx a1, rtx a2, rtx a3, rtx a4, rtx a5, rtx a6, rtx a7, rtx a8, rtx a9, rtx a10, rtx a11, rtx a12, rtx a13, rtx a14) const { return ((f15)func) (a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14); }
  rtx_insn * operator () (rtx a0, rtx a1, rtx a2, rtx a3, rtx a4, rtx a5, rtx a6, rtx a7, rtx a8, rtx a9, rtx a10, rtx a11, rtx a12, rtx a13, rtx a14, rtx a15) const { return ((f16)func) (a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15); }

  // This is for compatibility of code that invokes functions like
  //   (*funcptr) (arg)
  insn_gen_fn operator * (void) const { return *this; }

  // The wrapped function pointer must be public and there must not be any
  // constructors.  Otherwise the insn_data_d struct initializers generated
  // by genoutput.c will result in static initializer functions, which defeats
  // the purpose of the generated insn_data_d array.
  stored_funcptr func;
};

struct insn_operand_data
{
  const insn_operand_predicate_fn predicate;

  const char *const constraint;

  ENUM_BITFIELD(machine_mode) const mode : 16;

  const char strict_low;

  const char is_operator;

  const char eliminable;

  const char allows_mem;
};

/* Legal values for insn_data.output_format.  Indicate what type of data
   is stored in insn_data.output.  */
#define INSN_OUTPUT_FORMAT_NONE		0	/* abort */
#define INSN_OUTPUT_FORMAT_SINGLE	1	/* const char * */
#define INSN_OUTPUT_FORMAT_MULTI	2	/* const char * const * */
#define INSN_OUTPUT_FORMAT_FUNCTION	3	/* const char * (*)(...) */

struct insn_data_d
{
  const char *const name;
#if HAVE_DESIGNATED_UNION_INITIALIZERS
  union {
    const char *single;
    const char *const *multi;
    insn_output_fn function;
  } output;
#else
  struct {
    const char *single;
    const char *const *multi;
    insn_output_fn function;
  } output;
#endif
  const insn_gen_fn genfun;
  const struct insn_operand_data *const operand;

  const char n_generator_args;
  const char n_operands;
  const char n_dups;
  const char n_alternatives;
  const char output_format;
};

extern const struct insn_data_d insn_data[];
extern int peep2_current_count;

#ifndef GENERATOR_FILE
#include "insn-codes.h"

/* An enum of boolean attributes that may only depend on the current
   subtarget, not on things like operands or compiler phase.  */
enum bool_attr {
  BA_ENABLED,
  BA_PREFERRED_FOR_SPEED,
  BA_PREFERRED_FOR_SIZE,
  BA_LAST = BA_PREFERRED_FOR_SIZE
};

/* Target-dependent globals.  */
struct target_recog {
  bool x_initialized;
  alternative_mask x_bool_attr_masks[NUM_INSN_CODES][BA_LAST + 1];
  operand_alternative *x_op_alt[NUM_INSN_CODES];
};

extern struct target_recog default_target_recog;
#if SWITCHABLE_TARGET
extern struct target_recog *this_target_recog;
#else
#define this_target_recog (&default_target_recog)
#endif

alternative_mask get_enabled_alternatives (rtx_insn *);
alternative_mask get_preferred_alternatives (rtx_insn *);
alternative_mask get_preferred_alternatives (rtx_insn *, basic_block);
bool check_bool_attrs (rtx_insn *);

void recog_init ();
#endif

#endif /* GCC_RECOG_H */
