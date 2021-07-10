/* Register Transfer Language (RTL) definitions for GCC
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

#ifndef GCC_RTL_H
#define GCC_RTL_H

/* This file is occasionally included by generator files which expect
   machmode.h and other files to exist and would not normally have been
   included by coretypes.h.  */
#ifdef GENERATOR_FILE
#include "real.h"
#include "fixed-value.h"
#include "statistics.h"
#include "vec.h"
#include "hash-table.h"
#include "hash-set.h"
#include "input.h"
#include "is-a.h"
#endif  /* GENERATOR_FILE */

#include "hard-reg-set.h"

/* Value used by some passes to "recognize" noop moves as valid
 instructions.  */
#define NOOP_MOVE_INSN_CODE	INT_MAX

/* Register Transfer Language EXPRESSIONS CODES */

#define RTX_CODE	enum rtx_code
enum rtx_code  {

#define DEF_RTL_EXPR(ENUM, NAME, FORMAT, CLASS)   ENUM ,
#include "rtl.def"		/* rtl expressions are documented here */
#undef DEF_RTL_EXPR

  LAST_AND_UNUSED_RTX_CODE};	/* A convenient way to get a value for
				   NUM_RTX_CODE.
				   Assumes default enum value assignment.  */

/* The cast here, saves many elsewhere.  */
#define NUM_RTX_CODE ((int) LAST_AND_UNUSED_RTX_CODE)

/* Similar, but since generator files get more entries... */
#ifdef GENERATOR_FILE
# define NON_GENERATOR_NUM_RTX_CODE ((int) MATCH_OPERAND)
#endif

/* Register Transfer Language EXPRESSIONS CODE CLASSES */

enum rtx_class  {
  /* We check bit 0-1 of some rtx class codes in the predicates below.  */

  /* Bit 0 = comparison if 0, arithmetic is 1
     Bit 1 = 1 if commutative.  */
  RTX_COMPARE,		/* 0 */
  RTX_COMM_COMPARE,
  RTX_BIN_ARITH,
  RTX_COMM_ARITH,

  /* Must follow the four preceding values.  */
  RTX_UNARY,		/* 4 */

  RTX_EXTRA,
  RTX_MATCH,
  RTX_INSN,

  /* Bit 0 = 1 if constant.  */
  RTX_OBJ,		/* 8 */
  RTX_CONST_OBJ,

  RTX_TERNARY,
  RTX_BITFIELD_OPS,
  RTX_AUTOINC
};

#define RTX_OBJ_MASK (~1)
#define RTX_OBJ_RESULT (RTX_OBJ & RTX_OBJ_MASK)
#define RTX_COMPARE_MASK (~1)
#define RTX_COMPARE_RESULT (RTX_COMPARE & RTX_COMPARE_MASK)
#define RTX_ARITHMETIC_MASK (~1)
#define RTX_ARITHMETIC_RESULT (RTX_COMM_ARITH & RTX_ARITHMETIC_MASK)
#define RTX_BINARY_MASK (~3)
#define RTX_BINARY_RESULT (RTX_COMPARE & RTX_BINARY_MASK)
#define RTX_COMMUTATIVE_MASK (~2)
#define RTX_COMMUTATIVE_RESULT (RTX_COMM_COMPARE & RTX_COMMUTATIVE_MASK)
#define RTX_NON_COMMUTATIVE_RESULT (RTX_COMPARE & RTX_COMMUTATIVE_MASK)

extern const unsigned char rtx_length[NUM_RTX_CODE];
#define GET_RTX_LENGTH(CODE)		(rtx_length[(int) (CODE)])

extern const char * const rtx_name[NUM_RTX_CODE];
#define GET_RTX_NAME(CODE)		(rtx_name[(int) (CODE)])

extern const char * const rtx_format[NUM_RTX_CODE];
#define GET_RTX_FORMAT(CODE)		(rtx_format[(int) (CODE)])

extern const enum rtx_class rtx_class[NUM_RTX_CODE];
#define GET_RTX_CLASS(CODE)		(rtx_class[(int) (CODE)])

/* True if CODE is part of the insn chain (i.e. has INSN_UID, PREV_INSN
   and NEXT_INSN fields).  */
#define INSN_CHAIN_CODE_P(CODE) IN_RANGE (CODE, DEBUG_INSN, NOTE)

extern const unsigned char rtx_code_size[NUM_RTX_CODE];
extern const unsigned char rtx_next[NUM_RTX_CODE];

/* The flags and bitfields of an ADDR_DIFF_VEC.  BASE is the base label
   relative to which the offsets are calculated, as explained in rtl.def.  */
struct addr_diff_vec_flags
{
  /* Set at the start of shorten_branches - ONLY WHEN OPTIMIZING - : */
  unsigned min_align: 8;
  /* Flags: */
  unsigned base_after_vec: 1; /* BASE is after the ADDR_DIFF_VEC.  */
  unsigned min_after_vec: 1;  /* minimum address target label is
				 after the ADDR_DIFF_VEC.  */
  unsigned max_after_vec: 1;  /* maximum address target label is
				 after the ADDR_DIFF_VEC.  */
  unsigned min_after_base: 1; /* minimum address target label is
				 after BASE.  */
  unsigned max_after_base: 1; /* maximum address target label is
				 after BASE.  */
  /* Set by the actual branch shortening process - ONLY WHEN OPTIMIZING - : */
  unsigned offset_unsigned: 1; /* offsets have to be treated as unsigned.  */
  unsigned : 2;
  unsigned scale : 8;
};

/* Structure used to describe the attributes of a MEM.  These are hashed
   so MEMs that the same attributes share a data structure.  This means
   they cannot be modified in place.  */
struct GTY(()) mem_attrs
{
  mem_attrs ();

  /* The expression that the MEM accesses, or null if not known.
     This expression might be larger than the memory reference itself.
     (In other words, the MEM might access only part of the object.)  */
  tree expr;

  /* The offset of the memory reference from the start of EXPR.
     Only valid if OFFSET_KNOWN_P.  */
  poly_int64 offset;

  /* The size of the memory reference in bytes.  Only valid if
     SIZE_KNOWN_P.  */
  poly_int64 size;

  /* The alias set of the memory reference.  */
  alias_set_type alias;

  /* The alignment of the reference in bits.  Always a multiple of
     BITS_PER_UNIT.  Note that EXPR may have a stricter alignment
     than the memory reference itself.  */
  unsigned int align;

  /* The address space that the memory reference uses.  */
  unsigned char addrspace;

  /* True if OFFSET is known.  */
  bool offset_known_p;

  /* True if SIZE is known.  */
  bool size_known_p;
};

/* Structure used to describe the attributes of a REG in similar way as
   mem_attrs does for MEM above.  Note that the OFFSET field is calculated
   in the same way as for mem_attrs, rather than in the same way as a
   SUBREG_BYTE.  For example, if a big-endian target stores a byte
   object in the low part of a 4-byte register, the OFFSET field
   will be -3 rather than 0.  */

struct GTY((for_user)) reg_attrs {
  tree decl;			/* decl corresponding to REG.  */
  poly_int64 offset;		/* Offset from start of DECL.  */
};

/* Common union for an element of an rtx.  */

union rtunion
{
  int rt_int;
  unsigned int rt_uint;
  poly_uint16_pod rt_subreg;
  const char *rt_str;
  rtx rt_rtx;
  rtvec rt_rtvec;
  machine_mode rt_type;
  addr_diff_vec_flags rt_addr_diff_vec_flags;
  struct cselib_val *rt_cselib;
  tree rt_tree;
  basic_block rt_bb;
  mem_attrs *rt_mem;
  struct constant_descriptor_rtx *rt_constant;
  struct dw_cfi_node *rt_cfi;
};

/* Describes the properties of a REG.  */
struct GTY(()) reg_info {
  /* The value of REGNO.  */
  unsigned int regno;

  /* The value of REG_NREGS.  */
  unsigned int nregs : 8;
  unsigned int unused : 24;

  /* The value of REG_ATTRS.  */
  reg_attrs *attrs;
};

/* This structure remembers the position of a SYMBOL_REF within an
   object_block structure.  A SYMBOL_REF only provides this information
   if SYMBOL_REF_HAS_BLOCK_INFO_P is true.  */
struct GTY(()) block_symbol {
  /* The usual SYMBOL_REF fields.  */
  rtunion GTY ((skip)) fld[2];

  /* The block that contains this object.  */
  struct object_block *block;

  /* The offset of this object from the start of its block.  It is negative
     if the symbol has not yet been assigned an offset.  */
  HOST_WIDE_INT offset;
};

/* Describes a group of objects that are to be placed together in such
   a way that their relative positions are known.  */
struct GTY((for_user)) object_block {
  /* The section in which these objects should be placed.  */
  section *sect;

  /* The alignment of the first object, measured in bits.  */
  unsigned int alignment;

  /* The total size of the objects, measured in bytes.  */
  HOST_WIDE_INT size;

  /* The SYMBOL_REFs for each object.  The vector is sorted in
     order of increasing offset and the following conditions will
     hold for each element X:

	 SYMBOL_REF_HAS_BLOCK_INFO_P (X)
	 !SYMBOL_REF_ANCHOR_P (X)
	 SYMBOL_REF_BLOCK (X) == [address of this structure]
	 SYMBOL_REF_BLOCK_OFFSET (X) >= 0.  */
  vec<rtx, va_gc> *objects;

  /* All the anchor SYMBOL_REFs used to address these objects, sorted
     in order of increasing offset, and then increasing TLS model.
     The following conditions will hold for each element X in this vector:

	 SYMBOL_REF_HAS_BLOCK_INFO_P (X)
	 SYMBOL_REF_ANCHOR_P (X)
	 SYMBOL_REF_BLOCK (X) == [address of this structure]
	 SYMBOL_REF_BLOCK_OFFSET (X) >= 0.  */
  vec<rtx, va_gc> *anchors;
};

struct GTY((variable_size)) hwivec_def {
  HOST_WIDE_INT elem[1];
};

/* Number of elements of the HWIVEC if RTX is a CONST_WIDE_INT.  */
#define CWI_GET_NUM_ELEM(RTX)					\
  ((int)RTL_FLAG_CHECK1("CWI_GET_NUM_ELEM", (RTX), CONST_WIDE_INT)->u2.num_elem)
#define CWI_PUT_NUM_ELEM(RTX, NUM)					\
  (RTL_FLAG_CHECK1("CWI_PUT_NUM_ELEM", (RTX), CONST_WIDE_INT)->u2.num_elem = (NUM))

struct GTY((variable_size)) const_poly_int_def {
  trailing_wide_ints<NUM_POLY_INT_COEFFS> coeffs;
};

/* RTL expression ("rtx").  */

/* The GTY "desc" and "tag" options below are a kludge: we need a desc
   field for gengtype to recognize that inheritance is occurring,
   so that all subclasses are redirected to the traversal hook for the
   base class.
   However, all of the fields are in the base class, and special-casing
   is at work.  Hence we use desc and tag of 0, generating a switch
   statement of the form:
     switch (0)
       {
       case 0: // all the work happens here
      }
   in order to work with the existing special-casing in gengtype.  */

struct GTY((desc("0"), tag("0"),
	    chain_next ("RTX_NEXT (&%h)"),
	    chain_prev ("RTX_PREV (&%h)"))) rtx_def {
  /* The kind of expression this is.  */
  ENUM_BITFIELD(rtx_code) code: 16;

  /* The kind of value the expression has.  */
  ENUM_BITFIELD(machine_mode) mode : 8;

  /* 1 in a MEM if we should keep the alias set for this mem unchanged
     when we access a component.
     1 in a JUMP_INSN if it is a crossing jump.
     1 in a CALL_INSN if it is a sibling call.
     1 in a SET that is for a return.
     In a CODE_LABEL, part of the two-bit alternate entry field.
     1 in a CONCAT is VAL_EXPR_IS_COPIED in var-tracking.c.
     1 in a VALUE is SP_BASED_VALUE_P in cselib.c.
     1 in a SUBREG generated by LRA for reload insns.
     1 in a REG if this is a static chain register.
     1 in a CALL for calls instrumented by Pointer Bounds Checker.
     Dumped as "/j" in RTL dumps.  */
  unsigned int jump : 1;
  /* In a CODE_LABEL, part of the two-bit alternate entry field.
     1 in a MEM if it cannot trap.
     1 in a CALL_INSN logically equivalent to
       ECF_LOOPING_CONST_OR_PURE and DECL_LOOPING_CONST_OR_PURE_P.
     Dumped as "/c" in RTL dumps.  */
  unsigned int call : 1;
  /* 1 in a REG, MEM, or CONCAT if the value is set at most once, anywhere.
     1 in a SUBREG used for SUBREG_PROMOTED_UNSIGNED_P.
     1 in a SYMBOL_REF if it addresses something in the per-function
     constants pool.
     1 in a CALL_INSN logically equivalent to ECF_CONST and TREE_READONLY.
     1 in a NOTE, or EXPR_LIST for a const call.
     1 in a JUMP_INSN of an annulling branch.
     1 in a CONCAT is VAL_EXPR_IS_CLOBBERED in var-tracking.c.
     1 in a preserved VALUE is PRESERVED_VALUE_P in cselib.c.
     1 in a clobber temporarily created for LRA.
     Dumped as "/u" in RTL dumps.  */
  unsigned int unchanging : 1;
  /* 1 in a MEM or ASM_OPERANDS expression if the memory reference is volatile.
     1 in an INSN, CALL_INSN, JUMP_INSN, CODE_LABEL, BARRIER, or NOTE
     if it has been deleted.
     1 in a REG expression if corresponds to a variable declared by the user,
     0 for an internally generated temporary.
     1 in a SUBREG used for SUBREG_PROMOTED_UNSIGNED_P.
     1 in a LABEL_REF, REG_LABEL_TARGET or REG_LABEL_OPERAND note for a
     non-local label.
     In a SYMBOL_REF, this flag is used for machine-specific purposes.
     In a PREFETCH, this flag indicates that it should be considered a
     scheduling barrier.
     1 in a CONCAT is VAL_NEEDS_RESOLUTION in var-tracking.c.
     Dumped as "/v" in RTL dumps.  */
  unsigned int volatil : 1;
  /* 1 in a REG if the register is used only in exit code a loop.
     1 in a SUBREG expression if was generated from a variable with a
     promoted mode.
     1 in a CODE_LABEL if the label is used for nonlocal gotos
     and must not be deleted even if its count is zero.
     1 in an INSN, JUMP_INSN or CALL_INSN if this insn must be scheduled
     together with the preceding insn.  Valid only within sched.
     1 in an INSN, JUMP_INSN, or CALL_INSN if insn is in a delay slot and
     from the target of a branch.  Valid from reorg until end of compilation;
     cleared before used.

     The name of the field is historical.  It used to be used in MEMs
     to record whether the MEM accessed part of a structure.
     Dumped as "/s" in RTL dumps.  */
  unsigned int in_struct : 1;
  /* At the end of RTL generation, 1 if this rtx is used.  This is used for
     copying shared structure.  See `unshare_all_rtl'.
     In a REG, this is not needed for that purpose, and used instead
     in `leaf_renumber_regs_insn'.
     1 in a SYMBOL_REF, means that emit_library_call
     has used it as the function.
     1 in a CONCAT is VAL_HOLDS_TRACK_EXPR in var-tracking.c.
     1 in a VALUE or DEBUG_EXPR is VALUE_RECURSED_INTO in var-tracking.c.  */
  unsigned int used : 1;
  /* 1 in an INSN or a SET if this rtx is related to the call frame,
     either changing how we compute the frame address or saving and
     restoring registers in the prologue and epilogue.
     1 in a REG or MEM if it is a pointer.
     1 in a SYMBOL_REF if it addresses something in the per-function
     constant string pool.
     1 in a VALUE is VALUE_CHANGED in var-tracking.c.
     Dumped as "/f" in RTL dumps.  */
  unsigned frame_related : 1;
  /* 1 in a REG or PARALLEL that is the current function's return value.
     1 in a SYMBOL_REF for a weak symbol.
     1 in a CALL_INSN logically equivalent to ECF_PURE and DECL_PURE_P.
     1 in a CONCAT is VAL_EXPR_HAS_REVERSE in var-tracking.c.
     1 in a VALUE or DEBUG_EXPR is NO_LOC_P in var-tracking.c.
     Dumped as "/i" in RTL dumps.  */
  unsigned return_val : 1;

  union {
    /* The final union field is aligned to 64 bits on LP64 hosts,
       giving a 32-bit gap after the fields above.  We optimize the
       layout for that case and use the gap for extra code-specific
       information.  */

    /* The ORIGINAL_REGNO of a REG.  */
    unsigned int original_regno;

    /* The INSN_UID of an RTX_INSN-class code.  */
    int insn_uid;

    /* The SYMBOL_REF_FLAGS of a SYMBOL_REF.  */
    unsigned int symbol_ref_flags;

    /* The PAT_VAR_LOCATION_STATUS of a VAR_LOCATION.  */
    enum var_init_status var_location_status;

    /* In a CONST_WIDE_INT (aka hwivec_def), this is the number of
       HOST_WIDE_INTs in the hwivec_def.  */
    unsigned int num_elem;

    /* Information about a CONST_VECTOR.  */
    struct
    {
      /* The value of CONST_VECTOR_NPATTERNS.  */
      unsigned int npatterns : 16;

      /* The value of CONST_VECTOR_NELTS_PER_PATTERN.  */
      unsigned int nelts_per_pattern : 8;

      /* For future expansion.  */
      unsigned int unused : 8;
    } const_vector;
  } GTY ((skip)) u2;

  /* The first element of the operands of this rtx.
     The number of operands and their types are controlled
     by the `code' field, according to rtl.def.  */
  union u {
    rtunion fld[1];
    HOST_WIDE_INT hwint[1];
    struct reg_info reg;
    struct block_symbol block_sym;
    struct real_value rv;
    struct fixed_value fv;
    struct hwivec_def hwiv;
    struct const_poly_int_def cpi;
  } GTY ((special ("rtx_def"), desc ("GET_CODE (&%0)"))) u;
};

/* A node for constructing singly-linked lists of rtx.  */

class GTY(()) rtx_expr_list : public rtx_def
{
  /* No extra fields, but adds invariant: (GET_CODE (X) == EXPR_LIST).  */

public:
  /* Get next in list.  */
  rtx_expr_list *next () const;

  /* Get at the underlying rtx.  */
  rtx element () const;
};

template <>
template <>
inline bool
is_a_helper <rtx_expr_list *>::test (rtx rt)
{
  return rt->code == EXPR_LIST;
}

class GTY(()) rtx_insn_list : public rtx_def
{
  /* No extra fields, but adds invariant: (GET_CODE (X) == INSN_LIST).

     This is an instance of:

       DEF_RTL_EXPR(INSN_LIST, "insn_list", "ue", RTX_EXTRA)

     i.e. a node for constructing singly-linked lists of rtx_insn *, where
     the list is "external" to the insn (as opposed to the doubly-linked
     list embedded within rtx_insn itself).  */

public:
  /* Get next in list.  */
  rtx_insn_list *next () const;

  /* Get at the underlying instruction.  */
  rtx_insn *insn () const;

};

template <>
template <>
inline bool
is_a_helper <rtx_insn_list *>::test (rtx rt)
{
  return rt->code == INSN_LIST;
}

/* A node with invariant GET_CODE (X) == SEQUENCE i.e. a vector of rtx,
   typically (but not always) of rtx_insn *, used in the late passes.  */

class GTY(()) rtx_sequence : public rtx_def
{
  /* No extra fields, but adds invariant: (GET_CODE (X) == SEQUENCE).  */

public:
  /* Get number of elements in sequence.  */
  int len () const;

  /* Get i-th element of the sequence.  */
  rtx element (int index) const;

  /* Get i-th element of the sequence, with a checked cast to
     rtx_insn *.  */
  rtx_insn *insn (int index) const;
};

template <>
template <>
inline bool
is_a_helper <rtx_sequence *>::test (rtx rt)
{
  return rt->code == SEQUENCE;
}

template <>
template <>
inline bool
is_a_helper <const rtx_sequence *>::test (const_rtx rt)
{
  return rt->code == SEQUENCE;
}

class GTY(()) rtx_insn : public rtx_def
{
public:
  /* No extra fields, but adds the invariant:

     (INSN_P (X)
      || NOTE_P (X)
      || JUMP_TABLE_DATA_P (X)
      || BARRIER_P (X)
      || LABEL_P (X))

     i.e. that we must be able to use the following:
      INSN_UID ()
      NEXT_INSN ()
      PREV_INSN ()
    i.e. we have an rtx that has an INSN_UID field and can be part of
    a linked list of insns.
  */

  /* Returns true if this insn has been deleted.  */

  bool deleted () const { return volatil; }

  /* Mark this insn as deleted.  */

  void set_deleted () { volatil = true; }

  /* Mark this insn as not deleted.  */

  void set_undeleted () { volatil = false; }
};

/* Subclasses of rtx_insn.  */

class GTY(()) rtx_debug_insn : public rtx_insn
{
  /* No extra fields, but adds the invariant:
       DEBUG_INSN_P (X) aka (GET_CODE (X) == DEBUG_INSN)
     i.e. an annotation for tracking variable assignments.

     This is an instance of:
       DEF_RTL_EXPR(DEBUG_INSN, "debug_insn", "uuBeiie", RTX_INSN)
     from rtl.def.  */
};

class GTY(()) rtx_nonjump_insn : public rtx_insn
{
  /* No extra fields, but adds the invariant:
       NONJUMP_INSN_P (X) aka (GET_CODE (X) == INSN)
     i.e an instruction that cannot jump.

     This is an instance of:
       DEF_RTL_EXPR(INSN, "insn", "uuBeiie", RTX_INSN)
     from rtl.def.  */
};

class GTY(()) rtx_jump_insn : public rtx_insn
{
public:
  /* No extra fields, but adds the invariant:
       JUMP_P (X) aka (GET_CODE (X) == JUMP_INSN)
     i.e. an instruction that can possibly jump.

     This is an instance of:
       DEF_RTL_EXPR(JUMP_INSN, "jump_insn", "uuBeiie0", RTX_INSN)
     from rtl.def.  */

  /* Returns jump target of this instruction.  The returned value is not
     necessarily a code label: it may also be a RETURN or SIMPLE_RETURN
     expression.  Also, when the code label is marked "deleted", it is
     replaced by a NOTE.  In some cases the value is NULL_RTX.  */

  inline rtx jump_label () const;

  /* Returns jump target cast to rtx_code_label *.  */

  inline rtx_code_label *jump_target () const;

  /* Set jump target.  */

  inline void set_jump_target (rtx_code_label *);
};

class GTY(()) rtx_call_insn : public rtx_insn
{
  /* No extra fields, but adds the invariant:
       CALL_P (X) aka (GET_CODE (X) == CALL_INSN)
     i.e. an instruction that can possibly call a subroutine
     but which will not change which instruction comes next
     in the current function.

     This is an instance of:
       DEF_RTL_EXPR(CALL_INSN, "call_insn", "uuBeiiee", RTX_INSN)
     from rtl.def.  */
};

class GTY(()) rtx_jump_table_data : public rtx_insn
{
  /* No extra fields, but adds the invariant:
       JUMP_TABLE_DATA_P (X) aka (GET_CODE (INSN) == JUMP_TABLE_DATA)
     i.e. a data for a jump table, considered an instruction for
     historical reasons.

     This is an instance of:
       DEF_RTL_EXPR(JUMP_TABLE_DATA, "jump_table_data", "uuBe0000", RTX_INSN)
     from rtl.def.  */

public:

  /* This can be either:

       (a) a table of absolute jumps, in which case PATTERN (this) is an
           ADDR_VEC with arg 0 a vector of labels, or

       (b) a table of relative jumps (e.g. for -fPIC), in which case
           PATTERN (this) is an ADDR_DIFF_VEC, with arg 0 a LABEL_REF and
	   arg 1 the vector of labels.

     This method gets the underlying vec.  */

  inline rtvec get_labels () const;
  inline scalar_int_mode get_data_mode () const;
};

class GTY(()) rtx_barrier : public rtx_insn
{
  /* No extra fields, but adds the invariant:
       BARRIER_P (X) aka (GET_CODE (X) == BARRIER)
     i.e. a marker that indicates that control will not flow through.

     This is an instance of:
       DEF_RTL_EXPR(BARRIER, "barrier", "uu00000", RTX_EXTRA)
     from rtl.def.  */
};

class GTY(()) rtx_code_label : public rtx_insn
{
  /* No extra fields, but adds the invariant:
       LABEL_P (X) aka (GET_CODE (X) == CODE_LABEL)
     i.e. a label in the assembler.

     This is an instance of:
       DEF_RTL_EXPR(CODE_LABEL, "code_label", "uuB00is", RTX_EXTRA)
     from rtl.def.  */
};

class GTY(()) rtx_note : public rtx_insn
{
  /* No extra fields, but adds the invariant:
       NOTE_P(X) aka (GET_CODE (X) == NOTE)
     i.e. a note about the corresponding source code.

     This is an instance of:
       DEF_RTL_EXPR(NOTE, "note", "uuB0ni", RTX_EXTRA)
     from rtl.def.  */
};

/* The size in bytes of an rtx header (code, mode and flags).  */
#define RTX_HDR_SIZE offsetof (struct rtx_def, u)

/* The size in bytes of an rtx with code CODE.  */
#define RTX_CODE_SIZE(CODE) rtx_code_size[CODE]

#define NULL_RTX (rtx) 0

/* The "next" and "previous" RTX, relative to this one.  */

#define RTX_NEXT(X) (rtx_next[GET_CODE (X)] == 0 ? NULL			\
		     : *(rtx *)(((char *)X) + rtx_next[GET_CODE (X)]))

/* FIXME: the "NEXT_INSN (PREV_INSN (X)) == X" condition shouldn't be needed.
 */
#define RTX_PREV(X) ((INSN_P (X)       			\
                      || NOTE_P (X)       		\
                      || JUMP_TABLE_DATA_P (X)		\
                      || BARRIER_P (X)        		\
                      || LABEL_P (X))    		\
		     && PREV_INSN (as_a <rtx_insn *> (X)) != NULL	\
                     && NEXT_INSN (PREV_INSN (as_a <rtx_insn *> (X))) == X \
                     ? PREV_INSN (as_a <rtx_insn *> (X)) : NULL)

/* Define macros to access the `code' field of the rtx.  */

#define GET_CODE(RTX)	    ((enum rtx_code) (RTX)->code)
#define PUT_CODE(RTX, CODE) ((RTX)->code = (CODE))

#define GET_MODE(RTX)		((machine_mode) (RTX)->mode)
#define PUT_MODE_RAW(RTX, MODE)	((RTX)->mode = (MODE))

/* RTL vector.  These appear inside RTX's when there is a need
   for a variable number of things.  The principle use is inside
   PARALLEL expressions.  */

struct GTY(()) rtvec_def {
  int num_elem;		/* number of elements */
  rtx GTY ((length ("%h.num_elem"))) elem[1];
};

#define NULL_RTVEC (rtvec) 0

#define GET_NUM_ELEM(RTVEC)		((RTVEC)->num_elem)
#define PUT_NUM_ELEM(RTVEC, NUM)	((RTVEC)->num_elem = (NUM))

/* Predicate yielding nonzero iff X is an rtx for a register.  */
#define REG_P(X) (GET_CODE (X) == REG)

/* Predicate yielding nonzero iff X is an rtx for a memory location.  */
#define MEM_P(X) (GET_CODE (X) == MEM)

#if TARGET_SUPPORTS_WIDE_INT

/* Match CONST_*s that can represent compile-time constant integers.  */
#define CASE_CONST_SCALAR_INT \
   case CONST_INT: \
   case CONST_WIDE_INT

/* Match CONST_*s for which pointer equality corresponds to value
   equality.  */
#define CASE_CONST_UNIQUE \
   case CONST_INT: \
   case CONST_WIDE_INT: \
   case CONST_POLY_INT: \
   case CONST_DOUBLE: \
   case CONST_FIXED

/* Match all CONST_* rtxes.  */
#define CASE_CONST_ANY \
   case CONST_INT: \
   case CONST_WIDE_INT: \
   case CONST_POLY_INT: \
   case CONST_DOUBLE: \
   case CONST_FIXED: \
   case CONST_VECTOR

#else

/* Match CONST_*s that can represent compile-time constant integers.  */
#define CASE_CONST_SCALAR_INT \
   case CONST_INT: \
   case CONST_DOUBLE

/* Match CONST_*s for which pointer equality corresponds to value
   equality.  */
#define CASE_CONST_UNIQUE \
   case CONST_INT: \
   case CONST_DOUBLE: \
   case CONST_FIXED

/* Match all CONST_* rtxes.  */
#define CASE_CONST_ANY \
   case CONST_INT: \
   case CONST_DOUBLE: \
   case CONST_FIXED: \
   case CONST_VECTOR
#endif

/* Predicate yielding nonzero iff X is an rtx for a constant integer.  */
#define CONST_INT_P(X) (GET_CODE (X) == CONST_INT)

/* Predicate yielding nonzero iff X is an rtx for a constant integer.  */
#define CONST_WIDE_INT_P(X) (GET_CODE (X) == CONST_WIDE_INT)

/* Predicate yielding nonzero iff X is an rtx for a polynomial constant
   integer.  */
#define CONST_POLY_INT_P(X) \
  (NUM_POLY_INT_COEFFS > 1 && GET_CODE (X) == CONST_POLY_INT)

/* Predicate yielding nonzero iff X is an rtx for a constant fixed-point.  */
#define CONST_FIXED_P(X) (GET_CODE (X) == CONST_FIXED)

/* Predicate yielding true iff X is an rtx for a double-int
   or floating point constant.  */
#define CONST_DOUBLE_P(X) (GET_CODE (X) == CONST_DOUBLE)

/* Predicate yielding true iff X is an rtx for a double-int.  */
#define CONST_DOUBLE_AS_INT_P(X) \
  (GET_CODE (X) == CONST_DOUBLE && GET_MODE (X) == VOIDmode)

/* Predicate yielding true iff X is an rtx for a integer const.  */
#if TARGET_SUPPORTS_WIDE_INT
#define CONST_SCALAR_INT_P(X) \
  (CONST_INT_P (X) || CONST_WIDE_INT_P (X))
#else
#define CONST_SCALAR_INT_P(X) \
  (CONST_INT_P (X) || CONST_DOUBLE_AS_INT_P (X))
#endif

/* Predicate yielding true iff X is an rtx for a double-int.  */
#define CONST_DOUBLE_AS_FLOAT_P(X) \
  (GET_CODE (X) == CONST_DOUBLE && GET_MODE (X) != VOIDmode)

/* Predicate yielding nonzero iff X is a label insn.  */
#define LABEL_P(X) (GET_CODE (X) == CODE_LABEL)

/* Predicate yielding nonzero iff X is a jump insn.  */
#define JUMP_P(X) (GET_CODE (X) == JUMP_INSN)

/* Predicate yielding nonzero iff X is a call insn.  */
#define CALL_P(X) (GET_CODE (X) == CALL_INSN)

/* Predicate yielding nonzero iff X is an insn that cannot jump.  */
#define NONJUMP_INSN_P(X) (GET_CODE (X) == INSN)

/* Predicate yielding nonzero iff X is a debug note/insn.  */
#define DEBUG_INSN_P(X) (GET_CODE (X) == DEBUG_INSN)

/* Predicate yielding nonzero iff X is an insn that is not a debug insn.  */
#define NONDEBUG_INSN_P(X) (INSN_P (X) && !DEBUG_INSN_P (X))

/* Nonzero if DEBUG_MARKER_INSN_P may possibly hold.  */
#define MAY_HAVE_DEBUG_MARKER_INSNS debug_nonbind_markers_p
/* Nonzero if DEBUG_BIND_INSN_P may possibly hold.  */
#define MAY_HAVE_DEBUG_BIND_INSNS flag_var_tracking_assignments
/* Nonzero if DEBUG_INSN_P may possibly hold.  */
#define MAY_HAVE_DEBUG_INSNS					\
  (MAY_HAVE_DEBUG_MARKER_INSNS || MAY_HAVE_DEBUG_BIND_INSNS)

/* Predicate yielding nonzero iff X is a real insn.  */
#define INSN_P(X) \
  (NONJUMP_INSN_P (X) || DEBUG_INSN_P (X) || JUMP_P (X) || CALL_P (X))

/* Predicate yielding nonzero iff X is a note insn.  */
#define NOTE_P(X) (GET_CODE (X) == NOTE)

/* Predicate yielding nonzero iff X is a barrier insn.  */
#define BARRIER_P(X) (GET_CODE (X) == BARRIER)

/* Predicate yielding nonzero iff X is a data for a jump table.  */
#define JUMP_TABLE_DATA_P(INSN) (GET_CODE (INSN) == JUMP_TABLE_DATA)

/* Predicate yielding nonzero iff RTX is a subreg.  */
#define SUBREG_P(RTX) (GET_CODE (RTX) == SUBREG)

/* Predicate yielding true iff RTX is a symbol ref.  */
#define SYMBOL_REF_P(RTX) (GET_CODE (RTX) == SYMBOL_REF)

template <>
template <>
inline bool
is_a_helper <rtx_insn *>::test (rtx rt)
{
  return (INSN_P (rt)
	  || NOTE_P (rt)
	  || JUMP_TABLE_DATA_P (rt)
	  || BARRIER_P (rt)
	  || LABEL_P (rt));
}

template <>
template <>
inline bool
is_a_helper <const rtx_insn *>::test (const_rtx rt)
{
  return (INSN_P (rt)
	  || NOTE_P (rt)
	  || JUMP_TABLE_DATA_P (rt)
	  || BARRIER_P (rt)
	  || LABEL_P (rt));
}

template <>
template <>
inline bool
is_a_helper <rtx_debug_insn *>::test (rtx rt)
{
  return DEBUG_INSN_P (rt);
}

template <>
template <>
inline bool
is_a_helper <rtx_nonjump_insn *>::test (rtx rt)
{
  return NONJUMP_INSN_P (rt);
}

template <>
template <>
inline bool
is_a_helper <rtx_jump_insn *>::test (rtx rt)
{
  return JUMP_P (rt);
}

template <>
template <>
inline bool
is_a_helper <rtx_jump_insn *>::test (rtx_insn *insn)
{
  return JUMP_P (insn);
}

template <>
template <>
inline bool
is_a_helper <rtx_call_insn *>::test (rtx rt)
{
  return CALL_P (rt);
}

template <>
template <>
inline bool
is_a_helper <rtx_call_insn *>::test (rtx_insn *insn)
{
  return CALL_P (insn);
}

template <>
template <>
inline bool
is_a_helper <rtx_jump_table_data *>::test (rtx rt)
{
  return JUMP_TABLE_DATA_P (rt);
}

template <>
template <>
inline bool
is_a_helper <rtx_jump_table_data *>::test (rtx_insn *insn)
{
  return JUMP_TABLE_DATA_P (insn);
}

template <>
template <>
inline bool
is_a_helper <rtx_barrier *>::test (rtx rt)
{
  return BARRIER_P (rt);
}

template <>
template <>
inline bool
is_a_helper <rtx_code_label *>::test (rtx rt)
{
  return LABEL_P (rt);
}

template <>
template <>
inline bool
is_a_helper <rtx_code_label *>::test (rtx_insn *insn)
{
  return LABEL_P (insn);
}

template <>
template <>
inline bool
is_a_helper <rtx_note *>::test (rtx rt)
{
  return NOTE_P (rt);
}

template <>
template <>
inline bool
is_a_helper <rtx_note *>::test (rtx_insn *insn)
{
  return NOTE_P (insn);
}

/* Predicate yielding nonzero iff X is a return or simple_return.  */
#define ANY_RETURN_P(X) \
  (GET_CODE (X) == RETURN || GET_CODE (X) == SIMPLE_RETURN)

/* 1 if X is a unary operator.  */

#define UNARY_P(X)   \
  (GET_RTX_CLASS (GET_CODE (X)) == RTX_UNARY)

/* 1 if X is a binary operator.  */

#define BINARY_P(X)   \
  ((GET_RTX_CLASS (GET_CODE (X)) & RTX_BINARY_MASK) == RTX_BINARY_RESULT)

/* 1 if X is an arithmetic operator.  */

#define ARITHMETIC_P(X)   \
  ((GET_RTX_CLASS (GET_CODE (X)) & RTX_ARITHMETIC_MASK)			\
    == RTX_ARITHMETIC_RESULT)

/* 1 if X is an arithmetic operator.  */

#define COMMUTATIVE_ARITH_P(X)   \
  (GET_RTX_CLASS (GET_CODE (X)) == RTX_COMM_ARITH)

/* 1 if X is a commutative arithmetic operator or a comparison operator.
   These two are sometimes selected together because it is possible to
   swap the two operands.  */

#define SWAPPABLE_OPERANDS_P(X)   \
  ((1 << GET_RTX_CLASS (GET_CODE (X)))					\
    & ((1 << RTX_COMM_ARITH) | (1 << RTX_COMM_COMPARE)			\
       | (1 << RTX_COMPARE)))

/* 1 if X is a non-commutative operator.  */

#define NON_COMMUTATIVE_P(X)   \
  ((GET_RTX_CLASS (GET_CODE (X)) & RTX_COMMUTATIVE_MASK)		\
    == RTX_NON_COMMUTATIVE_RESULT)

/* 1 if X is a commutative operator on integers.  */

#define COMMUTATIVE_P(X)   \
  ((GET_RTX_CLASS (GET_CODE (X)) & RTX_COMMUTATIVE_MASK)		\
    == RTX_COMMUTATIVE_RESULT)

/* 1 if X is a relational operator.  */

#define COMPARISON_P(X)   \
  ((GET_RTX_CLASS (GET_CODE (X)) & RTX_COMPARE_MASK) == RTX_COMPARE_RESULT)

/* 1 if X is a constant value that is an integer.  */

#define CONSTANT_P(X)   \
  (GET_RTX_CLASS (GET_CODE (X)) == RTX_CONST_OBJ)

/* 1 if X can be used to represent an object.  */
#define OBJECT_P(X)							\
  ((GET_RTX_CLASS (GET_CODE (X)) & RTX_OBJ_MASK) == RTX_OBJ_RESULT)

/* General accessor macros for accessing the fields of an rtx.  */

#if defined ENABLE_RTL_CHECKING && (GCC_VERSION >= 2007)
/* The bit with a star outside the statement expr and an & inside is
   so that N can be evaluated only once.  */
#define RTL_CHECK1(RTX, N, C1) __extension__				\
(*({ __typeof (RTX) const _rtx = (RTX); const int _n = (N);		\
     const enum rtx_code _code = GET_CODE (_rtx);			\
     if (_n < 0 || _n >= GET_RTX_LENGTH (_code))			\
       rtl_check_failed_bounds (_rtx, _n, __FILE__, __LINE__,		\
				__FUNCTION__);				\
     if (GET_RTX_FORMAT (_code)[_n] != C1)				\
       rtl_check_failed_type1 (_rtx, _n, C1, __FILE__, __LINE__,	\
			       __FUNCTION__);				\
     &_rtx->u.fld[_n]; }))

#define RTL_CHECK2(RTX, N, C1, C2) __extension__			\
(*({ __typeof (RTX) const _rtx = (RTX); const int _n = (N);		\
     const enum rtx_code _code = GET_CODE (_rtx);			\
     if (_n < 0 || _n >= GET_RTX_LENGTH (_code))			\
       rtl_check_failed_bounds (_rtx, _n, __FILE__, __LINE__,		\
				__FUNCTION__);				\
     if (GET_RTX_FORMAT (_code)[_n] != C1				\
	 && GET_RTX_FORMAT (_code)[_n] != C2)				\
       rtl_check_failed_type2 (_rtx, _n, C1, C2, __FILE__, __LINE__,	\
			       __FUNCTION__);				\
     &_rtx->u.fld[_n]; }))

#define RTL_CHECKC1(RTX, N, C) __extension__				\
(*({ __typeof (RTX) const _rtx = (RTX); const int _n = (N);		\
     if (GET_CODE (_rtx) != (C))					\
       rtl_check_failed_code1 (_rtx, (C), __FILE__, __LINE__,		\
			       __FUNCTION__);				\
     &_rtx->u.fld[_n]; }))

#define RTL_CHECKC2(RTX, N, C1, C2) __extension__			\
(*({ __typeof (RTX) const _rtx = (RTX); const int _n = (N);		\
     const enum rtx_code _code = GET_CODE (_rtx);			\
     if (_code != (C1) && _code != (C2))				\
       rtl_check_failed_code2 (_rtx, (C1), (C2), __FILE__, __LINE__,	\
			       __FUNCTION__); \
     &_rtx->u.fld[_n]; }))

#define RTVEC_ELT(RTVEC, I) __extension__				\
(*({ __typeof (RTVEC) const _rtvec = (RTVEC); const int _i = (I);	\
     if (_i < 0 || _i >= GET_NUM_ELEM (_rtvec))				\
       rtvec_check_failed_bounds (_rtvec, _i, __FILE__, __LINE__,	\
				  __FUNCTION__);			\
     &_rtvec->elem[_i]; }))

#define XWINT(RTX, N) __extension__					\
(*({ __typeof (RTX) const _rtx = (RTX); const int _n = (N);		\
     const enum rtx_code _code = GET_CODE (_rtx);			\
     if (_n < 0 || _n >= GET_RTX_LENGTH (_code))			\
       rtl_check_failed_bounds (_rtx, _n, __FILE__, __LINE__,		\
				__FUNCTION__);				\
     if (GET_RTX_FORMAT (_code)[_n] != 'w')				\
       rtl_check_failed_type1 (_rtx, _n, 'w', __FILE__, __LINE__,	\
			       __FUNCTION__);				\
     &_rtx->u.hwint[_n]; }))

#define CWI_ELT(RTX, I) __extension__					\
(*({ __typeof (RTX) const _cwi = (RTX);					\
     int _max = CWI_GET_NUM_ELEM (_cwi);				\
     const int _i = (I);						\
     if (_i < 0 || _i >= _max)						\
       cwi_check_failed_bounds (_cwi, _i, __FILE__, __LINE__,		\
				__FUNCTION__);				\
     &_cwi->u.hwiv.elem[_i]; }))

#define XCWINT(RTX, N, C) __extension__					\
(*({ __typeof (RTX) const _rtx = (RTX);					\
     if (GET_CODE (_rtx) != (C))					\
       rtl_check_failed_code1 (_rtx, (C), __FILE__, __LINE__,		\
			       __FUNCTION__);				\
     &_rtx->u.hwint[N]; }))

#define XCMWINT(RTX, N, C, M) __extension__				\
(*({ __typeof (RTX) const _rtx = (RTX);					\
     if (GET_CODE (_rtx) != (C) || GET_MODE (_rtx) != (M))		\
       rtl_check_failed_code_mode (_rtx, (C), (M), false, __FILE__,	\
				   __LINE__, __FUNCTION__);		\
     &_rtx->u.hwint[N]; }))

#define XCNMPRV(RTX, C, M) __extension__				\
({ __typeof (RTX) const _rtx = (RTX);					\
   if (GET_CODE (_rtx) != (C) || GET_MODE (_rtx) == (M))		\
     rtl_check_failed_code_mode (_rtx, (C), (M), true, __FILE__,	\
				 __LINE__, __FUNCTION__);		\
   &_rtx->u.rv; })

#define XCNMPFV(RTX, C, M) __extension__				\
({ __typeof (RTX) const _rtx = (RTX);					\
   if (GET_CODE (_rtx) != (C) || GET_MODE (_rtx) == (M))		\
     rtl_check_failed_code_mode (_rtx, (C), (M), true, __FILE__,	\
				 __LINE__, __FUNCTION__);		\
   &_rtx->u.fv; })

#define REG_CHECK(RTX) __extension__					\
({ __typeof (RTX) const _rtx = (RTX);					\
   if (GET_CODE (_rtx) != REG)						\
     rtl_check_failed_code1 (_rtx, REG,  __FILE__, __LINE__,		\
			     __FUNCTION__);				\
   &_rtx->u.reg; })

#define BLOCK_SYMBOL_CHECK(RTX) __extension__				\
({ __typeof (RTX) const _symbol = (RTX);				\
   const unsigned int flags = SYMBOL_REF_FLAGS (_symbol);		\
   if ((flags & SYMBOL_FLAG_HAS_BLOCK_INFO) == 0)			\
     rtl_check_failed_block_symbol (__FILE__, __LINE__,			\
				    __FUNCTION__);			\
   &_symbol->u.block_sym; })

#define HWIVEC_CHECK(RTX,C) __extension__				\
({ __typeof (RTX) const _symbol = (RTX);				\
   RTL_CHECKC1 (_symbol, 0, C);						\
   &_symbol->u.hwiv; })

extern void rtl_check_failed_bounds (const_rtx, int, const char *, int,
				     const char *)
    ATTRIBUTE_NORETURN ATTRIBUTE_COLD;
extern void rtl_check_failed_type1 (const_rtx, int, int, const char *, int,
				    const char *)
    ATTRIBUTE_NORETURN ATTRIBUTE_COLD;
extern void rtl_check_failed_type2 (const_rtx, int, int, int, const char *,
				    int, const char *)
    ATTRIBUTE_NORETURN ATTRIBUTE_COLD;
extern void rtl_check_failed_code1 (const_rtx, enum rtx_code, const char *,
				    int, const char *)
    ATTRIBUTE_NORETURN ATTRIBUTE_COLD;
extern void rtl_check_failed_code2 (const_rtx, enum rtx_code, enum rtx_code,
				    const char *, int, const char *)
    ATTRIBUTE_NORETURN ATTRIBUTE_COLD;
extern void rtl_check_failed_code_mode (const_rtx, enum rtx_code, machine_mode,
					bool, const char *, int, const char *)
    ATTRIBUTE_NORETURN ATTRIBUTE_COLD;
extern void rtl_check_failed_block_symbol (const char *, int, const char *)
    ATTRIBUTE_NORETURN ATTRIBUTE_COLD;
extern void cwi_check_failed_bounds (const_rtx, int, const char *, int,
				     const char *)
    ATTRIBUTE_NORETURN ATTRIBUTE_COLD;
extern void rtvec_check_failed_bounds (const_rtvec, int, const char *, int,
				       const char *)
    ATTRIBUTE_NORETURN ATTRIBUTE_COLD;

#else   /* not ENABLE_RTL_CHECKING */

#define RTL_CHECK1(RTX, N, C1)      ((RTX)->u.fld[N])
#define RTL_CHECK2(RTX, N, C1, C2)  ((RTX)->u.fld[N])
#define RTL_CHECKC1(RTX, N, C)	    ((RTX)->u.fld[N])
#define RTL_CHECKC2(RTX, N, C1, C2) ((RTX)->u.fld[N])
#define RTVEC_ELT(RTVEC, I)	    ((RTVEC)->elem[I])
#define XWINT(RTX, N)		    ((RTX)->u.hwint[N])
#define CWI_ELT(RTX, I)		    ((RTX)->u.hwiv.elem[I])
#define XCWINT(RTX, N, C)	    ((RTX)->u.hwint[N])
#define XCMWINT(RTX, N, C, M)	    ((RTX)->u.hwint[N])
#define XCNMWINT(RTX, N, C, M)	    ((RTX)->u.hwint[N])
#define XCNMPRV(RTX, C, M)	    (&(RTX)->u.rv)
#define XCNMPFV(RTX, C, M)	    (&(RTX)->u.fv)
#define REG_CHECK(RTX)		    (&(RTX)->u.reg)
#define BLOCK_SYMBOL_CHECK(RTX)	    (&(RTX)->u.block_sym)
#define HWIVEC_CHECK(RTX,C)	    (&(RTX)->u.hwiv)

#endif

/* General accessor macros for accessing the flags of an rtx.  */

/* Access an individual rtx flag, with no checking of any kind.  */
#define RTX_FLAG(RTX, FLAG)	((RTX)->FLAG)

#if defined ENABLE_RTL_FLAG_CHECKING && (GCC_VERSION >= 2007)
#define RTL_FLAG_CHECK1(NAME, RTX, C1) __extension__			\
({ __typeof (RTX) const _rtx = (RTX);					\
   if (GET_CODE (_rtx) != C1)						\
     rtl_check_failed_flag  (NAME, _rtx, __FILE__, __LINE__,		\
			     __FUNCTION__);				\
   _rtx; })

#define RTL_FLAG_CHECK2(NAME, RTX, C1, C2) __extension__		\
({ __typeof (RTX) const _rtx = (RTX);					\
   if (GET_CODE (_rtx) != C1 && GET_CODE(_rtx) != C2)			\
     rtl_check_failed_flag  (NAME,_rtx, __FILE__, __LINE__,		\
			      __FUNCTION__);				\
   _rtx; })

#define RTL_FLAG_CHECK3(NAME, RTX, C1, C2, C3) __extension__		\
({ __typeof (RTX) const _rtx = (RTX);					\
   if (GET_CODE (_rtx) != C1 && GET_CODE(_rtx) != C2			\
       && GET_CODE (_rtx) != C3)					\
     rtl_check_failed_flag  (NAME, _rtx, __FILE__, __LINE__,		\
			     __FUNCTION__);				\
   _rtx; })

#define RTL_FLAG_CHECK4(NAME, RTX, C1, C2, C3, C4) __extension__	\
({ __typeof (RTX) const _rtx = (RTX);					\
   if (GET_CODE (_rtx) != C1 && GET_CODE(_rtx) != C2			\
       && GET_CODE (_rtx) != C3 && GET_CODE(_rtx) != C4)		\
     rtl_check_failed_flag  (NAME, _rtx, __FILE__, __LINE__,		\
			      __FUNCTION__);				\
   _rtx; })

#define RTL_FLAG_CHECK5(NAME, RTX, C1, C2, C3, C4, C5) __extension__	\
({ __typeof (RTX) const _rtx = (RTX);					\
   if (GET_CODE (_rtx) != C1 && GET_CODE (_rtx) != C2			\
       && GET_CODE (_rtx) != C3 && GET_CODE (_rtx) != C4		\
       && GET_CODE (_rtx) != C5)					\
     rtl_check_failed_flag  (NAME, _rtx, __FILE__, __LINE__,		\
			     __FUNCTION__);				\
   _rtx; })

#define RTL_FLAG_CHECK6(NAME, RTX, C1, C2, C3, C4, C5, C6)		\
  __extension__								\
({ __typeof (RTX) const _rtx = (RTX);					\
   if (GET_CODE (_rtx) != C1 && GET_CODE (_rtx) != C2			\
       && GET_CODE (_rtx) != C3 && GET_CODE (_rtx) != C4		\
       && GET_CODE (_rtx) != C5 && GET_CODE (_rtx) != C6)		\
     rtl_check_failed_flag  (NAME,_rtx, __FILE__, __LINE__,		\
			     __FUNCTION__);				\
   _rtx; })

#define RTL_FLAG_CHECK7(NAME, RTX, C1, C2, C3, C4, C5, C6, C7)		\
  __extension__								\
({ __typeof (RTX) const _rtx = (RTX);					\
   if (GET_CODE (_rtx) != C1 && GET_CODE (_rtx) != C2			\
       && GET_CODE (_rtx) != C3 && GET_CODE (_rtx) != C4		\
       && GET_CODE (_rtx) != C5 && GET_CODE (_rtx) != C6		\
       && GET_CODE (_rtx) != C7)					\
     rtl_check_failed_flag  (NAME, _rtx, __FILE__, __LINE__,		\
			     __FUNCTION__);				\
   _rtx; })

#define RTL_INSN_CHAIN_FLAG_CHECK(NAME, RTX) 				\
  __extension__								\
({ __typeof (RTX) const _rtx = (RTX);					\
   if (!INSN_CHAIN_CODE_P (GET_CODE (_rtx)))				\
     rtl_check_failed_flag (NAME, _rtx, __FILE__, __LINE__,		\
			    __FUNCTION__);				\
   _rtx; })

extern void rtl_check_failed_flag (const char *, const_rtx, const char *,
				   int, const char *)
    ATTRIBUTE_NORETURN ATTRIBUTE_COLD
    ;

#else	/* not ENABLE_RTL_FLAG_CHECKING */

#define RTL_FLAG_CHECK1(NAME, RTX, C1)					(RTX)
#define RTL_FLAG_CHECK2(NAME, RTX, C1, C2)				(RTX)
#define RTL_FLAG_CHECK3(NAME, RTX, C1, C2, C3)				(RTX)
#define RTL_FLAG_CHECK4(NAME, RTX, C1, C2, C3, C4)			(RTX)
#define RTL_FLAG_CHECK5(NAME, RTX, C1, C2, C3, C4, C5)			(RTX)
#define RTL_FLAG_CHECK6(NAME, RTX, C1, C2, C3, C4, C5, C6)		(RTX)
#define RTL_FLAG_CHECK7(NAME, RTX, C1, C2, C3, C4, C5, C6, C7)		(RTX)
#define RTL_INSN_CHAIN_FLAG_CHECK(NAME, RTX) 				(RTX)
#endif

#define XINT(RTX, N)	(RTL_CHECK2 (RTX, N, 'i', 'n').rt_int)
#define XUINT(RTX, N)   (RTL_CHECK2 (RTX, N, 'i', 'n').rt_uint)
#define XSTR(RTX, N)	(RTL_CHECK2 (RTX, N, 's', 'S').rt_str)
#define XEXP(RTX, N)	(RTL_CHECK2 (RTX, N, 'e', 'u').rt_rtx)
#define XVEC(RTX, N)	(RTL_CHECK2 (RTX, N, 'E', 'V').rt_rtvec)
#define XMODE(RTX, N)	(RTL_CHECK1 (RTX, N, 'M').rt_type)
#define XTREE(RTX, N)   (RTL_CHECK1 (RTX, N, 't').rt_tree)
#define XBBDEF(RTX, N)	(RTL_CHECK1 (RTX, N, 'B').rt_bb)
#define XTMPL(RTX, N)	(RTL_CHECK1 (RTX, N, 'T').rt_str)
#define XCFI(RTX, N)	(RTL_CHECK1 (RTX, N, 'C').rt_cfi)

#define XVECEXP(RTX, N, M)	RTVEC_ELT (XVEC (RTX, N), M)
#define XVECLEN(RTX, N)		GET_NUM_ELEM (XVEC (RTX, N))

/* These are like XINT, etc. except that they expect a '0' field instead
   of the normal type code.  */

#define X0INT(RTX, N)	   (RTL_CHECK1 (RTX, N, '0').rt_int)
#define X0UINT(RTX, N)	   (RTL_CHECK1 (RTX, N, '0').rt_uint)
#define X0STR(RTX, N)	   (RTL_CHECK1 (RTX, N, '0').rt_str)
#define X0EXP(RTX, N)	   (RTL_CHECK1 (RTX, N, '0').rt_rtx)
#define X0VEC(RTX, N)	   (RTL_CHECK1 (RTX, N, '0').rt_rtvec)
#define X0MODE(RTX, N)	   (RTL_CHECK1 (RTX, N, '0').rt_type)
#define X0TREE(RTX, N)	   (RTL_CHECK1 (RTX, N, '0').rt_tree)
#define X0BBDEF(RTX, N)	   (RTL_CHECK1 (RTX, N, '0').rt_bb)
#define X0ADVFLAGS(RTX, N) (RTL_CHECK1 (RTX, N, '0').rt_addr_diff_vec_flags)
#define X0CSELIB(RTX, N)   (RTL_CHECK1 (RTX, N, '0').rt_cselib)
#define X0MEMATTR(RTX, N)  (RTL_CHECKC1 (RTX, N, MEM).rt_mem)
#define X0CONSTANT(RTX, N) (RTL_CHECK1 (RTX, N, '0').rt_constant)

/* Access a '0' field with any type.  */
#define X0ANY(RTX, N)	   RTL_CHECK1 (RTX, N, '0')

#define XCINT(RTX, N, C)      (RTL_CHECKC1 (RTX, N, C).rt_int)
#define XCUINT(RTX, N, C)     (RTL_CHECKC1 (RTX, N, C).rt_uint)
#define XCSUBREG(RTX, N, C)   (RTL_CHECKC1 (RTX, N, C).rt_subreg)
#define XCSTR(RTX, N, C)      (RTL_CHECKC1 (RTX, N, C).rt_str)
#define XCEXP(RTX, N, C)      (RTL_CHECKC1 (RTX, N, C).rt_rtx)
#define XCVEC(RTX, N, C)      (RTL_CHECKC1 (RTX, N, C).rt_rtvec)
#define XCMODE(RTX, N, C)     (RTL_CHECKC1 (RTX, N, C).rt_type)
#define XCTREE(RTX, N, C)     (RTL_CHECKC1 (RTX, N, C).rt_tree)
#define XCBBDEF(RTX, N, C)    (RTL_CHECKC1 (RTX, N, C).rt_bb)
#define XCCFI(RTX, N, C)      (RTL_CHECKC1 (RTX, N, C).rt_cfi)
#define XCCSELIB(RTX, N, C)   (RTL_CHECKC1 (RTX, N, C).rt_cselib)

#define XCVECEXP(RTX, N, M, C)	RTVEC_ELT (XCVEC (RTX, N, C), M)
#define XCVECLEN(RTX, N, C)	GET_NUM_ELEM (XCVEC (RTX, N, C))

#define XC2EXP(RTX, N, C1, C2)      (RTL_CHECKC2 (RTX, N, C1, C2).rt_rtx)


/* Methods of rtx_expr_list.  */

inline rtx_expr_list *rtx_expr_list::next () const
{
  rtx tmp = XEXP (this, 1);
  return safe_as_a <rtx_expr_list *> (tmp);
}

inline rtx rtx_expr_list::element () const
{
  return XEXP (this, 0);
}

/* Methods of rtx_insn_list.  */

inline rtx_insn_list *rtx_insn_list::next () const
{
  rtx tmp = XEXP (this, 1);
  return safe_as_a <rtx_insn_list *> (tmp);
}

inline rtx_insn *rtx_insn_list::insn () const
{
  rtx tmp = XEXP (this, 0);
  return safe_as_a <rtx_insn *> (tmp);
}

/* Methods of rtx_sequence.  */

inline int rtx_sequence::len () const
{
  return XVECLEN (this, 0);
}

inline rtx rtx_sequence::element (int index) const
{
  return XVECEXP (this, 0, index);
}

inline rtx_insn *rtx_sequence::insn (int index) const
{
  return as_a <rtx_insn *> (XVECEXP (this, 0, index));
}

/* ACCESS MACROS for particular fields of insns.  */

/* Holds a unique number for each insn.
   These are not necessarily sequentially increasing.  */
inline int INSN_UID (const_rtx insn)
{
  return RTL_INSN_CHAIN_FLAG_CHECK ("INSN_UID",
				    (insn))->u2.insn_uid;
}
inline int& INSN_UID (rtx insn)
{
  return RTL_INSN_CHAIN_FLAG_CHECK ("INSN_UID",
				    (insn))->u2.insn_uid;
}

/* Chain insns together in sequence.  */

/* For now these are split in two: an rvalue form:
     PREV_INSN/NEXT_INSN
   and an lvalue form:
     SET_NEXT_INSN/SET_PREV_INSN.  */

inline rtx_insn *PREV_INSN (const rtx_insn *insn)
{
  rtx prev = XEXP (insn, 0);
  return safe_as_a <rtx_insn *> (prev);
}

inline rtx& SET_PREV_INSN (rtx_insn *insn)
{
  return XEXP (insn, 0);
}

inline rtx_insn *NEXT_INSN (const rtx_insn *insn)
{
  rtx next = XEXP (insn, 1);
  return safe_as_a <rtx_insn *> (next);
}

inline rtx& SET_NEXT_INSN (rtx_insn *insn)
{
  return XEXP (insn, 1);
}

inline basic_block BLOCK_FOR_INSN (const_rtx insn)
{
  return XBBDEF (insn, 2);
}

inline basic_block& BLOCK_FOR_INSN (rtx insn)
{
  return XBBDEF (insn, 2);
}

inline void set_block_for_insn (rtx_insn *insn, basic_block bb)
{
  BLOCK_FOR_INSN (insn) = bb;
}

/* The body of an insn.  */
inline rtx PATTERN (const_rtx insn)
{
  return XEXP (insn, 3);
}

inline rtx& PATTERN (rtx insn)
{
  return XEXP (insn, 3);
}

inline unsigned int INSN_LOCATION (const rtx_insn *insn)
{
  return XUINT (insn, 4);
}

inline unsigned int& INSN_LOCATION (rtx_insn *insn)
{
  return XUINT (insn, 4);
}

inline bool INSN_HAS_LOCATION (const rtx_insn *insn)
{
  return LOCATION_LOCUS (INSN_LOCATION (insn)) != UNKNOWN_LOCATION;
}

/* LOCATION of an RTX if relevant.  */
#define RTL_LOCATION(X) (INSN_P (X) ? \
			 INSN_LOCATION (as_a <rtx_insn *> (X)) \
			 : UNKNOWN_LOCATION)

/* Code number of instruction, from when it was recognized.
   -1 means this instruction has not been recognized yet.  */
#define INSN_CODE(INSN) XINT (INSN, 5)

inline rtvec rtx_jump_table_data::get_labels () const
{
  rtx pat = PATTERN (this);
  if (GET_CODE (pat) == ADDR_VEC)
    return XVEC (pat, 0);
  else
    return XVEC (pat, 1); /* presumably an ADDR_DIFF_VEC */
}

/* Return the mode of the data in the table, which is always a scalar
   integer.  */

inline scalar_int_mode
rtx_jump_table_data::get_data_mode () const
{
  return as_a <scalar_int_mode> (GET_MODE (PATTERN (this)));
}

/* If LABEL is followed by a jump table, return the table, otherwise
   return null.  */

inline rtx_jump_table_data *
jump_table_for_label (const rtx_code_label *label)
{
  return safe_dyn_cast <rtx_jump_table_data *> (NEXT_INSN (label));
}

#define RTX_FRAME_RELATED_P(RTX)					\
  (RTL_FLAG_CHECK6 ("RTX_FRAME_RELATED_P", (RTX), DEBUG_INSN, INSN,	\
		    CALL_INSN, JUMP_INSN, BARRIER, SET)->frame_related)

/* 1 if JUMP RTX is a crossing jump.  */
#define CROSSING_JUMP_P(RTX) \
  (RTL_FLAG_CHECK1 ("CROSSING_JUMP_P", (RTX), JUMP_INSN)->jump)

/* 1 if RTX is a call to a const function.  Built from ECF_CONST and
   TREE_READONLY.  */
#define RTL_CONST_CALL_P(RTX)					\
  (RTL_FLAG_CHECK1 ("RTL_CONST_CALL_P", (RTX), CALL_INSN)->unchanging)

/* 1 if RTX is a call to a pure function.  Built from ECF_PURE and
   DECL_PURE_P.  */
#define RTL_PURE_CALL_P(RTX)					\
  (RTL_FLAG_CHECK1 ("RTL_PURE_CALL_P", (RTX), CALL_INSN)->return_val)

/* 1 if RTX is a call to a const or pure function.  */
#define RTL_CONST_OR_PURE_CALL_P(RTX) \
  (RTL_CONST_CALL_P (RTX) || RTL_PURE_CALL_P (RTX))

/* 1 if RTX is a call to a looping const or pure function.  Built from
   ECF_LOOPING_CONST_OR_PURE and DECL_LOOPING_CONST_OR_PURE_P.  */
#define RTL_LOOPING_CONST_OR_PURE_CALL_P(RTX)				\
  (RTL_FLAG_CHECK1 ("CONST_OR_PURE_CALL_P", (RTX), CALL_INSN)->call)

/* 1 if RTX is a call_insn for a sibling call.  */
#define SIBLING_CALL_P(RTX)						\
  (RTL_FLAG_CHECK1 ("SIBLING_CALL_P", (RTX), CALL_INSN)->jump)

/* 1 if RTX is a jump_insn, call_insn, or insn that is an annulling branch.  */
#define INSN_ANNULLED_BRANCH_P(RTX)					\
  (RTL_FLAG_CHECK1 ("INSN_ANNULLED_BRANCH_P", (RTX), JUMP_INSN)->unchanging)

/* 1 if RTX is an insn in a delay slot and is from the target of the branch.
   If the branch insn has INSN_ANNULLED_BRANCH_P set, this insn should only be
   executed if the branch is taken.  For annulled branches with this bit
   clear, the insn should be executed only if the branch is not taken.  */
#define INSN_FROM_TARGET_P(RTX)						\
  (RTL_FLAG_CHECK3 ("INSN_FROM_TARGET_P", (RTX), INSN, JUMP_INSN, \
		    CALL_INSN)->in_struct)

/* In an ADDR_DIFF_VEC, the flags for RTX for use by branch shortening.
   See the comments for ADDR_DIFF_VEC in rtl.def.  */
#define ADDR_DIFF_VEC_FLAGS(RTX) X0ADVFLAGS (RTX, 4)

/* In a VALUE, the value cselib has assigned to RTX.
   This is a "struct cselib_val", see cselib.h.  */
#define CSELIB_VAL_PTR(RTX) X0CSELIB (RTX, 0)

/* Holds a list of notes on what this insn does to various REGs.
   It is a chain of EXPR_LIST rtx's, where the second operand is the
   chain pointer and the first operand is the REG being described.
   The mode field of the EXPR_LIST contains not a real machine mode
   but a value from enum reg_note.  */
#define REG_NOTES(INSN)	XEXP(INSN, 6)

/* In an ENTRY_VALUE this is the DECL_INCOMING_RTL of the argument in
   question.  */
#define ENTRY_VALUE_EXP(RTX) (RTL_CHECKC1 (RTX, 0, ENTRY_VALUE).rt_rtx)

enum reg_note
{
#define DEF_REG_NOTE(NAME) NAME,
#include "reg-notes.def"
#undef DEF_REG_NOTE
  REG_NOTE_MAX
};

/* Define macros to extract and insert the reg-note kind in an EXPR_LIST.  */
#define REG_NOTE_KIND(LINK) ((enum reg_note) GET_MODE (LINK))
#define PUT_REG_NOTE_KIND(LINK, KIND) \
  PUT_MODE_RAW (LINK, (machine_mode) (KIND))

/* Names for REG_NOTE's in EXPR_LIST insn's.  */

extern const char * const reg_note_name[];
#define GET_REG_NOTE_NAME(MODE) (reg_note_name[(int) (MODE)])

/* This field is only present on CALL_INSNs.  It holds a chain of EXPR_LIST of
   USE and CLOBBER expressions.
     USE expressions list the registers filled with arguments that
   are passed to the function.
     CLOBBER expressions document the registers explicitly clobbered
   by this CALL_INSN.
     Pseudo registers can not be mentioned in this list.  */
#define CALL_INSN_FUNCTION_USAGE(INSN)	XEXP(INSN, 7)

/* The label-number of a code-label.  The assembler label
   is made from `L' and the label-number printed in decimal.
   Label numbers are unique in a compilation.  */
#define CODE_LABEL_NUMBER(INSN)	XINT (INSN, 5)

/* In a NOTE that is a line number, this is a string for the file name that the
   line is in.  We use the same field to record block numbers temporarily in
   NOTE_INSN_BLOCK_BEG and NOTE_INSN_BLOCK_END notes.  (We avoid lots of casts
   between ints and pointers if we use a different macro for the block number.)
   */

/* Opaque data.  */
#define NOTE_DATA(INSN)	        RTL_CHECKC1 (INSN, 3, NOTE)
#define NOTE_DELETED_LABEL_NAME(INSN) XCSTR (INSN, 3, NOTE)
#define SET_INSN_DELETED(INSN) set_insn_deleted (INSN);
#define NOTE_BLOCK(INSN)	XCTREE (INSN, 3, NOTE)
#define NOTE_EH_HANDLER(INSN)	XCINT (INSN, 3, NOTE)
#define NOTE_BASIC_BLOCK(INSN)	XCBBDEF (INSN, 3, NOTE)
#define NOTE_VAR_LOCATION(INSN)	XCEXP (INSN, 3, NOTE)
#define NOTE_MARKER_LOCATION(INSN) XCUINT (INSN, 3, NOTE)
#define NOTE_CFI(INSN)		XCCFI (INSN, 3, NOTE)
#define NOTE_LABEL_NUMBER(INSN)	XCINT (INSN, 3, NOTE)

/* In a NOTE that is a line number, this is the line number.
   Other kinds of NOTEs are identified by negative numbers here.  */
#define NOTE_KIND(INSN) XCINT (INSN, 4, NOTE)

/* Nonzero if INSN is a note marking the beginning of a basic block.  */
#define NOTE_INSN_BASIC_BLOCK_P(INSN) \
  (NOTE_P (INSN) && NOTE_KIND (INSN) == NOTE_INSN_BASIC_BLOCK)

/* Nonzero if INSN is a debug nonbind marker note,
   for which NOTE_MARKER_LOCATION can be used.  */
#define NOTE_MARKER_P(INSN)				\
  (NOTE_P (INSN) &&					\
   (NOTE_KIND (INSN) == NOTE_INSN_BEGIN_STMT		\
    || NOTE_KIND (INSN) == NOTE_INSN_INLINE_ENTRY))

/* Variable declaration and the location of a variable.  */
#define PAT_VAR_LOCATION_DECL(PAT) (XCTREE ((PAT), 0, VAR_LOCATION))
#define PAT_VAR_LOCATION_LOC(PAT) (XCEXP ((PAT), 1, VAR_LOCATION))

/* Initialization status of the variable in the location.  Status
   can be unknown, uninitialized or initialized.  See enumeration
   type below.  */
#define PAT_VAR_LOCATION_STATUS(PAT) \
  (RTL_FLAG_CHECK1 ("PAT_VAR_LOCATION_STATUS", PAT, VAR_LOCATION) \
   ->u2.var_location_status)

/* Accessors for a NOTE_INSN_VAR_LOCATION.  */
#define NOTE_VAR_LOCATION_DECL(NOTE) \
  PAT_VAR_LOCATION_DECL (NOTE_VAR_LOCATION (NOTE))
#define NOTE_VAR_LOCATION_LOC(NOTE) \
  PAT_VAR_LOCATION_LOC (NOTE_VAR_LOCATION (NOTE))
#define NOTE_VAR_LOCATION_STATUS(NOTE) \
  PAT_VAR_LOCATION_STATUS (NOTE_VAR_LOCATION (NOTE))

/* Evaluate to TRUE if INSN is a debug insn that denotes a variable
   location/value tracking annotation.  */
#define DEBUG_BIND_INSN_P(INSN)			\
  (DEBUG_INSN_P (INSN)				\
   && (GET_CODE (PATTERN (INSN))		\
       == VAR_LOCATION))
/* Evaluate to TRUE if INSN is a debug insn that denotes a program
   source location marker.  */
#define DEBUG_MARKER_INSN_P(INSN)		\
  (DEBUG_INSN_P (INSN)				\
   && (GET_CODE (PATTERN (INSN))		\
       != VAR_LOCATION))
/* Evaluate to the marker kind.  */
#define INSN_DEBUG_MARKER_KIND(INSN)		  \
  (GET_CODE (PATTERN (INSN)) == DEBUG_MARKER	  \
   ? (GET_MODE (PATTERN (INSN)) == VOIDmode	  \
      ? NOTE_INSN_BEGIN_STMT			  \
      : GET_MODE (PATTERN (INSN)) == BLKmode	  \
      ? NOTE_INSN_INLINE_ENTRY			  \
      : (enum insn_note)-1) 			  \
   : (enum insn_note)-1)
/* Create patterns for debug markers.  These and the above abstract
   the representation, so that it's easier to get rid of the abuse of
   the mode to hold the marker kind.  Other marker types are
   envisioned, so a single bit flag won't do; maybe separate RTL codes
   wouldn't be a problem.  */
#define GEN_RTX_DEBUG_MARKER_BEGIN_STMT_PAT() \
  gen_rtx_DEBUG_MARKER (VOIDmode)
#define GEN_RTX_DEBUG_MARKER_INLINE_ENTRY_PAT() \
  gen_rtx_DEBUG_MARKER (BLKmode)

/* The VAR_LOCATION rtx in a DEBUG_INSN.  */
#define INSN_VAR_LOCATION(INSN) \
  (RTL_FLAG_CHECK1 ("INSN_VAR_LOCATION", PATTERN (INSN), VAR_LOCATION))
/* A pointer to the VAR_LOCATION rtx in a DEBUG_INSN.  */
#define INSN_VAR_LOCATION_PTR(INSN) \
  (&PATTERN (INSN))

/* Accessors for a tree-expanded var location debug insn.  */
#define INSN_VAR_LOCATION_DECL(INSN) \
  PAT_VAR_LOCATION_DECL (INSN_VAR_LOCATION (INSN))
#define INSN_VAR_LOCATION_LOC(INSN) \
  PAT_VAR_LOCATION_LOC (INSN_VAR_LOCATION (INSN))
#define INSN_VAR_LOCATION_STATUS(INSN) \
  PAT_VAR_LOCATION_STATUS (INSN_VAR_LOCATION (INSN))

/* Expand to the RTL that denotes an unknown variable location in a
   DEBUG_INSN.  */
#define gen_rtx_UNKNOWN_VAR_LOC() (gen_rtx_CLOBBER (VOIDmode, const0_rtx))

/* Determine whether X is such an unknown location.  */
#define VAR_LOC_UNKNOWN_P(X) \
  (GET_CODE (X) == CLOBBER && XEXP ((X), 0) == const0_rtx)

/* 1 if RTX is emitted after a call, but it should take effect before
   the call returns.  */
#define NOTE_DURING_CALL_P(RTX)				\
  (RTL_FLAG_CHECK1 ("NOTE_VAR_LOCATION_DURING_CALL_P", (RTX), NOTE)->call)

/* DEBUG_EXPR_DECL corresponding to a DEBUG_EXPR RTX.  */
#define DEBUG_EXPR_TREE_DECL(RTX) XCTREE (RTX, 0, DEBUG_EXPR)

/* VAR_DECL/PARM_DECL DEBUG_IMPLICIT_PTR takes address of.  */
#define DEBUG_IMPLICIT_PTR_DECL(RTX) XCTREE (RTX, 0, DEBUG_IMPLICIT_PTR)

/* PARM_DECL DEBUG_PARAMETER_REF references.  */
#define DEBUG_PARAMETER_REF_DECL(RTX) XCTREE (RTX, 0, DEBUG_PARAMETER_REF)

/* Codes that appear in the NOTE_KIND field for kinds of notes
   that are not line numbers.  These codes are all negative.

   Notice that we do not try to use zero here for any of
   the special note codes because sometimes the source line
   actually can be zero!  This happens (for example) when we
   are generating code for the per-translation-unit constructor
   and destructor routines for some C++ translation unit.  */

enum insn_note
{
#define DEF_INSN_NOTE(NAME) NAME,
#include "insn-notes.def"
#undef DEF_INSN_NOTE

  NOTE_INSN_MAX
};

/* Names for NOTE insn's other than line numbers.  */

extern const char * const note_insn_name[NOTE_INSN_MAX];
#define GET_NOTE_INSN_NAME(NOTE_CODE) \
  (note_insn_name[(NOTE_CODE)])

/* The name of a label, in case it corresponds to an explicit label
   in the input source code.  */
#define LABEL_NAME(RTX) XCSTR (RTX, 6, CODE_LABEL)

/* In jump.c, each label contains a count of the number
   of LABEL_REFs that point at it, so unused labels can be deleted.  */
#define LABEL_NUSES(RTX) XCINT (RTX, 4, CODE_LABEL)

/* Labels carry a two-bit field composed of the ->jump and ->call
   bits.  This field indicates whether the label is an alternate
   entry point, and if so, what kind.  */
enum label_kind
{
  LABEL_NORMAL = 0,	/* ordinary label */
  LABEL_STATIC_ENTRY,	/* alternate entry point, not exported */
  LABEL_GLOBAL_ENTRY,	/* alternate entry point, exported */
  LABEL_WEAK_ENTRY	/* alternate entry point, exported as weak symbol */
};

#if defined ENABLE_RTL_FLAG_CHECKING && (GCC_VERSION > 2007)

/* Retrieve the kind of LABEL.  */
#define LABEL_KIND(LABEL) __extension__					\
({ __typeof (LABEL) const _label = (LABEL);				\
   if (! LABEL_P (_label))						\
     rtl_check_failed_flag ("LABEL_KIND", _label, __FILE__, __LINE__,	\
			    __FUNCTION__);				\
   (enum label_kind) ((_label->jump << 1) | _label->call); })

/* Set the kind of LABEL.  */
#define SET_LABEL_KIND(LABEL, KIND) do {				\
   __typeof (LABEL) const _label = (LABEL);				\
   const unsigned int _kind = (KIND);					\
   if (! LABEL_P (_label))						\
     rtl_check_failed_flag ("SET_LABEL_KIND", _label, __FILE__, __LINE__, \
			    __FUNCTION__);				\
   _label->jump = ((_kind >> 1) & 1);					\
   _label->call = (_kind & 1);						\
} while (0)

#else

/* Retrieve the kind of LABEL.  */
#define LABEL_KIND(LABEL) \
   ((enum label_kind) (((LABEL)->jump << 1) | (LABEL)->call))

/* Set the kind of LABEL.  */
#define SET_LABEL_KIND(LABEL, KIND) do {				\
   rtx const _label = (LABEL);						\
   const unsigned int _kind = (KIND);					\
   _label->jump = ((_kind >> 1) & 1);					\
   _label->call = (_kind & 1);						\
} while (0)

#endif /* rtl flag checking */

#define LABEL_ALT_ENTRY_P(LABEL) (LABEL_KIND (LABEL) != LABEL_NORMAL)

/* In jump.c, each JUMP_INSN can point to a label that it can jump to,
   so that if the JUMP_INSN is deleted, the label's LABEL_NUSES can
   be decremented and possibly the label can be deleted.  */
#define JUMP_LABEL(INSN)   XCEXP (INSN, 7, JUMP_INSN)

inline rtx_insn *JUMP_LABEL_AS_INSN (const rtx_insn *insn)
{
  return safe_as_a <rtx_insn *> (JUMP_LABEL (insn));
}

/* Methods of rtx_jump_insn.  */

inline rtx rtx_jump_insn::jump_label () const
{
  return JUMP_LABEL (this);
}

inline rtx_code_label *rtx_jump_insn::jump_target () const
{
  return safe_as_a <rtx_code_label *> (JUMP_LABEL (this));
}

inline void rtx_jump_insn::set_jump_target (rtx_code_label *target)
{
  JUMP_LABEL (this) = target;
}

/* Once basic blocks are found, each CODE_LABEL starts a chain that
   goes through all the LABEL_REFs that jump to that label.  The chain
   eventually winds up at the CODE_LABEL: it is circular.  */
#define LABEL_REFS(LABEL) XCEXP (LABEL, 3, CODE_LABEL)

/* Get the label that a LABEL_REF references.  */
static inline rtx_insn *
label_ref_label (const_rtx ref)
{
  return as_a<rtx_insn *> (XCEXP (ref, 0, LABEL_REF));
}

/* Set the label that LABEL_REF ref refers to.  */

static inline void
set_label_ref_label (rtx ref, rtx_insn *label)
{
  XCEXP (ref, 0, LABEL_REF) = label;
}

/* For a REG rtx, REGNO extracts the register number.  REGNO can only
   be used on RHS.  Use SET_REGNO to change the value.  */
#define REGNO(RTX) (rhs_regno(RTX))
#define SET_REGNO(RTX, N) (df_ref_change_reg_with_loc (RTX, N))

/* Return the number of consecutive registers in a REG.  This is always
   1 for pseudo registers and is determined by TARGET_HARD_REGNO_NREGS for
   hard registers.  */
#define REG_NREGS(RTX) (REG_CHECK (RTX)->nregs)

/* ORIGINAL_REGNO holds the number the register originally had; for a
   pseudo register turned into a hard reg this will hold the old pseudo
   register number.  */
#define ORIGINAL_REGNO(RTX) \
  (RTL_FLAG_CHECK1 ("ORIGINAL_REGNO", (RTX), REG)->u2.original_regno)

/* Force the REGNO macro to only be used on the lhs.  */
static inline unsigned int
rhs_regno (const_rtx x)
{
  return REG_CHECK (x)->regno;
}

/* Return the final register in REG X plus one.  */
static inline unsigned int
END_REGNO (const_rtx x)
{
  return REGNO (x) + REG_NREGS (x);
}

/* Change the REGNO and REG_NREGS of REG X to the specified values,
   bypassing the df machinery.  */
static inline void
set_regno_raw (rtx x, unsigned int regno, unsigned int nregs)
{
  reg_info *reg = REG_CHECK (x);
  reg->regno = regno;
  reg->nregs = nregs;
}

/* 1 if RTX is a reg or parallel that is the current function's return
   value.  */
#define REG_FUNCTION_VALUE_P(RTX)					\
  (RTL_FLAG_CHECK2 ("REG_FUNCTION_VALUE_P", (RTX), REG, PARALLEL)->return_val)

/* 1 if RTX is a reg that corresponds to a variable declared by the user.  */
#define REG_USERVAR_P(RTX)						\
  (RTL_FLAG_CHECK1 ("REG_USERVAR_P", (RTX), REG)->volatil)

/* 1 if RTX is a reg that holds a pointer value.  */
#define REG_POINTER(RTX)						\
  (RTL_FLAG_CHECK1 ("REG_POINTER", (RTX), REG)->frame_related)

/* 1 if RTX is a mem that holds a pointer value.  */
#define MEM_POINTER(RTX)						\
  (RTL_FLAG_CHECK1 ("MEM_POINTER", (RTX), MEM)->frame_related)

/* 1 if the given register REG corresponds to a hard register.  */
#define HARD_REGISTER_P(REG) (HARD_REGISTER_NUM_P (REGNO (REG)))

/* 1 if the given register number REG_NO corresponds to a hard register.  */
#define HARD_REGISTER_NUM_P(REG_NO) ((REG_NO) < FIRST_PSEUDO_REGISTER)

/* For a CONST_INT rtx, INTVAL extracts the integer.  */
#define INTVAL(RTX) XCWINT (RTX, 0, CONST_INT)
#define UINTVAL(RTX) ((unsigned HOST_WIDE_INT) INTVAL (RTX))

/* For a CONST_WIDE_INT, CONST_WIDE_INT_NUNITS is the number of
   elements actually needed to represent the constant.
   CONST_WIDE_INT_ELT gets one of the elements.  0 is the least
   significant HOST_WIDE_INT.  */
#define CONST_WIDE_INT_VEC(RTX) HWIVEC_CHECK (RTX, CONST_WIDE_INT)
#define CONST_WIDE_INT_NUNITS(RTX) CWI_GET_NUM_ELEM (RTX)
#define CONST_WIDE_INT_ELT(RTX, N) CWI_ELT (RTX, N)

/* For a CONST_POLY_INT, CONST_POLY_INT_COEFFS gives access to the
   individual coefficients, in the form of a trailing_wide_ints structure.  */
#define CONST_POLY_INT_COEFFS(RTX) \
  (RTL_FLAG_CHECK1("CONST_POLY_INT_COEFFS", (RTX), \
		   CONST_POLY_INT)->u.cpi.coeffs)

/* For a CONST_DOUBLE:
#if TARGET_SUPPORTS_WIDE_INT == 0
   For a VOIDmode, there are two integers CONST_DOUBLE_LOW is the
     low-order word and ..._HIGH the high-order.
#endif
   For a float, there is a REAL_VALUE_TYPE structure, and
     CONST_DOUBLE_REAL_VALUE(r) is a pointer to it.  */
#define CONST_DOUBLE_LOW(r) XCMWINT (r, 0, CONST_DOUBLE, VOIDmode)
#define CONST_DOUBLE_HIGH(r) XCMWINT (r, 1, CONST_DOUBLE, VOIDmode)
#define CONST_DOUBLE_REAL_VALUE(r) \
  ((const struct real_value *) XCNMPRV (r, CONST_DOUBLE, VOIDmode))

#define CONST_FIXED_VALUE(r) \
  ((const struct fixed_value *) XCNMPFV (r, CONST_FIXED, VOIDmode))
#define CONST_FIXED_VALUE_HIGH(r) \
  ((HOST_WIDE_INT) (CONST_FIXED_VALUE (r)->data.high))
#define CONST_FIXED_VALUE_LOW(r) \
  ((HOST_WIDE_INT) (CONST_FIXED_VALUE (r)->data.low))

/* For a CONST_VECTOR, return element #n.  */
#define CONST_VECTOR_ELT(RTX, N) const_vector_elt (RTX, N)

/* See rtl.texi for a description of these macros.  */
#define CONST_VECTOR_NPATTERNS(RTX) \
 (RTL_FLAG_CHECK1 ("CONST_VECTOR_NPATTERNS", (RTX), CONST_VECTOR) \
  ->u2.const_vector.npatterns)

#define CONST_VECTOR_NELTS_PER_PATTERN(RTX) \
 (RTL_FLAG_CHECK1 ("CONST_VECTOR_NELTS_PER_PATTERN", (RTX), CONST_VECTOR) \
  ->u2.const_vector.nelts_per_pattern)

#define CONST_VECTOR_DUPLICATE_P(RTX) \
  (CONST_VECTOR_NELTS_PER_PATTERN (RTX) == 1)

#define CONST_VECTOR_STEPPED_P(RTX) \
  (CONST_VECTOR_NELTS_PER_PATTERN (RTX) == 3)

#define CONST_VECTOR_ENCODED_ELT(RTX, N) XCVECEXP (RTX, 0, N, CONST_VECTOR)

/* Return the number of elements encoded directly in a CONST_VECTOR.  */

inline unsigned int
const_vector_encoded_nelts (const_rtx x)
{
  return CONST_VECTOR_NPATTERNS (x) * CONST_VECTOR_NELTS_PER_PATTERN (x);
}

/* For a CONST_VECTOR, return the number of elements in a vector.  */
#define CONST_VECTOR_NUNITS(RTX) GET_MODE_NUNITS (GET_MODE (RTX))

/* For a SUBREG rtx, SUBREG_REG extracts the value we want a subreg of.
   SUBREG_BYTE extracts the byte-number.  */

#define SUBREG_REG(RTX) XCEXP (RTX, 0, SUBREG)
#define SUBREG_BYTE(RTX) XCSUBREG (RTX, 1, SUBREG)

/* in rtlanal.c */
/* Return the right cost to give to an operation
   to make the cost of the corresponding register-to-register instruction
   N times that of a fast register-to-register instruction.  */
#define COSTS_N_INSNS(N) ((N) * 4)

/* Maximum cost of an rtl expression.  This value has the special meaning
   not to use an rtx with this cost under any circumstances.  */
#define MAX_COST INT_MAX

/* Return true if CODE always has VOIDmode.  */

static inline bool
always_void_p (enum rtx_code code)
{
  return code == SET;
}

/* A structure to hold all available cost information about an rtl
   expression.  */
struct full_rtx_costs
{
  int speed;
  int size;
};

/* Initialize a full_rtx_costs structure C to the maximum cost.  */
static inline void
init_costs_to_max (struct full_rtx_costs *c)
{
  c->speed = MAX_COST;
  c->size = MAX_COST;
}

/* Initialize a full_rtx_costs structure C to zero cost.  */
static inline void
init_costs_to_zero (struct full_rtx_costs *c)
{
  c->speed = 0;
  c->size = 0;
}

/* Compare two full_rtx_costs structures A and B, returning true
   if A < B when optimizing for speed.  */
static inline bool
costs_lt_p (struct full_rtx_costs *a, struct full_rtx_costs *b,
	    bool speed)
{
  if (speed)
    return (a->speed < b->speed
	    || (a->speed == b->speed && a->size < b->size));
  else
    return (a->size < b->size
	    || (a->size == b->size && a->speed < b->speed));
}

/* Increase both members of the full_rtx_costs structure C by the
   cost of N insns.  */
static inline void
costs_add_n_insns (struct full_rtx_costs *c, int n)
{
  c->speed += COSTS_N_INSNS (n);
  c->size += COSTS_N_INSNS (n);
}

/* Describes the shape of a subreg:

   inner_mode == the mode of the SUBREG_REG
   offset     == the SUBREG_BYTE
   outer_mode == the mode of the SUBREG itself.  */
struct subreg_shape {
  subreg_shape (machine_mode, poly_uint16, machine_mode);
  bool operator == (const subreg_shape &) const;
  bool operator != (const subreg_shape &) const;
  unsigned HOST_WIDE_INT unique_id () const;

  machine_mode inner_mode;
  poly_uint16 offset;
  machine_mode outer_mode;
};

inline
subreg_shape::subreg_shape (machine_mode inner_mode_in,
			    poly_uint16 offset_in,
			    machine_mode outer_mode_in)
  : inner_mode (inner_mode_in), offset (offset_in), outer_mode (outer_mode_in)
{}

inline bool
subreg_shape::operator == (const subreg_shape &other) const
{
  return (inner_mode == other.inner_mode
	  && known_eq (offset, other.offset)
	  && outer_mode == other.outer_mode);
}

inline bool
subreg_shape::operator != (const subreg_shape &other) const
{
  return !operator == (other);
}

/* Return an integer that uniquely identifies this shape.  Structures
   like rtx_def assume that a mode can fit in an 8-bit bitfield and no
   current mode is anywhere near being 65536 bytes in size, so the
   id comfortably fits in an int.  */

inline unsigned HOST_WIDE_INT
subreg_shape::unique_id () const
{
  { STATIC_ASSERT (MAX_MACHINE_MODE <= 256); }
  { STATIC_ASSERT (NUM_POLY_INT_COEFFS <= 3); }
  { STATIC_ASSERT (sizeof (offset.coeffs[0]) <= 2); }
  int res = (int) inner_mode + ((int) outer_mode << 8);
  for (int i = 0; i < NUM_POLY_INT_COEFFS; ++i)
    res += (HOST_WIDE_INT) offset.coeffs[i] << ((1 + i) * 16);
  return res;
}

/* Return the shape of a SUBREG rtx.  */

static inline subreg_shape
shape_of_subreg (const_rtx x)
{
  return subreg_shape (GET_MODE (SUBREG_REG (x)),
		       SUBREG_BYTE (x), GET_MODE (x));
}

/* Information about an address.  This structure is supposed to be able
   to represent all supported target addresses.  Please extend it if it
   is not yet general enough.  */
struct address_info {
  /* The mode of the value being addressed, or VOIDmode if this is
     a load-address operation with no known address mode.  */
  machine_mode mode;

  /* The address space.  */
  addr_space_t as;

  /* True if this is an RTX_AUTOINC address.  */
  bool autoinc_p;

  /* A pointer to the top-level address.  */
  rtx *outer;

  /* A pointer to the inner address, after all address mutations
     have been stripped from the top-level address.  It can be one
     of the following:

     - A {PRE,POST}_{INC,DEC} of *BASE.  SEGMENT, INDEX and DISP are null.

     - A {PRE,POST}_MODIFY of *BASE.  In this case either INDEX or DISP
       points to the step value, depending on whether the step is variable
       or constant respectively.  SEGMENT is null.

     - A plain sum of the form SEGMENT + BASE + INDEX + DISP,
       with null fields evaluating to 0.  */
  rtx *inner;

  /* Components that make up *INNER.  Each one may be null or nonnull.
     When nonnull, their meanings are as follows:

     - *SEGMENT is the "segment" of memory to which the address refers.
       This value is entirely target-specific and is only called a "segment"
       because that's its most typical use.  It contains exactly one UNSPEC,
       pointed to by SEGMENT_TERM.  The contents of *SEGMENT do not need
       reloading.

     - *BASE is a variable expression representing a base address.
       It contains exactly one REG, SUBREG or MEM, pointed to by BASE_TERM.

     - *INDEX is a variable expression representing an index value.
       It may be a scaled expression, such as a MULT.  It has exactly
       one REG, SUBREG or MEM, pointed to by INDEX_TERM.

     - *DISP is a constant, possibly mutated.  DISP_TERM points to the
       unmutated RTX_CONST_OBJ.  */
  rtx *segment;
  rtx *base;
  rtx *index;
  rtx *disp;

  rtx *segment_term;
  rtx *base_term;
  rtx *index_term;
  rtx *disp_term;

  /* In a {PRE,POST}_MODIFY address, this points to a second copy
     of BASE_TERM, otherwise it is null.  */
  rtx *base_term2;

  /* ADDRESS if this structure describes an address operand, MEM if
     it describes a MEM address.  */
  enum rtx_code addr_outer_code;

  /* If BASE is nonnull, this is the code of the rtx that contains it.  */
  enum rtx_code base_outer_code;
};

/* This is used to bundle an rtx and a mode together so that the pair
   can be used with the wi:: routines.  If we ever put modes into rtx
   integer constants, this should go away and then just pass an rtx in.  */
typedef std::pair <rtx, machine_mode> rtx_mode_t;

namespace wi
{
  template <>
  struct int_traits <rtx_mode_t>
  {
    static const enum precision_type precision_type = VAR_PRECISION;
    static const bool host_dependent_precision = false;
    /* This ought to be true, except for the special case that BImode
       is canonicalized to STORE_FLAG_VALUE, which might be 1.  */
    static const bool is_sign_extended = false;
    static unsigned int get_precision (const rtx_mode_t &);
    static wi::storage_ref decompose (HOST_WIDE_INT *, unsigned int,
				      const rtx_mode_t &);
  };
}

inline unsigned int
wi::int_traits <rtx_mode_t>::get_precision (const rtx_mode_t &x)
{
  return GET_MODE_PRECISION (as_a <scalar_mode> (x.second));
}

inline wi::storage_ref
wi::int_traits <rtx_mode_t>::decompose (HOST_WIDE_INT *,
					unsigned int precision,
					const rtx_mode_t &x)
{
  gcc_checking_assert (precision == get_precision (x));
  switch (GET_CODE (x.first))
    {
    case CONST_INT:
      if (precision < HOST_BITS_PER_WIDE_INT)
	/* Nonzero BImodes are stored as STORE_FLAG_VALUE, which on many
	   targets is 1 rather than -1.  */
	gcc_checking_assert (INTVAL (x.first)
			     == sext_hwi (INTVAL (x.first), precision)
			     || (x.second == BImode && INTVAL (x.first) == 1));

      return wi::storage_ref (&INTVAL (x.first), 1, precision);

    case CONST_WIDE_INT:
      return wi::storage_ref (&CONST_WIDE_INT_ELT (x.first, 0),
			      CONST_WIDE_INT_NUNITS (x.first), precision);

#if TARGET_SUPPORTS_WIDE_INT == 0
    case CONST_DOUBLE:
      return wi::storage_ref (&CONST_DOUBLE_LOW (x.first), 2, precision);
#endif

    default:
      gcc_unreachable ();
    }
}

namespace wi
{
  hwi_with_prec shwi (HOST_WIDE_INT, machine_mode mode);
  wide_int min_value (machine_mode, signop);
  wide_int max_value (machine_mode, signop);
}

inline wi::hwi_with_prec
wi::shwi (HOST_WIDE_INT val, machine_mode mode)
{
  return shwi (val, GET_MODE_PRECISION (as_a <scalar_mode> (mode)));
}

/* Produce the smallest number that is represented in MODE.  The precision
   is taken from MODE and the sign from SGN.  */
inline wide_int
wi::min_value (machine_mode mode, signop sgn)
{
  return min_value (GET_MODE_PRECISION (as_a <scalar_mode> (mode)), sgn);
}

/* Produce the largest number that is represented in MODE.  The precision
   is taken from MODE and the sign from SGN.  */
inline wide_int
wi::max_value (machine_mode mode, signop sgn)
{
  return max_value (GET_MODE_PRECISION (as_a <scalar_mode> (mode)), sgn);
}

namespace wi
{
  typedef poly_int<NUM_POLY_INT_COEFFS,
		   generic_wide_int <wide_int_ref_storage <false, false> > >
    rtx_to_poly_wide_ref;
  rtx_to_poly_wide_ref to_poly_wide (const_rtx, machine_mode);
}

/* Return the value of a CONST_POLY_INT in its native precision.  */

inline wi::rtx_to_poly_wide_ref
const_poly_int_value (const_rtx x)
{
  poly_int<NUM_POLY_INT_COEFFS, WIDE_INT_REF_FOR (wide_int)> res;
  for (unsigned int i = 0; i < NUM_POLY_INT_COEFFS; ++i)
    res.coeffs[i] = CONST_POLY_INT_COEFFS (x)[i];
  return res;
}

/* Return true if X is a scalar integer or a CONST_POLY_INT.  The value
   can then be extracted using wi::to_poly_wide.  */

inline bool
poly_int_rtx_p (const_rtx x)
{
  return CONST_SCALAR_INT_P (x) || CONST_POLY_INT_P (x);
}

/* Access X (which satisfies poly_int_rtx_p) as a poly_wide_int.
   MODE is the mode of X.  */

inline wi::rtx_to_poly_wide_ref
wi::to_poly_wide (const_rtx x, machine_mode mode)
{
  if (CONST_POLY_INT_P (x))
    return const_poly_int_value (x);
  return rtx_mode_t (const_cast<rtx> (x), mode);
}

/* Return the value of X as a poly_int64.  */

inline poly_int64
rtx_to_poly_int64 (const_rtx x)
{
  if (CONST_POLY_INT_P (x))
    {
      poly_int64 res;
      for (unsigned int i = 0; i < NUM_POLY_INT_COEFFS; ++i)
	res.coeffs[i] = CONST_POLY_INT_COEFFS (x)[i].to_shwi ();
      return res;
    }
  return INTVAL (x);
}

/* Return true if arbitrary value X is an integer constant that can
   be represented as a poly_int64.  Store the value in *RES if so,
   otherwise leave it unmodified.  */

inline bool
poly_int_rtx_p (const_rtx x, poly_int64_pod *res)
{
  if (CONST_INT_P (x))
    {
      *res = INTVAL (x);
      return true;
    }
  if (CONST_POLY_INT_P (x))
    {
      for (unsigned int i = 0; i < NUM_POLY_INT_COEFFS; ++i)
	if (!wi::fits_shwi_p (CONST_POLY_INT_COEFFS (x)[i]))
	  return false;
      for (unsigned int i = 0; i < NUM_POLY_INT_COEFFS; ++i)
	res->coeffs[i] = CONST_POLY_INT_COEFFS (x)[i].to_shwi ();
      return true;
    }
  return false;
}

extern void init_rtlanal (void);
extern int rtx_cost (rtx, machine_mode, enum rtx_code, int, bool);
extern int address_cost (rtx, machine_mode, addr_space_t, bool);
extern void get_full_rtx_cost (rtx, machine_mode, enum rtx_code, int,
			       struct full_rtx_costs *);
extern poly_uint64 subreg_lsb (const_rtx);
extern poly_uint64 subreg_lsb_1 (machine_mode, machine_mode, poly_uint64);
extern poly_uint64 subreg_size_offset_from_lsb (poly_uint64, poly_uint64,
						poly_uint64);
extern bool read_modify_subreg_p (const_rtx);

/* Return the subreg byte offset for a subreg whose outer mode is
   OUTER_MODE, whose inner mode is INNER_MODE, and where there are
   LSB_SHIFT *bits* between the lsb of the outer value and the lsb of
   the inner value.  This is the inverse of subreg_lsb_1 (which converts
   byte offsets to bit shifts).  */

inline poly_uint64
subreg_offset_from_lsb (machine_mode outer_mode,
			machine_mode inner_mode,
			poly_uint64 lsb_shift)
{
  return subreg_size_offset_from_lsb (GET_MODE_SIZE (outer_mode),
				      GET_MODE_SIZE (inner_mode), lsb_shift);
}

extern unsigned int subreg_regno_offset (unsigned int, machine_mode,
					 poly_uint64, machine_mode);
extern bool subreg_offset_representable_p (unsigned int, machine_mode,
					   poly_uint64, machine_mode);
extern unsigned int subreg_regno (const_rtx);
extern int simplify_subreg_regno (unsigned int, machine_mode,
				  poly_uint64, machine_mode);
extern unsigned int subreg_nregs (const_rtx);
extern unsigned int subreg_nregs_with_regno (unsigned int, const_rtx);
extern unsigned HOST_WIDE_INT nonzero_bits (const_rtx, machine_mode);
extern unsigned int num_sign_bit_copies (const_rtx, machine_mode);
extern bool constant_pool_constant_p (rtx);
extern bool truncated_to_mode (machine_mode, const_rtx);
extern int low_bitmask_len (machine_mode, unsigned HOST_WIDE_INT);
extern void split_double (rtx, rtx *, rtx *);
extern rtx *strip_address_mutations (rtx *, enum rtx_code * = 0);
extern void decompose_address (struct address_info *, rtx *,
			       machine_mode, addr_space_t, enum rtx_code);
extern void decompose_lea_address (struct address_info *, rtx *);
extern void decompose_mem_address (struct address_info *, rtx);
extern void update_address (struct address_info *);
extern HOST_WIDE_INT get_index_scale (const struct address_info *);
extern enum rtx_code get_index_code (const struct address_info *);

/* 1 if RTX is a subreg containing a reg that is already known to be
   sign- or zero-extended from the mode of the subreg to the mode of
   the reg.  SUBREG_PROMOTED_UNSIGNED_P gives the signedness of the
   extension.

   When used as a LHS, is means that this extension must be done
   when assigning to SUBREG_REG.  */

#define SUBREG_PROMOTED_VAR_P(RTX)					\
  (RTL_FLAG_CHECK1 ("SUBREG_PROMOTED", (RTX), SUBREG)->in_struct)

/* Valid for subregs which are SUBREG_PROMOTED_VAR_P().  In that case
   this gives the necessary extensions:
   0  - signed (SPR_SIGNED)
   1  - normal unsigned (SPR_UNSIGNED)
   2  - value is both sign and unsign extended for mode
	(SPR_SIGNED_AND_UNSIGNED).
   -1 - pointer unsigned, which most often can be handled like unsigned
        extension, except for generating instructions where we need to
	emit special code (ptr_extend insns) on some architectures
	(SPR_POINTER). */

const int SRP_POINTER = -1;
const int SRP_SIGNED = 0;
const int SRP_UNSIGNED = 1;
const int SRP_SIGNED_AND_UNSIGNED = 2;

/* Sets promoted mode for SUBREG_PROMOTED_VAR_P().  */
#define SUBREG_PROMOTED_SET(RTX, VAL)		                        \
do {								        \
  rtx const _rtx = RTL_FLAG_CHECK1 ("SUBREG_PROMOTED_SET",		\
                                    (RTX), SUBREG);			\
  switch (VAL)								\
  {									\
    case SRP_POINTER:							\
      _rtx->volatil = 0;						\
      _rtx->unchanging = 0;						\
      break;								\
    case SRP_SIGNED:							\
      _rtx->volatil = 0;						\
      _rtx->unchanging = 1;						\
      break;								\
    case SRP_UNSIGNED:							\
      _rtx->volatil = 1;						\
      _rtx->unchanging = 0;						\
      break;								\
    case SRP_SIGNED_AND_UNSIGNED:					\
      _rtx->volatil = 1;						\
      _rtx->unchanging = 1;						\
      break;								\
  }									\
} while (0)

/* Gets the value stored in promoted mode for SUBREG_PROMOTED_VAR_P(),
   including SRP_SIGNED_AND_UNSIGNED if promoted for
   both signed and unsigned.  */
#define SUBREG_PROMOTED_GET(RTX)	\
  (2 * (RTL_FLAG_CHECK1 ("SUBREG_PROMOTED_GET", (RTX), SUBREG)->volatil)\
   + (RTX)->unchanging - 1)

/* Returns sign of promoted mode for SUBREG_PROMOTED_VAR_P().  */
#define SUBREG_PROMOTED_SIGN(RTX)	\
  ((RTL_FLAG_CHECK1 ("SUBREG_PROMOTED_SIGN", (RTX), SUBREG)->volatil) ? 1\
   : (RTX)->unchanging - 1)

/* Predicate to check if RTX of SUBREG_PROMOTED_VAR_P() is promoted
   for SIGNED type.  */
#define SUBREG_PROMOTED_SIGNED_P(RTX)	\
  (RTL_FLAG_CHECK1 ("SUBREG_PROMOTED_SIGNED_P", (RTX), SUBREG)->unchanging)

/* Predicate to check if RTX of SUBREG_PROMOTED_VAR_P() is promoted
   for UNSIGNED type.  */
#define SUBREG_PROMOTED_UNSIGNED_P(RTX)	\
  (RTL_FLAG_CHECK1 ("SUBREG_PROMOTED_UNSIGNED_P", (RTX), SUBREG)->volatil)

/* Checks if RTX of SUBREG_PROMOTED_VAR_P() is promoted for given SIGN.  */
#define SUBREG_CHECK_PROMOTED_SIGN(RTX, SIGN)	\
((SIGN) == SRP_POINTER ? SUBREG_PROMOTED_GET (RTX) == SRP_POINTER	\
 : (SIGN) == SRP_SIGNED ? SUBREG_PROMOTED_SIGNED_P (RTX)		\
 : SUBREG_PROMOTED_UNSIGNED_P (RTX))

/* True if the REG is the static chain register for some CALL_INSN.  */
#define STATIC_CHAIN_REG_P(RTX)	\
  (RTL_FLAG_CHECK1 ("STATIC_CHAIN_REG_P", (RTX), REG)->jump)

/* True if the subreg was generated by LRA for reload insns.  Such
   subregs are valid only during LRA.  */
#define LRA_SUBREG_P(RTX)	\
  (RTL_FLAG_CHECK1 ("LRA_SUBREG_P", (RTX), SUBREG)->jump)

/* True if call is instrumented by Pointer Bounds Checker.  */
#define CALL_EXPR_WITH_BOUNDS_P(RTX) \
  (RTL_FLAG_CHECK1 ("CALL_EXPR_WITH_BOUNDS_P", (RTX), CALL)->jump)

/* Access various components of an ASM_OPERANDS rtx.  */

#define ASM_OPERANDS_TEMPLATE(RTX) XCSTR (RTX, 0, ASM_OPERANDS)
#define ASM_OPERANDS_OUTPUT_CONSTRAINT(RTX) XCSTR (RTX, 1, ASM_OPERANDS)
#define ASM_OPERANDS_OUTPUT_IDX(RTX) XCINT (RTX, 2, ASM_OPERANDS)
#define ASM_OPERANDS_INPUT_VEC(RTX) XCVEC (RTX, 3, ASM_OPERANDS)
#define ASM_OPERANDS_INPUT_CONSTRAINT_VEC(RTX) XCVEC (RTX, 4, ASM_OPERANDS)
#define ASM_OPERANDS_INPUT(RTX, N) XCVECEXP (RTX, 3, N, ASM_OPERANDS)
#define ASM_OPERANDS_INPUT_LENGTH(RTX) XCVECLEN (RTX, 3, ASM_OPERANDS)
#define ASM_OPERANDS_INPUT_CONSTRAINT_EXP(RTX, N) \
  XCVECEXP (RTX, 4, N, ASM_OPERANDS)
#define ASM_OPERANDS_INPUT_CONSTRAINT(RTX, N) \
  XSTR (XCVECEXP (RTX, 4, N, ASM_OPERANDS), 0)
#define ASM_OPERANDS_INPUT_MODE(RTX, N)  \
  GET_MODE (XCVECEXP (RTX, 4, N, ASM_OPERANDS))
#define ASM_OPERANDS_LABEL_VEC(RTX) XCVEC (RTX, 5, ASM_OPERANDS)
#define ASM_OPERANDS_LABEL_LENGTH(RTX) XCVECLEN (RTX, 5, ASM_OPERANDS)
#define ASM_OPERANDS_LABEL(RTX, N) XCVECEXP (RTX, 5, N, ASM_OPERANDS)
#define ASM_OPERANDS_SOURCE_LOCATION(RTX) XCUINT (RTX, 6, ASM_OPERANDS)
#define ASM_INPUT_SOURCE_LOCATION(RTX) XCUINT (RTX, 1, ASM_INPUT)

/* 1 if RTX is a mem that is statically allocated in read-only memory.  */
#define MEM_READONLY_P(RTX) \
  (RTL_FLAG_CHECK1 ("MEM_READONLY_P", (RTX), MEM)->unchanging)

/* 1 if RTX is a mem and we should keep the alias set for this mem
   unchanged when we access a component.  Set to 1, or example, when we
   are already in a non-addressable component of an aggregate.  */
#define MEM_KEEP_ALIAS_SET_P(RTX)					\
  (RTL_FLAG_CHECK1 ("MEM_KEEP_ALIAS_SET_P", (RTX), MEM)->jump)

/* 1 if RTX is a mem or asm_operand for a volatile reference.  */
#define MEM_VOLATILE_P(RTX)						\
  (RTL_FLAG_CHECK3 ("MEM_VOLATILE_P", (RTX), MEM, ASM_OPERANDS,		\
		    ASM_INPUT)->volatil)

/* 1 if RTX is a mem that cannot trap.  */
#define MEM_NOTRAP_P(RTX) \
  (RTL_FLAG_CHECK1 ("MEM_NOTRAP_P", (RTX), MEM)->call)

/* The memory attribute block.  We provide access macros for each value
   in the block and provide defaults if none specified.  */
#define MEM_ATTRS(RTX) X0MEMATTR (RTX, 1)

/* The register attribute block.  We provide access macros for each value
   in the block and provide defaults if none specified.  */
#define REG_ATTRS(RTX) (REG_CHECK (RTX)->attrs)

#ifndef GENERATOR_FILE
/* For a MEM rtx, the alias set.  If 0, this MEM is not in any alias
   set, and may alias anything.  Otherwise, the MEM can only alias
   MEMs in a conflicting alias set.  This value is set in a
   language-dependent manner in the front-end, and should not be
   altered in the back-end.  These set numbers are tested with
   alias_sets_conflict_p.  */
#define MEM_ALIAS_SET(RTX) (get_mem_attrs (RTX)->alias)

/* For a MEM rtx, the decl it is known to refer to, if it is known to
   refer to part of a DECL.  It may also be a COMPONENT_REF.  */
#define MEM_EXPR(RTX) (get_mem_attrs (RTX)->expr)

/* For a MEM rtx, true if its MEM_OFFSET is known.  */
#define MEM_OFFSET_KNOWN_P(RTX) (get_mem_attrs (RTX)->offset_known_p)

/* For a MEM rtx, the offset from the start of MEM_EXPR.  */
#define MEM_OFFSET(RTX) (get_mem_attrs (RTX)->offset)

/* For a MEM rtx, the address space.  */
#define MEM_ADDR_SPACE(RTX) (get_mem_attrs (RTX)->addrspace)

/* For a MEM rtx, true if its MEM_SIZE is known.  */
#define MEM_SIZE_KNOWN_P(RTX) (get_mem_attrs (RTX)->size_known_p)

/* For a MEM rtx, the size in bytes of the MEM.  */
#define MEM_SIZE(RTX) (get_mem_attrs (RTX)->size)

/* For a MEM rtx, the alignment in bits.  We can use the alignment of the
   mode as a default when STRICT_ALIGNMENT, but not if not.  */
#define MEM_ALIGN(RTX) (get_mem_attrs (RTX)->align)
#else
#define MEM_ADDR_SPACE(RTX) ADDR_SPACE_GENERIC
#endif

/* For a REG rtx, the decl it is known to refer to, if it is known to
   refer to part of a DECL.  */
#define REG_EXPR(RTX) (REG_ATTRS (RTX) == 0 ? 0 : REG_ATTRS (RTX)->decl)

/* For a REG rtx, the offset from the start of REG_EXPR, if known, as an
   HOST_WIDE_INT.  */
#define REG_OFFSET(RTX) (REG_ATTRS (RTX) == 0 ? 0 : REG_ATTRS (RTX)->offset)

/* Copy the attributes that apply to memory locations from RHS to LHS.  */
#define MEM_COPY_ATTRIBUTES(LHS, RHS)				\
  (MEM_VOLATILE_P (LHS) = MEM_VOLATILE_P (RHS),			\
   MEM_NOTRAP_P (LHS) = MEM_NOTRAP_P (RHS),			\
   MEM_READONLY_P (LHS) = MEM_READONLY_P (RHS),			\
   MEM_KEEP_ALIAS_SET_P (LHS) = MEM_KEEP_ALIAS_SET_P (RHS),	\
   MEM_POINTER (LHS) = MEM_POINTER (RHS),			\
   MEM_ATTRS (LHS) = MEM_ATTRS (RHS))

/* 1 if RTX is a label_ref for a nonlocal label.  */
/* Likewise in an expr_list for a REG_LABEL_OPERAND or
   REG_LABEL_TARGET note.  */
#define LABEL_REF_NONLOCAL_P(RTX)					\
  (RTL_FLAG_CHECK1 ("LABEL_REF_NONLOCAL_P", (RTX), LABEL_REF)->volatil)

/* 1 if RTX is a code_label that should always be considered to be needed.  */
#define LABEL_PRESERVE_P(RTX)						\
  (RTL_FLAG_CHECK2 ("LABEL_PRESERVE_P", (RTX), CODE_LABEL, NOTE)->in_struct)

/* During sched, 1 if RTX is an insn that must be scheduled together
   with the preceding insn.  */
#define SCHED_GROUP_P(RTX)						\
  (RTL_FLAG_CHECK4 ("SCHED_GROUP_P", (RTX), DEBUG_INSN, INSN,		\
		    JUMP_INSN, CALL_INSN)->in_struct)

/* For a SET rtx, SET_DEST is the place that is set
   and SET_SRC is the value it is set to.  */
#define SET_DEST(RTX) XC2EXP (RTX, 0, SET, CLOBBER)
#define SET_SRC(RTX) XCEXP (RTX, 1, SET)
#define SET_IS_RETURN_P(RTX)						\
  (RTL_FLAG_CHECK1 ("SET_IS_RETURN_P", (RTX), SET)->jump)

/* For a TRAP_IF rtx, TRAP_CONDITION is an expression.  */
#define TRAP_CONDITION(RTX) XCEXP (RTX, 0, TRAP_IF)
#define TRAP_CODE(RTX) XCEXP (RTX, 1, TRAP_IF)

/* For a COND_EXEC rtx, COND_EXEC_TEST is the condition to base
   conditionally executing the code on, COND_EXEC_CODE is the code
   to execute if the condition is true.  */
#define COND_EXEC_TEST(RTX) XCEXP (RTX, 0, COND_EXEC)
#define COND_EXEC_CODE(RTX) XCEXP (RTX, 1, COND_EXEC)

/* 1 if RTX is a symbol_ref that addresses this function's rtl
   constants pool.  */
#define CONSTANT_POOL_ADDRESS_P(RTX)					\
  (RTL_FLAG_CHECK1 ("CONSTANT_POOL_ADDRESS_P", (RTX), SYMBOL_REF)->unchanging)

/* 1 if RTX is a symbol_ref that addresses a value in the file's
   tree constant pool.  This information is private to varasm.c.  */
#define TREE_CONSTANT_POOL_ADDRESS_P(RTX)				\
  (RTL_FLAG_CHECK1 ("TREE_CONSTANT_POOL_ADDRESS_P",			\
		    (RTX), SYMBOL_REF)->frame_related)

/* Used if RTX is a symbol_ref, for machine-specific purposes.  */
#define SYMBOL_REF_FLAG(RTX)						\
  (RTL_FLAG_CHECK1 ("SYMBOL_REF_FLAG", (RTX), SYMBOL_REF)->volatil)

/* 1 if RTX is a symbol_ref that has been the library function in
   emit_library_call.  */
#define SYMBOL_REF_USED(RTX)						\
  (RTL_FLAG_CHECK1 ("SYMBOL_REF_USED", (RTX), SYMBOL_REF)->used)

/* 1 if RTX is a symbol_ref for a weak symbol.  */
#define SYMBOL_REF_WEAK(RTX)						\
  (RTL_FLAG_CHECK1 ("SYMBOL_REF_WEAK", (RTX), SYMBOL_REF)->return_val)

/* A pointer attached to the SYMBOL_REF; either SYMBOL_REF_DECL or
   SYMBOL_REF_CONSTANT.  */
#define SYMBOL_REF_DATA(RTX) X0ANY ((RTX), 1)

/* Set RTX's SYMBOL_REF_DECL to DECL.  RTX must not be a constant
   pool symbol.  */
#define SET_SYMBOL_REF_DECL(RTX, DECL) \
  (gcc_assert (!CONSTANT_POOL_ADDRESS_P (RTX)), X0TREE ((RTX), 1) = (DECL))

/* The tree (decl or constant) associated with the symbol, or null.  */
#define SYMBOL_REF_DECL(RTX) \
  (CONSTANT_POOL_ADDRESS_P (RTX) ? NULL : X0TREE ((RTX), 1))

/* Set RTX's SYMBOL_REF_CONSTANT to C.  RTX must be a constant pool symbol.  */
#define SET_SYMBOL_REF_CONSTANT(RTX, C) \
  (gcc_assert (CONSTANT_POOL_ADDRESS_P (RTX)), X0CONSTANT ((RTX), 1) = (C))

/* The rtx constant pool entry for a symbol, or null.  */
#define SYMBOL_REF_CONSTANT(RTX) \
  (CONSTANT_POOL_ADDRESS_P (RTX) ? X0CONSTANT ((RTX), 1) : NULL)

/* A set of flags on a symbol_ref that are, in some respects, redundant with
   information derivable from the tree decl associated with this symbol.
   Except that we build a *lot* of SYMBOL_REFs that aren't associated with a
   decl.  In some cases this is a bug.  But beyond that, it's nice to cache
   this information to avoid recomputing it.  Finally, this allows space for
   the target to store more than one bit of information, as with
   SYMBOL_REF_FLAG.  */
#define SYMBOL_REF_FLAGS(RTX) \
  (RTL_FLAG_CHECK1 ("SYMBOL_REF_FLAGS", (RTX), SYMBOL_REF) \
   ->u2.symbol_ref_flags)

/* These flags are common enough to be defined for all targets.  They
   are computed by the default version of targetm.encode_section_info.  */

/* Set if this symbol is a function.  */
#define SYMBOL_FLAG_FUNCTION	(1 << 0)
#define SYMBOL_REF_FUNCTION_P(RTX) \
  ((SYMBOL_REF_FLAGS (RTX) & SYMBOL_FLAG_FUNCTION) != 0)
/* Set if targetm.binds_local_p is true.  */
#define SYMBOL_FLAG_LOCAL	(1 << 1)
#define SYMBOL_REF_LOCAL_P(RTX) \
  ((SYMBOL_REF_FLAGS (RTX) & SYMBOL_FLAG_LOCAL) != 0)
/* Set if targetm.in_small_data_p is true.  */
#define SYMBOL_FLAG_SMALL	(1 << 2)
#define SYMBOL_REF_SMALL_P(RTX) \
  ((SYMBOL_REF_FLAGS (RTX) & SYMBOL_FLAG_SMALL) != 0)
/* The three-bit field at [5:3] is true for TLS variables; use
   SYMBOL_REF_TLS_MODEL to extract the field as an enum tls_model.  */
#define SYMBOL_FLAG_TLS_SHIFT	3
#define SYMBOL_REF_TLS_MODEL(RTX) \
  ((enum tls_model) ((SYMBOL_REF_FLAGS (RTX) >> SYMBOL_FLAG_TLS_SHIFT) & 7))
/* Set if this symbol is not defined in this translation unit.  */
#define SYMBOL_FLAG_EXTERNAL	(1 << 6)
#define SYMBOL_REF_EXTERNAL_P(RTX) \
  ((SYMBOL_REF_FLAGS (RTX) & SYMBOL_FLAG_EXTERNAL) != 0)
/* Set if this symbol has a block_symbol structure associated with it.  */
#define SYMBOL_FLAG_HAS_BLOCK_INFO (1 << 7)
#define SYMBOL_REF_HAS_BLOCK_INFO_P(RTX) \
  ((SYMBOL_REF_FLAGS (RTX) & SYMBOL_FLAG_HAS_BLOCK_INFO) != 0)
/* Set if this symbol is a section anchor.  SYMBOL_REF_ANCHOR_P implies
   SYMBOL_REF_HAS_BLOCK_INFO_P.  */
#define SYMBOL_FLAG_ANCHOR	(1 << 8)
#define SYMBOL_REF_ANCHOR_P(RTX) \
  ((SYMBOL_REF_FLAGS (RTX) & SYMBOL_FLAG_ANCHOR) != 0)

/* Subsequent bits are available for the target to use.  */
#define SYMBOL_FLAG_MACH_DEP_SHIFT	9
#define SYMBOL_FLAG_MACH_DEP		(1 << SYMBOL_FLAG_MACH_DEP_SHIFT)

/* If SYMBOL_REF_HAS_BLOCK_INFO_P (RTX), this is the object_block
   structure to which the symbol belongs, or NULL if it has not been
   assigned a block.  */
#define SYMBOL_REF_BLOCK(RTX) (BLOCK_SYMBOL_CHECK (RTX)->block)

/* If SYMBOL_REF_HAS_BLOCK_INFO_P (RTX), this is the offset of RTX from
   the first object in SYMBOL_REF_BLOCK (RTX).  The value is negative if
   RTX has not yet been assigned to a block, or it has not been given an
   offset within that block.  */
#define SYMBOL_REF_BLOCK_OFFSET(RTX) (BLOCK_SYMBOL_CHECK (RTX)->offset)

/* True if RTX is flagged to be a scheduling barrier.  */
#define PREFETCH_SCHEDULE_BARRIER_P(RTX)					\
  (RTL_FLAG_CHECK1 ("PREFETCH_SCHEDULE_BARRIER_P", (RTX), PREFETCH)->volatil)

/* Indicate whether the machine has any sort of auto increment addressing.
   If not, we can avoid checking for REG_INC notes.  */

#if (defined (HAVE_PRE_INCREMENT) || defined (HAVE_PRE_DECREMENT) \
     || defined (HAVE_POST_INCREMENT) || defined (HAVE_POST_DECREMENT) \
     || defined (HAVE_PRE_MODIFY_DISP) || defined (HAVE_POST_MODIFY_DISP) \
     || defined (HAVE_PRE_MODIFY_REG) || defined (HAVE_POST_MODIFY_REG))
#define AUTO_INC_DEC 1
#else
#define AUTO_INC_DEC 0
#endif

/* Define a macro to look for REG_INC notes,
   but save time on machines where they never exist.  */

#if AUTO_INC_DEC
#define FIND_REG_INC_NOTE(INSN, REG)			\
  ((REG) != NULL_RTX && REG_P ((REG))			\
   ? find_regno_note ((INSN), REG_INC, REGNO (REG))	\
   : find_reg_note ((INSN), REG_INC, (REG)))
#else
#define FIND_REG_INC_NOTE(INSN, REG) 0
#endif

#ifndef HAVE_PRE_INCREMENT
#define HAVE_PRE_INCREMENT 0
#endif

#ifndef HAVE_PRE_DECREMENT
#define HAVE_PRE_DECREMENT 0
#endif

#ifndef HAVE_POST_INCREMENT
#define HAVE_POST_INCREMENT 0
#endif

#ifndef HAVE_POST_DECREMENT
#define HAVE_POST_DECREMENT 0
#endif

#ifndef HAVE_POST_MODIFY_DISP
#define HAVE_POST_MODIFY_DISP 0
#endif

#ifndef HAVE_POST_MODIFY_REG
#define HAVE_POST_MODIFY_REG 0
#endif

#ifndef HAVE_PRE_MODIFY_DISP
#define HAVE_PRE_MODIFY_DISP 0
#endif

#ifndef HAVE_PRE_MODIFY_REG
#define HAVE_PRE_MODIFY_REG 0
#endif


/* Some architectures do not have complete pre/post increment/decrement
   instruction sets, or only move some modes efficiently.  These macros
   allow us to tune autoincrement generation.  */

#ifndef USE_LOAD_POST_INCREMENT
#define USE_LOAD_POST_INCREMENT(MODE)   HAVE_POST_INCREMENT
#endif

#ifndef USE_LOAD_POST_DECREMENT
#define USE_LOAD_POST_DECREMENT(MODE)   HAVE_POST_DECREMENT
#endif

#ifndef USE_LOAD_PRE_INCREMENT
#define USE_LOAD_PRE_INCREMENT(MODE)    HAVE_PRE_INCREMENT
#endif

#ifndef USE_LOAD_PRE_DECREMENT
#define USE_LOAD_PRE_DECREMENT(MODE)    HAVE_PRE_DECREMENT
#endif

#ifndef USE_STORE_POST_INCREMENT
#define USE_STORE_POST_INCREMENT(MODE)  HAVE_POST_INCREMENT
#endif

#ifndef USE_STORE_POST_DECREMENT
#define USE_STORE_POST_DECREMENT(MODE)  HAVE_POST_DECREMENT
#endif

#ifndef USE_STORE_PRE_INCREMENT
#define USE_STORE_PRE_INCREMENT(MODE)   HAVE_PRE_INCREMENT
#endif

#ifndef USE_STORE_PRE_DECREMENT
#define USE_STORE_PRE_DECREMENT(MODE)   HAVE_PRE_DECREMENT
#endif

/* Nonzero when we are generating CONCATs.  */
extern int generating_concat_p;

/* Nonzero when we are expanding trees to RTL.  */
extern int currently_expanding_to_rtl;

/* Generally useful functions.  */

#ifndef GENERATOR_FILE
/* Return the cost of SET X.  SPEED_P is true if optimizing for speed
   rather than size.  */

static inline int
set_rtx_cost (rtx x, bool speed_p)
{
  return rtx_cost (x, VOIDmode, INSN, 4, speed_p);
}

/* Like set_rtx_cost, but return both the speed and size costs in C.  */

static inline void
get_full_set_rtx_cost (rtx x, struct full_rtx_costs *c)
{
  get_full_rtx_cost (x, VOIDmode, INSN, 4, c);
}

/* Return the cost of moving X into a register, relative to the cost
   of a register move.  SPEED_P is true if optimizing for speed rather
   than size.  */

static inline int
set_src_cost (rtx x, machine_mode mode, bool speed_p)
{
  return rtx_cost (x, mode, SET, 1, speed_p);
}

/* Like set_src_cost, but return both the speed and size costs in C.  */

static inline void
get_full_set_src_cost (rtx x, machine_mode mode, struct full_rtx_costs *c)
{
  get_full_rtx_cost (x, mode, SET, 1, c);
}
#endif

/* A convenience macro to validate the arguments of a zero_extract
   expression.  It determines whether SIZE lies inclusively within
   [1, RANGE], POS lies inclusively within between [0, RANGE - 1]
   and the sum lies inclusively within [1, RANGE].  RANGE must be
   >= 1, but SIZE and POS may be negative.  */
#define EXTRACT_ARGS_IN_RANGE(SIZE, POS, RANGE) \
  (IN_RANGE ((POS), 0, (unsigned HOST_WIDE_INT) (RANGE) - 1) \
   && IN_RANGE ((SIZE), 1, (unsigned HOST_WIDE_INT) (RANGE) \
			   - (unsigned HOST_WIDE_INT)(POS)))

/* In explow.c */
extern HOST_WIDE_INT trunc_int_for_mode	(HOST_WIDE_INT, machine_mode);
extern poly_int64 trunc_int_for_mode (poly_int64, machine_mode);
extern rtx plus_constant (machine_mode, rtx, poly_int64, bool = false);
extern HOST_WIDE_INT get_stack_check_protect (void);

/* In rtl.c */
extern rtx rtx_alloc (RTX_CODE CXX_MEM_STAT_INFO);
extern rtx rtx_alloc_stat_v (RTX_CODE MEM_STAT_DECL, int);
#define rtx_alloc_v(c, SZ) rtx_alloc_stat_v (c MEM_STAT_INFO, SZ)
#define const_wide_int_alloc(NWORDS)				\
  rtx_alloc_v (CONST_WIDE_INT,					\
	       (sizeof (struct hwivec_def)			\
		+ ((NWORDS)-1) * sizeof (HOST_WIDE_INT)))	\

extern rtvec rtvec_alloc (int);
extern rtvec shallow_copy_rtvec (rtvec);
extern bool shared_const_p (const_rtx);
extern rtx copy_rtx (rtx);
extern enum rtx_code classify_insn (rtx);
extern void dump_rtx_statistics (void);

/* In emit-rtl.c */
extern rtx copy_rtx_if_shared (rtx);

/* In rtl.c */
extern unsigned int rtx_size (const_rtx);
extern rtx shallow_copy_rtx (const_rtx CXX_MEM_STAT_INFO);
extern int rtx_equal_p (const_rtx, const_rtx);
extern bool rtvec_all_equal_p (const_rtvec);

/* Return true if X is a vector constant with a duplicated element value.  */

inline bool
const_vec_duplicate_p (const_rtx x)
{
  return (GET_CODE (x) == CONST_VECTOR
	  && CONST_VECTOR_NPATTERNS (x) == 1
	  && CONST_VECTOR_DUPLICATE_P (x));
}

/* Return true if X is a vector constant with a duplicated element value.
   Store the duplicated element in *ELT if so.  */

template <typename T>
inline bool
const_vec_duplicate_p (T x, T *elt)
{
  if (const_vec_duplicate_p (x))
    {
      *elt = CONST_VECTOR_ENCODED_ELT (x, 0);
      return true;
    }
  return false;
}

/* Return true if X is a vector with a duplicated element value, either
   constant or nonconstant.  Store the duplicated element in *ELT if so.  */

template <typename T>
inline bool
vec_duplicate_p (T x, T *elt)
{
  if (GET_CODE (x) == VEC_DUPLICATE
      && !VECTOR_MODE_P (GET_MODE (XEXP (x, 0))))
    {
      *elt = XEXP (x, 0);
      return true;
    }
  return const_vec_duplicate_p (x, elt);
}

/* If X is a vector constant with a duplicated element value, return that
   element value, otherwise return X.  */

template <typename T>
inline T
unwrap_const_vec_duplicate (T x)
{
  if (const_vec_duplicate_p (x))
    x = CONST_VECTOR_ELT (x, 0);
  return x;
}

/* In emit-rtl.c.  */
extern wide_int const_vector_int_elt (const_rtx, unsigned int);
extern rtx const_vector_elt (const_rtx, unsigned int);
extern bool const_vec_series_p_1 (const_rtx, rtx *, rtx *);

/* Return true if X is an integer constant vector that contains a linear
   series of the form:

   { B, B + S, B + 2 * S, B + 3 * S, ... }

   for a nonzero S.  Store B and S in *BASE_OUT and *STEP_OUT on sucess.  */

inline bool
const_vec_series_p (const_rtx x, rtx *base_out, rtx *step_out)
{
  if (GET_CODE (x) == CONST_VECTOR
      && CONST_VECTOR_NPATTERNS (x) == 1
      && !CONST_VECTOR_DUPLICATE_P (x))
    return const_vec_series_p_1 (x, base_out, step_out);
  return false;
}

/* Return true if X is a vector that contains a linear series of the
   form:

   { B, B + S, B + 2 * S, B + 3 * S, ... }

   where B and S are constant or nonconstant.  Store B and S in
   *BASE_OUT and *STEP_OUT on sucess.  */

inline bool
vec_series_p (const_rtx x, rtx *base_out, rtx *step_out)
{
  if (GET_CODE (x) == VEC_SERIES)
    {
      *base_out = XEXP (x, 0);
      *step_out = XEXP (x, 1);
      return true;
    }
  return const_vec_series_p (x, base_out, step_out);
}

/* Return the unpromoted (outer) mode of SUBREG_PROMOTED_VAR_P subreg X.  */

inline scalar_int_mode
subreg_unpromoted_mode (rtx x)
{
  gcc_checking_assert (SUBREG_PROMOTED_VAR_P (x));
  return as_a <scalar_int_mode> (GET_MODE (x));
}

/* Return the promoted (inner) mode of SUBREG_PROMOTED_VAR_P subreg X.  */

inline scalar_int_mode
subreg_promoted_mode (rtx x)
{
  gcc_checking_assert (SUBREG_PROMOTED_VAR_P (x));
  return as_a <scalar_int_mode> (GET_MODE (SUBREG_REG (x)));
}

/* In emit-rtl.c */
extern rtvec gen_rtvec_v (int, rtx *);
extern rtvec gen_rtvec_v (int, rtx_insn **);
extern rtx gen_reg_rtx (machine_mode);
extern rtx gen_rtx_REG_offset (rtx, machine_mode, unsigned int, poly_int64);
extern rtx gen_reg_rtx_offset (rtx, machine_mode, int);
extern rtx gen_reg_rtx_and_attrs (rtx);
extern rtx_code_label *gen_label_rtx (void);
extern rtx gen_lowpart_common (machine_mode, rtx);

/* In cse.c */
extern rtx gen_lowpart_if_possible (machine_mode, rtx);

/* In emit-rtl.c */
extern rtx gen_highpart (machine_mode, rtx);
extern rtx gen_highpart_mode (machine_mode, machine_mode, rtx);
extern rtx operand_subword (rtx, poly_uint64, int, machine_mode);

/* In emit-rtl.c */
extern rtx operand_subword_force (rtx, poly_uint64, machine_mode);
extern int subreg_lowpart_p (const_rtx);
extern poly_uint64 subreg_size_lowpart_offset (poly_uint64, poly_uint64);

/* Return true if a subreg of mode OUTERMODE would only access part of
   an inner register with mode INNERMODE.  The other bits of the inner
   register would then be "don't care" on read.  The behavior for writes
   depends on REGMODE_NATURAL_SIZE; bits in the same REGMODE_NATURAL_SIZE-d
   chunk would be clobbered but other bits would be preserved.  */

inline bool
partial_subreg_p (machine_mode outermode, machine_mode innermode)
{
  /* Modes involved in a subreg must be ordered.  In particular, we must
     always know at compile time whether the subreg is paradoxical.  */
  poly_int64 outer_prec = GET_MODE_PRECISION (outermode);
  poly_int64 inner_prec = GET_MODE_PRECISION (innermode);
  gcc_checking_assert (ordered_p (outer_prec, inner_prec));
  return maybe_lt (outer_prec, inner_prec);
}

/* Likewise return true if X is a subreg that is smaller than the inner
   register.  Use read_modify_subreg_p to test whether writing to such
   a subreg preserves any part of the inner register.  */

inline bool
partial_subreg_p (const_rtx x)
{
  if (GET_CODE (x) != SUBREG)
    return false;
  return partial_subreg_p (GET_MODE (x), GET_MODE (SUBREG_REG (x)));
}

/* Return true if a subreg with the given outer and inner modes is
   paradoxical.  */

inline bool
paradoxical_subreg_p (machine_mode outermode, machine_mode innermode)
{
  /* Modes involved in a subreg must be ordered.  In particular, we must
     always know at compile time whether the subreg is paradoxical.  */
  poly_int64 outer_prec = GET_MODE_PRECISION (outermode);
  poly_int64 inner_prec = GET_MODE_PRECISION (innermode);
  gcc_checking_assert (ordered_p (outer_prec, inner_prec));
  return maybe_gt (outer_prec, inner_prec);
}

/* Return true if X is a paradoxical subreg, false otherwise.  */

inline bool
paradoxical_subreg_p (const_rtx x)
{
  if (GET_CODE (x) != SUBREG)
    return false;
  return paradoxical_subreg_p (GET_MODE (x), GET_MODE (SUBREG_REG (x)));
}

/* Return the SUBREG_BYTE for an OUTERMODE lowpart of an INNERMODE value.  */

inline poly_uint64
subreg_lowpart_offset (machine_mode outermode, machine_mode innermode)
{
  return subreg_size_lowpart_offset (GET_MODE_SIZE (outermode),
				     GET_MODE_SIZE (innermode));
}

/* Given that a subreg has outer mode OUTERMODE and inner mode INNERMODE,
   return the smaller of the two modes if they are different sizes,
   otherwise return the outer mode.  */

inline machine_mode
narrower_subreg_mode (machine_mode outermode, machine_mode innermode)
{
  return paradoxical_subreg_p (outermode, innermode) ? innermode : outermode;
}

/* Given that a subreg has outer mode OUTERMODE and inner mode INNERMODE,
   return the mode that is big enough to hold both the outer and inner
   values.  Prefer the outer mode in the event of a tie.  */

inline machine_mode
wider_subreg_mode (machine_mode outermode, machine_mode innermode)
{
  return partial_subreg_p (outermode, innermode) ? innermode : outermode;
}

/* Likewise for subreg X.  */

inline machine_mode
wider_subreg_mode (const_rtx x)
{
  return wider_subreg_mode (GET_MODE (x), GET_MODE (SUBREG_REG (x)));
}

extern poly_uint64 subreg_size_highpart_offset (poly_uint64, poly_uint64);

/* Return the SUBREG_BYTE for an OUTERMODE highpart of an INNERMODE value.  */

inline poly_uint64
subreg_highpart_offset (machine_mode outermode, machine_mode innermode)
{
  return subreg_size_highpart_offset (GET_MODE_SIZE (outermode),
				      GET_MODE_SIZE (innermode));
}

extern poly_int64 byte_lowpart_offset (machine_mode, machine_mode);
extern poly_int64 subreg_memory_offset (machine_mode, machine_mode,
					poly_uint64);
extern poly_int64 subreg_memory_offset (const_rtx);
extern rtx make_safe_from (rtx, rtx);
extern rtx convert_memory_address_addr_space_1 (scalar_int_mode, rtx,
						addr_space_t, bool, bool);
extern rtx convert_memory_address_addr_space (scalar_int_mode, rtx,
					      addr_space_t);
#define convert_memory_address(to_mode,x) \
	convert_memory_address_addr_space ((to_mode), (x), ADDR_SPACE_GENERIC)
extern const char *get_insn_name (int);
extern rtx_insn *get_last_insn_anywhere (void);
extern rtx_insn *get_first_nonnote_insn (void);
extern rtx_insn *get_last_nonnote_insn (void);
extern void start_sequence (void);
extern void push_to_sequence (rtx_insn *);
extern void push_to_sequence2 (rtx_insn *, rtx_insn *);
extern void end_sequence (void);
#if TARGET_SUPPORTS_WIDE_INT == 0
extern double_int rtx_to_double_int (const_rtx);
#endif
extern void cwi_output_hex (FILE *, const_rtx);
#if TARGET_SUPPORTS_WIDE_INT == 0
extern rtx immed_double_const (HOST_WIDE_INT, HOST_WIDE_INT,
			       machine_mode);
#endif
extern rtx immed_wide_int_const (const poly_wide_int_ref &, machine_mode);

/* In varasm.c  */
extern rtx force_const_mem (machine_mode, rtx);

/* In varasm.c  */

struct function;
extern rtx get_pool_constant (const_rtx);
extern rtx get_pool_constant_mark (rtx, bool *);
extern fixed_size_mode get_pool_mode (const_rtx);
extern rtx simplify_subtraction (rtx);
extern void decide_function_section (tree);

/* In emit-rtl.c */
extern rtx_insn *emit_insn_before (rtx, rtx);
extern rtx_insn *emit_insn_before_noloc (rtx, rtx_insn *, basic_block);
extern rtx_insn *emit_insn_before_setloc (rtx, rtx_insn *, int);
extern rtx_jump_insn *emit_jump_insn_before (rtx, rtx);
extern rtx_jump_insn *emit_jump_insn_before_noloc (rtx, rtx_insn *);
extern rtx_jump_insn *emit_jump_insn_before_setloc (rtx, rtx_insn *, int);
extern rtx_insn *emit_call_insn_before (rtx, rtx_insn *);
extern rtx_insn *emit_call_insn_before_noloc (rtx, rtx_insn *);
extern rtx_insn *emit_call_insn_before_setloc (rtx, rtx_insn *, int);
extern rtx_insn *emit_debug_insn_before (rtx, rtx_insn *);
extern rtx_insn *emit_debug_insn_before_noloc (rtx, rtx);
extern rtx_insn *emit_debug_insn_before_setloc (rtx, rtx, int);
extern rtx_barrier *emit_barrier_before (rtx);
extern rtx_code_label *emit_label_before (rtx, rtx_insn *);
extern rtx_note *emit_note_before (enum insn_note, rtx_insn *);
extern rtx_insn *emit_insn_after (rtx, rtx);
extern rtx_insn *emit_insn_after_noloc (rtx, rtx, basic_block);
extern rtx_insn *emit_insn_after_setloc (rtx, rtx, int);
extern rtx_jump_insn *emit_jump_insn_after (rtx, rtx);
extern rtx_jump_insn *emit_jump_insn_after_noloc (rtx, rtx);
extern rtx_jump_insn *emit_jump_insn_after_setloc (rtx, rtx, int);
extern rtx_insn *emit_call_insn_after (rtx, rtx);
extern rtx_insn *emit_call_insn_after_noloc (rtx, rtx);
extern rtx_insn *emit_call_insn_after_setloc (rtx, rtx, int);
extern rtx_insn *emit_debug_insn_after (rtx, rtx);
extern rtx_insn *emit_debug_insn_after_noloc (rtx, rtx);
extern rtx_insn *emit_debug_insn_after_setloc (rtx, rtx, int);
extern rtx_barrier *emit_barrier_after (rtx);
extern rtx_insn *emit_label_after (rtx, rtx_insn *);
extern rtx_note *emit_note_after (enum insn_note, rtx_insn *);
extern rtx_insn *emit_insn (rtx);
extern rtx_insn *emit_debug_insn (rtx);
extern rtx_insn *emit_jump_insn (rtx);
extern rtx_insn *emit_call_insn (rtx);
extern rtx_code_label *emit_label (rtx);
extern rtx_jump_table_data *emit_jump_table_data (rtx);
extern rtx_barrier *emit_barrier (void);
extern rtx_note *emit_note (enum insn_note);
extern rtx_note *emit_note_copy (rtx_note *);
extern rtx_insn *gen_clobber (rtx);
extern rtx_insn *emit_clobber (rtx);
extern rtx_insn *gen_use (rtx);
extern rtx_insn *emit_use (rtx);
extern rtx_insn *make_insn_raw (rtx);
extern void add_function_usage_to (rtx, rtx);
extern rtx_call_insn *last_call_insn (void);
extern rtx_insn *previous_insn (rtx_insn *);
extern rtx_insn *next_insn (rtx_insn *);
extern rtx_insn *prev_nonnote_insn (rtx_insn *);
extern rtx_insn *next_nonnote_insn (rtx_insn *);
extern rtx_insn *prev_nondebug_insn (rtx_insn *);
extern rtx_insn *next_nondebug_insn (rtx_insn *);
extern rtx_insn *prev_nonnote_nondebug_insn (rtx_insn *);
extern rtx_insn *prev_nonnote_nondebug_insn_bb (rtx_insn *);
extern rtx_insn *next_nonnote_nondebug_insn (rtx_insn *);
extern rtx_insn *next_nonnote_nondebug_insn_bb (rtx_insn *);
extern rtx_insn *prev_real_insn (rtx_insn *);
extern rtx_insn *next_real_insn (rtx);
extern rtx_insn *prev_real_nondebug_insn (rtx_insn *);
extern rtx_insn *next_real_nondebug_insn (rtx);
extern rtx_insn *prev_active_insn (rtx_insn *);
extern rtx_insn *next_active_insn (rtx_insn *);
extern int active_insn_p (const rtx_insn *);
extern rtx_insn *next_cc0_user (rtx_insn *);
extern rtx_insn *prev_cc0_setter (rtx_insn *);

/* In emit-rtl.c  */
extern int insn_line (const rtx_insn *);
extern const char * insn_file (const rtx_insn *);
extern tree insn_scope (const rtx_insn *);
extern expanded_location insn_location (const rtx_insn *);
extern location_t prologue_location, epilogue_location;

/* In jump.c */
extern enum rtx_code reverse_condition (enum rtx_code);
extern enum rtx_code reverse_condition_maybe_unordered (enum rtx_code);
extern enum rtx_code swap_condition (enum rtx_code);
extern enum rtx_code unsigned_condition (enum rtx_code);
extern enum rtx_code signed_condition (enum rtx_code);
extern void mark_jump_label (rtx, rtx_insn *, int);

/* In jump.c */
extern rtx_insn *delete_related_insns (rtx);

/* In recog.c  */
extern rtx *find_constant_term_loc (rtx *);

/* In emit-rtl.c  */
extern rtx_insn *try_split (rtx, rtx_insn *, int);

/* In insn-recog.c (generated by genrecog).  */
extern rtx_insn *split_insns (rtx, rtx_insn *);

/* In simplify-rtx.c  */
extern rtx simplify_const_unary_operation (enum rtx_code, machine_mode,
					   rtx, machine_mode);
extern rtx simplify_unary_operation (enum rtx_code, machine_mode, rtx,
				     machine_mode);
extern rtx simplify_const_binary_operation (enum rtx_code, machine_mode,
					    rtx, rtx);
extern rtx simplify_binary_operation (enum rtx_code, machine_mode, rtx,
				      rtx);
extern rtx simplify_ternary_operation (enum rtx_code, machine_mode,
				       machine_mode, rtx, rtx, rtx);
extern rtx simplify_const_relational_operation (enum rtx_code,
						machine_mode, rtx, rtx);
extern rtx simplify_relational_operation (enum rtx_code, machine_mode,
					  machine_mode, rtx, rtx);
extern rtx simplify_gen_binary (enum rtx_code, machine_mode, rtx, rtx);
extern rtx simplify_gen_unary (enum rtx_code, machine_mode, rtx,
			       machine_mode);
extern rtx simplify_gen_ternary (enum rtx_code, machine_mode,
				 machine_mode, rtx, rtx, rtx);
extern rtx simplify_gen_relational (enum rtx_code, machine_mode,
				    machine_mode, rtx, rtx);
extern rtx simplify_subreg (machine_mode, rtx, machine_mode, poly_uint64);
extern rtx simplify_gen_subreg (machine_mode, rtx, machine_mode, poly_uint64);
extern rtx lowpart_subreg (machine_mode, rtx, machine_mode);
extern rtx simplify_replace_fn_rtx (rtx, const_rtx,
				    rtx (*fn) (rtx, const_rtx, void *), void *);
extern rtx simplify_replace_rtx (rtx, const_rtx, rtx);
extern rtx simplify_rtx (const_rtx);
extern rtx avoid_constant_pool_reference (rtx);
extern rtx delegitimize_mem_from_attrs (rtx);
extern bool mode_signbit_p (machine_mode, const_rtx);
extern bool val_signbit_p (machine_mode, unsigned HOST_WIDE_INT);
extern bool val_signbit_known_set_p (machine_mode,
				     unsigned HOST_WIDE_INT);
extern bool val_signbit_known_clear_p (machine_mode,
				       unsigned HOST_WIDE_INT);

/* In reginfo.c  */
extern machine_mode choose_hard_reg_mode (unsigned int, unsigned int,
					       bool);
extern const HARD_REG_SET &simplifiable_subregs (const subreg_shape &);

/* In emit-rtl.c  */
extern rtx set_for_reg_notes (rtx);
extern rtx set_unique_reg_note (rtx, enum reg_note, rtx);
extern rtx set_dst_reg_note (rtx, enum reg_note, rtx, rtx);
extern void set_insn_deleted (rtx);

/* Functions in rtlanal.c */

extern rtx single_set_2 (const rtx_insn *, const_rtx);
extern bool contains_symbol_ref_p (const_rtx);
extern bool contains_symbolic_reference_p (const_rtx);

/* Handle the cheap and common cases inline for performance.  */

inline rtx single_set (const rtx_insn *insn)
{
  if (!INSN_P (insn))
    return NULL_RTX;

  if (GET_CODE (PATTERN (insn)) == SET)
    return PATTERN (insn);

  /* Defer to the more expensive case.  */
  return single_set_2 (insn, PATTERN (insn));
}

extern scalar_int_mode get_address_mode (rtx mem);
extern int rtx_addr_can_trap_p (const_rtx);
extern bool nonzero_address_p (const_rtx);
extern int rtx_unstable_p (const_rtx);
extern bool rtx_varies_p (const_rtx, bool);
extern bool rtx_addr_varies_p (const_rtx, bool);
extern rtx get_call_rtx_from (rtx);
extern HOST_WIDE_INT get_integer_term (const_rtx);
extern rtx get_related_value (const_rtx);
extern bool offset_within_block_p (const_rtx, HOST_WIDE_INT);
extern void split_const (rtx, rtx *, rtx *);
extern rtx strip_offset (rtx, poly_int64_pod *);
extern poly_int64 get_args_size (const_rtx);
extern bool unsigned_reg_p (rtx);
extern int reg_mentioned_p (const_rtx, const_rtx);
extern int count_occurrences (const_rtx, const_rtx, int);
extern int reg_referenced_p (const_rtx, const_rtx);
extern int reg_used_between_p (const_rtx, const rtx_insn *, const rtx_insn *);
extern int reg_set_between_p (const_rtx, const rtx_insn *, const rtx_insn *);
extern int commutative_operand_precedence (rtx);
extern bool swap_commutative_operands_p (rtx, rtx);
extern int modified_between_p (const_rtx, const rtx_insn *, const rtx_insn *);
extern int no_labels_between_p (const rtx_insn *, const rtx_insn *);
extern int modified_in_p (const_rtx, const_rtx);
extern int reg_set_p (const_rtx, const_rtx);
extern int multiple_sets (const_rtx);
extern int set_noop_p (const_rtx);
extern int noop_move_p (const rtx_insn *);
extern bool refers_to_regno_p (unsigned int, unsigned int, const_rtx, rtx *);
extern int reg_overlap_mentioned_p (const_rtx, const_rtx);
extern const_rtx set_of (const_rtx, const_rtx);
extern void record_hard_reg_sets (rtx, const_rtx, void *);
extern void record_hard_reg_uses (rtx *, void *);
extern void find_all_hard_regs (const_rtx, HARD_REG_SET *);
extern void find_all_hard_reg_sets (const rtx_insn *, HARD_REG_SET *, bool);
extern void note_stores (const_rtx, void (*) (rtx, const_rtx, void *), void *);
extern void note_uses (rtx *, void (*) (rtx *, void *), void *);
extern int dead_or_set_p (const rtx_insn *, const_rtx);
extern int dead_or_set_regno_p (const rtx_insn *, unsigned int);
extern rtx find_reg_note (const_rtx, enum reg_note, const_rtx);
extern rtx find_regno_note (const_rtx, enum reg_note, unsigned int);
extern rtx find_reg_equal_equiv_note (const_rtx);
extern rtx find_constant_src (const rtx_insn *);
extern int find_reg_fusage (const_rtx, enum rtx_code, const_rtx);
extern int find_regno_fusage (const_rtx, enum rtx_code, unsigned int);
extern rtx alloc_reg_note (enum reg_note, rtx, rtx);
extern void add_reg_note (rtx, enum reg_note, rtx);
extern void add_int_reg_note (rtx_insn *, enum reg_note, int);
extern void add_args_size_note (rtx_insn *, poly_int64);
extern void add_shallow_copy_of_reg_note (rtx_insn *, rtx);
extern rtx duplicate_reg_note (rtx);
extern void remove_note (rtx_insn *, const_rtx);
extern bool remove_reg_equal_equiv_notes (rtx_insn *);
extern void remove_reg_equal_equiv_notes_for_regno (unsigned int);
extern int side_effects_p (const_rtx);
extern int volatile_refs_p (const_rtx);
extern int volatile_insn_p (const_rtx);
extern int may_trap_p_1 (const_rtx, unsigned);
extern int may_trap_p (const_rtx);
extern int may_trap_or_fault_p (const_rtx);
extern bool can_throw_internal (const_rtx);
extern bool can_throw_external (const_rtx);
extern bool insn_could_throw_p (const_rtx);
extern bool insn_nothrow_p (const_rtx);
extern bool can_nonlocal_goto (const rtx_insn *);
extern void copy_reg_eh_region_note_forward (rtx, rtx_insn *, rtx);
extern void copy_reg_eh_region_note_backward (rtx, rtx_insn *, rtx);
extern int inequality_comparisons_p (const_rtx);
extern rtx replace_rtx (rtx, rtx, rtx, bool = false);
extern void replace_label (rtx *, rtx, rtx, bool);
extern void replace_label_in_insn (rtx_insn *, rtx_insn *, rtx_insn *, bool);
extern bool rtx_referenced_p (const_rtx, const_rtx);
extern bool tablejump_p (const rtx_insn *, rtx_insn **, rtx_jump_table_data **);
extern int computed_jump_p (const rtx_insn *);
extern bool tls_referenced_p (const_rtx);
extern bool contains_mem_rtx_p (rtx x);

/* Overload for refers_to_regno_p for checking a single register.  */
inline bool
refers_to_regno_p (unsigned int regnum, const_rtx x, rtx* loc = NULL)
{
  return refers_to_regno_p (regnum, regnum + 1, x, loc);
}

/* Callback for for_each_inc_dec, to process the autoinc operation OP
   within MEM that sets DEST to SRC + SRCOFF, or SRC if SRCOFF is
   NULL.  The callback is passed the same opaque ARG passed to
   for_each_inc_dec.  Return zero to continue looking for other
   autoinc operations or any other value to interrupt the traversal and
   return that value to the caller of for_each_inc_dec.  */
typedef int (*for_each_inc_dec_fn) (rtx mem, rtx op, rtx dest, rtx src,
				    rtx srcoff, void *arg);
extern int for_each_inc_dec (rtx, for_each_inc_dec_fn, void *arg);

typedef int (*rtx_equal_p_callback_function) (const_rtx *, const_rtx *,
                                              rtx *, rtx *);
extern int rtx_equal_p_cb (const_rtx, const_rtx,
                           rtx_equal_p_callback_function);

typedef int (*hash_rtx_callback_function) (const_rtx, machine_mode, rtx *,
                                           machine_mode *);
extern unsigned hash_rtx_cb (const_rtx, machine_mode, int *, int *,
                             bool, hash_rtx_callback_function);

extern rtx regno_use_in (unsigned int, rtx);
extern int auto_inc_p (const_rtx);
extern bool in_insn_list_p (const rtx_insn_list *, const rtx_insn *);
extern void remove_node_from_expr_list (const_rtx, rtx_expr_list **);
extern void remove_node_from_insn_list (const rtx_insn *, rtx_insn_list **);
extern int loc_mentioned_in_p (rtx *, const_rtx);
extern rtx_insn *find_first_parameter_load (rtx_insn *, rtx_insn *);
extern bool keep_with_call_p (const rtx_insn *);
extern bool label_is_jump_target_p (const_rtx, const rtx_insn *);
extern int pattern_cost (rtx, bool);
extern int insn_cost (rtx_insn *, bool);
extern unsigned seq_cost (const rtx_insn *, bool);

/* Given an insn and condition, return a canonical description of
   the test being made.  */
extern rtx canonicalize_condition (rtx_insn *, rtx, int, rtx_insn **, rtx,
				   int, int);

/* Given a JUMP_INSN, return a canonical description of the test
   being made.  */
extern rtx get_condition (rtx_insn *, rtx_insn **, int, int);

/* Information about a subreg of a hard register.  */
struct subreg_info
{
  /* Offset of first hard register involved in the subreg.  */
  int offset;
  /* Number of hard registers involved in the subreg.  In the case of
     a paradoxical subreg, this is the number of registers that would
     be modified by writing to the subreg; some of them may be don't-care
     when reading from the subreg.  */
  int nregs;
  /* Whether this subreg can be represented as a hard reg with the new
     mode (by adding OFFSET to the original hard register).  */
  bool representable_p;
};

extern void subreg_get_info (unsigned int, machine_mode,
			     poly_uint64, machine_mode,
			     struct subreg_info *);

/* lists.c */

extern void free_EXPR_LIST_list (rtx_expr_list **);
extern void free_INSN_LIST_list (rtx_insn_list **);
extern void free_EXPR_LIST_node (rtx);
extern void free_INSN_LIST_node (rtx);
extern rtx_insn_list *alloc_INSN_LIST (rtx, rtx);
extern rtx_insn_list *copy_INSN_LIST (rtx_insn_list *);
extern rtx_insn_list *concat_INSN_LIST (rtx_insn_list *, rtx_insn_list *);
extern rtx_expr_list *alloc_EXPR_LIST (int, rtx, rtx);
extern void remove_free_INSN_LIST_elem (rtx_insn *, rtx_insn_list **);
extern rtx remove_list_elem (rtx, rtx *);
extern rtx_insn *remove_free_INSN_LIST_node (rtx_insn_list **);
extern rtx remove_free_EXPR_LIST_node (rtx_expr_list **);


/* reginfo.c */

/* Resize reg info.  */
extern bool resize_reg_info (void);
/* Free up register info memory.  */
extern void free_reg_info (void);
extern void init_subregs_of_mode (void);
extern void finish_subregs_of_mode (void);

/* recog.c */
extern rtx extract_asm_operands (rtx);
extern int asm_noperands (const_rtx);
extern const char *decode_asm_operands (rtx, rtx *, rtx **, const char **,
					machine_mode *, location_t *);
extern void get_referenced_operands (const char *, bool *, unsigned int);

extern enum reg_class reg_preferred_class (int);
extern enum reg_class reg_alternate_class (int);
extern enum reg_class reg_allocno_class (int);
extern void setup_reg_classes (int, enum reg_class, enum reg_class,
			       enum reg_class);

extern void split_all_insns (void);
extern unsigned int split_all_insns_noflow (void);

#define MAX_SAVED_CONST_INT 64
extern GTY(()) rtx const_int_rtx[MAX_SAVED_CONST_INT * 2 + 1];

#define const0_rtx	(const_int_rtx[MAX_SAVED_CONST_INT])
#define const1_rtx	(const_int_rtx[MAX_SAVED_CONST_INT+1])
#define const2_rtx	(const_int_rtx[MAX_SAVED_CONST_INT+2])
#define constm1_rtx	(const_int_rtx[MAX_SAVED_CONST_INT-1])
extern GTY(()) rtx const_true_rtx;

extern GTY(()) rtx const_tiny_rtx[4][(int) MAX_MACHINE_MODE];

/* Returns a constant 0 rtx in mode MODE.  Integer modes are treated the
   same as VOIDmode.  */

#define CONST0_RTX(MODE) (const_tiny_rtx[0][(int) (MODE)])

/* Likewise, for the constants 1 and 2 and -1.  */

#define CONST1_RTX(MODE) (const_tiny_rtx[1][(int) (MODE)])
#define CONST2_RTX(MODE) (const_tiny_rtx[2][(int) (MODE)])
#define CONSTM1_RTX(MODE) (const_tiny_rtx[3][(int) (MODE)])

extern GTY(()) rtx pc_rtx;
extern GTY(()) rtx cc0_rtx;
extern GTY(()) rtx ret_rtx;
extern GTY(()) rtx simple_return_rtx;
extern GTY(()) rtx_insn *invalid_insn_rtx;

/* If HARD_FRAME_POINTER_REGNUM is defined, then a special dummy reg
   is used to represent the frame pointer.  This is because the
   hard frame pointer and the automatic variables are separated by an amount
   that cannot be determined until after register allocation.  We can assume
   that in this case ELIMINABLE_REGS will be defined, one action of which
   will be to eliminate FRAME_POINTER_REGNUM into HARD_FRAME_POINTER_REGNUM.  */
#ifndef HARD_FRAME_POINTER_REGNUM
#define HARD_FRAME_POINTER_REGNUM FRAME_POINTER_REGNUM
#endif

#ifndef HARD_FRAME_POINTER_IS_FRAME_POINTER
#define HARD_FRAME_POINTER_IS_FRAME_POINTER \
  (HARD_FRAME_POINTER_REGNUM == FRAME_POINTER_REGNUM)
#endif

#ifndef HARD_FRAME_POINTER_IS_ARG_POINTER
#define HARD_FRAME_POINTER_IS_ARG_POINTER \
  (HARD_FRAME_POINTER_REGNUM == ARG_POINTER_REGNUM)
#endif

/* Index labels for global_rtl.  */
enum global_rtl_index
{
  GR_STACK_POINTER,
  GR_FRAME_POINTER,
/* For register elimination to work properly these hard_frame_pointer_rtx,
   frame_pointer_rtx, and arg_pointer_rtx must be the same if they refer to
   the same register.  */
#if FRAME_POINTER_REGNUM == ARG_POINTER_REGNUM
  GR_ARG_POINTER = GR_FRAME_POINTER,
#endif
#if HARD_FRAME_POINTER_IS_FRAME_POINTER
  GR_HARD_FRAME_POINTER = GR_FRAME_POINTER,
#else
  GR_HARD_FRAME_POINTER,
#endif
#if FRAME_POINTER_REGNUM != ARG_POINTER_REGNUM
#if HARD_FRAME_POINTER_IS_ARG_POINTER
  GR_ARG_POINTER = GR_HARD_FRAME_POINTER,
#else
  GR_ARG_POINTER,
#endif
#endif
  GR_VIRTUAL_INCOMING_ARGS,
  GR_VIRTUAL_STACK_ARGS,
  GR_VIRTUAL_STACK_DYNAMIC,
  GR_VIRTUAL_OUTGOING_ARGS,
  GR_VIRTUAL_CFA,
  GR_VIRTUAL_PREFERRED_STACK_BOUNDARY,

  GR_MAX
};

/* Target-dependent globals.  */
struct GTY(()) target_rtl {
  /* All references to the hard registers in global_rtl_index go through
     these unique rtl objects.  On machines where the frame-pointer and
     arg-pointer are the same register, they use the same unique object.

     After register allocation, other rtl objects which used to be pseudo-regs
     may be clobbered to refer to the frame-pointer register.
     But references that were originally to the frame-pointer can be
     distinguished from the others because they contain frame_pointer_rtx.

     When to use frame_pointer_rtx and hard_frame_pointer_rtx is a little
     tricky: until register elimination has taken place hard_frame_pointer_rtx
     should be used if it is being set, and frame_pointer_rtx otherwise.  After
     register elimination hard_frame_pointer_rtx should always be used.
     On machines where the two registers are same (most) then these are the
     same.  */
  rtx x_global_rtl[GR_MAX];

  /* A unique representation of (REG:Pmode PIC_OFFSET_TABLE_REGNUM).  */
  rtx x_pic_offset_table_rtx;

  /* A unique representation of (REG:Pmode RETURN_ADDRESS_POINTER_REGNUM).
     This is used to implement __builtin_return_address for some machines;
     see for instance the MIPS port.  */
  rtx x_return_address_pointer_rtx;

  /* Commonly used RTL for hard registers.  These objects are not
     necessarily unique, so we allocate them separately from global_rtl.
     They are initialized once per compilation unit, then copied into
     regno_reg_rtx at the beginning of each function.  */
  rtx x_initial_regno_reg_rtx[FIRST_PSEUDO_REGISTER];

  /* A sample (mem:M stack_pointer_rtx) rtx for each mode M.  */
  rtx x_top_of_stack[MAX_MACHINE_MODE];

  /* Static hunks of RTL used by the aliasing code; these are treated
     as persistent to avoid unnecessary RTL allocations.  */
  rtx x_static_reg_base_value[FIRST_PSEUDO_REGISTER];

  /* The default memory attributes for each mode.  */
  struct mem_attrs *x_mode_mem_attrs[(int) MAX_MACHINE_MODE];

  /* Track if RTL has been initialized.  */
  bool target_specific_initialized;
};

extern GTY(()) struct target_rtl default_target_rtl;
#if SWITCHABLE_TARGET
extern struct target_rtl *this_target_rtl;
#else
#define this_target_rtl (&default_target_rtl)
#endif

#define global_rtl				\
  (this_target_rtl->x_global_rtl)
#define pic_offset_table_rtx \
  (this_target_rtl->x_pic_offset_table_rtx)
#define return_address_pointer_rtx \
  (this_target_rtl->x_return_address_pointer_rtx)
#define top_of_stack \
  (this_target_rtl->x_top_of_stack)
#define mode_mem_attrs \
  (this_target_rtl->x_mode_mem_attrs)

/* All references to certain hard regs, except those created
   by allocating pseudo regs into them (when that's possible),
   go through these unique rtx objects.  */
#define stack_pointer_rtx       (global_rtl[GR_STACK_POINTER])
#define frame_pointer_rtx       (global_rtl[GR_FRAME_POINTER])
#define hard_frame_pointer_rtx	(global_rtl[GR_HARD_FRAME_POINTER])
#define arg_pointer_rtx		(global_rtl[GR_ARG_POINTER])

#ifndef GENERATOR_FILE
/* Return the attributes of a MEM rtx.  */
static inline const struct mem_attrs *
get_mem_attrs (const_rtx x)
{
  struct mem_attrs *attrs;

  attrs = MEM_ATTRS (x);
  if (!attrs)
    attrs = mode_mem_attrs[(int) GET_MODE (x)];
  return attrs;
}
#endif

/* Include the RTL generation functions.  */

#ifndef GENERATOR_FILE
#include "genrtl.h"
#undef gen_rtx_ASM_INPUT
#define gen_rtx_ASM_INPUT(MODE, ARG0)				\
  gen_rtx_fmt_si (ASM_INPUT, (MODE), (ARG0), 0)
#define gen_rtx_ASM_INPUT_loc(MODE, ARG0, LOC)			\
  gen_rtx_fmt_si (ASM_INPUT, (MODE), (ARG0), (LOC))
#endif

/* There are some RTL codes that require special attention; the
   generation functions included above do the raw handling.  If you
   add to this list, modify special_rtx in gengenrtl.c as well.  */

extern rtx_expr_list *gen_rtx_EXPR_LIST (machine_mode, rtx, rtx);
extern rtx_insn_list *gen_rtx_INSN_LIST (machine_mode, rtx, rtx);
extern rtx_insn *
gen_rtx_INSN (machine_mode mode, rtx_insn *prev_insn, rtx_insn *next_insn,
	      basic_block bb, rtx pattern, int location, int code,
	      rtx reg_notes);
extern rtx gen_rtx_CONST_INT (machine_mode, HOST_WIDE_INT);
extern rtx gen_rtx_CONST_VECTOR (machine_mode, rtvec);
extern void set_mode_and_regno (rtx, machine_mode, unsigned int);
extern rtx gen_raw_REG (machine_mode, unsigned int);
extern rtx gen_rtx_REG (machine_mode, unsigned int);
extern rtx gen_rtx_SUBREG (machine_mode, rtx, poly_uint64);
extern rtx gen_rtx_MEM (machine_mode, rtx);
extern rtx gen_rtx_VAR_LOCATION (machine_mode, tree, rtx,
				 enum var_init_status);

#ifdef GENERATOR_FILE
#define PUT_MODE(RTX, MODE) PUT_MODE_RAW (RTX, MODE)
#else
static inline void
PUT_MODE (rtx x, machine_mode mode)
{
  if (REG_P (x))
    set_mode_and_regno (x, mode, REGNO (x));
  else
    PUT_MODE_RAW (x, mode);
}
#endif

#define GEN_INT(N)  gen_rtx_CONST_INT (VOIDmode, (N))

/* Virtual registers are used during RTL generation to refer to locations into
   the stack frame when the actual location isn't known until RTL generation
   is complete.  The routine instantiate_virtual_regs replaces these with
   the proper value, which is normally {frame,arg,stack}_pointer_rtx plus
   a constant.  */

#define FIRST_VIRTUAL_REGISTER	(FIRST_PSEUDO_REGISTER)

/* This points to the first word of the incoming arguments passed on the stack,
   either by the caller or by the callee when pretending it was passed by the
   caller.  */

#define virtual_incoming_args_rtx       (global_rtl[GR_VIRTUAL_INCOMING_ARGS])

#define VIRTUAL_INCOMING_ARGS_REGNUM	(FIRST_VIRTUAL_REGISTER)

/* If FRAME_GROWS_DOWNWARD, this points to immediately above the first
   variable on the stack.  Otherwise, it points to the first variable on
   the stack.  */

#define virtual_stack_vars_rtx	        (global_rtl[GR_VIRTUAL_STACK_ARGS])

#define VIRTUAL_STACK_VARS_REGNUM	((FIRST_VIRTUAL_REGISTER) + 1)

/* This points to the location of dynamically-allocated memory on the stack
   immediately after the stack pointer has been adjusted by the amount
   desired.  */

#define virtual_stack_dynamic_rtx	(global_rtl[GR_VIRTUAL_STACK_DYNAMIC])

#define VIRTUAL_STACK_DYNAMIC_REGNUM	((FIRST_VIRTUAL_REGISTER) + 2)

/* This points to the location in the stack at which outgoing arguments should
   be written when the stack is pre-pushed (arguments pushed using push
   insns always use sp).  */

#define virtual_outgoing_args_rtx	(global_rtl[GR_VIRTUAL_OUTGOING_ARGS])

#define VIRTUAL_OUTGOING_ARGS_REGNUM	((FIRST_VIRTUAL_REGISTER) + 3)

/* This points to the Canonical Frame Address of the function.  This
   should correspond to the CFA produced by INCOMING_FRAME_SP_OFFSET,
   but is calculated relative to the arg pointer for simplicity; the
   frame pointer nor stack pointer are necessarily fixed relative to
   the CFA until after reload.  */

#define virtual_cfa_rtx			(global_rtl[GR_VIRTUAL_CFA])

#define VIRTUAL_CFA_REGNUM		((FIRST_VIRTUAL_REGISTER) + 4)

#define LAST_VIRTUAL_POINTER_REGISTER	((FIRST_VIRTUAL_REGISTER) + 4)

/* This is replaced by crtl->preferred_stack_boundary / BITS_PER_UNIT
   when finalized.  */

#define virtual_preferred_stack_boundary_rtx \
	(global_rtl[GR_VIRTUAL_PREFERRED_STACK_BOUNDARY])

#define VIRTUAL_PREFERRED_STACK_BOUNDARY_REGNUM \
					((FIRST_VIRTUAL_REGISTER) + 5)

#define LAST_VIRTUAL_REGISTER		((FIRST_VIRTUAL_REGISTER) + 5)

/* Nonzero if REGNUM is a pointer into the stack frame.  */
#define REGNO_PTR_FRAME_P(REGNUM)		\
  ((REGNUM) == STACK_POINTER_REGNUM		\
   || (REGNUM) == FRAME_POINTER_REGNUM		\
   || (REGNUM) == HARD_FRAME_POINTER_REGNUM	\
   || (REGNUM) == ARG_POINTER_REGNUM		\
   || ((REGNUM) >= FIRST_VIRTUAL_REGISTER	\
       && (REGNUM) <= LAST_VIRTUAL_POINTER_REGISTER))

/* REGNUM never really appearing in the INSN stream.  */
#define INVALID_REGNUM			(~(unsigned int) 0)

/* REGNUM for which no debug information can be generated.  */
#define IGNORED_DWARF_REGNUM            (INVALID_REGNUM - 1)

extern rtx output_constant_def (tree, int);
extern rtx lookup_constant_def (tree);

/* Nonzero after end of reload pass.
   Set to 1 or 0 by reload1.c.  */

extern int reload_completed;

/* Nonzero after thread_prologue_and_epilogue_insns has run.  */
extern int epilogue_completed;

/* Set to 1 while reload_as_needed is operating.
   Required by some machines to handle any generated moves differently.  */

extern int reload_in_progress;

/* Set to 1 while in lra.  */
extern int lra_in_progress;

/* This macro indicates whether you may create a new
   pseudo-register.  */

#define can_create_pseudo_p() (!reload_in_progress && !reload_completed)

#ifdef STACK_REGS
/* Nonzero after end of regstack pass.
   Set to 1 or 0 by reg-stack.c.  */
extern int regstack_completed;
#endif

/* If this is nonzero, we do not bother generating VOLATILE
   around volatile memory references, and we are willing to
   output indirect addresses.  If cse is to follow, we reject
   indirect addresses so a useful potential cse is generated;
   if it is used only once, instruction combination will produce
   the same indirect address eventually.  */
extern int cse_not_expected;

/* Translates rtx code to tree code, for those codes needed by
   real_arithmetic.  The function returns an int because the caller may not
   know what `enum tree_code' means.  */

extern int rtx_to_tree_code (enum rtx_code);

/* In cse.c */
extern int delete_trivially_dead_insns (rtx_insn *, int);
extern int exp_equiv_p (const_rtx, const_rtx, int, bool);
extern unsigned hash_rtx (const_rtx x, machine_mode, int *, int *, bool);

/* In dse.c */
extern bool check_for_inc_dec (rtx_insn *insn);

/* In jump.c */
extern int comparison_dominates_p (enum rtx_code, enum rtx_code);
extern bool jump_to_label_p (const rtx_insn *);
extern int condjump_p (const rtx_insn *);
extern int any_condjump_p (const rtx_insn *);
extern int any_uncondjump_p (const rtx_insn *);
extern rtx pc_set (const rtx_insn *);
extern rtx condjump_label (const rtx_insn *);
extern int simplejump_p (const rtx_insn *);
extern int returnjump_p (const rtx_insn *);
extern int eh_returnjump_p (rtx_insn *);
extern int onlyjump_p (const rtx_insn *);
extern int only_sets_cc0_p (const_rtx);
extern int sets_cc0_p (const_rtx);
extern int invert_jump_1 (rtx_jump_insn *, rtx);
extern int invert_jump (rtx_jump_insn *, rtx, int);
extern int rtx_renumbered_equal_p (const_rtx, const_rtx);
extern int true_regnum (const_rtx);
extern unsigned int reg_or_subregno (const_rtx);
extern int redirect_jump_1 (rtx_insn *, rtx);
extern void redirect_jump_2 (rtx_jump_insn *, rtx, rtx, int, int);
extern int redirect_jump (rtx_jump_insn *, rtx, int);
extern void rebuild_jump_labels (rtx_insn *);
extern void rebuild_jump_labels_chain (rtx_insn *);
extern rtx reversed_comparison (const_rtx, machine_mode);
extern enum rtx_code reversed_comparison_code (const_rtx, const rtx_insn *);
extern enum rtx_code reversed_comparison_code_parts (enum rtx_code, const_rtx,
						     const_rtx, const rtx_insn *);
extern void delete_for_peephole (rtx_insn *, rtx_insn *);
extern int condjump_in_parallel_p (const rtx_insn *);

/* In emit-rtl.c.  */
extern int max_reg_num (void);
extern int max_label_num (void);
extern int get_first_label_num (void);
extern void maybe_set_first_label_num (rtx_code_label *);
extern void delete_insns_since (rtx_insn *);
extern void mark_reg_pointer (rtx, int);
extern void mark_user_reg (rtx);
extern void reset_used_flags (rtx);
extern void set_used_flags (rtx);
extern void reorder_insns (rtx_insn *, rtx_insn *, rtx_insn *);
extern void reorder_insns_nobb (rtx_insn *, rtx_insn *, rtx_insn *);
extern int get_max_insn_count (void);
extern int in_sequence_p (void);
extern void init_emit (void);
extern void init_emit_regs (void);
extern void init_derived_machine_modes (void);
extern void init_emit_once (void);
extern void push_topmost_sequence (void);
extern void pop_topmost_sequence (void);
extern void set_new_first_and_last_insn (rtx_insn *, rtx_insn *);
extern unsigned int unshare_all_rtl (void);
extern void unshare_all_rtl_again (rtx_insn *);
extern void unshare_all_rtl_in_chain (rtx_insn *);
extern void verify_rtl_sharing (void);
extern void add_insn (rtx_insn *);
extern void add_insn_before (rtx, rtx, basic_block);
extern void add_insn_after (rtx, rtx, basic_block);
extern void remove_insn (rtx);
extern rtx_insn *emit (rtx, bool = true);
extern void emit_insn_at_entry (rtx);
extern rtx gen_lowpart_SUBREG (machine_mode, rtx);
extern rtx gen_const_mem (machine_mode, rtx);
extern rtx gen_frame_mem (machine_mode, rtx);
extern rtx gen_tmp_stack_mem (machine_mode, rtx);
extern bool validate_subreg (machine_mode, machine_mode,
			     const_rtx, poly_uint64);

/* In combine.c  */
extern unsigned int extended_count (const_rtx, machine_mode, int);
extern rtx remove_death (unsigned int, rtx_insn *);
extern void dump_combine_stats (FILE *);
extern void dump_combine_total_stats (FILE *);
extern rtx make_compound_operation (rtx, enum rtx_code);

/* In sched-rgn.c.  */
extern void schedule_insns (void);

/* In sched-ebb.c.  */
extern void schedule_ebbs (void);

/* In sel-sched-dump.c.  */
extern void sel_sched_fix_param (const char *param, const char *val);

/* In print-rtl.c */
extern const char *print_rtx_head;
extern void debug (const rtx_def &ref);
extern void debug (const rtx_def *ptr);
extern void debug_rtx (const_rtx);
extern void debug_rtx_list (const rtx_insn *, int);
extern void debug_rtx_range (const rtx_insn *, const rtx_insn *);
extern const rtx_insn *debug_rtx_find (const rtx_insn *, int);
extern void print_mem_expr (FILE *, const_tree);
extern void print_rtl (FILE *, const_rtx);
extern void print_simple_rtl (FILE *, const_rtx);
extern int print_rtl_single (FILE *, const_rtx);
extern int print_rtl_single_with_indent (FILE *, const_rtx, int);
extern void print_inline_rtx (FILE *, const_rtx, int);

/* In stmt.c */
extern void expand_null_return (void);
extern void expand_naked_return (void);
extern void emit_jump (rtx);

/* In expr.c */
extern rtx move_by_pieces (rtx, rtx, unsigned HOST_WIDE_INT,
			   unsigned int, int);
extern poly_int64 find_args_size_adjust (rtx_insn *);
extern poly_int64 fixup_args_size_notes (rtx_insn *, rtx_insn *, poly_int64);

/* In expmed.c */
extern void init_expmed (void);
extern void expand_inc (rtx, rtx);
extern void expand_dec (rtx, rtx);

/* In lower-subreg.c */
extern void init_lower_subreg (void);

/* In gcse.c */
extern bool can_copy_p (machine_mode);
extern bool can_assign_to_reg_without_clobbers_p (rtx, machine_mode);
extern rtx_insn *prepare_copy_insn (rtx, rtx);

/* In cprop.c */
extern rtx fis_get_condition (rtx_insn *);

/* In ira.c */
extern HARD_REG_SET eliminable_regset;
extern void mark_elimination (int, int);

/* In reginfo.c */
extern int reg_classes_intersect_p (reg_class_t, reg_class_t);
extern int reg_class_subset_p (reg_class_t, reg_class_t);
extern void globalize_reg (tree, int);
extern void init_reg_modes_target (void);
extern void init_regs (void);
extern void reinit_regs (void);
extern void init_fake_stack_mems (void);
extern void save_register_info (void);
extern void init_reg_sets (void);
extern void regclass (rtx, int);
extern void reg_scan (rtx_insn *, unsigned int);
extern void fix_register (const char *, int, int);
extern const HARD_REG_SET *valid_mode_changes_for_regno (unsigned int);

/* In reload1.c */
extern int function_invariant_p (const_rtx);

/* In calls.c */
enum libcall_type
{
  LCT_NORMAL = 0,
  LCT_CONST = 1,
  LCT_PURE = 2,
  LCT_NORETURN = 3,
  LCT_THROW = 4,
  LCT_RETURNS_TWICE = 5
};

extern rtx emit_library_call_value_1 (int, rtx, rtx, enum libcall_type,
				      machine_mode, int, rtx_mode_t *);

/* Output a library call and discard the returned value.  FUN is the
   address of the function, as a SYMBOL_REF rtx, and OUTMODE is the mode
   of the (discarded) return value.  FN_TYPE is LCT_NORMAL for `normal'
   calls, LCT_CONST for `const' calls, LCT_PURE for `pure' calls, or
   another LCT_ value for other types of library calls.

   There are different overloads of this function for different numbers
   of arguments.  In each case the argument value is followed by its mode.  */

inline void
emit_library_call (rtx fun, libcall_type fn_type, machine_mode outmode)
{
  emit_library_call_value_1 (0, fun, NULL_RTX, fn_type, outmode, 0, NULL);
}

inline void
emit_library_call (rtx fun, libcall_type fn_type, machine_mode outmode,
		   rtx arg1, machine_mode arg1_mode)
{
  rtx_mode_t args[] = { rtx_mode_t (arg1, arg1_mode) };
  emit_library_call_value_1 (0, fun, NULL_RTX, fn_type, outmode, 1, args);
}

inline void
emit_library_call (rtx fun, libcall_type fn_type, machine_mode outmode,
		   rtx arg1, machine_mode arg1_mode,
		   rtx arg2, machine_mode arg2_mode)
{
  rtx_mode_t args[] = {
    rtx_mode_t (arg1, arg1_mode),
    rtx_mode_t (arg2, arg2_mode)
  };
  emit_library_call_value_1 (0, fun, NULL_RTX, fn_type, outmode, 2, args);
}

inline void
emit_library_call (rtx fun, libcall_type fn_type, machine_mode outmode,
		   rtx arg1, machine_mode arg1_mode,
		   rtx arg2, machine_mode arg2_mode,
		   rtx arg3, machine_mode arg3_mode)
{
  rtx_mode_t args[] = {
    rtx_mode_t (arg1, arg1_mode),
    rtx_mode_t (arg2, arg2_mode),
    rtx_mode_t (arg3, arg3_mode)
  };
  emit_library_call_value_1 (0, fun, NULL_RTX, fn_type, outmode, 3, args);
}

inline void
emit_library_call (rtx fun, libcall_type fn_type, machine_mode outmode,
		   rtx arg1, machine_mode arg1_mode,
		   rtx arg2, machine_mode arg2_mode,
		   rtx arg3, machine_mode arg3_mode,
		   rtx arg4, machine_mode arg4_mode)
{
  rtx_mode_t args[] = {
    rtx_mode_t (arg1, arg1_mode),
    rtx_mode_t (arg2, arg2_mode),
    rtx_mode_t (arg3, arg3_mode),
    rtx_mode_t (arg4, arg4_mode)
  };
  emit_library_call_value_1 (0, fun, NULL_RTX, fn_type, outmode, 4, args);
}

/* Like emit_library_call, but return the value produced by the call.
   Use VALUE to store the result if it is nonnull, otherwise pick a
   convenient location.  */

inline rtx
emit_library_call_value (rtx fun, rtx value, libcall_type fn_type,
			 machine_mode outmode)
{
  return emit_library_call_value_1 (1, fun, value, fn_type, outmode, 0, NULL);
}

inline rtx
emit_library_call_value (rtx fun, rtx value, libcall_type fn_type,
			 machine_mode outmode,
			 rtx arg1, machine_mode arg1_mode)
{
  rtx_mode_t args[] = { rtx_mode_t (arg1, arg1_mode) };
  return emit_library_call_value_1 (1, fun, value, fn_type, outmode, 1, args);
}

inline rtx
emit_library_call_value (rtx fun, rtx value, libcall_type fn_type,
			 machine_mode outmode,
			 rtx arg1, machine_mode arg1_mode,
			 rtx arg2, machine_mode arg2_mode)
{
  rtx_mode_t args[] = {
    rtx_mode_t (arg1, arg1_mode),
    rtx_mode_t (arg2, arg2_mode)
  };
  return emit_library_call_value_1 (1, fun, value, fn_type, outmode, 2, args);
}

inline rtx
emit_library_call_value (rtx fun, rtx value, libcall_type fn_type,
			 machine_mode outmode,
			 rtx arg1, machine_mode arg1_mode,
			 rtx arg2, machine_mode arg2_mode,
			 rtx arg3, machine_mode arg3_mode)
{
  rtx_mode_t args[] = {
    rtx_mode_t (arg1, arg1_mode),
    rtx_mode_t (arg2, arg2_mode),
    rtx_mode_t (arg3, arg3_mode)
  };
  return emit_library_call_value_1 (1, fun, value, fn_type, outmode, 3, args);
}

inline rtx
emit_library_call_value (rtx fun, rtx value, libcall_type fn_type,
			 machine_mode outmode,
			 rtx arg1, machine_mode arg1_mode,
			 rtx arg2, machine_mode arg2_mode,
			 rtx arg3, machine_mode arg3_mode,
			 rtx arg4, machine_mode arg4_mode)
{
  rtx_mode_t args[] = {
    rtx_mode_t (arg1, arg1_mode),
    rtx_mode_t (arg2, arg2_mode),
    rtx_mode_t (arg3, arg3_mode),
    rtx_mode_t (arg4, arg4_mode)
  };
  return emit_library_call_value_1 (1, fun, value, fn_type, outmode, 4, args);
}

/* In varasm.c */
extern void init_varasm_once (void);

extern rtx make_debug_expr_from_rtl (const_rtx);

/* In read-rtl.c */
#ifdef GENERATOR_FILE
extern bool read_rtx (const char *, vec<rtx> *);
#endif

/* In alias.c */
extern rtx canon_rtx (rtx);
extern int true_dependence (const_rtx, machine_mode, const_rtx);
extern rtx get_addr (rtx);
extern int canon_true_dependence (const_rtx, machine_mode, rtx,
				  const_rtx, rtx);
extern int read_dependence (const_rtx, const_rtx);
extern int anti_dependence (const_rtx, const_rtx);
extern int canon_anti_dependence (const_rtx, bool,
				  const_rtx, machine_mode, rtx);
extern int output_dependence (const_rtx, const_rtx);
extern int canon_output_dependence (const_rtx, bool,
				    const_rtx, machine_mode, rtx);
extern int may_alias_p (const_rtx, const_rtx);
extern void init_alias_target (void);
extern void init_alias_analysis (void);
extern void end_alias_analysis (void);
extern void vt_equate_reg_base_value (const_rtx, const_rtx);
extern bool memory_modified_in_insn_p (const_rtx, const_rtx);
extern bool may_be_sp_based_p (rtx);
extern rtx gen_hard_reg_clobber (machine_mode, unsigned int);
extern rtx get_reg_known_value (unsigned int);
extern bool get_reg_known_equiv_p (unsigned int);
extern rtx get_reg_base_value (unsigned int);

#ifdef STACK_REGS
extern int stack_regs_mentioned (const_rtx insn);
#endif

/* In toplev.c */
extern GTY(()) rtx stack_limit_rtx;

/* In var-tracking.c */
extern unsigned int variable_tracking_main (void);
extern void delete_vta_debug_insns (bool);

/* In stor-layout.c.  */
extern void get_mode_bounds (scalar_int_mode, int,
			     scalar_int_mode, rtx *, rtx *);

/* In loop-iv.c  */
extern rtx canon_condition (rtx);
extern void simplify_using_condition (rtx, rtx *, bitmap);

/* In final.c  */
extern unsigned int compute_alignments (void);
extern void update_alignments (vec<rtx> &);
extern int asm_str_count (const char *templ);

struct rtl_hooks
{
  rtx (*gen_lowpart) (machine_mode, rtx);
  rtx (*gen_lowpart_no_emit) (machine_mode, rtx);
  rtx (*reg_nonzero_bits) (const_rtx, scalar_int_mode, scalar_int_mode,
			   unsigned HOST_WIDE_INT *);
  rtx (*reg_num_sign_bit_copies) (const_rtx, scalar_int_mode, scalar_int_mode,
				  unsigned int *);
  bool (*reg_truncated_to_mode) (machine_mode, const_rtx);

  /* Whenever you add entries here, make sure you adjust rtlhooks-def.h.  */
};

/* Each pass can provide its own.  */
extern struct rtl_hooks rtl_hooks;

/* ... but then it has to restore these.  */
extern const struct rtl_hooks general_rtl_hooks;

/* Keep this for the nonce.  */
#define gen_lowpart rtl_hooks.gen_lowpart

extern void insn_locations_init (void);
extern void insn_locations_finalize (void);
extern void set_curr_insn_location (location_t);
extern location_t curr_insn_location (void);

/* rtl-error.c */
extern void _fatal_insn_not_found (const_rtx, const char *, int, const char *)
     ATTRIBUTE_NORETURN ATTRIBUTE_COLD;
extern void _fatal_insn (const char *, const_rtx, const char *, int, const char *)
     ATTRIBUTE_NORETURN ATTRIBUTE_COLD;

#define fatal_insn(msgid, insn) \
	_fatal_insn (msgid, insn, __FILE__, __LINE__, __FUNCTION__)
#define fatal_insn_not_found(insn) \
	_fatal_insn_not_found (insn, __FILE__, __LINE__, __FUNCTION__)

/* reginfo.c */
extern tree GTY(()) global_regs_decl[FIRST_PSEUDO_REGISTER];

/* Information about the function that is propagated by the RTL backend.
   Available only for functions that has been already assembled.  */

struct GTY(()) cgraph_rtl_info {
   unsigned int preferred_incoming_stack_boundary;

  /* Call unsaved hard registers really used by the corresponding
     function (including ones used by functions called by the
     function).  */
  HARD_REG_SET function_used_regs;
  /* Set if function_used_regs is valid.  */
  unsigned function_used_regs_valid: 1;
};

/* If loads from memories of mode MODE always sign or zero extend,
   return SIGN_EXTEND or ZERO_EXTEND as appropriate.  Return UNKNOWN
   otherwise.  */

inline rtx_code
load_extend_op (machine_mode mode)
{
  scalar_int_mode int_mode;
  if (is_a <scalar_int_mode> (mode, &int_mode)
      && GET_MODE_PRECISION (int_mode) < BITS_PER_WORD)
    return LOAD_EXTEND_OP (int_mode);
  return UNKNOWN;
}

/* If X is a PLUS of a base and a constant offset, add the constant to *OFFSET
   and return the base.  Return X otherwise.  */

inline rtx
strip_offset_and_add (rtx x, poly_int64_pod *offset)
{
  if (GET_CODE (x) == PLUS)
    {
      poly_int64 suboffset;
      x = strip_offset (x, &suboffset);
      *offset = poly_uint64 (*offset) + suboffset;
    }
  return x;
}

/* Return true if X is an operation that always operates on the full
   registers for WORD_REGISTER_OPERATIONS architectures.  */

inline bool
word_register_operation_p (const_rtx x)
{
  switch (GET_CODE (x))
    {
    case ROTATE:
    case ROTATERT:
    case SIGN_EXTRACT:
    case ZERO_EXTRACT:
      return false;
    
    default:
      return true;
    }
}
    
/* gtype-desc.c.  */
extern void gt_ggc_mx (rtx &);
extern void gt_pch_nx (rtx &);
extern void gt_pch_nx (rtx &, gt_pointer_operator, void *);

#endif /* ! GCC_RTL_H */
