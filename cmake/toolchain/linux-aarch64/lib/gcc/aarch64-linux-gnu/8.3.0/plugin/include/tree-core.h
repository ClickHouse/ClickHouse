/* Core data structures for the 'tree' type.
   Copyright (C) 1989-2018 Free Software Foundation, Inc.

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

#ifndef GCC_TREE_CORE_H
#define GCC_TREE_CORE_H

#include "symtab.h"

/* This file contains all the data structures that define the 'tree' type.
   There are no accessor macros nor functions in this file. Only the
   basic data structures, extern declarations and type definitions.  */

/*---------------------------------------------------------------------------
   Forward type declarations.  Mostly to avoid including unnecessary headers
---------------------------------------------------------------------------*/
struct function;
struct real_value;
struct fixed_value;
struct ptr_info_def;
struct range_info_def;
struct die_struct;


/*---------------------------------------------------------------------------
                              #defined constants
---------------------------------------------------------------------------*/
/* Nonzero if this is a call to a function whose return value depends
   solely on its arguments, has no side effects, and does not read
   global memory.  This corresponds to TREE_READONLY for function
   decls.  */
#define ECF_CONST		  (1 << 0)

/* Nonzero if this is a call to "pure" function (like const function,
   but may read memory.  This corresponds to DECL_PURE_P for function
   decls.  */
#define ECF_PURE		  (1 << 1)

/* Nonzero if this is ECF_CONST or ECF_PURE but cannot be proven to no
   infinite loop.  This corresponds to DECL_LOOPING_CONST_OR_PURE_P
   for function decls.*/
#define ECF_LOOPING_CONST_OR_PURE (1 << 2)

/* Nonzero if this call will never return.  */
#define ECF_NORETURN		  (1 << 3)

/* Nonzero if this is a call to malloc or a related function.  */
#define ECF_MALLOC		  (1 << 4)

/* Nonzero if it is plausible that this is a call to alloca.  */
#define ECF_MAY_BE_ALLOCA	  (1 << 5)

/* Nonzero if this is a call to a function that won't throw an exception.  */
#define ECF_NOTHROW		  (1 << 6)

/* Nonzero if this is a call to setjmp or a related function.  */
#define ECF_RETURNS_TWICE	  (1 << 7)

/* Nonzero if this call replaces the current stack frame.  */
#define ECF_SIBCALL		  (1 << 8)

/* Function does not read or write memory (but may have side effects, so
   it does not necessarily fit ECF_CONST).  */
#define ECF_NOVOPS		  (1 << 9)

/* The function does not lead to calls within current function unit.  */
#define ECF_LEAF		  (1 << 10)

/* Nonzero if this call returns its first argument.  */
#define ECF_RET1		  (1 << 11)

/* Nonzero if this call does not affect transactions.  */
#define ECF_TM_PURE		  (1 << 12)

/* Nonzero if this call is into the transaction runtime library.  */
#define ECF_TM_BUILTIN		  (1 << 13)

/* Nonzero if this is an indirect call by descriptor.  */
#define ECF_BY_DESCRIPTOR	  (1 << 14)

/* Nonzero if this is a cold function.  */
#define ECF_COLD		  (1 << 15)

/* Call argument flags.  */
/* Nonzero if the argument is not dereferenced recursively, thus only
   directly reachable memory is read or written.  */
#define EAF_DIRECT		(1 << 0)

/* Nonzero if memory reached by the argument is not clobbered.  */
#define EAF_NOCLOBBER		(1 << 1)

/* Nonzero if the argument does not escape.  */
#define EAF_NOESCAPE		(1 << 2)

/* Nonzero if the argument is not used by the function.  */
#define EAF_UNUSED		(1 << 3)

/* Call return flags.  */
/* Mask for the argument number that is returned.  Lower two bits of
   the return flags, encodes argument slots zero to three.  */
#define ERF_RETURN_ARG_MASK	(3)

/* Nonzero if the return value is equal to the argument number
   flags & ERF_RETURN_ARG_MASK.  */
#define ERF_RETURNS_ARG		(1 << 2)

/* Nonzero if the return value does not alias with anything.  Functions
   with the malloc attribute have this set on their return value.  */
#define ERF_NOALIAS		(1 << 3)


/*---------------------------------------------------------------------------
                                  Enumerations
---------------------------------------------------------------------------*/
/* Codes of tree nodes.  */
#define DEFTREECODE(SYM, STRING, TYPE, NARGS)   SYM,
#define END_OF_BASE_TREE_CODES LAST_AND_UNUSED_TREE_CODE,

enum tree_code {
#include "all-tree.def"
MAX_TREE_CODES
};

#undef DEFTREECODE
#undef END_OF_BASE_TREE_CODES

/* Number of language-independent tree codes.  */
#define NUM_TREE_CODES \
  ((int) LAST_AND_UNUSED_TREE_CODE)

#define CODE_CONTAINS_STRUCT(CODE, STRUCT) \
  (tree_contains_struct[(CODE)][(STRUCT)])


/* Classify which part of the compiler has defined a given builtin function.
   Note that we assume below that this is no more than two bits.  */
enum built_in_class {
  NOT_BUILT_IN = 0,
  BUILT_IN_FRONTEND,
  BUILT_IN_MD,
  BUILT_IN_NORMAL
};

/* Last marker used for LTO stremaing of built_in_class.  We can not add it
   to the enum since we need the enumb to fit in 2 bits.  */
#define BUILT_IN_LAST (BUILT_IN_NORMAL + 1)

/* Codes that identify the various built in functions
   so that expand_call can identify them quickly.  */
#define DEF_BUILTIN(ENUM, N, C, T, LT, B, F, NA, AT, IM, COND) ENUM,
enum built_in_function {
#include "builtins.def"

  BEGIN_CHKP_BUILTINS,

#define DEF_BUILTIN(ENUM, N, C, T, LT, B, F, NA, AT, IM, COND)
#define DEF_BUILTIN_CHKP(ENUM, N, C, T, LT, B, F, NA, AT, IM, COND) \
  ENUM##_CHKP = ENUM + BEGIN_CHKP_BUILTINS + 1,
#include "builtins.def"

  END_CHKP_BUILTINS = BEGIN_CHKP_BUILTINS * 2 + 1,

  /* Complex division routines in libgcc.  These are done via builtins
     because emit_library_call_value can't handle complex values.  */
  BUILT_IN_COMPLEX_MUL_MIN,
  BUILT_IN_COMPLEX_MUL_MAX
    = BUILT_IN_COMPLEX_MUL_MIN
      + MAX_MODE_COMPLEX_FLOAT
      - MIN_MODE_COMPLEX_FLOAT,

  BUILT_IN_COMPLEX_DIV_MIN,
  BUILT_IN_COMPLEX_DIV_MAX
    = BUILT_IN_COMPLEX_DIV_MIN
      + MAX_MODE_COMPLEX_FLOAT
      - MIN_MODE_COMPLEX_FLOAT,

  /* Upper bound on non-language-specific builtins.  */
  END_BUILTINS
};

/* Internal functions.  */
enum internal_fn {
#define DEF_INTERNAL_FN(CODE, FLAGS, FNSPEC) IFN_##CODE,
#include "internal-fn.def"
  IFN_LAST
};

/* An enum that combines target-independent built-in functions with
   internal functions, so that they can be treated in a similar way.
   The numbers for built-in functions are the same as for the
   built_in_function enum.  The numbers for internal functions
   start at END_BUITLINS.  */
enum combined_fn {
#define DEF_BUILTIN(ENUM, N, C, T, LT, B, F, NA, AT, IM, COND) \
  CFN_##ENUM = int (ENUM),
#include "builtins.def"

#define DEF_BUILTIN(ENUM, N, C, T, LT, B, F, NA, AT, IM, COND)
#define DEF_BUILTIN_CHKP(ENUM, N, C, T, LT, B, F, NA, AT, IM, COND) \
  CFN_##ENUM##_CHKP = int (ENUM##_CHKP),
#include "builtins.def"

#define DEF_INTERNAL_FN(CODE, FLAGS, FNSPEC) \
  CFN_##CODE = int (END_BUILTINS) + int (IFN_##CODE),
#include "internal-fn.def"

  CFN_LAST
};

/* Tree code classes.  Each tree_code has an associated code class
   represented by a TREE_CODE_CLASS.  */
enum tree_code_class {
  tcc_exceptional, /* An exceptional code (fits no category).  */
  tcc_constant,    /* A constant.  */
  /* Order of tcc_type and tcc_declaration is important.  */
  tcc_type,        /* A type object code.  */
  tcc_declaration, /* A declaration (also serving as variable refs).  */
  tcc_reference,   /* A reference to storage.  */
  tcc_comparison,  /* A comparison expression.  */
  tcc_unary,       /* A unary arithmetic expression.  */
  tcc_binary,      /* A binary arithmetic expression.  */
  tcc_statement,   /* A statement expression, which have side effects
		      but usually no interesting value.  */
  tcc_vl_exp,      /* A function call or other expression with a
		      variable-length operand vector.  */
  tcc_expression   /* Any other expression.  */
};

/* OMP_CLAUSE codes.  Do not reorder, as this is used to index into
   the tables omp_clause_num_ops and omp_clause_code_name.  */
enum omp_clause_code {
  /* Clause zero is special-cased inside the parser
     (c_parser_omp_variable_list).  */
  OMP_CLAUSE_ERROR = 0,

  /* OpenACC/OpenMP clause: private (variable_list).  */
  OMP_CLAUSE_PRIVATE,

  /* OpenMP clause: shared (variable_list).  */
  OMP_CLAUSE_SHARED,

  /* OpenACC/OpenMP clause: firstprivate (variable_list).  */
  OMP_CLAUSE_FIRSTPRIVATE,

  /* OpenMP clause: lastprivate (variable_list).  */
  OMP_CLAUSE_LASTPRIVATE,

  /* OpenACC/OpenMP clause: reduction (operator:variable_list).
     OMP_CLAUSE_REDUCTION_CODE: The tree_code of the operator.
     Operand 1: OMP_CLAUSE_REDUCTION_INIT: Stmt-list to initialize the var.
     Operand 2: OMP_CLAUSE_REDUCTION_MERGE: Stmt-list to merge private var
                into the shared one.
     Operand 3: OMP_CLAUSE_REDUCTION_PLACEHOLDER: A dummy VAR_DECL
                placeholder used in OMP_CLAUSE_REDUCTION_{INIT,MERGE}.
     Operand 4: OMP_CLAUSE_REDUCTION_DECL_PLACEHOLDER: Another dummy
		VAR_DECL placeholder, used like the above for C/C++ array
		reductions.  */
  OMP_CLAUSE_REDUCTION,

  /* OpenMP clause: copyin (variable_list).  */
  OMP_CLAUSE_COPYIN,

  /* OpenMP clause: copyprivate (variable_list).  */
  OMP_CLAUSE_COPYPRIVATE,

  /* OpenMP clause: linear (variable-list[:linear-step]).  */
  OMP_CLAUSE_LINEAR,

  /* OpenMP clause: aligned (variable-list[:alignment]).  */
  OMP_CLAUSE_ALIGNED,

  /* OpenMP clause: depend ({in,out,inout}:variable-list).  */
  OMP_CLAUSE_DEPEND,

  /* OpenMP clause: uniform (argument-list).  */
  OMP_CLAUSE_UNIFORM,

  /* OpenMP clause: to (extended-list).
     Only when it appears in declare target.  */
  OMP_CLAUSE_TO_DECLARE,

  /* OpenMP clause: link (variable-list).  */
  OMP_CLAUSE_LINK,

  /* OpenMP clause: from (variable-list).  */
  OMP_CLAUSE_FROM,

  /* OpenMP clause: to (variable-list).  */
  OMP_CLAUSE_TO,

  /* OpenACC clauses: {copy, copyin, copyout, create, delete, deviceptr,
     device, host (self), present, present_or_copy (pcopy), present_or_copyin
     (pcopyin), present_or_copyout (pcopyout), present_or_create (pcreate)}
     (variable-list).

     OpenMP clause: map ({alloc:,to:,from:,tofrom:,}variable-list).  */
  OMP_CLAUSE_MAP,

  /* OpenACC clause: use_device (variable_list).
     OpenMP clause: use_device_ptr (variable-list).  */
  OMP_CLAUSE_USE_DEVICE_PTR,

  /* OpenMP clause: is_device_ptr (variable-list).  */
  OMP_CLAUSE_IS_DEVICE_PTR,

  /* Internal structure to hold OpenACC cache directive's variable-list.
     #pragma acc cache (variable-list).  */
  OMP_CLAUSE__CACHE_,

  /* OpenACC clause: gang [(gang-argument-list)].
     Where
      gang-argument-list: [gang-argument-list, ] gang-argument
      gang-argument: [num:] integer-expression
                   | static: size-expression
      size-expression: * | integer-expression.  */
  OMP_CLAUSE_GANG,

  /* OpenACC clause: async [(integer-expression)].  */
  OMP_CLAUSE_ASYNC,

  /* OpenACC clause: wait [(integer-expression-list)].  */
  OMP_CLAUSE_WAIT,

  /* OpenACC clause: auto.  */
  OMP_CLAUSE_AUTO,

  /* OpenACC clause: seq.  */
  OMP_CLAUSE_SEQ,

  /* Internal clause: temporary for combined loops expansion.  */
  OMP_CLAUSE__LOOPTEMP_,

  /* OpenACC/OpenMP clause: if (scalar-expression).  */
  OMP_CLAUSE_IF,

  /* OpenMP clause: num_threads (integer-expression).  */
  OMP_CLAUSE_NUM_THREADS,

  /* OpenMP clause: schedule.  */
  OMP_CLAUSE_SCHEDULE,

  /* OpenMP clause: nowait.  */
  OMP_CLAUSE_NOWAIT,

  /* OpenMP clause: ordered [(constant-integer-expression)].  */
  OMP_CLAUSE_ORDERED,

  /* OpenACC/OpenMP clause: default.  */
  OMP_CLAUSE_DEFAULT,

  /* OpenACC/OpenMP clause: collapse (constant-integer-expression).  */
  OMP_CLAUSE_COLLAPSE,

  /* OpenMP clause: untied.  */
  OMP_CLAUSE_UNTIED,

  /* OpenMP clause: final (scalar-expression).  */
  OMP_CLAUSE_FINAL,

  /* OpenMP clause: mergeable.  */
  OMP_CLAUSE_MERGEABLE,

  /* OpenMP clause: device (integer-expression).  */
  OMP_CLAUSE_DEVICE,

  /* OpenMP clause: dist_schedule (static[:chunk-size]).  */
  OMP_CLAUSE_DIST_SCHEDULE,

  /* OpenMP clause: inbranch.  */
  OMP_CLAUSE_INBRANCH,

  /* OpenMP clause: notinbranch.  */
  OMP_CLAUSE_NOTINBRANCH,

  /* OpenMP clause: num_teams(integer-expression).  */
  OMP_CLAUSE_NUM_TEAMS,

  /* OpenMP clause: thread_limit(integer-expression).  */
  OMP_CLAUSE_THREAD_LIMIT,

  /* OpenMP clause: proc_bind ({master,close,spread}).  */
  OMP_CLAUSE_PROC_BIND,

  /* OpenMP clause: safelen (constant-integer-expression).  */
  OMP_CLAUSE_SAFELEN,

  /* OpenMP clause: simdlen (constant-integer-expression).  */
  OMP_CLAUSE_SIMDLEN,

  /* OpenMP clause: for.  */
  OMP_CLAUSE_FOR,

  /* OpenMP clause: parallel.  */
  OMP_CLAUSE_PARALLEL,

  /* OpenMP clause: sections.  */
  OMP_CLAUSE_SECTIONS,

  /* OpenMP clause: taskgroup.  */
  OMP_CLAUSE_TASKGROUP,

  /* OpenMP clause: priority (integer-expression).  */
  OMP_CLAUSE_PRIORITY,

  /* OpenMP clause: grainsize (integer-expression).  */
  OMP_CLAUSE_GRAINSIZE,

  /* OpenMP clause: num_tasks (integer-expression).  */
  OMP_CLAUSE_NUM_TASKS,

  /* OpenMP clause: nogroup.  */
  OMP_CLAUSE_NOGROUP,

  /* OpenMP clause: threads.  */
  OMP_CLAUSE_THREADS,

  /* OpenMP clause: simd.  */
  OMP_CLAUSE_SIMD,

  /* OpenMP clause: hint (integer-expression).  */
  OMP_CLAUSE_HINT,

  /* OpenMP clause: defaultmap (tofrom: scalar).  */
  OMP_CLAUSE_DEFAULTMAP,

  /* Internally used only clause, holding SIMD uid.  */
  OMP_CLAUSE__SIMDUID_,

  /* Internally used only clause, flag whether this is SIMT simd
     loop or not.  */
  OMP_CLAUSE__SIMT_,

  /* OpenACC clause: independent.  */
  OMP_CLAUSE_INDEPENDENT,

  /* OpenACC clause: worker [( [num:] integer-expression)].  */
  OMP_CLAUSE_WORKER,

  /* OpenACC clause: vector [( [length:] integer-expression)].  */
  OMP_CLAUSE_VECTOR,

  /* OpenACC clause: num_gangs (integer-expression).  */
  OMP_CLAUSE_NUM_GANGS,

  /* OpenACC clause: num_workers (integer-expression).  */
  OMP_CLAUSE_NUM_WORKERS,

  /* OpenACC clause: vector_length (integer-expression).  */
  OMP_CLAUSE_VECTOR_LENGTH,

  /* OpenACC clause: tile ( size-expr-list ).  */
  OMP_CLAUSE_TILE,

  /* OpenMP internal-only clause to specify grid dimensions of a gridified
     kernel.  */
  OMP_CLAUSE__GRIDDIM_
};

#undef DEFTREESTRUCT
#define DEFTREESTRUCT(ENUM, NAME) ENUM,
enum tree_node_structure_enum {
#include "treestruct.def"
  LAST_TS_ENUM
};
#undef DEFTREESTRUCT

enum omp_clause_schedule_kind {
  OMP_CLAUSE_SCHEDULE_STATIC,
  OMP_CLAUSE_SCHEDULE_DYNAMIC,
  OMP_CLAUSE_SCHEDULE_GUIDED,
  OMP_CLAUSE_SCHEDULE_AUTO,
  OMP_CLAUSE_SCHEDULE_RUNTIME,
  OMP_CLAUSE_SCHEDULE_MASK = (1 << 3) - 1,
  OMP_CLAUSE_SCHEDULE_MONOTONIC = (1 << 3),
  OMP_CLAUSE_SCHEDULE_NONMONOTONIC = (1 << 4),
  OMP_CLAUSE_SCHEDULE_LAST = 2 * OMP_CLAUSE_SCHEDULE_NONMONOTONIC - 1
};

enum omp_clause_default_kind {
  OMP_CLAUSE_DEFAULT_UNSPECIFIED,
  OMP_CLAUSE_DEFAULT_SHARED,
  OMP_CLAUSE_DEFAULT_NONE,
  OMP_CLAUSE_DEFAULT_PRIVATE,
  OMP_CLAUSE_DEFAULT_FIRSTPRIVATE,
  OMP_CLAUSE_DEFAULT_PRESENT,
  OMP_CLAUSE_DEFAULT_LAST
};

/* There is a TYPE_QUAL value for each type qualifier.  They can be
   combined by bitwise-or to form the complete set of qualifiers for a
   type.  */
enum cv_qualifier {
  TYPE_UNQUALIFIED   = 0x0,
  TYPE_QUAL_CONST    = 0x1,
  TYPE_QUAL_VOLATILE = 0x2,
  TYPE_QUAL_RESTRICT = 0x4,
  TYPE_QUAL_ATOMIC   = 0x8
};

/* Standard named or nameless data types of the C compiler.  */
enum tree_index {
  TI_ERROR_MARK,
  TI_INTQI_TYPE,
  TI_INTHI_TYPE,
  TI_INTSI_TYPE,
  TI_INTDI_TYPE,
  TI_INTTI_TYPE,

  TI_UINTQI_TYPE,
  TI_UINTHI_TYPE,
  TI_UINTSI_TYPE,
  TI_UINTDI_TYPE,
  TI_UINTTI_TYPE,

  TI_ATOMICQI_TYPE,
  TI_ATOMICHI_TYPE,
  TI_ATOMICSI_TYPE,
  TI_ATOMICDI_TYPE,
  TI_ATOMICTI_TYPE,

  TI_UINT16_TYPE,
  TI_UINT32_TYPE,
  TI_UINT64_TYPE,

  TI_VOID,

  TI_INTEGER_ZERO,
  TI_INTEGER_ONE,
  TI_INTEGER_THREE,
  TI_INTEGER_MINUS_ONE,
  TI_NULL_POINTER,

  TI_SIZE_ZERO,
  TI_SIZE_ONE,

  TI_BITSIZE_ZERO,
  TI_BITSIZE_ONE,
  TI_BITSIZE_UNIT,

  TI_PUBLIC,
  TI_PROTECTED,
  TI_PRIVATE,

  TI_BOOLEAN_FALSE,
  TI_BOOLEAN_TRUE,

  TI_FLOAT_TYPE,
  TI_DOUBLE_TYPE,
  TI_LONG_DOUBLE_TYPE,

  /* The _FloatN and _FloatNx types must be consecutive, and in the
     same sequence as the corresponding complex types, which must also
     be consecutive; _FloatN must come before _FloatNx; the order must
     also be the same as in the floatn_nx_types array and the RID_*
     values in c-common.h.  This is so that iterations over these
     types work as intended.  */
  TI_FLOAT16_TYPE,
  TI_FLOATN_TYPE_FIRST = TI_FLOAT16_TYPE,
  TI_FLOATN_NX_TYPE_FIRST = TI_FLOAT16_TYPE,
  TI_FLOAT32_TYPE,
  TI_FLOAT64_TYPE,
  TI_FLOAT128_TYPE,
  TI_FLOATN_TYPE_LAST = TI_FLOAT128_TYPE,
#define NUM_FLOATN_TYPES (TI_FLOATN_TYPE_LAST - TI_FLOATN_TYPE_FIRST + 1)
  TI_FLOAT32X_TYPE,
  TI_FLOATNX_TYPE_FIRST = TI_FLOAT32X_TYPE,
  TI_FLOAT64X_TYPE,
  TI_FLOAT128X_TYPE,
  TI_FLOATNX_TYPE_LAST = TI_FLOAT128X_TYPE,
  TI_FLOATN_NX_TYPE_LAST = TI_FLOAT128X_TYPE,
#define NUM_FLOATNX_TYPES (TI_FLOATNX_TYPE_LAST - TI_FLOATNX_TYPE_FIRST + 1)
#define NUM_FLOATN_NX_TYPES (TI_FLOATN_NX_TYPE_LAST		\
			     - TI_FLOATN_NX_TYPE_FIRST		\
			     + 1)

  /* Put the complex types after their component types, so that in (sequential)
     tree streaming we can assert that their component types have already been
     handled (see tree-streamer.c:record_common_node).  */
  TI_COMPLEX_INTEGER_TYPE,
  TI_COMPLEX_FLOAT_TYPE,
  TI_COMPLEX_DOUBLE_TYPE,
  TI_COMPLEX_LONG_DOUBLE_TYPE,

  TI_COMPLEX_FLOAT16_TYPE,
  TI_COMPLEX_FLOATN_NX_TYPE_FIRST = TI_COMPLEX_FLOAT16_TYPE,
  TI_COMPLEX_FLOAT32_TYPE,
  TI_COMPLEX_FLOAT64_TYPE,
  TI_COMPLEX_FLOAT128_TYPE,
  TI_COMPLEX_FLOAT32X_TYPE,
  TI_COMPLEX_FLOAT64X_TYPE,
  TI_COMPLEX_FLOAT128X_TYPE,

  TI_FLOAT_PTR_TYPE,
  TI_DOUBLE_PTR_TYPE,
  TI_LONG_DOUBLE_PTR_TYPE,
  TI_INTEGER_PTR_TYPE,

  TI_VOID_TYPE,
  TI_PTR_TYPE,
  TI_CONST_PTR_TYPE,
  TI_SIZE_TYPE,
  TI_PID_TYPE,
  TI_PTRDIFF_TYPE,
  TI_VA_LIST_TYPE,
  TI_VA_LIST_GPR_COUNTER_FIELD,
  TI_VA_LIST_FPR_COUNTER_FIELD,
  TI_BOOLEAN_TYPE,
  TI_FILEPTR_TYPE,
  TI_CONST_TM_PTR_TYPE,
  TI_FENV_T_PTR_TYPE,
  TI_CONST_FENV_T_PTR_TYPE,
  TI_FEXCEPT_T_PTR_TYPE,
  TI_CONST_FEXCEPT_T_PTR_TYPE,
  TI_POINTER_SIZED_TYPE,

  TI_POINTER_BOUNDS_TYPE,

  TI_DFLOAT32_TYPE,
  TI_DFLOAT64_TYPE,
  TI_DFLOAT128_TYPE,
  TI_DFLOAT32_PTR_TYPE,
  TI_DFLOAT64_PTR_TYPE,
  TI_DFLOAT128_PTR_TYPE,

  TI_VOID_LIST_NODE,

  TI_MAIN_IDENTIFIER,

  TI_SAT_SFRACT_TYPE,
  TI_SAT_FRACT_TYPE,
  TI_SAT_LFRACT_TYPE,
  TI_SAT_LLFRACT_TYPE,
  TI_SAT_USFRACT_TYPE,
  TI_SAT_UFRACT_TYPE,
  TI_SAT_ULFRACT_TYPE,
  TI_SAT_ULLFRACT_TYPE,
  TI_SFRACT_TYPE,
  TI_FRACT_TYPE,
  TI_LFRACT_TYPE,
  TI_LLFRACT_TYPE,
  TI_USFRACT_TYPE,
  TI_UFRACT_TYPE,
  TI_ULFRACT_TYPE,
  TI_ULLFRACT_TYPE,
  TI_SAT_SACCUM_TYPE,
  TI_SAT_ACCUM_TYPE,
  TI_SAT_LACCUM_TYPE,
  TI_SAT_LLACCUM_TYPE,
  TI_SAT_USACCUM_TYPE,
  TI_SAT_UACCUM_TYPE,
  TI_SAT_ULACCUM_TYPE,
  TI_SAT_ULLACCUM_TYPE,
  TI_SACCUM_TYPE,
  TI_ACCUM_TYPE,
  TI_LACCUM_TYPE,
  TI_LLACCUM_TYPE,
  TI_USACCUM_TYPE,
  TI_UACCUM_TYPE,
  TI_ULACCUM_TYPE,
  TI_ULLACCUM_TYPE,
  TI_QQ_TYPE,
  TI_HQ_TYPE,
  TI_SQ_TYPE,
  TI_DQ_TYPE,
  TI_TQ_TYPE,
  TI_UQQ_TYPE,
  TI_UHQ_TYPE,
  TI_USQ_TYPE,
  TI_UDQ_TYPE,
  TI_UTQ_TYPE,
  TI_SAT_QQ_TYPE,
  TI_SAT_HQ_TYPE,
  TI_SAT_SQ_TYPE,
  TI_SAT_DQ_TYPE,
  TI_SAT_TQ_TYPE,
  TI_SAT_UQQ_TYPE,
  TI_SAT_UHQ_TYPE,
  TI_SAT_USQ_TYPE,
  TI_SAT_UDQ_TYPE,
  TI_SAT_UTQ_TYPE,
  TI_HA_TYPE,
  TI_SA_TYPE,
  TI_DA_TYPE,
  TI_TA_TYPE,
  TI_UHA_TYPE,
  TI_USA_TYPE,
  TI_UDA_TYPE,
  TI_UTA_TYPE,
  TI_SAT_HA_TYPE,
  TI_SAT_SA_TYPE,
  TI_SAT_DA_TYPE,
  TI_SAT_TA_TYPE,
  TI_SAT_UHA_TYPE,
  TI_SAT_USA_TYPE,
  TI_SAT_UDA_TYPE,
  TI_SAT_UTA_TYPE,

  TI_OPTIMIZATION_DEFAULT,
  TI_OPTIMIZATION_CURRENT,
  TI_TARGET_OPTION_DEFAULT,
  TI_TARGET_OPTION_CURRENT,
  TI_CURRENT_TARGET_PRAGMA,
  TI_CURRENT_OPTIMIZE_PRAGMA,

  TI_MAX
};

/* An enumeration of the standard C integer types.  These must be
   ordered so that shorter types appear before longer ones, and so
   that signed types appear before unsigned ones, for the correct
   functioning of interpret_integer() in c-lex.c.  */
enum integer_type_kind {
  itk_char,
  itk_signed_char,
  itk_unsigned_char,
  itk_short,
  itk_unsigned_short,
  itk_int,
  itk_unsigned_int,
  itk_long,
  itk_unsigned_long,
  itk_long_long,
  itk_unsigned_long_long,

  itk_intN_0,
  itk_unsigned_intN_0,
  itk_intN_1,
  itk_unsigned_intN_1,
  itk_intN_2,
  itk_unsigned_intN_2,
  itk_intN_3,
  itk_unsigned_intN_3,

  itk_none
};

/* A pointer-to-function member type looks like:

     struct {
       __P __pfn;
       ptrdiff_t __delta;
     };

   If __pfn is NULL, it is a NULL pointer-to-member-function.

   (Because the vtable is always the first thing in the object, we
   don't need its offset.)  If the function is virtual, then PFN is
   one plus twice the index into the vtable; otherwise, it is just a
   pointer to the function.

   Unfortunately, using the lowest bit of PFN doesn't work in
   architectures that don't impose alignment requirements on function
   addresses, or that use the lowest bit to tell one ISA from another,
   for example.  For such architectures, we use the lowest bit of
   DELTA instead of the lowest bit of the PFN, and DELTA will be
   multiplied by 2.  */
enum ptrmemfunc_vbit_where_t {
  ptrmemfunc_vbit_in_pfn,
  ptrmemfunc_vbit_in_delta
};

/* Flags that may be passed in the third argument of decl_attributes, and
   to handler functions for attributes.  */
enum attribute_flags {
  /* The type passed in is the type of a DECL, and any attributes that
     should be passed in again to be applied to the DECL rather than the
     type should be returned.  */
  ATTR_FLAG_DECL_NEXT = 1,
  /* The type passed in is a function return type, and any attributes that
     should be passed in again to be applied to the function type rather
     than the return type should be returned.  */
  ATTR_FLAG_FUNCTION_NEXT = 2,
  /* The type passed in is an array element type, and any attributes that
     should be passed in again to be applied to the array type rather
     than the element type should be returned.  */
  ATTR_FLAG_ARRAY_NEXT = 4,
  /* The type passed in is a structure, union or enumeration type being
     created, and should be modified in place.  */
  ATTR_FLAG_TYPE_IN_PLACE = 8,
  /* The attributes are being applied by default to a library function whose
     name indicates known behavior, and should be silently ignored if they
     are not in fact compatible with the function type.  */
  ATTR_FLAG_BUILT_IN = 16,
  /* A given attribute has been parsed as a C++-11 attribute.  */
  ATTR_FLAG_CXX11 = 32
};

/* Types used to represent sizes.  */
enum size_type_kind {
  stk_sizetype,		/* Normal representation of sizes in bytes.  */
  stk_ssizetype,	/* Signed representation of sizes in bytes.  */
  stk_bitsizetype,	/* Normal representation of sizes in bits.  */
  stk_sbitsizetype,	/* Signed representation of sizes in bits.  */
  stk_type_kind_last
};

enum operand_equal_flag {
  OEP_ONLY_CONST = 1,
  OEP_PURE_SAME = 2,
  OEP_MATCH_SIDE_EFFECTS = 4,
  OEP_ADDRESS_OF = 8,
  /* Internal within operand_equal_p:  */
  OEP_NO_HASH_CHECK = 16,
  /* Internal within inchash::add_expr:  */
  OEP_HASH_CHECK = 32,
  /* Makes operand_equal_p handle more expressions:  */
  OEP_LEXICOGRAPHIC = 64
};

/* Enum and arrays used for tree allocation stats.
   Keep in sync with tree.c:tree_node_kind_names.  */
enum tree_node_kind {
  d_kind,
  t_kind,
  b_kind,
  s_kind,
  r_kind,
  e_kind,
  c_kind,
  id_kind,
  vec_kind,
  binfo_kind,
  ssa_name_kind,
  constr_kind,
  x_kind,
  lang_decl,
  lang_type,
  omp_clause_kind,
  all_kinds
};

enum annot_expr_kind {
  annot_expr_ivdep_kind,
  annot_expr_unroll_kind,
  annot_expr_no_vector_kind,
  annot_expr_vector_kind,
  annot_expr_parallel_kind,
  annot_expr_kind_last
};

/*---------------------------------------------------------------------------
                                Type definitions
---------------------------------------------------------------------------*/
/* When processing aliases at the symbol table level, we need the
   declaration of target. For this reason we need to queue aliases and
   process them after all declarations has been produced.  */
struct GTY(()) alias_pair {
  tree decl;
  tree target;
};

/* An initialization priority.  */
typedef unsigned short priority_type;

/* The type of a callback function for walking over tree structure.  */
typedef tree (*walk_tree_fn) (tree *, int *, void *);

/* The type of a callback function that represents a custom walk_tree.  */
typedef tree (*walk_tree_lh) (tree *, int *, tree (*) (tree *, int *, void *),
			      void *, hash_set<tree> *);


/*---------------------------------------------------------------------------
                              Main data structures
---------------------------------------------------------------------------*/
/* A tree node can represent a data type, a variable, an expression
   or a statement.  Each node has a TREE_CODE which says what kind of
   thing it represents.  Some common codes are:
   INTEGER_TYPE -- represents a type of integers.
   ARRAY_TYPE -- represents a type of pointer.
   VAR_DECL -- represents a declared variable.
   INTEGER_CST -- represents a constant integer value.
   PLUS_EXPR -- represents a sum (an expression).

   As for the contents of a tree node: there are some fields
   that all nodes share.  Each TREE_CODE has various special-purpose
   fields as well.  The fields of a node are never accessed directly,
   always through accessor macros.  */

/* Every kind of tree node starts with this structure,
   so all nodes have these fields.

   See the accessor macros, defined below, for documentation of the
   fields, and the table below which connects the fields and the
   accessor macros.  */

struct GTY(()) tree_base {
  ENUM_BITFIELD(tree_code) code : 16;

  unsigned side_effects_flag : 1;
  unsigned constant_flag : 1;
  unsigned addressable_flag : 1;
  unsigned volatile_flag : 1;
  unsigned readonly_flag : 1;
  unsigned asm_written_flag: 1;
  unsigned nowarning_flag : 1;
  unsigned visited : 1;

  unsigned used_flag : 1;
  unsigned nothrow_flag : 1;
  unsigned static_flag : 1;
  unsigned public_flag : 1;
  unsigned private_flag : 1;
  unsigned protected_flag : 1;
  unsigned deprecated_flag : 1;
  unsigned default_def_flag : 1;

  union {
    /* The bits in the following structure should only be used with
       accessor macros that constrain inputs with tree checking.  */
    struct {
      unsigned lang_flag_0 : 1;
      unsigned lang_flag_1 : 1;
      unsigned lang_flag_2 : 1;
      unsigned lang_flag_3 : 1;
      unsigned lang_flag_4 : 1;
      unsigned lang_flag_5 : 1;
      unsigned lang_flag_6 : 1;
      unsigned saturating_flag : 1;

      unsigned unsigned_flag : 1;
      unsigned packed_flag : 1;
      unsigned user_align : 1;
      unsigned nameless_flag : 1;
      unsigned atomic_flag : 1;
      unsigned spare0 : 3;

      unsigned spare1 : 8;

      /* This field is only used with TREE_TYPE nodes; the only reason it is
	 present in tree_base instead of tree_type is to save space.  The size
	 of the field must be large enough to hold addr_space_t values.  */
      unsigned address_space : 8;
    } bits;

    /* The following fields are present in tree_base to save space.  The
       nodes using them do not require any of the flags above and so can
       make better use of the 4-byte sized word.  */

    /* The number of HOST_WIDE_INTs in an INTEGER_CST.  */
    struct {
      /* The number of HOST_WIDE_INTs if the INTEGER_CST is accessed in
	 its native precision.  */
      unsigned char unextended;

      /* The number of HOST_WIDE_INTs if the INTEGER_CST is extended to
	 wider precisions based on its TYPE_SIGN.  */
      unsigned char extended;

      /* The number of HOST_WIDE_INTs if the INTEGER_CST is accessed in
	 offset_int precision, with smaller integers being extended
	 according to their TYPE_SIGN.  This is equal to one of the two
	 fields above but is cached for speed.  */
      unsigned char offset;
    } int_length;

    /* VEC length.  This field is only used with TREE_VEC.  */
    int length;

    /* This field is only used with VECTOR_CST.  */
    struct {
      /* The value of VECTOR_CST_LOG2_NPATTERNS.  */
      unsigned int log2_npatterns : 8;

      /* The value of VECTOR_CST_NELTS_PER_PATTERN.  */
      unsigned int nelts_per_pattern : 8;

      /* For future expansion.  */
      unsigned int unused : 16;
    } vector_cst;

    /* SSA version number.  This field is only used with SSA_NAME.  */
    unsigned int version;

    /* CHREC_VARIABLE.  This field is only used with POLYNOMIAL_CHREC.  */
    unsigned int chrec_var;

    /* Internal function code.  */
    enum internal_fn ifn;

    /* The following two fields are used for MEM_REF and TARGET_MEM_REF
       expression trees and specify known data non-dependences.  For
       two memory references in a function they are known to not
       alias if dependence_info.clique are equal and dependence_info.base
       are distinct.  */
    struct {
      unsigned short clique;
      unsigned short base;
    } dependence_info;
  } GTY((skip(""))) u;
};

/* The following table lists the uses of each of the above flags and
   for which types of nodes they are defined.

   addressable_flag:

       TREE_ADDRESSABLE in
           VAR_DECL, PARM_DECL, RESULT_DECL, FUNCTION_DECL, LABEL_DECL
           SSA_NAME
           all types
           CONSTRUCTOR, IDENTIFIER_NODE
           STMT_EXPR

       CALL_EXPR_TAILCALL in
           CALL_EXPR

       CASE_LOW_SEEN in
           CASE_LABEL_EXPR

       PREDICT_EXPR_OUTCOME in
	   PREDICT_EXPR

   static_flag:

       TREE_STATIC in
           VAR_DECL, FUNCTION_DECL
           CONSTRUCTOR

       TREE_NO_TRAMPOLINE in
           ADDR_EXPR

       BINFO_VIRTUAL_P in
           TREE_BINFO

       TREE_SYMBOL_REFERENCED in
           IDENTIFIER_NODE

       CLEANUP_EH_ONLY in
           TARGET_EXPR, WITH_CLEANUP_EXPR

       TRY_CATCH_IS_CLEANUP in
           TRY_CATCH_EXPR

       ASM_INPUT_P in
           ASM_EXPR

       TYPE_REF_CAN_ALIAS_ALL in
           POINTER_TYPE, REFERENCE_TYPE

       CASE_HIGH_SEEN in
           CASE_LABEL_EXPR

       ENUM_IS_SCOPED in
	   ENUMERAL_TYPE

       TRANSACTION_EXPR_OUTER in
	   TRANSACTION_EXPR

       SSA_NAME_ANTI_RANGE_P in
	   SSA_NAME

       MUST_TAIL_CALL in
	   CALL_EXPR

   public_flag:

       TREE_OVERFLOW in
           INTEGER_CST, REAL_CST, COMPLEX_CST, VECTOR_CST

       TREE_PUBLIC in
           VAR_DECL, FUNCTION_DECL
           IDENTIFIER_NODE

       CONSTRUCTOR_NO_CLEARING in
           CONSTRUCTOR

       ASM_VOLATILE_P in
           ASM_EXPR

       CALL_EXPR_VA_ARG_PACK in
           CALL_EXPR

       TYPE_CACHED_VALUES_P in
           all types

       SAVE_EXPR_RESOLVED_P in
           SAVE_EXPR

       OMP_CLAUSE_LASTPRIVATE_FIRSTPRIVATE in
           OMP_CLAUSE_LASTPRIVATE

       OMP_CLAUSE_PRIVATE_DEBUG in
           OMP_CLAUSE_PRIVATE

       OMP_CLAUSE_LINEAR_NO_COPYIN in
	   OMP_CLAUSE_LINEAR

       OMP_CLAUSE_MAP_ZERO_BIAS_ARRAY_SECTION in
	   OMP_CLAUSE_MAP

       OMP_CLAUSE_REDUCTION_OMP_ORIG_REF in
	   OMP_CLAUSE_REDUCTION

       TRANSACTION_EXPR_RELAXED in
	   TRANSACTION_EXPR

       FALLTHROUGH_LABEL_P in
	   LABEL_DECL

       SSA_NAME_IS_VIRTUAL_OPERAND in
	   SSA_NAME

       EXPR_LOCATION_WRAPPER_P in
	   NON_LVALUE_EXPR, VIEW_CONVERT_EXPR

   private_flag:

       TREE_PRIVATE in
           all decls

       CALL_EXPR_RETURN_SLOT_OPT in
           CALL_EXPR

       OMP_SECTION_LAST in
           OMP_SECTION

       OMP_PARALLEL_COMBINED in
           OMP_PARALLEL

       OMP_ATOMIC_SEQ_CST in
	   OMP_ATOMIC*

       OMP_CLAUSE_PRIVATE_OUTER_REF in
	   OMP_CLAUSE_PRIVATE

       OMP_CLAUSE_LINEAR_NO_COPYOUT in
	   OMP_CLAUSE_LINEAR

       TYPE_REF_IS_RVALUE in
	   REFERENCE_TYPE

       ENUM_IS_OPAQUE in
	   ENUMERAL_TYPE

   protected_flag:

       TREE_PROTECTED in
           BLOCK
           all decls

       CALL_FROM_THUNK_P and
       CALL_ALLOCA_FOR_VAR_P in
           CALL_EXPR

       OMP_CLAUSE_LINEAR_VARIABLE_STRIDE in
	   OMP_CLAUSE_LINEAR

       ASM_INLINE_P in
	   ASM_EXPR

   side_effects_flag:

       TREE_SIDE_EFFECTS in
           all expressions
           all decls
           all constants

       FORCED_LABEL in
           LABEL_DECL

   volatile_flag:

       TREE_THIS_VOLATILE in
           all expressions
           all decls

       TYPE_VOLATILE in
           all types

   readonly_flag:

       TREE_READONLY in
           all expressions
           all decls

       TYPE_READONLY in
           all types

   constant_flag:

       TREE_CONSTANT in
           all expressions
           all decls
           all constants

       TYPE_SIZES_GIMPLIFIED in
           all types

   unsigned_flag:

       TYPE_UNSIGNED in
           all types

       DECL_UNSIGNED in
           all decls

   asm_written_flag:

       TREE_ASM_WRITTEN in
           VAR_DECL, FUNCTION_DECL, TYPE_DECL
           RECORD_TYPE, UNION_TYPE, QUAL_UNION_TYPE
           BLOCK, STRING_CST

       SSA_NAME_OCCURS_IN_ABNORMAL_PHI in
           SSA_NAME

   used_flag:

       TREE_USED in
           all expressions
           all decls
           IDENTIFIER_NODE

   nothrow_flag:

       TREE_NOTHROW in
           CALL_EXPR
           FUNCTION_DECL

       TREE_THIS_NOTRAP in
          INDIRECT_REF, MEM_REF, TARGET_MEM_REF, ARRAY_REF, ARRAY_RANGE_REF

       SSA_NAME_IN_FREE_LIST in
          SSA_NAME

       DECL_NONALIASED in
	  VAR_DECL

   deprecated_flag:

       TREE_DEPRECATED in
           all decls
	   all types

       IDENTIFIER_TRANSPARENT_ALIAS in
           IDENTIFIER_NODE

   visited:

       TREE_VISITED in
           all trees (used liberally by many passes)

   saturating_flag:

       TYPE_REVERSE_STORAGE_ORDER in
           RECORD_TYPE, UNION_TYPE, QUAL_UNION_TYPE, ARRAY_TYPE

       TYPE_SATURATING in
           other types

       VAR_DECL_IS_VIRTUAL_OPERAND in
	   VAR_DECL

   nowarning_flag:

       TREE_NO_WARNING in
           all expressions
           all decls

       TYPE_ARTIFICIAL in
           all types

   default_def_flag:

       TYPE_FINAL_P in
	   RECORD_TYPE, UNION_TYPE and QUAL_UNION_TYPE

       TYPE_VECTOR_OPAQUE in
	   VECTOR_TYPE

       SSA_NAME_IS_DEFAULT_DEF in
           SSA_NAME

       DECL_NONLOCAL_FRAME in
	   VAR_DECL

       REF_REVERSE_STORAGE_ORDER in
           BIT_FIELD_REF, MEM_REF

       FUNC_ADDR_BY_DESCRIPTOR in
           ADDR_EXPR

       CALL_EXPR_BY_DESCRIPTOR in
           CALL_EXPR
*/

struct GTY(()) tree_typed {
  struct tree_base base;
  tree type;
};

struct GTY(()) tree_common {
  struct tree_typed typed;
  tree chain;
};

struct GTY(()) tree_int_cst {
  struct tree_typed typed;
  HOST_WIDE_INT val[1];
};


struct GTY(()) tree_real_cst {
  struct tree_typed typed;
  struct real_value * real_cst_ptr;
};

struct GTY(()) tree_fixed_cst {
  struct tree_typed typed;
  struct fixed_value * fixed_cst_ptr;
};

struct GTY(()) tree_string {
  struct tree_typed typed;
  int length;
  char str[1];
};

struct GTY(()) tree_complex {
  struct tree_typed typed;
  tree real;
  tree imag;
};

struct GTY(()) tree_vector {
  struct tree_typed typed;
  tree GTY ((length ("vector_cst_encoded_nelts ((tree) &%h)"))) elts[1];
};

struct GTY(()) tree_poly_int_cst {
  struct tree_typed typed;
  tree coeffs[NUM_POLY_INT_COEFFS];
};

struct GTY(()) tree_identifier {
  struct tree_common common;
  struct ht_identifier id;
};

struct GTY(()) tree_list {
  struct tree_common common;
  tree purpose;
  tree value;
};

struct GTY(()) tree_vec {
  struct tree_common common;
  tree GTY ((length ("TREE_VEC_LENGTH ((tree)&%h)"))) a[1];
};

/* A single element of a CONSTRUCTOR. VALUE holds the actual value of the
   element. INDEX can optionally design the position of VALUE: in arrays,
   it is the index where VALUE has to be placed; in structures, it is the
   FIELD_DECL of the member.  */
struct GTY(()) constructor_elt {
  tree index;
  tree value;
};

struct GTY(()) tree_constructor {
  struct tree_typed typed;
  vec<constructor_elt, va_gc> *elts;
};

enum omp_clause_depend_kind
{
  OMP_CLAUSE_DEPEND_IN,
  OMP_CLAUSE_DEPEND_OUT,
  OMP_CLAUSE_DEPEND_INOUT,
  OMP_CLAUSE_DEPEND_SOURCE,
  OMP_CLAUSE_DEPEND_SINK,
  OMP_CLAUSE_DEPEND_LAST
};

enum omp_clause_proc_bind_kind
{
  /* Numbers should match omp_proc_bind_t enum in omp.h.  */
  OMP_CLAUSE_PROC_BIND_FALSE = 0,
  OMP_CLAUSE_PROC_BIND_TRUE = 1,
  OMP_CLAUSE_PROC_BIND_MASTER = 2,
  OMP_CLAUSE_PROC_BIND_CLOSE = 3,
  OMP_CLAUSE_PROC_BIND_SPREAD = 4,
  OMP_CLAUSE_PROC_BIND_LAST
};

enum omp_clause_linear_kind
{
  OMP_CLAUSE_LINEAR_DEFAULT,
  OMP_CLAUSE_LINEAR_REF,
  OMP_CLAUSE_LINEAR_VAL,
  OMP_CLAUSE_LINEAR_UVAL
};

struct GTY(()) tree_exp {
  struct tree_typed typed;
  location_t locus;
  tree GTY ((special ("tree_exp"),
	     desc ("TREE_CODE ((tree) &%0)")))
    operands[1];
};

/* Immediate use linking structure.  This structure is used for maintaining
   a doubly linked list of uses of an SSA_NAME.  */
struct GTY(()) ssa_use_operand_t {
  struct ssa_use_operand_t* GTY((skip(""))) prev;
  struct ssa_use_operand_t* GTY((skip(""))) next;
  /* Immediate uses for a given SSA name are maintained as a cyclic
     list.  To recognize the root of this list, the location field
     needs to point to the original SSA name.  Since statements and
     SSA names are of different data types, we need this union.  See
     the explanation in struct imm_use_iterator.  */
  union { gimple *stmt; tree ssa_name; } GTY((skip(""))) loc;
  tree *GTY((skip(""))) use;
};

struct GTY(()) tree_ssa_name {
  struct tree_typed typed;

  /* _DECL wrapped by this SSA name.  */
  tree var;

  /* Statement that defines this SSA name.  */
  gimple *def_stmt;

  /* Value range information.  */
  union ssa_name_info_type {
    /* Pointer attributes used for alias analysis.  */
    struct GTY ((tag ("0"))) ptr_info_def *ptr_info;
    /* Value range attributes used for zero/sign extension elimination.  */
    struct GTY ((tag ("1"))) range_info_def *range_info;
  } GTY ((desc ("%1.typed.type ?" \
		"!POINTER_TYPE_P (TREE_TYPE ((tree)&%1)) : 2"))) info;

  /* Immediate uses list for this SSA_NAME.  */
  struct ssa_use_operand_t imm_uses;
};

struct GTY(()) phi_arg_d {
  /* imm_use MUST be the first element in struct because we do some
     pointer arithmetic with it.  See phi_arg_index_from_use.  */
  struct ssa_use_operand_t imm_use;
  tree def;
  location_t locus;
};

struct GTY(()) tree_omp_clause {
  struct tree_common common;
  location_t locus;
  enum omp_clause_code code;
  union omp_clause_subcode {
    enum omp_clause_default_kind   default_kind;
    enum omp_clause_schedule_kind  schedule_kind;
    enum omp_clause_depend_kind    depend_kind;
    /* See include/gomp-constants.h for enum gomp_map_kind's values.  */
    unsigned int		   map_kind;
    enum omp_clause_proc_bind_kind proc_bind_kind;
    enum tree_code                 reduction_code;
    enum omp_clause_linear_kind    linear_kind;
    enum tree_code                 if_modifier;
    /* The dimension a OMP_CLAUSE__GRIDDIM_ clause of a gridified target
       construct describes.  */
    unsigned int		   dimension;
  } GTY ((skip)) subcode;

  /* The gimplification of OMP_CLAUSE_REDUCTION_{INIT,MERGE} for omp-low's
     usage.  */
  gimple_seq gimple_reduction_init;
  gimple_seq gimple_reduction_merge;

  tree GTY ((length ("omp_clause_num_ops[OMP_CLAUSE_CODE ((tree)&%h)]")))
    ops[1];
};

struct GTY(()) tree_block {
  struct tree_base base;
  tree chain;

  unsigned abstract_flag : 1;
  unsigned block_num : 31;

  location_t locus;
  location_t end_locus;

  tree vars;
  vec<tree, va_gc> *nonlocalized_vars;

  tree subblocks;
  tree supercontext;
  tree abstract_origin;
  tree fragment_origin;
  tree fragment_chain;

  /* Pointer to the DWARF lexical block.  */
  struct die_struct *die;
};

struct GTY(()) tree_type_common {
  struct tree_common common;
  tree size;
  tree size_unit;
  tree attributes;
  unsigned int uid;

  unsigned int precision : 10;
  unsigned no_force_blk_flag : 1;
  unsigned needs_constructing_flag : 1;
  unsigned transparent_aggr_flag : 1;
  unsigned restrict_flag : 1;
  unsigned contains_placeholder_bits : 2;

  ENUM_BITFIELD(machine_mode) mode : 8;

  unsigned string_flag : 1;
  unsigned lang_flag_0 : 1;
  unsigned lang_flag_1 : 1;
  unsigned lang_flag_2 : 1;
  unsigned lang_flag_3 : 1;
  unsigned lang_flag_4 : 1;
  unsigned lang_flag_5 : 1;
  unsigned lang_flag_6 : 1;
  unsigned lang_flag_7 : 1;

  /* TYPE_ALIGN in log2; this has to be large enough to hold values
     of the maximum of BIGGEST_ALIGNMENT and MAX_OFILE_ALIGNMENT,
     the latter being usually the larger.  For ELF it is 8<<28,
     so we need to store the value 32 (not 31, as we need the zero
     as well), hence six bits.  */
  unsigned align : 6;
  unsigned warn_if_not_align : 6;
  unsigned typeless_storage : 1;
  unsigned empty_flag : 1;
  unsigned spare : 17;

  alias_set_type alias_set;
  tree pointer_to;
  tree reference_to;
  union tree_type_symtab {
    int GTY ((tag ("TYPE_SYMTAB_IS_ADDRESS"))) address;
    struct die_struct * GTY ((tag ("TYPE_SYMTAB_IS_DIE"))) die;
  } GTY ((desc ("debug_hooks->tree_type_symtab_field"))) symtab;
  tree canonical;
  tree next_variant;
  tree main_variant;
  tree context;
  tree name;
};

struct GTY(()) tree_type_with_lang_specific {
  struct tree_type_common common;
  /* Points to a structure whose details depend on the language in use.  */
  struct lang_type *lang_specific;
};

struct GTY(()) tree_type_non_common {
  struct tree_type_with_lang_specific with_lang_specific;
  tree values;
  tree minval;
  tree maxval;
  tree lang_1;
};

struct GTY (()) tree_binfo {
  struct tree_common common;

  tree offset;
  tree vtable;
  tree virtuals;
  tree vptr_field;
  vec<tree, va_gc> *base_accesses;
  tree inheritance;

  tree vtt_subvtt;
  tree vtt_vptr;

  vec<tree, va_gc> base_binfos;
};

struct GTY(()) tree_decl_minimal {
  struct tree_common common;
  location_t locus;
  unsigned int uid;
  tree name;
  tree context;
};

struct GTY(()) tree_decl_common {
  struct tree_decl_minimal common;
  tree size;

  ENUM_BITFIELD(machine_mode) mode : 8;

  unsigned nonlocal_flag : 1;
  unsigned virtual_flag : 1;
  unsigned ignored_flag : 1;
  unsigned abstract_flag : 1;
  unsigned artificial_flag : 1;
  unsigned preserve_flag: 1;
  unsigned debug_expr_is_from : 1;

  unsigned lang_flag_0 : 1;
  unsigned lang_flag_1 : 1;
  unsigned lang_flag_2 : 1;
  unsigned lang_flag_3 : 1;
  unsigned lang_flag_4 : 1;
  unsigned lang_flag_5 : 1;
  unsigned lang_flag_6 : 1;
  unsigned lang_flag_7 : 1;
  unsigned lang_flag_8 : 1;

  /* In VAR_DECL and PARM_DECL, this is DECL_REGISTER
     IN TRANSLATION_UNIT_DECL, this is TRANSLATION_UNIT_WARN_EMPTY_P.  */
  unsigned decl_flag_0 : 1;
  /* In FIELD_DECL, this is DECL_BIT_FIELD
     In VAR_DECL and FUNCTION_DECL, this is DECL_EXTERNAL.
     In TYPE_DECL, this is TYPE_DECL_SUPPRESS_DEBUG.  */
  unsigned decl_flag_1 : 1;
  /* In FIELD_DECL, this is DECL_NONADDRESSABLE_P
     In VAR_DECL, PARM_DECL and RESULT_DECL, this is
     DECL_HAS_VALUE_EXPR_P.  */
  unsigned decl_flag_2 : 1;
  /* In FIELD_DECL, this is DECL_PADDING_P.  */
  unsigned decl_flag_3 : 1;
  /* Logically, these two would go in a theoretical base shared by var and
     parm decl. */
  unsigned gimple_reg_flag : 1;
  /* In VAR_DECL, PARM_DECL and RESULT_DECL, this is DECL_BY_REFERENCE.  */
  unsigned decl_by_reference_flag : 1;
  /* In a VAR_DECL and PARM_DECL, this is DECL_READ_P.  */
  unsigned decl_read_flag : 1;
  /* In a VAR_DECL or RESULT_DECL, this is DECL_NONSHAREABLE.  */
  unsigned decl_nonshareable_flag : 1;

  /* DECL_OFFSET_ALIGN, used only for FIELD_DECLs.  */
  unsigned int off_align : 6;

  /* DECL_ALIGN.  It should have the same size as TYPE_ALIGN.  */
  unsigned int align : 6;

  /* DECL_WARN_IF_NOT_ALIGN.  It should have the same size as
     TYPE_WARN_IF_NOT_ALIGN.  */
  unsigned int warn_if_not_align : 6;

  /* 14 bits unused.  */

  /* UID for points-to sets, stable over copying from inlining.  */
  unsigned int pt_uid;

  tree size_unit;
  tree initial;
  tree attributes;
  tree abstract_origin;

  /* Points to a structure whose details depend on the language in use.  */
  struct lang_decl *lang_specific;
};

struct GTY(()) tree_decl_with_rtl {
  struct tree_decl_common common;
  rtx rtl;
};

struct GTY(()) tree_field_decl {
  struct tree_decl_common common;

  tree offset;
  tree bit_field_type;
  tree qualifier;
  tree bit_offset;
  tree fcontext;
};

struct GTY(()) tree_label_decl {
  struct tree_decl_with_rtl common;
  int label_decl_uid;
  int eh_landing_pad_nr;
};

struct GTY(()) tree_result_decl {
  struct tree_decl_with_rtl common;
};

struct GTY(()) tree_const_decl {
  struct tree_decl_common common;
};

struct GTY(()) tree_parm_decl {
  struct tree_decl_with_rtl common;
  rtx incoming_rtl;
};

struct GTY(()) tree_decl_with_vis {
 struct tree_decl_with_rtl common;
 tree assembler_name;
 struct symtab_node *symtab_node;

 /* Belong to VAR_DECL exclusively.  */
 unsigned defer_output : 1;
 unsigned hard_register : 1;
 unsigned common_flag : 1;
 unsigned in_text_section : 1;
 unsigned in_constant_pool : 1;
 unsigned dllimport_flag : 1;
 /* Don't belong to VAR_DECL exclusively.  */
 unsigned weak_flag : 1;

 unsigned seen_in_bind_expr : 1;
 unsigned comdat_flag : 1;
 /* Used for FUNCTION_DECL, VAR_DECL and in C++ for TYPE_DECL.  */
 ENUM_BITFIELD(symbol_visibility) visibility : 2;
 unsigned visibility_specified : 1;

 /* Belong to FUNCTION_DECL exclusively.  */
 unsigned init_priority_p : 1;
 /* Used by C++ only.  Might become a generic decl flag.  */
 unsigned shadowed_for_var_p : 1;
 /* Belong to FUNCTION_DECL exclusively.  */
 unsigned cxx_constructor : 1;
 /* Belong to FUNCTION_DECL exclusively.  */
 unsigned cxx_destructor : 1;
 /* Belong to FUNCTION_DECL exclusively.  */
 unsigned final : 1;
 /* Belong to FUNCTION_DECL exclusively.  */
 unsigned regdecl_flag : 1;
 /* 14 unused bits. */
};

struct GTY(()) tree_var_decl {
  struct tree_decl_with_vis common;
};

struct GTY(()) tree_decl_non_common {
  struct tree_decl_with_vis common;
  /* Almost all FE's use this.  */
  tree result;
};

/* FUNCTION_DECL inherits from DECL_NON_COMMON because of the use of the
   arguments/result/saved_tree fields by front ends.   It was either inherit
   FUNCTION_DECL from non_common, or inherit non_common from FUNCTION_DECL,
   which seemed a bit strange.  */

struct GTY(()) tree_function_decl {
  struct tree_decl_non_common common;

  struct function *f;

  /* Arguments of the function.  */
  tree arguments;
  /* The personality function. Used for stack unwinding. */
  tree personality;

  /* Function specific options that are used by this function.  */
  tree function_specific_target;	/* target options */
  tree function_specific_optimization;	/* optimization options */

  /* Generic function body.  */
  tree saved_tree;
  /* Index within a virtual table.  */
  tree vindex;

  /* In a FUNCTION_DECL for which DECL_BUILT_IN holds, this is
     DECL_FUNCTION_CODE.  Otherwise unused.
     ???  The bitfield needs to be able to hold all target function
	  codes as well.  */
  ENUM_BITFIELD(built_in_function) function_code : 12;
  ENUM_BITFIELD(built_in_class) built_in_class : 2;

  unsigned static_ctor_flag : 1;
  unsigned static_dtor_flag : 1;

  unsigned uninlinable : 1;
  unsigned possibly_inlined : 1;
  unsigned novops_flag : 1;
  unsigned returns_twice_flag : 1;
  unsigned malloc_flag : 1;
  unsigned operator_new_flag : 1;
  unsigned declared_inline_flag : 1;
  unsigned no_inline_warning_flag : 1;

  unsigned no_instrument_function_entry_exit : 1;
  unsigned no_limit_stack : 1;
  unsigned disregard_inline_limits : 1;
  unsigned pure_flag : 1;
  unsigned looping_const_or_pure_flag : 1;
  unsigned has_debug_args_flag : 1;
  unsigned versioned_function : 1;
  unsigned lambda_function: 1;
  /* No bits left.  */
};

struct GTY(()) tree_translation_unit_decl {
  struct tree_decl_common common;
  /* Source language of this translation unit.  Used for DWARF output.  */
  const char * GTY((skip(""))) language;
  /* TODO: Non-optimization used to build this translation unit.  */
  /* TODO: Root of a partial DWARF tree for global types and decls.  */
};

struct GTY(()) tree_type_decl {
  struct tree_decl_non_common common;

};

struct GTY ((chain_next ("%h.next"), chain_prev ("%h.prev"))) tree_statement_list_node
 {
  struct tree_statement_list_node *prev;
  struct tree_statement_list_node *next;
  tree stmt;
};

struct GTY(()) tree_statement_list
 {
  struct tree_typed typed;
  struct tree_statement_list_node *head;
  struct tree_statement_list_node *tail;
};


/* Optimization options used by a function.  */

struct GTY(()) tree_optimization_option {
  struct tree_base base;

  /* The optimization options used by the user.  */
  struct cl_optimization *opts;

  /* Target optabs for this set of optimization options.  This is of
     type `struct target_optabs *'.  */
  void *GTY ((atomic)) optabs;

  /* The value of this_target_optabs against which the optabs above were
     generated.  */
  struct target_optabs *GTY ((skip)) base_optabs;
};

/* Forward declaration, defined in target-globals.h.  */

struct GTY(()) target_globals;

/* Target options used by a function.  */

struct GTY(()) tree_target_option {
  struct tree_base base;

  /* Target globals for the corresponding target option.  */
  struct target_globals *globals;

  /* The optimization options used by the user.  */
  struct cl_target_option *opts;
};

/* Define the overall contents of a tree node.
   It may be any of the structures declared above
   for various types of node.  */
union GTY ((ptr_alias (union lang_tree_node),
	    desc ("tree_node_structure (&%h)"), variable_size)) tree_node {
  struct tree_base GTY ((tag ("TS_BASE"))) base;
  struct tree_typed GTY ((tag ("TS_TYPED"))) typed;
  struct tree_common GTY ((tag ("TS_COMMON"))) common;
  struct tree_int_cst GTY ((tag ("TS_INT_CST"))) int_cst;
  struct tree_poly_int_cst GTY ((tag ("TS_POLY_INT_CST"))) poly_int_cst;
  struct tree_real_cst GTY ((tag ("TS_REAL_CST"))) real_cst;
  struct tree_fixed_cst GTY ((tag ("TS_FIXED_CST"))) fixed_cst;
  struct tree_vector GTY ((tag ("TS_VECTOR"))) vector;
  struct tree_string GTY ((tag ("TS_STRING"))) string;
  struct tree_complex GTY ((tag ("TS_COMPLEX"))) complex;
  struct tree_identifier GTY ((tag ("TS_IDENTIFIER"))) identifier;
  struct tree_decl_minimal GTY((tag ("TS_DECL_MINIMAL"))) decl_minimal;
  struct tree_decl_common GTY ((tag ("TS_DECL_COMMON"))) decl_common;
  struct tree_decl_with_rtl GTY ((tag ("TS_DECL_WRTL"))) decl_with_rtl;
  struct tree_decl_non_common  GTY ((tag ("TS_DECL_NON_COMMON")))
    decl_non_common;
  struct tree_parm_decl  GTY  ((tag ("TS_PARM_DECL"))) parm_decl;
  struct tree_decl_with_vis GTY ((tag ("TS_DECL_WITH_VIS"))) decl_with_vis;
  struct tree_var_decl GTY ((tag ("TS_VAR_DECL"))) var_decl;
  struct tree_field_decl GTY ((tag ("TS_FIELD_DECL"))) field_decl;
  struct tree_label_decl GTY ((tag ("TS_LABEL_DECL"))) label_decl;
  struct tree_result_decl GTY ((tag ("TS_RESULT_DECL"))) result_decl;
  struct tree_const_decl GTY ((tag ("TS_CONST_DECL"))) const_decl;
  struct tree_type_decl GTY ((tag ("TS_TYPE_DECL"))) type_decl;
  struct tree_function_decl GTY ((tag ("TS_FUNCTION_DECL"))) function_decl;
  struct tree_translation_unit_decl GTY ((tag ("TS_TRANSLATION_UNIT_DECL")))
    translation_unit_decl;
  struct tree_type_common GTY ((tag ("TS_TYPE_COMMON"))) type_common;
  struct tree_type_with_lang_specific GTY ((tag ("TS_TYPE_WITH_LANG_SPECIFIC")))
    type_with_lang_specific;
  struct tree_type_non_common GTY ((tag ("TS_TYPE_NON_COMMON")))
    type_non_common;
  struct tree_list GTY ((tag ("TS_LIST"))) list;
  struct tree_vec GTY ((tag ("TS_VEC"))) vec;
  struct tree_exp GTY ((tag ("TS_EXP"))) exp;
  struct tree_ssa_name GTY ((tag ("TS_SSA_NAME"))) ssa_name;
  struct tree_block GTY ((tag ("TS_BLOCK"))) block;
  struct tree_binfo GTY ((tag ("TS_BINFO"))) binfo;
  struct tree_statement_list GTY ((tag ("TS_STATEMENT_LIST"))) stmt_list;
  struct tree_constructor GTY ((tag ("TS_CONSTRUCTOR"))) constructor;
  struct tree_omp_clause GTY ((tag ("TS_OMP_CLAUSE"))) omp_clause;
  struct tree_optimization_option GTY ((tag ("TS_OPTIMIZATION"))) optimization;
  struct tree_target_option GTY ((tag ("TS_TARGET_OPTION"))) target_option;
};

/* Structure describing an attribute and a function to handle it.  */
struct attribute_spec {
  /* The name of the attribute (without any leading or trailing __),
     or NULL to mark the end of a table of attributes.  */
  const char *name;
  /* The minimum length of the list of arguments of the attribute.  */
  int min_length;
  /* The maximum length of the list of arguments of the attribute
     (-1 for no maximum).  */
  int max_length;
  /* Whether this attribute requires a DECL.  If it does, it will be passed
     from types of DECLs, function return types and array element types to
     the DECLs, function types and array types respectively; but when
     applied to a type in any other circumstances, it will be ignored with
     a warning.  (If greater control is desired for a given attribute,
     this should be false, and the flags argument to the handler may be
     used to gain greater control in that case.)  */
  bool decl_required;
  /* Whether this attribute requires a type.  If it does, it will be passed
     from a DECL to the type of that DECL.  */
  bool type_required;
  /* Whether this attribute requires a function (or method) type.  If it does,
     it will be passed from a function pointer type to the target type,
     and from a function return type (which is not itself a function
     pointer type) to the function type.  */
  bool function_type_required;
  /* Specifies if attribute affects type's identity.  */
  bool affects_type_identity;
  /* Function to handle this attribute.  NODE points to the node to which
     the attribute is to be applied.  If a DECL, it should be modified in
     place; if a TYPE, a copy should be created.  NAME is the name of the
     attribute (possibly with leading or trailing __).  ARGS is the TREE_LIST
     of the arguments (which may be NULL).  FLAGS gives further information
     about the context of the attribute.  Afterwards, the attributes will
     be added to the DECL_ATTRIBUTES or TYPE_ATTRIBUTES, as appropriate,
     unless *NO_ADD_ATTRS is set to true (which should be done on error,
     as well as in any other cases when the attributes should not be added
     to the DECL or TYPE).  Depending on FLAGS, any attributes to be
     applied to another type or DECL later may be returned;
     otherwise the return value should be NULL_TREE.  This pointer may be
     NULL if no special handling is required beyond the checks implied
     by the rest of this structure.  */
  tree (*handler) (tree *node, tree name, tree args,
		   int flags, bool *no_add_attrs);

  /* Specifies the name of an attribute that's mutually exclusive with
     this one, and whether the relationship applies to the function,
     variable, or type form of the attribute.  */
  struct exclusions {
    const char *name;
    bool function;
    bool variable;
    bool type;
  };

  /* An array of attribute exclusions describing names of other attributes
     that this attribute is mutually exclusive with.  */
  const exclusions *exclude;
};

/* These functions allow a front-end to perform a manual layout of a
   RECORD_TYPE.  (For instance, if the placement of subsequent fields
   depends on the placement of fields so far.)  Begin by calling
   start_record_layout.  Then, call place_field for each of the
   fields.  Then, call finish_record_layout.  See layout_type for the
   default way in which these functions are used.  */
typedef struct record_layout_info_s {
  /* The RECORD_TYPE that we are laying out.  */
  tree t;
  /* The offset into the record so far, in bytes, not including bits in
     BITPOS.  */
  tree offset;
  /* The last known alignment of SIZE.  */
  unsigned int offset_align;
  /* The bit position within the last OFFSET_ALIGN bits, in bits.  */
  tree bitpos;
  /* The alignment of the record so far, in bits.  */
  unsigned int record_align;
  /* The alignment of the record so far, ignoring #pragma pack and
     __attribute__ ((packed)), in bits.  */
  unsigned int unpacked_align;
  /* The previous field laid out.  */
  tree prev_field;
  /* The static variables (i.e., class variables, as opposed to
     instance variables) encountered in T.  */
  vec<tree, va_gc> *pending_statics;
  /* Bits remaining in the current alignment group */
  int remaining_in_alignment;
  /* True if we've seen a packed field that didn't have normal
     alignment anyway.  */
  int packed_maybe_necessary;
} *record_layout_info;

/* Iterator for going through the function arguments.  */
struct function_args_iterator {
  tree next;			/* TREE_LIST pointing to the next argument */
};

/* Structures to map from a tree to another tree.  */
struct GTY(()) tree_map_base {
  tree from;
};

/* Map from a tree to another tree.  */

struct GTY((for_user)) tree_map {
  struct tree_map_base base;
  unsigned int hash;
  tree to;
};

/* Map from a decl tree to another tree.  */
struct GTY((for_user)) tree_decl_map {
  struct tree_map_base base;
  tree to;
};

/* Map from a tree to an int.  */
struct GTY((for_user)) tree_int_map {
  struct tree_map_base base;
  unsigned int to;
};

/* Map from a decl tree to a tree vector.  */
struct GTY((for_user)) tree_vec_map {
  struct tree_map_base base;
  vec<tree, va_gc> *to;
};

/* Abstract iterators for CALL_EXPRs.  These static inline definitions
   have to go towards the end of tree.h so that union tree_node is fully
   defined by this point.  */

/* Structure containing iterator state.  */
struct call_expr_arg_iterator {
  tree t;	/* the call_expr */
  int n;	/* argument count */
  int i;	/* next argument index */
};

struct const_call_expr_arg_iterator {
  const_tree t;	/* the call_expr */
  int n;	/* argument count */
  int i;	/* next argument index */
};

/* The builtin_info structure holds the FUNCTION_DECL of the standard builtin
   function, and flags.  */
struct GTY(()) builtin_info_type {
  tree decl;
  /* Whether the user can use <xxx> instead of explicitly using calls
     to __builtin_<xxx>.  */
  unsigned implicit_p : 1;
  /* Whether the user has provided a declaration of <xxx>.  */
  unsigned declared_p : 1;
};

/* Information about a _FloatN or _FloatNx type that may be
   supported.  */
struct floatn_type_info {
  /* The number N in the type name.  */
  int n;
  /* Whether it is an extended type _FloatNx (true) or an interchange
     type (false).  */
  bool extended;
};


/*---------------------------------------------------------------------------
                                Global variables
---------------------------------------------------------------------------*/
/* Matrix describing the structures contained in a given tree code.  */
extern bool tree_contains_struct[MAX_TREE_CODES][64];

/* Class of tree given its code.  */
extern const enum tree_code_class tree_code_type[];

/* Each tree code class has an associated string representation.
   These must correspond to the tree_code_class entries.  */
extern const char *const tree_code_class_strings[];

/* Number of argument-words in each kind of tree-node.  */
extern const unsigned char tree_code_length[];

/* Vector of all alias pairs for global symbols.  */
extern GTY(()) vec<alias_pair, va_gc> *alias_pairs;

/* Names of all the built_in classes.  */
extern const char *const built_in_class_names[BUILT_IN_LAST];

/* Names of all the built_in functions.  */
extern const char * built_in_names[(int) END_BUILTINS];

/* Number of operands and names for each OMP_CLAUSE node.  */
extern unsigned const char omp_clause_num_ops[];
extern const char * const omp_clause_code_name[];

/* A vector of all translation-units.  */
extern GTY (()) vec<tree, va_gc> *all_translation_units;

/* Vector of standard trees used by the C compiler.  */
extern GTY(()) tree global_trees[TI_MAX];

/* The standard C integer types.  Use integer_type_kind to index into
   this array.  */
extern GTY(()) tree integer_types[itk_none];

/* Types used to represent sizes.  */
extern GTY(()) tree sizetype_tab[(int) stk_type_kind_last];

/* Arrays for keeping track of tree node statistics.  */
extern uint64_t tree_node_counts[];
extern uint64_t tree_node_sizes[];

/* True if we are in gimple form and the actions of the folders need to
   be restricted.  False if we are not in gimple form and folding is not
   restricted to creating gimple expressions.  */
extern bool in_gimple_form;

/* Functional interface to the builtin functions.  */
extern GTY(()) builtin_info_type builtin_info[(int)END_BUILTINS];

/* If nonzero, an upper limit on alignment of structure fields, in bits,  */
extern unsigned int maximum_field_alignment;

/* Points to the FUNCTION_DECL of the function whose body we are reading.  */
extern GTY(()) tree current_function_decl;

/* Nonzero means a FUNC_BEGIN label was emitted.  */
extern GTY(()) const char * current_function_func_begin_label;

/* Information about the _FloatN and _FloatNx types.  */
extern const floatn_type_info floatn_nx_types[NUM_FLOATN_NX_TYPES];

#endif  // GCC_TREE_CORE_H
