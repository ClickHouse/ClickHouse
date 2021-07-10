/* params.h - Run-time parameters.
   Copyright (C) 2001-2018 Free Software Foundation, Inc.
   Written by Mark Mitchell <mark@codesourcery.com>.

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

/* This module provides a means for setting integral parameters
   dynamically.  Instead of encoding magic numbers in various places,
   use this module to organize all the magic numbers in a single
   place.  The values of the parameters can be set on the
   command-line, thereby providing a way to control the amount of
   effort spent on particular optimization passes, or otherwise tune
   the behavior of the compiler.

   Since their values can be set on the command-line, these parameters
   should not be used for non-dynamic memory allocation.  */

#ifndef GCC_PARAMS_H
#define GCC_PARAMS_H

/* No parameter shall have this value.  */

#define INVALID_PARAM_VAL (-1)

/* The information associated with each parameter.  */

struct param_info
{
  /* The name used with the `--param <name>=<value>' switch to set this
     value.  */
  const char *option;

  /* The default value.  */
  int default_value;

  /* Minimum acceptable value.  */
  int min_value;

  /* Maximum acceptable value, if greater than minimum  */
  int max_value;

  /* A short description of the option.  */
  const char *help;

  /* The optional names corresponding to the values.  */
  const char **value_names;
};

/* An array containing the compiler parameters and their current
   values.  */

extern param_info *compiler_params;

/* Returns the number of entries in the table, for the use by plugins.  */
extern size_t get_num_compiler_params (void);

/* Add the N PARAMS to the current list of compiler parameters.  */

extern void add_params (const param_info params[], size_t n);

/* Set the VALUE associated with the parameter given by NAME in the
   table PARAMS using PARAMS_SET to indicate which have been
   explicitly set.  */

extern void set_param_value (const char *name, int value,
			     int *params, int *params_set);


/* The parameters in use by language-independent code.  */

enum compiler_param
{
#include "params.list"
  LAST_PARAM
};

extern bool find_param (const char *, enum compiler_param *);
extern const char *find_param_fuzzy (const char *name);
extern bool param_string_value_p (enum compiler_param, const char *, int *);

/* The value of the parameter given by ENUM.  Not an lvalue.  */
#define PARAM_VALUE(ENUM) \
  ((int) global_options.x_param_values[(int) ENUM])

/* Set the value of the parameter given by NUM to VALUE, implicitly,
   if it has not been set explicitly by the user, in the table PARAMS
   using PARAMS_SET to indicate which have been explicitly set.  */

extern void maybe_set_param_value (compiler_param num, int value,
				   int *params, int *params_set);

/* Set the default value of a parameter given by NUM to VALUE, before
   option processing.  */

extern void set_default_param_value (compiler_param num, int value);

/* Add all parameters and default values that can be set in both the
   driver and the compiler proper.  */

extern void global_init_params (void);

/* Note that all parameters have been added and all default values
   set.  */
extern void finish_params (void);

/* Reset all state in params.c  */

extern void params_c_finalize (void);

/* Return the default value of parameter NUM.  */

extern int default_param_value (compiler_param num);

/* Initialize an array PARAMS with default values of the
   parameters.  */
extern void init_param_values (int *params);

/* Macros for the various parameters.  */
#define MAX_INLINE_INSNS_SINGLE \
  PARAM_VALUE (PARAM_MAX_INLINE_INSNS_SINGLE)
#define MAX_INLINE_INSNS \
  PARAM_VALUE (PARAM_MAX_INLINE_INSNS)
#define MAX_INLINE_SLOPE \
  PARAM_VALUE (PARAM_MAX_INLINE_SLOPE)
#define MIN_INLINE_INSNS \
  PARAM_VALUE (PARAM_MIN_INLINE_INSNS)
#define MAX_INLINE_INSNS_AUTO \
  PARAM_VALUE (PARAM_MAX_INLINE_INSNS_AUTO)
#define MAX_VARIABLE_EXPANSIONS \
  PARAM_VALUE (PARAM_MAX_VARIABLE_EXPANSIONS)
#define MIN_VECT_LOOP_BOUND \
  PARAM_VALUE (PARAM_MIN_VECT_LOOP_BOUND)
#define MAX_DELAY_SLOT_INSN_SEARCH \
  PARAM_VALUE (PARAM_MAX_DELAY_SLOT_INSN_SEARCH)
#define MAX_DELAY_SLOT_LIVE_SEARCH \
  PARAM_VALUE (PARAM_MAX_DELAY_SLOT_LIVE_SEARCH)
#define MAX_PENDING_LIST_LENGTH \
  PARAM_VALUE (PARAM_MAX_PENDING_LIST_LENGTH)
#define MAX_GCSE_MEMORY \
  ((size_t) PARAM_VALUE (PARAM_MAX_GCSE_MEMORY))
#define MAX_GCSE_INSERTION_RATIO \
  ((size_t) PARAM_VALUE (PARAM_MAX_GCSE_INSERTION_RATIO))
#define GCSE_AFTER_RELOAD_PARTIAL_FRACTION \
  PARAM_VALUE (PARAM_GCSE_AFTER_RELOAD_PARTIAL_FRACTION)
#define GCSE_AFTER_RELOAD_CRITICAL_FRACTION \
  PARAM_VALUE (PARAM_GCSE_AFTER_RELOAD_CRITICAL_FRACTION)
#define GCSE_COST_DISTANCE_RATIO \
  PARAM_VALUE (PARAM_GCSE_COST_DISTANCE_RATIO)
#define GCSE_UNRESTRICTED_COST \
  PARAM_VALUE (PARAM_GCSE_UNRESTRICTED_COST)
#define MAX_HOIST_DEPTH \
  PARAM_VALUE (PARAM_MAX_HOIST_DEPTH)
#define MAX_UNROLLED_INSNS \
  PARAM_VALUE (PARAM_MAX_UNROLLED_INSNS)
#define MAX_SMS_LOOP_NUMBER \
  PARAM_VALUE (PARAM_MAX_SMS_LOOP_NUMBER)
#define SMS_MAX_II_FACTOR \
  PARAM_VALUE (PARAM_SMS_MAX_II_FACTOR)
#define SMS_DFA_HISTORY \
  PARAM_VALUE (PARAM_SMS_DFA_HISTORY)
#define SMS_LOOP_AVERAGE_COUNT_THRESHOLD \
  PARAM_VALUE (PARAM_SMS_LOOP_AVERAGE_COUNT_THRESHOLD)
#define INTEGER_SHARE_LIMIT \
  PARAM_VALUE (PARAM_INTEGER_SHARE_LIMIT)
#define MAX_LAST_VALUE_RTL \
  PARAM_VALUE (PARAM_MAX_LAST_VALUE_RTL)
#define MIN_VIRTUAL_MAPPINGS \
  PARAM_VALUE (PARAM_MIN_VIRTUAL_MAPPINGS)
#define VIRTUAL_MAPPINGS_TO_SYMS_RATIO \
  PARAM_VALUE (PARAM_VIRTUAL_MAPPINGS_TO_SYMS_RATIO)
#define MAX_FIELDS_FOR_FIELD_SENSITIVE \
  ((size_t) PARAM_VALUE (PARAM_MAX_FIELDS_FOR_FIELD_SENSITIVE))
#define MAX_SCHED_READY_INSNS \
  PARAM_VALUE (PARAM_MAX_SCHED_READY_INSNS)
#define PREFETCH_LATENCY \
  PARAM_VALUE (PARAM_PREFETCH_LATENCY)
#define SIMULTANEOUS_PREFETCHES \
  PARAM_VALUE (PARAM_SIMULTANEOUS_PREFETCHES)
#define L1_CACHE_SIZE \
  PARAM_VALUE (PARAM_L1_CACHE_SIZE)
#define L1_CACHE_LINE_SIZE \
  PARAM_VALUE (PARAM_L1_CACHE_LINE_SIZE)
#define L2_CACHE_SIZE \
  PARAM_VALUE (PARAM_L2_CACHE_SIZE)
#define USE_CANONICAL_TYPES \
  PARAM_VALUE (PARAM_USE_CANONICAL_TYPES)
#define IRA_MAX_LOOPS_NUM \
  PARAM_VALUE (PARAM_IRA_MAX_LOOPS_NUM)
#define IRA_MAX_CONFLICT_TABLE_SIZE \
  PARAM_VALUE (PARAM_IRA_MAX_CONFLICT_TABLE_SIZE)
#define IRA_LOOP_RESERVED_REGS \
  PARAM_VALUE (PARAM_IRA_LOOP_RESERVED_REGS)
#define LRA_MAX_CONSIDERED_RELOAD_PSEUDOS \
  PARAM_VALUE (PARAM_LRA_MAX_CONSIDERED_RELOAD_PSEUDOS)
#define LRA_INHERITANCE_EBB_PROBABILITY_CUTOFF \
  PARAM_VALUE (PARAM_LRA_INHERITANCE_EBB_PROBABILITY_CUTOFF)
#define SWITCH_CONVERSION_BRANCH_RATIO \
  PARAM_VALUE (PARAM_SWITCH_CONVERSION_BRANCH_RATIO)
#define LOOP_INVARIANT_MAX_BBS_IN_LOOP \
  PARAM_VALUE (PARAM_LOOP_INVARIANT_MAX_BBS_IN_LOOP)
#define SLP_MAX_INSNS_IN_BB \
  PARAM_VALUE (PARAM_SLP_MAX_INSNS_IN_BB)
#define MIN_INSN_TO_PREFETCH_RATIO \
  PARAM_VALUE (PARAM_MIN_INSN_TO_PREFETCH_RATIO)
#define PREFETCH_MIN_INSN_TO_MEM_RATIO \
  PARAM_VALUE (PARAM_PREFETCH_MIN_INSN_TO_MEM_RATIO)
#define MIN_NONDEBUG_INSN_UID \
  PARAM_VALUE (PARAM_MIN_NONDEBUG_INSN_UID)
#define MAX_STORES_TO_SINK \
  PARAM_VALUE (PARAM_MAX_STORES_TO_SINK)
#define ALLOW_LOAD_DATA_RACES \
  PARAM_VALUE (PARAM_ALLOW_LOAD_DATA_RACES)
#define ALLOW_STORE_DATA_RACES \
  PARAM_VALUE (PARAM_ALLOW_STORE_DATA_RACES)
#define ALLOW_PACKED_LOAD_DATA_RACES \
  PARAM_VALUE (PARAM_ALLOW_PACKED_LOAD_DATA_RACES)
#define ALLOW_PACKED_STORE_DATA_RACES \
  PARAM_VALUE (PARAM_ALLOW_PACKED_STORE_DATA_RACES)
#define ASAN_STACK \
  PARAM_VALUE (PARAM_ASAN_STACK)
#define ASAN_PROTECT_ALLOCAS \
  PARAM_VALUE (PARAM_ASAN_PROTECT_ALLOCAS)
#define ASAN_GLOBALS \
  PARAM_VALUE (PARAM_ASAN_GLOBALS)
#define ASAN_INSTRUMENT_READS \
  PARAM_VALUE (PARAM_ASAN_INSTRUMENT_READS)
#define ASAN_INSTRUMENT_WRITES \
  PARAM_VALUE (PARAM_ASAN_INSTRUMENT_WRITES)
#define ASAN_MEMINTRIN \
  PARAM_VALUE (PARAM_ASAN_MEMINTRIN)
#define ASAN_USE_AFTER_RETURN \
  PARAM_VALUE (PARAM_ASAN_USE_AFTER_RETURN)
#define ASAN_INSTRUMENTATION_WITH_CALL_THRESHOLD \
  PARAM_VALUE (PARAM_ASAN_INSTRUMENTATION_WITH_CALL_THRESHOLD)
#define ASAN_PARAM_USE_AFTER_SCOPE_DIRECT_EMISSION_THRESHOLD \
  ((unsigned) PARAM_VALUE (PARAM_USE_AFTER_SCOPE_DIRECT_EMISSION_THRESHOLD))

#endif /* ! GCC_PARAMS_H */
