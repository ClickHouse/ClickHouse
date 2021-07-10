/* Compilation switch flag definitions for GCC.
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

#ifndef GCC_FLAGS_H
#define GCC_FLAGS_H

#if !defined(IN_LIBGCC2) && !defined(IN_TARGET_LIBS) && !defined(IN_RTS)

/* Names of debug_info_type, for error messages.  */
extern const char *const debug_type_names[];

extern void strip_off_ending (char *, int);
extern int base_of_path (const char *path, const char **base_out);

/* Return true iff flags are set as if -ffast-math.  */
extern bool fast_math_flags_set_p (const struct gcc_options *);
extern bool fast_math_flags_struct_set_p (struct cl_optimization *);


/* Now the symbols that are set with `-f' switches.  */

/* True if printing into -fdump-final-insns= dump.  */

extern bool final_insns_dump_p;


/* Other basic status info about current function.  */

/* Target-dependent global state.  */
struct target_flag_state {
  /* Values of the -falign-* flags: how much to align labels in code.
     0 means `use default', 1 means `don't align'.
     For each variable, there is an _log variant which is the power
     of two not less than the variable, for .align output.  */
  int x_align_loops_log;
  int x_align_loops_max_skip;
  int x_align_jumps_log;
  int x_align_jumps_max_skip;
  int x_align_labels_log;
  int x_align_labels_max_skip;
  int x_align_functions_log;

  /* The excess precision currently in effect.  */
  enum excess_precision x_flag_excess_precision;
};

extern struct target_flag_state default_target_flag_state;
#if SWITCHABLE_TARGET
extern struct target_flag_state *this_target_flag_state;
#else
#define this_target_flag_state (&default_target_flag_state)
#endif

#define align_loops_log \
  (this_target_flag_state->x_align_loops_log)
#define align_loops_max_skip \
  (this_target_flag_state->x_align_loops_max_skip)
#define align_jumps_log \
  (this_target_flag_state->x_align_jumps_log)
#define align_jumps_max_skip \
  (this_target_flag_state->x_align_jumps_max_skip)
#define align_labels_log \
  (this_target_flag_state->x_align_labels_log)
#define align_labels_max_skip \
  (this_target_flag_state->x_align_labels_max_skip)
#define align_functions_log \
  (this_target_flag_state->x_align_functions_log)
#define flag_excess_precision \
  (this_target_flag_state->x_flag_excess_precision)

/* Returns TRUE if generated code should match ABI version N or
   greater is in use.  */

#define abi_version_at_least(N) \
  (flag_abi_version == 0 || flag_abi_version >= (N))

/* Whether to emit an overflow warning whose code is C.  */
#define issue_strict_overflow_warning(c) (warn_strict_overflow >= (int) (c))

#endif

#endif /* ! GCC_FLAGS_H */
