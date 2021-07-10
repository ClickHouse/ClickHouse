/* Export function prototypes from explow.c.
   Copyright (C) 2015-2018 Free Software Foundation, Inc.

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

#ifndef GCC_EXPLOW_H
#define GCC_EXPLOW_H

/* Return a memory reference like MEMREF, but which is known to have a
   valid address.  */
extern rtx validize_mem (rtx);

extern rtx use_anchored_address (rtx);

/* Copy given rtx to a new temp reg and return that.  */
extern rtx copy_to_reg (rtx);

/* Like copy_to_reg but always make the reg Pmode.  */
extern rtx copy_addr_to_reg (rtx);

/* Like copy_to_reg but always make the reg the specified mode MODE.  */
extern rtx copy_to_mode_reg (machine_mode, rtx);

/* Copy given rtx to given temp reg and return that.  */
extern rtx copy_to_suggested_reg (rtx, rtx, machine_mode);

/* Copy a value to a register if it isn't already a register.
   Args are mode (in case value is a constant) and the value.  */
extern rtx force_reg (machine_mode, rtx);

/* Return given rtx, copied into a new temp reg if it was in memory.  */
extern rtx force_not_mem (rtx);

/* Return mode and signedness to use when an argument or result in the
   given mode is promoted.  */
extern machine_mode promote_function_mode (const_tree, machine_mode, int *,
					        const_tree, int);

/* Return mode and signedness to use when an object in the given mode
   is promoted.  */
extern machine_mode promote_mode (const_tree, machine_mode, int *);

/* Return mode and signedness to use when object is promoted.  */
machine_mode promote_decl_mode (const_tree, int *);

/* Return mode and signedness to use when object is promoted.  */
machine_mode promote_ssa_mode (const_tree, int *);

/* Remove some bytes from the stack.  An rtx says how many.  */
extern void adjust_stack (rtx);

/* Add some bytes to the stack.  An rtx says how many.  */
extern void anti_adjust_stack (rtx);

/* Add some bytes to the stack while probing it.  An rtx says how many. */
extern void anti_adjust_stack_and_probe (rtx, bool);

/* Support for building allocation/probing loops for stack-clash
   protection of dyamically allocated stack space.  */
extern void compute_stack_clash_protection_loop_data (rtx *, rtx *, rtx *,
						      HOST_WIDE_INT *, rtx);
extern void emit_stack_clash_protection_probe_loop_start (rtx *, rtx *,
							  rtx, bool);
extern void emit_stack_clash_protection_probe_loop_end (rtx, rtx,
							rtx, bool);

/* This enum is used for the following two functions.  */
enum save_level {SAVE_BLOCK, SAVE_FUNCTION, SAVE_NONLOCAL};

/* Save the stack pointer at the specified level.  */
extern void emit_stack_save (enum save_level, rtx *);

/* Restore the stack pointer from a save area of the specified level.  */
extern void emit_stack_restore (enum save_level, rtx);

/* Invoke emit_stack_save for the nonlocal_goto_save_area.  */
extern void update_nonlocal_goto_save_area (void);

/* Record a new stack level.  */
extern void record_new_stack_level (void);

/* Allocate some space on the stack dynamically and return its address.  */
extern rtx allocate_dynamic_stack_space (rtx, unsigned, unsigned,
					 HOST_WIDE_INT, bool);

/* Calculate the necessary size of a constant dynamic stack allocation from the
   size of the variable area.  */
extern void get_dynamic_stack_size (rtx *, unsigned, unsigned, HOST_WIDE_INT *);

/* Returns the address of the dynamic stack space without allocating it.  */
extern rtx get_dynamic_stack_base (poly_int64, unsigned);

/* Emit one stack probe at ADDRESS, an address within the stack.  */
extern void emit_stack_probe (rtx);

/* Probe a range of stack addresses from FIRST to FIRST+SIZE, inclusive.
   FIRST is a constant and size is a Pmode RTX.  These are offsets from
   the current stack pointer.  STACK_GROWS_DOWNWARD says whether to add
   or subtract them from the stack pointer.  */
extern void probe_stack_range (HOST_WIDE_INT, rtx);

/* Return an rtx that refers to the value returned by a library call
   in its original home.  This becomes invalid if any more code is emitted.  */
extern rtx hard_libcall_value (machine_mode, rtx);

/* Return an rtx that refers to the value returned by a function
   in its original home.  This becomes invalid if any more code is emitted.  */
extern rtx hard_function_value (const_tree, const_tree, const_tree, int);

/* Convert arg to a valid memory address for specified machine mode that points
   to a specific named address space, by emitting insns to perform arithmetic
   if necessary.  */
extern rtx memory_address_addr_space (machine_mode, rtx, addr_space_t);

extern rtx eliminate_constant_term (rtx, rtx *);

/* Like memory_address_addr_space, except assume the memory address points to
   the generic named address space.  */
#define memory_address(MODE,RTX) \
	memory_address_addr_space ((MODE), (RTX), ADDR_SPACE_GENERIC)

#endif /* GCC_EXPLOW_H */
