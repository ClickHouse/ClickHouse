/* Dwarf2 assembler output helper routines.
   Copyright (C) 2001-2018 Free Software Foundation, Inc.

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

#ifndef GCC_DWARF2ASM_H
#define GCC_DWARF2ASM_H

extern void dw2_assemble_integer (int, rtx);

extern void dw2_asm_output_data_raw (int, unsigned HOST_WIDE_INT);

extern void dw2_asm_output_data (int, unsigned HOST_WIDE_INT,
				 const char *, ...)
     ATTRIBUTE_NULL_PRINTF_3;

extern void dw2_asm_output_delta (int, const char *, const char *,
				  const char *, ...)
     ATTRIBUTE_NULL_PRINTF_4;

extern void dw2_asm_output_vms_delta (int, const char *, const char *,
				      const char *, ...)
     ATTRIBUTE_NULL_PRINTF_4;

extern void dw2_asm_output_offset (int, const char *, section *,
				   const char *, ...)
     ATTRIBUTE_NULL_PRINTF_4;

extern void dw2_asm_output_offset (int, const char *, HOST_WIDE_INT,
				   section *, const char *, ...)
     ATTRIBUTE_NULL_PRINTF_5;

extern void dw2_asm_output_addr (int, const char *, const char *, ...)
     ATTRIBUTE_NULL_PRINTF_3;

extern void dw2_asm_output_addr_rtx (int, rtx, const char *, ...)
     ATTRIBUTE_NULL_PRINTF_3;

extern void dw2_asm_output_encoded_addr_rtx (int, rtx, bool,
					     const char *, ...)
     ATTRIBUTE_NULL_PRINTF_4;

extern void dw2_asm_output_nstring (const char *, size_t,
				    const char *, ...)
     ATTRIBUTE_NULL_PRINTF_3;

extern void dw2_asm_output_data_uleb128_raw (unsigned HOST_WIDE_INT);

extern void dw2_asm_output_data_uleb128	(unsigned HOST_WIDE_INT,
					 const char *, ...)
     ATTRIBUTE_NULL_PRINTF_2;

extern void dw2_asm_output_data_sleb128_raw (HOST_WIDE_INT);

extern void dw2_asm_output_data_sleb128	(HOST_WIDE_INT,
					 const char *, ...)
     ATTRIBUTE_NULL_PRINTF_2;

extern void dw2_asm_output_symname_uleb128 (const char *,
					    const char *, ...)
     ATTRIBUTE_NULL_PRINTF_2;

extern void dw2_asm_output_delta_uleb128 (const char *, const char *,
					  const char *, ...)
     ATTRIBUTE_NULL_PRINTF_3;

extern int size_of_uleb128 (unsigned HOST_WIDE_INT);
extern int size_of_sleb128 (HOST_WIDE_INT);
extern int size_of_encoded_value (int);
extern const char *eh_data_format_name (int);

extern rtx dw2_force_const_mem (rtx, bool);
extern void dw2_output_indirect_constants (void);

/* These are currently unused.  */

#if 0
extern void dw2_asm_output_pcrel (int, const char *, const char *, ...)
     ATTRIBUTE_NULL_PRINTF_3;

extern void dw2_asm_output_delta_sleb128 (const char *, const char *,
					  const char *, ...)
     ATTRIBUTE_NULL_PRINTF_3;
#endif

#endif /* GCC_DWARF2ASM_H */
