/* Definitions for LTO section names.
   Copyright (C) 2013-2018 Free Software Foundation, Inc.

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

#ifndef GCC_LTO_SECTION_NAMES_H
#define GCC_LTO_SECTION_NAMES_H

/* The string that is the prefix on the section names we make for lto.
   For decls the DECL_ASSEMBLER_NAME is appended to make the section
   name for the functions and static_initializers.  For other types of
   sections a '.' and the section type are appended.  */
#define LTO_SECTION_NAME_PREFIX ".gnu.lto_"
#define OFFLOAD_SECTION_NAME_PREFIX ".gnu.offload_lto_"

/* Can be either OFFLOAD_SECTION_NAME_PREFIX when we stream IR for offload
   compiler, or LTO_SECTION_NAME_PREFIX for LTO case.  */
extern const char *section_name_prefix;

/* Segment name for LTO sections.  This is only used for Mach-O.  */

#define LTO_SEGMENT_NAME "__GNU_LTO"

#define OFFLOAD_VAR_TABLE_SECTION_NAME ".gnu.offload_vars"
#define OFFLOAD_FUNC_TABLE_SECTION_NAME ".gnu.offload_funcs"

#endif /* GCC_LTO_SECTION_NAMES_H */
