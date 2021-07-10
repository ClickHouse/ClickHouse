/* Copyright (C) 2003-2018 Free Software Foundation, Inc.

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

#ifndef GCC_XCOFF_H
#define GCC_XCOFF_H

/* Storage classes in XCOFF object file format designed for DBX's use.
   This info is from the `Files Reference' manual for IBM's AIX version 3
   for the RS6000.  */

#define C_GSYM		0x80
#define C_LSYM		0x81
#define C_PSYM		0x82
#define C_RSYM		0x83
#define C_RPSYM		0x84
#define C_STSYM		0x85

#define C_BCOMM		0x87
#define C_ECOML		0x88
#define C_ECOMM		0x89
#define C_DECL		0x8c
#define C_ENTRY		0x8d
#define C_FUN		0x8e

#endif /* GCC_XCOFF_H */
