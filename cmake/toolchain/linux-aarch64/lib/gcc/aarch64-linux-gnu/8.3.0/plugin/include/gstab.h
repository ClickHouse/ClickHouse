/* Copyright (C) 1997-2018 Free Software Foundation, Inc.

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

#ifndef GCC_GSTAB_H
#define GCC_GSTAB_H

#define __define_stab(NAME, CODE, STRING) NAME=CODE,

enum
{
#include "stab.def"
LAST_UNUSED_STAB_CODE
};

/* stabs debug codes really are integers with expressive names.  */
typedef int stab_code_type;

#undef __define_stab

#endif /* ! GCC_GSTAB_H */
