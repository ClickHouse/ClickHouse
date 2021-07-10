/* RTL specific diagnostic subroutines for GCC
   Copyright (C) 2010-2018 Free Software Foundation, Inc.

This file is part of GCC.

GCC is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option)
any later version.

GCC is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with GCC; see the file COPYING3.  If not see
<http://www.gnu.org/licenses/>.  */

#ifndef GCC_RTL_ERROR_H
#define GCC_RTL_ERROR_H

#include "rtl.h"
#include "diagnostic-core.h"

extern void error_for_asm (const rtx_insn *, const char *,
			   ...) ATTRIBUTE_GCC_DIAG(2,3);
extern void warning_for_asm (const rtx_insn *, const char *,
			     ...) ATTRIBUTE_GCC_DIAG(2,3);

#endif /* GCC_RTL_ERROR_H */
