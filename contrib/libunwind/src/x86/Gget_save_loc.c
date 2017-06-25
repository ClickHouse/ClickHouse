/* libunwind - a platform-independent unwind library
   Copyright (C) 2004 Hewlett-Packard Co
        Contributed by David Mosberger-Tang <davidm@hpl.hp.com>

This file is part of libunwind.

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.  */

#include "unwind_i.h"

PROTECTED int
unw_get_save_loc (unw_cursor_t *cursor, int reg, unw_save_loc_t *sloc)
{
  struct cursor *c = (struct cursor *) cursor;
  dwarf_loc_t loc;

  loc = DWARF_NULL_LOC;         /* default to "not saved" */

  switch (reg)
    {
    case UNW_X86_EIP: loc = c->dwarf.loc[EIP]; break;
    case UNW_X86_CFA: break;
    case UNW_X86_EAX: loc = c->dwarf.loc[EAX]; break;
    case UNW_X86_ECX: loc = c->dwarf.loc[ECX]; break;
    case UNW_X86_EDX: loc = c->dwarf.loc[EDX]; break;
    case UNW_X86_EBX: loc = c->dwarf.loc[EBX]; break;
    case UNW_X86_ESP: loc = c->dwarf.loc[ESP]; break;
    case UNW_X86_EBP: loc = c->dwarf.loc[EBP]; break;
    case UNW_X86_ESI: loc = c->dwarf.loc[ESI]; break;
    case UNW_X86_EDI: loc = c->dwarf.loc[EDI]; break;
    case UNW_X86_EFLAGS: loc = c->dwarf.loc[EFLAGS]; break;
    case UNW_X86_TRAPNO: loc = c->dwarf.loc[TRAPNO]; break;
    case UNW_X86_ST0: loc = c->dwarf.loc[ST0]; break;

    case UNW_X86_FCW:
    case UNW_X86_FSW:
    case UNW_X86_FTW:
    case UNW_X86_FOP:
    case UNW_X86_FCS:
    case UNW_X86_FIP:
    case UNW_X86_FEA:
    case UNW_X86_FDS:
    case UNW_X86_MXCSR:
    case UNW_X86_GS:
    case UNW_X86_FS:
    case UNW_X86_ES:
    case UNW_X86_DS:
    case UNW_X86_SS:
    case UNW_X86_CS:
    case UNW_X86_TSS:
    case UNW_X86_LDT:
      loc = x86_scratch_loc (c, reg);
      break;

      /* stacked fp registers */
    case UNW_X86_ST1:
    case UNW_X86_ST2:
    case UNW_X86_ST3:
    case UNW_X86_ST4:
    case UNW_X86_ST5:
    case UNW_X86_ST6:
    case UNW_X86_ST7:
      /* SSE fp registers */
    case UNW_X86_XMM0_lo:
    case UNW_X86_XMM0_hi:
    case UNW_X86_XMM1_lo:
    case UNW_X86_XMM1_hi:
    case UNW_X86_XMM2_lo:
    case UNW_X86_XMM2_hi:
    case UNW_X86_XMM3_lo:
    case UNW_X86_XMM3_hi:
    case UNW_X86_XMM4_lo:
    case UNW_X86_XMM4_hi:
    case UNW_X86_XMM5_lo:
    case UNW_X86_XMM5_hi:
    case UNW_X86_XMM6_lo:
    case UNW_X86_XMM6_hi:
    case UNW_X86_XMM7_lo:
    case UNW_X86_XMM7_hi:
    case UNW_X86_XMM0:
    case UNW_X86_XMM1:
    case UNW_X86_XMM2:
    case UNW_X86_XMM3:
    case UNW_X86_XMM4:
    case UNW_X86_XMM5:
    case UNW_X86_XMM6:
    case UNW_X86_XMM7:
      loc = x86_scratch_loc (c, reg);
      break;

    default:
      break;
    }

  memset (sloc, 0, sizeof (*sloc));

  if (DWARF_IS_NULL_LOC (loc))
    {
      sloc->type = UNW_SLT_NONE;
      return 0;
    }

#if !defined(UNW_LOCAL_ONLY)
  if (DWARF_IS_REG_LOC (loc))
    {
      sloc->type = UNW_SLT_REG;
      sloc->u.regnum = DWARF_GET_LOC (loc);
    }
  else
#endif
    {
      sloc->type = UNW_SLT_MEMORY;
      sloc->u.addr = DWARF_GET_LOC (loc);
    }
  return 0;
}
